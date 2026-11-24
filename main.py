"""Runs the complete SIS2 pipeline end-to-end:

    GBIF API -> raw files -> Kafka Producer -> Kafka topic -> Kafka Consumer
      -> Pandera validation -> pandas/NumPy cleaning -> SQLite (loaded twice)
      -> EDA + figures -> pipeline verification report

Every stage can also be started on its own (see README). Usage:

    python main.py                     # full run (topic is reset first -> clean finite stream)
    python main.py --skip-collect      # reuse data/raw/gbif_raw_records.jsonl (offline API)
    python main.py --keep-topic        # do not reset the topic (consumer reads only new messages)
    python main.py --no-invalid-messages
"""

import argparse
import asyncio
import logging
import sqlite3
import sys
import time

import pandas as pd

from src import analysis, api_client, cleaner, consumer, loader, producer, validator
from src.config import settings
from src.kafka_admin import describe, reset_topic
from src.stats import load_stats

log = logging.getLogger("main")


def banner(title: str) -> None:
    print(f"\n{'=' * 78}\n {title}\n{'=' * 78}", flush=True)


def verify() -> str:
    """Stage 7 - compare record counts of all stages and check the database."""
    s = load_stats()
    col, prod, cons = s["collection"], s["producer"], s["consumer"]
    val, cln, lod = s["validation"], s["cleaning"], s["loading"]

    with sqlite3.connect(settings.sqlite_db_path) as conn:
        db_ids = set(pd.read_sql_query("SELECT gbif_id FROM occurrences", conn)["gbif_id"])
        test_rows = conn.execute("SELECT COUNT(*) FROM occurrences WHERE gbif_id < 0 "
                                 "OR dataset_name = 'SIS2_VALIDATION_TEST'").fetchone()[0]
        dup_rows = conn.execute("SELECT COUNT(*) FROM (SELECT gbif_id FROM occurrences "
                                "GROUP BY gbif_id HAVING COUNT(*) > 1)").fetchone()[0]
    processed_ids = set(pd.read_csv(settings.processed_csv)["gbif_id"])
    raw_ids = set(pd.read_csv(settings.raw_csv)["gbif_id"])

    rows = [
        ("1. Records collected from GBIF API", col["records_collected"]),
        ("2. Messages published to Kafka (valid records)", prod["messages_published"]),
        ("   + deliberately broken demo messages", prod.get("invalid_demo_messages", 0)),
        ("3. Messages consumed and deserialized", cons["messages_consumed"]),
        ("   rejected by consumer (bad JSON / contract)", cons["deserialization_errors"]),
        ("4. Pandera: valid records", val["valid_records"]),
        ("   Pandera: invalid real records (quarantined)", val["invalid_real_records"]),
        ("   Pandera: invalid TEST records detected", val["invalid_test_records_detected"]),
        ("5. Records after cleaning", cln["records_out"]),
        ("   duplicates removed by cleaner", cln["duplicate_gbif_ids_removed"]
         + cln["same_observation_duplicates_removed"]),
        ("6. Records stored in SQLite", lod["stored_records"]),
    ]
    width = max(len(r[0]) for r in rows) + 2
    lines = [f"{name:<{width}}{value:>8}" for name, value in rows]

    checks = {
        "collected == published": col["records_collected"] == prod["messages_published"],
        "published == consumed": prod["messages_published"] == cons["messages_consumed"],
        "broken demo messages all rejected": cons["deserialization_errors"] == prod.get("invalid_demo_messages", 0),
        "consumed == valid + invalid real": cons["messages_consumed"] == val["valid_records"] + val["invalid_real_records"],
        "all test records detected": val["invalid_test_records_detected"] == val["test_records_added"],
        "valid - duplicates == cleaned": val["valid_records"] - cln["duplicate_gbif_ids_removed"]
                                          - cln["same_observation_duplicates_removed"] == cln["records_out"],
        "cleaned == stored": cln["records_out"] == lod["stored_records"],
        "DB ids == processed ids": db_ids == processed_ids,
        "DB ids subset of raw API ids": db_ids <= raw_ids,
        "no test records in DB": test_rows == 0,
        "no duplicate keys in DB": dup_rows == 0,
        "second load inserted/updated nothing": lod.get("idempotency", {}).get("idempotent", False),
    }
    lines.append("")
    lines += [f"[{'PASS' if ok else 'FAIL'}] {name}" for name, ok in checks.items()]
    lines.append("")
    lines.append("Explanation of differences:")
    lines.append(f"  * {prod.get('invalid_demo_messages', 0)} extra messages were broken on purpose; the consumer "
                 "rejected them, so consumed = published valid records.")
    lines.append(f"  * {val['invalid_real_records']} real record(s) failed Pandera rules and were quarantined to "
                 "data/invalid_records.csv; the test records were also rejected and never left the validator.")
    lines.append(f"  * {cln['same_observation_duplicates_removed']} records were the same observation (same taxon, "
                 "coordinates and day) published twice, usually by different datasets; one copy was kept.")
    lines.append("  * cleaned == stored: the loader writes every cleaned row exactly once (PRIMARY KEY gbif_id).")
    report = "\n".join(lines)
    (settings.summary_dir / "pipeline_verification.txt").write_text(report + "\n", encoding="utf-8")
    print(report)
    return "PASS" if all(checks.values()) else "FAIL"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-10s %(levelname)s %(message)s")
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("faststream").setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description="Run the full SIS2 pipeline")
    parser.add_argument("--skip-collect", action="store_true", help="reuse existing raw data")
    parser.add_argument("--keep-topic", action="store_true", help="do not reset the Kafka topic")
    parser.add_argument("--no-invalid-messages", action="store_true",
                        help="do not send the 3 broken demo messages")
    args = parser.parse_args()
    t0 = time.time()

    try:
        banner("STAGE 1 - API data collection (GBIF Occurrence API)")
        if args.skip_collect and settings.raw_jsonl.exists():
            print(f"skipped - reusing {settings.raw_jsonl}")
        else:
            api_client.collect()

        banner("STAGE 2 - Kafka streaming (FastStream Producer -> topic -> Consumer)")
        if not args.keep_topic:
            asyncio.run(reset_topic())
            print(f"topic '{settings.kafka_topic}' reset -> empty finite stream")
        prod = asyncio.run(producer.publish(inject_invalid=0 if args.no_invalid_messages else 3))
        print(f"published: {prod['messages_published']} (+{prod['invalid_demo_messages']} broken demo), "
              f"failed: {prod['messages_failed']}, per partition: {prod['messages_per_partition']}")
        cons = asyncio.run(consumer.consume())
        print(f"consumed: {cons['messages_consumed']}, rejected: {cons['deserialization_errors']}")
        print("topic state:", asyncio.run(describe()))

        banner("STAGE 3 - Pandera validation")
        valid, invalid, _ = validator.validate(add_test_records=True)
        print(f"valid: {len(valid)}, invalid: {len(invalid)} "
              f"(of which test records: {int(invalid['is_test_record'].astype(bool).sum())})")

        banner("STAGE 4 - Cleaning and transformation (pandas + NumPy)")
        cleaner.clean()

        banner("STAGE 5 - SQLite3 loading, executed twice (idempotency test)")
        if settings.sqlite_db_path.exists():
            settings.sqlite_db_path.unlink()  # fresh DB so run 1 vs run 2 is a clean comparison
        idem = loader.idempotency_test()
        processed = len(pd.read_csv(settings.processed_csv))
        from src.stats import update_stats
        update_stats("loading", {"processed_records": processed,
                                 "stored_records": idem["run_2"]["rows_in_table"],
                                 "match": processed == idem["run_2"]["rows_in_table"],
                                 "idempotency": {k: idem[k] for k in ("run_1", "run_2", "idempotent")}})
        loader.run_verification_queries(show=True)

        banner("STAGE 6 - Exploratory data analysis")
        analysis.analyse()

        banner("STAGE 7 - Pipeline verification")
        status = verify()
    except (ConnectionError, FileNotFoundError, api_client.GBIFRequestError, RuntimeError) as exc:
        log.error("Pipeline stopped: %s", exc)
        return 1

    print(f"\nPipeline finished in {time.time() - t0:.0f}s - verification {status}")
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
