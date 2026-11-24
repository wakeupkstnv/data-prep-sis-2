"""Stage 5 - store processed records in SQLite3 (idempotent).

Input : data/processed_records.csv (output of cleaner.py)
Output: data/output.db  -> tables `occurrences` and `load_runs` (schema in sql/schema.sql)

Idempotency:
  * gbif_id is the PRIMARY KEY, and a UNIQUE index forbids the same
    observation (name + coordinates + date) under two ids.
  * Every row carries a SHA-256 `row_hash` of its business columns.
  * Rows are written with an UPSERT:
        INSERT ... ON CONFLICT(gbif_id) DO UPDATE SET ...
        WHERE occurrences.row_hash <> excluded.row_hash
    -> new ids are inserted, changed rows are updated, identical rows are
       left completely untouched (even their timestamps do not change).
  * All writes of one run happen in a single transaction (all-or-nothing).

Run:  python -m src.loader            (load once + run verification queries)
      python -m src.loader --twice    (load twice to demonstrate idempotency)
"""

import argparse
import hashlib
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.config import PROJECT_ROOT, settings
from src.stats import update_stats

log = logging.getLogger("loader")

SCHEMA_SQL = PROJECT_ROOT / "sql" / "schema.sql"
VERIFY_SQL = PROJECT_ROOT / "sql" / "verification_queries.sql"

BOOL_COLUMNS = ["identified_to_species", "is_threatened", "uncertainty_imputed", "count_imputed",
                "is_high_quality"]
META_COLUMNS = ["row_hash", "first_loaded_at", "last_updated_at"]


def connect(db_path: Path = settings.sqlite_db_path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL.read_text(encoding="utf-8"))


def prepare_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"state_province": "state_province_raw"}).copy()
    for col in BOOL_COLUMNS:
        df[col] = df[col].astype(str).str.lower().map({"true": 1, "false": 0, "1": 1, "0": 0}).astype(int)
    df = df.astype(object).where(df.notna(), None)   # NaN -> NULL
    business_cols = [c for c in df.columns if c not in META_COLUMNS]
    # deterministic hash of the record content: same data -> same hash
    df["row_hash"] = [hashlib.sha256("|".join(map(str, row)).encode()).hexdigest()
                      for row in df[business_cols].itertuples(index=False, name=None)]
    return df


def load(csv_path: Path = settings.processed_csv) -> dict:
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} not found - run the cleaner first")
    df = prepare_frame(pd.read_csv(csv_path))
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    df["first_loaded_at"] = now
    df["last_updated_at"] = now

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in ("gbif_id", "first_loaded_at")]
    upsert_sql = (
        f"INSERT INTO occurrences ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))}) "
        f"ON CONFLICT(gbif_id) DO UPDATE SET "
        + ", ".join(f"{c} = excluded.{c}" for c in update_cols)
        + " WHERE occurrences.row_hash <> excluded.row_hash"
    )

    conn = connect()
    try:
        create_schema(conn)
        existing = dict(conn.execute("SELECT gbif_id, row_hash FROM occurrences").fetchall())
        ids, hashes = df["gbif_id"].astype(int), df["row_hash"]
        inserted = int((~ids.isin(existing.keys())).sum())
        updated = int(sum(1 for i, h in zip(ids, hashes) if i in existing and existing[i] != h))
        unchanged = len(df) - inserted - updated

        with conn:  # one transaction: commit on success, rollback on any error
            conn.executemany(upsert_sql, df.itertuples(index=False, name=None))
            total = conn.execute("SELECT COUNT(*) FROM occurrences").fetchone()[0]
            conn.execute("INSERT INTO load_runs (started_at, source_file, rows_in_file, inserted, updated, "
                         "unchanged, rows_in_table) VALUES (?, ?, ?, ?, ?, ?, ?)",
                         (now, csv_path.name, len(df), inserted, updated, unchanged, total))
    except sqlite3.IntegrityError as exc:
        log.error("Integrity error - transaction rolled back: %s", exc)
        raise
    finally:
        conn.close()

    result = {"rows_in_file": len(df), "inserted": inserted, "updated": updated,
              "unchanged": unchanged, "rows_in_table": total}
    log.info("Load result: %s", result)
    return result


def run_verification_queries(show: bool = True) -> dict[str, pd.DataFrame]:
    """Execute every query from sql/verification_queries.sql and return the results."""
    blocks = VERIFY_SQL.read_text(encoding="utf-8").split("-- name: ")[1:]
    results = {}
    with connect() as conn:
        for block in blocks:
            title, sql = block.split("\n", 1)
            results[title.strip()] = pd.read_sql_query(sql, conn)
    if show:
        for title, frame in results.items():
            print(f"\n-- {title}")
            print(frame.to_string(index=False) if len(frame) else "(0 rows)")
    return results


def table_fingerprint() -> str:
    """Hash of the whole table content - identical before/after an idempotent re-run."""
    with connect() as conn:
        rows = conn.execute("SELECT gbif_id, row_hash, last_updated_at FROM occurrences ORDER BY gbif_id").fetchall()
    return hashlib.sha256(repr(rows).encode()).hexdigest()[:16]


def idempotency_test() -> dict:
    """Execute the loading stage twice with the same file and compare."""
    first = load()
    fp1 = table_fingerprint()
    second = load()
    fp2 = table_fingerprint()
    passed = (second["inserted"] == 0 and second["updated"] == 0
              and first["rows_in_table"] == second["rows_in_table"] and fp1 == fp2)
    result = {"run_1": first, "run_2": second, "fingerprint_run_1": fp1, "fingerprint_run_2": fp2,
              "idempotent": passed}
    print("\n=== IDEMPOTENCY TEST ===")
    print(f"run 1: {first}")
    print(f"run 2: {second}")
    print(f"table fingerprint run 1 = {fp1}, run 2 = {fp2}")
    print(f"RESULT: {'PASSED - no duplicates, no changes' if passed else 'FAILED'}")
    return result


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Load processed records into SQLite")
    parser.add_argument("--twice", action="store_true", help="run the load twice (idempotency test)")
    parser.add_argument("--no-verify", action="store_true")
    args = parser.parse_args()

    processed = len(pd.read_csv(settings.processed_csv))
    if args.twice:
        res = idempotency_test()
        last = res["run_2"]
    else:
        res = load()
        last = res
    stored = last["rows_in_table"]

    stats = {"processed_records": processed, "stored_records": stored,
             "match": processed == stored, "db_path": str(settings.sqlite_db_path.relative_to(PROJECT_ROOT))}
    if args.twice:
        stats["idempotency"] = {k: res[k] for k in ("run_1", "run_2", "idempotent")}
    update_stats("loading", stats)

    print(f"\nprocessed records: {processed} | stored in SQLite: {stored} | "
          f"{'MATCH' if processed == stored else 'MISMATCH'}")
    if not args.no_verify:
        run_verification_queries()


if __name__ == "__main__":
    main()
