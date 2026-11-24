"""Stage 2a - Kafka Producer (FastStream + Pydantic).

Reads the preserved raw GBIF records (data/raw/gbif_raw_records.jsonl),
converts each one into a Pydantic `KafkaEnvelope` and publishes it to the
Kafka topic through a FastStream `KafkaBroker`.

Serialization: FastStream turns the Pydantic model into JSON bytes
(`model_dump_json`) and sets `content-type: application/json` header.

Message:
    key   = gbif_id (bytes)  -> the same record always lands in the same partition
    value = {"run_id": "...", "seq": 17, "source": "gbif-occurrence-api",
             "produced_at": "...", "record": {...OccurrenceRecord...}}

Run:  python -m src.producer            (create topic if missing, publish all)
      python -m src.producer --limit 50 (publish only the first 50 records)
      python -m src.producer --inject-invalid 3  (+3 broken messages, consumer must reject them)
"""

import argparse
import asyncio
import json
import logging
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone

from aiokafka.errors import KafkaConnectionError, KafkaError
from faststream.kafka import KafkaBroker
from pydantic import ValidationError

from src.config import settings
from src.kafka_admin import ensure_topic
from src.models import KafkaEnvelope, OccurrenceRecord
from src.stats import update_stats

log = logging.getLogger("producer")

PUBLISH_CHUNK = 100  # messages sent concurrently, then awaited together


def load_envelopes(run_id: str, limit: int | None) -> tuple[list[KafkaEnvelope], int]:
    """Raw JSONL -> list of validated Pydantic envelopes (+ number of malformed lines)."""
    if not settings.raw_jsonl.exists():
        raise FileNotFoundError(f"{settings.raw_jsonl} not found - run `python -m src.api_client` first")
    envelopes, malformed = [], 0
    with settings.raw_jsonl.open(encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            try:
                record = OccurrenceRecord.model_validate(json.loads(line))
            except (json.JSONDecodeError, ValidationError) as exc:
                malformed += 1
                log.warning("Skipping malformed raw line: %s", exc)
                continue
            envelopes.append(KafkaEnvelope(run_id=run_id, seq=len(envelopes) + 1,
                                           produced_at=datetime.now(timezone.utc), record=record))
            if limit and len(envelopes) >= limit:
                break
    return envelopes, malformed


# Deliberately broken messages for demonstrating consumer-side error handling
INVALID_DEMO_MESSAGES = [
    b"this is not JSON",
    b'{"run_id": "demo", "seq": 0, "record": {"scientific_name": "no gbif_id"}}',
    b'{"run_id": "demo", "seq": 1, "produced_at": "yesterday", "record": {"gbif_id": "abc"}}',
]


async def publish(limit: int | None = None, inject_invalid: int = 0) -> dict:
    if await ensure_topic():
        log.info("Created topic '%s' (%d partitions, RF=%d)", settings.kafka_topic,
                 settings.kafka_topic_partitions, settings.kafka_topic_replication)
    else:
        log.info("Topic '%s' already exists", settings.kafka_topic)
    run_id = uuid.uuid4().hex[:12]
    envelopes, malformed = load_envelopes(run_id, limit)

    broker = KafkaBroker(
        settings.kafka_bootstrap_servers,
        client_id="sis2-gbif-producer",
        acks="all",                # wait until the broker has persisted the message
        enable_idempotence=True,   # internal retries cannot create duplicates
        linger_ms=20,
    )
    publisher = broker.publisher(settings.kafka_topic, description="Flattened GBIF occurrences for KZ")

    delivered, failed = 0, 0
    per_partition: Counter = Counter()
    try:
        await broker.connect()
    except (KafkaConnectionError, OSError) as exc:
        raise ConnectionError(f"Cannot connect producer to {settings.kafka_bootstrap_servers}: {exc}") from exc

    try:
        for start in range(0, len(envelopes), PUBLISH_CHUNK):
            chunk = envelopes[start:start + PUBLISH_CHUNK]
            # publish() waits for the broker acknowledgement and returns RecordMetadata
            results = await asyncio.gather(
                *(publisher.publish(env, key=str(env.record.gbif_id).encode()) for env in chunk),
                return_exceptions=True)
            for env, res in zip(chunk, results):
                if isinstance(res, Exception):
                    failed += 1
                    log.error("Delivery failed for gbif_id=%s: %r", env.record.gbif_id, res)
                else:
                    delivered += 1
                    per_partition[res.partition] += 1
            log.info("... %d/%d messages acknowledged", delivered, len(envelopes))
        for raw in INVALID_DEMO_MESSAGES[:inject_invalid]:  # raw bytes bypass Pydantic on purpose
            await publisher.publish(raw, key=b"invalid-demo")
            log.warning("Injected invalid demo message: %r", raw[:60])
    except KafkaError as exc:
        log.error("Producer error: %s", exc)
        failed += len(envelopes) - delivered - failed
    finally:
        await broker.stop()

    result = {"run_id": run_id, "topic": settings.kafka_topic,
              "records_loaded": len(envelopes), "malformed_raw_lines": malformed,
              "messages_published": delivered, "invalid_demo_messages": min(inject_invalid, len(INVALID_DEMO_MESSAGES)), "messages_failed": failed,
              "messages_per_partition": {str(k): v for k, v in sorted(per_partition.items())}}
    update_stats("producer", result)
    return result


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("faststream").setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description="Publish GBIF records to Kafka")
    parser.add_argument("--limit", type=int, default=None, help="publish only the first N records")
    parser.add_argument("--inject-invalid", type=int, default=0, choices=range(0, 4),
                        help="also send N (0-3) deliberately broken messages to test the consumer")
    args = parser.parse_args()
    try:
        result = asyncio.run(publish(args.limit, args.inject_invalid))
    except (ConnectionError, FileNotFoundError, KafkaError) as exc:
        log.error("Producer aborted: %s", exc)
        return 1
    print("\n=== PRODUCER SUMMARY ===")
    for k, v in result.items():
        print(f"{k:>24}: {v}")
    return 0 if result["messages_failed"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
