"""Stage 2b - Kafka Consumer (FastStream + Pydantic).

A FastStream subscriber listens to the topic. Its handler is annotated with
the Pydantic model `KafkaEnvelope`, so FastStream deserializes the JSON bytes
and validates them against the model *before* the handler runs. Messages that
are not valid JSON or do not match the contract are rejected and counted by an
ExceptionMiddleware instead of crashing the consumer.

Termination condition (finite stream):
  * stop after --max-messages valid messages, OR
  * stop when no message arrives for KAFKA_CONSUMER_IDLE_TIMEOUT_S seconds.

Offsets are committed manually (AckPolicy.MANUAL) only *after* the received
records are written to data/consumed_records.csv -> at-least-once delivery.
The committed group offset lives in Kafka, so a second run reads only new
messages. `--from-beginning` uses a fresh group id to replay the whole topic.

Run:  python -m src.consumer
      python -m src.consumer --from-beginning
"""

import argparse
import asyncio
import logging
import sys
import time
import uuid
from collections import Counter

import pandas as pd
from aiokafka.errors import KafkaConnectionError, KafkaError
from faststream import AckPolicy, Context, ExceptionMiddleware
from faststream.kafka import KafkaBroker
from faststream.kafka.annotations import KafkaMessage
from pydantic import ValidationError

from src.config import settings
from src.kafka_admin import topic_exists
from src.models import KafkaEnvelope
from src.stats import update_stats

log = logging.getLogger("consumer")


async def consume(max_messages: int | None = None, from_beginning: bool = False) -> dict:
    if not await topic_exists():
        raise RuntimeError(f"Topic '{settings.kafka_topic}' does not exist - run the producer first")

    group_id = settings.kafka_consumer_group
    if from_beginning:
        group_id = f"{group_id}-replay-{uuid.uuid4().hex[:6]}"

    rows: list[dict] = []
    counters: Counter = Counter()
    runs_seen: Counter = Counter()
    state = {"last_message_at": time.monotonic(), "last_msg": None}

    # --- error handling for messages that fail JSON / Pydantic deserialization
    errors = ExceptionMiddleware()

    @errors.add_handler(ValidationError)
    async def on_bad_message(exc: ValidationError, msg=Context("message")) -> None:
        counters["deserialization_errors"] += 1
        state["last_message_at"] = time.monotonic()
        state["last_msg"] = msg  # bad message is skipped, but its offset is still committed
        log.warning("Rejected message that violates the KafkaEnvelope contract: %s",
                    exc.errors()[0].get("msg"))

    broker = KafkaBroker(settings.kafka_bootstrap_servers, client_id="sis2-gbif-consumer",
                         middlewares=[errors], log_level=logging.DEBUG)

    @broker.subscriber(settings.kafka_topic, group_id=group_id, auto_offset_reset="earliest",
                       ack_policy=AckPolicy.MANUAL)
    async def handle(envelope: KafkaEnvelope, msg: KafkaMessage) -> None:
        """`envelope` is already a validated Pydantic object here (JSON deserialization)."""
        state["last_message_at"] = time.monotonic()
        state["last_msg"] = msg
        record = envelope.record.model_dump()
        record["kafka_partition"] = msg.raw_message.partition
        record["kafka_offset"] = msg.raw_message.offset
        record["producer_run_id"] = envelope.run_id
        rows.append(record)
        runs_seen[envelope.run_id] += 1
        counters["consumed"] += 1
        if counters["consumed"] % 200 == 0:
            log.info("... %d messages consumed", counters["consumed"])

    try:
        await broker.start()
    except (KafkaConnectionError, OSError) as exc:
        raise ConnectionError(f"Cannot connect consumer to {settings.kafka_bootstrap_servers}: {exc}") from exc
    log.info("Subscribed to '%s' as group '%s' (idle timeout %.0fs)",
             settings.kafka_topic, group_id, settings.kafka_consumer_idle_timeout_s)

    try:
        while True:  # termination condition of the finite stream
            await asyncio.sleep(0.5)
            if max_messages and counters["consumed"] >= max_messages:
                log.info("Reached max-messages=%d -> stopping", max_messages)
                break
            if time.monotonic() - state["last_message_at"] > settings.kafka_consumer_idle_timeout_s:
                log.info("No messages for %.0fs -> end of finite stream", settings.kafka_consumer_idle_timeout_s)
                break

        if rows:
            pd.DataFrame(rows).to_csv(settings.consumed_csv, index=False)
            log.info("Wrote %d records -> %s", len(rows), settings.consumed_csv)
        else:
            log.warning("No new valid messages - %s left unchanged", settings.consumed_csv)
        if state["last_msg"] is not None:
            await state["last_msg"].ack()  # commit offsets only after data is stored
    finally:
        await broker.stop()

    result = {"topic": settings.kafka_topic, "group_id": group_id,
              "messages_consumed": counters["consumed"],
              "deserialization_errors": counters["deserialization_errors"],
              "producer_runs_seen": dict(runs_seen)}
    if rows:
        update_stats("consumer", result)
    return result


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("faststream").setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description="Consume GBIF records from Kafka")
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--from-beginning", action="store_true",
                        help="use a fresh consumer group and re-read the whole topic")
    args = parser.parse_args()
    try:
        result = asyncio.run(consume(args.max_messages, args.from_beginning))
    except (ConnectionError, RuntimeError, KafkaError) as exc:
        log.error("Consumer aborted: %s", exc)
        return 1
    print("\n=== CONSUMER SUMMARY ===")
    for k, v in result.items():
        print(f"{k:>24}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
