"""Kafka topic administration shared by producer, consumer and main.py.

Run:  python -m src.kafka_admin describe   (topic, partitions, end offsets, consumer lag)
      python -m src.kafka_admin create
      python -m src.kafka_admin reset      (delete + recreate the topic -> empty stream)
"""

import asyncio
import sys

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaConnectionError, TopicAlreadyExistsError, UnknownTopicOrPartitionError

from src.config import settings


async def _admin() -> AIOKafkaAdminClient:
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers, request_timeout_ms=10000)
    try:
        await admin.start()
    except KafkaConnectionError as exc:
        raise ConnectionError(f"Cannot reach Kafka broker at {settings.kafka_bootstrap_servers}: {exc}") from exc
    return admin


async def topic_exists() -> bool:
    admin = await _admin()
    try:
        return settings.kafka_topic in await admin.list_topics()
    finally:
        await admin.close()


async def ensure_topic() -> bool:
    """Create the topic if missing (broker has auto-create disabled). Returns True if created."""
    admin = await _admin()
    try:
        if settings.kafka_topic in await admin.list_topics():
            return False
        topic = NewTopic(settings.kafka_topic, num_partitions=settings.kafka_topic_partitions,
                         replication_factor=settings.kafka_topic_replication,
                         topic_configs={"retention.ms": str(7 * 24 * 3600 * 1000)})
        try:
            await admin.create_topics([topic])
        except TopicAlreadyExistsError:
            return False
        return True
    finally:
        await admin.close()


async def reset_topic() -> None:
    """Delete and recreate the topic. Committed group offsets of a deleted topic are
    dropped by Kafka, so the consumer group starts from the beginning again."""
    admin = await _admin()
    try:
        try:
            await admin.delete_topics([settings.kafka_topic])
        except UnknownTopicOrPartitionError:
            pass
    finally:
        await admin.close()
    for _ in range(30):                     # deletion is asynchronous on the broker
        if not await topic_exists():
            break
        await asyncio.sleep(1)
    for _ in range(10):
        try:
            await ensure_topic()
            return
        except TopicAlreadyExistsError:
            await asyncio.sleep(1)


async def describe() -> dict:
    """Partitions, end offsets (= messages stored) and committed offset of our group."""
    admin = await _admin()
    try:
        meta = await admin.describe_topics([settings.kafka_topic])
    finally:
        await admin.close()
    parts = {p["partition"] for p in meta[0].get("partitions", [])} if meta else set()
    consumer = AIOKafkaConsumer(bootstrap_servers=settings.kafka_bootstrap_servers,
                                group_id=settings.kafka_consumer_group, enable_auto_commit=False)
    await consumer.start()
    try:
        tps = [TopicPartition(settings.kafka_topic, p) for p in sorted(parts)]
        ends = await consumer.end_offsets(tps) if tps else {}
        info = {}
        for tp in tps:
            committed = await consumer.committed(tp)
            info[tp.partition] = {"end_offset": ends[tp], "committed": committed,
                                  "lag": ends[tp] - (committed or 0)}
        return {"topic": settings.kafka_topic, "group": settings.kafka_consumer_group,
                "partitions": info, "messages_in_topic": sum(v["end_offset"] for v in info.values())}
    finally:
        await consumer.stop()


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "describe"
    if action == "create":
        print("created" if asyncio.run(ensure_topic()) else "already exists")
    elif action == "reset":
        asyncio.run(reset_topic())
        print(f"topic '{settings.kafka_topic}' recreated (empty)")
    else:
        import json
        print(json.dumps(asyncio.run(describe()), indent=2))
