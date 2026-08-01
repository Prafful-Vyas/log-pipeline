"""One-shot topic creation, run as a compose init container.

Idempotent: safe to run every startup.
"""

from __future__ import annotations

import asyncio

import structlog
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from common.config import settings
from common.logging import configure_logging

log: structlog.stdlib.BoundLogger = configure_logging("topic-init")

HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS

TOPICS = (
    NewTopic(
        name=settings.topic_raw,
        num_partitions=settings.topic_raw_partitions,
        replication_factor=1,
        topic_configs={"retention.ms": str(6 * HOUR_MS)},
    ),
    NewTopic(
        name=settings.topic_security,
        num_partitions=settings.topic_security_partitions,
        replication_factor=1,
        topic_configs={"retention.ms": str(7 * DAY_MS)},
    ),
    NewTopic(
        name=settings.topic_dlq,
        num_partitions=1,
        replication_factor=1,
        topic_configs={"retention.ms": str(7 * DAY_MS)},
    ),
)


async def main() -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_brokers)

    for attempt in range(1, 31):
        try:
            await admin.start()
            break
        except Exception as e:  # noqa: BLE001
            log.info("broker_not_ready", attempt=attempt, error=str(e))
            await asyncio.sleep(2)
    else:
        raise SystemExit("broker never became reachable")

    try:
        try:
            await admin.create_topics(list(TOPICS))
            log.info("topics_created", topics=[t.name for t in TOPICS])
        except TopicAlreadyExistsError:
            log.info("topics_already_exist")
        except Exception:  # noqa: BLE001 - broad by design, see fallback comment below
            # aiokafka raises a single error for the whole batch on partial
            # overlap; fall back to creating one at a time so a topic that
            # already exists doesn't block the ones that don't.
            for topic in TOPICS:
                try:
                    await admin.create_topics([topic])
                    log.info("topic_created", topic=topic.name)
                except TopicAlreadyExistsError:
                    log.info("topic_already_exists", topic=topic.name)
    finally:
        await admin.close()


if __name__ == "__main__":
    asyncio.run(main())
