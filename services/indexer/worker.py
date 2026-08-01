from __future__ import annotations

import asyncio
import signal
import time
from datetime import UTC, datetime
from ipaddress import AddressValueError, IPv4Address

import asyncpg
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.abc import ConsumerRebalanceListener

try:
    # Cooperative-sticky avoids stop-the-world rebalances (aiokafka >= 0.12).
    from aiokafka.coordinator.assignors.sticky.sticky_assignor import (
        CooperativeStickyAssignor as _Assignor,
    )
except ImportError:  # older aiokafka: fall back to plain sticky assignment
    from aiokafka.coordinator.assignors.sticky.sticky_assignor import (
        StickyPartitionAssignor as _Assignor,
    )

from common.config import settings
from common.models import LogEvent, ParsedLog, SecurityAlert, normalize_level
from common.serde import decode_log_event, encode_security_alert
from services.indexer.batcher import Batcher, QueueItem
from services.indexer.classifier import Classifier
from services.indexer.enrich import redact_pii
from services.indexer.parser import parse
from services.indexer.sink_pg import PgSink

log = structlog.get_logger("indexer.worker")


def _safe_ip(v: str | None) -> IPv4Address | None:
    if not v:
        return None
    try:
        return IPv4Address(v)
    except (AddressValueError, ValueError):
        return None


async def write_dead_letter(pool: asyncpg.Pool, topic: str, partition: int, offset: int,
                             payload: bytes, error: str) -> None:
    try:
        async with pool.acquire() as con:
            await con.execute(
                "INSERT INTO dead_letters (topic, partition, kafka_offset, payload, error) "
                "VALUES ($1,$2,$3,$4,$5) ON CONFLICT (topic, partition, kafka_offset) DO NOTHING",
                topic, partition, offset, payload, error[:2000],
            )
    except Exception:
        log.exception("dead_letter_write_failed", topic=topic, partition=partition, offset=offset)


async def _build_parsed(
    envelope: LogEvent,
    classifier: Classifier,
    security_producer: AIOKafkaProducer,
) -> tuple[ParsedLog, list[SecurityAlert]]:
    ingested_at = datetime.now(UTC)
    fields, status = parse(envelope.raw, ingested_at)

    parsed = ParsedLog(
        event_id=envelope.event_id,
        ts=fields["ts"],
        ingested_at=ingested_at,
        emitted_at=datetime.fromtimestamp(envelope.emitted_at, tz=UTC),
        service=envelope.service,
        host=envelope.host,
        level=normalize_level(fields.get("level")),
        message=redact_pii(fields.get("message") or ""),
        status_code=fields.get("status_code"),
        latency_ms=fields.get("latency_ms"),
        trace_id=fields.get("trace_id"),
        user_id=fields.get("user_id"),
        client_ip=_safe_ip(fields.get("client_ip")),
        attrs=fields.get("attrs") or {},
        parse_status=status,
    )

    hits = classifier.classify(parsed)
    alerts: list[SecurityAlert] = []
    if hits:
        parsed = parsed.model_copy(
            update={"is_security": True, "security_rule": hits[0].rule_id}
        )
        for hit in hits:
            alert = SecurityAlert(
                event_id=parsed.event_id, ts=parsed.ts, rule_id=hit.rule_id,
                severity=hit.severity, service=parsed.service,
                client_ip=parsed.client_ip, user_id=parsed.user_id, detail=hit.detail,
            )
            alerts.append(alert)
            # Produced to logs.security BEFORE the offset commit (which only
            # happens later, after the batch containing this record flushes).
            await security_producer.send_and_wait(
                settings.topic_security,
                value=encode_security_alert(alert),
                key=parsed.service.encode(),
            )

    return parsed, alerts


async def fetch_loop(
    worker_id: int,
    consumer: AIOKafkaConsumer,
    queue: asyncio.Queue[QueueItem],
    classifier: Classifier,
    dlq_producer: AIOKafkaProducer,
    security_producer: AIOKafkaProducer,
    pg_pool: asyncpg.Pool,
    counters: dict,
) -> None:
    paused = False
    full_since: float | None = None

    async for msg in consumer:
        tp = TopicPartition(msg.topic, msg.partition)
        try:
            envelope = decode_log_event(msg.value)
        except Exception as e:  # noqa: BLE001 - poison envelope, route to DLQ
            log.warning("envelope_decode_failed", error=str(e), offset=msg.offset)
            await write_dead_letter(pg_pool, msg.topic, msg.partition, msg.offset, msg.value, str(e))
            dlq_producer.send(settings.topic_dlq, value=msg.value)
            await queue.put((None, [], tp, msg.offset))
        else:
            parsed, alerts = await _build_parsed(envelope, classifier, security_producer)
            await queue.put((parsed, alerts, tp, msg.offset))
            counters["processed"] += 1

        if queue.qsize() >= queue.maxsize:
            if full_since is None:
                full_since = time.monotonic()
            elif not paused and time.monotonic() - full_since > 0.5:
                consumer.pause(*consumer.assignment())
                paused = True
                log.warning("backpressure_pause", worker=worker_id)
        else:
            full_since = None
            if paused and queue.qsize() < queue.maxsize * 0.5:
                consumer.resume(*consumer.assignment())
                paused = False
                log.info("backpressure_resume", worker=worker_id)


class RebalanceListener(ConsumerRebalanceListener):
    def __init__(self, batcher: Batcher, consumer: AIOKafkaConsumer):
        self._batcher = batcher
        self._consumer = consumer

    async def on_partitions_revoked(self, revoked) -> None:
        log.info("partitions_revoked", partitions=[str(tp) for tp in revoked])
        try:
            await self._batcher.flush(self._consumer)
        except Exception:
            log.exception("flush_on_revoke_failed")

    async def on_partitions_assigned(self, assigned) -> None:
        log.info("partitions_assigned", partitions=[str(tp) for tp in assigned])


async def stats_loop(worker_id: int, queue: asyncio.Queue, counters: dict) -> None:
    last = 0
    while True:
        await asyncio.sleep(10)
        total = counters["processed"]
        log.info(
            "worker_stats", worker=worker_id, processed_total=total,
            eps_last_10s=(total - last) / 10.0, queue_depth=queue.qsize(),
        )
        last = total


async def run_worker(worker_id: int) -> None:
    consumer = AIOKafkaConsumer(
        settings.topic_raw,
        bootstrap_servers=settings.kafka_brokers,
        group_id="log-indexer",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=1000,
        fetch_max_bytes=8 * 1024 * 1024,
        fetch_max_wait_ms=100,
        session_timeout_ms=45_000,
        heartbeat_interval_ms=3_000,
        max_poll_interval_ms=300_000,
        partition_assignment_strategy=[_Assignor],
    )
    dlq_producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers, acks=1)
    security_producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers, acks=1)
    pg_pool = await asyncpg.create_pool(
        settings.pg_dsn, min_size=2, max_size=settings.pg_pool_max, command_timeout=30
    )

    sink = PgSink(pg_pool)
    flush_fn = sink.write_naive if settings.write_strategy == "naive" else sink.write_batch
    batcher = Batcher(settings.batch_size, settings.batch_max_age_ms, flush_fn)
    queue: asyncio.Queue[QueueItem] = asyncio.Queue(maxsize=settings.queue_max)
    classifier = Classifier()
    counters = {"processed": 0}

    await consumer.start()
    await dlq_producer.start()
    await security_producer.start()
    consumer.subscribe([settings.topic_raw], listener=RebalanceListener(batcher, consumer))

    log.info("worker_started", worker=worker_id, write_strategy=settings.write_strategy)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass  # Windows: fall back to KeyboardInterrupt handling in __main__

    fetch_task = asyncio.create_task(
        fetch_loop(worker_id, consumer, queue, classifier, dlq_producer, security_producer, pg_pool, counters)
    )
    batch_task = asyncio.create_task(batcher.run(queue, consumer))
    stats_task = asyncio.create_task(stats_loop(worker_id, queue, counters))
    stop_task = asyncio.create_task(stop_event.wait())

    try:
        done, pending = await asyncio.wait(
            {fetch_task, batch_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
        stats_task.cancel()
        await asyncio.gather(fetch_task, batch_task, stop_task, stats_task, return_exceptions=True)

        # A crash in the pipeline itself (not a clean shutdown) should
        # propagate so the supervisor restarts this worker. Offsets are safe
        # either way: they only advance after a successful flush.
        for t in (fetch_task, batch_task):
            if t in done and not t.cancelled() and t.exception() is not None:
                raise t.exception()
    finally:
        try:
            await batcher.flush(consumer)
        except Exception:
            log.exception("final_flush_failed", worker=worker_id)
        await consumer.stop()
        await dlq_producer.stop()
        await security_producer.stop()
        await pg_pool.close()
        log.info("worker_stopped", worker=worker_id, processed_total=counters["processed"])
