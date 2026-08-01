from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable

import structlog
from aiokafka import TopicPartition
from aiokafka.structs import OffsetAndMetadata

from common.models import ParsedLog, SecurityAlert

log = structlog.get_logger("indexer.batcher")

FlushFn = Callable[[list[ParsedLog], list[SecurityAlert]], Awaitable[None]]

QueueItem = tuple[ParsedLog | None, list[SecurityAlert], TopicPartition, int]


class Batcher:
    """Accumulates parsed records and flushes on size OR age, whichever
    triggers first. Offsets only advance after a successful flush — this is
    what makes crash recovery lossless (G3)."""

    MAX_ATTEMPTS = 5
    MAX_BACKOFF_S = 60.0

    def __init__(self, max_records: int, max_age_ms: int, flush_fn: FlushFn):
        self.max_records = max_records
        self.max_age_s = max_age_ms / 1000.0
        self._flush_fn = flush_fn
        self._buf: list[ParsedLog] = []
        self._alerts: list[SecurityAlert] = []
        self._offsets: dict[TopicPartition, int] = {}
        self._deadline: float | None = None

    def __len__(self) -> int:
        return len(self._buf)

    def add(
        self,
        parsed: ParsedLog | None,
        alerts: list[SecurityAlert],
        tp: TopicPartition,
        offset: int,
    ) -> None:
        # parsed is None for envelope-level DLQ items: no row to write, but the
        # offset still needs to advance once the batch is durably handled.
        if parsed is not None:
            self._buf.append(parsed)
        self._alerts.extend(alerts)
        self._offsets[tp] = max(self._offsets.get(tp, -1), offset)
        if self._deadline is None:
            self._deadline = time.monotonic() + self.max_age_s

    def should_flush(self) -> bool:
        if not self._buf and not self._offsets:
            return False
        if len(self._buf) >= self.max_records:
            return True
        return self._deadline is not None and time.monotonic() >= self._deadline

    async def flush(self, consumer) -> None:
        if not self._buf and not self._offsets:
            return
        rows, alerts, offsets = self._buf, self._alerts, self._offsets
        self._buf, self._alerts, self._offsets, self._deadline = [], [], {}, None

        if rows or alerts:
            await self._flush_with_retry(rows, alerts)

        if consumer is not None and offsets:
            await consumer.commit(
                {tp: OffsetAndMetadata(off + 1, "") for tp, off in offsets.items()}
            )

    async def _flush_with_retry(self, rows: list[ParsedLog], alerts: list[SecurityAlert]) -> None:
        attempt = 0
        while True:
            try:
                await self._flush_fn(rows, alerts)
                return
            except Exception:
                attempt += 1
                log.warning("flush_failed", attempt=attempt, batch_size=len(rows))
                if attempt >= self.MAX_ATTEMPTS:
                    log.error("flush_giving_up", attempts=attempt)
                    raise
                cap = min(self.MAX_BACKOFF_S, 0.25 * (2**attempt))
                await asyncio.sleep(random.uniform(0, cap))

    async def run(self, queue: asyncio.Queue[QueueItem], consumer) -> None:
        while True:
            timeout = None if self._deadline is None else max(0.0, self._deadline - time.monotonic())
            try:
                parsed, alerts, tp, offset = await asyncio.wait_for(queue.get(), timeout)
                self.add(parsed, alerts, tp, offset)
            except TimeoutError:
                pass
            if self.should_flush():
                await self.flush(consumer)
