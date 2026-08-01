from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from aiokafka import TopicPartition

from common.models import LogLevel, ParsedLog, ParseStatus
from services.indexer.batcher import Batcher


def make_parsed() -> ParsedLog:
    now = datetime.now(UTC)
    return ParsedLog(
        event_id=uuid4(), ts=now, ingested_at=now, emitted_at=now,
        service="svc", host="h", level=LogLevel.INFO, message="m",
        parse_status=ParseStatus.OK,
    )


class FakeConsumer:
    def __init__(self) -> None:
        self.committed: dict = {}

    async def commit(self, offsets) -> None:
        self.committed.update(offsets)


@pytest.mark.asyncio
async def test_flush_triggers_on_size() -> None:
    flushed = []

    async def flush_fn(rows, alerts):
        flushed.append(len(rows))

    b = Batcher(max_records=3, max_age_ms=10_000, flush_fn=flush_fn)
    tp = TopicPartition("t", 0)
    for i in range(3):
        b.add(make_parsed(), [], tp, i)
    assert b.should_flush()
    await b.flush(consumer=None)
    assert flushed == [3]
    assert len(b) == 0


@pytest.mark.asyncio
async def test_flush_triggers_on_age() -> None:
    flushed = []

    async def flush_fn(rows, alerts):
        flushed.append(len(rows))

    b = Batcher(max_records=1000, max_age_ms=20, flush_fn=flush_fn)
    tp = TopicPartition("t", 0)
    b.add(make_parsed(), [], tp, 0)
    assert not b.should_flush()
    await asyncio.sleep(0.2)  # generous margin over max_age_ms to avoid timer-resolution flakiness
    assert b.should_flush()
    await b.flush(consumer=None)
    assert flushed == [1]


@pytest.mark.asyncio
async def test_offsets_commit_only_after_successful_flush() -> None:
    async def flush_fn(rows, alerts):
        pass

    b = Batcher(max_records=2, max_age_ms=10_000, flush_fn=flush_fn)
    tp = TopicPartition("t", 0)
    b.add(make_parsed(), [], tp, 0)
    b.add(make_parsed(), [], tp, 1)
    consumer = FakeConsumer()
    await b.flush(consumer)
    assert consumer.committed[tp].offset == 2


@pytest.mark.asyncio
async def test_flush_retries_then_raises_and_does_not_commit() -> None:
    calls = {"n": 0}

    async def flaky_flush(rows, alerts):
        calls["n"] += 1
        raise RuntimeError("db down")

    b = Batcher(max_records=1, max_age_ms=10_000, flush_fn=flaky_flush)
    b.MAX_BACKOFF_S = 0.01
    tp = TopicPartition("t", 0)
    b.add(make_parsed(), [], tp, 0)
    consumer = FakeConsumer()
    with pytest.raises(RuntimeError):
        await b.flush(consumer)
    assert calls["n"] == Batcher.MAX_ATTEMPTS
    assert consumer.committed == {}  # never committed: offsets must not advance


@pytest.mark.asyncio
async def test_dlq_only_item_advances_offset_without_a_row() -> None:
    flushed = []

    async def flush_fn(rows, alerts):
        flushed.append(len(rows))

    b = Batcher(max_records=10, max_age_ms=10_000, flush_fn=flush_fn)
    tp = TopicPartition("t", 0)
    b.add(None, [], tp, 5)  # envelope-decode failure: no row, offset must still commit
    consumer = FakeConsumer()
    await b.flush(consumer)
    assert flushed == []  # no DB write attempted for an empty batch
    assert consumer.committed[tp].offset == 6
