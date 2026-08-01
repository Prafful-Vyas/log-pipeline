from __future__ import annotations

import time

import pytest

from services.producer.rate_limiter import TokenBucket


@pytest.mark.asyncio
async def test_token_bucket_holds_approximate_rate() -> None:
    rate = 200.0
    burst = 50.0
    bucket = TokenBucket(rate, burst=burst)
    n = 100
    start = time.monotonic()
    for _ in range(n):
        await bucket.acquire(1)
    elapsed = time.monotonic() - start
    # The first `burst` acquisitions drain the initial capacity for free;
    # only the remainder is throttled at `rate`.
    expected = max(0, n - burst) / rate
    assert elapsed >= expected * 0.7
    assert elapsed <= expected * 2.0 + 0.05


@pytest.mark.asyncio
async def test_token_bucket_allows_immediate_burst_up_to_capacity() -> None:
    bucket = TokenBucket(rate=100, burst=50)
    start = time.monotonic()
    await bucket.acquire(50)
    elapsed = time.monotonic() - start
    assert elapsed < 0.05


@pytest.mark.asyncio
async def test_token_bucket_blocks_until_refill() -> None:
    # Note: a single acquire() can never exceed capacity (tokens are capped
    # at `capacity` on refill), so this drains the bucket first and then
    # requests an amount within capacity that requires a partial refill.
    bucket = TokenBucket(rate=1000, burst=10)
    await bucket.acquire(10)
    start = time.monotonic()
    await bucket.acquire(5)
    elapsed = time.monotonic() - start
    assert elapsed >= (5 / 1000) * 0.5
