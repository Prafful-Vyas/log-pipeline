from __future__ import annotations

import asyncio
import time


class TokenBucket:
    """Monotonic-clock token bucket. asyncio.sleep(1/rate) is too coarse-grained
    (sleep granularity ~1-15ms) to hold a precise rate above a few hundred
    events/sec, so tokens are refilled continuously from elapsed wall time.
    """

    def __init__(self, rate: float, burst: float | None = None):
        self._rate = rate
        self._capacity = burst or max(rate * 0.1, 100)
        self._tokens = self._capacity
        self._last = time.monotonic()

    async def acquire(self, n: int = 1) -> None:
        while True:
            now = time.monotonic()
            self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._rate)
            self._last = now
            if self._tokens >= n:
                self._tokens -= n
                return
            await asyncio.sleep((n - self._tokens) / self._rate)
