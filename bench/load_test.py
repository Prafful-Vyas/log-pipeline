"""End-to-end load test harness (design doc SS5.1).

Requires the full compose stack up (`make up`) -- this produces real events
through Redpanda into the real indexer -> Postgres path, then independently
verifies against Postgres itself (not the producer's own counters, per SS5.1's
"independent verification" requirement): ingested row-count delta and
emitted_at -> ingested_at latency percentiles, for each rate step.

    python -m bench.load_test --rates 1000,2000,5000 --step-duration 30

The full design-doc ramp is 1k/2k/5k/8k/12k eps at 300s/step; reproduce it
exactly with:

    python -m bench.load_test --rates 1000,2000,5000,8000,12000 --step-duration 300

Consumer lag isn't polled here -- watch it live via Grafana (SS4.1's
"pipeline_health" dashboard) or Redpanda Console during the run instead.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime

import asyncpg
from aiokafka import AIOKafkaProducer

from common.config import settings
from services.producer.__main__ import emit_loop
from services.producer.generator import PROFILES, default_states
from services.producer.rate_limiter import TokenBucket

SETTLE_S = 2.0  # generous margin over the 250ms max batch age


async def _ingested_and_latency(pool: asyncpg.Pool, since: datetime) -> tuple[int, dict[str, float | None]]:
    async with pool.acquire() as con:
        count = await con.fetchval("SELECT count(*) FROM logs WHERE ingested_at >= $1", since)
        rows = await con.fetch(
            "SELECT extract(epoch from (ingested_at - emitted_at)) AS lat "
            "FROM logs WHERE ingested_at >= $1 AND emitted_at IS NOT NULL",
            since,
        )
    lats = sorted(r["lat"] for r in rows if r["lat"] is not None)

    def pct(p: float) -> float | None:
        if not lats:
            return None
        return lats[min(len(lats) - 1, int(len(lats) * p))]

    return count, {"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99)}


async def run_step(
    producer: AIOKafkaProducer, pool: asyncpg.Pool, rate: float, duration: int, topic: str
) -> tuple[int, int, dict[str, float | None]]:
    states = default_states(PROFILES)
    stats = {"sent": 0}
    bucket = TokenBucket(rate)
    step_start = datetime.now(UTC)

    await emit_loop(producer, topic, bucket, PROFILES, states, stats, duration)
    await asyncio.sleep(SETTLE_S)

    ingested, latency = await _ingested_and_latency(pool, step_start)
    return stats["sent"], ingested, latency


def _fmt_ms(v: float | None) -> str:
    return f"{v * 1000:.1f}" if v is not None else "n/a"


async def main() -> None:
    p = argparse.ArgumentParser(description="End-to-end load test (design doc SS5.1)")
    p.add_argument("--rates", default="1000,2000,5000", help="comma-separated eps steps")
    p.add_argument("--step-duration", type=int, default=30, help="design doc uses 300s/step")
    p.add_argument("--topic", default=settings.topic_raw)
    p.add_argument("--dsn", default=settings.pg_dsn)
    args = p.parse_args()

    rates = [float(x) for x in args.rates.split(",") if x.strip()]

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_brokers,
        acks=1,
        compression_type="lz4",
        linger_ms=20,
        max_batch_size=262_144,
        max_request_size=2_097_152,
        request_timeout_ms=15_000,
        enable_idempotence=False,
    )
    await producer.start()
    pool = await asyncpg.create_pool(args.dsn, min_size=1, max_size=2)

    print(f"{'rate_eps':>10}{'offered':>10}{'ingested':>10}{'p50_ms':>10}{'p95_ms':>10}{'p99_ms':>10}")
    try:
        for rate in rates:
            offered, ingested, lat = await run_step(producer, pool, rate, args.step_duration, args.topic)
            print(
                f"{rate:>10.0f}{offered:>10}{ingested:>10}"
                f"{_fmt_ms(lat['p50']):>10}{_fmt_ms(lat['p95']):>10}{_fmt_ms(lat['p99']):>10}"
            )
    finally:
        await producer.stop()
        await pool.close()

    print()
    print("Paste this table into bench/RESULTS.md under Claim 1.")


if __name__ == "__main__":
    asyncio.run(main())
