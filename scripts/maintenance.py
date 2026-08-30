"""Periodic Postgres maintenance: rollup refresh + partition upkeep.

Runs as a standalone compose sidecar. Vanilla postgres:16 has no pg_cron, so
this replaces it with a simple asyncio loop: every 30s it recomputes the
logs_1m_agg rollup for the last 3 minutes (design doc SS4.2), and every hour it
calls the partition-maintenance procedures from sql/001_schema.sql.
"""

from __future__ import annotations

import asyncio

import asyncpg
import structlog

from common.config import settings
from common.logging import configure_logging

log: structlog.stdlib.BoundLogger = configure_logging("maintenance")

ROLLUP_INTERVAL_S = 30
PARTITION_INTERVAL_S = 3600

_REFRESH_ROLLUP_SQL = """
    INSERT INTO logs_1m_agg (bucket, service, level, events, p50_lat, p95_lat, p99_lat)
    SELECT date_trunc('minute', ts), service, level, count(*),
           percentile_cont(0.5)  WITHIN GROUP (ORDER BY latency_ms),
           percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms),
           percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms)
    FROM logs
    WHERE ts >= date_trunc('minute', now()) - interval '3 minutes'
    GROUP BY 1,2,3
    ON CONFLICT (bucket, service, level) DO UPDATE
    SET events = EXCLUDED.events, p50_lat = EXCLUDED.p50_lat,
        p95_lat = EXCLUDED.p95_lat, p99_lat = EXCLUDED.p99_lat
"""


async def _connect_with_retry() -> asyncpg.Pool:
    for attempt in range(1, 31):
        try:
            return await asyncpg.create_pool(settings.pg_dsn, min_size=1, max_size=2)
        except Exception as e:  # noqa: BLE001
            log.info("postgres_not_ready", attempt=attempt, error=str(e))
            await asyncio.sleep(2)
    raise SystemExit("postgres never became reachable")


async def rollup_loop(pool: asyncpg.Pool) -> None:
    while True:
        try:
            async with pool.acquire() as con:
                await con.execute(_REFRESH_ROLLUP_SQL)
            log.info("rollup_refreshed")
        except Exception:
            log.exception("rollup_refresh_failed")
        await asyncio.sleep(ROLLUP_INTERVAL_S)


async def partition_loop(pool: asyncpg.Pool) -> None:
    while True:
        try:
            async with pool.acquire() as con:
                await con.execute("CALL ensure_log_partitions()")
                await con.execute("CALL drop_old_log_partitions()")
            log.info("partition_maintenance_ran")
        except Exception:
            log.exception("partition_maintenance_failed")
        await asyncio.sleep(PARTITION_INTERVAL_S)


async def main() -> None:
    pool = await _connect_with_retry()
    log.info("maintenance_started")
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(rollup_loop(pool))
            tg.create_task(partition_loop(pool))
    finally:
        await pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
