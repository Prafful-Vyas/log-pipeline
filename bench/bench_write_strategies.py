"""Write-strategy benchmark (design doc SS5.2) -- Experiments A and C.

Requires a live Postgres (the compose stack's `postgres` service, or any
Postgres 16 reachable at --dsn). Never touches the real `logs` table: each
strategy gets its own disposable `logs_bench_<strategy>` table created via
`LIKE logs INCLUDING ALL` and dropped when done.

    python -m bench.bench_write_strategies --rows 500000

"Write overhead" is defined here as wall-clock time + WAL bytes generated per
100k rows (via pg_wal_lsn_diff), not pg_stat_statements execution time -- that
extension isn't enabled in this repo's postgres:16 compose config, and the
design doc explicitly allows picking one definition and stating it. The WAL
delta is measured against the whole database's WAL stream, so run this against
an otherwise-idle stack for a clean number (a documented approximation, not a
per-table figure).

Skips Experiment B (index/schema variants) by design -- see the plan's
recorded scope decision.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import time
from datetime import UTC, datetime
from ipaddress import IPv4Address
from uuid import uuid4

import asyncpg

from common.config import settings
from common.models import LogLevel, ParsedLog, ParseStatus
from services.indexer.sink_pg import COLUMNS, _row

STRATEGIES = ("A1", "A2", "A3", "A4", "A5")
BATCH_SIZES = (1, 10, 100, 500, 1000, 2500, 5000, 10000)

_INSERT_SQL_TMPL = (
    f"INSERT INTO {{table}} ({', '.join(COLUMNS)}) "
    f"VALUES ({', '.join(f'${i + 1}' for i in range(len(COLUMNS)))}) "
    "ON CONFLICT (event_id, ts) DO NOTHING"
)


def make_rows(n: int) -> list[ParsedLog]:
    now = datetime.now(UTC)
    levels = list(LogLevel)
    return [
        ParsedLog(
            event_id=uuid4(), ts=now, ingested_at=now, emitted_at=now,
            service=f"svc-{i % 8}", host=f"host-{i % 20}",
            level=levels[i % len(levels)], message=f"benchmark row {i}",
            status_code=200, latency_ms=12.5, trace_id=f"t-{i}",
            user_id=f"u-{i % 1000}", client_ip=IPv4Address("10.0.0.1"),
            parse_status=ParseStatus.OK,
        )
        for i in range(n)
    ]


async def setup_table(con: asyncpg.Connection, name: str) -> None:
    await con.execute(f"DROP TABLE IF EXISTS {name}")
    await con.execute(f"CREATE TABLE {name} (LIKE logs INCLUDING ALL)")


async def teardown_table(con: asyncpg.Connection, name: str) -> None:
    await con.execute(f"DROP TABLE IF EXISTS {name}")


async def wal_bytes_since(con: asyncpg.Connection, start_lsn: str) -> int:
    return await con.fetchval(
        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)", start_lsn
    )


async def write_a1(con: asyncpg.Connection, table: str, rows: list[ParsedLog]) -> None:
    """A1: one INSERT per row, autocommit (no explicit transaction)."""
    sql = _INSERT_SQL_TMPL.format(table=table)
    for r in rows:
        await con.execute(sql, *_row(r))


async def write_a2(con: asyncpg.Connection, table: str, rows: list[ParsedLog]) -> None:
    """A2: one INSERT per row, all rows in a single transaction."""
    sql = _INSERT_SQL_TMPL.format(table=table)
    async with con.transaction():
        for r in rows:
            await con.execute(sql, *_row(r))


async def write_a3(con: asyncpg.Connection, table: str, rows: list[ParsedLog], chunk: int = 1000) -> None:
    """A3: multi-row INSERT ... VALUES, chunked to stay under the param limit."""
    ncols = len(COLUMNS)
    async with con.transaction():
        for i in range(0, len(rows), chunk):
            batch = rows[i : i + chunk]
            values_sql = ", ".join(
                "(" + ", ".join(f"${j * ncols + k + 1}" for k in range(ncols)) + ")"
                for j in range(len(batch))
            )
            params = [v for r in batch for v in _row(r)]
            sql = (
                f"INSERT INTO {table} ({', '.join(COLUMNS)}) VALUES {values_sql} "
                "ON CONFLICT (event_id, ts) DO NOTHING"
            )
            await con.execute(sql, *params)


async def write_a4(con: asyncpg.Connection, table: str, rows: list[ParsedLog]) -> None:
    """A4: asyncpg executemany."""
    sql = _INSERT_SQL_TMPL.format(table=table)
    await con.executemany(sql, [_row(r) for r in rows])


async def write_a5(con: asyncpg.Connection, table: str, rows: list[ParsedLog]) -> None:
    """A5 (chosen design): COPY into a temp stage, then INSERT ... ON CONFLICT."""
    await con.execute("DROP TABLE IF EXISTS _bench_stage")
    await con.execute(f"CREATE TEMP TABLE _bench_stage (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")
    async with con.transaction():
        await con.copy_records_to_table("_bench_stage", records=[_row(r) for r in rows], columns=COLUMNS)
        await con.execute(f"INSERT INTO {table} SELECT * FROM _bench_stage ON CONFLICT (event_id, ts) DO NOTHING")


WRITERS = {"A1": write_a1, "A2": write_a2, "A3": write_a3, "A4": write_a4, "A5": write_a5}


async def run_experiment_a(pool: asyncpg.Pool, rows: list[ParsedLog], strategies: list[str]) -> None:
    print("Experiment A -- write strategy comparison")
    print(f"{'strategy':<10}{'rows':>10}{'seconds':>12}{'rows/sec':>14}{'wal_bytes/100k':>18}")
    for name in strategies:
        table = f"logs_bench_{name.lower()}"
        async with pool.acquire() as con:
            await setup_table(con, table)
            start_lsn = str(await con.fetchval("SELECT pg_current_wal_lsn()"))
            start = time.perf_counter()
            await WRITERS[name](con, table, rows)
            elapsed = time.perf_counter() - start
            wal_bytes = await wal_bytes_since(con, start_lsn)
            await teardown_table(con, table)
        rows_per_sec = len(rows) / elapsed if elapsed > 0 else float("inf")
        wal_per_100k = wal_bytes / len(rows) * 100_000
        print(f"{name:<10}{len(rows):>10}{elapsed:>12.2f}{rows_per_sec:>14,.0f}{wal_per_100k:>18,.0f}")


async def run_experiment_c(pool: asyncpg.Pool, rows: list[ParsedLog], batch_sizes: tuple[int, ...]) -> None:
    print()
    print("Experiment C -- batch size sweep (A5 strategy)")
    print(f"{'batch_size':<12}{'rows/sec':>14}{'p99_flush_ms':>16}")
    table = "logs_bench_c"
    async with pool.acquire() as con:
        await setup_table(con, table)
        try:
            for size in batch_sizes:
                durations = []
                start_all = time.perf_counter()
                for i in range(0, len(rows), size):
                    chunk = rows[i : i + size]
                    t0 = time.perf_counter()
                    await write_a5(con, table, chunk)
                    durations.append(time.perf_counter() - t0)
                elapsed_all = time.perf_counter() - start_all
                durations.sort()
                p99 = durations[min(len(durations) - 1, int(len(durations) * 0.99))] * 1000
                rows_per_sec = len(rows) / elapsed_all if elapsed_all > 0 else float("inf")
                print(f"{size:<12}{rows_per_sec:>14,.0f}{p99:>16.2f}")
                await con.execute(f"TRUNCATE {table}")
        finally:
            await teardown_table(con, table)


async def main() -> None:
    p = argparse.ArgumentParser(description="Write-strategy benchmark (design doc SS5.2)")
    p.add_argument("--dsn", default=settings.pg_dsn)
    p.add_argument("--rows", type=int, default=50_000, help="design doc uses 500,000")
    p.add_argument("--strategies", default=",".join(STRATEGIES))
    p.add_argument("--skip-c", action="store_true")
    args = p.parse_args()

    random.seed(42)
    rows = make_rows(args.rows)
    pool = await asyncpg.create_pool(args.dsn, min_size=1, max_size=2)
    try:
        strategies = [s.strip().upper() for s in args.strategies.split(",") if s.strip()]
        await run_experiment_a(pool, rows, strategies)
        if not args.skip_c:
            await run_experiment_c(pool, rows, BATCH_SIZES)
    finally:
        await pool.close()

    print()
    print("Paste the tables above into bench/RESULTS.md under Claim 2.")


if __name__ == "__main__":
    asyncio.run(main())
