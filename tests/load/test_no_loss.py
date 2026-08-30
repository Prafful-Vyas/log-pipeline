"""Chaos / no-loss test (design doc SS5.3) -- proves G3 (zero message loss,
zero duplicates) empirically under indexer crashes and a Postgres restart.

Requires the full compose stack up (`make up`) and the `docker` CLI on PATH;
skips itself otherwise so it never breaks `pytest tests/unit`. Runs on the
HOST (like the rest of the test suite), so it talks to Postgres via the
published `localhost:5432` port -- but the *producer* has to run inside the
compose network (`docker compose run`), because Redpanda advertises itself as
`redpanda:9092`, a hostname that only resolves inside the compose network.

Deviation from the literal design-doc spec, documented here rather than
silently: this kills/restarts the whole `indexer` container (all worker
processes at once, via `docker compose kill -s SIGKILL`) rather than a single
worker process, since that's reliably controllable through the `docker
compose` CLI without exec'ing into container internals -- arguably a stronger
test of G3 than killing one of several workers.

    CHAOS_EVENTS=1000000 make chaos   # the literal design-doc scale

The always-on `producer` compose service is stopped for the duration of the
test (and restarted afterwards) so its ambient traffic doesn't confound the
before/after row-count deltas this test asserts on.
"""

from __future__ import annotations

import asyncio
import os
import re
import subprocess

import asyncpg
import pytest

CHAOS_EVENTS = int(os.environ.get("CHAOS_EVENTS", "100000"))
CHAOS_RATE = int(os.environ.get("CHAOS_RATE", "2000"))
PG_HOST_DSN = os.environ.get("CHAOS_PG_DSN", "postgresql://logs:logs@localhost:5432/logs")

SETTLE_S = 30
_TOTAL_SENT_RE = re.compile(r'"total_sent":\s*(\d+)')


def _compose(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", *args], capture_output=True, text=True, timeout=timeout, check=False
    )


def _compose_stack_reachable() -> bool:
    try:
        result = _compose("ps", "--format", "json", timeout=10)
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return False


@pytest.fixture(scope="module", autouse=True)
def _require_compose_stack():
    if not _compose_stack_reachable():
        pytest.skip("docker compose stack not reachable -- run `make up` first")


async def _counts(pool: asyncpg.Pool) -> tuple[int, int]:
    async with pool.acquire() as con:
        total = await con.fetchval("SELECT count(*) FROM logs")
        distinct = await con.fetchval("SELECT count(DISTINCT event_id) FROM logs")
    return total, distinct


async def _chaos_actions() -> None:
    """Fixed ~20s schedule: kill+restart the indexer twice, restart Postgres
    once, interleaved. Runs concurrently with production, independent of how
    long production takes."""
    schedule = (
        ("kill_indexer_1", ("kill", "-s", "SIGKILL", "indexer")),
        ("restart_indexer_1", ("up", "-d", "indexer")),
        ("restart_postgres", ("restart", "postgres")),
        ("kill_indexer_2", ("kill", "-s", "SIGKILL", "indexer")),
        ("restart_indexer_2", ("up", "-d", "indexer")),
    )
    for name, args in schedule:
        await asyncio.sleep(5)
        result = await asyncio.to_thread(_compose, *args)
        assert result.returncode == 0, f"chaos action {name} failed: {result.stderr}"


async def _produce_exact(rate: int, duration: int) -> int:
    """Runs a one-off producer container on the compose network (so it
    resolves `redpanda` by its compose hostname) and returns the exact count
    it logged as sent -- not rate*duration, since the token bucket is only
    approximate."""
    proc = await asyncio.create_subprocess_exec(
        "docker", "compose", "run", "--rm", "producer",
        "python", "-m", "services.producer",
        "--rate", str(rate), "--duration", str(duration), "--scenarios", "none",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    assert proc.stdout is not None
    chunks: list[str] = []
    async for line in proc.stdout:
        chunks.append(line.decode(errors="replace"))
    await proc.wait()
    text = "".join(chunks)
    matches = _TOTAL_SENT_RE.findall(text)
    assert matches, f"could not find total_sent in producer output:\n{text[-2000:]}"
    return int(matches[-1])


@pytest.mark.asyncio
async def test_no_loss_under_indexer_and_postgres_chaos() -> None:
    duration = max(10, round(CHAOS_EVENTS / CHAOS_RATE))

    stop_ambient = _compose("stop", "producer")
    assert stop_ambient.returncode == 0, f"failed to stop ambient producer: {stop_ambient.stderr}"
    try:
        pool = await asyncpg.create_pool(PG_HOST_DSN, min_size=1, max_size=2)
        try:
            baseline_total, baseline_distinct = await _counts(pool)

            sent, _ = await asyncio.gather(
                _produce_exact(CHAOS_RATE, duration),
                _chaos_actions(),
            )

            await asyncio.sleep(SETTLE_S)
            final_total, final_distinct = await _counts(pool)
        finally:
            await pool.close()
    finally:
        restart_ambient = _compose("start", "producer")
        assert restart_ambient.returncode == 0, (
            f"failed to restart ambient producer: {restart_ambient.stderr}"
        )

    delta_total = final_total - baseline_total
    delta_distinct = final_distinct - baseline_distinct
    assert delta_total == sent, f"row count mismatch: sent {sent}, delta {delta_total} (loss or extra rows)"
    assert delta_distinct == sent, f"duplicate rows: sent {sent} distinct ids, delta {delta_distinct}"
