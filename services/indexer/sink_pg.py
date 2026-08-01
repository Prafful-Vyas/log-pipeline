from __future__ import annotations

import asyncpg
import orjson
import structlog

from common.models import ParsedLog, SecurityAlert

log = structlog.get_logger("indexer.sink_pg")

COLUMNS = (
    "event_id", "ts", "ingested_at", "emitted_at", "service", "host",
    "level", "message", "status_code", "latency_ms", "trace_id",
    "user_id", "client_ip", "is_security", "security_rule", "attrs",
    "parse_status",
)

_INSERT_ROW_SQL = f"""
    INSERT INTO logs ({", ".join(COLUMNS)})
    VALUES ({", ".join(f"${i+1}" for i in range(len(COLUMNS)))})
    ON CONFLICT (event_id, ts) DO NOTHING
"""

_INSERT_ALERT_SQL = """
    INSERT INTO security_alerts
        (event_id, ts, rule_id, severity, service, client_ip, user_id, detail)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT (event_id, rule_id) DO NOTHING
"""


def _row(p: ParsedLog) -> tuple:
    return (
        p.event_id, p.ts, p.ingested_at, p.emitted_at, p.service, p.host,
        p.level.value, p.message, p.status_code, p.latency_ms, p.trace_id,
        p.user_id, str(p.client_ip) if p.client_ip else None, p.is_security,
        p.security_rule, orjson.dumps(p.attrs).decode(), p.parse_status.value,
    )


def _alert_row(a: SecurityAlert) -> tuple:
    return (
        a.event_id, a.ts, a.rule_id, a.severity, a.service,
        str(a.client_ip) if a.client_ip else None, a.user_id,
        orjson.dumps(a.detail).decode(),
    )


class PgSink:
    """Batched COPY-to-staging writer (the chosen design) plus a naive
    row-at-a-time writer kept behind WRITE_STRATEGY=naive as the M2 baseline
    for the write-strategy benchmark comparison."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def write_batch(self, rows: list[ParsedLog], alerts: list[SecurityAlert]) -> None:
        if not rows:
            return
        records = [_row(p) for p in rows]
        alert_records = [_alert_row(a) for a in alerts]
        async with self._pool.acquire() as con, con.transaction():
            await con.execute("DROP TABLE IF EXISTS _stage")
            await con.execute("CREATE TEMP TABLE _stage (LIKE logs INCLUDING DEFAULTS) ON COMMIT DROP")
            await con.copy_records_to_table("_stage", records=records, columns=COLUMNS)
            await con.execute(
                "INSERT INTO logs SELECT * FROM _stage ON CONFLICT (event_id, ts) DO NOTHING"
            )
            if alert_records:
                await con.executemany(_INSERT_ALERT_SQL, alert_records)

    async def write_naive(self, rows: list[ParsedLog], alerts: list[SecurityAlert]) -> None:
        """One INSERT per row, one transaction per row — the deliberately slow
        baseline for the batching benchmark (§5.2 of the design doc)."""
        async with self._pool.acquire() as con:
            for p in rows:
                await con.execute(_INSERT_ROW_SQL, *_row(p))
            for a in alerts:
                await con.execute(_INSERT_ALERT_SQL, *_alert_row(a))
