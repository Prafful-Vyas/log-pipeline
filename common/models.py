from __future__ import annotations

import enum
from datetime import datetime
from ipaddress import IPv4Address
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class LogLevel(str, enum.Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"


class ParseStatus(str, enum.Enum):
    OK = "ok"
    PARTIAL = "partial"
    FAILED = "failed"


LEVEL_ALIASES: dict[str, LogLevel] = {
    "debug": LogLevel.DEBUG, "dbg": LogLevel.DEBUG, "10": LogLevel.DEBUG,
    "info": LogLevel.INFO, "information": LogLevel.INFO, "20": LogLevel.INFO,
    "warn": LogLevel.WARN, "warning": LogLevel.WARN, "w": LogLevel.WARN, "30": LogLevel.WARN,
    "error": LogLevel.ERROR, "err": LogLevel.ERROR, "e": LogLevel.ERROR, "40": LogLevel.ERROR,
    "fatal": LogLevel.FATAL, "critical": LogLevel.FATAL, "crit": LogLevel.FATAL,
    "f": LogLevel.FATAL, "50": LogLevel.FATAL,
}


def normalize_level(raw: str | None) -> LogLevel:
    if not raw:
        return LogLevel.INFO
    return LEVEL_ALIASES.get(raw.strip().lower(), LogLevel.INFO)


class LogEvent(BaseModel):
    """Wire envelope published by the producer to `logs.raw`."""

    model_config = ConfigDict(frozen=True)

    event_id: UUID
    emitted_at: float  # unix epoch seconds, producer clock
    service: str
    host: str
    format: str  # json | syslog | logfmt | apache_combined | plain
    raw: str


class ParsedLog(BaseModel):
    """Internal representation after the parse/enrich/classify chain."""

    event_id: UUID
    ts: datetime
    ingested_at: datetime
    emitted_at: datetime
    service: str
    host: str
    level: LogLevel
    message: str
    status_code: int | None = None
    latency_ms: float | None = None
    trace_id: str | None = None
    user_id: str | None = None
    client_ip: IPv4Address | None = None
    is_security: bool = False
    security_rule: str | None = None
    attrs: dict[str, Any] = {}
    parse_status: ParseStatus = ParseStatus.OK


class SecurityHit(BaseModel):
    rule_id: str
    severity: int
    detail: dict[str, Any] = {}


class SecurityAlert(BaseModel):
    event_id: UUID
    ts: datetime
    rule_id: str
    severity: int
    service: str
    client_ip: IPv4Address | None = None
    user_id: str | None = None
    detail: dict[str, Any] = {}
