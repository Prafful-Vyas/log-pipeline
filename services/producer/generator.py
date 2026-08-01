from __future__ import annotations

import random
import string
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from common.models import LogEvent

_HEX = string.hexdigits[:16]


@dataclass(frozen=True)
class ServiceProfile:
    name: str
    formats: tuple[str, ...]
    level_weights: dict[str, float]
    endpoints: tuple[str, ...]
    latency_dist: tuple[float, float]  # lognormal (mu, sigma), milliseconds
    error_burst_prob: float
    volume_weight: float = 1.0


PROFILES: tuple[ServiceProfile, ...] = (
    ServiceProfile(
        name="api-gateway",
        formats=("json", "apache_combined", "plain"),
        level_weights={"INFO": 0.90, "WARN": 0.06, "ERROR": 0.035, "FATAL": 0.005},
        endpoints=("/api/v1/orders", "/api/v1/users", "/api/v1/search", "/health"),
        latency_dist=(3.2, 0.6),
        error_burst_prob=0.02,
        volume_weight=2.5,
    ),
    ServiceProfile(
        name="auth-service",
        formats=("json", "logfmt"),
        level_weights={"INFO": 0.85, "WARN": 0.08, "ERROR": 0.06, "FATAL": 0.01},
        endpoints=("/login", "/logout", "/refresh", "/verify"),
        latency_dist=(2.8, 0.5),
        error_burst_prob=0.03,
        volume_weight=1.5,
    ),
    ServiceProfile(
        name="payment-service",
        formats=("syslog", "logfmt"),
        level_weights={"INFO": 0.88, "WARN": 0.07, "ERROR": 0.045, "FATAL": 0.005},
        endpoints=("/charge", "/refund", "/webhook"),
        latency_dist=(4.0, 0.7),
        error_burst_prob=0.02,
        volume_weight=1.2,
    ),
    ServiceProfile(
        name="order-service",
        formats=("json", "logfmt"),
        level_weights={"INFO": 0.89, "WARN": 0.065, "ERROR": 0.04, "FATAL": 0.005},
        endpoints=("/orders", "/orders/{id}", "/cart"),
        latency_dist=(3.5, 0.6),
        error_burst_prob=0.02,
        volume_weight=1.3,
    ),
    ServiceProfile(
        name="inventory-service",
        formats=("logfmt", "plain"),
        level_weights={"INFO": 0.92, "WARN": 0.05, "ERROR": 0.025, "FATAL": 0.005},
        endpoints=("/stock", "/reserve", "/restock"),
        latency_dist=(3.0, 0.5),
        error_burst_prob=0.015,
        volume_weight=0.8,
    ),
    ServiceProfile(
        name="search-service",
        formats=("json", "apache_combined"),
        level_weights={"INFO": 0.90, "WARN": 0.06, "ERROR": 0.035, "FATAL": 0.005},
        endpoints=("/search", "/autocomplete", "/reindex"),
        latency_dist=(3.8, 0.8),
        error_burst_prob=0.02,
        volume_weight=1.0,
    ),
    ServiceProfile(
        name="notification-service",
        formats=("logfmt", "plain"),
        level_weights={"INFO": 0.91, "WARN": 0.055, "ERROR": 0.03, "FATAL": 0.005},
        endpoints=("/send-email", "/send-sms", "/push"),
        latency_dist=(3.3, 0.6),
        error_burst_prob=0.015,
        volume_weight=0.7,
    ),
    ServiceProfile(
        name="cron-worker",
        formats=("plain", "logfmt"),
        level_weights={"INFO": 0.95, "WARN": 0.03, "ERROR": 0.018, "FATAL": 0.002},
        endpoints=("/jobs/cleanup", "/jobs/rollup", "/jobs/export"),
        latency_dist=(5.0, 1.0),
        error_burst_prob=0.01,
        volume_weight=0.4,
    ),
)


@dataclass
class ScenarioState:
    """Mutable per-service overrides applied by scenarios.py."""

    error_mult: float = 1.0
    latency_mult: float = 1.0
    silenced: bool = False
    forced_status: int | None = None


def default_states(profiles: tuple[ServiceProfile, ...]) -> dict[str, ScenarioState]:
    return {p.name: ScenarioState() for p in profiles}


_STATUS_BY_LEVEL = {
    "INFO": (200, 200, 200, 201, 204, 304),
    "WARN": (400, 401, 404, 409, 429),
    "ERROR": (402, 500, 502, 503),
    "FATAL": (500, 500, 503),
}


def _rand_hex(n: int) -> str:
    return "".join(random.choices(_HEX, k=n))


def _rand_host(service: str) -> str:
    return f"{service[:3]}-{_rand_hex(5)}-{_rand_hex(2)}"


def _rand_ip() -> str:
    return f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"


def pick_level(profile: ServiceProfile, state: ScenarioState) -> str:
    weights = dict(profile.level_weights)
    if state.error_mult != 1.0:
        bump = weights["ERROR"] * (state.error_mult - 1.0)
        weights["ERROR"] += bump
        weights["INFO"] = max(0.01, weights["INFO"] - bump)
    levels, w = zip(*weights.items())
    return random.choices(levels, weights=w, k=1)[0]


def _fields_for(
    profile: ServiceProfile, state: ScenarioState, client_ip: str | None = None
) -> dict:
    level = pick_level(profile, state)
    latency = max(
        0.5, random.lognormvariate(*profile.latency_dist) * state.latency_mult
    )
    status = state.forced_status or random.choice(_STATUS_BY_LEVEL[level])
    return {
        "ts": datetime.now(UTC),
        "level": level,
        "service": profile.name,
        "host": _rand_host(profile.name),
        "endpoint": random.choice(profile.endpoints),
        "status": status,
        "latency_ms": round(latency, 2),
        "trace_id": _rand_hex(12),
        "request_id": f"r-{_rand_hex(6)}",
        "user_id": f"u-{random.randint(1000, 9999)}",
        "client_ip": client_ip or _rand_ip(),
        "message": _message_for(profile.name, level, random.choice(profile.endpoints)),
    }


_MESSAGES = {
    "INFO": "request completed",
    "WARN": "request completed with warning",
    "ERROR": "request failed",
    "FATAL": "unhandled exception",
}


def _message_for(service: str, level: str, endpoint: str) -> str:
    return f"{service} {endpoint} {_MESSAGES[level]}"


def render_json(f: dict) -> str:
    import orjson

    return orjson.dumps(
        {
            "ts": f["ts"].isoformat(),
            "level": f["level"],
            "service": f["service"],
            "host": f["host"],
            "msg": f["message"],
            "path": f["endpoint"],
            "status": f["status"],
            "latency_ms": f["latency_ms"],
            "trace_id": f["trace_id"],
            "request_id": f["request_id"],
            "user_id": f["user_id"],
            "client_ip": f["client_ip"],
        }
    ).decode()


def render_syslog(f: dict) -> str:
    pri = 134 if f["level"] in ("ERROR", "FATAL") else 14
    ts = f["ts"].strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    msgid = f"ID{random.randint(10, 99)}"
    body = (
        f'level={f["level"]} trace_id={f["trace_id"]} request_id={f["request_id"]} '
        f'msg="{f["message"]}" status={f["status"]} latency_ms={f["latency_ms"]} '
        f'user_id={f["user_id"]}'
    )
    return f'<{pri}>1 {ts} {f["host"]} {f["service"]} {random.randint(1000,9999)} {msgid} - {body}'


def render_logfmt(f: dict) -> str:
    return (
        f'level={f["level"]} service={f["service"]} host={f["host"]} '
        f'path={f["endpoint"]} status={f["status"]} latency_ms={f["latency_ms"]} '
        f'trace_id={f["trace_id"]} request_id={f["request_id"]} user_id={f["user_id"]} '
        f'client_ip={f["client_ip"]} msg="{f["message"]}"'
    )


_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def render_apache_combined(f: dict) -> str:
    t = f["ts"]
    ts_fmt = f"{t.day:02d}/{_MONTHS[t.month - 1]}/{t.year}:{t.hour:02d}:{t.minute:02d}:{t.second:02d} +0000"
    method = "GET" if f["level"] in ("INFO", "WARN") else random.choice(("GET", "POST"))
    size = random.randint(120, 8192)
    return (
        f'{f["client_ip"]} - - [{ts_fmt}] "{method} {f["endpoint"]} HTTP/1.1" '
        f'{f["status"]} {size} "-" "Mozilla/5.0 (bench-agent)"'
    )


def render_plain(f: dict) -> str:
    ts = f["ts"].strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    return (
        f'{ts} {f["level"]} [{f["service"]}] {f["message"]} '
        f'status={f["status"]} latency={f["latency_ms"]}ms user={f["user_id"]}'
    )


_RENDERERS = {
    "json": render_json,
    "syslog": render_syslog,
    "logfmt": render_logfmt,
    "apache_combined": render_apache_combined,
    "plain": render_plain,
}


def build_event(profile: ServiceProfile, state: ScenarioState) -> LogEvent | None:
    if state.silenced:
        return None
    fmt = random.choice(profile.formats)
    fields = _fields_for(profile, state)
    raw = _RENDERERS[fmt](fields)
    return LogEvent(
        event_id=uuid4(),
        emitted_at=time.time(),
        service=profile.name,
        host=fields["host"],
        format=fmt,
        raw=raw,
    )


def build_injected_event(
    profile: ServiceProfile,
    *,
    level: str = "ERROR",
    endpoint: str | None = None,
    client_ip: str | None = None,
    user_id: str | None = None,
    message: str | None = None,
    status: int = 401,
    fmt: str | None = None,
) -> LogEvent:
    """Build a one-off event outside the normal weighted distribution, used by
    scenario injections (brute force, sqli probes) that need specific field values."""
    fields = _fields_for(profile, ScenarioState(), client_ip=client_ip)
    fields["level"] = level
    fields["status"] = status
    fields["endpoint"] = endpoint or fields["endpoint"]
    fields["user_id"] = user_id or fields["user_id"]
    fields["message"] = message or fields["message"]
    chosen_fmt = fmt or random.choice(profile.formats)
    raw = _RENDERERS[chosen_fmt](fields)
    return LogEvent(
        event_id=uuid4(),
        emitted_at=time.time(),
        service=profile.name,
        host=fields["host"],
        format=chosen_fmt,
        raw=raw,
    )
