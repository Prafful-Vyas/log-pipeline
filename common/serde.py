from __future__ import annotations

from uuid import UUID

import orjson

from common.models import LogEvent, SecurityAlert


def encode_log_event(event: LogEvent) -> bytes:
    return orjson.dumps(
        {
            "event_id": str(event.event_id),
            "emitted_at": event.emitted_at,
            "service": event.service,
            "host": event.host,
            "format": event.format,
            "raw": event.raw,
        }
    )


def decode_log_event(payload: bytes) -> LogEvent:
    data = orjson.loads(payload)
    return LogEvent(
        event_id=UUID(data["event_id"]),
        emitted_at=data["emitted_at"],
        service=data["service"],
        host=data["host"],
        format=data["format"],
        raw=data["raw"],
    )


def encode_security_alert(alert: SecurityAlert) -> bytes:
    return orjson.dumps(
        {
            "event_id": str(alert.event_id),
            "ts": alert.ts.isoformat(),
            "rule_id": alert.rule_id,
            "severity": alert.severity,
            "service": alert.service,
            "client_ip": str(alert.client_ip) if alert.client_ip else None,
            "user_id": alert.user_id,
            "detail": alert.detail,
        }
    )


def decode_security_alert(payload: bytes) -> SecurityAlert:
    data = orjson.loads(payload)
    return SecurityAlert(
        event_id=UUID(data["event_id"]),
        ts=data["ts"],
        rule_id=data["rule_id"],
        severity=data["severity"],
        service=data["service"],
        client_ip=data.get("client_ip"),
        user_id=data.get("user_id"),
        detail=data.get("detail") or {},
    )
