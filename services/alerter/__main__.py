from __future__ import annotations

import asyncio
import json
import urllib.request
from collections import OrderedDict

import structlog
from aiokafka import AIOKafkaConsumer

from common.config import settings
from common.logging import configure_logging
from common.serde import decode_security_alert

log: structlog.stdlib.BoundLogger = configure_logging("alerter")

MAX_DEDUP_KEYS = 50_000


class Deduplicator:
    """Collapses a burst of identical alerts into one notification per
    (rule_id, client_ip, bucket) — e.g. a 2,000-event brute-force burst
    yields a single notification, not 2,000."""

    def __init__(self, window_s: int, max_keys: int = MAX_DEDUP_KEYS):
        self._window = window_s
        self._max_keys = max_keys
        self._seen: OrderedDict[tuple, None] = OrderedDict()

    def is_new(self, rule_id: str, client_ip: str | None, ts_epoch: float) -> bool:
        bucket = int(ts_epoch // self._window)
        key = (rule_id, client_ip or "-", bucket)
        if key in self._seen:
            self._seen.move_to_end(key)
            return False
        self._seen[key] = None
        while len(self._seen) > self._max_keys:
            self._seen.popitem(last=False)
        return True


def notify_stdout(alert) -> None:
    log.warning(
        "security_alert",
        rule_id=alert.rule_id,
        severity=alert.severity,
        service=alert.service,
        client_ip=str(alert.client_ip) if alert.client_ip else None,
        user_id=alert.user_id,
        detail=alert.detail,
        ts=alert.ts.isoformat(),
    )


def _post_webhook(url: str, body: dict) -> None:
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        resp.read()


async def notify_webhook(alert) -> None:
    body = {
        "rule_id": alert.rule_id,
        "severity": alert.severity,
        "service": alert.service,
        "client_ip": str(alert.client_ip) if alert.client_ip else None,
        "user_id": alert.user_id,
        "detail": alert.detail,
        "ts": alert.ts.isoformat(),
    }
    try:
        await asyncio.to_thread(_post_webhook, settings.alert_webhook_url, body)
    except Exception:
        log.exception("webhook_delivery_failed", rule_id=alert.rule_id)


async def main() -> None:
    consumer = AIOKafkaConsumer(
        settings.topic_security,
        bootstrap_servers=settings.kafka_brokers,
        group_id="alert-notifier",
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    await consumer.start()
    log.info("alerter_started", sink=settings.alert_sink, topic=settings.topic_security)

    dedup = Deduplicator(settings.alert_dedup_window_s)
    try:
        async for msg in consumer:
            try:
                alert = decode_security_alert(msg.value)
            except Exception:
                log.exception("alert_decode_failed", offset=msg.offset)
                continue

            if not dedup.is_new(alert.rule_id, str(alert.client_ip) if alert.client_ip else None,
                                 alert.ts.timestamp()):
                continue

            notify_stdout(alert)
            if settings.alert_sink == "webhook" and settings.alert_webhook_url:
                await notify_webhook(alert)
    finally:
        await consumer.stop()
        log.info("alerter_stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
