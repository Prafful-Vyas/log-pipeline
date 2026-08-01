from __future__ import annotations

import re
import time
from collections import OrderedDict, deque

from common.models import ParsedLog, SecurityHit

MAX_KEYS = 100_000

RE_SQLI = re.compile(r"union\s+select|'\s*or\s*'1'\s*=\s*'1|;\s*drop\s+table|/\*!", re.IGNORECASE)
RE_XSS = re.compile(r"<script|javascript:|onerror\s*=", re.IGNORECASE)
RE_PATH_TRAVERSAL = re.compile(r"\.\./\.\./|%2e%2e%2f", re.IGNORECASE)
RE_TOKEN_LEAK = re.compile(
    r"bearer\s+[a-z0-9\-_.]{20,}"
    r"|eyj[a-z0-9_-]{10,}\.[a-z0-9_-]{10,}\.[a-z0-9_-]{10,}"
    r"|akia[0-9a-z]{16}",
    re.IGNORECASE,
)
PRIV_KEYWORDS = ("sudo", "role=admin", "permission granted")

AUTH_FAIL_ENDPOINTS = ("login", "auth")


class SlidingWindowCounter:
    """Bounded-memory count of hits per key within a rolling window.
    LRU-evicts the oldest key once more than max_keys are tracked."""

    def __init__(self, window_s: float, max_keys: int = MAX_KEYS):
        self._window = window_s
        self._max_keys = max_keys
        self._data: OrderedDict[str, deque] = OrderedDict()

    def hit(self, key: str, now: float | None = None) -> int:
        now = now if now is not None else time.monotonic()
        dq = self._data.get(key)
        if dq is None:
            dq = deque()
            self._data[key] = dq
        self._data.move_to_end(key)
        dq.append(now)
        cutoff = now - self._window
        while dq and dq[0] < cutoff:
            dq.popleft()
        while len(self._data) > self._max_keys:
            self._data.popitem(last=False)
        return len(dq)


class DistinctWindowCounter:
    """Count of distinct values seen per key within a rolling window."""

    def __init__(self, window_s: float, max_keys: int = MAX_KEYS):
        self._window = window_s
        self._max_keys = max_keys
        self._data: OrderedDict[str, deque] = OrderedDict()

    def hit(self, key: str, value: str, now: float | None = None) -> int:
        now = now if now is not None else time.monotonic()
        dq = self._data.get(key)
        if dq is None:
            dq = deque()
            self._data[key] = dq
        self._data.move_to_end(key)
        dq.append((now, value))
        cutoff = now - self._window
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        while len(self._data) > self._max_keys:
            self._data.popitem(last=False)
        return len({v for _, v in dq})


class Classifier:
    def __init__(self) -> None:
        self._brute = SlidingWindowCounter(window_s=60)
        self._spray = DistinctWindowCounter(window_s=300)
        self._server_err = SlidingWindowCounter(window_s=30)

    def classify(self, log: ParsedLog) -> list[SecurityHit]:
        hits: list[SecurityHit] = []
        haystack = " ".join([log.message, *(str(v) for v in log.attrs.values())])

        is_auth_failure = log.status_code == 401 or "auth failed" in log.message.lower()
        if is_auth_failure and log.client_ip:
            ip = str(log.client_ip)
            count = self._brute.hit(ip)
            if count >= 20:
                hits.append(SecurityHit(
                    rule_id="AUTH_BRUTE_FORCE", severity=4,
                    detail={"client_ip": ip, "count_60s": count}))
            if log.user_id:
                distinct = self._spray.hit(ip, log.user_id)
                if distinct >= 10:
                    hits.append(SecurityHit(
                        rule_id="AUTH_SPRAY", severity=4,
                        detail={"client_ip": ip, "distinct_users_300s": distinct}))

        if RE_SQLI.search(haystack):
            hits.append(SecurityHit(rule_id="SQLI_SIGNATURE", severity=5, detail={}))

        if RE_XSS.search(haystack):
            hits.append(SecurityHit(rule_id="XSS_SIGNATURE", severity=3, detail={}))

        if RE_PATH_TRAVERSAL.search(haystack):
            hits.append(SecurityHit(rule_id="PATH_TRAVERSAL", severity=4, detail={}))

        if any(kw in haystack.lower() for kw in PRIV_KEYWORDS):
            hits.append(SecurityHit(rule_id="PRIV_ESCALATION", severity=3, detail={}))

        if RE_TOKEN_LEAK.search(haystack):
            hits.append(SecurityHit(rule_id="TOKEN_LEAK", severity=5, detail={}))

        if log.status_code and log.status_code >= 500:
            count = self._server_err.hit(log.service)
            if count >= 50:
                hits.append(SecurityHit(
                    rule_id="SERVER_ERROR_BURST", severity=2,
                    detail={"service": log.service, "count_30s": count}))

        return hits
