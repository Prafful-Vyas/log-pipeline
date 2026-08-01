from __future__ import annotations

import re
from collections.abc import Callable
from datetime import UTC, datetime

import orjson

from common.models import ParseStatus

MAX_MESSAGE_BYTES = 8192
MAX_ATTRS_KEYS = 64

# -- pre-compiled, anchored, non-backtracking-prone regexes ---------------

RE_SYSLOG5424 = re.compile(
    r"^<(?P<pri>\d{1,3})>(?P<version>\d) (?P<ts>\S+) (?P<host>\S+) "
    r"(?P<app>\S+) (?P<procid>\S+) (?P<msgid>\S+) (?P<sd>-|\[[^\]]*\]) (?P<msg>.*)$"
)

RE_APACHE_HEAD = re.compile(r"^\S+ \S+ \S+ \[[^\]]+\] \"")
RE_APACHE_COMBINED = re.compile(
    r'^(?P<ip>\S+) \S+ \S+ \[(?P<ts>[^\]]+)\] '
    r'"(?P<method>[A-Z]+) (?P<path>\S+) [^"]+" '
    r'(?P<status>\d{3}) (?P<size>\S+) "(?P<ref>[^"]*)" "(?P<ua>[^"]*)"$'
)

RE_PLAIN_HEAD = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) (?P<level>\w+) "
    r"\[(?P<service>[^\]]+)\] (?P<rest>.*)$"
)
RE_PLAIN_STATUS = re.compile(r"status=(\d{3})")
RE_PLAIN_LATENCY = re.compile(r"latency=([\d.]+)ms")
RE_PLAIN_USER = re.compile(r"user=(\S+)")


class ParseError(Exception):
    pass


def _cap_attrs(d: dict) -> dict:
    if len(d) <= MAX_ATTRS_KEYS:
        return d
    return dict(list(d.items())[:MAX_ATTRS_KEYS])


def scan_logfmt(s: str) -> dict[str, str]:
    """Hand-written scanner (not regex) for key=value / key="quoted value" pairs.
    The hottest parse path, benchmarked faster than an equivalent regex."""
    out: dict[str, str] = {}
    i, n = 0, len(s)
    while i < n:
        while i < n and s[i] == " ":
            i += 1
        start = i
        while i < n and s[i] not in "= ":
            i += 1
        if i >= n or s[i] != "=":
            i += 1
            continue
        key = s[start:i]
        i += 1  # skip '='
        if i < n and s[i] == '"':
            i += 1
            val_start = i
            while i < n and s[i] != '"':
                if s[i] == "\\":
                    i += 1
                i += 1
            out[key] = s[val_start:i].replace('\\"', '"')
            i += 1  # skip closing quote
        else:
            val_start = i
            while i < n and s[i] != " ":
                i += 1
            out[key] = s[val_start:i]
    return out


def parse_timestamp(raw: str | None, fallback: datetime) -> datetime:
    if not raw:
        return fallback
    try:
        dt = datetime.fromisoformat(raw)  # Python 3.11+ parses a trailing "Z" natively
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except ValueError:
        pass
    for fmt in ("%d/%b/%Y:%H:%M:%S %z", "%Y-%m-%d %H:%M:%S,%f"):
        try:
            dt = datetime.strptime(raw, fmt)  # noqa: DTZ007 - naive case handled below
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except ValueError:
            continue
    return fallback


def _to_int(v) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_float(v) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def parse_json(raw: str, ingested_at: datetime) -> dict:
    try:
        data = orjson.loads(raw)
    except orjson.JSONDecodeError as e:
        raise ParseError(str(e)) from e
    if not isinstance(data, dict):
        raise ParseError("json root is not an object")
    known = {"ts", "level", "service", "host", "msg", "message", "path", "status",
             "latency_ms", "trace_id", "request_id", "user_id", "client_ip"}
    attrs = _cap_attrs({k: v for k, v in data.items() if k not in known})
    return {
        "ts": parse_timestamp(data.get("ts"), ingested_at),
        "level": data.get("level"),
        "message": (data.get("msg") or data.get("message") or "")[:MAX_MESSAGE_BYTES],
        "status_code": _to_int(data.get("status")),
        "latency_ms": _to_float(data.get("latency_ms")),
        "trace_id": data.get("trace_id"),
        "user_id": data.get("user_id"),
        "client_ip": data.get("client_ip"),
        "attrs": {**attrs, **({"path": data["path"]} if "path" in data else {}),
                  **({"request_id": data["request_id"]} if "request_id" in data else {})},
    }


def parse_syslog5424(raw: str, ingested_at: datetime) -> dict:
    m = RE_SYSLOG5424.match(raw)
    if not m:
        raise ParseError("syslog header mismatch")
    kv = scan_logfmt(m.group("msg"))
    return {
        "ts": parse_timestamp(m.group("ts"), ingested_at),
        "level": kv.get("level"),
        "message": kv.get("msg", m.group("msg"))[:MAX_MESSAGE_BYTES],
        "status_code": _to_int(kv.get("status")),
        "latency_ms": _to_float(kv.get("latency_ms")),
        "trace_id": kv.get("trace_id"),
        "user_id": kv.get("user_id"),
        "client_ip": kv.get("client_ip"),
        "attrs": _cap_attrs(
            {k: v for k, v in kv.items()
             if k not in ("level", "msg", "status", "latency_ms", "trace_id", "user_id", "client_ip")}
        ),
    }


def parse_apache_combined(raw: str, ingested_at: datetime) -> dict:
    m = RE_APACHE_COMBINED.match(raw)
    if not m:
        raise ParseError("apache combined mismatch")
    status = int(m.group("status"))
    level = "ERROR" if status >= 500 else "WARN" if status >= 400 else "INFO"
    return {
        "ts": parse_timestamp(m.group("ts"), ingested_at),
        "level": level,
        "message": f'{m.group("method")} {m.group("path")}'[:MAX_MESSAGE_BYTES],
        "status_code": status,
        "latency_ms": None,
        "trace_id": None,
        "user_id": None,
        "client_ip": m.group("ip"),
        "attrs": _cap_attrs({"path": m.group("path"), "method": m.group("method"),
                              "referer": m.group("ref"), "user_agent": m.group("ua")}),
    }


def parse_logfmt(raw: str, ingested_at: datetime) -> dict:
    kv = scan_logfmt(raw)
    if not kv:
        raise ParseError("no key=value pairs found")
    return {
        "ts": ingested_at,
        "level": kv.get("level"),
        "message": kv.get("msg", raw)[:MAX_MESSAGE_BYTES],
        "status_code": _to_int(kv.get("status")),
        "latency_ms": _to_float(kv.get("latency_ms")),
        "trace_id": kv.get("trace_id"),
        "user_id": kv.get("user_id"),
        "client_ip": kv.get("client_ip"),
        "attrs": _cap_attrs(
            {k: v for k, v in kv.items()
             if k not in ("level", "service", "host", "msg", "status", "latency_ms",
                          "trace_id", "user_id", "client_ip")}
        ),
    }


def parse_plain(raw: str, ingested_at: datetime) -> dict:
    m = RE_PLAIN_HEAD.match(raw)
    if not m:
        return {
            "ts": ingested_at,
            "level": None,
            "message": raw[:MAX_MESSAGE_BYTES],
            "status_code": None,
            "latency_ms": None,
            "trace_id": None,
            "user_id": None,
            "client_ip": None,
            "attrs": {},
        }
    rest = m.group("rest")
    status_m = RE_PLAIN_STATUS.search(rest)
    latency_m = RE_PLAIN_LATENCY.search(rest)
    user_m = RE_PLAIN_USER.search(rest)
    return {
        "ts": parse_timestamp(m.group("ts"), ingested_at),
        "level": m.group("level"),
        "message": rest[:MAX_MESSAGE_BYTES],
        "status_code": _to_int(status_m.group(1)) if status_m else None,
        "latency_ms": _to_float(latency_m.group(1)) if latency_m else None,
        "trace_id": None,
        "user_id": user_m.group(1) if user_m else None,
        "client_ip": None,
        "attrs": {},
    }


PARSERS: list[tuple[Callable[[str], bool], Callable[[str, datetime], dict]]] = [
    (lambda s: s.startswith("{"), parse_json),
    (lambda s: s.startswith("<"), parse_syslog5424),
    (lambda s: bool(RE_APACHE_HEAD.match(s)), parse_apache_combined),
    (lambda s: "=" in s[:40], parse_logfmt),
    (lambda s: True, parse_plain),  # terminal fallback, never fails
]


def parse(raw: str, ingested_at: datetime) -> tuple[dict, ParseStatus]:
    if len(raw.encode("utf-8", "ignore")) > 16 * 1024:
        raw = raw[:16 * 1024]
    for detect, fn in PARSERS:
        if detect(raw):
            try:
                return fn(raw, ingested_at), ParseStatus.OK
            except ParseError:
                continue
    return {
        "ts": ingested_at, "level": None, "message": raw[:MAX_MESSAGE_BYTES],
        "status_code": None, "latency_ms": None, "trace_id": None,
        "user_id": None, "client_ip": None, "attrs": {},
    }, ParseStatus.FAILED
