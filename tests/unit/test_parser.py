from __future__ import annotations

from datetime import UTC, datetime

from common.models import ParseStatus
from services.indexer.parser import parse, scan_logfmt


def test_parse_json() -> None:
    raw = (
        '{"ts":"2026-07-30T11:32:03.481Z","level":"ERROR","msg":"charge declined",'
        '"status":402,"latency_ms":812.0,"trace_id":"abc123","user_id":"u-1",'
        '"client_ip":"10.0.0.1"}'
    )
    fields, status = parse(raw, datetime.now(UTC))
    assert status == ParseStatus.OK
    assert fields["level"] == "ERROR"
    assert fields["status_code"] == 402
    assert fields["latency_ms"] == 812.0
    assert fields["trace_id"] == "abc123"
    assert fields["client_ip"] == "10.0.0.1"


def test_parse_syslog5424() -> None:
    raw = (
        "<134>1 2026-07-30T11:32:03.481Z pay-7f9c4-xk2 payment-service 4412 ID47 - "
        'level=ERROR trace_id=8f2c1a request_id=r-88213 msg="charge declined" '
        "status=402 latency_ms=812 user_id=u-4471"
    )
    fields, status = parse(raw, datetime.now(UTC))
    assert status == ParseStatus.OK
    assert fields["level"] == "ERROR"
    assert fields["message"] == "charge declined"
    assert fields["status_code"] == 402
    assert fields["user_id"] == "u-4471"


def test_parse_logfmt() -> None:
    raw = (
        'level=WARN service=auth-service status=404 latency_ms=12.5 '
        'msg="not found" client_ip=1.2.3.4'
    )
    fields, status = parse(raw, datetime.now(UTC))
    assert status == ParseStatus.OK
    assert fields["level"] == "WARN"
    assert fields["status_code"] == 404
    assert fields["client_ip"] == "1.2.3.4"


def test_parse_apache_combined() -> None:
    raw = (
        '203.0.113.5 - - [30/Jul/2026:11:32:03 +0000] "GET /api/v1/orders HTTP/1.1" '
        '200 512 "-" "Mozilla/5.0"'
    )
    fields, status = parse(raw, datetime.now(UTC))
    assert status == ParseStatus.OK
    assert fields["status_code"] == 200
    assert fields["client_ip"] == "203.0.113.5"
    assert fields["level"] == "INFO"


def test_parse_plain() -> None:
    raw = (
        "2026-07-30 11:32:03,481 ERROR [payment-service] charge declined "
        "status=402 latency=812.0ms user=u-4471"
    )
    fields, status = parse(raw, datetime.now(UTC))
    assert status == ParseStatus.OK
    assert fields["level"] == "ERROR"
    assert fields["status_code"] == 402
    assert fields["latency_ms"] == 812.0
    assert fields["user_id"] == "u-4471"


def test_parse_never_raises_on_garbage() -> None:
    garbage_inputs = ["", "{", "not json at all !!! ###", "a" * 20_000, "\x00\x01\x02"]
    for raw in garbage_inputs:
        fields, status = parse(raw, datetime.now(UTC))
        assert isinstance(fields, dict)
        assert status in ParseStatus


def test_parse_caps_oversized_input() -> None:
    raw = "x" * (20 * 1024)
    fields, _ = parse(raw, datetime.now(UTC))
    assert len(fields["message"]) <= 8192


def test_scan_logfmt_quoted_values_with_spaces() -> None:
    kv = scan_logfmt('msg="hello world" status=200 empty= foo=bar')
    assert kv["msg"] == "hello world"
    assert kv["status"] == "200"
    assert kv["foo"] == "bar"


def test_scan_logfmt_escaped_quotes() -> None:
    kv = scan_logfmt(r'msg="say \"hi\"" level=INFO')
    assert kv["msg"] == 'say "hi"'
    assert kv["level"] == "INFO"
