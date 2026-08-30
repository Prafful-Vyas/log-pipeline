from __future__ import annotations

from prometheus_client import generate_latest

from common import metrics


def test_metrics_register_and_increment_without_error() -> None:
    metrics.producer_events_total.labels(service="svc", level="INFO").inc()
    metrics.consumer_events_total.labels(worker="0", service="svc").inc()
    metrics.consumer_parse_failures_total.labels(format="json").inc()
    metrics.consumer_dlq_total.labels(reason="envelope_decode_failed").inc()
    metrics.pipeline_e2e_latency_seconds.observe(0.05)
    metrics.parse_duration_seconds.labels(format="json").observe(0.0001)
    metrics.batch_size_records.observe(1000)
    metrics.batch_flush_duration_seconds.labels(outcome="success").observe(0.01)
    metrics.queue_depth.labels(worker="0").set(42)
    metrics.consumer_paused.labels(worker="0").set(1)
    metrics.consumer_lag_records.labels(topic="logs.raw", partition="0").set(10)
    metrics.security_alerts_total.labels(rule_id="SQLI_SIGNATURE", severity="5").inc()
    metrics.rebalances_total.inc()

    body = generate_latest().decode()
    assert "producer_events_total" in body
    assert "pipeline_e2e_latency_seconds" in body


def test_clear_multiprocess_dir_is_a_noop_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("PROMETHEUS_MULTIPROC_DIR", raising=False)
    metrics.clear_multiprocess_dir()  # must not raise


def test_mark_worker_dead_is_a_noop_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("PROMETHEUS_MULTIPROC_DIR", raising=False)
    metrics.mark_worker_dead(12345)  # must not raise
