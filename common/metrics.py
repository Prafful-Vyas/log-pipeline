from __future__ import annotations

import os
import threading

from prometheus_client import Counter, Gauge, Histogram

# ---------------------------------------------------------------- producer
producer_events_total = Counter(
    "producer_events_total", "Log events offered to the broker", ["service", "level"]
)
producer_send_errors_total = Counter(
    "producer_send_errors_total", "Producer send failures", ["reason"]
)

# ----------------------------------------------------------------- indexer
consumer_events_total = Counter(
    "consumer_events_total", "Log events consumed and parsed", ["worker", "service"]
)
consumer_parse_failures_total = Counter(
    "consumer_parse_failures_total", "Events that failed to parse", ["format"]
)
consumer_dlq_total = Counter(
    "consumer_dlq_total", "Events routed to the dead-letter queue", ["reason"]
)
pipeline_e2e_latency_seconds = Histogram(
    "pipeline_e2e_latency_seconds",
    "End-to-end latency: db flush time minus producer emitted_at",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
parse_duration_seconds = Histogram(
    "parse_duration_seconds", "Time spent in parser.parse()", ["format"],
    buckets=(0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05),
)
batch_size_records = Histogram(
    "batch_size_records", "Number of records per flushed batch",
    buckets=(1, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000),
)
batch_flush_duration_seconds = Histogram(
    "batch_flush_duration_seconds", "Time spent flushing a batch to Postgres", ["outcome"],
)
queue_depth = Gauge("queue_depth", "Bounded queue occupancy", ["worker"])
consumer_paused = Gauge("consumer_paused", "1 if partitions are paused for backpressure", ["worker"])
consumer_lag_records = Gauge(
    "consumer_lag_records", "end_offset - position per partition", ["topic", "partition"]
)
security_alerts_total = Counter(
    "security_alerts_total", "Security classifier hits", ["rule_id", "severity"]
)
rebalances_total = Counter("rebalances_total", "Consumer group rebalance events")


def start_metrics_server(port: int) -> None:
    """Single-process metrics endpoint (producer, alerter)."""
    from prometheus_client import start_http_server

    start_http_server(port)


def serve_multiprocess_metrics(port: int) -> None:
    """Aggregated metrics endpoint for the indexer supervisor.

    The indexer runs N worker *processes*; each writes its own metric deltas
    to files under PROMETHEUS_MULTIPROC_DIR (set via env before any worker
    imports this module). This serves one aggregated /metrics view over all
    of them, re-reading the directory on every scrape.

    No-ops (with a log line) if PROMETHEUS_MULTIPROC_DIR isn't set, so running
    the indexer ad hoc outside docker-compose (which always sets it) doesn't
    crash the supervisor over a missing metrics endpoint.
    """
    if not os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        import structlog

        structlog.get_logger("metrics").warning(
            "prometheus_multiproc_dir_unset", detail="indexer metrics endpoint not started"
        )
        return

    from wsgiref.simple_server import make_server

    from prometheus_client import CollectorRegistry, make_wsgi_app, multiprocess

    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    app = make_wsgi_app(registry)
    httpd = make_server("", port, app)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()


def clear_multiprocess_dir() -> None:
    """Remove stale per-process metric files on supervisor startup, as
    recommended by the prometheus_client multiprocess docs."""
    directory = os.environ.get("PROMETHEUS_MULTIPROC_DIR")
    if not directory or not os.path.isdir(directory):
        return
    for name in os.listdir(directory):
        if name.endswith(".db"):
            try:
                os.remove(os.path.join(directory, name))
            except OSError:
                pass


def mark_worker_dead(pid: int) -> None:
    """Clean up a dead worker process's metric files (multiprocess mode only)."""
    if not os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        return
    from prometheus_client import multiprocess

    multiprocess.mark_process_dead(pid)
