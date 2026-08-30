-- 1-minute aggregate rollup, read by the log_analytics Grafana dashboard.
-- Refreshed by scripts/maintenance.py every 30s (recomputes the last 3
-- minutes) since vanilla postgres:16 has no pg_cron extension.

CREATE TABLE logs_1m_agg (
    bucket     timestamptz NOT NULL,
    service    varchar(64) NOT NULL,
    level      log_level   NOT NULL,
    events     bigint      NOT NULL,
    p50_lat    real,
    p95_lat    real,
    p99_lat    real,
    PRIMARY KEY (bucket, service, level)
);
CREATE INDEX agg_bucket ON logs_1m_agg (bucket DESC);
