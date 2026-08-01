-- Core schema for the log pipeline.
-- Applied automatically on first container start via docker-entrypoint-initdb.d.

CREATE TYPE log_level AS ENUM ('DEBUG','INFO','WARN','ERROR','FATAL');
CREATE TYPE parse_status AS ENUM ('ok','partial','failed');

-- ---------------------------------------------------------------- logs
CREATE TABLE logs (
    event_id      uuid          NOT NULL,
    ts            timestamptz   NOT NULL,
    ingested_at   timestamptz   NOT NULL DEFAULT now(),
    emitted_at    timestamptz   NOT NULL,
    service       varchar(64)   NOT NULL,
    host          varchar(64)   NOT NULL,
    level         log_level     NOT NULL,
    message       text          NOT NULL,
    status_code   smallint,
    latency_ms    real,
    trace_id      varchar(64),
    user_id       varchar(64),
    client_ip     inet,
    is_security   boolean       NOT NULL DEFAULT false,
    security_rule varchar(48),
    attrs         jsonb         NOT NULL DEFAULT '{}'::jsonb,
    parse_status  parse_status  NOT NULL DEFAULT 'ok',
    PRIMARY KEY (event_id, ts)          -- partition key must be in PK
) PARTITION BY RANGE (ts);

-- Indexes are declared on the parent and propagate to every partition.
CREATE INDEX logs_ts_brin   ON logs USING brin (ts) WITH (pages_per_range = 32);
CREATE INDEX logs_svc_ts    ON logs (service, ts DESC);
CREATE INDEX logs_level_ts  ON logs (level, ts DESC) WHERE level IN ('ERROR','FATAL');
CREATE INDEX logs_trace     ON logs (trace_id) WHERE trace_id IS NOT NULL;
CREATE INDEX logs_attrs_gin ON logs USING gin (attrs jsonb_path_ops);

ALTER TABLE logs SET (
    autovacuum_vacuum_scale_factor  = 0.02,
    autovacuum_analyze_scale_factor = 0.01,
    fillfactor = 100
);

-- --------------------------------------------------- security_alerts
CREATE TABLE security_alerts (
    alert_id    bigserial PRIMARY KEY,
    event_id    uuid        NOT NULL,
    ts          timestamptz NOT NULL,
    rule_id     varchar(48) NOT NULL,
    severity    smallint    NOT NULL CHECK (severity BETWEEN 1 AND 5),
    service     varchar(64) NOT NULL,
    client_ip   inet,
    user_id     varchar(64),
    detail      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (event_id, rule_id)
);
CREATE INDEX sec_ts      ON security_alerts (ts DESC);
CREATE INDEX sec_rule_ts ON security_alerts (rule_id, ts DESC);
CREATE INDEX sec_ip_ts   ON security_alerts (client_ip, ts DESC);

-- ------------------------------------------------------ dead_letters
CREATE TABLE dead_letters (
    id           bigserial PRIMARY KEY,
    seen_at      timestamptz NOT NULL DEFAULT now(),
    topic        text NOT NULL,
    partition    int  NOT NULL,
    kafka_offset bigint NOT NULL,
    payload      bytea NOT NULL,
    error        text  NOT NULL,
    UNIQUE (topic, partition, kafka_offset)
);

-- ---------------------------------------------------- partition setup
-- Creates one partition per day. Called at init time for a window around
-- "today" and re-callable (e.g. from a cron sidecar) to roll forward.
CREATE OR REPLACE PROCEDURE ensure_log_partitions(days_behind int DEFAULT 2, days_ahead int DEFAULT 5)
LANGUAGE plpgsql AS $$
DECLARE d date; part text;
BEGIN
  FOR i IN -days_behind..days_ahead LOOP
    d := (current_date + i);
    part := format('logs_%s', to_char(d,'YYYY_MM_DD'));
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part) THEN
      EXECUTE format(
        'CREATE TABLE %I PARTITION OF logs FOR VALUES FROM (%L) TO (%L)',
        part, d, d + 1);
    END IF;
  END LOOP;
END $$;

CREATE OR REPLACE PROCEDURE drop_old_log_partitions(keep_days int DEFAULT 7)
LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT c.relname FROM pg_class c
           JOIN pg_inherits i ON i.inhrelid = c.oid
           JOIN pg_class p ON p.oid = i.inhparent
           WHERE p.relname = 'logs'
             AND c.relname < format('logs_%s',
                 to_char(current_date - keep_days,'YYYY_MM_DD'))
  LOOP
    EXECUTE format('DROP TABLE %I', r.relname);
  END LOOP;
END $$;

CALL ensure_log_partitions();
