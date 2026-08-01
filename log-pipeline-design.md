# Real-Time Log Ingestion & Analytics Pipeline
## Design Document (HLD + LLD)

| Field | Value |
|---|---|
| Version | 1.0 |
| Status | Draft for implementation |
| Owner | *(you)* |
| Target stack | Python 3.12 (asyncio), Redpanda/Kafka, PostgreSQL 16, Prometheus, Grafana, Docker Compose |
| Target scale | 5,000+ events/sec sustained, p99 end-to-end < 1s |

---

# 1. Overview

## 1.1 Problem statement

Distributed applications emit high-volume, unstructured or semi-structured log
streams. Naive approaches (writing logs directly to a database, or synchronous
HTTP shipping) fail in three ways:

1. **Coupling** — if the datastore is slow or down, the application blocks or drops logs.
2. **Write amplification** — one INSERT per log line destroys database throughput.
3. **No real-time visibility** — batch ETL means error spikes are discovered minutes or hours late.

We need a pipeline that decouples producers from storage via a durable log
(Kafka), applies parsing/filtering/enrichment in-flight, writes to the analytics
store in batches, and surfaces error rates and throughput in near real time —
**without dropping messages** under load or during downstream outages.

## 1.2 Goals

| # | Goal |
|---|---|
| G1 | Ingest ≥ 5,000 log events/sec sustained on a single laptop-class machine |
| G2 | p99 end-to-end latency (producer emit → queryable in Postgres) < 1 second |
| G3 | Zero message loss under consumer crash, DB outage, or broker restart (at-least-once with idempotent writes ⇒ effectively exactly-once at rest) |
| G4 | Real-time detection and routing of security-relevant events to a separate alert stream and table |
| G5 | Grafana dashboards for error rate, throughput, latency, consumer lag, and top offending services |
| G6 | Measurable proof of the batching optimisation (≥ 60% reduction in write overhead vs. row-at-a-time) |
| G7 | One-command local bring-up (`docker compose up`) |

## 1.3 Non-goals

- Multi-tenant auth / RBAC on the query layer.
- Full-text search over log bodies at Elasticsearch quality (Postgres GIN/trigram is "good enough"; ES is documented as an optional sink in §11).
- Log *collection* from real hosts (no Filebeat/Fluentbit agent); the producer is a synthetic simulator by design.
- Long-term cold storage / S3 tiering (retention is a partition-drop policy).
- Cross-region replication.

## 1.4 Key design decisions (summary)

| Decision | Choice | Rationale | Alternative rejected |
|---|---|---|---|
| Broker | Redpanda (Kafka API) | Single binary, no ZooKeeper/KRaft config, ~40% less memory for local dev; wire-compatible so code is unchanged for real Kafka | Apache Kafka (heavier for local dev), Redis Streams (weaker durability) |
| Concurrency model | **asyncio for I/O, multiprocessing for parallelism** | Parsing is CPU-bound and the GIL serialises it; one consumer *process* per partition group scales linearly across cores | Thread pool (GIL-bound on regex parsing), pure asyncio single process (CPU ceiling ~1.5k eps) |
| Storage | PostgreSQL 16, declaratively range-partitioned by day | Rich SQL for Grafana, cheap retention via `DETACH/DROP PARTITION`, JSONB for variable fields | Elasticsearch as primary (heavier ops, weaker joins); documented as optional secondary sink |
| Write path | `asyncpg` + `COPY ... FROM STDIN` binary | 3–6× faster than `executemany`, ~2× faster than multi-row `INSERT` | `psycopg2.executemany` (slowest), ORM (unusable at this rate) |
| Delivery semantics | At-least-once + idempotent upsert on `event_id` | Simple, crash-safe; duplicates collapse at the sink | Kafka transactions/EOS (adds coordinator overhead, unnecessary since sink is idempotent) |
| Offset commit | Manual, **after** DB commit | Guarantees G3 | Auto-commit (silently loses in-flight batch on crash) |
| Backpressure | Bounded `asyncio.Queue` + `consumer.pause()` on partitions | Bounded memory, no OOM under sink slowness | Unbounded queue (OOM), dropping (violates G3) |

---

# 2. High-Level Design

## 2.1 Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              PRODUCER TIER                                   │
│                                                                              │
│   ┌────────────────────────────────────────────────────────────┐             │
│   │  log_producer  (asyncio, N virtual services)               │             │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │             │
│   │  │auth-svc  │  │payment   │  │gateway   │  │search    │ …  │             │
│   │  │emitter   │  │emitter   │  │emitter   │  │emitter   │    │             │
│   │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘    │             │
│   │       └─────────────┴──────┬──────┴─────────────┘          │             │
│   │                   token-bucket rate limiter                │             │
│   │                   aiokafka AIOKafkaProducer                │             │
│   │                   (lz4, linger=20ms, acks=1)               │             │
│   └───────────────────────────┬────────────────────────────────┘             │
└───────────────────────────────┼──────────────────────────────────────────────┘
                                │  key = service_name
                                ▼
        ┌───────────────────────────────────────────────────────┐
        │           REDPANDA / KAFKA                            │
        │  topic: logs.raw        (12 partitions, RF=1 local)   │
        │  topic: logs.security   (3 partitions)                │
        │  topic: logs.dlq        (1 partition)                 │
        │  retention: 6h (raw) / 7d (security)                  │
        └───────────┬───────────────────────────────┬───────────┘
                    │ consumer-group: log-indexer   │
     ┌──────────────┼───────────────┬───────────────┼──────────────┐
     ▼              ▼               ▼               ▼              ▼
 ┌────────┐    ┌────────┐      ┌────────┐      ┌────────┐    (alerts consumer)
 │worker 0│    │worker 1│      │worker 2│      │worker 3│
 │ proc   │    │ proc   │      │ proc   │      │ proc   │
 │┌──────┐│    └────────┘      └────────┘      └────────┘
 ││fetch ││   each process: asyncio event loop
 ││ loop ││   ┌──────────────────────────────────────────┐
 │└──┬───┘│   │ fetch → decode → parse → validate →      │
 │   ▼    │   │ enrich → classify (security?) →          │
 │┌──────┐│   │ bounded Queue(maxsize=20k) →             │
 ││parse ││   │ Batcher(size=1000 | 250ms) →             │
 ││ pool ││   │ asyncpg COPY → commit → commit offsets   │
 │└──┬───┘│   └──────────────────────────────────────────┘
 │   ▼    │
 │┌──────┐│
 ││batch ││
 │└──┬───┘│
 └───┼────┘
     │
     ▼
┌──────────────────────────────────────────────────────────────┐
│  PostgreSQL 16                                               │
│   logs           (partitioned by day, BRIN on ts)            │
│   security_alerts                                            │
│   logs_1m_agg    (continuous rollup via cron/pg_cron)        │
│   dead_letters                                               │
└───────────────┬──────────────────────────────────────────────┘
                │
    ┌───────────┴────────────┐
    ▼                        ▼
┌─────────────┐      ┌────────────────┐
│  Grafana    │◄─────┤   Prometheus   │◄── /metrics from producer,
│ dashboards  │      │  (pipeline     │    consumers, redpanda,
└─────────────┘      │   telemetry)   │    postgres_exporter
                     └────────────────┘
```

## 2.2 Component responsibilities

| Component | Responsibility | Scaling axis |
|---|---|---|
| **Producer** (`services/producer`) | Generate realistic multi-format log lines for N simulated microservices at a configurable rate; inject error bursts and attack patterns; publish to `logs.raw` | Increase `--rate`; run multiple instances |
| **Broker** (Redpanda) | Durable, replayable, partitioned buffer; absorbs downstream outages | Partitions on `logs.raw` |
| **Consumer / Indexer** (`services/indexer`) | Parse, validate, enrich, classify, batch, persist, commit offsets | Processes ≤ partition count |
| **Alert consumer** (`services/alerter`) | Independent consumer group on `logs.security`; dedup + notify (webhook/stdout) | Separate group, no impact on indexing |
| **PostgreSQL** | Queryable store, retention, rollups | Partitions, indexes |
| **Prometheus** | Scrape pipeline counters/histograms | — |
| **Grafana** | Visualise error rate, throughput, latency, lag | — |

## 2.3 Data flow (happy path)

1. Emitter coroutine builds a `LogEvent` and serialises it to a **raw log line** (the producer deliberately emits *unstructured text*, so the consumer has real parsing work to do). A JSON envelope carries `event_id`, `emitted_at`, and `format`.
2. Producer publishes with `key = service_name` → guarantees per-service ordering and even partition spread.
3. Broker persists; consumer group `log-indexer` assigns partitions to worker processes via cooperative-sticky rebalancing.
4. Each worker fetches up to `max_poll_records` messages, decodes, and runs the parse chain.
5. Parsed records enter a bounded queue; the **Batcher** flushes on `size ≥ 1000` OR `age ≥ 250 ms`, whichever first.
6. Flush = single `COPY` into a staging temp table, then `INSERT … SELECT … ON CONFLICT (event_id, ts) DO NOTHING` into `logs`. One transaction per batch.
7. On DB commit success → `consumer.commit()` for the highest offset per partition in the batch.
8. Security-classified events are additionally produced to `logs.security` **before** the offset commit (so an alert is never lost).
9. Metrics incremented at every stage; Grafana queries Postgres for content metrics and Prometheus for pipeline metrics.

## 2.4 Delivery semantics & correctness

- **Broker → consumer:** at-least-once. Offsets commit only after durable write.
- **Duplicates:** possible on crash between DB commit and offset commit. Collapsed by `ON CONFLICT (event_id, ts) DO NOTHING`; `event_id` is a UUIDv7 generated at the producer, so duplicates are byte-identical.
- **Ordering:** per `(service_name)` key within a partition. Cross-service ordering is not guaranteed and not required — all queries are time-window based on `ts`.
- **Poison messages:** any record failing parse/validate after N retries goes to `logs.dlq` and the `dead_letters` table, then the offset advances. A permanently stuck message must never block a partition.

## 2.5 Capacity model (sizing the 5,000 eps target)

Assumptions: average serialised event ≈ 420 bytes.

| Quantity | Value | Derivation |
|---|---|---|
| Ingress bandwidth | ~2.1 MB/s | 5,000 × 420 B |
| Broker disk write (lz4 ~3.5:1) | ~0.6 MB/s | compressed |
| 6h raw retention on disk | ~13 GB uncompressed / ~3.7 GB on disk | 2.1 MB/s × 21,600 s |
| Postgres row width (incl. JSONB + TOAST-free) | ~500 B | measured via `pg_column_size` |
| Postgres ingest rate | ~2.5 MB/s → ~215 GB/day | drives the 1-day partition + 7-day retention default |
| Batches/sec at size=1000 | 5 | ⇒ 5 transactions/sec instead of 5,000 |
| Parse cost (measured target) | ≤ 120 µs/event | ⇒ 1 core ≈ 8,300 eps; 4 workers ≈ 33k eps headroom |
| Partitions needed | 12 | 5,000 eps / ~2,000 eps-per-partition safe ceiling, ×1.2 headroom for future scale-out |

**Bottleneck ranking (expected):** ① Python parsing (CPU) → ② Postgres index maintenance on write → ③ broker fsync → ④ network. This ordering justifies multiprocessing first, then BRIN-over-BTREE index choices.

## 2.6 Failure modes

| Failure | Detection | Behaviour | Recovery |
|---|---|---|---|
| Postgres down / slow | asyncpg exception, flush latency histogram spike | Batcher retries with exponential backoff + jitter (max 60s); queue fills; consumer `pause()`es partitions; **nothing committed, nothing lost** | Auto-resume on reconnect; broker replays from last committed offset |
| Consumer process crash | Group heartbeat timeout (`session.timeout.ms=45s`) | Partitions reassigned to surviving workers | Uncommitted messages redelivered; idempotent upsert collapses dupes |
| Broker restart | Producer/consumer reconnect errors | Producer buffers in memory up to `buffer_memory`, then blocks (never drops); consumer retries fetch | Automatic |
| Rebalance storm | Rebalance counter metric | Cooperative-sticky assignor avoids stop-the-world; in-flight batch flushed in `on_partitions_revoked` before releasing | — |
| Poison / unparseable message | Parse exception | → DLQ topic + `dead_letters` table, offset advances | Manual replay tool `scripts/replay_dlq.py` |
| Disk full (broker) | Redpanda metric | Retention policy (`retention.ms=6h`, `retention.bytes` cap) evicts oldest | Alert at 80% |
| Producer outpaces pipeline | Consumer lag metric rising | Lag grows (this is *correct* — the broker is the buffer); alert at lag > 100k or > 60s | Scale consumers up to partition count |
| Clock skew between producer and consumer | Negative latency samples | Latency computed from `emitted_at`; clamp at 0 and count `clock_skew_events_total` | NTP; document limitation |

## 2.7 Security considerations

- Producer/consumer credentials via env only; no secrets in the repo (`.env.example` committed, `.env` gitignored).
- Log bodies are treated as untrusted input: parsing uses **anchored, bounded regexes** with no catastrophic backtracking (see §3.4 ReDoS note), and a hard 16 KB message size cap.
- PII redaction hook (`enrichers/redact.py`) masks emails, bearer tokens, and card-like digit runs before persistence.
- The "security alert" classifier is heuristic and explicitly documented as such — it is a demo detection layer (brute-force auth failures, SQLi/XSS signatures, privilege-escalation keywords, anomalous 4xx/5xx bursts), not a production SIEM.

---

# 3. Low-Level Design

## 3.1 Repository layout

```
log-pipeline/
├── docker-compose.yml
├── Makefile                      # up, down, bench, migrate, lint, test
├── .env.example
├── pyproject.toml                # uv/poetry; ruff + mypy config
├── sql/
│   ├── 001_schema.sql            # tables, partitions, indexes
│   ├── 002_rollups.sql           # 1-minute aggregate + refresh fn
│   └── 003_maintenance.sql       # partition create/drop procs
├── common/
│   ├── __init__.py
│   ├── config.py                 # pydantic-settings, all env vars
│   ├── models.py                 # LogEvent, ParsedLog, SecurityAlert (pydantic)
│   ├── serde.py                  # msgpack/json encode+decode
│   ├── metrics.py                # prometheus_client registry + helpers
│   └── logging.py                # structlog setup (pipeline's own logs)
├── services/
│   ├── producer/
│   │   ├── __main__.py
│   │   ├── generator.py          # synthetic log templates per service
│   │   ├── scenarios.py          # error burst / attack injection
│   │   └── rate_limiter.py       # token bucket
│   ├── indexer/
│   │   ├── __main__.py           # supervisor: spawns N worker procs
│   │   ├── worker.py             # per-process asyncio pipeline
│   │   ├── parser.py             # format detection + parse chain
│   │   ├── enrich.py             # geo/env/redaction enrichers
│   │   ├── classifier.py         # security rules
│   │   ├── batcher.py            # size/time batching
│   │   └── sink_pg.py            # asyncpg COPY writer
│   ├── alerter/
│   │   └── __main__.py
│   └── sink_es/                  # OPTIONAL secondary sink (§11)
│       └── __main__.py
├── bench/
│   ├── bench_write_strategies.py # the 60%-reduction experiment
│   ├── bench_parser.py
│   └── load_test.py              # end-to-end latency harness
├── grafana/
│   ├── provisioning/{datasources,dashboards}/
│   └── dashboards/{pipeline_health.json,log_analytics.json}
├── prometheus/prometheus.yml
└── tests/
    ├── unit/  integration/  load/
```

## 3.2 Canonical data model

### 3.2.1 Wire format (producer → Kafka)

The Kafka **value** is a compact JSON envelope. The `raw` field holds the
unstructured line the consumer must parse — this is what makes the parsing
stage non-trivial.

```json
{
  "event_id":   "018f3c2a-7d11-7b3e-9a44-6f2c1d3e4b5a",
  "emitted_at": 1738245123.481293,
  "service":    "payment-service",
  "host":       "pay-7f9c4-xk2",
  "format":     "syslog",
  "raw":        "<134>1 2026-07-30T11:32:03.481Z pay-7f9c4-xk2 payment-service 4412 ID47 - level=ERROR trace_id=8f2c1a request_id=r-88213 msg=\"charge declined\" status=402 latency_ms=812 user_id=u-4471"
}
```

Kafka **key** = `service` (UTF-8 bytes). Headers: `schema_version=1`,
`producer_id`.

Supported `format` values the parser must handle:
`json` | `syslog` (RFC5424) | `logfmt` | `apache_combined` | `plain`

### 3.2.2 `ParsedLog` (internal, pydantic v2)

| Field | Type | Notes |
|---|---|---|
| `event_id` | `UUID` | idempotency key |
| `ts` | `datetime` (UTC, tz-aware) | log's own timestamp; partition key |
| `ingested_at` | `datetime` | set at parse time |
| `emitted_at` | `datetime` | from envelope, for latency measurement |
| `service` | `str(64)` | |
| `host` | `str(64)` | |
| `level` | `enum` | DEBUG/INFO/WARN/ERROR/FATAL — normalised from 12+ spellings |
| `message` | `str` | truncated to 8 KB |
| `status_code` | `int \| None` | HTTP status if present |
| `latency_ms` | `float \| None` | |
| `trace_id` | `str \| None` | |
| `user_id` | `str \| None` | |
| `client_ip` | `IPv4Address \| None` | |
| `is_security` | `bool` | classifier output |
| `security_rule` | `str \| None` | rule id that fired |
| `attrs` | `dict` | everything else → JSONB |
| `parse_status` | `enum` | `ok` / `partial` / `failed` |

## 3.3 PostgreSQL schema (`sql/001_schema.sql`)

```sql
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

-- Daily partitions, created ahead of time by maintenance job.
CREATE TABLE logs_2026_07_30 PARTITION OF logs
    FOR VALUES FROM ('2026-07-30') TO ('2026-07-31');

-- Indexes are created PER PARTITION (declared on parent → propagates).
CREATE INDEX logs_ts_brin      ON logs USING brin (ts) WITH (pages_per_range = 32);
CREATE INDEX logs_svc_ts       ON logs (service, ts DESC);
CREATE INDEX logs_level_ts     ON logs (level, ts DESC) WHERE level IN ('ERROR','FATAL');
CREATE INDEX logs_trace        ON logs (trace_id) WHERE trace_id IS NOT NULL;
CREATE INDEX logs_attrs_gin    ON logs USING gin (attrs jsonb_path_ops);

ALTER TABLE logs SET (
    autovacuum_vacuum_scale_factor  = 0.02,
    autovacuum_analyze_scale_factor = 0.01,
    fillfactor = 100          -- append-only, no HOT updates expected
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
CREATE INDEX sec_ts        ON security_alerts (ts DESC);
CREATE INDEX sec_rule_ts   ON security_alerts (rule_id, ts DESC);
CREATE INDEX sec_ip_ts     ON security_alerts (client_ip, ts DESC);

-- ------------------------------------------------------ dead_letters
CREATE TABLE dead_letters (
    id          bigserial PRIMARY KEY,
    seen_at     timestamptz NOT NULL DEFAULT now(),
    topic       text NOT NULL,
    partition   int  NOT NULL,
    kafka_offset bigint NOT NULL,
    payload     bytea NOT NULL,
    error       text  NOT NULL,
    UNIQUE (topic, partition, kafka_offset)
);

-- ------------------------------------------------------- 1-min rollup
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
```

**Index rationale.** `logs` is append-only and time-ordered, so a BRIN index on
`ts` costs ~a few KB per partition versus hundreds of MB for a B-tree, and gives
near-identical pruning for range scans — this is a large part of the write-cost
reduction. B-trees are kept only where selective point lookups matter
(`service`, `trace_id`) and a **partial** index on `ERROR/FATAL` keeps the
error-rate dashboard fast while indexing only ~3% of rows.

### 3.3.1 Partition maintenance (`sql/003_maintenance.sql`)

```sql
CREATE OR REPLACE PROCEDURE ensure_log_partitions(days_ahead int DEFAULT 3)
LANGUAGE plpgsql AS $$
DECLARE d date; part text;
BEGIN
  FOR i IN 0..days_ahead LOOP
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
    EXECUTE format('DROP TABLE %I', r.relname);   -- O(1) retention
  END LOOP;
END $$;
```

Scheduled hourly with `pg_cron` (or a sidecar container running `psql` on a
timer if `pg_cron` is unavailable).

## 3.4 Producer LLD

### 3.4.1 Rate limiting

Token bucket, refilled from a monotonic clock — accurate at 5k+/s where
`asyncio.sleep(1/rate)` per event is not (sleep granularity ≈ 1–15 ms).

```python
class TokenBucket:
    def __init__(self, rate: float, burst: float | None = None):
        self._rate = rate
        self._capacity = burst or max(rate * 0.1, 100)
        self._tokens = self._capacity
        self._last = time.monotonic()

    async def acquire(self, n: int = 1) -> None:
        while True:
            now = time.monotonic()
            self._tokens = min(self._capacity,
                               self._tokens + (now - self._last) * self._rate)
            self._last = now
            if self._tokens >= n:
                self._tokens -= n
                return
            await asyncio.sleep((n - self._tokens) / self._rate)
```

Events are acquired in **chunks of 50** (`acquire(50)` then emit 50) to keep
loop overhead below ~2% of CPU at 5k eps.

### 3.4.2 Generator

`generator.py` holds a per-service profile:

```python
@dataclass(frozen=True)
class ServiceProfile:
    name: str
    formats: tuple[str, ...]          # e.g. ("json","logfmt")
    level_weights: dict[str, float]   # baseline INFO 0.88 / WARN .07 / ERROR .045 / FATAL .005
    endpoints: tuple[str, ...]
    latency_dist: tuple[float, float] # lognormal (mu, sigma)
    error_burst_prob: float
```

Eight profiles ship by default: `api-gateway`, `auth-service`,
`payment-service`, `order-service`, `inventory-service`, `search-service`,
`notification-service`, `cron-worker`.

`scenarios.py` injects, on a schedule, realistic anomalies so the dashboards
have something to show:

| Scenario | Effect | Trigger |
|---|---|---|
| `error_burst` | ERROR ratio for one service → 40% for 30–90 s | every ~5 min, random service |
| `latency_spike` | latency dist ×8 | every ~7 min |
| `brute_force` | 200–2,000 `auth failed` from one IP in 60 s | every ~10 min |
| `sqli_probe` | query strings containing injection signatures | random, ~1/min |
| `cascade` | gateway 5xx correlated with a downstream service's errors | every ~15 min |
| `silence` | one service stops logging (tests "missing heartbeat" panel) | every ~20 min |

### 3.4.3 Kafka producer settings

```python
AIOKafkaProducer(
    bootstrap_servers=cfg.brokers,
    acks=1,                     # leader ack; RF=1 locally anyway
    compression_type="lz4",     # ~3.5:1 on log text, ~4x cheaper CPU than gzip
    linger_ms=20,               # batch aggressively; 20ms of the <1s budget
    max_batch_size=262_144,     # 256 KB
    max_request_size=2_097_152,
    request_timeout_ms=15_000,
    enable_idempotence=False,   # duplicates are handled at the sink
)
```

`await producer.send()` (not `send_and_wait`) — fire into the accumulator and
never await per message; a background task drains and counts errors. Backpressure
comes from the producer's own buffer becoming full, which blocks `send()` — this
is the desired "slow down, never drop" behaviour.

### 3.4.4 CLI

```
python -m services.producer --rate 5000 --services 8 --duration 600 \
       --scenarios all --topic logs.raw --metrics-port 9101
```

## 3.5 Consumer / Indexer LLD

### 3.5.1 Process supervision

`__main__.py` is a supervisor that forks `WORKERS` (default `min(cpu_count, 4)`)
child processes with `multiprocessing.spawn`. All children join the same
consumer group; Kafka handles partition assignment. The supervisor:

- restarts a child that exits non-zero (exponential backoff, max 5 in 60 s then abort),
- forwards SIGTERM/SIGINT for graceful drain,
- exposes an aggregated `/metrics` via `prometheus_client`'s multiprocess mode
  (`PROMETHEUS_MULTIPROC_DIR`).

**Why processes, not threads:** the parse chain is regex/CPU work; the GIL caps
a threaded design at ~1 core. Measured in `bench/bench_parser.py`, a single
process tops out near 8k eps parsing; 4 processes scale to ~30k, leaving
comfortable headroom above the 5k target.

### 3.5.2 Per-worker pipeline

```python
async def run_worker(worker_id: int) -> None:
    consumer = AIOKafkaConsumer(
        cfg.topic_raw,
        bootstrap_servers=cfg.brokers,
        group_id="log-indexer",
        enable_auto_commit=False,               # G3
        auto_offset_reset="latest",
        max_poll_records=1000,
        fetch_max_bytes=8 * 1024 * 1024,
        fetch_max_wait_ms=100,
        session_timeout_ms=45_000,
        heartbeat_interval_ms=3_000,
        max_poll_interval_ms=300_000,
        partition_assignment_strategy=[CooperativeStickyAssignor],
    )
    pool   = await asyncpg.create_pool(cfg.dsn, min_size=2, max_size=6,
                                       command_timeout=30)
    sink   = PgSink(pool)
    batcher = Batcher(max_records=cfg.batch_size,      # 1000
                      max_age_ms=cfg.batch_max_age_ms, # 250
                      flush=sink.write_batch)
    queue: asyncio.Queue[ParsedLog] = asyncio.Queue(maxsize=cfg.queue_max) # 20_000

    await consumer.start()
    consumer.subscribe(listener=RebalanceListener(batcher, consumer))

    async with asyncio.TaskGroup() as tg:
        tg.create_task(fetch_loop(consumer, queue, batcher))
        tg.create_task(batcher.run(queue, consumer))
        tg.create_task(metrics_loop())
```

**`fetch_loop`** — decode + parse + classify, then `await queue.put(...)`. The
`await` on a full queue *is* the backpressure signal; when the queue has been
full for > 500 ms the loop calls `consumer.pause(*assignment)` and resumes when
occupancy drops below 50%. Pausing (rather than just letting the loop block) keeps
heartbeats flowing so the worker is not evicted from the group during a long
downstream stall.

**Offset tracking.** The batcher accumulates `offsets: dict[TopicPartition, int]`
= max offset seen per partition in the current batch. On successful DB commit it
calls `consumer.commit({tp: OffsetAndMetadata(off + 1, "")})`. On failure it
retries the *same* batch; offsets never advance past unwritten data.

### 3.5.3 Parser (`parser.py`)

Chain-of-responsibility with cheap format detection first:

```python
PARSERS: list[tuple[Callable[[str], bool], Callable[[str], dict]]] = [
    (lambda s: s.startswith("{"),      parse_json),
    (lambda s: s.startswith("<"),      parse_syslog5424),
    (RE_APACHE_HEAD.match,             parse_apache_combined),
    (lambda s: "=" in s[:40],          parse_logfmt),
    (lambda s: True,                   parse_plain),   # terminal fallback
]

def parse(raw: str) -> tuple[dict, ParseStatus]:
    for detect, fn in PARSERS:
        if detect(raw):
            try:
                return fn(raw), ParseStatus.OK
            except ParseError:
                continue
    return {"message": raw[:8192]}, ParseStatus.FAILED
```

Implementation rules:

- All regexes are **module-level, pre-compiled, anchored**, and use possessive-style
  patterns (no nested quantifiers) to avoid ReDoS on adversarial input. Every
  pattern is fuzz-tested in `tests/unit/test_parser_fuzz.py` with a 50 ms budget
  per input via `hypothesis`.
- `parse_logfmt` is a hand-written character scanner, not a regex — it is the
  hottest path and the scanner benchmarks ~3× faster.
- Level normalisation table maps `warn|warning|W|30` → `WARN`, etc.
- Timestamps: try `datetime.fromisoformat` first (C-implemented, fast), fall back
  to a small table of `strptime` formats, finally `ingested_at`.
- Unknown key/values land in `attrs`; the field whitelist prevents JSONB blowup
  (cap 64 keys, 4 KB serialised).

### 3.5.4 Security classifier (`classifier.py`)

Stateless signature rules + small stateful windowed rules held in per-worker
TTL dicts. Each rule returns `SecurityHit(rule_id, severity, detail) | None`.

| Rule ID | Type | Logic | Severity |
|---|---|---|---|
| `AUTH_BRUTE_FORCE` | stateful | ≥ 20 auth failures from one `client_ip` in 60 s (sliding window counter) | 4 |
| `AUTH_SPRAY` | stateful | one IP, ≥ 10 distinct `user_id` failures in 300 s | 4 |
| `SQLI_SIGNATURE` | regex | `union\s+select`, `' or '1'='1`, `; drop table`, `/*!` in message/URL | 5 |
| `XSS_SIGNATURE` | regex | `<script`, `javascript:`, `onerror=` | 3 |
| `PATH_TRAVERSAL` | regex | `../../`, `%2e%2e%2f` | 4 |
| `PRIV_ESCALATION` | keyword | `sudo`, `role=admin`, `permission granted` on non-admin service | 3 |
| `TOKEN_LEAK` | regex | bearer/JWT/AWS-key shaped strings in a log body | 5 |
| `SERVER_ERROR_BURST` | stateful | ≥ 50 5xx for one service in 30 s | 2 |

Stateful windows use a fixed-size ring of 1-second buckets (`collections.deque`,
maxlen=window_seconds) keyed by IP/service, with a janitor coroutine evicting
keys idle > 10 min — bounded memory regardless of cardinality (hard cap 100k keys,
LRU eviction beyond).

**Important scoping note:** classifier state is per-worker-process, so a
brute-force spread across partitions could be diluted. Mitigated by keying the
Kafka message on `service` *and* documenting that IP-scoped rules are
approximate; the production answer (out of scope here) is a dedicated stateful
consumer with `client_ip` as the partition key, which §12 lists as a stretch goal.

Hits are (a) stamped onto the `ParsedLog`, (b) inserted into `security_alerts` in
the same transaction as the batch, and (c) produced to `logs.security`.

### 3.5.5 Batcher (`batcher.py`)

```python
class Batcher:
    def __init__(self, max_records, max_age_ms, flush):
        self._buf: list[ParsedLog] = []
        self._alerts: list[SecurityAlert] = []
        self._offsets: dict[TopicPartition, int] = {}
        self._deadline: float | None = None

    async def run(self, queue, consumer):
        while True:
            timeout = None if self._deadline is None else \
                      max(0.0, self._deadline - time.monotonic())
            try:
                rec = await asyncio.wait_for(queue.get(), timeout)
                self._add(rec)
            except TimeoutError:
                pass                                  # age-based flush
            if self._should_flush():
                await self._flush(consumer)
```

Flush triggers: `len(buf) >= 1000` **or** `age >= 250 ms` **or** rebalance/shutdown.
Retry policy on flush failure: 5 attempts, `0.25 · 2ⁿ` seconds + full jitter,
capped at 60 s; after that the worker exits non-zero and the supervisor restarts
it (offsets uncommitted ⇒ safe).

**Tuning note.** `batch=1000 / 250 ms` is chosen so that at the 5,000 eps target
the size trigger fires first (200 ms of data), keeping the batching contribution
to end-to-end latency ≈ 200 ms and leaving ~800 ms of the 1 s p99 budget for
producer linger (20 ms), broker, parse, and DB write. At low traffic the age
trigger bounds staleness at 250 ms.

### 3.5.6 Postgres sink (`sink_pg.py`)

```python
COLUMNS = ("event_id","ts","ingested_at","emitted_at","service","host",
           "level","message","status_code","latency_ms","trace_id",
           "user_id","client_ip","is_security","security_rule","attrs",
           "parse_status")

async def write_batch(self, rows, alerts) -> None:
    async with self._pool.acquire() as con, con.transaction():
        await con.execute(
            "CREATE TEMP TABLE _stage (LIKE logs INCLUDING DEFAULTS) "
            "ON COMMIT DROP")
        await con.copy_records_to_table("_stage", records=rows, columns=COLUMNS)
        await con.execute(
            "INSERT INTO logs SELECT * FROM _stage "
            "ON CONFLICT (event_id, ts) DO NOTHING")
        if alerts:
            await con.executemany(
                "INSERT INTO security_alerts "
                "(event_id, ts, rule_id, severity, service, client_ip, user_id, detail) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) "
                "ON CONFLICT (event_id, rule_id) DO NOTHING", alerts)
```

Why the temp-table staging step: `COPY` is the fastest path into Postgres but has
no `ON CONFLICT`. Staging into an unlogged temp table then `INSERT … SELECT …
ON CONFLICT DO NOTHING` keeps COPY speed *and* idempotency. Measured overhead of
the extra hop is ~8% versus raw COPY — worth it for G3.

Connection pool is deliberately small (`max_size=6` per worker): more concurrent
writers than that increases lock and WAL contention without raising throughput.

### 3.5.7 Postgres server tuning (documented in `docker-compose.yml`)

| Parameter | Value | Reason |
|---|---|---|
| `shared_buffers` | 2GB | working set of recent partitions |
| `max_wal_size` | 8GB | avoid checkpoint storms under sustained COPY |
| `checkpoint_timeout` | 15min | " |
| `checkpoint_completion_target` | 0.9 | spread checkpoint I/O |
| `synchronous_commit` | `off` | **documented trade-off:** up to ~200 ms of committed logs at risk on OS crash; broker replay covers it, and it is worth ~2× write throughput |
| `wal_compression` | on | |
| `random_page_cost` | 1.1 | SSD |
| `work_mem` | 32MB | rollup aggregation |
| `autovacuum_max_workers` | 4 | many partitions |

## 3.6 Alerter service

Independent consumer group `alert-notifier` on `logs.security`. Deduplicates by
`(rule_id, client_ip, 5-minute bucket)` so a brute-force burst yields one
notification, not 2,000. Sinks: stdout (default), webhook, or Slack via env
config. Kept separate from the indexer so that a slow webhook can never add
latency to the ingest path.

## 3.7 Configuration (`common/config.py`)

`pydantic-settings` `BaseSettings`, all overridable by env / `.env`:

| Var | Default | Meaning |
|---|---|---|
| `KAFKA_BROKERS` | `redpanda:9092` | |
| `TOPIC_RAW` / `TOPIC_SECURITY` / `TOPIC_DLQ` | `logs.raw`/`logs.security`/`logs.dlq` | |
| `PG_DSN` | `postgres://logs:logs@postgres:5432/logs` | |
| `WORKERS` | `4` | indexer processes |
| `BATCH_SIZE` | `1000` | |
| `BATCH_MAX_AGE_MS` | `250` | |
| `QUEUE_MAX` | `20000` | per-worker bounded queue |
| `PG_POOL_MAX` | `6` | |
| `PRODUCER_RATE` | `5000` | events/sec |
| `LOG_LEVEL` | `INFO` | pipeline's own logging |
| `METRICS_PORT` | `9101`/`9102` | |

## 3.8 Metrics catalogue (`common/metrics.py`)

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `producer_events_total` | counter | service, level | offered load |
| `producer_send_errors_total` | counter | reason | |
| `consumer_events_total` | counter | worker, service | throughput |
| `consumer_parse_failures_total` | counter | format | parser quality |
| `consumer_dlq_total` | counter | reason | |
| `pipeline_e2e_latency_seconds` | histogram | — | `db_commit_time − emitted_at`; buckets 5ms…5s |
| `parse_duration_seconds` | histogram | format | CPU hot spot |
| `batch_size_records` | histogram | — | validates the 1000/250ms tuning |
| `batch_flush_duration_seconds` | histogram | outcome | DB write cost |
| `queue_depth` | gauge | worker | backpressure indicator |
| `consumer_paused` | gauge | worker | backpressure engaged |
| `consumer_lag_records` | gauge | topic, partition | from `end_offsets − position` |
| `security_alerts_total` | counter | rule_id, severity | |
| `rebalances_total` | counter | — | stability |

---

# 4. Observability & Dashboards

Two Grafana dashboards, both provisioned as JSON in `grafana/dashboards/` so the
stack comes up pre-configured.

## 4.1 `pipeline_health.json` (Prometheus datasource)

| Panel | Query sketch |
|---|---|
| Offered vs. ingested rate | `sum(rate(producer_events_total[30s]))` vs `sum(rate(consumer_events_total[30s]))` |
| End-to-end latency p50/p95/p99 | `histogram_quantile(0.99, sum by (le) (rate(pipeline_e2e_latency_seconds_bucket[1m])))` |
| Consumer lag by partition | `consumer_lag_records` (heatmap) |
| Batch size distribution | `histogram_quantile(0.5, ...batch_size_records_bucket...)` |
| Flush duration p95 | `...batch_flush_duration_seconds_bucket...` |
| Queue depth / backpressure | `queue_depth`, `consumer_paused` |
| Parse failure ratio | `rate(consumer_parse_failures_total[1m]) / rate(consumer_events_total[1m])` |
| Rebalances | `increase(rebalances_total[5m])` |

Alert rules: lag > 100k for 2 min; e2e p99 > 1s for 5 min; parse failure ratio
> 1%; ingested rate < 90% of offered for 3 min.

## 4.2 `log_analytics.json` (PostgreSQL datasource)

Panels read from `logs_1m_agg` where possible (pre-aggregated → sub-100 ms
dashboard refresh) and fall back to `logs` for drill-down.

```sql
-- Error rate % by service, 1-minute resolution
SELECT bucket AS time,
       service,
       100.0 * SUM(events) FILTER (WHERE level IN ('ERROR','FATAL'))
             / NULLIF(SUM(events),0) AS error_pct
FROM logs_1m_agg
WHERE $__timeFilter(bucket)
GROUP BY 1,2 ORDER BY 1;

-- Throughput (events/sec)
SELECT bucket AS time, SUM(events)/60.0 AS eps
FROM logs_1m_agg WHERE $__timeFilter(bucket) GROUP BY 1 ORDER BY 1;

-- Top 10 error messages (drill-down)
SELECT left(message, 120) AS msg, count(*) AS n
FROM logs
WHERE $__timeFilter(ts) AND level IN ('ERROR','FATAL')
GROUP BY 1 ORDER BY n DESC LIMIT 10;

-- Security alerts timeline
SELECT ts AS time, rule_id, severity, host(client_ip) AS ip
FROM security_alerts WHERE $__timeFilter(ts) ORDER BY ts DESC LIMIT 200;

-- Latency percentiles by service
SELECT bucket AS time, service, p95_lat
FROM logs_1m_agg WHERE $__timeFilter(bucket) GROUP BY 1,2,3;
```

The rollup refresh runs every 30 s and only recomputes the last 3 minutes:

```sql
INSERT INTO logs_1m_agg (bucket, service, level, events, p50_lat, p95_lat, p99_lat)
SELECT date_trunc('minute', ts), service, level, count(*),
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY latency_ms),
       percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms),
       percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms)
FROM logs
WHERE ts >= date_trunc('minute', now()) - interval '3 minutes'
GROUP BY 1,2,3
ON CONFLICT (bucket, service, level) DO UPDATE
SET events = EXCLUDED.events, p50_lat = EXCLUDED.p50_lat,
    p95_lat = EXCLUDED.p95_lat, p99_lat = EXCLUDED.p99_lat;
```

---

# 5. Benchmarking Plan — proving the résumé claims

Both bullets should be *measured*, reproducible, and committed to the repo as
`bench/RESULTS.md` with the raw output. Interviewers will ask how you got the
numbers.

## 5.1 Claim 1 — "5,000+ events/sec, sub-second end-to-end latency"

**Harness:** `bench/load_test.py`
**Method:**

1. Warm up 60 s at 1,000 eps; discard.
2. Ramp: 1k → 2k → 5k → 8k → 12k eps, 5 minutes at each step.
3. At each step record: offered rate, ingested rate (from `count(*)` deltas in
   Postgres, not from the app's own counter — independent verification), e2e
   latency histogram, consumer lag, CPU/RSS per container (`docker stats`).
4. Define **sustainable rate** = highest step where ingested ≈ offered (±2%) and
   lag is flat (not growing) for the full 5 minutes and p99 e2e < 1 s.
5. Repeat 3× and report median.

**Latency definition (state this explicitly in the README):** `db_commit_wall_clock
− emitted_at`, sampled per batch on the last record. Producer and consumer share
the container host clock, so skew is negligible; cross-host runs must use NTP.

**Report:** a table of step → offered/ingested/p50/p95/p99/lag, plus the Grafana
screenshot. Also report the failure point (where lag starts growing) — knowing
your ceiling is more impressive than only knowing your target.

## 5.2 Claim 2 — "batch insertion + indexing reduced write overhead by 60%"

Run `bench/bench_write_strategies.py` against an identical dataset (500k
pre-parsed rows) and identical schema, varying **one** dimension at a time.

**Experiment A — write strategy** (schema fixed = final indexed schema):

| Strategy | Description |
|---|---|
| A1 | `execute` one INSERT per row, autocommit |
| A2 | one INSERT per row, 1000 rows per transaction |
| A3 | multi-row `INSERT … VALUES` (1000-row VALUES list) |
| A4 | `executemany` with asyncpg |
| A5 | **`COPY` → temp stage → `INSERT … ON CONFLICT`** (chosen design) |

**Experiment B — index/schema strategy** (write strategy fixed = A5):

| Variant | Description |
|---|---|
| B1 | Unpartitioned table, B-tree on `ts` + 4 more B-trees (naive) |
| B2 | Partitioned, B-tree on `ts`, same secondary indexes |
| B3 | **Partitioned, BRIN on `ts`, partial index on ERROR/FATAL, GIN `jsonb_path_ops`** (chosen) |

**Experiment C — batch size sweep:** 1, 10, 100, 500, 1000, 2500, 5000, 10000 →
plot throughput and p99 flush duration; this is how you justify `BATCH_SIZE=1000`
rather than asserting it.

**Metrics per run:** wall-clock for 500k rows, rows/sec, CPU-seconds of the
Postgres backend (`pg_stat_statements.total_exec_time`), WAL bytes generated
(`pg_current_wal_lsn()` delta), index size after load, `p99` flush latency.

**Define "write overhead" precisely** — pick one and say so, e.g.:

> *Write overhead = total Postgres backend execution time (ms) + WAL bytes
> generated, per 100k rows ingested.*

Then the bullet becomes defensible: *"reduced write overhead by X% (A1 vs A5
baseline, measured by backend exec time and WAL volume over a 500k-row load)."*
If the measured number is 82% or 47%, **write the measured number** — an honest,
sourced figure beats a rounder invented one, and every interviewer who has done
this work knows batching gains are usually far larger than 60%.

## 5.3 Correctness benchmark (worth its own bullet)

`tests/load/test_no_loss.py`: produce exactly 1,000,000 events with sequential
counters embedded, kill `-9` a random indexer worker every 20 s and restart
Postgres once mid-run, then assert:

```sql
SELECT count(*), count(DISTINCT event_id) FROM logs;  -- both = 1,000,000
```

This proves G3 empirically and is the single most credible thing you can show
for a data-pipeline role.

---

# 6. Deployment (`docker-compose.yml`)

| Service | Image | Notes |
|---|---|---|
| `redpanda` | `redpandadata/redpanda:latest` | `--smp 2 --memory 2G --overprovisioned`; ports 9092, 9644 |
| `redpanda-console` | `redpandadata/console` | topic/lag inspection UI, port 8080 |
| `postgres` | `postgres:16` | tuned via `command: postgres -c shared_buffers=2GB …`; init scripts mount `sql/` |
| `postgres-exporter` | `prometheuscommunity/postgres-exporter` | |
| `producer` | built from `Dockerfile` | `depends_on: redpanda` |
| `indexer` | same image, different entrypoint | `deploy.replicas` optional; internally forks WORKERS |
| `alerter` | same image | |
| `prometheus` | `prom/prometheus` | scrapes producer, indexer, redpanda, pg-exporter |
| `grafana` | `grafana/grafana` | provisioned datasources + dashboards, port 3000 |

Bring-up ordering handled with healthchecks (`pg_isready`, `rpk cluster health`)
and `depends_on: condition: service_healthy`. Topics are created by an init
container running `rpk topic create logs.raw -p 12 -r 1 …`.

`Makefile` targets: `make up`, `make bench`, `make load RATE=8000`, `make psql`,
`make chaos` (kills a worker every 20 s), `make down`.

---

# 7. Testing strategy

| Layer | Scope | Tooling |
|---|---|---|
| Unit | parser (all 5 formats + malformed), level normalisation, token bucket accuracy, batcher trigger logic, classifier rules | `pytest`, `freezegun` |
| Property/fuzz | parser never raises, never exceeds 50 ms on any input | `hypothesis` |
| Integration | real Redpanda + Postgres in containers; produce 10k → assert 10k rows, correct partitioning, alerts populated | `testcontainers-python` |
| Chaos | kill worker, kill broker, pause Postgres (`docker pause`) → assert no loss, no duplicates | `tests/load/` |
| Load | §5 harness | custom |
| Static | `ruff`, `mypy --strict` on `common/` and `services/` | pre-commit + CI |

CI (GitHub Actions): lint → unit → integration (services via `docker compose`) →
a 60-second 2,000 eps smoke load test with assertions on loss and p99.

---

# 8. Milestone plan

| Milestone | Deliverable | Est. |
|---|---|---|
| M0 | Compose skeleton: Redpanda + Postgres + Grafana up, topics created, schema applied | 0.5 d |
| M1 | Producer with 3 formats + token bucket; verify with `rpk topic consume` | 1 d |
| M2 | Single-process consumer, naive row-at-a-time insert (**this is the baseline for §5.2 — keep it behind a flag**) | 1 d |
| M3 | Parser chain (all 5 formats) + unit/fuzz tests | 1.5 d |
| M4 | Batcher + COPY sink + manual offset commit + backpressure | 1.5 d |
| M5 | Multiprocess supervisor, graceful rebalance drain | 1 d |
| M6 | Security classifier + `logs.security` + alerter | 1 d |
| M7 | Partitioning, index tuning, rollup table, maintenance procs | 1 d |
| M8 | Prometheus metrics + both Grafana dashboards | 1 d |
| M9 | Benchmarks §5 + `RESULTS.md` + chaos test | 1.5 d |
| M10 | README with architecture diagram, GIF of dashboards, honest limitations section | 0.5 d |
| | **Total** | **~11–12 focused days** |

Keeping M2's naive path behind `WRITE_STRATEGY=naive|batched` is deliberate: it
is both the benchmark baseline and a live demo you can toggle during an
interview.

---

# 9. Risks & mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Python parsing becomes the ceiling below 5k eps | Miss G1 | Profile early (M3); pre-compiled regexes, hand-written logfmt scanner, `orjson`; scale processes; last resort: `msgspec` for the envelope |
| Local machine can't host producer + broker + DB + Grafana at 5k eps | Benchmarks unreliable | Run producer on the host, stack in containers; report the exact hardware in `RESULTS.md`; cap Grafana refresh at 10 s during benchmarks |
| `synchronous_commit=off` criticised as cheating | Credibility | Report numbers **both ways**; it's a legitimate documented trade-off given broker replay |
| Rebalance drops in-flight batch | Duplicate storm | `on_partitions_revoked` flushes and commits before releasing |
| Stateful classifier memory growth | OOM | Bounded key cap + LRU + janitor; metric on key count |
| Scope creep (adding ES, K8s, auth) | Never ships | ES is explicitly optional (§11); ship M0–M10 first |
| Benchmark numbers don't match the résumé bullet | Honesty problem | Write the bullet **after** the benchmark, from the measured number |

---

# 10. Open questions

1. Do you want **exactly-once** to the sink via Kafka transactions, or is
   at-least-once + idempotent upsert sufficient? (Recommendation: the latter —
   simpler, faster, and the more common real-world answer.)
2. Should `ts` be the log's own timestamp (current design, correct for analytics
   but allows late/out-of-order data into old partitions) or ingest time
   (simpler partitioning)? Current design accepts a bounded lateness of 24 h and
   drops anything older to `dead_letters`.
3. Single `logs` table with `is_security` flag, or a fully separate pipeline for
   security events? Current design does both (flag + separate table/topic).
4. Retention: 7 days default — enough to show partition drops in the demo?

---

# 11. Optional extension — Elasticsearch as a secondary sink

Deliberately *secondary*, behind `ENABLE_ES=true`, consuming `logs.raw` under
its own group so a slow ES cluster cannot affect Postgres ingest.

- Index per day: `logs-2026.07.30`, via a composable index template.
- `_bulk` API, batch = 1000 docs / 5 MB / 250 ms, `_id = event_id` for idempotency.
- ILM policy: hot 1 d → warm 3 d → delete 7 d.
- Mapping: `message` as `text` (standard analyser) + `message.keyword`;
  `service`, `level`, `host` as `keyword`; `attrs` as `flattened` to prevent
  mapping explosion; `ts` as `date`.
- Refresh interval raised to `30s` and `number_of_replicas: 0` during bulk load —
  a well-known ~2–3× indexing win worth mentioning.
- Grafana gets a second datasource; the free-text search panel goes to ES,
  aggregates stay on Postgres.

**Honest framing:** running both is defensible as "Postgres for structured
analytics and joins, ES for full-text search" — but say so explicitly rather than
implying you needed both.

---

# 12. Stretch goals (post-v1)

- **Dedicated stateful detector** consumer keyed by `client_ip` for accurate
  cross-service brute-force detection.
- **Schema registry** (Avro/Protobuf) instead of ad-hoc JSON, with schema
  evolution tests.
- **Kafka Streams-style windowed aggregation** in Python (`faust`-style) to
  compute the 1-minute rollups in-stream instead of in Postgres.
- **OpenTelemetry** tracing across producer → broker → consumer → DB, so the
  latency breakdown is per-hop rather than end-to-end only.
- **Adaptive batching** — tune `BATCH_SIZE` at runtime from observed flush
  latency (AIMD), demoed against a bursty load profile.
- **Tiered storage** — nightly `COPY` of dropped partitions to Parquet on S3/MinIO,
  queryable via DuckDB.
- Kubernetes manifests + HPA on consumer lag (KEDA).

---

# 13. Résumé bullets — write these *after* the benchmarks

Templates to fill from `bench/RESULTS.md`:

- "Built a real-time log ingestion pipeline (Python asyncio, Kafka/Redpanda,
  PostgreSQL) sustaining **{measured} events/sec** with **p99 end-to-end latency
  of {measured} ms**, verified under a 5-minute soak with flat consumer lag."
- "Cut database write overhead **{measured}%** by replacing row-at-a-time inserts
  with a `COPY`-to-staging batch strategy (1,000 records / 250 ms) plus BRIN
  time-range and partial error indexes on daily-partitioned tables; measured via
  backend execution time and WAL volume over a 500k-row load."
- "Guaranteed zero message loss under worker crashes and database outages using
  manual offset commits after durable writes plus idempotent upserts on a
  producer-generated event ID — validated by a 1M-event chaos test with kill -9
  injection (1,000,000 rows, 0 duplicates, 0 loss)."
- "Designed backpressure via bounded queues and Kafka partition pause/resume,
  keeping consumer memory flat during simulated 60-second database stalls."

The third and fourth bullets are the ones that separate this project from the
hundreds of other Kafka demos — most candidates can move data; far fewer can
prove they never lost any.
