# Real-Time Log Ingestion & Analytics Pipeline

Implementation of the pipeline described in `log-pipeline-design.md`
(milestones M0–M10): synthetic producer → Redpanda → multiprocess async
indexer (parse/enrich/classify/batch) → PostgreSQL, an independent security
alerter, Prometheus/Grafana observability, and a benchmark + chaos-test suite.

**Not built** (explicitly out of scope, see design doc §11): the optional
Elasticsearch secondary sink. Experiment B (index/schema variants) of the
write-strategy benchmark (§5.2) is also skipped — see `bench/RESULTS.md`.

## Architecture

```
producer  →  Redpanda (logs.raw, logs.security, logs.dlq)  →  indexer (N processes)  →  PostgreSQL
                                                                     │                       │
                                                                     └─→ logs.security        └─→ maintenance (rollups, partitions)
                                                                          → alerter (stdout/webhook)

producer, indexer  →  Prometheus  →  Grafana (pipeline_health, log_analytics)
```

See `log-pipeline-design.md` for the full HLD/LLD.

## Quickstart

```bash
cp .env.example .env
docker compose up -d --build
docker compose logs -f producer indexer alerter
```

This brings up Redpanda, Postgres (schema applied automatically from
`sql/001_schema.sql` + `sql/002_rollups.sql`), a topic-init job, the producer
(default 5,000 events/sec across 8 simulated services), the multiprocess
indexer, the alerter, a `maintenance` sidecar (rollup refresh + partition
upkeep), and Prometheus/Grafana.

Inspect topics/lag at http://localhost:8080 (Redpanda Console).

## Observability

- Grafana: http://localhost:3000 (`admin`/`admin`) — two provisioned
  dashboards, **Pipeline Health** (Prometheus: offered/ingested rate, e2e
  latency percentiles, consumer lag, batch size/flush duration, queue
  depth/backpressure, parse failure ratio, rebalances) and **Log Analytics**
  (Postgres: error rate by service, throughput, top error messages, security
  alert timeline, latency by service).
- Prometheus: http://localhost:9090 — scrapes the producer (`:9101`), indexer
  (`:9102`, aggregated across worker processes via
  `prometheus_client`'s multiprocess mode), Redpanda, and postgres-exporter.
- Metric catalogue: `common/metrics.py` (mirrors design doc §3.8).

Query the data:

```bash
make psql
# or: docker compose exec postgres psql -U logs -d logs
select count(*) from logs;
select level, count(*) from logs group by 1;
select rule_id, severity, service, ts from security_alerts order by ts desc limit 20;
```

## Toggling the write strategy

`WRITE_STRATEGY=batched` (default) uses the COPY-to-staging-table sink
(`sink_pg.py:write_batch`). Set `WRITE_STRATEGY=naive` in `.env` to switch to
the row-at-a-time baseline (`write_naive`) — this is the M2 baseline kept
specifically so the batching win can be benchmarked later (design doc §5.2).

## Running a short local load

```bash
make produce   # runs the producer standalone at 2,000 eps for 60s
```

Or run the producer's CLI directly:

```bash
docker compose run --rm producer python -m services.producer \
  --rate 5000 --services 8 --duration 300 --scenarios all
```

`--scenarios none` disables the injected error bursts / brute-force /
sqli-probe / cascade / silence scenarios if you want a clean, uniform stream.

## Benchmarks & chaos test

```bash
python -m bench.bench_parser            # parser throughput/latency, no Docker needed
make bench                              # bench_parser + bench_write_strategies (needs `make up`)
make load                               # end-to-end ramp benchmark (needs `make up`)
make chaos                              # no-loss chaos test (needs `make up`)
```

- `bench/bench_write_strategies.py` compares write strategies A1–A5 (design
  doc §5.2 Experiment A) and sweeps batch size (Experiment C) against
  disposable tables — it never touches the real `logs` table. Experiment B
  (index/schema variants) is out of scope for this pass.
- `bench/load_test.py` ramps producer rate and independently verifies offered
  vs. ingested counts and latency percentiles from Postgres itself (design
  doc §5.1). Defaults to a short 1k/2k/5k eps ramp; pass
  `make load RATES=1000,2000,5000,8000,12000 STEP=300` for the full design-doc
  ramp.
- `tests/load/test_no_loss.py` (`make chaos`) kills/restarts the indexer
  container and restarts Postgres mid-run, then asserts zero row loss and zero
  duplicates (design doc §5.3, proving G3). Defaults to 100,000 events; set
  `CHAOS_EVENTS=1000000` to match the literal design-doc scale. Skips itself
  cleanly if the compose stack or `docker` CLI isn't available.
- **`bench/RESULTS.md` is an unfilled template** — this repo was built in an
  environment without Docker, so the benchmarks above have not actually been
  run. Run them locally and paste in the real output rather than trusting any
  numbers currently in that file (there aren't any).

## Tests

```bash
pip install -e ".[dev]"
make test     # pytest tests/unit
make lint     # ruff check
```

Unit tests cover the parser (all 5 formats + malformed-input fuzz), the
token-bucket rate limiter, the batcher's flush/offset/retry semantics
(size trigger, age trigger, retry-then-raise, and that a DLQ-only item still
advances the offset without writing a row), and the metrics module.
`tests/load/` holds the chaos test (see above) — it's excluded from `make
test`'s default `pytest tests/unit` run since it needs a live compose stack
(it still gets linted by `make lint`, and skips itself cleanly rather than
failing if you run `pytest tests/load` without Docker).

## What's implemented vs. the design doc

| Area | Status |
|---|---|
| Producer (5 formats, 8 profiles, 6 scenario types, token-bucket) | ✅ |
| Kafka/Redpanda topics + init | ✅ |
| Parser chain (json/syslog/logfmt/apache/plain) | ✅ |
| PII redaction | ✅ |
| Security classifier (8 rules, bounded stateful windows) | ✅ |
| Batcher (size/age trigger, retry+backoff, offset-after-flush) | ✅ |
| COPY-to-staging Postgres sink + naive baseline | ✅ |
| Multiprocess supervisor + graceful rebalance flush | ✅ |
| Alerter (dedup, stdout/webhook) | ✅ |
| Partitioned schema, BRIN/partial/GIN indexes, 1-min rollup table | ✅ |
| Prometheus metrics + Grafana dashboards | ✅ |
| `bench/` benchmark harness (parser, write-strategy A+C, load test) | ✅ code; **not yet run** — see `bench/RESULTS.md` |
| Chaos / no-loss test (`tests/load/`) | ✅ code; **not yet run** — needs Docker |
| Elasticsearch secondary sink | ❌ not built (§11, optional) |
| Write-strategy Experiment B (index/schema variants) | ❌ not built (scoped out, see `bench/RESULTS.md`) |

## Known limitations (honest, per the design doc's own framing)

- **Benchmarks/chaos test are unrun.** This pass was built in an environment
  without Docker installed, so `bench/RESULTS.md` is a template with no real
  numbers — run `make bench` / `make load` / `make chaos` locally to fill it in.
- The security classifier's stateful rules (brute force, spray, error burst)
  are scoped per worker process, so cross-partition correlation is
  approximate — documented as a known limitation in the design doc §3.5.4.
- `synchronous_commit=off` is a deliberate throughput trade-off (§3.5.7):
  up to ~200ms of committed rows are at risk on an OS crash; Redpanda replay
  from the last committed offset covers it.
- `bench/load_test.py` doesn't poll consumer lag (watch it live in Grafana or
  Redpanda Console during a run instead).
- `tests/load/test_no_loss.py` kills/restarts the whole `indexer` container
  (all worker processes at once) rather than a single worker process, since
  that's reliably controllable via the `docker compose` CLI without exec'ing
  into container internals — arguably a stronger test of G3, but a deviation
  from the design doc's literal single-worker-kill spec.
- No `pg_cron` in the vanilla `postgres:16` image, so rollup refresh and
  partition maintenance run from a Python sidecar (`scripts/maintenance.py`)
  instead of an in-database scheduled job.
