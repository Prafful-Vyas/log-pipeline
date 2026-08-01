# Real-Time Log Ingestion & Analytics Pipeline

Implementation of the core pipeline described in `log-pipeline-design.md`
(milestones M0–M6): synthetic producer → Redpanda → multiprocess async
indexer (parse/enrich/classify/batch) → PostgreSQL, plus an independent
security alerter.

**Not yet built** (deliberately out of scope for this pass — see the design
doc §4, §5, §7 for the full spec): Grafana/Prometheus dashboards, the
formal benchmark suite (`bench/`), and the chaos/no-loss test harness. The
`WRITE_STRATEGY=naive|batched` toggle and the schema/index choices needed
to run those benchmarks later are already in place.

## Architecture

```
producer  →  Redpanda (logs.raw, logs.security, logs.dlq)  →  indexer (N processes)  →  PostgreSQL
                                                                     │
                                                                     └─→ logs.security → alerter (stdout/webhook)
```

See `log-pipeline-design.md` for the full HLD/LLD.

## Quickstart

```bash
cp .env.example .env
docker compose up -d --build
docker compose logs -f producer indexer alerter
```

This brings up Redpanda, Postgres (schema applied automatically from
`sql/001_schema.sql`), a topic-init job, the producer (default 5,000
events/sec across 8 simulated services), the multiprocess indexer, and the
alerter.

Inspect topics/lag at http://localhost:8080 (Redpanda Console).

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

## Tests

```bash
pip install -e ".[dev]"
make test     # pytest tests/unit
make lint     # ruff check
```

Unit tests cover the parser (all 5 formats + malformed-input fuzz), the
token-bucket rate limiter, and the batcher's flush/offset/retry semantics
(size trigger, age trigger, retry-then-raise, and that a DLQ-only item still
advances the offset without writing a row).

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
| Partitioned schema, BRIN/partial/GIN indexes | ✅ |
| Prometheus metrics + Grafana dashboards | ❌ not built (structlog + periodic stats logs only) |
| `bench/` benchmark harness + `RESULTS.md` | ❌ not built |
| Chaos / no-loss test (`tests/load/`) | ❌ not built |
| Elasticsearch secondary sink | ❌ not built (§11, optional) |

## Known limitations (honest, per the design doc's own framing)

- No Prometheus/Grafana in this pass — throughput/lag/latency are visible
  only via structured JSON logs (`worker_stats`, `producer_stats`) and
  Redpanda Console.
- The security classifier's stateful rules (brute force, spray, error burst)
  are scoped per worker process, so cross-partition correlation is
  approximate — documented as a known limitation in the design doc §3.5.4.
- `synchronous_commit=off` is a deliberate throughput trade-off (§3.5.7):
  up to ~200ms of committed rows are at risk on an OS crash; Redpanda replay
  from the last committed offset covers it.
