# Benchmark results

**Status: PENDING.** These benchmarks were authored and code-reviewed in an
environment without Docker installed, so none of the commands below have
been executed and no numbers here are real. Run each command locally (with
`make up` already running for `load`/`chaos`) and replace the relevant
section with the actual output. Do not fill these in with invented numbers —
see design doc SS5.2's "write the measured number" principle.

## Hardware

_PENDING — fill in: CPU model/cores, RAM, disk type (SSD/NVMe), OS, Docker
version, and whether the producer ran on the host or in a container._

## Claim 1 — throughput & end-to-end latency (design doc SS5.1)

Command:

```bash
make up
make load RATES=1000,2000,5000,8000,12000 STEP=300
```

(The default `make load` uses a shorter `1000,2000,5000` / 30s-per-step ramp
for a quick check; the invocation above reproduces the full design-doc ramp.)

**Sustainable rate** = highest step where ingested ~= offered (+/-2%) and p99
< 1s. State it explicitly once measured:

> Sustained **_PENDING_** events/sec with p99 end-to-end latency of
> **_PENDING_** ms.

| rate_eps | offered | ingested | p50_ms | p95_ms | p99_ms |
|---|---|---|---|---|---|
| PENDING | | | | | |

## Claim 2 — write-strategy benchmark (design doc SS5.2)

Command:

```bash
make bench
```

Runs `bench/bench_parser.py` (no Docker needed) and
`bench/bench_write_strategies.py` (needs Postgres via `make up`).

**Scope note:** only Experiments A (write strategy) and C (batch-size sweep)
are implemented — Experiment B (index/schema variants) was scoped out; see
the plan this was built from. "Write overhead" here is defined as wall-clock
time + WAL bytes generated per 100k rows (`pg_wal_lsn_diff`), not
`pg_stat_statements` execution time, since that extension isn't enabled in
this repo's `postgres:16` compose config.

### Parser throughput (`bench_parser.py`)

| format | ops/sec | mean_us | p50_us | p95_us | p99_us | max_us |
|---|---|---|---|---|---|---|
| PENDING | | | | | | |

### Experiment A — write strategy (A1-A5)

| strategy | rows | seconds | rows/sec | wal_bytes/100k |
|---|---|---|---|---|
| A1 (row-at-a-time, autocommit) | PENDING | | | |
| A2 (row-at-a-time, 1 txn) | | | | |
| A3 (multi-row VALUES) | | | | |
| A4 (executemany) | | | | |
| A5 (COPY -> staging, chosen) | | | | |

> Write overhead reduced by **_PENDING_%** (A1 vs A5, wall-clock + WAL bytes
> per 100k rows over a _PENDING_-row load).

### Experiment C — batch size sweep (A5 strategy)

| batch_size | rows/sec | p99_flush_ms |
|---|---|---|
| 1 | | |
| 10 | | |
| 100 | | |
| 500 | | |
| 1000 | | |
| 2500 | | |
| 5000 | | |
| 10000 | | |

## Correctness — chaos / no-loss test (design doc SS5.3)

Command:

```bash
make up
CHAOS_EVENTS=1000000 make chaos   # 100,000 by default; 1,000,000 matches the literal design-doc claim
```

**Deviation from the literal spec** (documented in `tests/load/test_no_loss.py`):
kills/restarts the whole `indexer` container (all worker processes at once)
rather than a single worker process, and the always-on `producer` service is
paused for the test's duration to keep the row-count deltas unconfounded.

| events sent | rows in `logs` (delta) | distinct event_ids (delta) | result |
|---|---|---|---|
| PENDING | | | |

> Validated by a _PENDING_-event chaos test with `docker compose kill -9`
> injection against the indexer and a Postgres restart: _PENDING_ rows,
> _PENDING_ duplicates, _PENDING_ lost.
