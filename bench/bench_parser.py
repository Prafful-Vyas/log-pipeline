"""Parser throughput/latency benchmark (design doc SS3.5.3 / SS5).

No live services required — generates synthetic raw log lines with the same
producer used by the real pipeline (services.producer.generator), then times
services.indexer.parser.parse() over each format. Run standalone:

    python -m bench.bench_parser [--n 5000] [--seed 42]

Validates the parser's own performance budget (<=50ms per event, matching the
fuzz-test spirit of tests/unit/test_parser.py) and reports ops/sec plus
mean/p50/p95/p99 latency per format — this is what justifies "1 core ~= 8,300
eps" in the design doc's capacity model (SS2.5).
"""

from __future__ import annotations

import argparse
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from statistics import mean

from services.indexer.parser import parse
from services.producer.generator import PROFILES, build_event, default_states

FORMATS = ("json", "syslog", "logfmt", "apache_combined", "plain")
MAX_LATENCY_S = 0.050  # 50ms budget, matches design doc SS3.4.4 fuzz-test claim


@dataclass
class BenchResult:
    format: str
    n: int
    mean_us: float
    p50_us: float
    p95_us: float
    p99_us: float
    max_us: float
    ops_per_sec: float


def collect_samples(per_format: int, seed: int) -> dict[str, list[str]]:
    random.seed(seed)
    states = default_states(PROFILES)
    samples: dict[str, list[str]] = {f: [] for f in FORMATS}
    attempts, max_attempts = 0, per_format * len(FORMATS) * 200
    while any(len(v) < per_format for v in samples.values()) and attempts < max_attempts:
        attempts += 1
        profile = random.choice(PROFILES)
        built = build_event(profile, states[profile.name])
        if built is None:
            continue
        event, _level = built
        bucket = samples.get(event.format)
        if bucket is not None and len(bucket) < per_format:
            bucket.append(event.raw)
    return samples


def bench_format(fmt: str, raws: list[str]) -> BenchResult:
    ingested_at = datetime.now(UTC)
    durations: list[float] = []
    for raw in raws:
        start = time.perf_counter()
        fields, status = parse(raw, ingested_at)
        durations.append(time.perf_counter() - start)
        assert isinstance(fields, dict)
        assert status is not None
    durations.sort()
    n = len(durations)
    total = sum(durations) or 1e-9
    return BenchResult(
        format=fmt,
        n=n,
        mean_us=mean(durations) * 1e6,
        p50_us=durations[int(n * 0.50)] * 1e6,
        p95_us=durations[int(n * 0.95)] * 1e6,
        p99_us=durations[min(n - 1, int(n * 0.99))] * 1e6,
        max_us=durations[-1] * 1e6,
        ops_per_sec=n / total,
    )


def bench_never_raises() -> None:
    """Cheap sanity pass matching tests/unit/test_parser.py's garbage inputs,
    run here too so a benchmark run also re-confirms the safety property."""
    ingested_at = datetime.now(UTC)
    for raw in ("", "{", "not json at all !!! ###", "a" * 20_000, "\x00\x01\x02"):
        fields, status = parse(raw, ingested_at)
        assert isinstance(fields, dict)
        assert status is not None


def main() -> None:
    p = argparse.ArgumentParser(description="Parser throughput/latency benchmark")
    p.add_argument("--n", type=int, default=2000, help="samples per format")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    bench_never_raises()
    samples = collect_samples(args.n, args.seed)
    results = [bench_format(fmt, raws) for fmt, raws in samples.items()]

    header = f"{'format':<18}{'n':>8}{'ops/sec':>12}{'mean_us':>10}{'p50_us':>10}{'p95_us':>10}{'p99_us':>10}{'max_us':>10}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r.format:<18}{r.n:>8}{r.ops_per_sec:>12,.0f}{r.mean_us:>10.2f}"
            f"{r.p50_us:>10.2f}{r.p95_us:>10.2f}{r.p99_us:>10.2f}{r.max_us:>10.2f}"
        )
        assert r.max_us / 1e6 < MAX_LATENCY_S, f"{r.format} exceeded the 50ms budget"

    print()
    print("All formats within the 50ms/event budget.")


if __name__ == "__main__":
    main()
