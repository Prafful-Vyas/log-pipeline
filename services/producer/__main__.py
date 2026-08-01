from __future__ import annotations

import argparse
import asyncio
import random
import time

import structlog
from aiokafka import AIOKafkaProducer

from common.config import settings
from common.logging import configure_logging
from common.serde import encode_log_event
from services.producer.generator import PROFILES, build_event, default_states
from services.producer.rate_limiter import TokenBucket
from services.producer.scenarios import ScenarioEngine

log: structlog.stdlib.BoundLogger = configure_logging("producer")

CHUNK = 50


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Synthetic log producer")
    p.add_argument("--rate", type=float, default=settings.producer_rate)
    p.add_argument("--services", type=int, default=settings.producer_services)
    p.add_argument("--duration", type=int, default=settings.producer_duration)
    p.add_argument("--scenarios", type=str, default=settings.producer_scenarios)
    p.add_argument("--topic", type=str, default=settings.topic_raw)
    return p.parse_args()


async def emit_loop(
    producer: AIOKafkaProducer,
    topic: str,
    bucket: TokenBucket,
    profiles: tuple,
    states: dict,
    stats: dict,
    duration: int,
) -> None:
    weights = [p.volume_weight for p in profiles]
    start = time.monotonic()
    while duration <= 0 or (time.monotonic() - start) < duration:
        await bucket.acquire(CHUNK)
        for _ in range(CHUNK):
            profile = random.choices(profiles, weights=weights, k=1)[0]
            state = states[profile.name]
            event = build_event(profile, state)
            if event is None:
                continue
            payload = encode_log_event(event)
            producer.send(topic, value=payload, key=event.service.encode())
            stats["sent"] += 1


async def send_injected(producer: AIOKafkaProducer, topic: str, stats: dict, event) -> None:
    payload = encode_log_event(event)
    producer.send(topic, value=payload, key=event.service.encode())
    stats["sent"] += 1


async def stats_loop(stats: dict) -> None:
    last = 0
    while True:
        await asyncio.sleep(10)
        sent = stats["sent"]
        log.info("producer_stats", sent_total=sent, eps_last_10s=(sent - last) / 10.0)
        last = sent


async def main() -> None:
    args = parse_args()
    profiles = PROFILES[: args.services] if args.services < len(PROFILES) else PROFILES
    states = default_states(profiles)

    if args.scenarios in ("none", ""):
        enabled = set()
    elif args.scenarios == "all":
        enabled = None
    else:
        enabled = {s.strip() for s in args.scenarios.split(",") if s.strip()}

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_brokers,
        acks=1,
        compression_type="lz4",
        linger_ms=20,
        max_batch_size=262_144,
        max_request_size=2_097_152,
        request_timeout_ms=15_000,
        enable_idempotence=False,
    )
    await producer.start()
    log.info(
        "producer_starting",
        rate=args.rate,
        services=[p.name for p in profiles],
        duration=args.duration or "forever",
        scenarios=args.scenarios,
    )

    stats = {"sent": 0}
    bucket = TokenBucket(args.rate)

    tasks = [
        asyncio.create_task(
            emit_loop(producer, args.topic, bucket, profiles, states, stats, args.duration)
        ),
        asyncio.create_task(stats_loop(stats)),
    ]
    if enabled != set():
        engine = ScenarioEngine(
            states, lambda evt: send_injected(producer, args.topic, stats, evt), enabled
        )
        tasks.append(asyncio.create_task(engine.run()))

    try:
        if args.duration > 0:
            await tasks[0]
            for t in tasks[1:]:
                t.cancel()
        else:
            await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        await producer.stop()
        log.info("producer_stopped", total_sent=stats["sent"])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
