from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable

import structlog

from services.producer.generator import PROFILES, ScenarioState, build_injected_event

log = structlog.get_logger("producer.scenarios")

SendFn = Callable[["object", str], Awaitable[None]]  # accepts (LogEvent, level)

# (name, mean_interval_s, duration_s)
SCHEDULE = (
    ("error_burst", 300, 60),
    ("latency_spike", 420, 45),
    ("brute_force", 600, 45),
    ("sqli_probe", 60, 5),
    ("cascade", 900, 30),
    ("silence", 1200, 45),
)


class ScenarioEngine:
    def __init__(
        self,
        states: dict[str, ScenarioState],
        send: SendFn,
        enabled: set[str] | None = None,
    ):
        self._states = states
        self._send = send
        self._enabled = enabled  # None = all enabled
        self._names = list(states.keys())
        self._profiles_by_name = {p.name: p for p in PROFILES}

    def _active(self, name: str) -> bool:
        return self._enabled is None or name in self._enabled

    async def run(self) -> None:
        async with asyncio.TaskGroup() as tg:
            for name, interval, duration in SCHEDULE:
                if self._active(name):
                    tg.create_task(self._loop(name, interval, duration))

    async def _loop(self, name: str, interval: int, duration: int) -> None:
        handler = getattr(self, f"_scenario_{name}")
        while True:
            jitter = random.uniform(-0.25, 0.25) * interval
            await asyncio.sleep(max(5.0, interval + jitter))
            try:
                await handler(duration)
            except Exception:
                log.exception("scenario_failed", scenario=name)

    async def _scenario_error_burst(self, duration: int) -> None:
        svc = random.choice(self._names)
        log.info("scenario_start", scenario="error_burst", service=svc, duration=duration)
        self._states[svc].error_mult = 10.0
        await asyncio.sleep(duration)
        self._states[svc].error_mult = 1.0
        log.info("scenario_end", scenario="error_burst", service=svc)

    async def _scenario_latency_spike(self, duration: int) -> None:
        svc = random.choice(self._names)
        log.info("scenario_start", scenario="latency_spike", service=svc, duration=duration)
        self._states[svc].latency_mult = 8.0
        await asyncio.sleep(duration)
        self._states[svc].latency_mult = 1.0
        log.info("scenario_end", scenario="latency_spike", service=svc)

    async def _scenario_brute_force(self, duration: int) -> None:
        profile = self._profiles_by_name.get("auth-service") or self._profiles_by_name[
            self._names[0]
        ]
        ip = f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
        total = random.randint(200, 2000)
        log.info("scenario_start", scenario="brute_force", ip=ip, total=total)
        interval = duration / max(total, 1)
        for _ in range(total):
            evt, level = build_injected_event(
                profile,
                level="ERROR",
                endpoint="/login",
                client_ip=ip,
                message="auth failed",
                status=401,
            )
            await self._send(evt, level)
            await asyncio.sleep(interval)
        log.info("scenario_end", scenario="brute_force", ip=ip)

    async def _scenario_sqli_probe(self, duration: int) -> None:
        profile = random.choice(PROFILES)
        payloads = (
            "' OR '1'='1",
            "1; DROP TABLE users;--",
            "UNION SELECT username, password FROM users",
            "/*!50000 SELECT */ 1",
        )
        payload = random.choice(payloads)
        log.info("scenario_start", scenario="sqli_probe", service=profile.name)
        evt, level = build_injected_event(
            profile,
            level="WARN",
            endpoint=f"/search?q={payload}",
            message=f"suspicious query string: {payload}",
            status=400,
        )
        await self._send(evt, level)

    async def _scenario_cascade(self, duration: int) -> None:
        downstream = random.choice([p for p in self._names if p != "api-gateway"])
        log.info("scenario_start", scenario="cascade", downstream=downstream, duration=duration)
        self._states["api-gateway"].error_mult = 6.0
        self._states[downstream].error_mult = 8.0
        await asyncio.sleep(duration)
        self._states["api-gateway"].error_mult = 1.0
        self._states[downstream].error_mult = 1.0
        log.info("scenario_end", scenario="cascade", downstream=downstream)

    async def _scenario_silence(self, duration: int) -> None:
        svc = random.choice(self._names)
        log.info("scenario_start", scenario="silence", service=svc, duration=duration)
        self._states[svc].silenced = True
        await asyncio.sleep(duration)
        self._states[svc].silenced = False
        log.info("scenario_end", scenario="silence", service=svc)
