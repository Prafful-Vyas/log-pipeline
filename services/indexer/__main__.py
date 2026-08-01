from __future__ import annotations

import multiprocessing as mp
import signal
import time

import structlog

from common.config import settings
from common.logging import configure_logging

log: structlog.stdlib.BoundLogger = configure_logging("indexer.supervisor")


def _worker_entrypoint(worker_id: int) -> None:
    import asyncio

    from common.logging import configure_logging as _configure
    from services.indexer.worker import run_worker

    _configure(f"indexer.worker.{worker_id}")
    try:
        asyncio.run(run_worker(worker_id))
    except KeyboardInterrupt:
        pass


class Supervisor:
    """Forks `WORKERS` child processes, all joining the same Kafka consumer
    group — Kafka handles partition assignment across them. A child that
    exits non-zero is restarted with backoff; more than MAX_RESTARTS in
    RESTART_WINDOW_S aborts the whole supervisor rather than crash-looping
    forever."""

    MAX_RESTARTS = 5
    RESTART_WINDOW_S = 60

    def __init__(self, n_workers: int):
        self._n = n_workers
        self._ctx = mp.get_context("spawn")
        self._procs: dict[int, mp.process.BaseProcess] = {}
        self._restart_times: dict[int, list[float]] = {i: [] for i in range(n_workers)}
        self._shutting_down = False

    def _spawn(self, worker_id: int) -> None:
        p = self._ctx.Process(target=_worker_entrypoint, args=(worker_id,), daemon=False)
        p.start()
        self._procs[worker_id] = p
        log.info("worker_spawned", worker=worker_id, pid=p.pid)

    def start(self) -> None:
        for i in range(self._n):
            self._spawn(i)

    def _handle_signal(self, signum, frame) -> None:
        log.info("supervisor_shutdown_signal", signum=signum)
        self._shutting_down = True
        for p in self._procs.values():
            if p.is_alive():
                p.terminate()

    def run_forever(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        self.start()

        while not self._shutting_down:
            time.sleep(1)
            for worker_id, p in list(self._procs.items()):
                if p.is_alive() or self._shutting_down:
                    continue
                log.warning("worker_exited", worker=worker_id, exit_code=p.exitcode)
                now = time.time()
                times = [t for t in self._restart_times[worker_id] if now - t < self.RESTART_WINDOW_S]
                times.append(now)
                self._restart_times[worker_id] = times
                if len(times) > self.MAX_RESTARTS:
                    log.error("worker_crash_loop_abort", worker=worker_id, restarts=len(times))
                    self._shutting_down = True
                    break
                self._spawn(worker_id)

        log.info("supervisor_draining")
        for p in self._procs.values():
            p.join(timeout=30)
            if p.is_alive():
                p.kill()
        log.info("supervisor_stopped")


def main() -> None:
    log.info("supervisor_starting", workers=settings.workers, write_strategy=settings.write_strategy)
    Supervisor(settings.workers).run_forever()


if __name__ == "__main__":
    main()
