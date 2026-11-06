"""Drive one execution to a terminal state: decide, run ready tasks, reap leases, wait for the next due time."""

import threading
from typing import Any

from .engine import Decision, Engine
from .errors import StuckError
from .store import Store
from .worker import Registry, Worker


def drive(store: Store, registry: Registry, execution_id: str, *, workers: int = 1,
          lease_seconds: float = 30.0, poll_seconds: float = 0.02, max_rounds: int = 1_000_000) -> Decision:
    if workers > 1:
        return _drive_threaded(store, registry, execution_id, workers, lease_seconds, poll_seconds)
    engine = Engine(store)
    worker = Worker(store, registry, lease_seconds=lease_seconds)
    for _ in range(max_rounds):
        decision = engine.decide(execution_id)
        if decision.done:
            return decision
        ran = worker.drain()
        reaped = store.reap()
        if ran or reaped or decision.new_events:
            continue
        _wait_until(store, _next_due(store, decision), poll_seconds)
    raise StuckError(f"execution {execution_id} did not finish in {max_rounds} rounds")


def _next_due(store: Store, decision: Decision) -> float:
    due = [t for t in (store.next_due(), decision.next_timer) if t is not None]
    if not due:
        raise StuckError("execution is running but has no runnable task and no timer")
    return min(due)


def _wait_until(store: Store, due: float, poll_seconds: float) -> None:
    clock: Any = store.clock
    if clock.virtual:
        clock.advance_to(due)
    else:
        clock.sleep(min(max(due - clock.now(), 0.0), 0.25) or poll_seconds)


def _drive_threaded(store: Store, registry: Registry, execution_id: str, workers: int,
                    lease_seconds: float, poll_seconds: float) -> Decision:
    stop = threading.Event()
    errors: list[BaseException] = []

    def loop(n: int) -> None:
        worker = Worker(store, registry, owner=f"worker-{n}", lease_seconds=lease_seconds)
        try:
            while not stop.is_set():
                if not worker.run_once():
                    stop.wait(poll_seconds)
        except BaseException as exc:
            errors.append(exc)
            stop.set()

    threads = [threading.Thread(target=loop, args=(n,), daemon=True) for n in range(workers)]
    for t in threads:
        t.start()
    engine = Engine(store)
    try:
        while True:
            if errors:
                raise errors[0]
            decision = engine.decide(execution_id)
            if decision.done:
                return decision
            if not (store.reap() or decision.new_events):
                stop.wait(poll_seconds)
    finally:
        stop.set()
        for t in threads:
            t.join()
