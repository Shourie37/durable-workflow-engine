"""Workers lease tasks from the queue, run the handler, and report the result."""

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .errors import TaskError
from .store import Store, Task

Handler = Callable[[Any, "TaskContext"], Any]


class Registry:
    """Maps a Task state's Resource name to a Python handler."""

    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def register(self, name: str) -> Callable[[Handler], Handler]:
        def wrap(fn: Handler) -> Handler:
            self._handlers[name] = fn
            return fn
        return wrap

    def add(self, name: str, fn: Handler) -> None:
        self._handlers[name] = fn

    def get(self, name: str) -> Handler | None:
        return self._handlers.get(name)


@dataclass
class TaskContext:
    store: Store
    task: Task
    owner: str
    lease_seconds: float

    @property
    def token(self) -> str:
        return self.task.token

    @property
    def attempt(self) -> int:
        return self.task.attempt

    @property
    def delivery(self) -> int:
        return self.task.deliveries

    @property
    def idempotency_key(self) -> str:
        """Stable across retries and redeliveries of the same state visit."""
        return self.task.idempotency_key

    def heartbeat(self) -> bool:
        return self.store.heartbeat(self.task.token, self.owner, self.lease_seconds)

    def effect(self, fn: Callable[[], Any], name: str | None = None) -> Any:
        """Apply an external side effect at most once per idempotency key."""
        key = self.idempotency_key if name is None else f"{self.idempotency_key}:{name}"
        return self.store.apply_effect(key, self.task.activity, fn)


class Worker:
    def __init__(self, store: Store, registry: Registry, owner: str | None = None,
                 lease_seconds: float = 30.0) -> None:
        self.store = store
        self.registry = registry
        self.owner = owner or f"worker-{uuid.uuid4().hex[:6]}"
        self.lease_seconds = lease_seconds

    def run_once(self) -> bool:
        task = self.store.lease_task(self.owner, self.lease_seconds)
        if task is None:
            return False
        self.store.crash.point("worker:leased")
        handler = self.registry.get(task.activity)
        output, error, cause = None, None, ""
        if handler is None:
            error, cause = "States.TaskFailed", f"no handler registered for {task.activity!r}"
        else:
            try:
                output = handler(task.input, TaskContext(self.store, task, self.owner, self.lease_seconds))
            except TaskError as exc:
                error, cause = exc.error, exc.cause
            except Exception as exc:
                error, cause = type(exc).__name__, str(exc)
        self.store.crash.point("worker:before_report")
        self.store.complete_task(task.token, output, error, cause)
        return True

    def drain(self) -> int:
        count = 0
        while self.run_once():
            count += 1
        return count
