import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from dwe import CrashInjector, Engine, FakeClock, Registry, Store, drive
from dwe.examples import path as example_path


def task(resource: str, **extra: Any) -> dict:
    return {"Type": "Task", "Resource": resource, **extra}


def machine(start: str, **states: dict) -> dict:
    return {"StartAt": start, "States": states}


def load_example(name: str) -> dict:
    return json.loads(example_path(name).read_text())


class Harness:
    """One SQLite file plus a shared fake clock. Each `store()` call is a fresh 'process'."""

    def __init__(self, db: Path) -> None:
        self.db = db
        self.clock = FakeClock()

    def store(self, crash: CrashInjector | None = None) -> Store:
        return Store(self.db, clock=self.clock, crash=crash)

    def start(self, definition: dict, input: Any = None) -> str:
        return Engine(self.store()).start(definition, input)

    def run(self, definition: dict, input: Any, registry: Registry, **kw: Any):
        store = self.store()
        execution_id = Engine(store).start(definition, input)
        return execution_id, drive(store, registry, execution_id, **kw)

    def events(self, execution_id: str, type_: str | None = None):
        evs = self.store().load_events(execution_id)
        return [e for e in evs if type_ is None or e.type == type_]


@pytest.fixture
def h(tmp_path: Path) -> Harness:
    return Harness(tmp_path / "wf.db")


def handlers(**fns: Callable) -> Registry:
    reg = Registry()
    for name, fn in fns.items():
        reg.add(name, fn)
    return reg
