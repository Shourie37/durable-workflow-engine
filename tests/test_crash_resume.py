"""Property: crash the engine at random points, resume from history, and get the same result as a clean run."""

import random
from collections import Counter

import pytest

from conftest import Harness, handlers, load_example, machine, task
from dwe import CrashInjector, Engine, SimulatedCrash, TaskError, drive
from dwe.examples.activities import REGISTRY as EXAMPLES

SEEDS = range(50)


def _prep(inp, ctx):
    if ctx.attempt == 1:
        raise TaskError("Transient", "cold start")
    return ctx.effect(lambda: {"mode": "batch", "n": len(inp["items"])})


def _score(item, ctx):
    if item % 4 == 1 and ctx.attempt == 1:
        raise TaskError("Transient", f"item {item} busy")
    if item in (5, 9):
        raise TaskError("BadItem", f"item {item}")
    return ctx.effect(lambda: item * item)


SINK = handlers(
    prep=_prep,
    score=_score,
    audit=lambda inp, ctx: ctx.effect(lambda: {"audited": True}),
    finish=lambda inp, ctx: ctx.effect(lambda: {"total": sum(x for x in inp["fan"][0] if isinstance(x, int))}),
)

KITCHEN_SINK = machine(
    "Prepare",
    Prepare=task("prep", ResultPath="$.prep", Next="Route",
                 Retry=[{"ErrorEquals": ["Transient"], "IntervalSeconds": 2, "JitterStrategy": "FULL"}]),
    Route={"Type": "Choice", "Choices": [{"Variable": "$.prep.mode", "StringEquals": "batch", "Next": "Fanout"}],
           "Default": "Unexpected"},
    Fanout={"Type": "Parallel", "ResultPath": "$.fan", "Next": "Finish", "Branches": [
        machine("Each", Each={
            "Type": "Map", "ItemsPath": "$.items", "MaxConcurrency": 3, "ToleratedFailureCount": 2, "End": True,
            "ItemProcessor": machine("Score", Score=task("score", End=True, Retry=[
                {"ErrorEquals": ["Transient"], "IntervalSeconds": 1, "BackoffRate": 2}]))}),
        machine("Pause", Pause={"Type": "Wait", "Seconds": 5, "Next": "Audit"}, Audit=task("audit", End=True)),
    ]},
    Finish=task("finish", ResultPath="$.result", End=True),
    Unexpected={"Type": "Fail", "Error": "Unexpected"},
)

WORKFLOWS = {
    "kitchen_sink": (KITCHEN_SINK, {"items": list(range(12))}, SINK),
    "saga_compensates": (load_example("saga"), {"trip_id": "T-9", "fail_at": "car", "flaky": "hotel"}, EXAMPLES),
    "map_analysis": (load_example("map_analysis"),
                     {"items": [{"id": i, **({"corrupt": True} if i == 4 else {}),
                                 **({"flaky": True} if i % 3 == 0 else {})} for i in range(8)]}, EXAMPLES),
}


def clean_run(h: Harness, name: str):
    definition, inp, registry = WORKFLOWS[name]
    counter = CrashInjector()
    store = h.store(counter)
    ex = Engine(store).start(definition, inp)
    decision = drive(store, registry, ex)
    return decision, counter.count, Counter(a for _, a, _ in h.store().effects())


@pytest.mark.parametrize("name", WORKFLOWS)
@pytest.mark.parametrize("seed", SEEDS)
def test_crash_and_resume_matches_clean_run(tmp_path, name, seed):
    baseline, points, baseline_effects = clean_run(Harness(tmp_path / "clean.db"), name)
    definition, inp, registry = WORKFLOWS[name]
    rng = random.Random(seed)
    h = Harness(tmp_path / "crashy.db")
    ex = h.start(definition, inp)
    crashes = 0
    for _ in range(rng.randint(1, 4)):
        store = h.store(CrashInjector(at=rng.randint(1, points - 1)))
        try:
            drive(store, registry, ex)
            break
        except SimulatedCrash:
            crashes += 1
        finally:
            store.close()
    final = drive(h.store(), registry, ex)

    assert crashes >= 1
    assert (final.status, final.output, final.error) == (baseline.status, baseline.output, baseline.error)
    effects = h.store().effects()
    assert Counter(a for _, a, _ in effects) == baseline_effects
    assert len({k for k, _, _ in effects}) == len(effects)


def test_crashes_actually_land_mid_run(tmp_path):
    """Guard against a vacuous property: most random crash points must interrupt a live run."""
    _, points, _ = clean_run(Harness(tmp_path / "clean.db"), "kitchen_sink")
    assert points > 100


def test_recorded_decisions_win_over_changed_code(h, monkeypatch):
    """Replay reads past state results from history, so a code change cannot reroute a running execution."""
    defn = machine(
        "Route",
        Route={"Type": "Choice", "Choices": [{"Variable": "$.vip", "BooleanEquals": True, "Next": "Fast"}],
               "Default": "Slow"},
        Fast=task("fast", End=True),
        Slow=task("slow", End=True),
    )
    reg = handlers(fast=lambda i, c: "fast lane", slow=lambda i, c: "slow lane")
    store = h.store()
    ex = Engine(store).start(defn, {"vip": True})
    Engine(store).decide(ex)
    monkeypatch.setattr("dwe.engine.evaluate", lambda rule, data: False)
    assert drive(h.store(), reg, ex).output == "fast lane"
