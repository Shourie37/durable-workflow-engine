from conftest import handlers, machine, task
from dwe import TaskError


def branch(resource: str) -> dict:
    return machine("T", T=task(resource, End=True))


def test_parallel_joins_branch_outputs_in_branch_order(h):
    defn = machine("P", P={"Type": "Parallel", "Branches": [branch("slow"), branch("fast")],
                           "ResultPath": "$.out", "End": True})
    reg = handlers(slow=lambda i, c: "slow", fast=lambda i, c: "fast")
    ex, d = h.run(defn, {"k": 1}, reg)
    assert d.output == {"k": 1, "out": ["slow", "fast"]}
    entered = [e.path for e in h.events(ex, "TaskScheduled")]
    assert len(entered) == 2


def test_parallel_waits_for_every_branch(h):
    defn = machine("P", P={"Type": "Parallel", "End": True, "Branches": [
        machine("W", W={"Type": "Wait", "Seconds": 30, "Next": "T"}, T=task("a", End=True)),
        branch("b")]})
    store = h.store()
    from dwe import Engine, Worker
    ex = Engine(store).start(defn, {})
    Engine(store).decide(ex)
    Worker(store, handlers(a=lambda i, c: "a", b=lambda i, c: "b")).drain()
    assert Engine(store).decide(ex).status == "RUNNING"
    h.clock.advance(30)
    Engine(store).decide(ex)
    Worker(store, handlers(a=lambda i, c: "a", b=lambda i, c: "b")).drain()
    assert Engine(store).decide(ex).output == ["a", "b"]


def test_failed_branch_fails_parallel_and_catch_routes(h):
    def bad(i, c):
        raise TaskError("BranchBroke")

    defn = machine("P", P={"Type": "Parallel", "Branches": [branch("ok"), branch("bad")],
                           "Catch": [{"ErrorEquals": ["BranchBroke"], "Next": "Recover"}], "Next": "Recover"},
                   Recover={"Type": "Pass", "Result": "recovered", "End": True})
    _, d = h.run(defn, {}, handlers(ok=lambda i, c: 1, bad=bad))
    assert d.output == "recovered"


def test_parallel_retry_reruns_all_branches(h):
    calls = []

    def sometimes(i, c):
        calls.append(c.task.path)
        if len(calls) == 1:
            raise TaskError("Transient")
        return "ok"

    defn = machine("P", P={"Type": "Parallel", "End": True, "Branches": [branch("s")],
                           "Retry": [{"ErrorEquals": ["Transient"], "IntervalSeconds": 1}]})
    _, d = h.run(defn, {}, handlers(s=sometimes))
    assert d.output == ["ok"]
    assert calls[0].startswith("/0.P#1/") and calls[1].startswith("/0.P#2/")
