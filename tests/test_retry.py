import pytest

from conftest import handlers, machine, task
from dwe import TaskError
from dwe.engine import error_matches, retry_delay


def flaky(failures: int, error: str = "Transient"):
    def fn(inp, ctx):
        if ctx.attempt <= failures:
            raise TaskError(error, f"attempt {ctx.attempt}")
        return {"attempt": ctx.attempt}
    return fn


def test_retries_with_exponential_backoff(h):
    defn = machine("A", A=task("a", End=True, Retry=[
        {"ErrorEquals": ["Transient"], "IntervalSeconds": 1, "BackoffRate": 2, "MaxAttempts": 3}]))
    ex, d = h.run(defn, {}, handlers(a=flaky(2)))
    assert d.status == "SUCCEEDED"
    assert d.output == {"attempt": 3}
    retries = h.events(ex, "RetryScheduled")
    assert [r.data["delay"] for r in retries] == [1.0, 2.0]
    failed = {e.data["attempt"]: e.ts for e in h.events(ex, "AttemptFailed")}
    scheduled = {e.data["attempt"]: e.ts for e in h.events(ex, "TaskScheduled")}
    assert scheduled[2] - failed[1] >= 1.0
    assert scheduled[3] - failed[2] >= 2.0


def test_retries_exhausted_fails_execution(h):
    defn = machine("A", A=task("a", End=True, Retry=[{"ErrorEquals": ["Transient"], "MaxAttempts": 2,
                                                      "IntervalSeconds": 0.1}]))
    ex, d = h.run(defn, {}, handlers(a=flaky(10)))
    assert (d.status, d.error) == ("FAILED", "Transient")
    assert len(h.events(ex, "TaskScheduled")) == 3


def test_retrier_only_matches_listed_errors(h):
    defn = machine("A", A=task("a", End=True, Retry=[{"ErrorEquals": ["Transient"], "MaxAttempts": 5}]))
    ex, d = h.run(defn, {}, handlers(a=flaky(1, error="Fatal")))
    assert (d.status, d.error) == ("FAILED", "Fatal")
    assert h.events(ex, "RetryScheduled") == []


def test_first_matching_retrier_wins_and_counts_separately(h):
    seen = []

    def fn(inp, ctx):
        seen.append(ctx.attempt)
        if ctx.attempt <= 2:
            raise TaskError("Throttled")
        if ctx.attempt == 3:
            raise TaskError("Transient")
        return "ok"

    defn = machine("A", A=task("a", End=True, Retry=[
        {"ErrorEquals": ["Throttled"], "MaxAttempts": 2, "IntervalSeconds": 5, "BackoffRate": 1},
        {"ErrorEquals": ["States.ALL"], "MaxAttempts": 1, "IntervalSeconds": 0.5}]))
    ex, d = h.run(defn, {}, handlers(a=fn))
    assert d.output == "ok"
    assert [r.data["delay"] for r in h.events(ex, "RetryScheduled")] == [5.0, 5.0, 0.5]


def test_max_attempts_zero_never_retries(h):
    defn = machine("A", A=task("a", End=True, Retry=[{"ErrorEquals": ["States.ALL"], "MaxAttempts": 0}]))
    _, d = h.run(defn, {}, handlers(a=flaky(1)))
    assert d.status == "FAILED"


def test_backoff_cap_and_full_jitter_are_deterministic():
    r = {"IntervalSeconds": 1, "BackoffRate": 3, "MaxDelaySeconds": 5}
    assert [retry_delay(r, n, "s") for n in (1, 2, 3, 4)] == [1, 3, 5, 5]
    jr = {**r, "JitterStrategy": "FULL"}
    samples = [retry_delay(jr, 3, f"seed-{i}") for i in range(200)]
    assert all(0 <= s <= 5 for s in samples)
    assert len(set(samples)) > 150
    assert retry_delay(jr, 3, "seed-7") == retry_delay(jr, 3, "seed-7")


@pytest.mark.parametrize("equals,error,expected", [
    (["States.ALL"], "Anything", True),
    (["States.TaskFailed"], "Boom", True),
    (["States.TaskFailed"], "States.Timeout", False),
    (["States.Timeout"], "States.Timeout", True),
    (["A", "B"], "C", False),
])
def test_error_matching(equals, error, expected):
    assert error_matches(equals, error) is expected
