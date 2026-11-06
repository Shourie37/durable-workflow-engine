from conftest import handlers, machine, task
from dwe import Engine, Worker


def setup(h, state: dict, fn):
    store = h.store()
    ex = Engine(store).start(machine("A", A=state), {})
    Engine(store).decide(ex)
    return store, ex, handlers(a=fn)


def test_expired_lease_is_redispatched_to_another_worker(h):
    store, ex, reg = setup(h, task("a", End=True), lambda i, c: {"by": c.delivery})
    leased = store.lease_task("worker-dead", lease_seconds=10)
    assert leased is not None and leased.deliveries == 1
    assert Worker(store, reg, owner="worker-b").run_once() is False
    h.clock.advance(10)
    assert store.reap() == 1
    assert Worker(store, reg, owner="worker-b").run_once() is True
    d = Engine(store).decide(ex)
    assert d.output == {"by": 2}
    assert [e.data["owner"] for e in h.events(ex, "LeaseExpired")] == ["worker-dead"]
    assert [e.data["owner"] for e in h.events(ex, "TaskStarted")] == ["worker-dead", "worker-b"]


def test_heartbeat_keeps_lease_alive(h):
    store, ex, reg = setup(h, task("a", End=True, HeartbeatSeconds=5), lambda i, c: 1)
    t = store.lease_task("w1", lease_seconds=60)
    for _ in range(4):
        h.clock.advance(4)
        assert store.heartbeat(t.token, "w1", 60)
        assert store.reap() == 0
    h.clock.advance(6)
    assert store.reap() == 1
    assert not store.heartbeat(t.token, "w1", 60)


def test_task_timeout_fails_attempt_and_retry_runs(h):
    calls = []

    def fn(i, c):
        calls.append(c.attempt)
        return "done"

    state = task("a", End=True, TimeoutSeconds=3, Retry=[{"ErrorEquals": ["States.Timeout"], "IntervalSeconds": 1}])
    store, ex, reg = setup(h, state, fn)
    hung = store.lease_task("w-hung", lease_seconds=60)
    h.clock.advance(3)
    store.reap()
    assert not store.complete_task(hung.token, "late result")
    from dwe import drive
    d = drive(store, reg, ex)
    assert d.output == "done"
    assert calls == [2]
    failed = h.events(ex, "TaskFailed")
    assert failed[0].data["error"] == "States.Timeout"


def test_timeout_can_be_caught(h):
    state = task("a", TimeoutSeconds=2, Catch=[{"ErrorEquals": ["States.Timeout"], "Next": "Slow"}], Next="Slow")
    store = h.store()
    ex = Engine(store).start(machine("A", A=state, Slow={"Type": "Pass", "Result": "fallback", "End": True}), {})
    Engine(store).decide(ex)
    store.lease_task("w", lease_seconds=60)
    h.clock.advance(2)
    store.reap()
    assert Engine(store).decide(ex).output == "fallback"
