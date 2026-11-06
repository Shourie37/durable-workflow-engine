from conftest import handlers, load_example, machine, task
from dwe import CrashInjector, Engine, SimulatedCrash, Worker, drive
from dwe.examples.activities import REGISTRY


def test_redelivered_task_applies_side_effect_once(h):
    calls = []

    def charge(inp, ctx):
        calls.append(ctx.delivery)
        return ctx.effect(lambda: {"charged": inp["amount"]})

    reg = handlers(charge=charge)
    store = h.store()
    ex = Engine(store).start(machine("C", C=task("charge", End=True)), {"amount": 42})
    Engine(store).decide(ex)

    crashing = h.store(CrashInjector(on="worker:before_report"))
    try:
        Worker(crashing, reg, lease_seconds=5).run_once()
        raise AssertionError("expected a crash after the side effect")
    except SimulatedCrash as exc:
        assert "worker:before_report" in str(exc)

    h.clock.advance(5)
    d = drive(h.store(), reg, ex)
    assert d.output == {"charged": 42}
    assert calls == [1, 2]
    assert [k for k, _, _ in h.store().effects()] == [f"{ex}/0.C"]


def test_first_result_wins_for_a_token(h):
    store = h.store()
    ex = Engine(store).start(machine("A", A=task("a", End=True)), {})
    Engine(store).decide(ex)
    t = store.lease_task("w1", 30)
    assert store.complete_task(t.token, "first")
    assert not store.complete_task(t.token, "second")
    assert not store.complete_task(t.token, error="Late")
    assert Engine(store).decide(ex).output == "first"


def test_task_tokens_are_stable_across_replays(h):
    store = h.store()
    ex = Engine(store).start(load_example("saga"), {"trip_id": "T"})
    Engine(store).decide(ex)
    Engine(h.store()).decide(ex)
    Engine(h.store()).decide(ex)
    assert len(store.tasks(ex)) == 1
    assert len(h.events(ex, "TaskScheduled")) == 1


def test_crash_inside_transaction_rolls_back_event_and_task_together(h):
    store = h.store(CrashInjector(at=2))
    ex = Engine(h.store()).start(machine("A", A=task("a", End=True)), {})
    try:
        Engine(store).decide(ex)
    except SimulatedCrash:
        pass
    fresh = h.store()
    scheduled = [e for e in fresh.load_events(ex) if e.type == "TaskScheduled"]
    assert len(scheduled) == len(fresh.tasks(ex))
    d = drive(fresh, handlers(a=lambda i, c: "ok"), ex)
    assert d.output == "ok"


def test_finished_history_replays_with_no_new_decisions(h):
    ex, d = h.run(load_example("saga"), {"trip_id": "T", "fail_at": "car"}, REGISTRY)
    before = len(h.events(ex))
    store = h.store()
    store.get_execution(ex)
    store.conn.execute("UPDATE executions SET status='RUNNING' WHERE id=?", (ex,))
    store.conn.execute("DELETE FROM events WHERE execution_id=? AND key='end'", (ex,))
    again = Engine(store).decide(ex)
    assert (again.status, again.error) == (d.status, d.error)
    assert again.new_events == 1
    assert len(h.events(ex)) == before
