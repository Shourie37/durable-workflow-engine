import pytest

from conftest import handlers, load_example, machine, task
from dwe import Engine, Store, SystemClock, TaskError, drive
from dwe.examples.activities import REGISTRY


def map_state(**extra) -> dict:
    return {"Type": "Map", "ItemsPath": "$.items", "End": True,
            "ItemProcessor": machine("Work", Work=task("work", End=True)), **extra}


def work(item, ctx):
    if item.get("bad"):
        raise TaskError("ItemFailed", f"item {item['id']}")
    return item["id"] * 10


def items(n: int, bad=()) -> dict:
    return {"items": [{"id": i, **({"bad": True} if i in bad else {})} for i in range(n)]}


def max_in_flight(events) -> int:
    running = peak = 0
    for e in events:
        if e.type == "MapItemStarted":
            running += 1
            peak = max(peak, running)
        elif e.type in ("MapItemSucceeded", "MapItemFailed"):
            running -= 1
    return peak


def test_map_preserves_item_order(h):
    _, d = h.run(machine("M", M=map_state()), items(5), handlers(work=work))
    assert d.output == [0, 10, 20, 30, 40]


@pytest.mark.parametrize("limit", [1, 3])
def test_map_respects_max_concurrency(h, limit):
    ex, d = h.run(machine("M", M=map_state(MaxConcurrency=limit)), items(8), handlers(work=work))
    assert d.status == "SUCCEEDED"
    assert max_in_flight(h.events(ex)) == limit


def test_map_marks_tolerated_failures_in_output(h):
    defn = machine("M", M=map_state(ToleratedFailurePercentage=25))
    ex, d = h.run(defn, items(8, bad={2, 5}), handlers(work=work))
    assert d.status == "SUCCEEDED"
    assert d.output[2] == {"Error": "ItemFailed", "Cause": "item 2"}
    assert [o for i, o in enumerate(d.output) if i not in (2, 5)] == [0, 10, 30, 40, 60, 70]
    assert len(h.events(ex, "MapItemFailed")) == 2


def test_map_exceeding_tolerance_fails_and_stops_new_items(h):
    defn = machine("M", M=map_state(MaxConcurrency=2, ToleratedFailureCount=1,
                                    Catch=[{"ErrorEquals": ["States.ExceedToleratedFailureThreshold"],
                                            "ResultPath": "$.error", "Next": "Rejected"}]),
                   Rejected={"Type": "Pass", "OutputPath": "$.error", "End": True})
    defn["States"]["M"].pop("End")
    defn["States"]["M"]["Next"] = "Rejected"
    ex, d = h.run(defn, items(10, bad={0, 1}), handlers(work=work))
    assert d.output["Error"] == "States.ExceedToleratedFailureThreshold"
    assert len(h.events(ex, "MapItemStarted")) < 10


def test_map_without_tolerance_fails_with_item_error(h):
    _, d = h.run(machine("M", M=map_state()), items(4, bad={1}), handlers(work=work))
    assert (d.status, d.error) == ("FAILED", "ItemFailed")


def test_map_item_selector_and_empty_list(h):
    defn = machine("M", M=map_state(ItemSelector={"idx.$": "$$.Map.Item.Index", "v.$": "$$.Map.Item.Value"}))
    _, d = h.run(defn, {"items": ["a", "b"]}, handlers(work=lambda i, c: f"{i['idx']}:{i['v']}"))
    assert d.output == ["0:a", "1:b"]
    _, d = h.run(machine("M", M=map_state()), {"items": []}, handlers())
    assert d.output == []


def test_map_example_with_real_threads(tmp_path):
    store = Store(tmp_path / "threads.db", clock=SystemClock())
    inp = {"items": [{"id": i, **({"corrupt": True} if i in (3, 11) else {})} for i in range(12)]}
    ex = Engine(store).start(load_example("map_analysis"), inp)
    d = drive(store, REGISTRY, ex, workers=4, lease_seconds=5)
    assert d.status == "SUCCEEDED"
    assert d.output["failed_ids"] == [3, 11]
    assert d.output["analyzed"] == 10
    assert max_in_flight(store.load_events(ex)) <= 4
