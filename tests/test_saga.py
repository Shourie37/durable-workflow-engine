import pytest

from conftest import load_example
from dwe.examples.activities import REGISTRY


def run_saga(h, **inp):
    ex, d = h.run(load_example("saga"), {"trip_id": "T-1", **inp}, REGISTRY)
    store = h.store()
    tasks = [e.data["activity"] for e in h.events(ex, "TaskScheduled")]
    return d, tasks, [activity for _, activity, _ in store.effects()]


def test_happy_path_books_everything_without_compensation(h):
    d, tasks, effects = run_saga(h)
    assert d.status == "SUCCEEDED"
    assert d.output["car"] == {"kind": "car", "confirmation": "CAR-T-1"}
    assert effects == ["book_flight", "book_hotel", "book_car"]


def test_car_failure_compensates_in_reverse_order(h):
    d, tasks, effects = run_saga(h, fail_at="car")
    assert (d.status, d.error) == ("FAILED", "TripBookingFailed")
    assert tasks == ["book_flight", "book_hotel", "book_car", "cancel_hotel", "cancel_flight"]
    assert effects == ["book_flight", "book_hotel", "cancel_hotel", "cancel_flight"]


def test_hotel_failure_only_cancels_flight(h):
    d, _, effects = run_saga(h, fail_at="hotel")
    assert d.error == "TripBookingFailed"
    assert effects == ["book_flight", "cancel_flight"]


def test_flight_failure_needs_no_compensation(h):
    d, _, effects = run_saga(h, fail_at="flight")
    assert d.error == "TripBookingFailed"
    assert effects == []


@pytest.mark.parametrize("kind", ["flight", "hotel", "car"])
def test_transient_failure_is_retried_not_compensated(h, kind):
    d, tasks, effects = run_saga(h, flaky=kind)
    assert d.status == "SUCCEEDED"
    assert tasks.count(f"book_{kind}") == 2
    assert not any(t.startswith("cancel") for t in tasks)
