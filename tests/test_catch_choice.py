import pytest

from conftest import handlers, machine, task
from dwe import DefinitionError, TaskError


def boom(error: str):
    def fn(inp, ctx):
        raise TaskError(error, "it broke")
    return fn


def test_catch_routes_to_handler_with_error_in_result_path(h):
    defn = machine(
        "A",
        A=task("a", Catch=[{"ErrorEquals": ["Other"], "Next": "Wrong"},
                           {"ErrorEquals": ["Boom"], "ResultPath": "$.err", "Next": "Handle"}], Next="Wrong"),
        Handle={"Type": "Pass", "End": True},
        Wrong={"Type": "Fail", "Error": "WrongRoute"},
    )
    ex, d = h.run(defn, {"order": 1}, handlers(a=boom("Boom")))
    assert d.status == "SUCCEEDED"
    assert d.output == {"order": 1, "err": {"Error": "Boom", "Cause": "it broke"}}


def test_catch_runs_after_retries_are_exhausted(h):
    defn = machine(
        "A",
        A=task("a", Retry=[{"ErrorEquals": ["Boom"], "MaxAttempts": 2, "IntervalSeconds": 0.1}],
               Catch=[{"ErrorEquals": ["States.ALL"], "ResultPath": None, "Next": "Done"}], Next="Done"),
        Done={"Type": "Succeed"},
    )
    ex, d = h.run(defn, {"x": 1}, handlers(a=boom("Boom")))
    assert d.output == {"x": 1}
    assert len(h.events(ex, "TaskScheduled")) == 3


def test_uncaught_error_fails_execution(h):
    defn = machine("A", A=task("a", Catch=[{"ErrorEquals": ["Other"], "Next": "B"}], Next="B"),
                   B={"Type": "Succeed"})
    _, d = h.run(defn, {}, handlers(a=boom("Boom")))
    assert (d.status, d.error, d.cause) == ("FAILED", "Boom", "it broke")


def choice_machine() -> dict:
    return machine(
        "Route",
        Route={"Type": "Choice", "Choices": [
            {"And": [{"Variable": "$.amount", "NumericGreaterThanEquals": 100},
                     {"Not": {"Variable": "$.vip", "BooleanEquals": True}}], "Next": "Review"},
            {"Variable": "$.country", "StringMatches": "U*", "Next": "Domestic"},
            {"Variable": "$.limit", "IsPresent": True, "Next": "Limited"},
        ], "Default": "Other"},
        Review={"Type": "Pass", "Result": "review", "End": True},
        Domestic={"Type": "Pass", "Result": "domestic", "End": True},
        Limited={"Type": "Pass", "Result": "limited", "End": True},
        Other={"Type": "Pass", "Result": "other", "End": True},
    )


@pytest.mark.parametrize("inp,expected", [
    ({"amount": 150, "vip": False, "country": "US"}, "review"),
    ({"amount": 150, "vip": True, "country": "US"}, "domestic"),
    ({"amount": "150", "country": "FR", "limit": 3}, "limited"),
    ({"amount": 5, "country": "FR"}, "other"),
])
def test_choice_routes(h, inp, expected):
    _, d = h.run(choice_machine(), inp, handlers())
    assert d.output == expected


def test_choice_without_match_or_default_fails(h):
    defn = machine("C", C={"Type": "Choice", "Choices": [{"Variable": "$.x", "NumericEquals": 1, "Next": "E"}]},
                   E={"Type": "Succeed"})
    _, d = h.run(defn, {"x": 2}, handlers())
    assert d.error == "States.NoChoiceMatched"


def test_choice_loop_gives_each_visit_its_own_history(h):
    defn = machine(
        "Inc",
        Inc=task("inc", Next="Check"),
        Check={"Type": "Choice", "Choices": [{"Variable": "$.n", "NumericLessThan": 3, "Next": "Inc"}],
               "Default": "Done"},
        Done={"Type": "Succeed"},
    )
    ex, d = h.run(defn, {"n": 0}, handlers(inc=lambda inp, ctx: {"n": inp["n"] + 1}))
    assert d.output == {"n": 3}
    assert len(h.events(ex, "TaskScheduled")) == 3


def test_input_output_processing(h):
    defn = machine("A", A=task("a", InputPath="$.req", Parameters={"q.$": "$.query", "fixed": 1},
                               ResultSelector={"rows.$": "$.rows"}, ResultPath="$.res", OutputPath="$.res",
                               End=True))
    _, d = h.run(defn, {"req": {"query": "x"}}, handlers(a=lambda inp, ctx: {"rows": [inp], "noise": 1}))
    assert d.output == {"rows": [{"q": "x", "fixed": 1}]}


def test_bad_path_is_a_catchable_runtime_error(h):
    defn = machine("A", A={"Type": "Pass", "InputPath": "$.missing", "Next": "B",
                           "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "B"}]}, B={"Type": "Succeed"})
    _, d = h.run(defn, {}, handlers())
    assert d.error == "States.Runtime"


def test_invalid_definition_is_rejected(h):
    with pytest.raises(DefinitionError) as info:
        h.start(machine("A", A=task("a", Next="Nowhere"), B={"Type": "Bogus"}))
    assert any("Nowhere" in p for p in info.value.problems)
    assert any("Bogus" in p for p in info.value.problems)
