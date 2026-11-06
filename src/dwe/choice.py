"""Choice rule evaluation."""

import fnmatch
from collections.abc import Callable
from typing import Any

from . import jsonpath


def _num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _str(v: Any) -> bool:
    return isinstance(v, str)


def _bool(v: Any) -> bool:
    return isinstance(v, bool)


_OPS: dict[str, tuple[Callable[[Any], bool], Callable[[Any, Any], bool]]] = {
    "StringEquals": (_str, lambda a, b: a == b),
    "StringLessThan": (_str, lambda a, b: a < b),
    "StringGreaterThan": (_str, lambda a, b: a > b),
    "StringLessThanEquals": (_str, lambda a, b: a <= b),
    "StringGreaterThanEquals": (_str, lambda a, b: a >= b),
    "StringMatches": (_str, lambda a, b: fnmatch.fnmatchcase(a, b)),
    "NumericEquals": (_num, lambda a, b: a == b),
    "NumericLessThan": (_num, lambda a, b: a < b),
    "NumericGreaterThan": (_num, lambda a, b: a > b),
    "NumericLessThanEquals": (_num, lambda a, b: a <= b),
    "NumericGreaterThanEquals": (_num, lambda a, b: a >= b),
    "BooleanEquals": (_bool, lambda a, b: a == b),
}

_TYPE_TESTS: dict[str, Callable[[Any], bool]] = {
    "IsNull": lambda v: v is None,
    "IsNumeric": _num,
    "IsString": _str,
    "IsBoolean": _bool,
}

OPERATORS = set(_OPS) | {op + "Path" for op in _OPS} | set(_TYPE_TESTS) | {"IsPresent", "And", "Or", "Not"}


def evaluate(rule: dict[str, Any], data: Any) -> bool:
    if "And" in rule:
        return all(evaluate(r, data) for r in rule["And"])
    if "Or" in rule:
        return any(evaluate(r, data) for r in rule["Or"])
    if "Not" in rule:
        return not evaluate(rule["Not"], data)
    var = rule["Variable"]
    if "IsPresent" in rule:
        return jsonpath.exists(data, var) == rule["IsPresent"]
    if not jsonpath.exists(data, var):
        return False
    value = jsonpath.get(data, var)
    for name, test in _TYPE_TESTS.items():
        if name in rule:
            return test(value) == rule[name]
    for name, (type_ok, compare) in _OPS.items():
        if name in rule:
            other = rule[name]
        elif name + "Path" in rule:
            other = jsonpath.get(data, rule[name + "Path"])
        else:
            continue
        return type_ok(value) and type_ok(other) and compare(value, other)
    raise ValueError(f"choice rule has no known operator: {rule}")
