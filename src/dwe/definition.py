"""Static checks for a state machine definition, run before an execution starts."""

from typing import Any

from .choice import OPERATORS

STATE_TYPES = {"Task", "Choice", "Parallel", "Map", "Wait", "Pass", "Succeed", "Fail"}
TERMINAL = {"Succeed", "Fail"}


class DefinitionError(ValueError):
    def __init__(self, problems: list[str]) -> None:
        super().__init__("invalid definition:\n  " + "\n  ".join(problems))
        self.problems = problems


def validate(definition: dict[str, Any]) -> None:
    problems: list[str] = []
    _check_machine(definition, "", problems)
    if problems:
        raise DefinitionError(problems)


def _check_machine(machine: Any, where: str, problems: list[str]) -> None:
    if not isinstance(machine, dict) or not isinstance(machine.get("States"), dict) or not machine["States"]:
        problems.append(f"{where or 'root'}: needs a non-empty 'States' object")
        return
    states = machine["States"]
    if machine.get("StartAt") not in states:
        problems.append(f"{where or 'root'}: StartAt {machine.get('StartAt')!r} is not a state")
    for name, state in states.items():
        _check_state(name, state, states, f"{where}/{name}", problems)


def _check_target(target: Any, states: dict, where: str, problems: list[str]) -> None:
    if target not in states:
        problems.append(f"{where}: Next {target!r} is not a state")


def _check_state(name: str, state: Any, states: dict, where: str, problems: list[str]) -> None:
    if not isinstance(state, dict) or state.get("Type") not in STATE_TYPES:
        problems.append(f"{where}: unknown Type {state.get('Type') if isinstance(state, dict) else state!r}")
        return
    kind = state["Type"]
    if kind == "Choice":
        if not state.get("Choices"):
            problems.append(f"{where}: Choice needs at least one rule")
        for i, rule in enumerate(state.get("Choices", [])):
            _check_target(rule.get("Next"), states, f"{where}/Choices[{i}]", problems)
            _check_rule(rule, f"{where}/Choices[{i}]", problems)
        if "Default" in state:
            _check_target(state["Default"], states, f"{where}/Default", problems)
    elif kind not in TERMINAL:
        if state.get("End") is True:
            if "Next" in state:
                problems.append(f"{where}: has both End and Next")
        else:
            _check_target(state.get("Next"), states, where, problems)
    if kind == "Task" and not isinstance(state.get("Resource"), str):
        problems.append(f"{where}: Task needs a string Resource")
    if kind == "Wait" and ("Seconds" in state) == ("SecondsPath" in state):
        problems.append(f"{where}: Wait needs exactly one of Seconds or SecondsPath")
    if kind == "Parallel":
        for i, branch in enumerate(state.get("Branches") or [None]):
            _check_machine(branch, f"{where}/Branches[{i}]", problems)
    if kind == "Map":
        _check_machine(state.get("ItemProcessor") or state.get("Iterator"), f"{where}/ItemProcessor", problems)
        pct = state.get("ToleratedFailurePercentage")
        if pct is not None and not 0 <= pct <= 100:
            problems.append(f"{where}: ToleratedFailurePercentage must be 0-100")
    for i, retrier in enumerate(state.get("Retry", [])):
        if not retrier.get("ErrorEquals"):
            problems.append(f"{where}/Retry[{i}]: needs ErrorEquals")
    for i, catcher in enumerate(state.get("Catch", [])):
        if not catcher.get("ErrorEquals"):
            problems.append(f"{where}/Catch[{i}]: needs ErrorEquals")
        _check_target(catcher.get("Next"), states, f"{where}/Catch[{i}]", problems)


def _check_rule(rule: dict, where: str, problems: list[str]) -> None:
    for key in ("And", "Or"):
        for sub in rule.get(key, []):
            _check_rule(sub, where, problems)
    if "Not" in rule:
        _check_rule(rule["Not"], where, problems)
    if not any(key in OPERATORS for key in rule):
        problems.append(f"{where}: rule has no known operator")
