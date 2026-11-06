"""The decider: rebuilds execution state from the event history and records the next decisions.

Every decision has a deterministic key, derived from the state's position in the run
(scope path + step index + attempt). A pass over a history that already holds a decision
reads it back instead of making it again, so a pass can run any number of times, from any
process, after a crash at any point, and it always converges on the same result.
"""

import hashlib
import random
from dataclasses import dataclass
from typing import Any

from . import jsonpath
from .choice import evaluate
from .definition import validate
from .store import Event, Execution, Store, TaskSpec

PENDING = object()
MAX_STEPS = 10_000


@dataclass(frozen=True)
class Ok:
    output: Any
    next: str | None


@dataclass(frozen=True)
class Err:
    error: str
    cause: str = ""


@dataclass(frozen=True)
class Caught:
    output: Any
    next: str
    error: str


@dataclass(frozen=True)
class Decision:
    status: str
    output: Any = None
    error: str | None = None
    cause: str | None = None
    new_events: int = 0
    next_timer: float | None = None

    @property
    def done(self) -> bool:
        return self.status != "RUNNING"


def task_token(execution_id: str, attempt_path: str) -> str:
    return hashlib.sha256(f"{execution_id}|{attempt_path}".encode()).hexdigest()[:32]


def error_matches(error_equals: list[str], error: str) -> bool:
    if "States.ALL" in error_equals or error in error_equals:
        return True
    return "States.TaskFailed" in error_equals and error != "States.Timeout"


def retry_delay(retrier: dict[str, Any], retry_number: int, seed: str) -> float:
    """Exponential backoff with an optional cap and full jitter. The jitter is seeded, so replay is stable."""
    delay = float(retrier.get("IntervalSeconds", 1)) * float(retrier.get("BackoffRate", 2.0)) ** (retry_number - 1)
    if "MaxDelaySeconds" in retrier:
        delay = min(delay, float(retrier["MaxDelaySeconds"]))
    if retrier.get("JitterStrategy") == "FULL":
        delay = random.Random(seed).uniform(0, delay)
    return delay


class Engine:
    def __init__(self, store: Store) -> None:
        self.store = store

    def start(self, definition: dict[str, Any], input: Any = None, name: str | None = None) -> str:
        validate(definition)
        return self.store.create_execution(definition, {} if input is None else input, name)

    def decide(self, execution_id: str) -> Decision:
        ex = self.store.get_execution(execution_id)
        if ex.status != "RUNNING":
            return Decision(ex.status, ex.output, ex.error, ex.cause)
        return _Pass(self.store, ex).run()


class _Pass:
    def __init__(self, store: Store, ex: Execution) -> None:
        self.store = store
        self.ex = ex
        self.events: dict[str, Event] = {e.key: e for e in store.load_events(ex.id)}
        self.new_events = 0
        self.timers: list[float] = []
        self.context = {"Execution": {"Id": ex.id, "Input": ex.input, "StartTime": ex.started_at}}

    def run(self) -> Decision:
        result = self.run_scope("", self.ex.definition, self.ex.input)
        if result is PENDING:
            return Decision("RUNNING", new_events=self.new_events, next_timer=min(self.timers, default=None))
        if isinstance(result, Err):
            self.store.close_execution(self.ex.id, "FAILED", error=result.error, cause=result.cause)
            return Decision("FAILED", error=result.error, cause=result.cause, new_events=self.new_events + 1)
        self.store.close_execution(self.ex.id, "SUCCEEDED", output=result.output)
        return Decision("SUCCEEDED", output=result.output, new_events=self.new_events + 1)

    # history helpers

    def now(self) -> float:
        return self.store.clock.now()

    def record(self, key: str, type_: str, path: str, data: dict, task: TaskSpec | None = None,
               cancel_prefix: str | None = None) -> Event:
        existing = self.events.get(key)
        if existing is not None:
            return existing
        self.store.crash.point(f"decide:{type_}")
        event = self.store.append_event(self.ex.id, key, type_, path, data, task=task, cancel_prefix=cancel_prefix)
        self.events[key] = event
        self.new_events += 1
        return event

    # scopes and states

    def run_scope(self, scope: str, machine: dict[str, Any], data: Any) -> Any:
        name = machine["StartAt"]
        for step in range(MAX_STEPS):
            state = machine["States"][name]
            result = self.run_state(f"{scope}/{step}.{name}", name, state, data)
            if result is PENDING or isinstance(result, Err):
                return result
            if result.next is None:
                return result
            name, data = result.next, result.output
        return Err("States.Runtime", f"more than {MAX_STEPS} transitions in one scope")

    def run_state(self, path: str, name: str, state: dict[str, Any], data: Any) -> Any:
        exited = self.events.get("exit:" + path)
        if exited is not None:
            d = exited.data
            return Err(d["error"], d["cause"]) if "error" in d else Ok(d["output"], d["next"])
        kind = state["Type"]
        self.record("enter:" + path, "StateEntered", path, {"name": name, "type": kind, "input": data})
        try:
            result = getattr(self, "_" + kind.lower())(path, state, data)
        except jsonpath.PathError as exc:
            result = Err("States.Runtime", str(exc))
        if result is PENDING:
            return result
        info: dict[str, Any] = {"name": name, "type": kind}
        if isinstance(result, Caught):
            info["caught"] = result.error
            result = Ok(result.output, result.next)
        if isinstance(result, Err):
            info.update(error=result.error, cause=result.cause)
        else:
            info.update(output=result.output, next=result.next)
        self.record("exit:" + path, "StateExited", path, info)
        return result

    # input and output processing

    def effective_input(self, state: dict, data: Any) -> Any:
        value = _input_path(state, data)
        if "Parameters" in state:
            value = jsonpath.render(state["Parameters"], value, self.context)
        return value

    def finish(self, state: dict, data: Any, result: Any) -> Any:
        if result is PENDING or isinstance(result, (Err, Caught)):
            return result
        if "ResultSelector" in state:
            result = jsonpath.render(state["ResultSelector"], result, self.context)
        out = jsonpath.set_path(data, state.get("ResultPath", "$"), result)
        return Ok(_output_path(state, out), _next(state))

    # retry and catch

    def with_retry(self, path: str, state: dict, data: Any, attempt_fn: Any) -> Any:
        retriers = state.get("Retry", [])
        used = [0] * len(retriers)
        attempt = 1
        while True:
            apath = f"{path}#{attempt}"
            failed = self.events.get("failed:" + apath)
            if failed is None:
                result = attempt_fn(apath, attempt)
                if result is PENDING or not isinstance(result, Err):
                    return result
                failed = self.record("failed:" + apath, "AttemptFailed", path,
                                     {"attempt": attempt, "error": result.error, "cause": result.cause},
                                     cancel_prefix=apath + "/")
            err = Err(failed.data["error"], failed.data["cause"])
            idx = next((i for i, r in enumerate(retriers) if error_matches(r["ErrorEquals"], err.error)), None)
            if idx is not None and used[idx] < retriers[idx].get("MaxAttempts", 3):
                used[idx] += 1
                delay = retry_delay(retriers[idx], used[idx], f"{self.ex.id}|{apath}")
                not_before = failed.ts + delay
                self.record("retry:" + apath, "RetryScheduled", path,
                            {"attempt": attempt + 1, "delay": delay, "error": err.error, "not_before": not_before})
                attempt += 1
                if not self.gate(f"{path}#{attempt}", not_before):
                    return PENDING
                continue
            for catcher in state.get("Catch", []):
                if error_matches(catcher["ErrorEquals"], err.error):
                    value = {"Error": err.error, "Cause": err.cause}
                    return Caught(jsonpath.set_path(data, catcher.get("ResultPath", "$"), value),
                                  catcher["Next"], err.error)
            return err

    def gate(self, apath: str, not_before: float) -> bool:
        """Hold a retry attempt until its backoff has passed."""
        if "begin:" + apath in self.events:
            return True
        if self.now() < not_before:
            self.timers.append(not_before)
            return False
        self.record("begin:" + apath, "AttemptStarted", apath, {})
        return True

    # state types

    def _pass(self, path: str, state: dict, data: Any) -> Any:
        value = self.effective_input(state, data)
        return self.finish(state, data, state["Result"] if "Result" in state else value)

    def _succeed(self, path: str, state: dict, data: Any) -> Any:
        return Ok(_output_path(state, _input_path(state, data)), None)

    def _fail(self, path: str, state: dict, data: Any) -> Any:
        return Err(state.get("Error", "States.Fail"), state.get("Cause", ""))

    def _choice(self, path: str, state: dict, data: Any) -> Any:
        value = _input_path(state, data)
        for rule in state["Choices"]:
            if evaluate(rule, value):
                return Ok(_output_path(state, value), rule["Next"])
        if "Default" in state:
            return Ok(_output_path(state, value), state["Default"])
        return Err("States.NoChoiceMatched", "no Choice rule matched and there is no Default")

    def _wait(self, path: str, state: dict, data: Any) -> Any:
        value = _input_path(state, data)
        started = self.events.get("timer:" + path)
        if started is None:
            seconds = state["Seconds"] if "Seconds" in state else jsonpath.get(value, state["SecondsPath"])
            started = self.record("timer:" + path, "TimerStarted", path,
                                  {"seconds": seconds, "fire_at": self.now() + float(seconds)})
        if "fired:" + path not in self.events:
            fire_at = started.data["fire_at"]
            if self.now() < fire_at:
                self.timers.append(fire_at)
                return PENDING
            self.record("fired:" + path, "TimerFired", path, {})
        return Ok(_output_path(state, value), _next(state))

    def _task(self, path: str, state: dict, data: Any) -> Any:
        value = self.effective_input(state, data)

        def attempt(apath: str, n: int) -> Any:
            token = task_token(self.ex.id, apath)
            result = self.events.get("result:" + token)
            if result is None:
                spec = TaskSpec(token=token, path=apath, activity=state["Resource"], input=value, attempt=n,
                                idempotency_key=f"{self.ex.id}{path}", available_at=self.now(),
                                timeout=state.get("TimeoutSeconds"), heartbeat=state.get("HeartbeatSeconds"))
                self.record("sched:" + apath, "TaskScheduled", apath,
                            {"activity": spec.activity, "attempt": n, "token": token}, task=spec)
                return PENDING
            if result.type == "TaskSucceeded":
                return result.data["output"]
            return Err(result.data["error"], result.data.get("cause", ""))

        return self.finish(state, data, self.with_retry(path, state, data, attempt))

    def _parallel(self, path: str, state: dict, data: Any) -> Any:
        value = self.effective_input(state, data)

        def attempt(apath: str, n: int) -> Any:
            outputs, errors, pending = [], [], False
            for i, branch in enumerate(state["Branches"]):
                result = self.run_scope(f"{apath}/b{i}", branch, value)
                if result is PENDING:
                    pending = True
                elif isinstance(result, Err):
                    errors.append(result)
                else:
                    outputs.append(result.output)
            if errors:
                return errors[0]
            return PENDING if pending else outputs

        return self.finish(state, data, self.with_retry(path, state, data, attempt))

    def _map(self, path: str, state: dict, data: Any) -> Any:
        value = _input_path(state, data)
        processor = state.get("ItemProcessor") or state["Iterator"]
        selector = state.get("ItemSelector", state.get("Parameters"))

        def attempt(apath: str, n: int) -> Any:
            items = jsonpath.get(value, state.get("ItemsPath", "$"))
            if not isinstance(items, list):
                return Err("States.Runtime", "ItemsPath must resolve to a list")
            total, limit = len(items), int(state.get("MaxConcurrency") or 0)
            outcomes: dict[int, Any] = {}
            running = started = 0

            def failures() -> list[int]:
                return sorted(i for i, o in outcomes.items() if isinstance(o, Err))

            while started < total:
                is_started = f"item:{apath}:{started}" in self.events
                if not is_started:
                    if (limit and running >= limit) or _tolerance_exceeded(state, len(failures()), total):
                        break
                    self.record(f"item:{apath}:{started}", "MapItemStarted", f"{apath}/i{started}",
                                {"index": started})
                result = self.run_item(apath, started, items[started], processor, selector)
                if result is PENDING:
                    running += 1
                else:
                    outcomes[started] = result
                started += 1

            failed = failures()
            if _tolerance_exceeded(state, len(failed), total):
                if not _has_tolerance(state):
                    return outcomes[failed[0]]
                return Err("States.ExceedToleratedFailureThreshold",
                           f"{len(failed)} of {total} items failed, above the tolerated threshold")
            if running or started < total:
                return PENDING
            return [o.output if isinstance(o, Ok) else {"Error": o.error, "Cause": o.cause}
                    for _, o in sorted(outcomes.items())]

        return self.finish(state, data, self.with_retry(path, state, data, attempt))

    def run_item(self, apath: str, index: int, item: Any, processor: dict, selector: Any) -> Any:
        done = self.events.get(f"itemdone:{apath}:{index}")
        if done is not None:
            d = done.data
            return Ok(d["output"], None) if done.type == "MapItemSucceeded" else Err(d["error"], d["cause"])
        if selector is None:
            item_input = item
        else:
            context = {**self.context, "Map": {"Item": {"Index": index, "Value": item}}}
            item_input = jsonpath.render(selector, item, context)
        result = self.run_scope(f"{apath}/i{index}", processor, item_input)
        if result is PENDING:
            return result
        if isinstance(result, Err):
            self.record(f"itemdone:{apath}:{index}", "MapItemFailed", f"{apath}/i{index}",
                        {"error": result.error, "cause": result.cause})
        else:
            self.record(f"itemdone:{apath}:{index}", "MapItemSucceeded", f"{apath}/i{index}",
                        {"output": result.output})
        return result


def _input_path(state: dict, data: Any) -> Any:
    path = state.get("InputPath", "$")
    return {} if path is None else jsonpath.get(data, path)


def _output_path(state: dict, data: Any) -> Any:
    path = state.get("OutputPath", "$")
    return {} if path is None else jsonpath.get(data, path)


def _next(state: dict) -> str | None:
    return None if state.get("End") else state.get("Next")


def _has_tolerance(state: dict) -> bool:
    return state.get("ToleratedFailureCount") is not None or state.get("ToleratedFailurePercentage") is not None


def _tolerance_exceeded(state: dict, failed: int, total: int) -> bool:
    if not _has_tolerance(state):
        return failed > 0
    count, pct = state.get("ToleratedFailureCount"), state.get("ToleratedFailurePercentage")
    return (count is not None and failed > count) or (pct is not None and failed * 100 > pct * total)
