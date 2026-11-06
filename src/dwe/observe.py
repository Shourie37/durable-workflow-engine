"""Execution metrics, a text timeline, and a history dump, all derived from the event history."""

import json
import math
import re
from collections import Counter, defaultdict
from typing import Any

from .store import Event, Store

_SEGMENT = re.compile(r"^(\d+)\.(.+?)(?:#(\d+))?$")


def _percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    rank = max(1, math.ceil(pct / 100 * len(ordered)))
    return ordered[rank - 1]


def metrics(store: Store, execution_id: str) -> dict[str, Any]:
    ex = store.get_execution(execution_id)
    events = store.load_events(execution_id)
    types = Counter(e.type for e in events)
    scheduled = {e.data["token"]: e for e in events if e.type == "TaskScheduled"}
    latency: dict[str, list[float]] = defaultdict(list)
    timeouts = 0
    for e in events:
        if e.type in ("TaskSucceeded", "TaskFailed"):
            sched = scheduled.get(e.key.removeprefix("result:"))
            if sched is not None:
                latency[sched.data["activity"]].append(e.ts - sched.ts)
            timeouts += e.type == "TaskFailed" and e.data.get("error") == "States.Timeout"
    end = ex.closed_at if ex.closed_at is not None else store.clock.now()
    return {
        "execution_id": ex.id,
        "status": ex.status,
        "duration_s": round(end - ex.started_at, 3),
        "events": len(events),
        "states_entered": types["StateEntered"],
        "tasks_scheduled": types["TaskScheduled"],
        "tasks_succeeded": types["TaskSucceeded"],
        "tasks_failed": types["TaskFailed"],
        "deliveries": types["TaskStarted"],
        "retries": types["RetryScheduled"],
        "timeouts": timeouts,
        "lease_expiries": types["LeaseExpired"],
        "caught_errors": sum(1 for e in events if e.type == "StateExited" and "caught" in e.data),
        "map_items_succeeded": types["MapItemSucceeded"],
        "map_items_failed": types["MapItemFailed"],
        "activity_latency_s": {
            name: {"n": len(v), "p50": round(_percentile(v, 50), 3), "p95": round(_percentile(v, 95), 3)}
            for name, v in sorted(latency.items())
        },
    }


def format_metrics(m: dict[str, Any]) -> str:
    lines = [f"{k:<20} {v}" for k, v in m.items() if k != "activity_latency_s"]
    lines.append("activity latency (scheduled -> result):")
    for name, s in m["activity_latency_s"].items():
        lines.append(f"  {name:<18} n={s['n']:<3} p50={s['p50']:.3f}s p95={s['p95']:.3f}s")
    return "\n".join(lines)


def describe_path(path: str) -> tuple[int, str]:
    """Turn '/0.AnalyzeAll#1/i3/0.Analyze' into (depth 1, '[3] Analyze')."""
    depth, tags, name = 0, [], path
    for seg in path.strip("/").split("/"):
        m = _SEGMENT.match(seg)
        if m:
            name = m.group(2)
        elif seg[:1] in "ib" and seg[1:].isdigit():
            depth += 1
            tags.append(f"[{seg[1:]}]" if seg[0] == "i" else f"branch {seg[1:]}")
    label = f"{tags[-1]} {name}" if tags else name
    return depth, label


def timeline(store: Store, execution_id: str, width: int = 30) -> str:
    ex = store.get_execution(execution_id)
    events = store.load_events(execution_id)
    t0 = ex.started_at
    t_end = ex.closed_at if ex.closed_at is not None else store.clock.now()
    span = max(t_end - t0, 1e-9)
    entered: dict[str, Event] = {}
    exited: dict[str, Event] = {}
    retries: Counter = Counter()
    expiries: Counter = Counter()
    for e in events:
        if e.type == "StateEntered":
            entered[e.path] = e
        elif e.type == "StateExited":
            exited[e.path] = e
        elif e.type == "RetryScheduled":
            retries[e.path] += 1
        elif e.type == "LeaseExpired":
            expiries[e.path.rsplit("#", 1)[0]] += 1
    lines = [f"execution {ex.id}  {ex.status}  {span:.2f}s"]
    for path, start in sorted(entered.items(), key=lambda kv: kv[1].seq):
        end_ev = exited.get(path)
        end = end_ev.ts if end_ev else t_end
        a = int((start.ts - t0) / span * width)
        b = max(a + 1, int((end - t0) / span * width))
        bar = "." * a + "#" * (b - a) + "." * (width - b)
        depth, label = describe_path(path)
        if end_ev is None:
            outcome = "running"
        elif "caught" in end_ev.data:
            outcome = f"caught {end_ev.data['caught']} -> {end_ev.data['next']}"
        elif "error" in end_ev.data:
            outcome = f"failed {end_ev.data['error']}"
        else:
            outcome = "ok"
        notes = []
        if retries[path]:
            notes.append(f"retries={retries[path]}")
        if expiries[path]:
            notes.append(f"lease_expired={expiries[path]}")
        note = f" ({', '.join(notes)})" if notes else ""
        lines.append(f"  +{start.ts - t0:7.3f}s {end - start.ts:7.3f}s |{bar}| "
                     f"{'  ' * depth}{label} [{start.data['type']}] {outcome}{note}")
    return "\n".join(lines)


def history(store: Store, execution_id: str) -> str:
    ex = store.get_execution(execution_id)
    lines = []
    for e in store.load_events(execution_id):
        detail = {k: v for k, v in e.data.items() if k not in ("input",)}
        text = json.dumps(detail, separators=(",", ":"))
        if len(text) > 70:
            text = text[:67] + "..."
        lines.append(f"{e.seq:>5} +{e.ts - ex.started_at:7.3f}s {e.type:<18} {e.path or '-':<34} {text}")
    return "\n".join(lines)
