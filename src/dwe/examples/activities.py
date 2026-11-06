"""Demo task handlers. Each handler is deterministic in its input and attempt number,
so a crashed and resumed run gives the same result as a clean run."""

import hashlib
from typing import Any

from ..errors import TaskError
from ..worker import Registry, TaskContext

REGISTRY = Registry()


def _booker(kind: str):
    def book(inp: dict, ctx: TaskContext) -> dict:
        if inp.get("flaky") == kind and ctx.attempt == 1:
            raise TaskError("Transient", f"{kind} service timed out")
        if inp.get("fail_at") == kind:
            raise TaskError(f"{kind.capitalize()}Unavailable", f"no {kind} available for trip {inp['trip_id']}")
        return ctx.effect(lambda: {"kind": kind, "confirmation": f"{kind[:3].upper()}-{inp['trip_id']}"})
    return book


def _canceller(kind: str):
    def cancel(inp: dict, ctx: TaskContext) -> dict:
        return ctx.effect(lambda: {"cancelled": kind, "trip_id": inp["trip_id"]})
    return cancel


for _kind in ("flight", "hotel", "car"):
    REGISTRY.add(f"book_{_kind}", _booker(_kind))
    REGISTRY.add(f"cancel_{_kind}", _canceller(_kind))


@REGISTRY.register("analyze_item")
def analyze_item(item: dict, ctx: TaskContext) -> dict:
    if item.get("flaky") and ctx.attempt == 1:
        raise TaskError("Transient", f"item {item['id']}: worker busy")
    if item.get("corrupt"):
        raise TaskError("AnalysisFailed", f"item {item['id']}: input is corrupt")
    digest = hashlib.sha256(str(item["id"]).encode()).digest()
    return {"id": item["id"], "score": round(digest[0] / 255, 3)}


@REGISTRY.register("summarize")
def summarize(inp: dict, ctx: TaskContext) -> dict:
    ok = [r for r in inp["results"] if "Error" not in r]
    failed = [item["id"] for item, r in zip(inp["items"], inp["results"]) if "Error" in r]
    mean = round(sum(r["score"] for r in ok) / len(ok), 3) if ok else None
    return {"analyzed": len(ok), "failed_ids": failed, "mean_score": mean}


@REGISTRY.register("validate_order")
def validate_order(order: dict, ctx: TaskContext) -> dict:
    if not order.get("lines"):
        raise TaskError("InvalidOrder", "order has no lines")
    return {"total": round(sum(l["qty"] * l["price"] for l in order["lines"]), 2)}


@REGISTRY.register("reserve_inventory")
def reserve_inventory(order: dict, ctx: TaskContext) -> dict:
    return ctx.effect(lambda: {"reserved": sum(l["qty"] for l in order["lines"])})


@REGISTRY.register("charge_card")
def charge_card(order: dict, ctx: TaskContext) -> Any:
    if ctx.attempt <= order.get("card_failures", 0):
        raise TaskError("Transient", "payment gateway returned 503")
    return ctx.effect(lambda: {"charged": order["total"], "currency": "USD"})


@REGISTRY.register("send_receipt")
def send_receipt(order: dict, ctx: TaskContext) -> dict:
    return ctx.effect(lambda: {"sent_to": order["email"]})
