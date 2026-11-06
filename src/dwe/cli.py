"""Command line interface: dwe run | resume | list | history | timeline | metrics."""

import argparse
import importlib
import json
import os
import sys
from pathlib import Path

from . import observe
from .crash import CrashInjector, SimulatedCrash
from .definition import DefinitionError
from .engine import Engine
from .examples import DIR as EXAMPLES
from .runner import drive
from .store import Store

CRASH_EXIT = 3


def _load_definition(ref: str) -> dict:
    path = Path(ref)
    if not path.exists() and (EXAMPLES / f"{ref}.json").exists():
        path = EXAMPLES / f"{ref}.json"
    return json.loads(path.read_text())


def _registry(module: str):
    return importlib.import_module(module).REGISTRY


def _drive(args: argparse.Namespace, store: Store, execution_id: str) -> int:
    try:
        decision = drive(store, _registry(args.activities), execution_id, workers=args.workers,
                         lease_seconds=args.lease)
    except SimulatedCrash as exc:
        print(f"execution {execution_id}: process crashed ({exc})", file=sys.stderr)
        print(f"resume with: dwe --db {args.db} resume {execution_id}", file=sys.stderr)
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(CRASH_EXIT)
    print(f"execution {execution_id} {decision.status}")
    if decision.status == "SUCCEEDED":
        print(json.dumps(decision.output, indent=2))
    else:
        print(f"error: {decision.error}: {decision.cause}")
    if args.show:
        print()
        print(observe.timeline(store, execution_id))
        print()
        print(observe.format_metrics(observe.metrics(store, execution_id)))
    return 0 if decision.status == "SUCCEEDED" else 1


def cmd_run(args: argparse.Namespace) -> int:
    store = Store(args.db, crash=CrashInjector(at=args.crash_at, on=args.crash_on))
    try:
        execution_id = Engine(store).start(_load_definition(args.definition), json.loads(args.input), args.name)
    except DefinitionError as exc:
        print(exc, file=sys.stderr)
        return 2
    print(f"started {execution_id}", file=sys.stderr)
    return _drive(args, store, execution_id)


def cmd_resume(args: argparse.Namespace) -> int:
    store = Store(args.db, crash=CrashInjector(at=args.crash_at, on=args.crash_on))
    return _drive(args, store, args.execution_id)


def cmd_list(args: argparse.Namespace) -> int:
    store = Store(args.db)
    print(f"{'ID':<14}{'NAME':<16}{'STATUS':<11}{'EVENTS':>7}  ERROR")
    for ex in store.list_executions():
        n = len(store.load_events(ex.id))
        print(f"{ex.id:<14}{(ex.name or '-'):<16}{ex.status:<11}{n:>7}  {ex.error or ''}")
    return 0


def cmd_history(args: argparse.Namespace) -> int:
    print(observe.history(Store(args.db), args.execution_id))
    return 0


def cmd_timeline(args: argparse.Namespace) -> int:
    print(observe.timeline(Store(args.db), args.execution_id))
    return 0


def cmd_metrics(args: argparse.Namespace) -> int:
    m = observe.metrics(Store(args.db), args.execution_id)
    print(json.dumps(m, indent=2) if args.json else observe.format_metrics(m))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="dwe", description="Durable workflow engine")
    p.add_argument("--db", default="dwe.db", help="SQLite file (default: dwe.db)")
    sub = p.add_subparsers(dest="command", required=True)

    def runtime(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--activities", default="dwe.examples.activities", help="module with a REGISTRY")
        sp.add_argument("--workers", type=int, default=1, help="worker threads (default: 1)")
        sp.add_argument("--lease", type=float, default=2.0, help="task lease in seconds (default: 2)")
        sp.add_argument("--crash-at", type=int, help="kill the process at the N-th crash point")
        sp.add_argument("--crash-on", help="kill the process at the first crash point with this name")
        sp.add_argument("--show", action="store_true", help="print the timeline and metrics at the end")

    run = sub.add_parser("run", help="start an execution and drive it to the end")
    run.add_argument("definition", help="path to a JSON definition, or an example name (saga, map_analysis, order)")
    run.add_argument("--input", default="{}", help="execution input as JSON")
    run.add_argument("--name")
    runtime(run)
    run.set_defaults(fn=cmd_run)

    resume = sub.add_parser("resume", help="continue an execution from its history")
    resume.add_argument("execution_id")
    runtime(resume)
    resume.set_defaults(fn=cmd_resume)

    sub.add_parser("list", help="list executions").set_defaults(fn=cmd_list)
    for name, fn in (("history", cmd_history), ("timeline", cmd_timeline), ("metrics", cmd_metrics)):
        sp = sub.add_parser(name, help=f"show the {name} of an execution")
        sp.add_argument("execution_id")
        if name == "metrics":
            sp.add_argument("--json", action="store_true")
        sp.set_defaults(fn=fn)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.fn(args)
