"""SQLite persistence: executions, the append-only event history, the task queue, and a side-effect ledger."""

import json
import sqlite3
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from .clock import SystemClock
from .crash import CrashInjector

SCHEMA = """
CREATE TABLE IF NOT EXISTS executions (
    id TEXT PRIMARY KEY,
    name TEXT,
    definition TEXT NOT NULL,
    input TEXT NOT NULL,
    status TEXT NOT NULL,
    output TEXT,
    error TEXT,
    cause TEXT,
    started_at REAL NOT NULL,
    closed_at REAL
);
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    key TEXT NOT NULL,
    type TEXT NOT NULL,
    path TEXT NOT NULL,
    ts REAL NOT NULL,
    data TEXT NOT NULL,
    UNIQUE (execution_id, key)
);
CREATE TABLE IF NOT EXISTS tasks (
    token TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    path TEXT NOT NULL,
    activity TEXT NOT NULL,
    input TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    idempotency_key TEXT NOT NULL,
    status TEXT NOT NULL,
    available_at REAL NOT NULL,
    timeout REAL,
    heartbeat REAL,
    deadline REAL,
    lease_owner TEXT,
    lease_expires_at REAL,
    deliveries INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS tasks_ready ON tasks (status, available_at);
CREATE TABLE IF NOT EXISTS side_effects (
    key TEXT NOT NULL,
    activity TEXT NOT NULL,
    result TEXT NOT NULL,
    ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS side_effects_key ON side_effects (key);
"""

OPEN = ("queued", "leased")


@dataclass(frozen=True)
class Event:
    seq: int
    key: str
    type: str
    path: str
    ts: float
    data: dict[str, Any]


@dataclass(frozen=True)
class TaskSpec:
    token: str
    path: str
    activity: str
    input: Any
    attempt: int
    idempotency_key: str
    available_at: float
    timeout: float | None = None
    heartbeat: float | None = None


@dataclass(frozen=True)
class Task:
    token: str
    execution_id: str
    path: str
    activity: str
    input: Any
    attempt: int
    idempotency_key: str
    status: str
    deliveries: int
    heartbeat: float | None
    lease_expires_at: float | None


@dataclass(frozen=True)
class Execution:
    id: str
    name: str | None
    definition: dict[str, Any]
    input: Any
    status: str
    output: Any
    error: str | None
    cause: str | None
    started_at: float
    closed_at: float | None


class Store:
    def __init__(self, path: str, clock: Any = None, crash: CrashInjector | None = None) -> None:
        self.path = str(path)
        self.clock = clock or SystemClock()
        self.crash = crash or CrashInjector()
        self._local = threading.local()
        self._conns: list[sqlite3.Connection] = []
        self._conns_lock = threading.Lock()
        self.conn.executescript(SCHEMA)

    @property
    def conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
            with self._conns_lock:
                self._conns.append(conn)
        return conn

    def close(self) -> None:
        with self._conns_lock:
            for conn in self._conns:
                conn.close()
            self._conns.clear()
        self._local = threading.local()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            self.crash.point("store:before_commit")
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    # executions

    def create_execution(self, definition: dict, input: Any, name: str | None = None) -> str:
        execution_id = uuid.uuid4().hex[:12]
        now = self.clock.now()
        with self.tx() as c:
            c.execute(
                "INSERT INTO executions (id, name, definition, input, status, started_at) VALUES (?,?,?,?,?,?)",
                (execution_id, name, json.dumps(definition), json.dumps(input), "RUNNING", now),
            )
            self._insert_event(c, execution_id, "start", "ExecutionStarted", "", {"input": input, "name": name}, now)
        return execution_id

    def get_execution(self, execution_id: str) -> Execution:
        row = self.conn.execute("SELECT * FROM executions WHERE id=?", (execution_id,)).fetchone()
        if row is None:
            raise KeyError(f"no execution {execution_id!r}")
        return _execution(row)

    def list_executions(self) -> list[Execution]:
        rows = self.conn.execute("SELECT * FROM executions ORDER BY started_at, id").fetchall()
        return [_execution(r) for r in rows]

    def close_execution(self, execution_id: str, status: str, output: Any = None,
                        error: str | None = None, cause: str | None = None) -> None:
        self.crash.point("store:close_execution")
        now = self.clock.now()
        kind = "ExecutionSucceeded" if status == "SUCCEEDED" else "ExecutionFailed"
        data = {"output": output} if status == "SUCCEEDED" else {"error": error, "cause": cause}
        with self.tx() as c:
            self._insert_event(c, execution_id, "end", kind, "", data, now)
            c.execute(
                "UPDATE executions SET status=?, output=?, error=?, cause=?, closed_at=? WHERE id=? AND status='RUNNING'",
                (status, json.dumps(output), error, cause, now, execution_id),
            )
            c.execute("UPDATE tasks SET status='cancelled' WHERE execution_id=? AND status IN ('queued','leased')",
                      (execution_id,))

    # events

    def load_events(self, execution_id: str) -> list[Event]:
        rows = self.conn.execute("SELECT * FROM events WHERE execution_id=? ORDER BY seq", (execution_id,)).fetchall()
        return [_event(r) for r in rows]

    def append_event(self, execution_id: str, key: str, type_: str, path: str, data: dict,
                     task: TaskSpec | None = None, cancel_prefix: str | None = None) -> Event:
        """Append one decision event. A task enqueue and a scope cancel commit in the same transaction."""
        now = self.clock.now()
        with self.tx() as c:
            inserted = self._insert_event(c, execution_id, key, type_, path, data, now)
            if inserted and task is not None:
                c.execute(
                    "INSERT OR IGNORE INTO tasks (token, execution_id, path, activity, input, attempt, idempotency_key,"
                    " status, available_at, timeout, heartbeat) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (task.token, execution_id, task.path, task.activity, json.dumps(task.input), task.attempt,
                     task.idempotency_key, "queued", task.available_at, task.timeout, task.heartbeat),
                )
            if inserted and cancel_prefix is not None:
                c.execute(
                    "UPDATE tasks SET status='cancelled' WHERE execution_id=? AND status IN ('queued','leased')"
                    " AND substr(path, 1, ?) = ?",
                    (execution_id, len(cancel_prefix), cancel_prefix),
                )
        row = self.conn.execute("SELECT * FROM events WHERE execution_id=? AND key=?", (execution_id, key)).fetchone()
        return _event(row)

    def _insert_event(self, c: sqlite3.Connection, execution_id: str, key: str, type_: str, path: str,
                      data: dict, ts: float) -> bool:
        cur = c.execute(
            "INSERT OR IGNORE INTO events (execution_id, key, type, path, ts, data) VALUES (?,?,?,?,?,?)",
            (execution_id, key, type_, path, ts, json.dumps(data)),
        )
        return cur.rowcount == 1

    # task queue

    def lease_task(self, owner: str, lease_seconds: float) -> Task | None:
        now = self.clock.now()
        with self.tx() as c:
            row = c.execute(
                "SELECT token FROM tasks WHERE status='queued' AND available_at<=? ORDER BY available_at, rowid LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                return None
            token = row["token"]
            c.execute(
                "UPDATE tasks SET status='leased', lease_owner=?, lease_expires_at=?+COALESCE(heartbeat, ?),"
                " deliveries=deliveries+1, deadline=COALESCE(deadline, ?+timeout) WHERE token=?",
                (owner, now, lease_seconds, now, token),
            )
            task = _task(c.execute("SELECT * FROM tasks WHERE token=?", (token,)).fetchone())
            self._insert_event(c, task.execution_id, f"started:{token}:{task.deliveries}", "TaskStarted", task.path,
                               {"token": token, "owner": owner, "delivery": task.deliveries}, now)
        return task

    def heartbeat(self, token: str, owner: str, lease_seconds: float) -> bool:
        now = self.clock.now()
        with self.tx() as c:
            cur = c.execute(
                "UPDATE tasks SET lease_expires_at=?+COALESCE(heartbeat, ?) WHERE token=? AND status='leased'"
                " AND lease_owner=?",
                (now, lease_seconds, token, owner),
            )
        return cur.rowcount == 1

    def complete_task(self, token: str, output: Any = None, error: str | None = None, cause: str = "") -> bool:
        """Record a task result. The first result for a token wins; later ones return False."""
        now = self.clock.now()
        with self.tx() as c:
            row = c.execute("SELECT * FROM tasks WHERE token=?", (token,)).fetchone()
            if row is None or row["status"] not in OPEN:
                return False
            c.execute("UPDATE tasks SET status='done' WHERE token=?", (token,))
            if error is None:
                self._insert_event(c, row["execution_id"], f"result:{token}", "TaskSucceeded", row["path"],
                                   {"output": output}, now)
            else:
                self._insert_event(c, row["execution_id"], f"result:{token}", "TaskFailed", row["path"],
                                   {"error": error, "cause": cause}, now)
        return True

    def reap(self) -> int:
        """Fail tasks past their deadline, and put tasks with an expired lease back on the queue."""
        now = self.clock.now()
        changed = 0
        with self.tx() as c:
            for row in c.execute("SELECT * FROM tasks WHERE status IN ('queued','leased') AND deadline<=?",
                                 (now,)).fetchall():
                c.execute("UPDATE tasks SET status='done' WHERE token=?", (row["token"],))
                self._insert_event(c, row["execution_id"], f"result:{row['token']}", "TaskFailed", row["path"],
                                   {"error": "States.Timeout", "cause": f"no result within {row['timeout']}s"}, now)
                changed += 1
            for row in c.execute("SELECT * FROM tasks WHERE status='leased' AND lease_expires_at<=?",
                                 (now,)).fetchall():
                c.execute("UPDATE tasks SET status='queued', available_at=?, lease_owner=NULL WHERE token=?",
                          (now, row["token"]))
                self._insert_event(c, row["execution_id"], f"lease:{row['token']}:{row['deliveries']}",
                                   "LeaseExpired", row["path"],
                                   {"token": row["token"], "owner": row["lease_owner"]}, now)
                changed += 1
        return changed

    def next_due(self) -> float | None:
        row = self.conn.execute(
            "SELECT MIN(t) FROM ("
            " SELECT available_at AS t FROM tasks WHERE status='queued'"
            " UNION ALL SELECT lease_expires_at FROM tasks WHERE status='leased'"
            " UNION ALL SELECT deadline FROM tasks WHERE status IN ('queued','leased') AND deadline IS NOT NULL)"
        ).fetchone()
        return row[0]

    def tasks(self, execution_id: str) -> list[Task]:
        rows = self.conn.execute("SELECT * FROM tasks WHERE execution_id=? ORDER BY rowid", (execution_id,)).fetchall()
        return [_task(r) for r in rows]

    # side-effect ledger: stands in for an external system that honours idempotency keys

    def apply_effect(self, key: str, activity: str, fn: Callable[[], Any]) -> Any:
        with self.tx() as c:
            row = c.execute("SELECT result FROM side_effects WHERE key=?", (key,)).fetchone()
            if row is not None:
                return json.loads(row["result"])
            result = fn()
            c.execute("INSERT INTO side_effects (key, activity, result, ts) VALUES (?,?,?,?)",
                      (key, activity, json.dumps(result), self.clock.now()))
        return result

    def effects(self) -> list[tuple[str, str, Any]]:
        rows = self.conn.execute("SELECT key, activity, result FROM side_effects ORDER BY rowid").fetchall()
        return [(r["key"], r["activity"], json.loads(r["result"])) for r in rows]


def _event(row: sqlite3.Row) -> Event:
    return Event(row["seq"], row["key"], row["type"], row["path"], row["ts"], json.loads(row["data"]))


def _task(row: sqlite3.Row) -> Task:
    return Task(row["token"], row["execution_id"], row["path"], row["activity"], json.loads(row["input"]),
                row["attempt"], row["idempotency_key"], row["status"], row["deliveries"], row["heartbeat"],
                row["lease_expires_at"])


def _execution(row: sqlite3.Row) -> Execution:
    return Execution(row["id"], row["name"], json.loads(row["definition"]), json.loads(row["input"]), row["status"],
                     json.loads(row["output"]) if row["output"] is not None else None, row["error"], row["cause"],
                     row["started_at"], row["closed_at"])
