import json
import os
import subprocess
import sys
from pathlib import Path

SRC = str(Path(__file__).resolve().parents[1] / "src")


def dwe(db: Path, *args: str) -> subprocess.CompletedProcess:
    env = {**os.environ, "PYTHONPATH": SRC}
    return subprocess.run([sys.executable, "-m", "dwe", "--db", str(db), *args],
                          capture_output=True, text=True, env=env, timeout=60)


def test_run_list_history_timeline_metrics(tmp_path):
    db = tmp_path / "cli.db"
    run = dwe(db, "run", "saga", "--input", json.dumps({"trip_id": "T-5"}))
    assert run.returncode == 0, run.stderr
    assert "SUCCEEDED" in run.stdout
    ex = run.stderr.split()[1]
    assert ex in dwe(db, "list").stdout
    assert "ExecutionSucceeded" in dwe(db, "history", ex).stdout
    assert "BookCar [Task] ok" in dwe(db, "timeline", ex).stdout
    metrics = json.loads(dwe(db, "metrics", ex, "--json").stdout)
    assert metrics["tasks_succeeded"] == 3


def test_crash_then_resume_in_a_new_process(tmp_path):
    db = tmp_path / "crash.db"
    inp = json.dumps({"trip_id": "T-6", "fail_at": "car"})
    crashed = dwe(db, "run", "saga", "--input", inp, "--crash-on", "worker:before_report", "--lease", "0.3")
    assert crashed.returncode == 3, crashed.stderr
    ex = crashed.stderr.split()[1]
    assert "RUNNING" in dwe(db, "list").stdout
    resumed = dwe(db, "resume", ex, "--lease", "0.3")
    assert resumed.returncode == 1
    assert "TripBookingFailed" in resumed.stdout
    history = dwe(db, "history", ex).stdout
    assert "LeaseExpired" in history


def test_invalid_definition_exits_2(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"StartAt": "X", "States": {"Y": {"Type": "Succeed"}}}))
    result = dwe(tmp_path / "x.db", "run", str(bad))
    assert result.returncode == 2
    assert "StartAt" in result.stderr
