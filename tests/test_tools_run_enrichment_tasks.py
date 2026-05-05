"""Zero-network tests for ``tools/run_enrichment_tasks.py``.

The script wraps the enrichment runner with argparse + safe-path
gating + filter dispatch. These tests pin the operator-facing
surface so a refactor can't silently change behavior.
"""
from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

import pytest

from tools import run_enrichment_tasks as script


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "tools" / "run_enrichment_tasks.py"


# ---- helpers --------------------------------------------------------


def _make_safe_output_dir(tmp_path: Path) -> Path:
    """Build an output dir under .tmp/ at the repo root so the
    safe-path check accepts it."""
    target = REPO_ROOT / ".tmp" / "test_enrichment_runner" / tmp_path.name
    target.mkdir(parents=True, exist_ok=True)
    return target


def _write_search_tasks(path: Path, tasks: list) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "generated_at": "2026-05-05T00:00:00Z",
        "source_lane": "sfchronicle_pursuits",
        "candidate_count": 1,
        "task_count": len(tasks),
        "tasks": tasks,
    }), encoding="utf-8")
    return path


def _task(*, cid="x:1", grade="A", task_type="youtube_query", query="q"):
    return {
        "candidate_id": cid,
        "grade": grade,
        "task_type": task_type,
        "query": query,
        "context": {"agency": "Phoenix PD"},
    }


def _run(argv):
    out = io.StringIO()
    err = io.StringIO()
    code = script.main(argv, stdout=out, stderr=err)
    return code, out.getvalue(), err.getvalue()


# ---- happy-path dry-run --------------------------------------------


def test_dry_run_default_does_not_invoke_provider(tmp_path):
    inp = _write_search_tasks(tmp_path / "tasks.json", [
        _task(cid="x:1"), _task(cid="x:2"),
    ])
    code, out, _err = _run([
        "--input", str(inp),
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["dry_run"] is True
    assert payload["provider"] == "mock"  # default
    assert payload["selected_count"] == 2
    assert all(r["status"] == "dry_run" for r in payload["results"])


def test_dry_run_writes_summary_when_requested(tmp_path):
    inp = _write_search_tasks(tmp_path / "tasks.json", [_task()])
    output_dir = _make_safe_output_dir(tmp_path)
    summary_out = output_dir / "summary.json"
    code, _out, _err = _run([
        "--input", str(inp),
        "--summary-out", str(summary_out),
    ])
    assert code == 0
    written = json.loads(summary_out.read_text(encoding="utf-8"))
    assert written["dry_run"] is True
    assert written["selected_count"] == 1


# ---- mock-run mode -------------------------------------------------


def test_run_with_mock_provider_completes(tmp_path):
    inp = _write_search_tasks(tmp_path / "tasks.json", [
        _task(cid="x:1", query="q1"),
        _task(cid="x:2", query="q2"),
    ])
    output_dir = _make_safe_output_dir(tmp_path)
    code, out, _err = _run([
        "--input", str(inp),
        "--output-json", str(output_dir / "results.json"),
        "--summary-out", str(output_dir / "summary.json"),
        "--run",
        "--provider", "mock",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["dry_run"] is False
    assert payload["provider"] == "mock"
    assert payload["completed_count"] == 2
    assert payload["failed_count"] == 0
    for r in payload["results"]:
        assert r["status"] == "completed"
        assert r["result_urls"][0].startswith("https://mock.example.com/")
    assert (output_dir / "results.json").exists()
    assert (output_dir / "summary.json").exists()


# ---- filtering -----------------------------------------------------


def test_task_type_filter(tmp_path):
    inp = _write_search_tasks(tmp_path / "tasks.json", [
        _task(task_type="youtube_query"),
        _task(task_type="muckrock_query"),
        _task(task_type="outcome_query"),
    ])
    code, out, _err = _run([
        "--input", str(inp),
        "--task-type", "youtube_query",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["selected_count"] == 1
    assert payload["results"][0]["task_type"] == "youtube_query"


def test_candidate_id_filter(tmp_path):
    inp = _write_search_tasks(tmp_path / "tasks.json", [
        _task(cid="x:1"),
        _task(cid="x:2"),
        _task(cid="x:3"),
    ])
    code, out, _err = _run([
        "--input", str(inp),
        "--candidate-id", "x:2",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["selected_count"] == 1
    assert payload["results"][0]["candidate_id"] == "x:2"


def test_grade_filter(tmp_path):
    inp = _write_search_tasks(tmp_path / "tasks.json", [
        _task(cid="x:1", grade="A"),
        _task(cid="x:2", grade="B"),
        _task(cid="x:3", grade="C"),
    ])
    code, out, _err = _run([
        "--input", str(inp),
        "--grade", "A",
        "--grade", "B",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["selected_count"] == 2


def test_max_tasks_caps_selection(tmp_path):
    inp = _write_search_tasks(tmp_path / "tasks.json", [
        _task(cid=f"x:{i}", query=f"q{i}") for i in range(10)
    ])
    code, out, _err = _run([
        "--input", str(inp),
        "--max-tasks", "3",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["selected_count"] == 3


# ---- provider gates ------------------------------------------------


def test_unknown_provider_rejected(tmp_path):
    inp = _write_search_tasks(tmp_path / "tasks.json", [_task()])
    code, _out, err = _run([
        "--input", str(inp),
        "--run",
        "--provider", "not-a-real-provider",
    ])
    assert code == 2
    assert "unknown provider" in err


def test_deferred_provider_run_mode_rejected(tmp_path):
    """Live providers (youtube/muckrock/brave/exa/tavily) must
    raise NotImplementedError at the harness before any execute()
    is attempted."""
    inp = _write_search_tasks(tmp_path / "tasks.json", [_task()])
    code, _out, err = _run([
        "--input", str(inp),
        "--run",
        "--provider", "youtube",
    ])
    assert code == 2
    assert "follow-up PR" in err


def test_deferred_provider_dry_run_does_not_raise(tmp_path):
    """Dry-run should accept a deferred provider name (no execute()
    is ever called) so operators can preview what a future PR's
    provider would receive."""
    inp = _write_search_tasks(tmp_path / "tasks.json", [_task()])
    code, out, _err = _run([
        "--input", str(inp),
        "--provider", "youtube",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["dry_run"] is True
    assert payload["provider"] == "youtube"


# ---- input + safe-path errors --------------------------------------


def test_missing_input_returns_exit_2(tmp_path):
    code, _out, err = _run([
        "--input", str(tmp_path / "nope.json"),
    ])
    assert code == 2
    assert "input not found" in err


def test_malformed_search_tasks_returns_exit_2(tmp_path):
    bad = tmp_path / "tasks.json"
    bad.write_text(json.dumps({"tasks": "not-a-list"}), encoding="utf-8")
    code, _out, err = _run([
        "--input", str(bad),
    ])
    assert code == 2
    assert "could not load search tasks" in err


def test_unsafe_output_json_path_rejected(tmp_path):
    inp = _write_search_tasks(tmp_path / "tasks.json", [_task()])
    unsafe = REPO_ROOT / "pipeline2_discovery" / "_should_be_rejected.json"
    code, _out, err = _run([
        "--input", str(inp),
        "--output-json", str(unsafe),
    ])
    assert code == 2
    assert "safe artifact dirs" in err


def test_unsafe_summary_out_path_rejected(tmp_path):
    inp = _write_search_tasks(tmp_path / "tasks.json", [_task()])
    unsafe = REPO_ROOT / "pipeline2_discovery" / "_should_be_rejected_summary.json"
    code, _out, err = _run([
        "--input", str(inp),
        "--summary-out", str(unsafe),
    ])
    assert code == 2
    assert "safe artifact dirs" in err


def test_negative_max_tasks_rejected(tmp_path):
    inp = _write_search_tasks(tmp_path / "tasks.json", [_task()])
    code, _out, err = _run([
        "--input", str(inp),
        "--max-tasks", "0",
    ])
    assert code == 2
    assert "max-tasks" in err


# ---- subprocess invocation regression guards -----------------------


def _run_subprocess(argv, cwd=REPO_ROOT, env=None):
    return subprocess.run(
        argv, cwd=str(cwd), capture_output=True, text=True, env=env, timeout=30,
    )


def test_direct_script_invocation_help_exits_zero():
    result = _run_subprocess([sys.executable, str(SCRIPT_PATH), "--help"])
    assert result.returncode == 0, (
        f"stderr={result.stderr!r}\nstdout={result.stdout!r}"
    )
    assert "--input" in result.stdout
    assert "--task-type" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


def test_module_invocation_help_exits_zero():
    result = _run_subprocess(
        [sys.executable, "-m", "tools.run_enrichment_tasks", "--help"],
    )
    assert result.returncode == 0
    assert "--input" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


# ---- zero-network --------------------------------------------------


def test_runner_makes_zero_network_calls(monkeypatch, tmp_path):
    import requests

    def fail_get(self, *args, **kwargs):
        raise AssertionError("enrichment runner must never touch the network")

    monkeypatch.setattr(requests.Session, "get", fail_get)

    inp = _write_search_tasks(tmp_path / "tasks.json", [
        _task(cid="x:1"), _task(cid="x:2"),
    ])
    code, _out, _err = _run([
        "--input", str(inp),
        "--run",
        "--provider", "mock",
    ])
    assert code == 0
