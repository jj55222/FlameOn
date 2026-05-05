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


@pytest.mark.parametrize("name", ["brave", "exa", "tavily"])
def test_deferred_provider_run_mode_rejected(tmp_path, name):
    """Still-deferred providers (muckrock/brave/exa/tavily) must
    raise NotImplementedError at the harness before any execute()
    is attempted."""
    inp = _write_search_tasks(tmp_path / "tasks.json", [_task()])
    code, _out, err = _run([
        "--input", str(inp),
        "--run",
        "--provider", name,
    ])
    assert code == 2
    assert "follow-up PR" in err


@pytest.mark.parametrize("name", ["brave", "exa", "tavily"])
def test_deferred_provider_dry_run_does_not_raise(tmp_path, name):
    """Dry-run should accept a deferred provider name (no execute()
    is ever called) so operators can preview what a future PR's
    provider would receive."""
    inp = _write_search_tasks(tmp_path / "tasks.json", [_task()])
    code, out, _err = _run([
        "--input", str(inp),
        "--provider", name,
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["dry_run"] is True
    assert payload["provider"] == name


# ---- youtube provider (zero-network) --------------------------------


class _FakeYdlForCLI:
    """Drop-in for yt_dlp.YoutubeDL in CLI tests — captures opts and
    returns canned entries. No network."""

    captured: dict = {}

    def __init__(self, opts):
        type(self).captured["opts"] = opts

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def extract_info(self, url, *, download):
        type(self).captured["url"] = url
        type(self).captured["download"] = download
        return {
            "entries": [
                {
                    "id": "vid01",
                    "title": "Phoenix bodycam fake",
                    "url": "https://www.youtube.com/watch?v=vid01",
                },
                {
                    "id": "vid02",
                    "title": "Phoenix PD second fake update",
                    "url": "",
                },
            ]
        }


def test_youtube_dry_run_does_not_invoke_yt_dlp(monkeypatch, tmp_path):
    """Dry-run must not trigger yt-dlp import or execution even with
    --provider youtube."""
    from pipeline2_discovery.enrichment import providers as prov_mod

    def boom(self):
        raise AssertionError("yt-dlp must not be loaded in dry-run")

    monkeypatch.setattr(prov_mod.YtDlpYouTubeSearchClient, "_load_yt_dlp", boom)

    inp = _write_search_tasks(tmp_path / "tasks.json", [
        _task(cid="x:1", task_type="youtube_query", query="q1"),
        _task(cid="x:2", task_type="youtube_query", query="q2"),
    ])
    code, out, _err = _run([
        "--input", str(inp),
        "--task-type", "youtube_query",
        "--provider", "youtube",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["dry_run"] is True
    assert payload["provider"] == "youtube"
    assert all(r["status"] == "dry_run" for r in payload["results"])


def test_youtube_run_mode_uses_monkeypatched_yt_dlp(monkeypatch, tmp_path):
    """Run mode with --provider youtube dispatches to the yt-dlp
    backed client, but with a monkeypatched fake YoutubeDL class
    so no network is hit."""
    from pipeline2_discovery.enrichment import providers as prov_mod

    _FakeYdlForCLI.captured = {}
    monkeypatch.setattr(
        prov_mod.YtDlpYouTubeSearchClient,
        "_load_yt_dlp",
        lambda self: _FakeYdlForCLI,
    )

    inp = _write_search_tasks(tmp_path / "tasks.json", [
        _task(cid="x:1", task_type="youtube_query", query="phoenix bodycam"),
    ])
    output_dir = _make_safe_output_dir(tmp_path)
    code, out, _err = _run([
        "--input", str(inp),
        "--task-type", "youtube_query",
        "--output-json", str(output_dir / "results.json"),
        "--summary-out", str(output_dir / "summary.json"),
        "--run",
        "--provider", "youtube",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["dry_run"] is False
    assert payload["provider"] == "youtube"
    assert payload["completed_count"] == 1
    assert payload["failed_count"] == 0

    r = payload["results"][0]
    assert r["status"] == "completed"
    assert r["provider"] == "youtube"
    assert "https://www.youtube.com/watch?v=vid01" in r["result_urls"]
    assert "https://www.youtube.com/watch?v=vid02" in r["result_urls"]
    assert r["confidence"] == "medium"
    assert "youtube_metadata" in r["next_actions_hint"]

    # Verify yt-dlp was actually invoked by the fake (search url shape)
    assert _FakeYdlForCLI.captured["url"].startswith("ytsearch")
    assert "phoenix bodycam" in _FakeYdlForCLI.captured["url"]
    assert _FakeYdlForCLI.captured["download"] is False

    # Output files written
    assert (output_dir / "results.json").exists()
    assert (output_dir / "summary.json").exists()


class _UnrelatedYdlForCLI:
    """Fake yt-dlp returning topical-but-unanchored entries — the
    relevance gate should drop everything and the CLI run should
    surface confidence=low + empty result_urls + no youtube_metadata
    hint."""

    def __init__(self, opts):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def extract_info(self, url, *, download):
        return {
            "entries": [
                {"id": "g1", "title": "Three officers resign from Centralia police", "url": ""},
                {"id": "g2", "title": "Capitol Police officer lies in honor", "url": ""},
                {"id": "g3", "title": "Random Florida bodycam compilation", "url": ""},
            ]
        }


def test_youtube_run_mode_drops_unanchored_results(monkeypatch, tmp_path):
    """End-to-end CLI: yt-dlp returns generic police news with no
    Longmont/Joe-Gold anchor; relevance gate drops all → JSON shows
    confidence=low, empty result_urls, no youtube_metadata hint."""
    from pipeline2_discovery.enrichment import youtube_provider as yt_mod

    monkeypatch.setattr(
        yt_mod.YtDlpYouTubeSearchClient,
        "_load_yt_dlp",
        lambda self: _UnrelatedYdlForCLI,
    )

    inp = tmp_path / "tasks.json"
    inp.write_text(json.dumps({
        "generated_at": "2026-05-05T00:00:00Z",
        "source_lane": "sfchronicle_pursuits",
        "candidate_count": 1,
        "task_count": 1,
        "tasks": [{
            "candidate_id": "sfchronicle_pursuits:0",
            "grade": "A",
            "task_type": "youtube_query",
            "query": "joe william gold longmont police bodycam",
            "context": {
                "agency": "longmont police services",
                "subject_name": "joe william gold",
                "city": "longmont",
                "state": "CO",
            },
        }],
    }), encoding="utf-8")

    code, out, _err = _run([
        "--input", str(inp),
        "--task-type", "youtube_query",
        "--run",
        "--provider", "youtube",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["completed_count"] == 1
    r = payload["results"][0]
    assert r["status"] == "completed"
    assert r["result_urls"] == []
    assert r["confidence"] == "low"
    assert r["next_actions_hint"] == []
    notes_str = " ".join(r["notes"])
    assert "raw_result_count=3" in notes_str
    assert "filtered_result_count=0" in notes_str
    assert "dropped_irrelevant_count=3" in notes_str


# ---- muckrock provider (zero-network) -------------------------------


class _FakeMuckRockClient:
    """Fake HTTP client for the MuckRockProvider end-to-end CLI test.

    Returns a single anchored 'done with files' record matching the
    Joe-William-Gold / Longmont task context, so the gate keeps it."""

    def __init__(self, results=None):
        self._results = results if results is not None else [{
            "id": 42,
            "title": "Longmont Police body-worn camera release — Joe Gold incident",
            "agency": {"name": "Longmont Police Services"},
            "jurisdiction": {"name": "Longmont", "level": "city"},
            "status": "done",
            "datetime_done": "2025-01-01",
            "files": [{"ffile": "https://www.muckrock.com/files/x.mp4"}],
            "absolute_url": "/foi/longmont-co-9999/42-test/",
        }]
        self.calls = []

    def get(self, url, *, params, headers, timeout):
        self.calls.append({
            "url": url, "params": dict(params),
            "headers": dict(headers), "timeout": timeout,
        })

        class _R:
            status_code = 200

            def __init__(self, body):
                self._body = body

            def json(self):
                return self._body

        return _R({"results": self._results})


def test_muckrock_dry_run_does_not_invoke_http(monkeypatch, tmp_path):
    """Dry-run with --provider muckrock must not instantiate the
    real HTTP client — the runner sees the dry-run name-only stub
    and never calls execute()."""
    from pipeline2_discovery.enrichment import muckrock_provider as mr_mod

    def boom(self):
        raise AssertionError("HTTP client must not be loaded in dry-run")

    monkeypatch.setattr(mr_mod.MuckRockProvider, "_client", boom)

    inp = tmp_path / "tasks.json"
    inp.write_text(json.dumps({
        "generated_at": "2026-05-05T00:00:00Z",
        "source_lane": "sfchronicle_pursuits",
        "candidate_count": 1,
        "task_count": 1,
        "tasks": [{
            "candidate_id": "x:1", "grade": "A",
            "task_type": "muckrock_query",
            "query": "longmont body-worn camera",
            "context": {
                "agency": "Longmont Police Services",
                "subject_name": "joe william gold",
                "city": "longmont",
                "state": "CO",
            },
        }],
    }), encoding="utf-8")

    code, out, _err = _run([
        "--input", str(inp),
        "--task-type", "muckrock_query",
        "--provider", "muckrock",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["dry_run"] is True
    assert payload["provider"] == "muckrock"
    assert all(r["status"] == "dry_run" for r in payload["results"])


def test_muckrock_run_mode_uses_monkeypatched_client(monkeypatch, tmp_path):
    """End-to-end CLI: --run --provider muckrock dispatches via the
    real factory, but with a monkeypatched HTTP client so no network
    is touched."""
    from pipeline2_discovery.enrichment import muckrock_provider as mr_mod

    fake = _FakeMuckRockClient()
    # Patch the factory hook so get_provider("muckrock") returns a
    # provider already wired with our fake client.
    def _patched_factory():
        return mr_mod.MuckRockProvider(
            http_client=fake, sleeper=lambda _: None,
            rate_limit_seconds=0, read_token=False,
        )

    from pipeline2_discovery.enrichment import providers as prov_mod
    monkeypatch.setattr(prov_mod, "MuckRockProvider", _patched_factory)

    inp = tmp_path / "tasks.json"
    inp.write_text(json.dumps({
        "generated_at": "2026-05-05T00:00:00Z",
        "source_lane": "sfchronicle_pursuits",
        "candidate_count": 1,
        "task_count": 1,
        "tasks": [{
            "candidate_id": "x:1", "grade": "A",
            "task_type": "muckrock_query",
            "query": "longmont body-worn camera",
            "context": {
                "agency": "Longmont Police Services",
                "subject_name": "joe william gold",
                "city": "longmont",
                "state": "CO",
            },
        }],
    }), encoding="utf-8")

    output_dir = _make_safe_output_dir(tmp_path)
    code, out, _err = _run([
        "--input", str(inp),
        "--task-type", "muckrock_query",
        "--output-json", str(output_dir / "results.json"),
        "--summary-out", str(output_dir / "summary.json"),
        "--run",
        "--provider", "muckrock",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["dry_run"] is False
    assert payload["provider"] == "muckrock"
    assert payload["completed_count"] == 1
    assert payload["failed_count"] == 0

    r = payload["results"][0]
    assert r["status"] == "completed"
    assert r["provider"] == "muckrock"
    assert r["result_urls"] == ["https://www.muckrock.com/foi/longmont-co-9999/42-test/"]
    assert r["confidence"] == "high"
    assert "MUCKROCK_PARSE_RELEASED_FILES" in r["next_actions_hint"]
    assert "ARTIFACT_SEARCH" in r["next_actions_hint"]

    # Fake client received exactly one GET (no other HTTP verbs).
    assert len(fake.calls) == 1
    assert fake.calls[0]["params"]["title"] == "longmont body-worn camera"

    # Output files written
    assert (output_dir / "results.json").exists()
    assert (output_dir / "summary.json").exists()


def test_muckrock_help_still_works():
    """`--help` exits 0 and lists --provider — sanity-check that
    promoting muckrock to KNOWN_PROVIDERS didn't break argparse
    setup."""
    result = _run_subprocess(
        [sys.executable, str(SCRIPT_PATH), "--provider", "muckrock", "--help"],
    )
    assert result.returncode == 0
    assert "--provider" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


def test_youtube_help_still_works():
    """`--help` exits 0 and lists --provider — sanity-check that
    promoting youtube to KNOWN_PROVIDERS didn't break argparse
    setup."""
    result = _run_subprocess(
        [sys.executable, str(SCRIPT_PATH), "--provider", "youtube", "--help"],
    )
    assert result.returncode == 0
    assert "--provider" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


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
