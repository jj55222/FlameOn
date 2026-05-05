"""Zero-network tests for ``tools/run_portal_live_batch.py``.

The batch runner shells out to the existing CaseGraph CLI, but every
test here monkeypatches ``script._run_cli_subprocess`` and
``script._sleep`` so no real subprocess and no real network call ever
happens. Combined with the existing portal-live-fetch tests (which
already pin the CLI's behavior), this gives the batch runner full
coverage without doubling the test wallclock.
"""
from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path
from typing import List

import pytest

from tools import run_portal_live_batch as script


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "tools" / "run_portal_live_batch.py"


# ---- canned CLI stdout shapes ---------------------------------------


def _completed_cli_stdout(target_id: str) -> str:
    """Mimic the CaseGraph CLI's --json output for a successful
    extract-required smoke. Shape pulled from the real Sept 2 Tonto
    smoke captured during PR #29's operational validation."""
    return json.dumps({
        "input_summary": {
            "input_type": "portal_live",
            "target_id": target_id,
            "url": f"https://www.phoenix.gov/newsroom/police-department-news/{target_id}.html",
            "fetcher": "requests",
        },
        "live_fetch": {
            "target_id": target_id,
            "url": f"https://www.phoenix.gov/newsroom/police-department-news/{target_id}.html",
            "fetcher": "requests",
            "require_extraction": True,
            "raw_payload_path": f"autoresearch/.runs/live_payloads/{target_id}.raw.json",
            "extracted_payload_path": f"autoresearch/.runs/live_payloads/{target_id}.extracted.json",
            "status": "completed",
            "blocked_reason": None,
            "status_code": 200,
            "api_calls": {"requests": 1},
            "wallclock_seconds": 1.2,
            "safety_status": "allowed",
            "target_domain_status": "allowed",
            "replayed": True,
        },
        "packet_summary": {
            "verified_artifact_types": ["bodycam"],
            "identity_confidence": "medium",
            "outcome_status": "unknown",
            "packet_verdict": "HOLD",
            "score_verdict": "HOLD",
            "verified_artifact_count": 1,
        },
        "result": {
            "verdict": "HOLD",
            "reason_codes": [
                "medium_identity",
                "media_artifact_present",
                "bodycam_present",
                "outcome_not_concluded_advisory",
            ],
            "risk_flags": ["identity_not_locked"],
        },
    })


def _blocked_cli_stdout(target_id: str, blocked_reason: str) -> str:
    return json.dumps({
        "input_summary": {
            "input_type": "portal_live",
            "target_id": target_id,
            "url": f"https://www.phoenix.gov/newsroom/police-department-news/{target_id}.html",
            "fetcher": "requests",
        },
        "live_fetch": {
            "target_id": target_id,
            "url": f"https://www.phoenix.gov/newsroom/police-department-news/{target_id}.html",
            "fetcher": "requests",
            "status": "blocked",
            "blocked_reason": blocked_reason,
            "status_code": None,
            "api_calls": {},
            "raw_payload_path": None,
            "extracted_payload_path": None,
            "replayed": False,
            "safety_status": "blocked",
            "target_domain_status": "allowed",
        },
    })


def _completed_proc(stdout, returncode=0, stderr=""):
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=stderr,
    )


# ---- fixture helpers ------------------------------------------------


def _make_fixture(
    path: Path,
    target_id: str,
    *,
    url: str = None,
    require_extraction: bool = True,
):
    fixture = {
        "target_id": target_id,
        "url": url or (
            f"https://www.phoenix.gov/newsroom/police-department-news/"
            f"{target_id}.html"
        ),
        "profile_id": "agency_ois_detail",
        "fetcher": "requests",
        "max_pages": 1,
        "max_links": 5,
        "expected_response_status": 200,
        "save_raw_payload": True,
        "allowed_domains": ["www.phoenix.gov"],
        "save_extracted_payload": require_extraction,
        "replay_through_portal_replay": require_extraction,
        "require_extraction": require_extraction,
    }
    path.write_text(json.dumps(fixture), encoding="utf-8")


def _make_safe_output_dir(tmp_path: Path) -> Path:
    """Build an output dir under .tmp/ at the repo root so the safe-path
    check inside the batch runner accepts it. Tests that exercise the
    full main() path need this; the CLI's _is_safe_bundle_path resolves
    paths against the repo root, so tmp_path-only paths fail the check."""
    target = REPO_ROOT / ".tmp" / "test_batch_runner" / tmp_path.name
    target.mkdir(parents=True, exist_ok=True)
    return target


def _run(argv):
    out = io.StringIO()
    err = io.StringIO()
    code = script.main(argv, stdout=out, stderr=err)
    return code, out.getvalue(), err.getvalue()


# ---- discovery ------------------------------------------------------


def test_dry_run_selects_matching_extract_required_fixtures_sorted(tmp_path, monkeypatch):
    """Default pattern matches *_real_extract_required.json only and
    returns them sorted."""
    fixtures_dir = tmp_path / "generated"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "b_real_extract_required.json", "b")
    _make_fixture(fixtures_dir / "a_real_extract_required.json", "a")
    _make_fixture(
        fixtures_dir / "c_real_fetch_only.json", "c", require_extraction=False,
    )

    output_dir = _make_safe_output_dir(tmp_path)
    code, out, err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
        "--json",
    ])
    assert code == 0, err
    payload = json.loads(out)
    assert payload["dry_run"] is True
    assert payload["selected_count"] == 2  # default pattern excludes fetch_only
    assert payload["attempted_count"] == 2
    target_ids = [r["target_id"] for r in payload["results"]]
    assert target_ids == ["a", "b"], "fixture order must be sorted"


def test_dry_run_does_not_call_cli_subprocess_or_sleep(tmp_path, monkeypatch):
    fixtures_dir = tmp_path / "generated"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "x_real_extract_required.json", "x")
    _make_fixture(fixtures_dir / "y_real_extract_required.json", "y")

    cli_calls: List = []
    sleep_calls: List = []

    def fail_subprocess(*args, **kwargs):
        cli_calls.append((args, kwargs))
        raise AssertionError("dry-run must not call subprocess")

    def record_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(script, "_run_cli_subprocess", fail_subprocess)
    monkeypatch.setattr(script, "_sleep", record_sleep)

    output_dir = _make_safe_output_dir(tmp_path)
    code, _out, _err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
    ])
    assert code == 0
    assert cli_calls == []
    assert sleep_calls == []


def test_dry_run_summary_json_shape(tmp_path):
    fixtures_dir = tmp_path / "generated"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "x_real_extract_required.json", "x")

    output_dir = _make_safe_output_dir(tmp_path)
    code, out, _err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    for key in (
        "run_id", "started_at", "finished_at", "elapsed_seconds",
        "dry_run", "selected_count", "attempted_count",
        "completed_count", "failed_count", "blocked_count",
        "skipped_count", "total_api_calls", "fixtures_selected",
        "output_dir", "delay_seconds", "continue_on_error", "results",
    ):
        assert key in payload, f"missing summary key {key!r}"
    assert payload["completed_count"] == 0
    assert payload["failed_count"] == 0
    assert payload["blocked_count"] == 0
    assert payload["total_api_calls"] == {}
    row = payload["results"][0]
    assert row["status"] == "dry_run"
    assert row["target_id"] == "x"
    assert Path(row["bundle_path"]).parts[-2:] == ("x", "bundle.json")


# ---- caps + selection error paths -----------------------------------


def test_max_targets_cap_truncates_selection(tmp_path):
    fixtures_dir = tmp_path / "generated"
    fixtures_dir.mkdir()
    for i in range(8):
        _make_fixture(
            fixtures_dir / f"target{i:02d}_real_extract_required.json",
            f"target{i:02d}",
        )
    output_dir = _make_safe_output_dir(tmp_path)
    code, out, _err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
        "--max-targets", "3",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["selected_count"] == 3
    assert [r["target_id"] for r in payload["results"]] == [
        "target00", "target01", "target02",
    ]


def test_no_fixtures_found_returns_clean_error(tmp_path):
    fixtures_dir = tmp_path / "empty"
    fixtures_dir.mkdir()
    output_dir = _make_safe_output_dir(tmp_path)
    code, _out, err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
    ])
    assert code == 2
    assert "no fixtures matched" in err


def test_explicit_fixture_paths_work(tmp_path):
    f1 = tmp_path / "a.json"
    f2 = tmp_path / "b.json"
    _make_fixture(f1, "a")
    _make_fixture(f2, "b")
    output_dir = _make_safe_output_dir(tmp_path)
    code, out, _err = _run([
        "--fixture", str(f1),
        "--fixture", str(f2),
        "--output-dir", str(output_dir),
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["selected_count"] == 2
    assert sorted(r["target_id"] for r in payload["results"]) == ["a", "b"]


def test_mutual_exclusion_of_fixture_and_fixtures_dir(tmp_path):
    fixtures_dir = tmp_path / "d"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "x_real_extract_required.json", "x")
    f = tmp_path / "y.json"
    _make_fixture(f, "y")
    output_dir = _make_safe_output_dir(tmp_path)
    code, _out, err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--fixture", str(f),
        "--output-dir", str(output_dir),
    ])
    assert code == 2
    assert "either --fixtures-dir or --fixture" in err


def test_max_targets_zero_rejected(tmp_path):
    fixtures_dir = tmp_path / "d"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "x_real_extract_required.json", "x")
    output_dir = _make_safe_output_dir(tmp_path)
    code, _out, err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
        "--max-targets", "0",
    ])
    assert code == 2
    assert "max-targets" in err


def test_unsafe_output_dir_rejected_at_main(tmp_path):
    """--output-dir must resolve under one of the gitignored artifact
    dirs. tmp_path is outside the repo so the safe-path check accepts
    it; this test forces an unsafe in-repo path to verify the gate."""
    fixtures_dir = tmp_path / "d"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "x_real_extract_required.json", "x")
    unsafe_output = REPO_ROOT / "pipeline2_discovery" / "_batch_should_be_rejected"
    code, _out, err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(unsafe_output),
    ])
    assert code == 2
    assert "safe artifact dirs" in err


# ---- run mode -------------------------------------------------------


def test_run_mode_calls_cli_subprocess_per_fixture(tmp_path, monkeypatch):
    fixtures_dir = tmp_path / "d"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "a_real_extract_required.json", "a")
    _make_fixture(fixtures_dir / "b_real_extract_required.json", "b")
    output_dir = _make_safe_output_dir(tmp_path)

    cli_calls: List[dict] = []

    def fake_cli(*, fixture_path, bundle_path, env, timeout=120.0):
        target_id = fixture_path.stem.split("_real_")[0]
        cli_calls.append(
            {"fixture": str(fixture_path), "bundle": str(bundle_path)}
        )
        return _completed_proc(_completed_cli_stdout(target_id))

    sleep_calls: List[float] = []
    monkeypatch.setattr(script, "_run_cli_subprocess", fake_cli)
    monkeypatch.setattr(script, "_sleep", lambda s: sleep_calls.append(s))

    code, out, _err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
        "--run",
        "--delay-seconds", "0.5",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["dry_run"] is False
    assert payload["completed_count"] == 2
    assert payload["failed_count"] == 0
    assert payload["blocked_count"] == 0
    assert payload["total_api_calls"] == {"requests": 2}
    assert len(cli_calls) == 2
    # Sleep called between targets only — not after the last.
    assert sleep_calls == [0.5]


def test_run_mode_aggregates_per_target_results(tmp_path, monkeypatch):
    fixtures_dir = tmp_path / "d"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "a_real_extract_required.json", "a")
    _make_fixture(fixtures_dir / "b_real_extract_required.json", "b")
    output_dir = _make_safe_output_dir(tmp_path)

    def fake_cli(*, fixture_path, bundle_path, env, timeout=120.0):
        target_id = fixture_path.stem.split("_real_")[0]
        return _completed_proc(_completed_cli_stdout(target_id))

    monkeypatch.setattr(script, "_run_cli_subprocess", fake_cli)
    monkeypatch.setattr(script, "_sleep", lambda s: None)

    code, out, _err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
        "--run",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    rows = payload["results"]
    assert all(r["status"] == "completed" for r in rows)
    assert all(r["verdict"] == "HOLD" for r in rows)
    assert all(r["verified_artifact_types"] == ["bodycam"] for r in rows)
    assert all(r["status_code"] == 200 for r in rows)
    assert all(r["api_calls"] == {"requests": 1} for r in rows)
    assert all(r["replayed"] is True for r in rows)
    for r in rows:
        assert Path(r["bundle_path"]).parts[-2:] == (r["target_id"], "bundle.json")


def test_run_mode_continue_on_error_continues_after_failure(tmp_path, monkeypatch):
    fixtures_dir = tmp_path / "d"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "a_real_extract_required.json", "a")
    _make_fixture(fixtures_dir / "b_real_extract_required.json", "b")
    _make_fixture(fixtures_dir / "c_real_extract_required.json", "c")
    output_dir = _make_safe_output_dir(tmp_path)

    def fake_cli(*, fixture_path, bundle_path, env, timeout=120.0):
        target_id = fixture_path.stem.split("_real_")[0]
        if target_id == "b":
            return _completed_proc("", returncode=2, stderr="boom")
        return _completed_proc(_completed_cli_stdout(target_id))

    monkeypatch.setattr(script, "_run_cli_subprocess", fake_cli)
    monkeypatch.setattr(script, "_sleep", lambda s: None)

    code, out, _err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
        "--run",
        "--continue-on-error",
        "--json",
    ])
    # With --continue-on-error, exit is 0 even though one fixture failed.
    assert code == 0
    payload = json.loads(out)
    assert payload["selected_count"] == 3
    assert payload["attempted_count"] == 3  # all three attempted
    assert payload["completed_count"] == 2
    assert payload["failed_count"] == 1
    statuses = [r["status"] for r in payload["results"]]
    assert statuses == ["completed", "failed", "completed"]


def test_run_mode_default_stops_on_first_failure(tmp_path, monkeypatch):
    fixtures_dir = tmp_path / "d"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "a_real_extract_required.json", "a")
    _make_fixture(fixtures_dir / "b_real_extract_required.json", "b")
    _make_fixture(fixtures_dir / "c_real_extract_required.json", "c")
    output_dir = _make_safe_output_dir(tmp_path)

    cli_calls: List[str] = []

    def fake_cli(*, fixture_path, bundle_path, env, timeout=120.0):
        target_id = fixture_path.stem.split("_real_")[0]
        cli_calls.append(target_id)
        if target_id == "a":
            return _completed_proc("", returncode=2, stderr="boom")
        return _completed_proc(_completed_cli_stdout(target_id))

    monkeypatch.setattr(script, "_run_cli_subprocess", fake_cli)
    monkeypatch.setattr(script, "_sleep", lambda s: None)

    code, out, _err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
        "--run",
        "--json",
    ])
    # First failure with default stop-on-first-failure -> exit 1.
    assert code == 1
    payload = json.loads(out)
    assert payload["selected_count"] == 3
    assert payload["attempted_count"] == 1
    assert payload["completed_count"] == 0
    assert payload["failed_count"] == 1
    # Subsequent fixtures must NOT have been attempted.
    assert cli_calls == ["a"]


def test_run_mode_blocked_status_stops_unless_continue_flag(tmp_path, monkeypatch):
    """Blocked (e.g. missing env gate, denied domain) is treated as a
    failure for stop/continue purposes."""
    fixtures_dir = tmp_path / "d"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "a_real_extract_required.json", "a")
    _make_fixture(fixtures_dir / "b_real_extract_required.json", "b")
    output_dir = _make_safe_output_dir(tmp_path)

    def fake_cli(*, fixture_path, bundle_path, env, timeout=120.0):
        target_id = fixture_path.stem.split("_real_")[0]
        return _completed_proc(_blocked_cli_stdout(
            target_id,
            "missing_env_gates:FLAMEON_RUN_LIVE_CASEGRAPH",
        ))

    monkeypatch.setattr(script, "_run_cli_subprocess", fake_cli)
    monkeypatch.setattr(script, "_sleep", lambda s: None)

    code, out, _err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
        "--run",
        "--json",
    ])
    # Blocked + default stop -> exit 0 (no failed, just blocked) BUT
    # the loop stopped after the first blocked fixture.
    assert code == 0
    payload = json.loads(out)
    assert payload["attempted_count"] == 1
    assert payload["blocked_count"] == 1
    assert payload["completed_count"] == 0


def test_run_mode_summary_includes_api_call_totals(tmp_path, monkeypatch):
    fixtures_dir = tmp_path / "d"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "a_real_extract_required.json", "a")
    _make_fixture(fixtures_dir / "b_real_extract_required.json", "b")
    _make_fixture(fixtures_dir / "c_real_extract_required.json", "c")
    output_dir = _make_safe_output_dir(tmp_path)

    def fake_cli(*, fixture_path, bundle_path, env, timeout=120.0):
        target_id = fixture_path.stem.split("_real_")[0]
        return _completed_proc(_completed_cli_stdout(target_id))

    monkeypatch.setattr(script, "_run_cli_subprocess", fake_cli)
    monkeypatch.setattr(script, "_sleep", lambda s: None)

    code, out, _err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
        "--run",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["completed_count"] == 3
    assert payload["total_api_calls"] == {"requests": 3}


def test_run_mode_per_target_bundle_paths_are_deterministic(tmp_path, monkeypatch):
    fixtures_dir = tmp_path / "d"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "a_real_extract_required.json", "a")
    output_dir = _make_safe_output_dir(tmp_path)

    captured_bundle_paths: List[str] = []

    def fake_cli(*, fixture_path, bundle_path, env, timeout=120.0):
        captured_bundle_paths.append(str(bundle_path))
        return _completed_proc(_completed_cli_stdout("a"))

    monkeypatch.setattr(script, "_run_cli_subprocess", fake_cli)
    monkeypatch.setattr(script, "_sleep", lambda s: None)

    code, out, _err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
        "--run",
        "--json",
    ])
    assert code == 0
    assert len(captured_bundle_paths) == 1
    assert Path(captured_bundle_paths[0]).parts[-2:] == ("a", "bundle.json")
    payload = json.loads(out)
    assert Path(payload["results"][0]["bundle_path"]).parts[-2:] == ("a", "bundle.json")


def test_run_mode_writes_summary_json_when_requested(tmp_path, monkeypatch):
    fixtures_dir = tmp_path / "d"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "a_real_extract_required.json", "a")
    output_dir = _make_safe_output_dir(tmp_path)
    summary_out = output_dir / "summary.json"

    def fake_cli(*, fixture_path, bundle_path, env, timeout=120.0):
        return _completed_proc(_completed_cli_stdout("a"))

    monkeypatch.setattr(script, "_run_cli_subprocess", fake_cli)
    monkeypatch.setattr(script, "_sleep", lambda s: None)

    code, _out, _err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
        "--summary-out", str(summary_out),
        "--run",
        "--json",
    ])
    assert code == 0
    assert summary_out.exists()
    written = json.loads(summary_out.read_text(encoding="utf-8"))
    assert written["completed_count"] == 1


# ---- zero-network ---------------------------------------------------


def test_dry_run_makes_zero_network_calls(monkeypatch, tmp_path):
    import requests

    def fail_get(self, *args, **kwargs):
        raise AssertionError("batch runner must never touch the network")

    monkeypatch.setattr(requests.Session, "get", fail_get)

    fixtures_dir = tmp_path / "d"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "a_real_extract_required.json", "a")
    output_dir = _make_safe_output_dir(tmp_path)
    code, _out, _err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
    ])
    assert code == 0


def test_run_mode_with_monkeypatched_subprocess_makes_zero_network_calls(
    monkeypatch, tmp_path,
):
    import requests

    def fail_get(self, *args, **kwargs):
        raise AssertionError("batch runner subprocess wrapper must be patched")

    monkeypatch.setattr(requests.Session, "get", fail_get)

    fixtures_dir = tmp_path / "d"
    fixtures_dir.mkdir()
    _make_fixture(fixtures_dir / "a_real_extract_required.json", "a")
    output_dir = _make_safe_output_dir(tmp_path)

    monkeypatch.setattr(
        script, "_run_cli_subprocess",
        lambda **kw: _completed_proc(_completed_cli_stdout("a")),
    )
    monkeypatch.setattr(script, "_sleep", lambda s: None)

    code, _out, _err = _run([
        "--fixtures-dir", str(fixtures_dir),
        "--output-dir", str(output_dir),
        "--run",
    ])
    assert code == 0


# ---- subprocess invocation forms (sys.path bootstrap regression) ----
#
# Mirrors the subprocess-level guards from PR #28 / #29.


def _run_subprocess(argv, cwd=REPO_ROOT, env=None):
    return subprocess.run(
        argv, cwd=str(cwd), capture_output=True, text=True, env=env, timeout=30,
    )


def test_direct_script_invocation_help_exits_zero():
    result = _run_subprocess([sys.executable, str(SCRIPT_PATH), "--help"])
    assert result.returncode == 0, (
        f"direct --help failed:\nstdout={result.stdout!r}\n"
        f"stderr={result.stderr!r}"
    )
    assert "--fixtures-dir" in result.stdout
    assert "--run" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


def test_module_invocation_help_exits_zero():
    result = _run_subprocess(
        [sys.executable, "-m", "tools.run_portal_live_batch", "--help"],
    )
    assert result.returncode == 0, (
        f"module --help failed:\nstdout={result.stdout!r}\n"
        f"stderr={result.stderr!r}"
    )
    assert "--fixtures-dir" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr
