"""Zero-network tests for ``tools/generate_portal_live_targets.py``.

The script is a thin argparse wrapper over
``portal_live_target_generator.generate_targets``; these tests pin
its operator-facing surface (exit codes, --dry-run, --json output,
file writes) so a future refactor can't silently change behavior.

Two kinds of tests:

  - In-process tests call ``script.main(...)`` directly — fast, but
    they bypass Python's import-path setup and so cannot catch
    invocation-form regressions.
  - Subprocess tests invoke the real ``python`` interpreter with
    either ``python tools/generate_portal_live_targets.py`` or
    ``python -m tools.generate_portal_live_targets``. These are the
    only tests that catch ``ModuleNotFoundError: No module named
    'pipeline2_discovery'`` regressions when the script is run
    directly from the repo root.
"""
from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

import pytest

from tools import generate_portal_live_targets as script


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "tools" / "generate_portal_live_targets.py"


PHOENIX_3286 = "https://www.phoenix.gov/newsroom/police-department-news/3286.html"
PHOENIX_3369 = "https://www.phoenix.gov/newsroom/police-department-news/3369.html"


def _write_input(tmp_path, rows):
    p = tmp_path / "input.json"
    p.write_text(json.dumps(rows), encoding="utf-8")
    return p


def _run(argv):
    out = io.StringIO()
    err = io.StringIO()
    code = script.main(argv, stdout=out, stderr=err)
    return code, out.getvalue(), err.getvalue()


def test_dry_run_emits_human_report_and_writes_no_files(tmp_path):
    inp = _write_input(tmp_path, [{"url": PHOENIX_3369}])
    out_dir = tmp_path / "generated"
    code, out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(out_dir),
        "--mode", "both",
        "--dry-run",
    ])
    assert code == 0
    assert "dry_run:         True" in out
    assert "accepted:        1" in out
    assert "files written:   2" in out
    # Output dir must not be created in dry-run mode.
    assert not out_dir.exists()


def test_writes_fixtures_to_output_dir(tmp_path):
    inp = _write_input(tmp_path, [
        {"url": PHOENIX_3369},
        {"url": PHOENIX_3286},
    ])
    out_dir = tmp_path / "generated"
    code, _out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(out_dir),
        "--mode", "fetch_only",
    ])
    assert code == 0
    written = sorted(p.name for p in out_dir.iterdir())
    assert written == [
        "phoenix_pd_newsroom_3286_real_fetch_only.json",
        "phoenix_pd_newsroom_3369_real_fetch_only.json",
    ]


def test_json_mode_emits_machine_readable_report(tmp_path):
    inp = _write_input(tmp_path, [{"url": PHOENIX_3369}])
    out_dir = tmp_path / "generated"
    code, out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(out_dir),
        "--mode", "fetch_only",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["accepted_count"] == 1
    assert payload["rejected_count"] == 0
    assert payload["dry_run"] is False
    assert len(payload["written_paths"]) == 1
    assert payload["written_paths"][0].endswith(
        "phoenix_pd_newsroom_3369_real_fetch_only.json"
    )


def test_input_must_be_json_array(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"not_a_list": True}), encoding="utf-8")
    out_dir = tmp_path / "out"
    code, _out, err = _run([
        "--input", str(bad),
        "--output-dir", str(out_dir),
    ])
    assert code == 2
    assert "JSON array" in err


def test_input_array_entries_must_be_objects(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps([PHOENIX_3369, PHOENIX_3286]), encoding="utf-8")
    out_dir = tmp_path / "out"
    code, _out, err = _run([
        "--input", str(bad),
        "--output-dir", str(out_dir),
    ])
    assert code == 2
    assert "JSON object" in err


def test_missing_input_returns_clean_error(tmp_path):
    code, _out, err = _run([
        "--input", str(tmp_path / "nope.json"),
        "--output-dir", str(tmp_path / "out"),
    ])
    assert code == 2
    assert "input not found" in err


def test_max_targets_flag_caps_writes(tmp_path):
    rows = [
        {
            "url": (
                f"https://www.phoenix.gov/newsroom/police-department-news/{i}.html"
            )
        }
        for i in range(1000, 1010)
    ]
    inp = _write_input(tmp_path, rows)
    out_dir = tmp_path / "generated"
    code, out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(out_dir),
        "--mode", "fetch_only",
        "--max-targets", "2",
    ])
    assert code == 0
    written = list(out_dir.iterdir())
    assert len(written) == 2
    assert "max_targets_cap: 8" in out


def test_human_report_lists_rejected_rows_with_reasons(tmp_path):
    inp = _write_input(tmp_path, [
        {"url": "http://www.phoenix.gov/newsroom/police-department-news/x.html"},
        {"url": "https://www.muckrock.com/x"},
        {"url": PHOENIX_3286},
    ])
    out_dir = tmp_path / "generated"
    code, out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(out_dir),
        "--mode", "fetch_only",
    ])
    assert code == 0
    assert "rejected rows:" in out
    assert "non_https_scheme" in out
    assert "host_not_in_allowlist" in out
    assert "accepted:        1" in out
    assert "rejected:        2" in out


def test_script_makes_zero_network_calls(monkeypatch, tmp_path):
    import requests

    def fail_get(self, *args, **kwargs):
        raise AssertionError("script must never touch the network")

    monkeypatch.setattr(requests.Session, "get", fail_get)
    inp = _write_input(tmp_path, [
        {"url": PHOENIX_3369},
        {"url": PHOENIX_3286},
        {"url": "https://www.muckrock.com/x"},  # rejected at lint
    ])
    out_dir = tmp_path / "generated"
    code, _out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(out_dir),
        "--mode", "both",
    ])
    assert code == 0


def test_invalid_mode_choice_rejected_by_argparse(tmp_path):
    """argparse's ``choices`` constraint rejects unknown modes with
    exit 2 before reaching the generator. Pinned so adding a new mode
    requires updating both the choices and this test together."""
    inp = _write_input(tmp_path, [{"url": PHOENIX_3369}])
    with pytest.raises(SystemExit) as exc_info:
        script.main([
            "--input", str(inp),
            "--output-dir", str(tmp_path / "out"),
            "--mode", "invalid_mode",
        ])
    assert exc_info.value.code == 2


# ---- subprocess-level invocation regressions -------------------------
#
# These tests are the only ones that catch the direct-vs-module
# invocation gap. ``python tools/generate_portal_live_targets.py``
# only puts ``tools/`` on sys.path, so the script's import of
# ``pipeline2_discovery.casegraph...`` would fail with
# ``ModuleNotFoundError`` without the repo-root bootstrap at the top
# of the script. The in-process tests above cannot catch this because
# pytest's rootdir handling already places the repo root on sys.path.


def _run_subprocess(argv, cwd=REPO_ROOT, env=None):
    """Run the script as a real subprocess from the repo root."""
    return subprocess.run(
        argv,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )


def test_direct_script_invocation_help_exits_zero():
    """``python tools/generate_portal_live_targets.py --help`` must
    succeed from the repo root. Regression guard for the
    ModuleNotFoundError that surfaced during PR #27 operational
    validation."""
    result = _run_subprocess(
        [sys.executable, str(SCRIPT_PATH), "--help"],
    )
    assert result.returncode == 0, (
        f"direct --help failed:\nstdout={result.stdout!r}\n"
        f"stderr={result.stderr!r}"
    )
    assert "--input" in result.stdout
    assert "--output-dir" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


def test_direct_script_invocation_dry_run_exits_zero(tmp_path):
    """End-to-end: direct script invocation runs the generator, lints
    a real Phoenix URL, exits 0 in dry-run mode."""
    inp = _write_input(tmp_path, [{"url": PHOENIX_3369}])
    out_dir = tmp_path / "generated"
    result = _run_subprocess([
        sys.executable, str(SCRIPT_PATH),
        "--input", str(inp),
        "--output-dir", str(out_dir),
        "--mode", "fetch_only",
        "--dry-run",
        "--json",
    ])
    assert result.returncode == 0, (
        f"direct dry-run failed:\nstdout={result.stdout!r}\n"
        f"stderr={result.stderr!r}"
    )
    payload = json.loads(result.stdout)
    assert payload["accepted_count"] == 1
    assert payload["dry_run"] is True
    assert not out_dir.exists()


def test_module_invocation_help_exits_zero():
    """``python -m tools.generate_portal_live_targets --help`` must
    keep working alongside the direct invocation."""
    result = _run_subprocess(
        [sys.executable, "-m", "tools.generate_portal_live_targets", "--help"],
    )
    assert result.returncode == 0, (
        f"module --help failed:\nstdout={result.stdout!r}\n"
        f"stderr={result.stderr!r}"
    )
    assert "--input" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


def test_module_invocation_dry_run_exits_zero(tmp_path):
    inp = _write_input(tmp_path, [{"url": PHOENIX_3286}])
    out_dir = tmp_path / "generated"
    result = _run_subprocess([
        sys.executable, "-m", "tools.generate_portal_live_targets",
        "--input", str(inp),
        "--output-dir", str(out_dir),
        "--mode", "fetch_only",
        "--dry-run",
        "--json",
    ])
    assert result.returncode == 0, (
        f"module dry-run failed:\nstdout={result.stdout!r}\n"
        f"stderr={result.stderr!r}"
    )
    payload = json.loads(result.stdout)
    assert payload["accepted_count"] == 1
    assert payload["dry_run"] is True


def test_subprocess_invocations_make_zero_real_network_calls(tmp_path):
    """Smoke-level zero-network guard for the subprocess path. The
    monkeypatch trick used by the in-process tests doesn't cross
    process boundaries, so we instead assert that both invocation
    forms succeed in dry-run with a non-trivial input and never
    surface a network-related error in stderr. Combined with the
    in-process monkeypatch tests above, this triangulates that no
    code path under test reaches the network."""
    inp = _write_input(tmp_path, [
        {"url": PHOENIX_3369},
        {"url": "https://www.muckrock.com/x"},  # rejected at lint
    ])
    out_dir = tmp_path / "generated"
    for argv in (
        [sys.executable, str(SCRIPT_PATH)],
        [sys.executable, "-m", "tools.generate_portal_live_targets"],
    ):
        result = _run_subprocess(argv + [
            "--input", str(inp),
            "--output-dir", str(out_dir),
            "--mode", "both",
            "--dry-run",
        ])
        assert result.returncode == 0
        for needle in (
            "ConnectionError", "TimeoutError", "TLS",
            "SSL", "Connection refused", "Name or service",
        ):
            assert needle not in result.stderr, (
                f"network-related error in stderr: {needle!r}"
            )
