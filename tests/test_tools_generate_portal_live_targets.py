"""Zero-network tests for ``tools/generate_portal_live_targets.py``.

The script is a thin argparse wrapper over
``portal_live_target_generator.generate_targets``; these tests pin
its operator-facing surface (exit codes, --dry-run, --json output,
file writes) so a future refactor can't silently change behavior.
"""
from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from tools import generate_portal_live_targets as script


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
