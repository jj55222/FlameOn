"""Zero-network tests for ``tools/run_dataset_intake.py``."""
from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

import pytest

from tools import run_dataset_intake as script


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "tools" / "run_dataset_intake.py"


SAMPLE_SFC_CSV = """\
date,year,number_killed,name,initial_reason,person_role,main_agency,news_urls,city,county,state,in_fars_pursuit
2025-04-12,2025,2,John Doe,traffic stop,bystander,Phoenix Police Department,https://example.com/news/john-doe,Phoenix,Maricopa,AZ,1
2024-12-20,2024,1,,warrant,driver,Some Sheriff,,Anywhere,Anywhere,WY,
"""


def _run(argv):
    out = io.StringIO()
    err = io.StringIO()
    code = script.main(argv, stdout=out, stderr=err)
    return code, out.getvalue(), err.getvalue()


def _write(p: Path, body: str) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(body, encoding="utf-8")
    return p


# ---- SF Chronicle path ---------------------------------------------


def test_sfchronicle_intake_writes_csv_json_tasks_and_stubs(tmp_path):
    inp = _write(tmp_path / "sfc.csv", SAMPLE_SFC_CSV)
    csv_out = tmp_path / "candidates.csv"
    json_out = tmp_path / "candidates.json"
    tasks_out = tmp_path / "tasks.json"
    stubs_dir = tmp_path / "stubs"

    code, out, err = _run([
        "--source", "sfchronicle_pursuits",
        "--input", str(inp),
        "--output-csv", str(csv_out),
        "--output-json", str(json_out),
        "--search-tasks-out", str(tasks_out),
        "--packet-stubs-dir", str(stubs_dir),
        "--top-n", "10",
        "--json",
    ])
    assert code == 0, err

    # All three flat outputs created.
    assert csv_out.exists()
    assert json_out.exists()
    assert tasks_out.exists()

    # candidates.json round-trips and is sorted by descending score.
    candidates = json.loads(json_out.read_text(encoding="utf-8"))
    assert len(candidates) == 2
    assert candidates[0]["packet_priority_score"] >= candidates[1]["packet_priority_score"]
    assert candidates[0]["grade"] == "A"  # John Doe row
    assert candidates[0]["candidate_id"] == "sfchronicle_pursuits:0"

    # search_tasks.json carries a flat tasks list with task types.
    tasks_doc = json.loads(tasks_out.read_text(encoding="utf-8"))
    assert tasks_doc["source_lane"] == "sfchronicle_pursuits"
    assert tasks_doc["candidate_count"] == 2
    assert tasks_doc["task_count"] == len(tasks_doc["tasks"])
    task_types = {t["task_type"] for t in tasks_doc["tasks"]}
    assert "youtube_query" in task_types
    assert "muckrock_query" in task_types
    assert "official_source_query" in task_types
    assert "outcome_query" in task_types
    # Every task carries a candidate_id and a grade.
    for t in tasks_doc["tasks"]:
        assert t["candidate_id"]
        assert t["grade"] in ("A", "B", "C", "D")

    # CSV header + body.
    csv_text = csv_out.read_text(encoding="utf-8")
    assert "candidate_id,grade,packet_priority_score" in csv_text
    assert "John Doe" in csv_text

    # Packet stubs: one .md per top-N candidate.
    stub_files = sorted(stubs_dir.glob("*.md"))
    assert len(stub_files) == 2
    first_stub = stub_files[0].read_text(encoding="utf-8")
    assert "# Packet stub:" in first_stub
    assert "**Grade:**" in first_stub
    assert "Phoenix Police Department" in first_stub or "Some Sheriff" in first_stub


def test_sfchronicle_intake_top_n_truncates_stubs(tmp_path):
    inp = _write(tmp_path / "sfc.csv", SAMPLE_SFC_CSV)
    stubs_dir = tmp_path / "stubs"
    code, _out, _err = _run([
        "--source", "sfchronicle_pursuits",
        "--input", str(inp),
        "--packet-stubs-dir", str(stubs_dir),
        "--top-n", "1",
    ])
    assert code == 0
    stubs = list(stubs_dir.glob("*.md"))
    assert len(stubs) == 1


def test_sfchronicle_intake_target_states_override_changes_grade(tmp_path):
    """Changing --target-states should re-bonus rows. The WY row
    becomes grade C (not D) under target-states=WY because the +2
    state bonus pushes its score from 1 to 3."""
    inp = _write(tmp_path / "sfc.csv", SAMPLE_SFC_CSV)
    json_out = tmp_path / "candidates.json"
    code, _out, _err = _run([
        "--source", "sfchronicle_pursuits",
        "--input", str(inp),
        "--output-json", str(json_out),
        "--target-states", "WY",
    ])
    assert code == 0
    candidates = json.loads(json_out.read_text(encoding="utf-8"))
    wy_row = [c for c in candidates if c["jurisdiction_state"] == "WY"][0]
    assert wy_row["packet_priority_score"] == 3
    assert wy_row["grade"] == "C"


def test_human_report_summarises_grade_counts_and_top_n(tmp_path):
    inp = _write(tmp_path / "sfc.csv", SAMPLE_SFC_CSV)
    code, out, _err = _run([
        "--source", "sfchronicle_pursuits",
        "--input", str(inp),
    ])
    assert code == 0
    assert "candidates:       2" in out
    assert "A: 1" in out
    assert "D: 1" in out
    assert "top 10:" in out


# ---- MuckRock URL-list path -----------------------------------------


def test_muckrock_curated_urls_path(tmp_path):
    inp = _write(
        tmp_path / "urls.txt",
        "https://www.muckrock.com/foi/phoenix/1001-body-worn-camera-2024-09-22/\n"
        "https://www.muckrock.com/foi/tampa/1004-policy-manual/\n",
    )
    json_out = tmp_path / "candidates.json"
    code, _out, _err = _run([
        "--source", "muckrock_curated",
        "--input", str(inp),
        "--output-json", str(json_out),
    ])
    assert code == 0
    candidates = json.loads(json_out.read_text(encoding="utf-8"))
    assert len(candidates) == 2
    grades = {c["candidate_id"]: c["grade"] for c in candidates}
    assert grades["muckrock_curated:1001"] == "A"
    assert grades["muckrock_curated:1004"] == "D"


def test_muckrock_curated_json_path(tmp_path):
    rec = {
        "id": 9999,
        "title": "Body-worn camera footage from John Doe officer-involved shooting on 2024-09-22",
        "agency": {"name": "Phoenix Police Department"},
        "status": "done",
        "files": [{"url": "https://example.com/bwc.mp4", "name": "bwc.mp4"}],
        "absolute_url": "/foi/phoenix/9999-bwc-john-doe/",
    }
    inp = _write(tmp_path / "records.json", json.dumps([rec]))
    json_out = tmp_path / "candidates.json"
    code, _out, _err = _run([
        "--source", "muckrock_curated",
        "--input", str(inp),
        "--output-json", str(json_out),
    ])
    assert code == 0
    candidates = json.loads(json_out.read_text(encoding="utf-8"))
    assert len(candidates) == 1
    assert candidates[0]["grade"] == "A"
    assert candidates[0]["agency_name"] == "Phoenix Police Department"


# ---- error handling -------------------------------------------------


def test_missing_input_returns_clean_error(tmp_path):
    code, _out, err = _run([
        "--source", "sfchronicle_pursuits",
        "--input", str(tmp_path / "nope.csv"),
    ])
    assert code == 2
    assert "input not found" in err


def test_invalid_source_rejected_by_argparse(tmp_path):
    inp = _write(tmp_path / "sfc.csv", SAMPLE_SFC_CSV)
    with pytest.raises(SystemExit) as exc_info:
        script.main([
            "--source", "not_a_real_source",
            "--input", str(inp),
        ])
    assert exc_info.value.code == 2


def test_negative_top_n_rejected(tmp_path):
    inp = _write(tmp_path / "sfc.csv", SAMPLE_SFC_CSV)
    code, _out, err = _run([
        "--source", "sfchronicle_pursuits",
        "--input", str(inp),
        "--top-n", "-1",
    ])
    assert code == 2
    assert "top-n" in err


# ---- subprocess invocation -----------------------------------------


def _run_subprocess(argv, cwd=REPO_ROOT, env=None):
    return subprocess.run(
        argv, cwd=str(cwd), capture_output=True, text=True, env=env, timeout=30,
    )


def test_direct_script_invocation_help_exits_zero():
    result = _run_subprocess([sys.executable, str(SCRIPT_PATH), "--help"])
    assert result.returncode == 0, (
        f"stderr={result.stderr!r}\nstdout={result.stdout!r}"
    )
    assert "--source" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


def test_module_invocation_help_exits_zero():
    result = _run_subprocess(
        [sys.executable, "-m", "tools.run_dataset_intake", "--help"],
    )
    assert result.returncode == 0
    assert "--source" in result.stdout


# ---- zero-network ---------------------------------------------------


def test_intake_makes_zero_network_calls(monkeypatch, tmp_path):
    import requests

    def fail_get(self, *args, **kwargs):
        raise AssertionError("intake script must never touch the network")

    monkeypatch.setattr(requests.Session, "get", fail_get)

    inp = _write(tmp_path / "sfc.csv", SAMPLE_SFC_CSV)
    code, _out, _err = _run([
        "--source", "sfchronicle_pursuits",
        "--input", str(inp),
        "--output-json", str(tmp_path / "out.json"),
    ])
    assert code == 0
