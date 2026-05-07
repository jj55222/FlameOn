"""End-to-end tests for ``tools/regenerate_packet_briefs.py``.

Synthetic two-packet fixture under ``tmp_path`` — one A-grade with a
populated ``involved_officers[]``, one B-grade without. Tests:

  - regenerates 1 brief by default (grade=A only)
  - regenerates 2 briefs when --grade A --grade B is passed
  - the regenerated brief carries the auto-enriched officers in
    section 4 (proves enriched master fields surface in the renderer)
  - the input JSONL is byte-identical before and after the run
  - --baseline-dir produces a size-delta column in the report
  - missing transcript dir does not crash the run
  - no leftover *.tmp temp packet files after success
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from tools.regenerate_packet_briefs import main as cli_main  # noqa: E402


PACKETS = [
    {
        "packet_id": "pcs-test-A1",
        "subject_or_case": "Test Subject — Test PD pursuit, 2024-01-15",
        "agency": "Test PD",
        "jurisdiction": "Testville, TS",
        "incident_type": "fatal pursuit",
        "incident_date": "2024-01-15",
        "confidence_grade": "A",
        "confidence_reason": "synthetic A-grade fixture",
        "source_urls": [
            "https://www.justice.gov/usao-tx/pr/officer-convicted",
            "https://www.youtube.com/watch?v=abc123",
        ],
        "transcript_paths": ["transcripts/abc123/abc123.txt"],
        "involved_officers": [
            {
                "name": "Officer Synthetic Sutton",
                "role": "officer",
                "status": "convicted",
                "agency": "Test PD",
                "badge": None,
            },
        ],
        "case_outcome": {
            "conviction_status": "convicted (per fixture)",
            "doj_url": "https://www.justice.gov/usao-tx/pr/officer-convicted",
        },
    },
    {
        "packet_id": "pcs-test-B1",
        "subject_or_case": "Other Subject — Other PD",
        "agency": "Other PD",
        "confidence_grade": "B",
        "source_urls": ["https://example.com/news"],
        "transcript_paths": ["transcripts/def456/def456.txt"],
    },
]


TRANSCRIPT = """
this is a synthetic caption transcript with enough content to clear
the noise filter for paragraph chunks in the adapter
officer synthetic sutton was charged with fatal pursuit policy violation
the incident occurred on 2024-01-15 in testville
"""


def _setup(tmp_path: Path) -> tuple[Path, Path]:
    """Write packets_master.jsonl + transcripts. Returns (jsonl, transcript_root)."""
    transcript_root = tmp_path
    for vid_id in ("abc123", "def456"):
        d = transcript_root / "transcripts" / vid_id
        d.mkdir(parents=True)
        (d / f"{vid_id}.txt").write_text(TRANSCRIPT, encoding="utf-8")
    pm = tmp_path / "packets_master_enriched.jsonl"
    with open(pm, "w", encoding="utf-8") as f:
        for p in PACKETS:
            f.write(json.dumps(p) + "\n")
    return pm, transcript_root


def test_regenerator_default_filter_builds_one_a_brief(tmp_path):
    pm, troot = _setup(tmp_path)
    out = tmp_path / "out"
    rc = cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
    ])
    assert rc == 0
    assert (out / "pcs-test-A1_packet_brief.md").exists()
    assert (out / "pcs-test-A1_packet_brief.json").exists()
    assert not (out / "pcs-test-B1_packet_brief.md").exists()
    assert (out / "REGENERATION_REPORT.md").exists()


def test_regenerator_grade_filter_includes_b(tmp_path):
    pm, troot = _setup(tmp_path)
    out = tmp_path / "out"
    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
        "--grade", "A",
        "--grade", "B",
    ])
    assert (out / "pcs-test-A1_packet_brief.md").exists()
    assert (out / "pcs-test-B1_packet_brief.md").exists()


def test_regenerator_renders_enriched_officers_in_section_4(tmp_path):
    """The whole point of the regenerator + enriched master loop:
    auto-extracted officers in the input packet must surface in
    section 4 of the regenerated brief, with no hand curation."""
    pm, troot = _setup(tmp_path)
    out = tmp_path / "out"
    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
    ])
    md = (out / "pcs-test-A1_packet_brief.md").read_text(encoding="utf-8")
    assert "## 4. Key people" in md
    assert "Officer Synthetic Sutton" in md
    assert "convicted" in md  # status from the optional field
    # Section 8 should also carry the case_outcome block.
    s8_idx = md.index("## 8. Why this case matters")
    s9_idx = md.index("## 9. Missing fields")
    assert "convicted (per fixture)" in md[s8_idx:s9_idx]


def test_regenerator_does_not_mutate_input_file(tmp_path):
    pm, troot = _setup(tmp_path)
    h_before = hashlib.sha256(pm.read_bytes()).hexdigest()
    out = tmp_path / "out"
    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
    ])
    h_after = hashlib.sha256(pm.read_bytes()).hexdigest()
    assert h_before == h_after


def test_regenerator_baseline_dir_produces_delta_column(tmp_path):
    """When --baseline-dir is passed, the report includes a delta column
    comparing each new brief's size to the baseline copy."""
    pm, troot = _setup(tmp_path)
    out = tmp_path / "out"
    baseline = tmp_path / "baseline"
    baseline.mkdir()
    # Seed baseline with a much smaller "old" brief so the delta is
    # large + positive.
    (baseline / "pcs-test-A1_packet_brief.md").write_text("# tiny\n", encoding="utf-8")

    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
        "--baseline-dir", str(baseline),
    ])
    report = (out / "REGENERATION_REPORT.md").read_text(encoding="utf-8")
    assert "baseline size" in report
    assert "delta" in report
    # The delta cell should be a positive value because the regenerated
    # brief is much larger than the seeded 7-byte baseline.
    assert "+" in report


def test_regenerator_missing_transcripts_does_not_crash(tmp_path):
    """A packet whose transcript_paths cannot be resolved should still
    produce a brief — section 7 just falls back to the
    no-captions placeholder. The CLI must not crash."""
    pm = tmp_path / "no_transcripts.jsonl"
    packet_no_trans = {
        "packet_id": "pcs-test-no-trans",
        "subject_or_case": "Subject Without Transcripts — Some PD",
        "agency": "Some PD",
        "confidence_grade": "A",
        "transcript_paths": ["does/not/exist.txt"],
    }
    with open(pm, "w", encoding="utf-8") as f:
        f.write(json.dumps(packet_no_trans) + "\n")

    out = tmp_path / "out"
    rc = cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(tmp_path),
        "--output-dir", str(out),
    ])
    assert rc == 0
    md_path = out / "pcs-test-no-trans_packet_brief.md"
    assert md_path.exists()
    md = md_path.read_text(encoding="utf-8")
    assert "no captioned transcripts available" in md


def test_regenerator_leaves_no_temp_files_on_success(tmp_path):
    """Per-packet temp packet JSON files used to drive ``assemble_packet``
    must be cleaned up after a successful run."""
    pm, troot = _setup(tmp_path)
    out = tmp_path / "out"
    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
    ])
    # The only files in out/ should be the packet brief md/json + the report.
    leftover = sorted(p.name for p in out.iterdir())
    assert leftover == sorted([
        "pcs-test-A1_packet_brief.md",
        "pcs-test-A1_packet_brief.json",
        "REGENERATION_REPORT.md",
    ])


def test_regenerator_missing_input_returns_error(tmp_path):
    rc = cli_main([
        "--packets-master", str(tmp_path / "no-such-file.jsonl"),
    ])
    assert rc == 2
