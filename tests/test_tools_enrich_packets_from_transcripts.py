"""End-to-end tests for ``tools/enrich_packets_from_transcripts.py``.

Synthetic two-packet fixture under ``tmp_path`` exercises the full CLI:

  - reads packets_master.jsonl
  - resolves transcript paths against a custom --transcript-root
  - applies the deterministic extractor
  - writes enriched JSONL + provenance JSON + Markdown report

Asserts:
  - the input file is byte-identical before and after (no mutation)
  - the enriched JSONL carries the new optional fields
  - the provenance JSON tracks per-field source / count
  - --grade filter works
  - --dry-run does not write outputs
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from tools.enrich_packets_from_transcripts import main as cli_main  # noqa: E402


# Two synthetic packets: one A-grade with a DOJ URL + a transcript that
# triggers all three extractors; one B-grade that should be skipped by
# the default A-only filter but processed when --grade B is added.
PACKETS = [
    {
        "packet_id": "pcs-test-A1",
        "subject_or_case": "Test Subject — Test PD pursuit, 2024-01-15",
        "agency": "Test PD",
        "confidence_grade": "A",
        "source_urls": [
            "https://www.justice.gov/usao-tx/pr/officer-convicted-of-pursuit-violation",
            "https://www.youtube.com/watch?v=abc123",
        ],
        "transcript_paths": ["transcripts/abc123/abc123.txt"],
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


TRANSCRIPT_A = """
officer terrence sutton was indicted today
for the death of test subject in the pursuit
his sister jane doe filed a wrongful death
lawsuit against the department
jane doe spoke to reporters outside court
"""


TRANSCRIPT_B = """
officer alpha bravo arrived at the scene
his sister kim doe is a witness in this case
of other subject in the recent incident
"""


def _setup_fixture(tmp_path: Path) -> tuple[Path, Path]:
    """Write packets_master.jsonl + transcripts under tmp_path. Returns
    (packets_master_path, transcript_root)."""
    transcript_root = tmp_path
    (transcript_root / "transcripts" / "abc123").mkdir(parents=True)
    (transcript_root / "transcripts" / "abc123" / "abc123.txt").write_text(
        TRANSCRIPT_A, encoding="utf-8"
    )
    (transcript_root / "transcripts" / "def456").mkdir(parents=True)
    (transcript_root / "transcripts" / "def456" / "def456.txt").write_text(
        TRANSCRIPT_B, encoding="utf-8"
    )

    pm = tmp_path / "packets_master.jsonl"
    with open(pm, "w", encoding="utf-8") as f:
        for p in PACKETS:
            f.write(json.dumps(p) + "\n")
    return pm, transcript_root


def test_cli_writes_three_output_files(tmp_path):
    pm, troot = _setup_fixture(tmp_path)
    out = tmp_path / "out"
    rc = cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
    ])
    assert rc == 0
    assert (out / "packets_master_enriched.jsonl").exists()
    assert (out / "PACKET_ENRICHMENT_PROVENANCE.json").exists()
    assert (out / "EXTRACTION_VALIDATION.md").exists()


def test_cli_does_not_mutate_input_file(tmp_path):
    pm, troot = _setup_fixture(tmp_path)
    before = pm.read_bytes()
    out = tmp_path / "out"
    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
    ])
    after = pm.read_bytes()
    assert before == after, "input packets_master.jsonl was mutated"


def _read_jsonl_rows(path: Path) -> list[dict]:
    return [
        json.loads(ln)
        for ln in path.read_text(encoding="utf-8").splitlines()
        if ln.strip()
    ]


def test_cli_default_filter_keeps_all_rows_drop_in_master(tmp_path):
    """The enriched master is a drop-in replacement for the raw master:
    all rows preserved in input order, with only filter-matching rows
    enriched. Out-of-filter rows pass through verbatim."""
    pm, troot = _setup_fixture(tmp_path)
    out = tmp_path / "out"
    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
    ])
    rows = _read_jsonl_rows(out / "packets_master_enriched.jsonl")
    # Both rows preserved, in the original order.
    assert len(rows) == 2
    assert [r["packet_id"] for r in rows] == ["pcs-test-A1", "pcs-test-B1"]
    # The A row got enriched.
    assert "involved_officers" in rows[0]
    # The B row passed through unchanged.
    assert rows[1] == PACKETS[1]
    assert "involved_officers" not in rows[1]
    assert "case_outcome" not in rows[1]


def test_cli_grade_filter_includes_all_grades_when_specified(tmp_path):
    """When --grade A --grade B is passed, both grades are enriched.
    Output still contains all rows in original order."""
    pm, troot = _setup_fixture(tmp_path)
    out = tmp_path / "out"
    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
        "--grade", "A",
        "--grade", "B",
    ])
    rows = _read_jsonl_rows(out / "packets_master_enriched.jsonl")
    assert len(rows) == 2
    assert [r["packet_id"] for r in rows] == ["pcs-test-A1", "pcs-test-B1"]


def test_cli_enriched_packet_carries_new_fields(tmp_path):
    pm, troot = _setup_fixture(tmp_path)
    out = tmp_path / "out"
    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
    ])
    rows = _read_jsonl_rows(out / "packets_master_enriched.jsonl")
    enriched = next(r for r in rows if r["packet_id"] == "pcs-test-A1")
    assert any(
        o["name"] == "Officer Terrence Sutton"
        for o in enriched.get("involved_officers", [])
    )
    rels = enriched.get("family_decedent", {}).get("primary_relations", [])
    assert any(r["name"] == "Jane Doe" and r["relationship"] == "sister" for r in rels)
    co = enriched.get("case_outcome", {})
    assert "doj_url" in co
    assert "convicted" in co.get("conviction_status", "")
    # Civil suit cue from transcript.
    assert "civil_suit_status" in co


def test_cli_enriched_master_byte_stable_input_file(tmp_path):
    """A second guard for raw-master immutability: hash the input file
    before and after the run and assert they match. The CLI itself
    raises sys.exit(3) if they differ; this test asserts the
    happy path leaves the file untouched."""
    import hashlib
    pm, troot = _setup_fixture(tmp_path)
    out = tmp_path / "out"
    h_before = hashlib.sha256(pm.read_bytes()).hexdigest()
    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
    ])
    h_after = hashlib.sha256(pm.read_bytes()).hexdigest()
    assert h_before == h_after


def test_cli_atomic_write_leaves_no_tmp_files_on_success(tmp_path):
    """The atomic-write helper uses ``<file>.tmp`` siblings during
    write. After a successful run those temp files should NOT remain
    in the output directory — the rename swaps them into place."""
    pm, troot = _setup_fixture(tmp_path)
    out = tmp_path / "out"
    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
    ])
    leftover = list(out.glob("*.tmp"))
    assert leftover == []


def test_cli_provenance_round_trip(tmp_path):
    pm, troot = _setup_fixture(tmp_path)
    out = tmp_path / "out"
    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
    ])
    prov = json.loads(
        (out / "PACKET_ENRICHMENT_PROVENANCE.json").read_text(encoding="utf-8")
    )
    assert "packets" in prov
    assert len(prov["packets"]) == 1
    p = prov["packets"][0]
    assert p["packet_id"] == "pcs-test-A1"
    assert p["transcripts_loaded"] == 1
    assert "involved_officers" in p["fields"]
    assert "family_decedent" in p["fields"]
    assert "case_outcome" in p["fields"]
    # Provenance carries the resolved transcript paths.
    assert any("abc123" in tp for tp in p["transcript_paths_resolved"])


def test_cli_dry_run_writes_nothing(tmp_path, capsys):
    pm, troot = _setup_fixture(tmp_path)
    out = tmp_path / "out"
    rc = cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
        "--dry-run",
    ])
    assert rc == 0
    assert not out.exists()
    captured = capsys.readouterr()
    # The Markdown report header is printed in dry-run mode.
    assert "Packet enrichment — extraction validation" in captured.out


def test_cli_missing_input_returns_error(tmp_path, capsys):
    rc = cli_main([
        "--packets-master", str(tmp_path / "no-such-file.jsonl"),
    ])
    assert rc == 2


def test_cli_main_module_invocation(tmp_path):
    """Direct invocation via patched argv (mirrors __main__ entry)."""
    pm, troot = _setup_fixture(tmp_path)
    out = tmp_path / "out"
    argv = [
        "tools.enrich_packets_from_transcripts",
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
    ]
    with patch.object(sys, "argv", argv):
        # cli_main parses argv directly via argparse if argv passed; the
        # __main__ block calls main() with no args, which uses sys.argv.
        # We simulate that path here.
        rc = cli_main()
    assert rc == 0
    assert (out / "packets_master_enriched.jsonl").exists()
