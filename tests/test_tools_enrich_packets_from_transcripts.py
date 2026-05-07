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


def test_cli_default_filter_is_grade_A(tmp_path):
    pm, troot = _setup_fixture(tmp_path)
    out = tmp_path / "out"
    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
    ])
    enriched_lines = (out / "packets_master_enriched.jsonl").read_text(
        encoding="utf-8"
    ).strip().splitlines()
    assert len(enriched_lines) == 1
    enriched = json.loads(enriched_lines[0])
    assert enriched["packet_id"] == "pcs-test-A1"


def test_cli_grade_filter_can_include_B(tmp_path):
    pm, troot = _setup_fixture(tmp_path)
    out = tmp_path / "out"
    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
        "--grade", "A",
        "--grade", "B",
    ])
    enriched_lines = (out / "packets_master_enriched.jsonl").read_text(
        encoding="utf-8"
    ).strip().splitlines()
    assert len(enriched_lines) == 2


def test_cli_enriched_packet_carries_new_fields(tmp_path):
    pm, troot = _setup_fixture(tmp_path)
    out = tmp_path / "out"
    cli_main([
        "--packets-master", str(pm),
        "--transcript-root", str(troot),
        "--output-dir", str(out),
    ])
    enriched = json.loads(
        (out / "packets_master_enriched.jsonl").read_text(encoding="utf-8").strip()
    )
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
