"""Tests for the packet-mode adapter in pipeline5_assemble.

Covers:

1. ``_load_packet_transcripts`` — reads ``.txt`` files recursively,
   skips empty files, returns sorted-path order, preserves provenance.

2. ``_chunk_packet_paragraphs`` — deterministic chunking; drops VTT
   header lines (``Kind:`` / ``Language:``); discards short noise.

3. ``build_packet_brief`` — produces the 11-section dict from a
   packet stub, populates evidence chunks from transcripts, and
   passes packet ``next_search_tasks`` through to research_gaps.

4. ``render_packet_markdown`` — emits all 11 sections; includes
   packet_id / candidate_id / subject / agency / source URLs /
   transcript excerpts / research gaps in the rendered output.

5. ``assemble_packet`` — orchestrator: loads packet + transcripts,
   handles missing transcript dir gracefully, writes
   ``{safe_id}_packet_brief.{md,json}`` under ``--output``.

6. Existing ``--verdict`` mode regression: ``main()`` still routes
   verdict-mode args correctly when ``--packet`` is absent. (Direct
   integration not exercised here; the existing pipeline5 test suite
   covers verdict-mode contract.)

Zero network. Synthetic packet fixtures (no .tmp/ dependency).
"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

from pipeline5_assemble import (  # noqa: E402
    _chunk_packet_paragraphs,
    _load_packet_transcripts,
    _packet_safe_id,
    assemble_packet,
    build_packet_brief,
    main,
    render_packet_markdown,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


SAMPLE_PACKET = {
    "packet_id": "pcs-test-001",
    "source_lane": "sfchronicle_youtube",
    "candidate_id": "sfchronicle_pursuits:42",
    "subject_or_case": "Test Subject — Test PD pursuit, 2024-01-15",
    "agency": "Test Police Department",
    "jurisdiction": "Testville, Test County, TS",
    "incident_type": "fatal pursuit / bystander killed",
    "incident_date": "2024-01-15",
    "source_urls": [
        "https://example.com/news/test-case",
        "https://www.youtube.com/watch?v=abc123",
        "https://www.justice.gov/test/pr",
    ],
    "transcript_paths": [],
    "matched_terms": ["full_subject_name", "agency_token:test"],
    "artifact_indicators": {"bwc": True, "pursuit": True, "court_outcome": False},
    "confidence_grade": "A",
    "confidence_reason": "Subject + agency + city anchored; multi-source.",
    "missing_fields": ["bwc_url", "court_outcome"],
    "next_search_tasks": [
        "MuckRock: 'test police department incident report 2024-01-15'",
        "Court: county court for civil suit",
    ],
}


SAMPLE_TRANSCRIPT_TEXT = """Kind: captions
Language: en

This is the first paragraph of the cached caption text. It contains
substantive content about the incident which is long enough to clear
the noise filter for paragraph chunks in the adapter.

Officer Test pursued the suspect through downtown Testville at high
speeds. The suspect's vehicle crashed into a bystander's car at the
intersection of First and Main, killing the bystander instantly.

Family attorneys have filed a wrongful-death suit against the
department. The Test PD has not yet released bodycam footage despite
public demands for transparency.
"""


def _write_packet(tmp_path: Path, packet: dict, name: str = "packet.json") -> Path:
    """Write a packet JSON to tmp_path and return its path."""
    p = tmp_path / name
    p.write_text(json.dumps(packet), encoding="utf-8")
    return p


def _write_transcript(tmp_path: Path, name: str, text: str = SAMPLE_TRANSCRIPT_TEXT) -> Path:
    """Write a caption .txt to tmp_path/transcripts/<video_id>/<name>."""
    sub = tmp_path / "transcripts" / "video_abc123"
    sub.mkdir(parents=True, exist_ok=True)
    p = sub / name
    p.write_text(text, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# _load_packet_transcripts
# ---------------------------------------------------------------------------


def test_load_packet_transcripts_returns_text_and_paths(tmp_path):
    _write_transcript(tmp_path, "vid.txt")
    out = _load_packet_transcripts(tmp_path / "transcripts")
    assert len(out) == 1
    assert out[0]["text"].startswith("Kind: captions")
    assert out[0]["basename"] == "vid.txt"
    assert out[0]["parent_dirname"] == "video_abc123"


def test_load_packet_transcripts_recursive(tmp_path):
    _write_transcript(tmp_path, "a.txt")
    sub2 = tmp_path / "transcripts" / "video_def456"
    sub2.mkdir(parents=True, exist_ok=True)
    (sub2 / "b.txt").write_text(SAMPLE_TRANSCRIPT_TEXT, encoding="utf-8")
    out = _load_packet_transcripts(tmp_path / "transcripts")
    assert len(out) == 2
    # sorted by path
    assert out[0]["parent_dirname"] == "video_abc123"
    assert out[1]["parent_dirname"] == "video_def456"


def test_load_packet_transcripts_skips_empty_files(tmp_path):
    _write_transcript(tmp_path, "good.txt")
    sub = tmp_path / "transcripts" / "video_abc123"
    (sub / "empty.txt").write_text("", encoding="utf-8")
    out = _load_packet_transcripts(tmp_path / "transcripts")
    assert len(out) == 1
    assert out[0]["basename"] == "good.txt"


def test_load_packet_transcripts_missing_dir_returns_empty(tmp_path):
    out = _load_packet_transcripts(tmp_path / "nonexistent")
    assert out == []


# ---------------------------------------------------------------------------
# _chunk_packet_paragraphs
# ---------------------------------------------------------------------------


def test_chunk_paragraphs_handles_blank_separated(tmp_path):
    text = "para one with enough characters to clear the noise filter line one\nline two\n\npara two with enough characters to clear the noise filter line one\nline two"
    chunks = _chunk_packet_paragraphs(text)
    assert len(chunks) >= 1
    assert all(len(c) >= 40 for c in chunks)


def test_chunk_paragraphs_drops_vtt_headers():
    text = "Kind: captions\nLanguage: en\n\nthis is a real paragraph with enough characters to clear the noise filter\nline two of the same paragraph\n"
    chunks = _chunk_packet_paragraphs(text)
    # The Kind: / Language: lines should NOT appear in any chunk
    joined = " ".join(chunks)
    assert "Kind:" not in joined
    assert "Language:" not in joined


def test_chunk_paragraphs_respects_max_chunks():
    para = " ".join(["x" * 8] * 10)  # ~80 chars per "paragraph"
    text = "\n\n".join([para] * 20)
    chunks = _chunk_packet_paragraphs(text, max_chunks=3)
    assert len(chunks) == 3


def test_chunk_paragraphs_drops_short_noise():
    # Chunks under _PACKET_MIN_PARAGRAPH_CHARS are filtered.
    text = "tiny\n\nsmall words\n\n"
    chunks = _chunk_packet_paragraphs(text)
    assert chunks == []


# ---------------------------------------------------------------------------
# _packet_safe_id
# ---------------------------------------------------------------------------


def test_packet_safe_id_replaces_colons_and_slashes():
    p = {"packet_id": "pcs-foo:bar/baz"}
    assert _packet_safe_id(p) == "pcs-foo_bar_baz"


def test_packet_safe_id_falls_back_to_candidate_id():
    p = {"candidate_id": "sfchronicle_pursuits:99"}
    assert _packet_safe_id(p) == "sfchronicle_pursuits_99"


def test_packet_safe_id_falls_back_to_default():
    p = {}
    assert _packet_safe_id(p) == "packet"


# ---------------------------------------------------------------------------
# build_packet_brief
# ---------------------------------------------------------------------------


def test_build_packet_brief_uses_packet_fields_for_identity():
    brief = build_packet_brief(SAMPLE_PACKET, transcripts=[])
    ci = brief["case_identity"]
    assert ci["subject_or_case"] == "Test Subject — Test PD pursuit, 2024-01-15"
    assert ci["agency"] == "Test Police Department"
    assert ci["jurisdiction"] == "Testville, Test County, TS"
    assert ci["incident_type"] == "fatal pursuit / bystander killed"
    assert ci["incident_date"] == "2024-01-15"
    assert ci["confidence_grade"] == "A"


def test_build_packet_brief_splits_youtube_vs_other_urls():
    brief = build_packet_brief(SAMPLE_PACKET, transcripts=[])
    ev = brief["evidence_artifacts"]
    assert "https://www.youtube.com/watch?v=abc123" in ev["youtube_urls"]
    assert "https://www.justice.gov/test/pr" in ev["other_source_urls"]
    assert "https://example.com/news/test-case" in ev["other_source_urls"]


def test_build_packet_brief_passes_research_gaps_through():
    brief = build_packet_brief(SAMPLE_PACKET, transcripts=[])
    rg = brief["research_gaps"]
    assert "bwc_url" in rg["missing_fields"]
    assert any("MuckRock" in t for t in rg["next_search_tasks"])
    # next_research_tasks at top level mirrors the next_search_tasks
    assert brief["next_research_tasks"] == SAMPLE_PACKET["next_search_tasks"]


def test_build_packet_brief_emits_transcript_evidence_chunks():
    transcripts = [{
        "path": "/test/video_abc123/abc123.txt",
        "text": SAMPLE_TRANSCRIPT_TEXT,
        "basename": "abc123.txt",
        "parent_dirname": "video_abc123",
    }]
    brief = build_packet_brief(SAMPLE_PACKET, transcripts=transcripts)
    te = brief["transcript_evidence"]
    assert len(te) >= 1
    assert all(c["source_basename"] == "abc123.txt" for c in te)
    assert all(c["speaker"] == "unknown" for c in te)
    assert all(c["kind"] == "cached_caption_text" for c in te)
    # Body text from the synthetic transcript should appear in at
    # least one chunk
    joined = " ".join(c["excerpt"] for c in te)
    assert "Officer Test" in joined or "Testville" in joined


def test_build_packet_brief_handles_missing_optional_fields():
    minimal = {"packet_id": "pcs-min-001", "candidate_id": "x:1"}
    brief = build_packet_brief(minimal, transcripts=[])
    ci = brief["case_identity"]
    # Missing fields render as None — caller-side renderer turns
    # them into "(missing)" strings.
    assert ci["subject_or_case"] is None
    assert ci["agency"] is None
    assert brief["transcript_evidence"] == []
    assert brief["research_gaps"]["missing_fields"] == []
    # No timeline event without an incident_date.
    assert brief["timeline"] == []


def test_build_packet_brief_marks_brief_kind_packet_mode():
    brief = build_packet_brief(SAMPLE_PACKET, transcripts=[])
    assert brief["brief_kind"] == "packet_mode"
    assert brief["brief_id"].startswith("pcs-test-001")


# ---------------------------------------------------------------------------
# render_packet_markdown
# ---------------------------------------------------------------------------


_REQUIRED_RENDER_ANCHORS = [
    "## 1. Case identity",
    "## 2. Case summary",
    "## 3. Narrative spine",
    "## 4. Key people",
    "## 5. Timeline",
    "## 6. Evidence and artifacts",
    "## 7. Transcript / source evidence",
    "## 8. Why this case matters",
    "## 9. Missing fields / research gaps",
    "## 10. Production angle",
    "## 11. Next research tasks",
]


def test_render_packet_markdown_includes_all_eleven_sections():
    brief = build_packet_brief(SAMPLE_PACKET, transcripts=[])
    md = render_packet_markdown(brief)
    for anchor in _REQUIRED_RENDER_ANCHORS:
        assert anchor in md, f"missing section: {anchor}"


def test_render_packet_markdown_includes_packet_identity():
    brief = build_packet_brief(SAMPLE_PACKET, transcripts=[])
    md = render_packet_markdown(brief)
    assert "pcs-test-001" in md
    assert "sfchronicle_pursuits:42" in md
    assert "Test Police Department" in md
    assert "Testville, Test County, TS" in md
    assert "2024-01-15" in md


def test_render_packet_markdown_includes_source_urls():
    brief = build_packet_brief(SAMPLE_PACKET, transcripts=[])
    md = render_packet_markdown(brief)
    assert "https://www.youtube.com/watch?v=abc123" in md
    assert "https://www.justice.gov/test/pr" in md
    assert "https://example.com/news/test-case" in md


def test_render_packet_markdown_includes_transcript_evidence():
    transcripts = [{
        "path": "/test/abc.txt",
        "text": SAMPLE_TRANSCRIPT_TEXT,
        "basename": "abc.txt",
        "parent_dirname": "vid",
    }]
    brief = build_packet_brief(SAMPLE_PACKET, transcripts=transcripts)
    md = render_packet_markdown(brief)
    assert "abc.txt" in md
    # At least one transcript excerpt should appear as a blockquote.
    assert "> " in md


def test_render_packet_markdown_includes_research_gaps():
    brief = build_packet_brief(SAMPLE_PACKET, transcripts=[])
    md = render_packet_markdown(brief)
    assert "MuckRock" in md
    assert "bwc_url" in md


def test_render_packet_markdown_handles_no_transcripts_gracefully():
    brief = build_packet_brief(SAMPLE_PACKET, transcripts=[])
    md = render_packet_markdown(brief)
    assert "no captioned transcripts available" in md


def test_render_packet_markdown_renders_artifact_indicators():
    brief = build_packet_brief(SAMPLE_PACKET, transcripts=[])
    md = render_packet_markdown(brief)
    assert "bwc:" in md.lower() or "BWC" in md or "bwc" in md
    assert "pursuit" in md.lower()


# ---------------------------------------------------------------------------
# assemble_packet
# ---------------------------------------------------------------------------


def test_assemble_packet_writes_md_and_json(tmp_path):
    pkt = _write_packet(tmp_path, SAMPLE_PACKET)
    _write_transcript(tmp_path, "vid.txt")
    out_dir = tmp_path / "out"

    brief = assemble_packet(
        packet_path=pkt,
        transcript_dir=tmp_path / "transcripts",
        dry_run=False,
        output_dir=out_dir,
    )
    assert brief is not None
    safe_id = "pcs-test-001"
    assert (out_dir / f"{safe_id}_packet_brief.md").exists()
    assert (out_dir / f"{safe_id}_packet_brief.json").exists()


def test_assemble_packet_dry_run_does_not_write(tmp_path, capsys):
    pkt = _write_packet(tmp_path, SAMPLE_PACKET)
    out_dir = tmp_path / "out"

    brief = assemble_packet(
        packet_path=pkt,
        transcript_dir=None,
        dry_run=True,
        output_dir=out_dir,
    )
    assert brief is not None
    assert not out_dir.exists()
    captured = capsys.readouterr()
    assert "DRY RUN" in captured.out
    assert "## 1. Case identity" in captured.out


def test_assemble_packet_missing_transcripts_warns_but_succeeds(tmp_path, capsys):
    pkt = _write_packet(tmp_path, SAMPLE_PACKET)
    out_dir = tmp_path / "out"

    brief = assemble_packet(
        packet_path=pkt,
        transcript_dir=tmp_path / "no_such_dir",
        dry_run=False,
        output_dir=out_dir,
    )
    assert brief is not None
    captured = capsys.readouterr()
    assert "WARN" in captured.out
    assert (out_dir / "pcs-test-001_packet_brief.md").exists()


def test_assemble_packet_missing_packet_returns_none(tmp_path, capsys):
    out_dir = tmp_path / "out"
    result = assemble_packet(
        packet_path=tmp_path / "no_packet.json",
        transcript_dir=None,
        dry_run=False,
        output_dir=out_dir,
    )
    assert result is None


def test_assemble_packet_minimal_packet_does_not_crash(tmp_path):
    """Packet with only packet_id / candidate_id should still produce a
    brief — every other field is optional with a fallback."""
    minimal = {"packet_id": "pcs-min-001", "candidate_id": "x:1"}
    pkt = _write_packet(tmp_path, minimal)
    out_dir = tmp_path / "out"
    brief = assemble_packet(
        packet_path=pkt,
        transcript_dir=None,
        dry_run=False,
        output_dir=out_dir,
    )
    assert brief is not None
    md = (out_dir / "pcs-min-001_packet_brief.md").read_text(encoding="utf-8")
    # All 11 sections still present, even with missing data.
    for anchor in _REQUIRED_RENDER_ANCHORS:
        assert anchor in md
    # And the "(missing)" fallback string appears at least once.
    assert "(missing)" in md


# ---------------------------------------------------------------------------
# main() CLI integration
# ---------------------------------------------------------------------------


def test_main_packet_mode_routes_to_assemble_packet(tmp_path, capsys):
    pkt = _write_packet(tmp_path, SAMPLE_PACKET)
    _write_transcript(tmp_path, "vid.txt")
    out_dir = tmp_path / "out"

    argv = [
        "pipeline5_assemble.py",
        "--packet", str(pkt),
        "--transcript-dir", str(tmp_path / "transcripts"),
        "--output", str(out_dir),
    ]
    with patch.object(sys, "argv", argv):
        main()

    captured = capsys.readouterr()
    assert "Assembling 1 packet brief" in captured.out
    assert "Built: 1" in captured.out
    assert (out_dir / "pcs-test-001_packet_brief.md").exists()


def test_main_packet_mode_dry_run(tmp_path, capsys):
    pkt = _write_packet(tmp_path, SAMPLE_PACKET)
    out_dir = tmp_path / "out"

    argv = [
        "pipeline5_assemble.py",
        "--packet", str(pkt),
        "--output", str(out_dir),
        "--dry-run",
    ]
    with patch.object(sys, "argv", argv):
        main()

    captured = capsys.readouterr()
    assert "[DRY RUN]" in captured.out
    assert "Built: 1" in captured.out
    assert not out_dir.exists()


def test_main_rejects_packet_with_verdict(tmp_path):
    """The mutually-exclusive group should reject --packet + --verdict."""
    pkt = _write_packet(tmp_path, SAMPLE_PACKET)
    argv = [
        "pipeline5_assemble.py",
        "--packet", str(pkt),
        "--verdict", "/some/verdict.json",
    ]
    with patch.object(sys, "argv", argv):
        with pytest.raises(SystemExit) as excinfo:
            main()
        # argparse exits with code 2 on mutually-exclusive violations
        assert excinfo.value.code == 2


def test_main_requires_at_least_one_source(capsys):
    """Without --verdict, --verdict-dir, or --packet, argparse exits."""
    argv = ["pipeline5_assemble.py"]
    with patch.object(sys, "argv", argv):
        with pytest.raises(SystemExit) as excinfo:
            main()
        assert excinfo.value.code == 2
