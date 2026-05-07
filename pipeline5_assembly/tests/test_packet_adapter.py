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


# ---------------------------------------------------------------------------
# Optional structured fields — additive packet schema (P1/P5 schema migration)
# ---------------------------------------------------------------------------
#
# These tests cover the optional fields that brief generation reads when
# present and falls back to placeholder/gap-note behavior when absent:
#   involved_officers[], family_decedent, timeline_events[],
#   narrative_spine, production_angles[], case_outcome.
#
# The existing tests above already cover the absent-field fallback paths;
# these tests cover the present-field render paths and a mixed-field
# graceful-fallback case.


ENRICHED_PACKET = {
    **SAMPLE_PACKET,
    "involved_officers": [
        {
            "name": "Officer Test One",
            "role": "involved officer",
            "status": "convicted",
            "agency": "Test PD",
            "badge": "1234",
        },
        {
            "name": "Sergeant Test Two",
            "role": "supervisor",
            "status": "cleared",
            "agency": "Test PD",
        },
    ],
    "family_decedent": {
        "primary_relations": [
            {"name": "Jane Doe", "relationship": "mother"},
            {"name": "John Doe", "relationship": "brother"},
        ],
        "emotional_anchors": [
            "Decedent had a 3-month-old daughter.",
        ],
    },
    "timeline_events": [
        {"date": "2024-01-15", "event": "Fatal pursuit ends in bystander death.", "source": "packet"},
        {"date": "2024-03-02", "event": "Civil suit filed by family.", "source": "court records"},
    ],
    "narrative_spine": {
        "setup": "Test PD initiated a high-speed pursuit over a minor traffic infraction.",
        "pursuit": "Officers continued the chase against department policy through downtown.",
        "death": "Suspect's vehicle struck a bystander at First and Main.",
        "investigation": "Internal affairs and the DA opened parallel reviews.",
        "outcome": "Officer One convicted; Officer Two cleared.",
    },
    "production_angles": [
        {
            "rank": 1,
            "title": "The chase that became a federal accountability case",
            "recommended": True,
            "risks": ["fatal incident", "family sensitivity"],
        },
        {
            "rank": 2,
            "title": "Bystander-rights angle: when does pursuit policy fail?",
            "recommended": False,
            "risks": [],
        },
    ],
    "case_outcome": {
        "conviction_status": "officer convicted",
        "sentence": "5 years federal",
        "doj_url": "https://www.justice.gov/test/pr-doj",
        "civil_suit_status": "settled (sealed)",
        "court": "USDC for the District of Test",
    },
}


# ----- involved_officers[] -----


def test_build_packet_brief_populates_officers_when_present():
    brief = build_packet_brief(ENRICHED_PACKET, transcripts=[])
    kp = brief["key_people"]
    names = [o["name"] for o in kp["officers_named"]]
    assert "Officer Test One" in names
    assert "Sergeant Test Two" in names
    # When officers are present, the gap note must clear so the
    # renderer does not emit the "schema does not carry" sentence.
    assert kp["structured_field_missing_note"] is None


def test_render_packet_markdown_renders_officers_in_section_4():
    brief = build_packet_brief(ENRICHED_PACKET, transcripts=[])
    md = render_packet_markdown(brief)
    # Section header + each officer name + at least one role/status/agency hint.
    assert "## 4. Key people" in md
    assert "Involved officers" in md
    assert "Officer Test One" in md
    assert "Sergeant Test Two" in md
    assert "convicted" in md
    assert "Test PD" in md
    # Gap note must NOT appear when officers are populated.
    assert "Packet schema does not carry involved_officers" not in md


def test_build_packet_brief_accepts_string_officer_entries():
    """A bare-string entry should normalize into a name-only officer dict."""
    pkt = {**SAMPLE_PACKET, "involved_officers": ["Officer Bare String"]}
    brief = build_packet_brief(pkt, transcripts=[])
    officers = brief["key_people"]["officers_named"]
    assert officers == [{"name": "Officer Bare String", "role": None, "status": None, "agency": None, "badge": None}]


# ----- family_decedent -----


def test_build_packet_brief_populates_family_when_present():
    brief = build_packet_brief(ENRICHED_PACKET, transcripts=[])
    kp = brief["key_people"]
    rels = [(f["name"], f["relationship"]) for f in kp["family_named"]]
    assert ("Jane Doe", "mother") in rels
    assert ("John Doe", "brother") in rels
    assert kp["emotional_anchors"] == ["Decedent had a 3-month-old daughter."]


def test_render_packet_markdown_renders_family_and_anchors():
    brief = build_packet_brief(ENRICHED_PACKET, transcripts=[])
    md = render_packet_markdown(brief)
    assert "Family / decedent" in md
    assert "Jane Doe" in md
    assert "mother" in md
    assert "Emotional anchors" in md
    assert "3-month-old daughter" in md


# ----- timeline_events[] -----


def test_build_packet_brief_uses_timeline_events_when_present():
    brief = build_packet_brief(ENRICHED_PACKET, transcripts=[])
    timeline = brief["timeline"]
    # All timeline_events[] entries pass through; the synthesized
    # incident-only fallback event is suppressed when timeline_events
    # is non-empty.
    assert len(timeline) == 2
    assert timeline[0]["date"] == "2024-01-15"
    assert timeline[0]["source"] == "packet"
    assert timeline[1]["date"] == "2024-03-02"
    assert "Civil suit" in timeline[1]["event"]


def test_render_packet_markdown_renders_timeline_events_with_source():
    brief = build_packet_brief(ENRICHED_PACKET, transcripts=[])
    md = render_packet_markdown(brief)
    assert "2024-01-15" in md
    assert "Fatal pursuit ends in bystander death" in md
    assert "Civil suit filed" in md
    # Source is rendered inline.
    assert "court records" in md


def test_build_packet_brief_skips_timeline_events_with_no_date_or_event():
    pkt = {**SAMPLE_PACKET, "timeline_events": [{"date": "", "event": ""}, {"foo": "bar"}]}
    brief = build_packet_brief(pkt, transcripts=[])
    # All entries dropped → falls back to incident_date synthesized event.
    assert len(brief["timeline"]) == 1
    assert brief["timeline"][0]["date"] == SAMPLE_PACKET["incident_date"]


# ----- narrative_spine -----


def test_build_packet_brief_uses_narrative_spine_when_present():
    brief = build_packet_brief(ENRICHED_PACKET, transcripts=[])
    ns = brief["narrative_spine"]
    assert "high-speed pursuit" in ns["setup"]
    assert "department policy" in ns["pursuit"]
    assert "First and Main" in ns["death"]
    assert "internal affairs" in ns["investigation"].lower()
    assert "convicted" in ns["outcome"]
    # Legacy placeholder strings must NOT leak into an enriched spine.
    assert "(packet mode" not in ns.get("setup", "")


def test_render_packet_markdown_renders_narrative_spine_keys_in_order():
    brief = build_packet_brief(ENRICHED_PACKET, transcripts=[])
    md = render_packet_markdown(brief)
    # Each new-shape key surfaces as a bullet header.
    assert "**Setup**" in md
    assert "**Pursuit**" in md
    assert "**Death**" in md
    assert "**Investigation**" in md
    assert "**Outcome**" in md
    # Stable order: setup before pursuit before death.
    setup_pos = md.index("**Setup**")
    pursuit_pos = md.index("**Pursuit**")
    death_pos = md.index("**Death**")
    investigation_pos = md.index("**Investigation**")
    outcome_pos = md.index("**Outcome**")
    assert setup_pos < pursuit_pos < death_pos < investigation_pos < outcome_pos


# ----- production_angles[] -----


def test_build_packet_brief_uses_production_angles_when_present():
    brief = build_packet_brief(ENRICHED_PACKET, transcripts=[])
    angles = brief["production_angles"]
    assert len(angles) == 2
    titles = [a["title"] for a in angles]
    assert "The chase that became a federal accountability case" in titles
    assert angles[0]["rank"] == 1
    assert angles[0]["recommended"] is True
    assert "fatal incident" in angles[0]["risks"]
    # Generic placeholder must NOT appear when packet-supplied angles exist.
    assert all("packet mode" not in t for t in titles)


def test_render_packet_markdown_renders_angles_with_risks():
    brief = build_packet_brief(ENRICHED_PACKET, transcripts=[])
    md = render_packet_markdown(brief)
    assert "## 10. Production angle" in md
    assert "The chase that became a federal accountability case" in md
    assert "Bystander-rights angle" in md
    assert "Risk: fatal incident" in md
    assert "Risk: family sensitivity" in md


def test_build_packet_brief_orders_angles_by_rank():
    """Out-of-order ranks in the input should be sorted ascending."""
    pkt = {
        **SAMPLE_PACKET,
        "production_angles": [
            {"rank": 3, "title": "Third"},
            {"rank": 1, "title": "First"},
            {"rank": 2, "title": "Second"},
        ],
    }
    brief = build_packet_brief(pkt, transcripts=[])
    titles = [a["title"] for a in brief["production_angles"]]
    assert titles == ["First", "Second", "Third"]


# ----- case_outcome -----


def test_build_packet_brief_carries_case_outcome_top_level_and_in_why_it_matters():
    brief = build_packet_brief(ENRICHED_PACKET, transcripts=[])
    co = brief["case_outcome"]
    assert co["conviction_status"] == "officer convicted"
    assert co["sentence"] == "5 years federal"
    assert co["doj_url"].startswith("https://")
    assert co["court"] == "USDC for the District of Test"
    # Why-it-matters carries the same dict so section 8 can render it.
    assert brief["why_it_matters"]["case_outcome"] == co


def test_render_packet_markdown_renders_case_outcome_in_section_2_and_section_8():
    brief = build_packet_brief(ENRICHED_PACKET, transcripts=[])
    md = render_packet_markdown(brief)
    # Appears in section 2 and section 8.
    s2_idx = md.index("## 2. Case summary")
    s3_idx = md.index("## 3. Narrative spine")
    s8_idx = md.index("## 8. Why this case matters")
    s9_idx = md.index("## 9. Missing fields")
    assert "officer convicted" in md[s2_idx:s3_idx]
    assert "5 years federal" in md[s2_idx:s3_idx]
    assert "officer convicted" in md[s8_idx:s9_idx]
    assert "5 years federal" in md[s8_idx:s9_idx]
    assert "USDC for the District of Test" in md


def test_build_packet_brief_drops_empty_case_outcome_fields():
    pkt = {**SAMPLE_PACKET, "case_outcome": {"conviction_status": "", "sentence": None, "court": "X"}}
    brief = build_packet_brief(pkt, transcripts=[])
    assert brief["case_outcome"] == {"court": "X"}


# ----- mixed: some fields populated, some absent -----


def test_build_packet_brief_mixed_fields_falls_back_per_field():
    """A packet with only some new fields should populate those sections
    while remaining sections fall back to the prior placeholder behavior."""
    mixed = {
        **SAMPLE_PACKET,
        "involved_officers": [{"name": "Officer Mixed", "role": "involved officer"}],
        # No family_decedent, no timeline_events, no narrative_spine,
        # no production_angles, no case_outcome.
    }
    brief = build_packet_brief(mixed, transcripts=[])
    # Officers populated.
    assert any(o["name"] == "Officer Mixed" for o in brief["key_people"]["officers_named"])
    # Family unset → no family_named, but officers_named is non-empty so the
    # gap note is suppressed (officers alone count as populated).
    assert brief["key_people"]["family_named"] == []
    assert brief["key_people"]["structured_field_missing_note"] is None
    # Timeline falls back to synthesized incident-only event.
    assert len(brief["timeline"]) == 1
    assert brief["timeline"][0]["date"] == SAMPLE_PACKET["incident_date"]
    # Narrative spine falls back to legacy placeholders.
    assert "(packet mode" in brief["narrative_spine"]["setup"]
    # Production angles falls back to the generic single-angle placeholder.
    assert len(brief["production_angles"]) == 1
    assert "packet mode" in brief["production_angles"][0]["title"]
    # case_outcome empty.
    assert brief["case_outcome"] == {}


def test_render_packet_markdown_mixed_fields_does_not_leak_legacy_into_populated_sections():
    """Where new fields are populated, the rendered markdown must not
    contain the legacy gap-note / placeholder strings for those sections."""
    mixed = {
        **SAMPLE_PACKET,
        "involved_officers": [{"name": "Officer Mixed"}],
        "narrative_spine": {"setup": "Real setup line."},
    }
    brief = build_packet_brief(mixed, transcripts=[])
    md = render_packet_markdown(brief)
    # Section 4: officer populated → gap note absent.
    assert "Packet schema does not carry involved_officers" not in md
    assert "Officer Mixed" in md
    # Section 3: narrative_spine present → legacy placeholder absent.
    assert "(packet mode: setup not synthesized" not in md
    assert "Real setup line." in md


# ----- JSON sidecar preserves new fields -----


def test_assemble_packet_json_sidecar_carries_new_fields(tmp_path):
    """The JSON sidecar must round-trip the new optional fields so downstream
    tools and audits can inspect them without re-reading the packet."""
    pkt = _write_packet(tmp_path, ENRICHED_PACKET, name="enriched.json")
    out_dir = tmp_path / "out"
    brief = assemble_packet(
        packet_path=pkt,
        transcript_dir=None,
        dry_run=False,
        output_dir=out_dir,
    )
    assert brief is not None
    safe_id = "pcs-test-001"
    sidecar_path = out_dir / f"{safe_id}_packet_brief.json"
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    # The new fields appear in the sidecar verbatim where the brief
    # carries them.
    assert sidecar["case_outcome"]["conviction_status"] == "officer convicted"
    assert any(
        o["name"] == "Officer Test One" for o in sidecar["key_people"]["officers_named"]
    )
    assert any(
        f["name"] == "Jane Doe" for f in sidecar["key_people"]["family_named"]
    )
    assert sidecar["timeline"][0]["source"] == "packet"
    assert sidecar["narrative_spine"]["pursuit"].startswith("Officers")
    assert sidecar["production_angles"][0]["risks"] == ["fatal incident", "family sensitivity"]


def test_render_packet_markdown_minimal_packet_keeps_gap_note():
    """Regression guard: a packet with no new optional fields must still
    surface the original 'schema does not carry' gap note in section 4."""
    minimal = {"packet_id": "pcs-min-002", "candidate_id": "x:1"}
    brief = build_packet_brief(minimal, transcripts=[])
    md = render_packet_markdown(brief)
    assert "Packet schema does not carry involved_officers" in md
    assert "(packet mode: setup not synthesized" in md
    assert "(packet mode: angle not synthesized" in md
