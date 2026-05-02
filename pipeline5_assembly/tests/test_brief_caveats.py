"""Tests for the Production caveats advisory layer in
pipeline5_assemble.

Covers two pieces:

1. ``_assemble_production_caveats`` -- the helper that pulls advisory
   fields off the P4 verdict + metadata. Defensive-by-default so old
   verdict files (predating the Batch 2 advisory-flag work in P4)
   still produce a valid empty caveats dict.

2. ``render_markdown`` -- the Production caveats section renders only
   when at least one caveat fires; absent / null caveats omit the
   section entirely.

Doctrine: caveats are advisory. They MUST NOT modify or override the
verdict. These tests pin that contract.
"""
from __future__ import annotations

import sys
from pathlib import Path

# pipeline5_assemble.py lives one directory up from this test file.
# Add it to sys.path so the module imports cleanly under pytest.
PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

from pipeline5_assemble import (  # noqa: E402
    _assemble_production_caveats,
    build_brief,
    render_markdown,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _verdict_with(**overrides):
    """Return a minimally-valid P4 verdict dict with caveats populated
    via overrides. Top-level overrides go straight on; keys prefixed
    with ``meta_`` go inside _pipeline4_metadata."""
    v = {
        "case_id": "test_001",
        "verdict": "PRODUCE",
        "narrative_score": 70.0,
        "confidence": 0.7,
        "key_moments": [],
        "content_pitch": "test pitch",
        "narrative_arc_recommendation": "cold_open",
        "estimated_runtime_min": 20.0,
        "artifact_completeness": {"available": [], "missing_recommended": []},
        "scoring_breakdown": {},
        "_pipeline4_metadata": {},
    }
    md = v["_pipeline4_metadata"]
    for k, val in overrides.items():
        if k.startswith("meta_"):
            md[k[5:]] = val
        else:
            v[k] = val
    return v


def _build(verdict):
    return build_brief(verdict, case_research=None, transcripts=[], weights=None)


# ---------------------------------------------------------------------------
# _assemble_production_caveats unit tests
# ---------------------------------------------------------------------------


def test_caveats_for_confirmed_final_outcome_has_no_caveats():
    """confirmed_final_outcome -> production_status_flag is None,
    nothing else fires -> has_any is False. The Production caveats
    markdown section will be omitted for these cases."""
    v = _verdict_with(
        resolution_status="confirmed_final_outcome",
        production_status_flag=None,
        meta_degraded=False,
        meta_resolution_gate_applied=False,
    )
    c = _assemble_production_caveats(v)
    assert c["has_any"] is False
    assert c["production_status_flag"] is None
    assert c["resolution_status"] == "confirmed_final_outcome"


def test_caveats_for_pending_case_surfaces_flag():
    """The headline doctrine case: a pending winner keeps verdict =
    PRODUCE and surfaces a non-blocking advisory caveat."""
    v = _verdict_with(
        resolution_status="charges_filed_pending",
        production_status_flag="pending_case_review",
    )
    c = _assemble_production_caveats(v)
    assert c["has_any"] is True
    assert c["production_status_flag"] == "pending_case_review"
    assert c["resolution_status"] == "charges_filed_pending"


def test_caveats_for_missing_status_surfaces_resolution_unknown():
    v = _verdict_with(
        resolution_status="missing",
        production_status_flag="resolution_unknown",
    )
    c = _assemble_production_caveats(v)
    assert c["has_any"] is True
    assert c["production_status_flag"] == "resolution_unknown"


def test_caveats_degraded_pass2_surfaces_caveat():
    """Pass 2 fallback (degraded=True) is its own caveat — independent
    of resolution_status. Producers should see this even when the case
    is fully resolved."""
    v = _verdict_with(
        resolution_status="confirmed_final_outcome",
        production_status_flag=None,
        meta_degraded=True,
    )
    c = _assemble_production_caveats(v)
    assert c["has_any"] is True
    assert c["degraded"] is True


def test_caveats_gate_applied_surfaces_caveat():
    """When the optional resolution gate fires (gate ON +
    pre_gate_verdict differed from emitted verdict), surface the cap
    so producers know the verdict was tightened."""
    v = _verdict_with(
        verdict="HOLD",
        resolution_status="charges_filed_pending",
        production_status_flag="pending_case_review",
        meta_resolution_gate_enabled=True,
        meta_resolution_gate_applied=True,
        meta_pre_gate_verdict="PRODUCE",
    )
    c = _assemble_production_caveats(v)
    assert c["has_any"] is True
    assert c["resolution_gate_applied"] is True
    assert c["pre_gate_verdict"] == "PRODUCE"


def test_caveats_backward_compat_with_pre_batch2_verdict():
    """Old verdict files predating the Batch 2 advisory-flag work in
    P4 lack `resolution_status`, `production_status_flag`, and the
    four new metadata fields. The caveats dict must still build
    without KeyError, and has_any must be False."""
    legacy = {
        "case_id": "legacy_001",
        "verdict": "PRODUCE",
        "narrative_score": 80.0,
        "key_moments": [],
        "content_pitch": "legacy pitch",
        # NO resolution_status / production_status_flag / metadata
    }
    c = _assemble_production_caveats(legacy)
    assert c["resolution_status"] is None
    assert c["production_status_flag"] is None
    assert c["degraded"] is False
    assert c["resolution_gate_applied"] is None
    assert c["pre_gate_verdict"] is None
    assert c["has_any"] is False


def test_caveats_handles_none_verdict():
    """Defensive: even None verdict shouldn't crash. Returns the empty
    caveats shape so downstream rendering can skip cleanly."""
    c = _assemble_production_caveats(None)
    assert c["has_any"] is False
    assert c["production_status_flag"] is None


# ---------------------------------------------------------------------------
# build_brief integration: caveats appear in brief dict
# ---------------------------------------------------------------------------


def test_build_brief_includes_production_caveats_field():
    """The brief dict always contains a `production_caveats` key, even
    when no caveats fire — keeps the JSON schema stable across cases."""
    v = _verdict_with(
        resolution_status="confirmed_final_outcome",
        production_status_flag=None,
    )
    brief = _build(v)
    assert "production_caveats" in brief
    assert brief["production_caveats"]["has_any"] is False


def test_build_brief_does_not_modify_verdict_for_pending_case():
    """The headline doctrine guarantee: the brief preserves
    verdict=PRODUCE for a pending case AND surfaces the caveat. The
    advisory layer never overrides P4's verdict."""
    v = _verdict_with(
        resolution_status="charges_filed_pending",
        production_status_flag="pending_case_review",
    )
    brief = _build(v)
    assert brief["verdict"] == "PRODUCE"
    assert brief["production_caveats"]["has_any"] is True
    assert brief["production_caveats"]["production_status_flag"] == "pending_case_review"


# ---------------------------------------------------------------------------
# render_markdown: section appears only when a caveat fires
# ---------------------------------------------------------------------------


def test_markdown_omits_caveats_section_for_confirmed_case():
    """No caveats -> the Production caveats heading does NOT appear.
    Section is suppressed entirely (not rendered as 'no caveats')."""
    v = _verdict_with(
        resolution_status="confirmed_final_outcome",
        production_status_flag=None,
    )
    md = render_markdown(_build(v))
    assert "## Production caveats" not in md


def test_markdown_includes_caveats_section_for_pending_case():
    """A pending winner triggers the section, with the production_status_flag
    name visible as a bold note."""
    v = _verdict_with(
        resolution_status="charges_filed_pending",
        production_status_flag="pending_case_review",
    )
    md = render_markdown(_build(v))
    assert "## Production caveats" in md
    assert "pending_case_review" in md
    assert "charges_filed_pending" in md


def test_markdown_includes_degraded_note_when_degraded():
    v = _verdict_with(meta_degraded=True)
    md = render_markdown(_build(v))
    assert "## Production caveats" in md
    assert "Pass 2 fallback" in md


def test_markdown_includes_gate_cap_note_when_gate_fired():
    v = _verdict_with(
        verdict="HOLD",
        resolution_status="charges_filed_pending",
        production_status_flag="pending_case_review",
        meta_resolution_gate_enabled=True,
        meta_resolution_gate_applied=True,
        meta_pre_gate_verdict="PRODUCE",
    )
    md = render_markdown(_build(v))
    assert "## Production caveats" in md
    assert "Verdict capped by resolution gate" in md
    assert "PRODUCE" in md  # the pre-gate verdict
    assert "HOLD" in md     # the emitted verdict


def test_markdown_caveats_section_is_advisory_only_does_not_change_verdict_line():
    """Doctrine pin: rendering the caveats must NOT change the verdict
    shown in the brief header. A pending winner's brief still reads
    'Verdict: PRODUCE' even when the caveat is loud."""
    v = _verdict_with(
        verdict="PRODUCE",
        resolution_status="charges_filed_pending",
        production_status_flag="pending_case_review",
    )
    md = render_markdown(_build(v))
    # The header line shows the original verdict
    assert "**Verdict:** PRODUCE" in md
    # AND the caveat surfaces
    assert "pending_case_review" in md


def test_markdown_caveats_for_legacy_verdict_omits_section():
    """Backward compat through the rendering layer: a verdict file
    without any of the new fields produces a brief with no caveats
    section (and no crash)."""
    legacy = {
        "case_id": "legacy_001",
        "verdict": "PRODUCE",
        "narrative_score": 80.0,
        "key_moments": [],
        "content_pitch": "legacy pitch",
    }
    md = render_markdown(_build(legacy))
    assert "## Production caveats" not in md


# ---------------------------------------------------------------------------
# Polish: empty Case summary section suppression
# ---------------------------------------------------------------------------


def test_markdown_omits_case_summary_when_no_p2_research():
    """When no P2 case_research is provided, the Case summary heading
    must NOT render as an empty section. Producers reading the brief
    should not see a dangling header with no content."""
    legacy = {
        "case_id": "legacy_001",
        "verdict": "PRODUCE",
        "narrative_score": 80.0,
        "key_moments": [],
        "content_pitch": "legacy pitch",
    }
    md = render_markdown(_build(legacy))
    assert "## Case summary" not in md


def test_markdown_includes_case_summary_when_p2_research_present():
    """Conversely, when P2 case_research IS provided, the Case
    summary heading + body must render."""
    case_research = {
        "defendant": "Test Defendant",
        "jurisdiction": "Test County, ST",
        "charges": ["murder", "tampering"],
        "incident_date": "2024-01-15",
        "summary": "A test case for rendering verification.",
    }
    v = _verdict_with()
    brief = build_brief(v, case_research=case_research, transcripts=[], weights=None)
    md = render_markdown(brief)
    assert "## Case summary" in md
    assert "Test Defendant" in md
    assert "Test County, ST" in md


def test_markdown_includes_case_summary_when_only_one_field_present():
    """Even a single field of case_research content is enough to
    render the section -- the suppression only applies when ALL
    fields are absent."""
    minimal_research = {"defendant": "Lone Defendant"}
    v = _verdict_with()
    brief = build_brief(v, case_research=minimal_research, transcripts=[], weights=None)
    md = render_markdown(brief)
    assert "## Case summary" in md
    assert "Lone Defendant" in md


# ---------------------------------------------------------------------------
# Polish: gate-cap defensive guard
# ---------------------------------------------------------------------------


def test_markdown_omits_gate_cap_bullet_when_pre_equals_emitted():
    """Defensive guard: synthetic / malformed input where
    gate_applied=True but pre_gate_verdict equals the emitted verdict
    must NOT render the cap line. The cap text would read vacuously
    ('would have been X without the gate; emitted as X') -- suppress
    the bullet entirely. Other caveat bullets still render."""
    v = _verdict_with(
        verdict="PRODUCE",
        resolution_status="charges_filed_pending",
        production_status_flag="pending_case_review",
        meta_resolution_gate_enabled=True,
        meta_resolution_gate_applied=True,
        meta_pre_gate_verdict="PRODUCE",  # SAME as emitted -- vacuous cap
    )
    md = render_markdown(_build(v))
    # Section still renders because production_status_flag fires
    assert "## Production caveats" in md
    assert "pending_case_review" in md
    # But the gate-cap bullet does NOT
    assert "Verdict capped by resolution gate" not in md


def test_markdown_includes_gate_cap_bullet_when_pre_differs_from_emitted():
    """Real-world case from apply_resolution_gate: gate fired and the
    cap actually changed the verdict (PRODUCE -> HOLD). Bullet renders."""
    v = _verdict_with(
        verdict="HOLD",
        resolution_status="charges_filed_pending",
        production_status_flag="pending_case_review",
        meta_resolution_gate_enabled=True,
        meta_resolution_gate_applied=True,
        meta_pre_gate_verdict="PRODUCE",
    )
    md = render_markdown(_build(v))
    assert "Verdict capped by resolution gate" in md
    # Bullet references both the pre-gate verdict and the emitted verdict
    assert "PRODUCE" in md
    assert "HOLD" in md


def test_markdown_suppresses_caveats_section_when_only_trigger_is_vacuous_gate():
    """Edge case: gate_applied=True is the ONLY caveat trigger, and
    the gate-cap bullet's defensive guard suppresses it (pre==emitted).
    Then no bullets remain -- the entire Production caveats section
    must be suppressed too. No empty section heading."""
    v = _verdict_with(
        verdict="PRODUCE",
        resolution_status="confirmed_final_outcome",
        production_status_flag=None,  # no production note
        meta_degraded=False,           # no Pass 2 fallback
        meta_resolution_gate_enabled=True,
        meta_resolution_gate_applied=True,
        meta_pre_gate_verdict="PRODUCE",  # vacuous cap
    )
    md = render_markdown(_build(v))
    assert "## Production caveats" not in md


def test_caveats_json_unchanged_by_markdown_guard():
    """Doctrine pin: the markdown gate-cap guard is a RENDERING-only
    polish. The underlying production_caveats JSON must still report
    `resolution_gate_applied: true` and `has_any: true` so audit and
    machine-parseable consumers still see the raw signal."""
    v = _verdict_with(
        verdict="PRODUCE",
        resolution_status="confirmed_final_outcome",
        production_status_flag=None,
        meta_degraded=False,
        meta_resolution_gate_enabled=True,
        meta_resolution_gate_applied=True,
        meta_pre_gate_verdict="PRODUCE",
    )
    brief = _build(v)
    # JSON faithfully records the raw signal
    assert brief["production_caveats"]["resolution_gate_applied"] is True
    assert brief["production_caveats"]["has_any"] is True
    # Markdown layer hides the vacuous cap bullet AND the section
    md = render_markdown(brief)
    assert "## Production caveats" not in md
