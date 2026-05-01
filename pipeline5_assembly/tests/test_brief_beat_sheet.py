"""Tests for the deterministic beat-sheet helper in pipeline5_assemble.

Covers:

1. ``_build_beat_sheet`` -- the pure 5-beat sheet builder. Selection
   order: hook -> climax -> escalation -> setup -> aftermath. Hook
   prefers reveal/emotional_peak/tension_shift; climax claims the
   strongest critical/high not used by the hook. Empty key_moments
   produces generic beat rows.

2. End-to-end: ``build_brief`` adds ``beat_sheet`` to the brief JSON;
   ``render_markdown`` produces "## Narrative Arc" + the
   "### Suggested Beat Sheet" table; verdict and inputs unchanged.

The beat sheet is an EDITORIAL SUGGESTION, not a scoring decision.
These tests pin both the deterministic selection logic and the
doctrine that the beat sheet never modifies the verdict or inputs.
"""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

from pipeline5_assemble import (  # noqa: E402
    _build_beat_sheet,
    build_brief,
    render_markdown,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


BEAT_NAMES = ["hook", "setup", "escalation", "climax", "aftermath"]


def _moment(**overrides):
    base = {
        "moment_type": "reveal",
        "source_idx": 0,
        "timestamp_sec": 100.0,
        "end_timestamp_sec": 110.0,
        "description": "test moment",
        "importance": "critical",
        "transcript_excerpt": "",
    }
    base.update(overrides)
    return base


def _verdict_with(moments=None, runtime_min=18.0, arc="cold_open"):
    return {
        "case_id": "beat_test_001",
        "verdict": "PRODUCE",
        "narrative_score": 80.0,
        "confidence": 0.8,
        "key_moments": moments or [],
        "content_pitch": "test pitch",
        "narrative_arc_recommendation": arc,
        "estimated_runtime_min": runtime_min,
        "artifact_completeness": {"available": [], "missing_recommended": []},
        "scoring_breakdown": {},
        "_pipeline4_metadata": {},
    }


def _build(moments, runtime_min=18.0, arc="cold_open"):
    return build_brief(
        _verdict_with(moments, runtime_min, arc),
        case_research=None, transcripts=[], weights=None,
    )


def _by_beat(beats):
    return {b["beat"]: b for b in beats}


# ---------------------------------------------------------------------------
# _build_beat_sheet unit tests
# ---------------------------------------------------------------------------


def test_beat_sheet_returns_five_beats_in_order():
    """Always 5 beats, always in canonical order regardless of input."""
    sheet = _build_beat_sheet("cold_open", 12.0, [])
    names = [b["beat"] for b in sheet["beats"]]
    assert names == BEAT_NAMES


def test_beat_sheet_empty_moments_uses_generic_descriptions():
    """No key_moments -> generic descriptions per beat, no crash."""
    sheet = _build_beat_sheet("cold_open", 12.0, [])
    bb = _by_beat(sheet["beats"])
    assert bb["hook"]["moment_type"] is None
    assert bb["hook"]["moment_description"]  # non-empty generic
    assert "case context" in bb["hook"]["moment_description"].lower() \
        or "available beat" in bb["hook"]["moment_description"].lower()


def test_beat_sheet_runtime_drives_minute_ranges():
    """estimated_runtime_min should produce proportional beat ranges
    (hook 0-15%, setup 15-30%, escalation 30-65%, climax 65-85%,
    aftermath 85-100%)."""
    sheet = _build_beat_sheet("cold_open", 20.0, [])
    bb = _by_beat(sheet["beats"])
    assert bb["hook"]["start_min"] == 0.0
    assert bb["hook"]["end_min"] == 3.0     # 15% of 20
    assert bb["setup"]["end_min"] == 6.0    # 30% of 20
    assert bb["escalation"]["end_min"] == 13.0  # 65% of 20
    assert bb["climax"]["end_min"] == 17.0  # 85% of 20
    assert bb["aftermath"]["end_min"] == 20.0   # 100% of 20


def test_beat_sheet_missing_runtime_defaults_to_12_minutes():
    """When estimated_runtime_min is None, the beat sheet defaults to
    a 12-minute runtime (sane editorial baseline)."""
    sheet = _build_beat_sheet("cold_open", None, [])
    assert sheet["estimated_runtime_min"] == 12.0
    bb = _by_beat(sheet["beats"])
    assert bb["aftermath"]["end_min"] == 12.0


def test_beat_sheet_invalid_runtime_falls_back_to_default():
    """Non-numeric / zero runtime -> 12 minutes, no crash."""
    for bad in [0, -5, "twelve", None]:
        sheet = _build_beat_sheet("cold_open", bad, [])
        assert sheet["estimated_runtime_min"] == 12.0


def test_beat_sheet_hook_prefers_reveal_over_other_critical():
    """Spec: hook selects from reveal/emotional_peak/tension_shift
    BEFORE falling back to other types. A reveal critical wins over
    a contradiction critical for the hook slot."""
    moments = [
        _moment(moment_type="contradiction", importance="critical",
                timestamp_sec=100.0, description="contradiction critical"),
        _moment(moment_type="reveal", importance="critical",
                timestamp_sec=200.0, description="reveal critical"),
    ]
    sheet = _build_beat_sheet("cold_open", 12.0, moments)
    bb = _by_beat(sheet["beats"])
    assert bb["hook"]["moment_type"] == "reveal"
    assert bb["hook"]["moment_description"] == "reveal critical"


def test_beat_sheet_hook_prefers_emotional_peak_when_no_reveal():
    moments = [
        _moment(moment_type="contradiction", importance="critical",
                description="contra"),
        _moment(moment_type="emotional_peak", importance="high",
                description="emo peak"),
    ]
    sheet = _build_beat_sheet("cold_open", 12.0, moments)
    bb = _by_beat(sheet["beats"])
    assert bb["hook"]["moment_type"] == "emotional_peak"


def test_beat_sheet_hook_falls_back_to_first_critical_when_no_hook_type():
    """No reveal/emotional_peak/tension_shift available -> hook picks
    the first chronological critical/high moment instead."""
    moments = [
        _moment(moment_type="contradiction", importance="critical",
                source_idx=0, timestamp_sec=300.0, description="late contra"),
        _moment(moment_type="callback", importance="high",
                source_idx=0, timestamp_sec=100.0, description="early callback"),
        _moment(moment_type="detail_noticed", importance="medium",
                source_idx=0, timestamp_sec=50.0, description="early detail"),
    ]
    sheet = _build_beat_sheet("cold_open", 12.0, moments)
    bb = _by_beat(sheet["beats"])
    # First chronological critical/high -> the callback at 100s
    assert bb["hook"]["moment_description"] == "early callback"


def test_beat_sheet_climax_does_not_reuse_hook_when_alternatives_exist():
    """The hook moment is consumed; climax must pick a different
    critical/high moment when one is available."""
    moments = [
        _moment(moment_type="reveal", importance="critical",
                timestamp_sec=100.0, description="reveal A"),
        _moment(moment_type="contradiction", importance="critical",
                timestamp_sec=200.0, description="contradiction B"),
    ]
    sheet = _build_beat_sheet("cold_open", 12.0, moments)
    bb = _by_beat(sheet["beats"])
    assert bb["hook"]["moment_description"] == "reveal A"
    # Climax must NOT be the same moment
    assert bb["climax"]["moment_description"] != "reveal A"
    assert bb["climax"]["moment_description"] == "contradiction B"


def test_beat_sheet_climax_can_reuse_hook_moment_if_only_one_critical():
    """Edge case: only one critical/high moment exists. Hook uses it.
    Climax's pool is then empty -> climax slot is generic."""
    moments = [
        _moment(moment_type="reveal", importance="critical",
                description="only critical"),
    ]
    sheet = _build_beat_sheet("cold_open", 12.0, moments)
    bb = _by_beat(sheet["beats"])
    assert bb["hook"]["moment_description"] == "only critical"
    assert bb["climax"]["moment_type"] is None  # generic
    assert "strongest narrative moment" in bb["climax"]["moment_description"].lower()


def test_beat_sheet_escalation_picks_remaining_high_or_medium():
    """After hook + climax claim the criticals, escalation gets the
    next best high/medium."""
    moments = [
        _moment(moment_type="reveal", importance="critical",
                description="hook"),
        _moment(moment_type="contradiction", importance="critical",
                description="climax"),
        _moment(moment_type="callback", importance="high",
                description="esc high"),
        _moment(moment_type="detail_noticed", importance="medium",
                description="esc medium"),
    ]
    sheet = _build_beat_sheet("cold_open", 12.0, moments)
    bb = _by_beat(sheet["beats"])
    # Highest remaining importance after hook+climax is the high callback
    assert bb["escalation"]["moment_description"] == "esc high"


def test_beat_sheet_setup_picks_earliest_remaining_chronologically():
    """Setup picks the earliest chronological moment NOT yet used by
    hook/climax/escalation."""
    moments = [
        _moment(moment_type="reveal", importance="critical",
                source_idx=0, timestamp_sec=300.0, description="hook"),
        _moment(moment_type="contradiction", importance="critical",
                source_idx=0, timestamp_sec=400.0, description="climax"),
        _moment(moment_type="callback", importance="high",
                source_idx=0, timestamp_sec=500.0, description="escalation"),
        _moment(moment_type="detail_noticed", importance="low",
                source_idx=0, timestamp_sec=50.0, description="early setup"),
        _moment(moment_type="detail_noticed", importance="low",
                source_idx=0, timestamp_sec=600.0, description="late detail"),
    ]
    sheet = _build_beat_sheet("cold_open", 12.0, moments)
    bb = _by_beat(sheet["beats"])
    # Earliest unused -> the 50s low-importance detail
    assert bb["setup"]["moment_description"] == "early setup"


def test_beat_sheet_aftermath_picks_latest_remaining_chronologically():
    moments = [
        _moment(moment_type="reveal", importance="critical",
                timestamp_sec=300.0, description="hook"),
        _moment(moment_type="contradiction", importance="critical",
                timestamp_sec=400.0, description="climax"),
        _moment(moment_type="detail_noticed", importance="low",
                timestamp_sec=50.0, description="setup"),
        _moment(moment_type="detail_noticed", importance="low",
                timestamp_sec=600.0, description="late detail"),
    ]
    sheet = _build_beat_sheet("cold_open", 12.0, moments)
    bb = _by_beat(sheet["beats"])
    # Latest unused -> the 600s detail
    assert bb["aftermath"]["moment_description"] == "late detail"


def test_beat_sheet_does_not_mutate_input_moments():
    """Doctrine pin: the helper must not modify the input moment dicts.
    Defensive against accidental aliasing that would corrupt the
    on-disk verdict file if the caller writes it back later."""
    m = _moment(moment_type="reveal", importance="critical",
                description="should stay clean")
    original_keys = set(m.keys())
    _ = _build_beat_sheet("cold_open", 12.0, [m])
    assert set(m.keys()) == original_keys


# ---------------------------------------------------------------------------
# build_brief integration
# ---------------------------------------------------------------------------


def test_brief_json_includes_beat_sheet():
    moments = [_moment(moment_type="reveal", importance="critical")]
    brief = _build(moments)
    assert "beat_sheet" in brief
    assert brief["beat_sheet"]["narrative_arc"] == "cold_open"
    assert len(brief["beat_sheet"]["beats"]) == 5


def test_brief_beat_sheet_for_empty_moments():
    """Brief built from a verdict with no key_moments still has a
    valid 5-beat sheet (all generic)."""
    brief = _build([])
    beats = brief["beat_sheet"]["beats"]
    assert len(beats) == 5
    for b in beats:
        assert b["moment_type"] is None
        assert b["moment_description"]


def test_brief_beat_sheet_uses_p4_estimated_runtime_when_present():
    moments = [_moment()]
    brief = _build(moments, runtime_min=20.0)
    assert brief["beat_sheet"]["estimated_runtime_min"] == 20.0
    bb = _by_beat(brief["beat_sheet"]["beats"])
    assert bb["aftermath"]["end_min"] == 20.0


def test_brief_does_not_mutate_input_verdict():
    """build_brief enrichment of beat_sheet must not leak back into
    the input verdict dict."""
    moments = [_moment(moment_type="reveal", importance="critical",
                       description="test")]
    verdict = _verdict_with(moments)
    _ = build_brief(verdict, case_research=None, transcripts=[], weights=None)
    # No new keys leaked into the original verdict
    assert "beat_sheet" not in verdict
    # Original moments unchanged
    assert verdict["key_moments"][0]["description"] == "test"


# ---------------------------------------------------------------------------
# render_markdown integration
# ---------------------------------------------------------------------------


def test_markdown_renders_narrative_arc_heading():
    """Markdown contains '## Narrative Arc: <arc>' from beat_sheet.narrative_arc."""
    moments = [_moment()]
    md = render_markdown(_build(moments, arc="cold_open"))
    assert "## Narrative Arc: cold_open" in md


def test_markdown_renders_suggested_beat_sheet_heading():
    moments = [_moment()]
    md = render_markdown(_build(moments))
    assert "### Suggested Beat Sheet" in md


def test_markdown_renders_beat_sheet_table_with_all_five_beats():
    moments = [_moment()]
    md = render_markdown(_build(moments))
    # Table header
    assert "| Beat | Timing (min) | Moment | Description |" in md
    # All 5 beat names appear in the table body
    for beat_name in BEAT_NAMES:
        assert f"| {beat_name} |" in md


def test_markdown_renders_beat_sheet_for_empty_moments():
    """Even with no key_moments, the markdown beat sheet should still
    render with generic descriptions -- the editorial scaffold is
    useful even when the case has sparse moment extraction."""
    md = render_markdown(_build([]))
    assert "## Narrative Arc:" in md
    assert "### Suggested Beat Sheet" in md
    assert "(generic)" in md


def test_markdown_does_not_modify_verdict():
    """Doctrine pin: rendering the beat sheet must not change the
    verdict header line."""
    moments = [_moment(moment_type="reveal", importance="critical")]
    md = render_markdown(_build(moments))
    assert "**Verdict:** PRODUCE" in md


def test_markdown_beat_sheet_renders_moment_type_and_importance():
    """Beat rows show 'moment_type / importance' label when a moment
    is assigned."""
    moments = [
        _moment(moment_type="reveal", importance="critical",
                description="rev critical desc"),
    ]
    md = render_markdown(_build(moments))
    assert "reveal / critical" in md


def test_markdown_beat_sheet_pipe_in_description_does_not_break_table():
    """Defensive: a description containing the pipe character `|`
    must be escaped so it doesn't break the markdown table column
    boundaries."""
    moments = [
        _moment(moment_type="reveal", importance="critical",
                description="contains | pipe inside"),
    ]
    md = render_markdown(_build(moments))
    # The literal "| pipe inside" without escaping would break the
    # row into more cells. Our escape produces "\|" instead.
    assert r"contains \| pipe inside" in md
