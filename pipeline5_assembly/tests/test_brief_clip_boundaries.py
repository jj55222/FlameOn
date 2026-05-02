"""Tests for clip-boundary suggestions in pipeline5_assemble.

Covers two pieces:

1. ``_add_clip_boundaries`` -- the per-moment helper that adds
   ``clip_start_sec`` / ``clip_end_sec`` for editor convenience.
   Defaults: 5 seconds before timestamp_sec, 3 seconds after end (or
   3 seconds after timestamp_sec when end is missing). Original
   timestamps are preserved unchanged.

2. ``build_brief`` + ``render_markdown`` -- the boundaries appear in
   the brief JSON and are rendered per moment in markdown as
   ``Clip suggestion: HH:MM:SS -> HH:MM:SS``.
"""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

from pipeline5_assemble import (  # noqa: E402
    _add_clip_boundaries,
    build_brief,
    render_markdown,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _moment(**overrides):
    """Build a minimal P4-style key_moment dict with overrides."""
    base = {
        "moment_type": "reveal",
        "source_idx": 0,
        "timestamp_sec": 100.0,
        "end_timestamp_sec": 110.0,
        "description": "test moment",
        "importance": "critical",
        "transcript_excerpt": "test excerpt",
    }
    base.update(overrides)
    return base


def _verdict_with_moments(moments):
    return {
        "case_id": "clip_test_001",
        "verdict": "PRODUCE",
        "narrative_score": 70.0,
        "confidence": 0.7,
        "key_moments": moments,
        "content_pitch": "test pitch",
        "narrative_arc_recommendation": "cold_open",
        "estimated_runtime_min": 20.0,
        "artifact_completeness": {"available": [], "missing_recommended": []},
        "scoring_breakdown": {},
        "_pipeline4_metadata": {},
    }


def _build(moments):
    return build_brief(
        _verdict_with_moments(moments),
        case_research=None, transcripts=[], weights=None,
    )


# ---------------------------------------------------------------------------
# _add_clip_boundaries unit tests
# ---------------------------------------------------------------------------


def test_clip_boundaries_normal_moment_with_start_and_end():
    """Standard case: timestamp + end both present.
    clip_start = max(0, ts - 5), clip_end = end + 3."""
    m = _moment(timestamp_sec=100.0, end_timestamp_sec=110.0)
    out = _add_clip_boundaries(m)
    assert out["clip_start_sec"] == 95.0
    assert out["clip_end_sec"] == 113.0


def test_clip_boundaries_missing_end_falls_back_to_ts_plus_3():
    """No end_timestamp_sec -> clip_end uses timestamp_sec + 3."""
    m = _moment(timestamp_sec=200.0, end_timestamp_sec=None)
    m.pop("end_timestamp_sec", None)  # also test with key absent
    out = _add_clip_boundaries(m)
    assert out["clip_start_sec"] == 195.0
    assert out["clip_end_sec"] == 203.0


def test_clip_boundaries_explicit_none_end_falls_back_to_ts_plus_3():
    """end_timestamp_sec explicitly None: same fallback."""
    m = _moment(timestamp_sec=200.0, end_timestamp_sec=None)
    out = _add_clip_boundaries(m)
    assert out["clip_end_sec"] == 203.0


def test_clip_boundaries_near_zero_clamps_start_to_zero():
    """timestamp_sec < 5 -> clip_start clamped to 0 (no negative starts)."""
    m = _moment(timestamp_sec=2.0, end_timestamp_sec=8.0)
    out = _add_clip_boundaries(m)
    assert out["clip_start_sec"] == 0.0
    assert out["clip_end_sec"] == 11.0


def test_clip_boundaries_at_exactly_five_seconds_starts_at_zero():
    """Boundary case: ts=5.0 -> clip_start = max(0, 0) = 0."""
    m = _moment(timestamp_sec=5.0, end_timestamp_sec=10.0)
    out = _add_clip_boundaries(m)
    assert out["clip_start_sec"] == 0.0


def test_clip_boundaries_preserves_original_timestamps():
    """The helper must NOT modify the original timestamp_sec /
    end_timestamp_sec fields. Editor consumers rely on the raw values
    being available alongside the clip suggestion."""
    m = _moment(timestamp_sec=412.5, end_timestamp_sec=431.0)
    out = _add_clip_boundaries(m)
    assert out["timestamp_sec"] == 412.5
    assert out["end_timestamp_sec"] == 431.0


def test_clip_boundaries_returns_copy_not_mutation():
    """The helper must return a new dict, not mutate input. Otherwise
    iterating moments and computing boundaries would silently change
    the caller's data."""
    m = _moment(timestamp_sec=100.0, end_timestamp_sec=110.0)
    original_keys = set(m.keys())
    _ = _add_clip_boundaries(m)
    assert set(m.keys()) == original_keys
    assert "clip_start_sec" not in m


def test_clip_boundaries_skips_moment_with_no_timestamp():
    """Defensive: moments without timestamp_sec get returned unchanged
    (no clip_* fields). Don't fabricate boundaries when we have no
    anchor."""
    m = {"moment_type": "reveal", "description": "no timestamp"}
    out = _add_clip_boundaries(m)
    assert "clip_start_sec" not in out
    assert "clip_end_sec" not in out
    assert out == m


def test_clip_boundaries_skips_non_numeric_timestamp():
    """Defensive: malformed timestamp (string, dict, etc.) -> no
    clip fields, no crash."""
    m = _moment(timestamp_sec="not a number", end_timestamp_sec=110.0)
    out = _add_clip_boundaries(m)
    assert "clip_start_sec" not in out
    assert "clip_end_sec" not in out


def test_clip_boundaries_handles_non_numeric_end_timestamp():
    """Bad end_timestamp_sec but good timestamp_sec -> end falls back
    to ts + 3 (same as missing-end branch)."""
    m = _moment(timestamp_sec=100.0, end_timestamp_sec="bogus")
    out = _add_clip_boundaries(m)
    assert out["clip_start_sec"] == 95.0
    assert out["clip_end_sec"] == 103.0


# ---------------------------------------------------------------------------
# build_brief integration: clip boundaries appear in brief JSON
# ---------------------------------------------------------------------------


def test_brief_json_includes_clip_boundaries_per_moment():
    moments = [_moment(timestamp_sec=100.0, end_timestamp_sec=110.0)]
    brief = _build(moments)
    assert len(brief["key_moments"]) == 1
    bm = brief["key_moments"][0]
    assert bm["clip_start_sec"] == 95.0
    assert bm["clip_end_sec"] == 113.0
    # Original timestamps still present
    assert bm["timestamp_sec"] == 100.0
    assert bm["end_timestamp_sec"] == 110.0


def test_brief_json_clip_boundaries_for_multiple_moments():
    """Each moment in a multi-moment brief gets its own boundaries."""
    moments = [
        _moment(timestamp_sec=100.0, end_timestamp_sec=110.0),
        _moment(timestamp_sec=200.0, end_timestamp_sec=215.0),
        _moment(timestamp_sec=3.0, end_timestamp_sec=8.0),  # near-zero
    ]
    brief = _build(moments)
    bms = brief["key_moments"]
    assert len(bms) == 3
    # After sorting by (source_idx, timestamp_sec), order is 3.0, 100.0, 200.0
    assert bms[0]["clip_start_sec"] == 0.0    # clamped from 3 - 5
    assert bms[1]["clip_start_sec"] == 95.0
    assert bms[2]["clip_start_sec"] == 195.0


# ---------------------------------------------------------------------------
# render_markdown: clip suggestion line per moment
# ---------------------------------------------------------------------------


def test_markdown_renders_clip_suggestion_for_each_moment():
    """Markdown output includes a 'Clip suggestion:' line for each
    moment with clip boundaries."""
    moments = [_moment(timestamp_sec=100.0, end_timestamp_sec=110.0)]
    md = render_markdown(_build(moments))
    assert "Clip suggestion:" in md


def test_markdown_clip_suggestion_uses_arrow_format():
    """Clip suggestion line uses 'HH:MM:SS -> HH:MM:SS' format."""
    moments = [_moment(timestamp_sec=100.0, end_timestamp_sec=110.0)]
    md = render_markdown(_build(moments))
    # 100s -> 1:35, 113s -> 1:53. Format: 'Clip suggestion: 1:35 -> 1:53'
    assert "1:35 -> 1:53" in md


def test_markdown_clip_suggestion_for_near_zero_moment():
    """Near-zero moment renders clip suggestion starting at 0:00."""
    moments = [_moment(timestamp_sec=2.0, end_timestamp_sec=8.0)]
    md = render_markdown(_build(moments))
    assert "0:00 -> 0:11" in md


def test_markdown_renders_clip_suggestion_even_without_excerpt():
    """Pre-Batch-2 behaviour rendered transcript excerpts only.
    Now every moment shows its clip suggestion regardless of whether
    it carries a transcript excerpt -- editor info is universal."""
    moments = [
        _moment(timestamp_sec=100.0, end_timestamp_sec=110.0,
                transcript_excerpt=""),  # empty excerpt
    ]
    md = render_markdown(_build(moments))
    assert "Clip suggestion:" in md


def test_markdown_omits_clip_line_when_moment_has_no_timestamp():
    """If a moment lacks timestamp_sec, _add_clip_boundaries returns
    no clip_* fields, and the renderer must NOT emit a vacuous
    'Clip suggestion: ? -> ?' line."""
    moments = [
        {
            "moment_type": "reveal",
            "source_idx": 0,
            "description": "no timestamp",
            "importance": "high",
        }
    ]
    md = render_markdown(_build(moments))
    assert "Clip suggestion:" not in md


def test_markdown_clip_suggestion_when_only_start_present():
    """Moment with timestamp_sec but no end_timestamp_sec still gets
    a clip suggestion (using the ts + 3 fallback)."""
    moments = [
        _moment(timestamp_sec=200.0),
    ]
    moments[0].pop("end_timestamp_sec", None)
    md = render_markdown(_build(moments))
    # 200s -> 3:15, 203s -> 3:23
    assert "3:15 -> 3:23" in md


def test_markdown_does_not_modify_verdict():
    """Doctrine pin: rendering clip boundaries must not affect the
    verdict shown in the brief header."""
    moments = [_moment(timestamp_sec=100.0, end_timestamp_sec=110.0)]
    md = render_markdown(_build(moments))
    assert "**Verdict:** PRODUCE" in md


def test_brief_clip_boundaries_does_not_alter_original_verdict_dict():
    """The build_brief enrichment must not mutate the input verdict's
    key_moments list. Defensive against accidental aliasing that
    would corrupt the on-disk verdict file if the caller writes it
    back later."""
    original_moment = _moment(timestamp_sec=100.0, end_timestamp_sec=110.0)
    verdict = _verdict_with_moments([original_moment])
    _ = build_brief(verdict, case_research=None, transcripts=[], weights=None)
    # Original moment in the verdict is unchanged -- no clip_* fields
    assert "clip_start_sec" not in verdict["key_moments"][0]
    assert "clip_end_sec" not in verdict["key_moments"][0]
