"""Tier 1 #4 — contract tests for the adapter shape that
``evaluate.adapt_winner_to_merged`` produces.

WHY THIS TEST FILE EXISTS:

``pipeline4_scoring/evaluate.py`` is the immutable scorer. Inside it,
``adapt_winner_to_merged`` deliberately constructs a non-natural
merged-transcript dict shape:

- It builds **N source entries** (one per artifact in the winner's
  ``artifact_combination``).
- It tags **all transcript segments with source_idx=0**, even though
  there are N source entries.
- ``available_evidence_types`` is set from the winner profile's
  ``artifact_combination``, not derived from segment groupings.

This is a hack — but it's the discriminator that lifted V6 from 35
to 71 (per ``results.tsv``). Winners with 4-artifact combos get high
``artifact_completeness_score`` while admin/single-source content
gets the partial-credit fallback. The hack works because the only
piece of P4 that consumes "what artifacts exist for this case" is
``available_evidence_types`` — segments-per-source isn't needed.

THE CONTRACT THIS HACK DEPENDS ON:

1. ``compute_all`` reads ``available_artifacts`` from the caller —
   which in ``pipeline4_score.score_case`` comes from
   ``merged.get("available_evidence_types", [])``. It does NOT
   re-derive artifacts from segment-source groupings.
2. ``format_for_llm`` writes per-source banners but only walks
   ``merged["segments"]``, looking up the source by index. With all
   segments at idx=0, only S0's banner appears — which is fine for
   the LLM (it sees one combined transcript banner, not four).
3. The LLM prompt's ``AVAILABLE ARTIFACTS:`` line in
   ``render_pass2`` reads ``merged.get("available_evidence_types")``
   — the canonical source of truth for what artifacts the case has,
   independent of segment grouping.

If a future change makes ``format_for_llm`` derive evidence types
from segment-source groupings, OR makes ``score_case`` re-derive
artifacts by walking sources, the hack silently breaks and winners
collapse back to the partial-credit fallback (~50 points lost).

The tests below pin those contracts:
- A simulated adapter-output dict scores correctly.
- ``format_for_llm`` doesn't crash on the all-segments-at-idx=0 shape.
- ``available_evidence_types`` survives unchanged through scoring.
- The artifact_completeness_score reads from
  ``available_evidence_types``, not segment groupings.

If any of these tests start failing, INVESTIGATE before "fixing" —
the test is probably reporting a real regression in the contract,
not a mis-spec on its own end.
"""
from __future__ import annotations

import pytest

from scoring_math import compute_all, equal_weight_fallback
from transcript_loader import format_for_llm


def _winner_shape_merged(case_id: str, artifacts: list, total_dur: float = 3000.0):
    """Reproduce the exact dict shape that
    ``evaluate.adapt_winner_to_merged`` builds: N source entries (one
    per artifact), all segments tagged source_idx=0,
    ``available_evidence_types`` mirroring the artifacts list.
    """
    sources = [
        {
            "source_idx": idx,
            "source_url": f"https://youtube.com/watch?v={case_id}#{art}",
            "evidence_type": art,
            "duration_sec": float(total_dur),
            "processed_duration_sec": float(total_dur),
            "transcript_path": f"/tmp/{case_id}.json",
        }
        for idx, art in enumerate(artifacts)
    ]
    segments = [
        {"source_idx": 0, "start_sec": 0.0, "end_sec": 5.0, "text": "scene one",
         "speaker": None, "confidence": None},
        {"source_idx": 0, "start_sec": 60.0, "end_sec": 70.0, "text": "scene two",
         "speaker": None, "confidence": None},
        {"source_idx": 0, "start_sec": 120.0, "end_sec": 130.0, "text": "scene three",
         "speaker": None, "confidence": None},
    ]
    return {
        "case_id": case_id,
        "sources": sources,
        "segments": segments,
        "total_duration_sec": float(total_dur),
        "transcript_refs": [s["transcript_path"] for s in sources],
        "available_evidence_types": list(artifacts),
    }


def test_format_for_llm_handles_segments_all_at_idx_zero():
    """The hack: all segments tagged source_idx=0 with multiple source
    entries. format_for_llm must not crash and must emit at least the
    S0 banner (the others can be silent — that's fine; the LLM sees
    a unified transcript)."""
    merged = _winner_shape_merged(
        "winner_X",
        artifacts=["bodycam", "interrogation", "911_audio", "narration"],
    )
    rendered = format_for_llm(merged)
    assert rendered  # non-empty
    # S0 banner present
    assert "=== SOURCE S0:" in rendered
    # Transcript text is in the rendered output
    assert "scene one" in rendered
    assert "scene three" in rendered


def test_available_evidence_types_carries_through_to_scoring():
    """compute_all's ``available_artifacts`` set must come from the
    caller (i.e., merged['available_evidence_types']), NOT from
    walking segments and looking up per-source evidence_type. This is
    what makes the artifact discriminator work."""
    merged = _winner_shape_merged(
        "winner_X",
        artifacts=["bodycam", "interrogation", "911_audio", "narration"],
    )
    # Simulated weights with an artifact_value combo for the full set.
    # If the contract holds, the full subset matches and the score
    # equals the combo's value × 100.
    weights = {
        "moment_weights": equal_weight_fallback()["moment_weights"],
        "arc_patterns": [],
        "artifact_value": {
            "bodycam+interrogation+911_audio+narration": 0.95,
            "bodycam+interrogation": 0.5,
        },
    }
    moments = [
        {"type": "contradiction", "provisional_importance": "critical"},
    ]
    out = compute_all(
        moments=moments,
        weights=weights,
        runtime_sec=merged["total_duration_sec"],
        # The CRITICAL line: pass the merged's available_evidence_types
        # directly as the artifact set, exactly as score_case does.
        available_artifacts=set(merged["available_evidence_types"]),
        detected_structure="cold_open",
        factual_anchors=[],
    )
    breakdown = out["scoring_breakdown"]
    # Full subset match → 0.95 × 100 = 95.0
    assert breakdown["artifact_completeness_score"] == 95.0


def test_artifact_score_collapses_if_segments_are_used_instead():
    """Counter-test: if a future regression makes the score path use
    segment-derived artifacts (which are EMPTY because all segments
    are at source_idx=0 → only one source consulted, only one
    artifact attributed), the score collapses to partial-credit
    fallback."""
    merged = _winner_shape_merged(
        "winner_X",
        artifacts=["bodycam", "interrogation", "911_audio", "narration"],
    )
    weights = {
        "moment_weights": equal_weight_fallback()["moment_weights"],
        "arc_patterns": [],
        "artifact_value": {
            "bodycam+interrogation+911_audio+narration": 0.95,
        },
    }
    # SIMULATED REGRESSION: derive available_artifacts from segments
    # (which would only see source 0 = "bodycam").
    seg_idxs = {s["source_idx"] for s in merged["segments"]}
    artifacts_from_segments = {
        merged["sources"][i]["evidence_type"] for i in seg_idxs
    }
    out = compute_all(
        moments=[],
        weights=weights,
        runtime_sec=merged["total_duration_sec"],
        available_artifacts=artifacts_from_segments,  # WRONG path
        detected_structure="cold_open",
        factual_anchors=[],
    )
    # The score collapses to partial credit (1 of 4 artifacts × 0.95 × 100 = 23.75).
    breakdown = out["scoring_breakdown"]
    assert breakdown["artifact_completeness_score"] < 30
    # This is the regression we're guarding against. score_case MUST
    # use merged["available_evidence_types"] directly, not derive it
    # from segments.


def test_winner_shape_does_not_break_score_orchestration():
    """End-to-end smoke: a winner-shape merged dict can flow through
    compute_all without errors. This catches surprises like None
    handling, segment-iteration crashes, etc."""
    merged = _winner_shape_merged(
        "winner_X",
        artifacts=["bodycam", "interrogation"],
    )
    weights = equal_weight_fallback()
    out = compute_all(
        moments=[],
        weights=weights,
        runtime_sec=merged["total_duration_sec"],
        available_artifacts=set(merged["available_evidence_types"]),
        detected_structure="cold_open",
        factual_anchors=[],
    )
    assert "narrative_score" in out
    assert "verdict" in out


def test_winner_shape_with_zero_artifacts_does_not_crash():
    """Edge case: a malformed winner profile with empty
    artifact_combination falls back to ['other'] in evaluate.py.
    Make sure that empty/single-artifact case still scores cleanly."""
    merged = _winner_shape_merged("winner_X", artifacts=["other"])
    weights = equal_weight_fallback()
    out = compute_all(
        moments=[],
        weights=weights,
        runtime_sec=merged["total_duration_sec"],
        available_artifacts=set(merged["available_evidence_types"]),
        detected_structure=None,
        factual_anchors=[],
    )
    assert isinstance(out["narrative_score"], (int, float))
    assert out["verdict"] in ("PRODUCE", "HOLD", "SKIP")
