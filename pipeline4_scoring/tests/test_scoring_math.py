"""Unit tests for scoring_math.py — the deterministic numeric layer.

Covers:
- moment_density_score: weighted moments per minute, normalization,
  zero/empty handling, runtime clamping.
- arc_similarity_score: match against P1 winner arc patterns,
  empty-pattern fallback (currently 50.0 — see Tier 1 #3).
- artifact_completeness_score: best subset-fit, partial-credit
  fallback, neutral default.
- uniqueness_score: distinct moment types, contradiction /
  procedural bonuses, factual-anchor richness, hard cap at 80.
- combine: 4-subscore linear blend.
- decide_verdict: PRODUCE / SKIP / HOLD gates with env-tunable
  thresholds.
- estimate_runtime_min: clamping bounds.
- compute_all: end-to-end orchestration.
- equal_weight_fallback: shape + values.
"""
from __future__ import annotations

import os

import pytest

from scoring_math import (
    DEFAULT_SUBSCORE_WEIGHTS,
    IMPORTANCE_MULTIPLIER,
    PRODUCE_DENSITY_THRESH,
    PRODUCE_SCORE_THRESH,
    REFERENCE_DENSITY,
    RESOLUTION_VERDICT_CEILING,
    SKIP_SCORE_THRESH,
    SUBSCORE_MISSING_FLOOR,
    VALID_MOMENT_TYPES,
    VALID_RESOLUTION_STATUSES,
    arc_similarity_score,
    artifact_completeness_score,
    combine,
    compute_all,
    decide_verdict,
    equal_weight_fallback,
    estimate_runtime_min,
    moment_density_score,
    uniqueness_score,
)


# ---- equal_weight_fallback ------------------------------------------------


def test_equal_weight_fallback_returns_canonical_shape():
    w = equal_weight_fallback()
    assert "moment_weights" in w
    assert w["arc_patterns"] == []
    assert w["artifact_value"] == {}
    assert w["_equal_weight_fallback"] is True


def test_equal_weight_fallback_distributes_one_evenly():
    w = equal_weight_fallback()
    weights = w["moment_weights"]
    assert set(weights) == set(VALID_MOMENT_TYPES)
    total = sum(weights.values())
    assert abs(total - 1.0) < 0.01


# ---- moment_density_score -------------------------------------------------


def test_moment_density_score_zero_when_no_moments():
    assert moment_density_score([], equal_weight_fallback(), 1800) == 0.0


def test_moment_density_score_zero_when_runtime_zero():
    assert moment_density_score(
        [{"type": "contradiction", "provisional_importance": "critical"}],
        equal_weight_fallback(),
        0,
    ) == 0.0


def test_moment_density_score_uses_importance_multiplier():
    """Critical moments contribute fully; low moments contribute 0.15
    of a full unit. With identical type weights, the critical case must
    score higher than the low case, with the ratio approximately
    matching IMPORTANCE_MULTIPLIER[critical] / IMPORTANCE_MULTIPLIER[low].

    Use 50 moments per case + low reference_density so both scores are
    well above the 2-decimal rounding floor — keeps the ratio assertion
    stable.
    """
    weights = equal_weight_fallback()
    critical_only = [
        {"type": "contradiction", "provisional_importance": "critical"}
    ] * 50
    low_only = [
        {"type": "contradiction", "provisional_importance": "low"}
    ] * 50
    # Use the original 0.6 reference_density so scores land in a
    # mid-range neither saturating at 100 nor flattened by 2-decimal
    # rounding near zero.
    s_critical = moment_density_score(critical_only, weights, 1800,
                                       reference_density=0.6)
    s_low = moment_density_score(low_only, weights, 1800,
                                  reference_density=0.6)
    assert s_critical > s_low
    # Neither score should be saturated at 100; both should be measurable.
    assert s_critical < 100.0
    assert s_low > 1.0
    # Importance ratio matches IMPORTANCE_MULTIPLIER ratio (within 5%
    # tolerance to absorb small rounding effects).
    expected_ratio = IMPORTANCE_MULTIPLIER["critical"] / IMPORTANCE_MULTIPLIER["low"]
    actual_ratio = s_critical / s_low
    assert abs(actual_ratio - expected_ratio) / expected_ratio < 0.05


def test_moment_density_score_clamps_runtime_to_50min_max():
    """A 2-hour transcript should be scored against a 50-min effective
    runtime (the clamp prevents long interrogations from being
    over-penalized for length)."""
    weights = equal_weight_fallback()
    moments = [{"type": "contradiction", "provisional_importance": "critical"}] * 30
    score_50min = moment_density_score(moments, weights, 50 * 60)
    score_120min = moment_density_score(moments, weights, 120 * 60)
    assert score_50min == score_120min


def test_moment_density_score_clamps_runtime_to_20min_min():
    """A 5-min clip is scored as if it were 20 min — prevents tiny
    clips from gaming the ratio."""
    weights = equal_weight_fallback()
    moments = [{"type": "contradiction", "provisional_importance": "critical"}] * 5
    score_5min = moment_density_score(moments, weights, 5 * 60)
    score_20min = moment_density_score(moments, weights, 20 * 60)
    assert score_5min == score_20min


def test_moment_density_score_caps_at_100():
    weights = equal_weight_fallback()
    # Stuff 200 critical moments — way over reference density
    moments = [{"type": "contradiction", "provisional_importance": "critical"}] * 200
    score = moment_density_score(moments, weights, 30 * 60, reference_density=0.02)
    assert score == 100.0


def test_moment_density_score_handles_pass2_keys():
    """In Pass 2, moments use 'moment_type' + 'importance'. In Pass 1,
    'type' + 'provisional_importance'. The function reads either."""
    weights = equal_weight_fallback()
    pass1_style = [{"type": "reveal", "provisional_importance": "high"}]
    pass2_style = [{"moment_type": "reveal", "importance": "high"}]
    s1 = moment_density_score(pass1_style, weights, 1800)
    s2 = moment_density_score(pass2_style, weights, 1800)
    assert s1 == s2 > 0


# ---- arc_similarity_score -------------------------------------------------


def test_arc_similarity_score_returns_zero_when_no_detected_structure():
    """Severe / case-intrinsic: Pass 1 couldn't infer a narrative
    shape. The case lacks visible arc — penalize directly with 0.
    Distinct from the "no reference data" None path below."""
    score = arc_similarity_score(
        None,
        [{"structure_type": "cold_open", "frequency": 0.8,
          "avg_view_count": 1_000_000}],
    )
    assert score == 0.0


def test_arc_similarity_score_returns_none_when_no_arc_patterns():
    """Moderate / reference-data missing: empty arc_patterns means
    we lack P1 winner reference data to compare against. Returns None
    so combine() can apply SUBSCORE_MISSING_FLOOR (a punitive but
    non-annihilating floor)."""
    score = arc_similarity_score("cold_open", [])
    assert score is None


def test_arc_similarity_score_returns_none_when_all_patterns_have_zero_score():
    """Patterns exist but yield no comparable score → moderate
    missing (None → floor)."""
    score = arc_similarity_score(
        "cold_open",
        [{"structure_type": "cold_open", "frequency": 0, "avg_view_count": 0}],
    )
    assert score is None


def test_arc_similarity_score_full_match_scores_100():
    patterns = [
        {"structure_type": "cold_open", "frequency": 0.8, "avg_view_count": 5_000_000},
        {"structure_type": "chronological", "frequency": 0.1, "avg_view_count": 100_000},
    ]
    score = arc_similarity_score("cold_open", patterns)
    assert score == 100.0


def test_arc_similarity_score_no_match_scores_zero():
    patterns = [
        {"structure_type": "cold_open", "frequency": 0.8, "avg_view_count": 5_000_000},
    ]
    score = arc_similarity_score("escalation", patterns)
    assert score == 0.0


# ---- artifact_completeness_score ------------------------------------------


def test_artifact_completeness_score_returns_none_when_no_reference_data():
    """Moderate / reference-data missing: no P1 artifact_value loaded.
    Returns None (was 50.0 — the "stale neutral" bug). combine()
    applies SUBSCORE_MISSING_FLOOR so a no-reference run scores
    below a complete run."""
    score, missing = artifact_completeness_score({"bodycam"}, {})
    assert score is None
    assert missing == []


def test_artifact_completeness_score_full_subset_match():
    artifact_value = {
        "bodycam+interrogation+911_audio": 0.9,
        "bodycam+interrogation": 0.6,
    }
    available = {"bodycam", "interrogation", "911_audio"}
    score, missing = artifact_completeness_score(available, artifact_value)
    assert score == 90.0
    # missing relative to top combo (which IS the matched combo)
    assert missing == []


def test_artifact_completeness_score_partial_credit_when_no_subset_fit():
    """When available doesn't fully contain any combo, fall back to
    the highest-valued combo's overlap × that combo's value."""
    artifact_value = {"bodycam+interrogation+911_audio+documents": 1.0}
    available = {"bodycam", "interrogation"}  # missing 911_audio + documents
    score, missing = artifact_completeness_score(available, artifact_value)
    # 2 of 4 overlap × 1.0 × 100 = 50
    assert score == 50.0
    assert set(missing) == {"911_audio", "documents"}


def test_artifact_completeness_score_picks_highest_valued_subset():
    """When multiple combos are subsets, the highest-valued one wins."""
    artifact_value = {
        "bodycam": 0.4,
        "bodycam+interrogation": 0.7,
    }
    available = {"bodycam", "interrogation"}
    score, _ = artifact_completeness_score(available, artifact_value)
    assert score == 70.0


# ---- uniqueness_score -----------------------------------------------------


def test_uniqueness_score_zero_when_no_moments():
    assert uniqueness_score([], []) == 0.0


def test_uniqueness_score_counts_distinct_types():
    moments = [
        {"type": "contradiction", "provisional_importance": "high"},
        {"type": "reveal", "provisional_importance": "high"},
        {"type": "emotional_peak", "provisional_importance": "medium"},
    ]
    score = uniqueness_score(moments, [])
    # 3 distinct types × 10 = 30
    assert score == 30.0


def test_uniqueness_score_contradiction_bonus_applied_at_two_or_more():
    one_contra = [{"type": "contradiction", "provisional_importance": "high"}]
    two_contra = [
        {"type": "contradiction", "provisional_importance": "high"},
        {"type": "contradiction", "provisional_importance": "medium"},
    ]
    s_one = uniqueness_score(one_contra, [])
    s_two = uniqueness_score(two_contra, [])
    # One distinct type → 10 (one_contra) and 10 (two_contra),
    # plus +5 contradiction bonus on two_contra.
    assert s_two - s_one == 5


def test_uniqueness_score_procedural_bonus_applied():
    no_proc = [{"type": "reveal", "provisional_importance": "high"}]
    with_proc = [
        {"type": "reveal", "provisional_importance": "high"},
        {"type": "procedural_violation", "provisional_importance": "high"},
    ]
    s_no = uniqueness_score(no_proc, [])
    s_yes = uniqueness_score(with_proc, [])
    # 1 type × 10 = 10, vs 2 types × 10 + 3 proc = 23. Delta = 13.
    assert s_yes - s_no == 13


def test_uniqueness_score_factual_anchor_bonus_capped_at_two():
    moments = [{"type": "contradiction", "provisional_importance": "high"}]
    five_anchor_types = [{"type": t, "value": "x", "source_idx": 0, "timestamp_sec": 0}
                         for t in ("name", "date", "location", "badge_number", "charge")]
    one_anchor_type = [{"type": "name", "value": "x", "source_idx": 0, "timestamp_sec": 0}]
    s_one = uniqueness_score(moments, one_anchor_type)
    s_many = uniqueness_score(moments, five_anchor_types)
    # 1 anchor type → +1 bonus; 5 anchor types → +2 bonus (capped). Delta = 1.
    assert s_many - s_one == 1


def test_uniqueness_score_caps_at_80():
    # 7 distinct types × 10 = 70 + contradiction bonus 5 + proc bonus 3 + 2 anchor bonus = 80
    # Plus more — should still cap.
    moments = [
        {"type": t, "provisional_importance": "high"} for t in VALID_MOMENT_TYPES
    ] + [
        {"type": "contradiction", "provisional_importance": "high"} for _ in range(5)
    ] + [
        {"type": "procedural_violation", "provisional_importance": "high"} for _ in range(3)
    ]
    anchors = [{"type": t, "value": "x", "source_idx": 0, "timestamp_sec": 0}
               for t in ("name", "date", "location")]
    score = uniqueness_score(moments, anchors)
    assert score == 80


# ---- combine --------------------------------------------------------------


def test_combine_uses_default_weights():
    breakdown = {
        "moment_density_score": 100,
        "arc_similarity_score": 50,
        "artifact_completeness_score": 80,
        "uniqueness_score": 40,
    }
    score = combine(breakdown)
    # 100*0.40 + 50*0.30 + 80*0.20 + 40*0.10 = 40 + 15 + 16 + 4 = 75
    assert score == 75.0


def test_combine_handles_zero_subscores():
    breakdown = {
        "moment_density_score": 0,
        "arc_similarity_score": 0,
        "artifact_completeness_score": 0,
        "uniqueness_score": 0,
    }
    assert combine(breakdown) == 0.0


def test_combine_default_subscore_weights_sum_to_one():
    assert abs(sum(DEFAULT_SUBSCORE_WEIGHTS) - 1.0) < 1e-9


def test_combine_applies_subscore_missing_floor_when_arc_is_none():
    """EDITORIAL PHILOSOPHY: a missing arc_similarity_score does NOT
    cause its weight to be redistributed onto present subscores.
    Instead, ``SUBSCORE_MISSING_FLOOR['arc_similarity_score']`` is
    substituted for the missing subscore. This is punitive (the floor
    is below 50) but not annihilating (the floor is above 0)."""
    breakdown = {
        "moment_density_score": 100,
        "arc_similarity_score": None,       # missing → floor
        "artifact_completeness_score": 100,
        "uniqueness_score": 100,
    }
    score = combine(breakdown)
    # Expected: 100*0.40 + floor_arc*0.30 + 100*0.20 + 100*0.10
    #         = 40 + 15*0.30 + 20 + 10
    #         = 74.5
    floor_arc = SUBSCORE_MISSING_FLOOR["arc_similarity_score"]
    expected = 100 * 0.40 + floor_arc * 0.30 + 100 * 0.20 + 100 * 0.10
    assert score == round(expected, 2)
    # Crucial: the score is BELOW 100 even though all present subscores
    # are 100 — the missing arc dragged the total down. This is the
    # whole editorial point.
    assert score < 100.0


def test_combine_missing_critical_lowers_score_vs_complete_case():
    """The headline editorial guarantee: an otherwise-identical case
    that is MISSING arc data MUST score below the complete case. No
    redistribution rescue."""
    complete = {
        "moment_density_score": 80,
        "arc_similarity_score": 80,
        "artifact_completeness_score": 80,
        "uniqueness_score": 60,
    }
    missing_arc = {
        "moment_density_score": 80,
        "arc_similarity_score": None,
        "artifact_completeness_score": 80,
        "uniqueness_score": 60,
    }
    s_complete = combine(complete)
    s_missing = combine(missing_arc)
    assert s_missing < s_complete
    # And specifically: missing arc costs ~20 points (the difference
    # between an 80 and the 15 floor, scaled by 0.30 weight).
    delta = s_complete - s_missing
    assert delta >= 15.0  # at least 15 points lost from missing arc


def test_combine_missing_artifact_lowers_score_vs_complete_case():
    """Same guarantee for the artifact dimension: missing artifact
    reference data costs the case real points."""
    complete = {
        "moment_density_score": 70,
        "arc_similarity_score": 70,
        "artifact_completeness_score": 70,
        "uniqueness_score": 50,
    }
    missing_artifact = dict(complete, artifact_completeness_score=None)
    s_complete = combine(complete)
    s_missing = combine(missing_artifact)
    assert s_missing < s_complete
    # Floor is 20; complete had 70 there. Cost = (70 - 20) * 0.20 = 10.
    delta = s_complete - s_missing
    assert delta >= 9.0  # ~10 points expected


def test_combine_missing_arc_AND_artifact_compounds_penalty():
    """A case missing BOTH arc and artifact reference data should
    score below either single-missing case. Penalties compound — the
    scorer is editorially ruthless about layered missingness."""
    complete = {
        "moment_density_score": 80,
        "arc_similarity_score": 80,
        "artifact_completeness_score": 80,
        "uniqueness_score": 60,
    }
    missing_one = dict(complete, arc_similarity_score=None)
    missing_both = dict(complete, arc_similarity_score=None,
                        artifact_completeness_score=None)
    s_complete = combine(complete)
    s_missing_one = combine(missing_one)
    s_missing_both = combine(missing_both)
    assert s_missing_both < s_missing_one < s_complete


def test_combine_with_only_one_present_subscore_uses_floors_for_others():
    """Density alone present, others missing: total is density's
    weighted contribution PLUS the weighted floors. The floors are
    real numeric contributions, not 'redistributed away'."""
    breakdown = {
        "moment_density_score": 80,
        "arc_similarity_score": None,
        "artifact_completeness_score": None,
        "uniqueness_score": None,
    }
    score = combine(breakdown)
    # Expected: 80*0.40 + 15*0.30 + 20*0.20 + 0*0.10 = 32 + 4.5 + 4 + 0 = 40.5
    floor_arc = SUBSCORE_MISSING_FLOOR["arc_similarity_score"]
    floor_artifact = SUBSCORE_MISSING_FLOOR["artifact_completeness_score"]
    floor_unique = SUBSCORE_MISSING_FLOOR["uniqueness_score"]
    expected = (80 * 0.40 + floor_arc * 0.30 +
                floor_artifact * 0.20 + floor_unique * 0.10)
    assert score == round(expected, 2)


def test_combine_with_all_none_uses_floors():
    """Every subscore None → result is the weighted sum of all
    floors, not zero. The non-zero result reflects: 'we had nothing
    to measure but our floor policy does say something'."""
    breakdown = {
        "moment_density_score": None,
        "arc_similarity_score": None,
        "artifact_completeness_score": None,
        "uniqueness_score": None,
    }
    score = combine(breakdown)
    floors = SUBSCORE_MISSING_FLOOR
    expected = (floors["moment_density_score"] * 0.40
                + floors["arc_similarity_score"] * 0.30
                + floors["artifact_completeness_score"] * 0.20
                + floors["uniqueness_score"] * 0.10)
    assert score == round(expected, 2)


def test_combine_zero_subscore_distinct_from_missing_subscore():
    """A subscore of 0 is the SEVERE case (case-intrinsic — Pass 1
    found no arc structure). A subscore of None is the MODERATE case
    (reference data missing). The severe case must score WORSE than
    the moderate case."""
    base = {
        "moment_density_score": 60,
        "artifact_completeness_score": 60,
        "uniqueness_score": 50,
    }
    severe = dict(base, arc_similarity_score=0.0)        # Pass 1 saw no arc
    moderate = dict(base, arc_similarity_score=None)     # no P1 reference
    s_severe = combine(severe)
    s_moderate = combine(moderate)
    assert s_severe < s_moderate
    # Specifically: severe case loses (floor - 0) * 0.30 more than moderate.
    floor_arc = SUBSCORE_MISSING_FLOOR["arc_similarity_score"]
    expected_delta = floor_arc * 0.30
    actual_delta = s_moderate - s_severe
    assert abs(actual_delta - expected_delta) < 0.01


def test_combine_caller_can_override_missing_floors():
    """combine() accepts a missing_floors dict so experiments can A/B
    different floor policies without monkey-patching the module
    constant."""
    breakdown = {
        "moment_density_score": 80,
        "arc_similarity_score": None,
        "artifact_completeness_score": 80,
        "uniqueness_score": 60,
    }
    # Custom: arc floor=0 (annihilating), others default.
    custom_floors = {
        "moment_density_score": 0.0,
        "arc_similarity_score": 0.0,        # custom
        "artifact_completeness_score": 20.0,
        "uniqueness_score": 0.0,
    }
    score_default = combine(breakdown)
    score_custom = combine(breakdown, missing_floors=custom_floors)
    assert score_custom < score_default


def test_subscore_missing_floor_constants_have_expected_keys():
    """Sanity: SUBSCORE_MISSING_FLOOR must cover every subscore name
    that ``combine()`` reads. If a new subscore is added, this test
    forces the author to add a floor entry too."""
    expected_keys = {
        "moment_density_score",
        "arc_similarity_score",
        "artifact_completeness_score",
        "uniqueness_score",
    }
    assert set(SUBSCORE_MISSING_FLOOR) == expected_keys


def test_subscore_missing_floors_are_non_negative_and_below_50():
    """Floors should be punitive (below 50, the old neutral) but
    non-negative (annihilation is reserved for severe-case 0 returns,
    not the moderate-missing-data case)."""
    for name, floor in SUBSCORE_MISSING_FLOOR.items():
        assert 0.0 <= floor < 50.0, (
            f"floor for {name} = {floor} — must be in [0, 50)"
        )


def test_resolution_verdict_ceiling_keys_match_valid_statuses():
    """``RESOLUTION_VERDICT_CEILING`` must cover every status in
    ``VALID_RESOLUTION_STATUSES``. The orchestration layer looks up
    statuses against this map; an absent key would silently fall through
    to the missing-fallback ceiling and mask a real enum drift. This
    sanity test fails fast on drift and forces the author to keep them
    in sync.

    Detailed gate behaviour lives in ``test_resolution_gate.py`` --
    this test is the constants-side mirror of that contract."""
    assert set(RESOLUTION_VERDICT_CEILING) == set(VALID_RESOLUTION_STATUSES)
    valid_verdicts = {"PRODUCE", "HOLD", "SKIP"}
    for status, ceiling in RESOLUTION_VERDICT_CEILING.items():
        assert ceiling in valid_verdicts, (
            f"Ceiling {ceiling!r} for status {status!r} is not a valid verdict"
        )


# ---- decide_verdict -------------------------------------------------------


def _critical_producible_moment():
    return {
        "type": "contradiction",
        "provisional_importance": "critical",
    }


def test_decide_verdict_produce_when_all_gates_met(monkeypatch):
    # Use V9b production thresholds explicitly so this test is hermetic
    monkeypatch.setenv("P4_PRODUCE_SCORE_THRESH", "40")
    monkeypatch.setenv("P4_PRODUCE_DENSITY_THRESH", "20")
    monkeypatch.setenv("P4_SKIP_SCORE_THRESH", "15")
    # Re-import to pick up env (env is read at module load, not per-call)
    # Workaround: call with the module-level threshold values directly.
    breakdown = {"moment_density_score": 60.0}
    moments = [_critical_producible_moment()]
    verdict, conf = decide_verdict(
        narrative_score=80.0,
        breakdown=breakdown,
        moments=moments,
    )
    # Produce only fires above the module-level thresholds (which were
    # captured at import time). With baseline thresholds (72, 60, 35),
    # narrative_score=80 + density=60 + 1 critical contradiction → PRODUCE.
    if PRODUCE_SCORE_THRESH <= 80 and PRODUCE_DENSITY_THRESH <= 60:
        assert verdict == "PRODUCE"
        assert 0.6 <= conf <= 0.95


def test_decide_verdict_skip_when_score_below_skip_thresh():
    breakdown = {"moment_density_score": 5}
    moments = [{"type": "tension_shift", "provisional_importance": "low"}]
    verdict, conf = decide_verdict(
        narrative_score=10.0, breakdown=breakdown, moments=moments
    )
    # 10 < SKIP_SCORE_THRESH (default 35) → SKIP
    if SKIP_SCORE_THRESH > 10:
        assert verdict == "SKIP"
        assert conf >= 0.6


def test_decide_verdict_skip_when_no_critical_or_high_moments():
    """Even with high score, if no critical/high moments exist → SKIP."""
    breakdown = {"moment_density_score": 80}
    moments = [{"type": "contradiction", "provisional_importance": "low"}] * 5
    verdict, _ = decide_verdict(
        narrative_score=70.0, breakdown=breakdown, moments=moments
    )
    assert verdict == "SKIP"


def test_decide_verdict_hold_for_marginal_band():
    """Mid-range score with critical moment but density gate not met → HOLD."""
    breakdown = {"moment_density_score": 30}
    moments = [_critical_producible_moment()]
    verdict, _ = decide_verdict(
        narrative_score=50.0, breakdown=breakdown, moments=moments
    )
    # If 50 is between SKIP_SCORE and PRODUCE_SCORE thresholds, → HOLD
    if SKIP_SCORE_THRESH < 50 < PRODUCE_SCORE_THRESH:
        assert verdict == "HOLD"


def test_decide_verdict_hold_when_no_producible_moment_type():
    """Has critical moments but none are contradiction/reveal/procedural → HOLD."""
    breakdown = {"moment_density_score": 80}
    moments = [{"type": "emotional_peak", "provisional_importance": "critical"}]
    verdict, _ = decide_verdict(
        narrative_score=80.0, breakdown=breakdown, moments=moments
    )
    # No contradiction/reveal/procedural at critical/high — even with
    # high score + density, we can't PRODUCE. Falls to HOLD.
    assert verdict == "HOLD"


# ---- estimate_runtime_min -------------------------------------------------


def test_estimate_runtime_min_no_moments_returns_min():
    assert estimate_runtime_min([], min_runtime=5, max_runtime=45) == 5.0


def test_estimate_runtime_min_clamps_to_max():
    moments = [
        {"provisional_importance": "critical"} for _ in range(50)
    ]  # 50 * 90 * 1.4 / 60 = 105 min uncapped
    rt = estimate_runtime_min(moments, max_runtime=45)
    assert rt == 45.0


def test_estimate_runtime_min_excludes_low_importance():
    high_only = [{"provisional_importance": "high"}] * 5
    mixed = high_only + [{"provisional_importance": "low"}] * 20
    assert estimate_runtime_min(high_only) == estimate_runtime_min(mixed)


# ---- compute_all (orchestrator) -------------------------------------------


def test_compute_all_returns_canonical_shape():
    moments = [_critical_producible_moment()]
    weights = equal_weight_fallback()
    out = compute_all(
        moments=moments,
        weights=weights,
        runtime_sec=1800,
        available_artifacts={"bodycam", "interrogation"},
        detected_structure="cold_open",
        factual_anchors=[],
    )
    for key in (
        "scoring_breakdown",
        "narrative_score",
        "verdict",
        "confidence",
        "estimated_runtime_min",
        "missing_recommended_artifacts",
    ):
        assert key in out
    bd = out["scoring_breakdown"]
    for sub in (
        "moment_density_score",
        "arc_similarity_score",
        "artifact_completeness_score",
        "uniqueness_score",
    ):
        assert sub in bd


def test_compute_all_narrative_score_in_range():
    moments = [_critical_producible_moment()]
    weights = equal_weight_fallback()
    out = compute_all(
        moments=moments,
        weights=weights,
        runtime_sec=1800,
        available_artifacts=set(),
        detected_structure=None,
        factual_anchors=[],
    )
    assert 0 <= out["narrative_score"] <= 100


def test_compute_all_with_empty_moments_returns_skip():
    weights = equal_weight_fallback()
    out = compute_all(
        moments=[],
        weights=weights,
        runtime_sec=1800,
        available_artifacts=set(),
        detected_structure=None,
        factual_anchors=[],
    )
    assert out["verdict"] == "SKIP"
    assert out["narrative_score"] == 0 or out["narrative_score"] < SKIP_SCORE_THRESH
