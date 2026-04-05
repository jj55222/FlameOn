"""
scoring_math.py — Deterministic scoring logic for Pipeline 4.

Pure Python, no LLM calls. The numeric narrative_score comes from here,
NOT from the LLM, so the <30% PRODUCE target can be enforced reliably
via fixed thresholds. Pass 2 is trusted for labels and reasoning;
Python is trusted for numbers.
"""

import math
from typing import Optional


# Importance multipliers — how much each importance level contributes to density
IMPORTANCE_MULTIPLIER = {
    "critical": 1.0,
    "high": 0.7,
    "medium": 0.4,
    "low": 0.15,
}

# Sub-score weights — how the 4 sub-scores combine into narrative_score
DEFAULT_SUBSCORE_WEIGHTS = (0.40, 0.30, 0.20, 0.10)  # density, arc, artifact, uniqueness

# The seven valid moment types
VALID_MOMENT_TYPES = [
    "contradiction",
    "emotional_peak",
    "procedural_violation",
    "reveal",
    "detail_noticed",
    "callback",
    "tension_shift",
]


def equal_weight_fallback() -> dict:
    """
    Equal-weight scoring when Pipeline 1 weights aren't available.
    1/7 per moment type, no arc patterns, no artifact value map.
    """
    w = 1.0 / len(VALID_MOMENT_TYPES)
    return {
        "moment_weights": {k: round(w, 4) for k in VALID_MOMENT_TYPES},
        "arc_patterns": [],
        "artifact_value": {},
        "_equal_weight_fallback": True,
    }


def moment_density_score(
    moments: list,
    weights: dict,
    runtime_sec: float,
    reference_density: float = 0.6,
) -> float:
    """
    Score the density of high-value moments per minute, weighted by type and importance.

    A "typical winner" (10 videos analyzed in Pipeline 1) averages ~10 moments per video.
    At ~50 min average runtime, that's ~0.2 moments/min raw, but with weighting and
    importance the reference_density (weighted moments per minute) is ~0.6.

    Returns 0..100.
    """
    if not moments or runtime_sec <= 0:
        return 0.0

    moment_weights = weights.get("moment_weights", {})
    runtime_min = runtime_sec / 60.0
    weighted_sum = 0.0

    for m in moments:
        mtype = m.get("moment_type") or m.get("type", "")
        importance = m.get("importance") or m.get("provisional_importance", "medium")
        type_weight = moment_weights.get(mtype, 0)
        imp_mult = IMPORTANCE_MULTIPLIER.get(importance, 0.4)
        weighted_sum += type_weight * imp_mult

    density = weighted_sum / runtime_min
    # Normalize: reference_density → 60. Cap at 100.
    score = (density / reference_density) * 60
    return max(0.0, min(100.0, round(score, 2)))


def arc_similarity_score(detected_structure: Optional[str], arc_patterns: list) -> float:
    """
    Score the arc match against winner patterns.
    If detected structure matches a high-frequency high-view arc, score high.
    Returns 0..100.
    """
    if not detected_structure or not arc_patterns:
        return 50.0  # neutral when no data

    # Compute a score for each arc pattern: frequency × log(avg_views)
    best_score = 0.0
    matched_score = 0.0

    for pat in arc_patterns:
        freq = float(pat.get("frequency", 0))
        views = float(pat.get("avg_view_count", 1))
        # Add 1 to avg_views to avoid log(0)
        pat_score = freq * math.log10(max(views, 10) + 1)
        best_score = max(best_score, pat_score)
        if pat.get("structure_type") == detected_structure:
            matched_score = pat_score

    if best_score == 0:
        return 50.0
    return round((matched_score / best_score) * 100, 2)


def artifact_completeness_score(
    available: set,
    artifact_value: dict,
) -> tuple:
    """
    Find the highest-valued artifact combo whose artifact set is a subset of available.
    Returns (score_0_100, missing_recommended_list).

    If no artifact_value data, return neutral 50 and empty list.
    """
    if not artifact_value:
        return 50.0, []

    available = set(available)
    best_score = 0.0
    best_combo = None

    for combo_key, value in artifact_value.items():
        combo_set = set(combo_key.split("+"))
        if combo_set.issubset(available):
            if value > best_score:
                best_score = float(value)
                best_combo = combo_set

    # If no combo fits within available, partial credit based on overlap with highest combo
    if best_combo is None:
        # Find the highest-valued combo overall
        top_key = max(artifact_value.items(), key=lambda x: x[1])[0]
        top_set = set(top_key.split("+"))
        overlap = available & top_set
        if top_set:
            partial = len(overlap) / len(top_set) * float(artifact_value[top_key])
        else:
            partial = 0
        missing = list(top_set - available)
        return round(partial * 100, 2), missing

    # Find missing items from the top combo to suggest upgrades
    top_key = max(artifact_value.items(), key=lambda x: x[1])[0]
    top_set = set(top_key.split("+"))
    missing = list(top_set - available)

    return round(best_score * 100, 2), missing


def uniqueness_score(moments: list, factual_anchors: list) -> float:
    """
    Score uniqueness: distinct moment types present, bonus for multiple contradictions
    or strong procedural violations, bonus for rich factual anchors.
    Capped at 80 so it can't dominate.
    """
    if not moments:
        return 0.0

    # Count distinct moment types
    types_present = set()
    contradiction_count = 0
    procedural_count = 0
    for m in moments:
        mtype = m.get("moment_type") or m.get("type", "")
        if mtype:
            types_present.add(mtype)
        if mtype == "contradiction":
            contradiction_count += 1
        elif mtype == "procedural_violation":
            procedural_count += 1

    # Base: 10 points per distinct moment type (max 70 for 7 types)
    base = len(types_present) * 10

    # Bonuses
    if contradiction_count >= 2:
        base += 5
    if procedural_count >= 1:
        base += 3

    # Factual anchor richness bonus (caps at 2 bonus points)
    if factual_anchors:
        unique_types = len({a.get("type") for a in factual_anchors if a.get("type")})
        base += min(unique_types, 2)

    return float(min(base, 80))


def combine(
    breakdown: dict,
    weights: tuple = DEFAULT_SUBSCORE_WEIGHTS,
) -> float:
    """
    Combine the 4 sub-scores into a final narrative_score (0..100).
    weights order: (density, arc, artifact, uniqueness).
    """
    md = breakdown.get("moment_density_score", 0)
    asim = breakdown.get("arc_similarity_score", 0)
    ac = breakdown.get("artifact_completeness_score", 0)
    un = breakdown.get("uniqueness_score", 0)
    total = md * weights[0] + asim * weights[1] + ac * weights[2] + un * weights[3]
    return round(total, 2)


def decide_verdict(
    narrative_score: float,
    breakdown: dict,
    moments: list,
) -> tuple:
    """
    Precision-biased verdict decision.

    PRODUCE only if:
      - narrative_score >= 72
      - moment_density_score >= 60
      - At least one critical/high moment of type contradiction/reveal/procedural_violation

    SKIP if:
      - narrative_score < 35
      - OR zero critical/high moments

    HOLD otherwise (default).

    Returns (verdict, confidence_0_to_1).
    """
    md = breakdown.get("moment_density_score", 0)

    # Count gating moments
    critical_or_high = [
        m for m in moments
        if (m.get("importance") or m.get("provisional_importance")) in ("critical", "high")
    ]
    producible_types = {"contradiction", "reveal", "procedural_violation"}
    has_producible_critical = any(
        (m.get("moment_type") or m.get("type")) in producible_types
        and (m.get("importance") or m.get("provisional_importance")) in ("critical", "high")
        for m in moments
    )

    # Thresholds
    PRODUCE_SCORE = 72
    PRODUCE_DENSITY = 60
    SKIP_SCORE = 35

    if (
        narrative_score >= PRODUCE_SCORE
        and md >= PRODUCE_DENSITY
        and has_producible_critical
    ):
        verdict = "PRODUCE"
        # Confidence: how far above thresholds
        margin_score = (narrative_score - PRODUCE_SCORE) / (100 - PRODUCE_SCORE)
        margin_density = (md - PRODUCE_DENSITY) / (100 - PRODUCE_DENSITY)
        confidence = min(0.95, 0.6 + 0.35 * min(margin_score, margin_density))
    elif narrative_score < SKIP_SCORE or not critical_or_high:
        verdict = "SKIP"
        # Confidence: how far below thresholds
        margin = max(0, (SKIP_SCORE - narrative_score) / SKIP_SCORE)
        confidence = min(0.95, 0.6 + 0.35 * margin)
    else:
        verdict = "HOLD"
        # Confidence is lowest in the middle
        mid = (PRODUCE_SCORE + SKIP_SCORE) / 2
        distance_from_mid = abs(narrative_score - mid)
        confidence = max(0.3, 0.4 + (distance_from_mid / mid) * 0.3)

    return verdict, round(confidence, 3)


def estimate_runtime_min(
    moments: list,
    per_moment_sec: int = 90,
    min_runtime: int = 5,
    max_runtime: int = 45,
) -> float:
    """
    Estimate content video runtime in minutes.

    Each critical/high moment gets ~90s of screen time, plus narration buffer.
    Clamped to [min, max].
    """
    if not moments:
        return float(min_runtime)

    screen_moments = [
        m for m in moments
        if (m.get("importance") or m.get("provisional_importance")) in ("critical", "high", "medium")
    ]
    base_sec = len(screen_moments) * per_moment_sec
    # Add 40% narration/context buffer
    total_sec = base_sec * 1.4
    runtime_min = total_sec / 60

    return round(max(min_runtime, min(max_runtime, runtime_min)), 1)


def compute_all(
    moments: list,
    weights: dict,
    runtime_sec: float,
    available_artifacts: set,
    detected_structure: Optional[str],
    factual_anchors: Optional[list] = None,
) -> dict:
    """
    Compute all sub-scores + final narrative_score + verdict + runtime estimate.
    Single entry point for orchestration.
    """
    if factual_anchors is None:
        factual_anchors = []

    md = moment_density_score(moments, weights, runtime_sec)
    asim = arc_similarity_score(detected_structure, weights.get("arc_patterns", []))
    ac, missing = artifact_completeness_score(
        available_artifacts, weights.get("artifact_value", {})
    )
    un = uniqueness_score(moments, factual_anchors)

    breakdown = {
        "moment_density_score": md,
        "arc_similarity_score": asim,
        "artifact_completeness_score": ac,
        "uniqueness_score": un,
    }
    narrative_score = combine(breakdown)
    verdict, confidence = decide_verdict(narrative_score, breakdown, moments)
    runtime_min = estimate_runtime_min(moments)

    return {
        "scoring_breakdown": breakdown,
        "narrative_score": narrative_score,
        "verdict": verdict,
        "confidence": confidence,
        "estimated_runtime_min": runtime_min,
        "missing_recommended_artifacts": missing,
    }
