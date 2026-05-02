"""
scoring_math.py — Deterministic scoring logic for Pipeline 4.

Pure Python, no LLM calls. The numeric narrative_score comes from here,
NOT from the LLM, so the <30% PRODUCE target can be enforced reliably
via fixed thresholds. Pass 2 is trusted for labels and reasoning;
Python is trusted for numbers.

Tunable via env vars (for A/B experiments):
    P4_REFERENCE_DENSITY       default 0.6   (weighted moments/min baseline)
    P4_PRODUCE_SCORE_THRESH    default 72    (min narrative_score for PRODUCE)
    P4_PRODUCE_DENSITY_THRESH  default 60    (min density subscore for PRODUCE)
    P4_SKIP_SCORE_THRESH       default 35    (below = SKIP)


MISSING-EVIDENCE PENALTY POLICY
-------------------------------

This scorer is editorially ruthless: a case missing narrative-critical
evidence (arc fit, artifact completeness, moment density) MUST score
lower than an otherwise-identical case where that evidence is present.
We do NOT redistribute the weight of a missing subscore onto the
remaining subscores — that would silently let weak cases score as well
as complete cases.

Two distinct missing-states are handled differently:

1. **Severe / case-intrinsic**: the case itself lacks the data
   - Pass 1 couldn't infer a narrative structure → arc_similarity=0
   - Case has no available artifacts → artifact_completeness=0
   - Case has no extractable moments → moment_density=0, uniqueness=0
   These return **0** directly from the subscore function. The full
   subscore weight is at risk.

2. **Moderate / reference-data unavailable**: we can't measure
   - No P1 arc_patterns loaded → arc_similarity=None
   - No P1 artifact_value loaded → artifact_completeness=None
   These return **None** and ``combine()`` substitutes a per-subscore
   floor from ``SUBSCORE_MISSING_FLOOR``. The floor is non-zero so a
   sanity-check run without P1 weights still produces a usable
   relative ranking, but it suppresses these cases below cases scored
   with full reference data.

The floors are defined as module-level constants so the policy is
auditable and tunable. Increasing the floor makes "no-reference-data"
runs less harsh; decreasing it makes them more punitive.
"""

import math
import os
from typing import Optional

# Env-var-tunable constants (module-level so experiments can override at shell level)
REFERENCE_DENSITY = float(os.environ.get("P4_REFERENCE_DENSITY", "0.6"))
PRODUCE_SCORE_THRESH = float(os.environ.get("P4_PRODUCE_SCORE_THRESH", "72"))
PRODUCE_DENSITY_THRESH = float(os.environ.get("P4_PRODUCE_DENSITY_THRESH", "60"))
SKIP_SCORE_THRESH = float(os.environ.get("P4_SKIP_SCORE_THRESH", "35"))


# Importance multipliers — how much each importance level contributes to density
IMPORTANCE_MULTIPLIER = {
    "critical": 1.0,
    "high": 0.7,
    "medium": 0.4,
    "low": 0.15,
}

# Sub-score weights — how the 4 sub-scores combine into narrative_score
DEFAULT_SUBSCORE_WEIGHTS = (0.40, 0.30, 0.20, 0.10)  # density, arc, artifact, uniqueness


# Per-subscore floors applied by `combine` when a subscore returns
# `None` (i.e., we lack the reference data needed to measure it). The
# floor is the value that the missing subscore contributes, scaled by
# its DEFAULT_SUBSCORE_WEIGHTS share. Choices reflect editorial
# severity — see the "MISSING-EVIDENCE PENALTY POLICY" docstring at
# the top of this module.
#
# These are floors, not neutrals: a case missing reference data scores
# *below* a case where the subscore actually computed to a higher
# value, but *above* a case whose intrinsic data is so poor that the
# subscore returned a real low number.
SUBSCORE_MISSING_FLOOR = {
    "moment_density_score": 0.0,        # never None in practice — listed for completeness
    "arc_similarity_score": 15.0,       # moderate: no P1 arc_patterns reference data
    "artifact_completeness_score": 20.0,  # moderate: no P1 artifact_value reference data
    "uniqueness_score": 0.0,            # never None in practice
}

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
    reference_density: Optional[float] = None,
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

    if reference_density is None:
        reference_density = REFERENCE_DENSITY

    moment_weights = weights.get("moment_weights", {})
    runtime_min = runtime_sec / 60.0
    # Clamp the runtime divisor to [20, 50] min. Prevents tiny clips from
    # gaming the ratio and stops long interrogations from being over-penalized
    # for length — what matters is that 20-40 strong moments exist, not
    # whether they're in 30min or 120min.
    effective_runtime_min = max(20.0, min(50.0, runtime_min))
    weighted_sum = 0.0

    for m in moments:
        mtype = m.get("moment_type") or m.get("type", "")
        importance = m.get("importance") or m.get("provisional_importance", "medium")
        type_weight = moment_weights.get(mtype, 0)
        imp_mult = IMPORTANCE_MULTIPLIER.get(importance, 0.4)
        weighted_sum += type_weight * imp_mult

    density = weighted_sum / effective_runtime_min
    # Normalize: reference_density → 60. Cap at 100.
    score = (density / reference_density) * 60
    return max(0.0, min(100.0, round(score, 2)))


def arc_similarity_score(detected_structure: Optional[str], arc_patterns: list) -> Optional[float]:
    """
    Score the arc match against winner patterns. Returns 0..100 when
    measurable, ``0`` when the case itself lacks an inferable arc, or
    ``None`` when we lack reference data to measure against.

    Two distinct return paths for missing-ness reflect the editorial
    severity policy (see module docstring):

    - **Severe / case-intrinsic**: ``detected_structure`` is None
      means Pass 1 couldn't identify any narrative shape in the
      transcript. The case lacks visible arc — penalize directly with
      ``0``. The full arc-subscore weight (default 30 %) is at risk.

    - **Moderate / reference-data missing**: ``arc_patterns`` is
      empty (no P1 winner patterns loaded), or all patterns produce
      zero score. We can't compare to winners. Return ``None`` so
      ``combine()`` substitutes ``SUBSCORE_MISSING_FLOOR``
      (currently 15) — a punitive floor, but not annihilating.

    - **Measurable**: at least one matching pattern exists with a
      positive frequency × log(avg_views) score. Return
      ``(matched_score / best_score) * 100``.
    """
    if not detected_structure:
        # Severe: Pass 1 could not infer narrative structure.
        # The case lacks a discernible arc → explicit 0, not None.
        return 0.0

    if not arc_patterns:
        # Moderate: we lack P1 reference data to compare against.
        # combine() applies SUBSCORE_MISSING_FLOOR.
        return None

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
        # Patterns exist but none yields a comparable score → no
        # informative ranking → moderate missing (None → floor).
        return None
    return round((matched_score / best_score) * 100, 2)


def artifact_completeness_score(
    available: set,
    artifact_value: dict,
) -> tuple:
    """
    Find the highest-valued artifact combo whose artifact set is a
    subset of available. Returns ``(score, missing_recommended_list)``
    where ``score`` is 0..100 when measurable, or ``None`` when we
    lack reference data.

    Missing-state behaviour (mirrors the policy in
    ``arc_similarity_score`` — see module docstring):

    - **Moderate / reference-data missing**: ``artifact_value`` is
      empty (no P1 winner-combo reference loaded). We can't measure
      completeness against winning combos. Return ``(None, [])`` so
      ``combine()`` applies ``SUBSCORE_MISSING_FLOOR``
      (currently 20).

    - **Severe / case-intrinsic**: ``available`` is empty (the case
      itself has no artifacts at all). The partial-credit path below
      returns 0 directly — full subscore weight is lost as it should
      be.
    """
    if not artifact_value:
        # Moderate: no P1 reference data — combine() applies
        # SUBSCORE_MISSING_FLOOR. This is NOT a neutral 50.0 — that
        # would silently let "no data" cases score as well as
        # measurable mid-range cases.
        return None, []

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
    missing_floors: Optional[dict] = None,
) -> float:
    """
    Combine the 4 sub-scores into a final narrative_score (0..100).
    weights order: (density, arc, artifact, uniqueness).

    Missing-evidence policy: when a subscore is ``None`` (i.e., we
    lack the reference data needed to measure it), this function
    substitutes the per-subscore floor from ``SUBSCORE_MISSING_FLOOR``
    (or the caller-supplied ``missing_floors`` dict). The full subscore
    weight is then applied to that floor — we do NOT redistribute the
    weight onto present subscores. That would silently let weak cases
    score as well as complete cases, which is editorially wrong.

    See the "MISSING-EVIDENCE PENALTY POLICY" docstring at the top of
    this module for the rationale.
    """
    floors = missing_floors if missing_floors is not None else SUBSCORE_MISSING_FLOOR

    def _resolve(name, idx):
        v = breakdown.get(name)
        if v is None:
            return float(floors.get(name, 0.0))
        return float(v)

    md = _resolve("moment_density_score", 0)
    asim = _resolve("arc_similarity_score", 1)
    ac = _resolve("artifact_completeness_score", 2)
    un = _resolve("uniqueness_score", 3)
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

    # Thresholds (env-var tunable — see module header)
    PRODUCE_SCORE = PRODUCE_SCORE_THRESH
    PRODUCE_DENSITY = PRODUCE_DENSITY_THRESH
    SKIP_SCORE = SKIP_SCORE_THRESH

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


# ---------------------------------------------------------------------------
# Resolution-status verdict-ceiling gate
# ---------------------------------------------------------------------------
#
# Production-readiness gate applied at the orchestration layer AFTER the
# four-subscore math and AFTER LLM/deterministic verdict reconciliation.
# It does NOT alter narrative_score; it only caps the emitted verdict
# based on whether the case has a court-final disposition. Editorial
# doctrine: a case missing final criminal/court disposition is not
# production-ready, regardless of how strong its narrative metrics are.
#
# Pure functions only. The caller (pipeline4_score.score_case) reads the
# P4_RESOLUTION_GATE env var and passes the resulting bool as
# ``gate_enabled`` -- this module never touches the environment so the
# gate stays trivially testable.
#
# See ARCHITECTURE.md "Resolution gate" for the full doctrine.

VALID_RESOLUTION_STATUSES = (
    "confirmed_final_outcome",
    "charges_filed_pending",
    "ongoing_or_unclear",
    "missing",
)

# Maximum verdict each resolution_status permits. The gate caps a
# reconciled verdict at the listed ceiling -- never upgrades.
RESOLUTION_VERDICT_CEILING = {
    "confirmed_final_outcome": "PRODUCE",
    "charges_filed_pending": "HOLD",
    "ongoing_or_unclear": "SKIP",
    "missing": "SKIP",
}

# Severity ordering used to compare a verdict against its ceiling.
# PRODUCE is strongest, SKIP is weakest. A verdict whose rank is <=
# the ceiling rank is already at-or-below the ceiling and passes
# through unchanged.
_VERDICT_RANK = {"SKIP": 0, "HOLD": 1, "PRODUCE": 2}


def apply_resolution_gate(
    verdict: str,
    resolution_status: Optional[str],
    gate_enabled: bool = False,
) -> tuple:
    """
    Cap ``verdict`` at the ceiling permitted by ``resolution_status``.

    Pure function -- no env reads. The orchestration layer is responsible
    for reading ``P4_RESOLUTION_GATE`` and passing the resulting bool as
    ``gate_enabled``.

    Returns ``(emitted_verdict, gate_applied)``:
      * ``emitted_verdict`` -- the verdict after the ceiling has been
        applied (or the unchanged input if the gate is disabled or the
        verdict is already at-or-below the ceiling).
      * ``gate_applied`` -- True iff the ceiling actually changed the
        verdict; False otherwise (gate off, no-op clamp, or status
        unrecognized but verdict already at-or-below the missing-fallback
        ceiling).

    Behaviour:
      * gate_enabled=False -> returns (verdict, False) unchanged
      * resolution_status is None or not in VALID_RESOLUTION_STATUSES
        -> treated as "missing" (fail-closed) so unknown / mis-typed
        statuses cannot accidentally promote a case
      * verdict already at-or-below ceiling -> returns (verdict, False)
      * verdict above ceiling -> returns (ceiling, True)
    """
    if not gate_enabled:
        return verdict, False

    if resolution_status not in RESOLUTION_VERDICT_CEILING:
        # Unknown / None status -> fail closed under the missing ceiling.
        ceiling = RESOLUTION_VERDICT_CEILING["missing"]
    else:
        ceiling = RESOLUTION_VERDICT_CEILING[resolution_status]

    if _VERDICT_RANK.get(verdict, 0) <= _VERDICT_RANK[ceiling]:
        return verdict, False

    return ceiling, True


# ---------------------------------------------------------------------------
# Advisory production-status flag (default doctrine)
# ---------------------------------------------------------------------------
#
# RESOLUTION_PRODUCTION_FLAG translates resolution_status into a
# human-friendly production note. This is the DEFAULT advisory output --
# not a verdict ceiling, not a pass/fail. Pipeline 5 / production teams
# read this field as a short string ("pending_case_review", etc.) and
# decide downstream how to handle the case. It does NOT alter
# narrative_score or verdict.
#
# The resolution gate (apply_resolution_gate above) remains available
# as an OPTIONAL conservative mode via P4_RESOLUTION_GATE=1, but it is
# NOT the default scoring path. Default doctrine: adjudication status
# is advisory metadata, not determinant.
#
# None for confirmed cases means "no flag needed -- case is ready".
# All other entries are short strings consumers can display as warnings.

RESOLUTION_PRODUCTION_FLAG = {
    "confirmed_final_outcome": None,
    "charges_filed_pending": "pending_case_review",
    "ongoing_or_unclear": "ongoing_status_review",
    "missing": "resolution_unknown",
}


def production_status_flag(resolution_status):
    """Map ``resolution_status`` to a human-friendly production flag.

    Pure function -- the inverse of "verdict ceiling": this never
    changes the verdict, only annotates it. ``None`` for confirmed
    cases means "no flag needed". Unknown / ``None`` / invalid statuses
    fail closed to ``"resolution_unknown"`` so consumers always see a
    recognizable advisory string instead of ``None`` for unexpected
    input.

    See ARCHITECTURE.md "Resolution gate / advisory flag" for the full
    doctrine.
    """
    if resolution_status not in RESOLUTION_PRODUCTION_FLAG:
        return "resolution_unknown"
    return RESOLUTION_PRODUCTION_FLAG[resolution_status]
