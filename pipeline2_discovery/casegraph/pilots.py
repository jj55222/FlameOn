"""PILOT2 — Pure no-live runner for the CaseGraph pilot manifest.

Reads ``tests/fixtures/pilot_cases/pilot_manifest.json`` (or any
manifest with the same shape) and assesses each pilot's readiness for
a future live-smoke run, WITHOUT making any network call.

For each pilot the runner:

- loads the seed fixture and scores it via ``score_case_packet``
  (dry mode, no live); when no seed fixture is given, the dry verdict
  is reported as ``None``
- enumerates which CaseGraph gates the seed already satisfies
  (identity_high, concluded_outcome, media_artifact_present) and
  which it does not (``missing_gates``)
- validates pilot policy: paid connectors, downloads, scraping, LLM
  use, and ``media_required_for_produce`` are all forbidden by
  default (``policy_violations``)
- validates pilot budget against the live_safety hard caps
  (max_results_per_connector <= MAX_RESULTS_HARD_CAP,
  max_live_calls <= MAX_QUERIES_HARD_CAP * MAX_CONNECTORS_HARD_CAP,
  len(allowed_connectors) <= MAX_CONNECTORS_HARD_CAP) — anything
  outside the envelope produces a ``budget_violations`` entry
- assigns a single readiness status:
  ``ready_for_live_smoke``,
  ``blocked_missing_fixture``,
  ``blocked_policy``,
  ``blocked_invalid_budget``,
  or ``blocked_verdict_drift``
- emits ``next_actions`` describing what the operator should do to
  unblock or proceed

The runner does not authorize any live work — its sole purpose is to
declare which pilots are SAFE to graduate to a controlled live smoke
under their declared budget.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .assembly import assemble_structured_case_packet
from .cli import _load_fixture, _parse_structured_fixture
from .live_safety import (
    ALLOWED_FREE_CONNECTORS,
    MAX_QUERIES_HARD_CAP,
    MAX_RESULTS_HARD_CAP,
    PAID_CONNECTORS,
)
from .live_smoke import MAX_CONNECTORS_HARD_CAP
from .models import CasePacket
from .scoring import MEDIA_ARTIFACT_TYPES, MEDIA_FORMATS, score_case_packet


CONCLUDED_OUTCOMES = frozenset({"sentenced", "closed", "convicted"})
MAX_LIVE_CALLS_ENVELOPE = MAX_QUERIES_HARD_CAP * MAX_CONNECTORS_HARD_CAP


READINESS_READY = "ready_for_live_smoke"
READINESS_MISSING_FIXTURE = "blocked_missing_fixture"
READINESS_POLICY = "blocked_policy"
READINESS_BUDGET = "blocked_invalid_budget"
READINESS_VERDICT_DRIFT = "blocked_verdict_drift"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_seed_packet(seed_path: Path) -> CasePacket:
    """Load a pilot seed fixture as a CasePacket. Dispatches by shape:
    CasePacket fixtures (carrying ``case_id`` + ``case_identity``) load
    via the CLI loader directly; structured-row fixtures (carrying
    ``dataset``) are parsed + assembled into an empty-source packet so
    scoring can run uniformly."""
    with seed_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    if isinstance(raw, dict) and "case_id" in raw and "case_identity" in raw:
        return _load_fixture(seed_path)
    if isinstance(raw, dict) and ("dataset" in raw or "dataset_name" in raw):
        parsed = _parse_structured_fixture(seed_path)
        return assemble_structured_case_packet(parsed).packet
    # Fall back to CasePacket loader; will raise a clear error if neither shape.
    return _load_fixture(seed_path)


def _seed_packet_gates(packet: CasePacket) -> Tuple[Dict[str, bool], List[str]]:
    """Return (satisfied_gates, missing_gates) for a seed CasePacket.

    The three gates checked here are the same gates the scorer enforces
    for PRODUCE: high identity, concluded outcome, and at least one
    media VerifiedArtifact."""
    identity_high = packet.case_identity.identity_confidence == "high"
    outcome_concluded = packet.case_identity.outcome_status in CONCLUDED_OUTCOMES
    has_media = any(
        a.artifact_type in MEDIA_ARTIFACT_TYPES or a.format in MEDIA_FORMATS
        for a in packet.verified_artifacts
    )
    satisfied = {
        "identity_high": identity_high,
        "concluded_outcome": outcome_concluded,
        "media_artifact_present": has_media,
    }
    missing = sorted(name for name, ok in satisfied.items() if not ok)
    return satisfied, missing


def _policy_violations(pilot: Mapping[str, Any]) -> List[str]:
    violations: List[str] = []
    paid = set(PAID_CONNECTORS)
    listed = set(pilot.get("allowed_connectors") or [])
    leaked_paid = sorted(listed & paid)
    if leaked_paid:
        violations.append(f"paid_connectors_listed:{','.join(leaked_paid)}")
    if pilot.get("allow_downloads"):
        violations.append("allow_downloads_true")
    if pilot.get("allow_scraping"):
        violations.append("allow_scraping_true")
    if pilot.get("allow_llm"):
        violations.append("allow_llm_true")
    expected_minimum = pilot.get("expected_minimum") or {}
    if not expected_minimum.get("media_required_for_produce", False):
        violations.append("media_required_for_produce_false")
    unknown = sorted(c for c in listed if c not in ALLOWED_FREE_CONNECTORS and c not in paid)
    if unknown:
        violations.append(f"unknown_connectors_listed:{','.join(unknown)}")
    return violations


def _budget_violations(pilot: Mapping[str, Any]) -> List[str]:
    violations: List[str] = []
    max_live_calls = pilot.get("max_live_calls")
    if not isinstance(max_live_calls, int) or max_live_calls < 0:
        violations.append("max_live_calls_invalid_type")
    elif max_live_calls > MAX_LIVE_CALLS_ENVELOPE:
        violations.append(
            f"max_live_calls_over_envelope:{max_live_calls}>{MAX_LIVE_CALLS_ENVELOPE}"
        )
    max_results = pilot.get("max_results_per_connector")
    if not isinstance(max_results, int) or max_results <= 0:
        violations.append("max_results_per_connector_invalid_type")
    elif max_results > MAX_RESULTS_HARD_CAP:
        violations.append(
            f"max_results_per_connector_over_cap:{max_results}>{MAX_RESULTS_HARD_CAP}"
        )
    listed = list(pilot.get("allowed_connectors") or [])
    if len(listed) > MAX_CONNECTORS_HARD_CAP:
        violations.append(
            f"allowed_connectors_over_cap:{len(listed)}>{MAX_CONNECTORS_HARD_CAP}"
        )
    return violations


def _next_actions(
    *,
    readiness: str,
    pilot: Mapping[str, Any],
    missing_gates: List[str],
    policy_violations: List[str],
    budget_violations: List[str],
) -> List[str]:
    actions: List[str] = []
    if readiness == READINESS_READY:
        actions.append("schedule_live_smoke_under_declared_budget")
        for gate in missing_gates:
            actions.append(f"target_gate_via_live:{gate}")
    elif readiness == READINESS_MISSING_FIXTURE:
        actions.append(
            f"create_or_correct_seed_fixture:{pilot.get('seed_fixture_path')!r}"
        )
    elif readiness == READINESS_POLICY:
        for v in policy_violations:
            actions.append(f"resolve_policy_violation:{v}")
    elif readiness == READINESS_BUDGET:
        for v in budget_violations:
            actions.append(f"resolve_budget_violation:{v}")
    elif readiness == READINESS_VERDICT_DRIFT:
        actions.append(
            "investigate_verdict_drift_between_pilot_expected_and_dry_score"
        )
    return actions


def assess_pilot(
    pilot: Mapping[str, Any],
    *,
    repo_root: Optional[Path] = None,
) -> Dict[str, Any]:
    """Assess a single pilot manifest entry. Pure no-live."""
    if repo_root is None:
        repo_root = _repo_root()

    seed_rel = pilot.get("seed_fixture_path")
    seed_abs: Optional[Path] = (
        repo_root / str(seed_rel) if seed_rel is not None else None
    )

    expected_verdict_without_live = pilot.get("expected_verdict_without_live")
    actual_dry_verdict: Optional[str] = None
    research_score = 0.0
    production_score = 0.0
    actionability_score = 0.0
    verified_artifact_count = 0
    media_artifact_count = 0
    document_artifact_count = 0
    identity_confidence: Optional[str] = None
    outcome_status: Optional[str] = None
    missing_gates: List[str] = []
    satisfied_gates: Dict[str, bool] = {}

    fixture_present = seed_abs is not None and seed_abs.exists()
    if fixture_present:
        packet = _load_seed_packet(seed_abs)  # type: ignore[arg-type]
        result = score_case_packet(packet)
        actual_dry_verdict = result.verdict
        research_score = result.research_completeness_score
        production_score = result.production_actionability_score
        actionability_score = result.actionability_score
        verified_artifact_count = len(packet.verified_artifacts)
        media_artifact_count = sum(
            1
            for a in packet.verified_artifacts
            if a.artifact_type in MEDIA_ARTIFACT_TYPES or a.format in MEDIA_FORMATS
        )
        document_artifact_count = verified_artifact_count - media_artifact_count
        identity_confidence = packet.case_identity.identity_confidence
        outcome_status = packet.case_identity.outcome_status
        satisfied_gates, missing_gates = _seed_packet_gates(packet)

    policy_violations = _policy_violations(pilot)
    budget_violations = _budget_violations(pilot)

    if not fixture_present and seed_rel is not None:
        readiness = READINESS_MISSING_FIXTURE
    elif policy_violations:
        readiness = READINESS_POLICY
    elif budget_violations:
        readiness = READINESS_BUDGET
    elif (
        expected_verdict_without_live is not None
        and actual_dry_verdict is not None
        and actual_dry_verdict != expected_verdict_without_live
    ):
        readiness = READINESS_VERDICT_DRIFT
    else:
        readiness = READINESS_READY

    return {
        "id": pilot.get("id"),
        "input_type": pilot.get("input_type"),
        "seed_fixture_path": seed_rel,
        "expected_minimum": dict(pilot.get("expected_minimum") or {}),
        "expected_verdict_without_live": expected_verdict_without_live,
        "actual_dry_verdict": actual_dry_verdict,
        "verdict_match": (
            actual_dry_verdict == expected_verdict_without_live
            if (actual_dry_verdict is not None and expected_verdict_without_live is not None)
            else None
        ),
        "research_completeness_score": research_score,
        "production_actionability_score": production_score,
        "actionability_score": actionability_score,
        "verified_artifact_count": verified_artifact_count,
        "media_artifact_count": media_artifact_count,
        "document_artifact_count": document_artifact_count,
        "identity_confidence": identity_confidence,
        "outcome_status": outcome_status,
        "satisfied_gates": satisfied_gates,
        "missing_gates": missing_gates,
        "allowed_connectors": list(pilot.get("allowed_connectors") or []),
        "max_live_calls": pilot.get("max_live_calls"),
        "max_results_per_connector": pilot.get("max_results_per_connector"),
        "policy_violations": policy_violations,
        "budget_violations": budget_violations,
        "readiness_status": readiness,
        "next_actions": _next_actions(
            readiness=readiness,
            pilot=pilot,
            missing_gates=missing_gates,
            policy_violations=policy_violations,
            budget_violations=budget_violations,
        ),
    }


def _aggregate(results: List[Mapping[str, Any]]) -> Dict[str, Any]:
    by_status: Dict[str, int] = {}
    any_paid = False
    any_downloads = False
    any_scraping = False
    any_llm = False
    any_missing_media_required = False
    total_planned_live_calls = 0
    ready_count = 0
    blocked_count = 0

    for r in results:
        status = r.get("readiness_status") or "unknown"
        by_status[status] = by_status.get(status, 0) + 1
        if status == READINESS_READY:
            ready_count += 1
        else:
            blocked_count += 1

        for v in r.get("policy_violations") or []:
            if v.startswith("paid_connectors_listed:"):
                any_paid = True
            if v == "allow_downloads_true":
                any_downloads = True
            if v == "allow_scraping_true":
                any_scraping = True
            if v == "allow_llm_true":
                any_llm = True
            if v == "media_required_for_produce_false":
                any_missing_media_required = True

        try:
            total_planned_live_calls += int(r.get("max_live_calls") or 0)
        except (TypeError, ValueError):
            pass

    return {
        "total_pilots": len(results),
        "ready_count": ready_count,
        "blocked_count": blocked_count,
        "by_readiness_status": dict(sorted(by_status.items())),
        "any_paid_connectors": any_paid,
        "any_downloads_enabled": any_downloads,
        "any_scraping_enabled": any_scraping,
        "any_llm_enabled": any_llm,
        "any_missing_media_required": any_missing_media_required,
        "total_planned_live_calls": total_planned_live_calls,
    }


# Connectors with documented yield against structured-row queries on
# the validation/pilot fixture corpus, ranked highest-yield first. The
# selector prefers pilots that include any of these.
PROVEN_ARTIFACT_CONNECTORS = ("documentcloud",)


def _selection_score(pilot_result: Mapping[str, Any]) -> int:
    """Deterministic score for picking the best ready pilot for the
    first live attempt. Higher is better. Returns ``-1`` for any
    pilot that is not ``ready_for_live_smoke`` so it can never win."""
    if pilot_result.get("readiness_status") != "ready_for_live_smoke":
        return -1
    score = 100
    allowed = list(pilot_result.get("allowed_connectors") or [])
    if any(c in PROVEN_ARTIFACT_CONNECTORS for c in allowed):
        score += 30
    score -= 2 * max(0, len(allowed) - 1)
    try:
        score -= 5 * max(0, int(pilot_result.get("max_live_calls") or 0) - 1)
    except (TypeError, ValueError):
        score -= 100
    expected = pilot_result.get("expected_verdict_without_live")
    if expected == "HOLD":
        score += 2
    elif expected == "PRODUCE":
        score += 1
    return score


def select_pilot_for_live_smoke(
    *,
    manifest_path: Optional[Path] = None,
    pilot_output: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Pure selection for the first known-case live-smoke attempt
    toward Endpoint v0.

    Selection criteria (deterministic):

    1. ``readiness_status == 'ready_for_live_smoke'`` (blocked pilots
       can never be selected)
    2. lowest ``max_live_calls`` (cheapest first)
    3. fewest connectors (prefers single-connector pilots)
    4. has at least one connector in :data:`PROVEN_ARTIFACT_CONNECTORS`
       (currently ``documentcloud``)
    5. tiebreaker by alphabetical pilot id for determinism

    No network calls. Returns a dict with the selected pilot's id,
    seed_fixture_path, allowed_connectors, max_live_calls,
    max_results_per_connector, expected_verdict_without_live,
    actual_dry_verdict, missing_gates, selection_score, the full
    selected per-pilot result dict, plus rationale (a short list of
    human-readable strings) and a ``no_ready_pilot`` flag when
    nothing is selectable.
    """
    if pilot_output is None:
        pilot_output = run_pilot_manifest(manifest_path)

    results = list(pilot_output.get("results") or [])
    ready = [r for r in results if r.get("readiness_status") == "ready_for_live_smoke"]
    if not ready:
        return {
            "selected_pilot_id": None,
            "no_ready_pilot": True,
            "candidate_count": 0,
            "rationale": [
                "no pilots in ready_for_live_smoke status; nothing to select"
            ],
        }

    ranked = sorted(
        ready,
        key=lambda r: (-_selection_score(r), str(r.get("id") or "")),
    )
    selected = ranked[0]
    selection_score = _selection_score(selected)

    rationale: List[str] = []
    rationale.append(
        f"selected {selected['id']!r} with score {selection_score} "
        f"out of {len(ready)} ready candidate(s)"
    )
    if any(c in PROVEN_ARTIFACT_CONNECTORS for c in selected.get("allowed_connectors") or []):
        rationale.append(
            "uses a connector in PROVEN_ARTIFACT_CONNECTORS "
            f"({', '.join(PROVEN_ARTIFACT_CONNECTORS)}) - higher artifact-yield expectation"
        )
    rationale.append(
        f"max_live_calls={selected.get('max_live_calls')}, "
        f"max_results_per_connector={selected.get('max_results_per_connector')}"
    )
    rationale.append(
        f"expected_verdict_without_live={selected.get('expected_verdict_without_live')!r}; "
        f"actual_dry_verdict={selected.get('actual_dry_verdict')!r}"
    )
    if selected.get("missing_gates"):
        rationale.append(
            f"missing gates that live data may help fill: {selected['missing_gates']}"
        )

    return {
        "selected_pilot_id": selected["id"],
        "no_ready_pilot": False,
        "candidate_count": len(ready),
        "selection_score": selection_score,
        "fixture_path": selected.get("seed_fixture_path"),
        "input_type": selected.get("input_type"),
        "allowed_connectors": list(selected.get("allowed_connectors") or []),
        "max_live_calls": selected.get("max_live_calls"),
        "max_results_per_connector": selected.get("max_results_per_connector"),
        "expected_verdict_without_live": selected.get("expected_verdict_without_live"),
        "actual_dry_verdict": selected.get("actual_dry_verdict"),
        "expected_minimum": dict(selected.get("expected_minimum") or {}),
        "missing_gates": list(selected.get("missing_gates") or []),
        "selected_pilot_result": dict(selected),
        "rationale": rationale,
    }


def run_pilot_manifest(
    manifest_path: Optional[Path] = None,
    *,
    manifest_dict: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Run every pilot through the no-live readiness assessment.

    Pass ``manifest_path`` to load from disk, or ``manifest_dict`` to
    pass a pre-loaded dict (useful in tests). Pure: no network, no LLM.
    """
    resolved_path: Optional[Path] = None
    if manifest_dict is None:
        if manifest_path is None:
            resolved_path = (
                _repo_root()
                / "tests"
                / "fixtures"
                / "pilot_cases"
                / "pilot_manifest.json"
            )
        else:
            resolved_path = Path(manifest_path)
        with resolved_path.open("r", encoding="utf-8") as f:
            manifest_dict = json.load(f)
    elif manifest_path is not None:
        resolved_path = Path(manifest_path)

    repo_root = _repo_root()
    pilots = list(manifest_dict.get("pilots") or [])
    results = [assess_pilot(p, repo_root=repo_root) for p in pilots]

    return {
        "manifest_path": str(resolved_path) if resolved_path is not None else None,
        "manifest_version": int(manifest_dict.get("manifest_version") or 0),
        "global_constraints": dict(manifest_dict.get("global_constraints") or {}),
        "total_pilots": len(pilots),
        "results": results,
        "summary": _aggregate(results),
    }
