"""EVAL1 — Production metrics report for CaseGraph scoring.

Deterministic, no-live aggregator. Given an iterable of CasePackets,
calls `score_case_packet` on each (pure — does NOT mutate the packet)
and returns a structured summary covering:

- verdict counts (PRODUCE / HOLD / SKIP)
- false-PRODUCE guard counts (weak identity, document-only,
  claim-only, protected/PACER, outcome-not-concluded, no-verified-media)
- artifact portfolio counts by category and shape
- research_completeness / production_actionability / actionability
  score distributions (min, max, mean, median, p90)
- risk flag and reason code occurrence counts
- input_type breakdown
- produce-eligible inventory (case_id, production score, media
  categories, reason codes — sorted by production score desc)

This module does not download, scrape, fetch transcripts, or call any
LLM. It is intentionally a pure read of already-built CasePackets so
it can run safely in any test or report context.
"""

from __future__ import annotations

from statistics import mean, median
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .ledger import DEFAULT_API_CALLS, RunLedgerEntry
from .media_relevance import MediaRelevanceResult, classify_media_relevance
from .models import CasePacket, VerifiedArtifact
from .scoring import (
    MEDIA_ARTIFACT_TYPES,
    MEDIA_FORMATS,
    ActionabilityResult,
    score_case_packet,
)


CONCLUDED_OUTCOMES = {"sentenced", "closed", "convicted"}

PROTECTED_RISK_FLAGS = {
    "protected_or_nonpublic",
    "protected_or_nonpublic_only",
    "pacer_or_paywalled",
}

TIER_ORDER = {"A": 3, "B": 2, "C": 1, "unknown": 0}


def _is_media_artifact(artifact: VerifiedArtifact) -> bool:
    return artifact.artifact_type in MEDIA_ARTIFACT_TYPES or artifact.format in MEDIA_FORMATS


def _stats(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"min": 0.0, "max": 0.0, "mean": 0.0, "median": 0.0, "p90": 0.0}
    sorted_vals = sorted(values)
    idx_p90 = max(0, int(round(0.9 * (len(sorted_vals) - 1))))
    return {
        "min": round(sorted_vals[0], 2),
        "max": round(sorted_vals[-1], 2),
        "mean": round(mean(values), 2),
        "median": round(median(values), 2),
        "p90": round(sorted_vals[idx_p90], 2),
    }


def _empty_report() -> Dict[str, Any]:
    zero_stats = {"min": 0.0, "max": 0.0, "mean": 0.0, "median": 0.0, "p90": 0.0}
    return {
        "total_cases": 0,
        "verdict_counts": {"PRODUCE": 0, "HOLD": 0, "SKIP": 0},
        "false_produce_guards": {
            "weak_identity_blocks": 0,
            "document_only_holds": 0,
            "claim_only_holds": 0,
            "protected_or_pacer_blocked": 0,
            "outcome_unconcluded_holds": 0,
            "no_verified_media_blocks": 0,
        },
        "artifact_portfolio": {
            "by_artifact_type": {},
            "media_only_cases": 0,
            "document_only_cases": 0,
            "no_artifact_cases": 0,
            "multi_media_cases": 0,
            "multi_artifact_premium_cases": 0,
        },
        "score_distribution": {
            "research_completeness": dict(zero_stats),
            "production_actionability": dict(zero_stats),
            "actionability": dict(zero_stats),
        },
        "risk_flag_counts": {},
        "reason_code_counts": {},
        "input_type_breakdown": {},
        "produce_eligible_inventory": [],
        "media_quality": _empty_media_quality_report(),
    }


def _empty_media_quality_report() -> Dict[str, Any]:
    return {
        "total_media_artifacts": 0,
        "tier_counts": {"A": 0, "B": 0, "C": 0, "unknown": 0},
        "primary_source_media_count": 0,
        "secondary_source_media_count": 0,
        "weak_or_uncertain_media_count": 0,
        "official_source_likelihood_counts": {"high": 0, "medium": 0, "low": 0},
        "needs_manual_review_count": 0,
        "media_risk_flags": {},
        "media_reason_codes": {},
        "produce_media_basis_counts": {"A": 0, "B": 0, "C": 0, "unknown": 0, "none": 0},
        "produce_cases": [],
        "warnings": [],
        "top_media_artifacts": [],
    }


def _likelihood_bucket(value: float) -> str:
    if value >= 0.7:
        return "high"
    if value >= 0.35:
        return "medium"
    return "low"


def _increment_counts(counts: Dict[str, int], values: Iterable[str]) -> None:
    for value in values:
        counts[value] = counts.get(value, 0) + 1


def _highest_tier(results: List[MediaRelevanceResult]) -> str:
    if not results:
        return "none"
    return max((r.media_relevance_tier for r in results), key=lambda tier: TIER_ORDER.get(tier, -1))


def _is_youtube_artifact(artifact: VerifiedArtifact) -> bool:
    url = artifact.artifact_url.lower()
    return "youtube.com/" in url or "youtu.be/" in url


def build_media_quality_report(packets: Iterable[CasePacket]) -> Dict[str, Any]:
    """Summarize media relevance quality across CasePackets.

    Advisory only: this report never changes scoring or verdicts. It
    grades already-verified media artifacts with the metadata-only
    MEDIA4 classifier and emits warnings when production depends on
    weak or uncertain media.
    """

    packet_list = list(packets)
    if not packet_list:
        return _empty_media_quality_report()

    report = _empty_media_quality_report()
    top_media: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for packet in packet_list:
        result = score_case_packet(packet)
        media_artifacts = [a for a in packet.verified_artifacts if _is_media_artifact(a)]
        relevance_results = [classify_media_relevance(a) for a in media_artifacts]

        if result.verdict == "PRODUCE":
            highest = _highest_tier(relevance_results)
            report["produce_media_basis_counts"][highest] += 1
            weak_only = bool(relevance_results) and all(
                r.media_relevance_tier in {"C", "unknown"} for r in relevance_results
            )
            report["produce_cases"].append(
                {
                    "case_id": packet.case_id,
                    "highest_media_relevance_tier": highest,
                    "media_relevance_tiers": [r.media_relevance_tier for r in relevance_results],
                    "weak_or_uncertain_media_only": weak_only,
                    "needs_manual_review": any(r.needs_manual_review for r in relevance_results),
                }
            )
            if weak_only:
                warnings.append(f"produce_based_only_on_weak_or_uncertain_media:{packet.case_id}")
        elif not relevance_results:
            # Non-PRODUCE rows with no media are not production basis,
            # but the key remains explicit for downstream consumers.
            pass

        if media_artifacts and all(_is_youtube_artifact(a) for a in media_artifacts):
            if not any(r.primary_source_likelihood >= 0.7 or r.official_source_likelihood >= 0.7 for r in relevance_results):
                warnings.append(f"all_media_youtube_no_official_or_primary:{packet.case_id}")

        for artifact, relevance in zip(media_artifacts, relevance_results):
            report["total_media_artifacts"] += 1
            tier = relevance.media_relevance_tier
            report["tier_counts"][tier] = report["tier_counts"].get(tier, 0) + 1
            if relevance.primary_source_likelihood >= 0.7:
                report["primary_source_media_count"] += 1
            if tier == "B":
                report["secondary_source_media_count"] += 1
            if tier in {"C", "unknown"}:
                report["weak_or_uncertain_media_count"] += 1
            bucket = _likelihood_bucket(relevance.official_source_likelihood)
            report["official_source_likelihood_counts"][bucket] += 1
            if relevance.needs_manual_review:
                report["needs_manual_review_count"] += 1
            _increment_counts(report["media_risk_flags"], relevance.risk_flags)
            _increment_counts(report["media_reason_codes"], relevance.reason_codes)
            for warning in relevance.mismatch_warnings:
                warnings.append(f"{warning}:{packet.case_id}:{artifact.artifact_id}")
            top_media.append(
                {
                    "case_id": packet.case_id,
                    "artifact_id": artifact.artifact_id,
                    "artifact_url": artifact.artifact_url,
                    "artifact_type": artifact.artifact_type,
                    "media_relevance_tier": relevance.media_relevance_tier,
                    "media_relevance_score": relevance.media_relevance_score,
                    "primary_source_likelihood": relevance.primary_source_likelihood,
                    "official_source_likelihood": relevance.official_source_likelihood,
                    "needs_manual_review": relevance.needs_manual_review,
                    "reason_codes": list(relevance.reason_codes),
                    "risk_flags": list(relevance.risk_flags),
                    "mismatch_warnings": list(relevance.mismatch_warnings),
                }
            )

    report["media_risk_flags"] = dict(sorted(report["media_risk_flags"].items(), key=lambda kv: (-kv[1], kv[0])))
    report["media_reason_codes"] = dict(sorted(report["media_reason_codes"].items(), key=lambda kv: (-kv[1], kv[0])))
    report["warnings"] = sorted(set(warnings))
    report["top_media_artifacts"] = sorted(
        top_media,
        key=lambda item: (-item["media_relevance_score"], item["case_id"], item["artifact_id"]),
    )[:10]
    return report


def build_endpoint_v2_status_report(
    *,
    endpoint_v0_status: Optional[Mapping[str, Any]] = None,
    endpoint_v1_status: Optional[Mapping[str, Any]] = None,
    endpoint_v11_media_quality: Optional[Mapping[str, Any]] = None,
    endpoint_v2_status: Optional[Mapping[str, Any]] = None,
    live_yield_report: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """EVAL9 - deterministic Endpoint v2 status summary.

    The report is advisory and no-live. It compares prior endpoint
    milestones, summarizes the current official-primary media attempt,
    and emits concrete blockers/next actions without changing scoring
    or verdict behavior.
    """

    v0 = dict(endpoint_v0_status or {})
    v1 = dict(endpoint_v1_status or {})
    v11 = dict(endpoint_v11_media_quality or {})
    v2 = dict(endpoint_v2_status or {})
    live = dict(live_yield_report or {})

    tier_counts = {"A": 0, "B": 0, "C": 0, "unknown": 0}
    for tier, count in dict(v2.get("media_relevance_tiers") or {}).items():
        tier_counts[tier if tier in tier_counts else "unknown"] += int(count or 0)

    risk_flags = list(v2.get("risk_flags") or [])
    reason_codes = list(v2.get("reason_codes") or [])
    blockers = list(v2.get("blockers") or [])
    if not bool(v2.get("endpoint_v2_achieved")):
        if int(v2.get("verified_artifact_count") or 0) == 0 and "no_verified_artifacts" not in blockers:
            blockers.append("no_verified_artifacts")
        if tier_counts["A"] == 0 and "no_verified_tier_a_media" not in blockers:
            blockers.append("no_verified_tier_a_media")
        if v2.get("verdict") and v2.get("verdict") != "PRODUCE" and "verdict_not_produce" not in blockers:
            blockers.append("verdict_not_produce")

    weak_advisory_count = 0
    for value in risk_flags + reason_codes:
        if value in {
            "produce_based_on_weak_or_uncertain_media",
            "weak_or_uncertain_media",
            "media_query_artifact_type_mismatch",
        }:
            weak_advisory_count += 1

    connector = v2.get("connector_used") or "unknown"
    connector_yields = {
        connector: {
            "source_records": int(v2.get("source_records_returned") or 0),
            "verified_artifacts": int(v2.get("verified_artifact_count") or 0),
            "media_artifacts": int(v2.get("media_artifact_count") or 0),
        }
    }
    if live.get("by_provider"):
        for provider, payload in live.get("by_provider", {}).items():
            if provider not in connector_yields:
                connector_yields[provider] = {
                    "source_records": int(payload.get("source_records") or payload.get("result_count") or 0),
                    "verified_artifacts": int(payload.get("verified_artifacts") or 0),
                    "media_artifacts": 0,
                }

    next_actions: List[str] = []
    if "no_verified_tier_a_media" in blockers:
        next_actions.append("Find a supported live path that returns concrete Tier A media URLs.")
    if connector == "youtube" and int(v2.get("source_records_returned") or 0) == 0:
        next_actions.append("Install or enable yt-dlp in the repo venv before retrying the capped YouTube pilot.")
    if "unsupported_agency_ois_only_path" in blockers:
        next_actions.append("Implement a live agency_ois connector or choose a supported-source seed.")
    if not next_actions and not bool(v2.get("endpoint_v2_achieved")):
        next_actions.append("Review blockers and rerun LIVE9 under the selected pilot budget.")

    return {
        "endpoint_v0": {
            "achieved": bool(v0.get("endpoint_v0_fully_achieved") or v0.get("achieved")),
            "verified_artifact_count": int(v0.get("verified_artifact_count") or 0),
            "document_artifact_count": int(v0.get("document_artifact_count") or 0),
        },
        "endpoint_v1": {
            "achieved": bool(v1.get("endpoint_v1_achieved") or v1.get("achieved")),
            "media_artifact_count": int(v1.get("media_artifact_count") or 0),
            "verdict": v1.get("verdict"),
        },
        "endpoint_v1_1": {
            "weak_media_warning_count": len(v11.get("warnings") or []),
            "tier_counts": dict(v11.get("tier_counts") or {}),
        },
        "endpoint_v2": {
            "achieved": bool(v2.get("endpoint_v2_achieved")),
            "pilot_id": v2.get("pilot_id"),
            "connector_used": connector,
            "query": v2.get("query"),
            "tier_a_media_count": tier_counts["A"],
            "tier_b_media_count": tier_counts["B"],
            "tier_c_or_unknown_media_count": tier_counts["C"] + tier_counts["unknown"],
            "weak_media_advisory_count": weak_advisory_count,
            "official_source_likelihood": v2.get("official_source_likelihood"),
            "connector_yields": connector_yields,
            "live_call_count": int(v2.get("live_calls_used") or live.get("total_live_calls") or 0),
            "cost": float(v2.get("estimated_cost_usd") or live.get("total_estimated_cost_usd") or 0.0),
            "verdict": v2.get("verdict"),
            "blockers": sorted(set(blockers)),
            "next_actions": next_actions,
        },
    }


def build_actionability_report(packets: Iterable[CasePacket]) -> Dict[str, Any]:
    """Build a structured no-live report from a packet collection.

    For each packet, calls `score_case_packet(packet)` to obtain its
    ActionabilityResult. The scoring function is documented as pure —
    this aggregator does not mutate any input packet.

    The output schema is stable: callers can rely on every key being
    present even when the packet list is empty.
    """

    packet_list = list(packets)
    if not packet_list:
        return _empty_report()

    scored: List[Tuple[CasePacket, ActionabilityResult]] = [
        (p, score_case_packet(p)) for p in packet_list
    ]

    verdict_counts = {"PRODUCE": 0, "HOLD": 0, "SKIP": 0}
    guards = {
        "weak_identity_blocks": 0,
        "document_only_holds": 0,
        "claim_only_holds": 0,
        "protected_or_pacer_blocked": 0,
        "outcome_unconcluded_holds": 0,
        "no_verified_media_blocks": 0,
    }
    by_type: Dict[str, int] = {}
    media_only = 0
    document_only = 0
    no_artifact = 0
    multi_media = 0
    premium_cases = 0
    risk_counts: Dict[str, int] = {}
    reason_counts: Dict[str, int] = {}
    input_types: Dict[str, int] = {}

    research_scores: List[float] = []
    production_scores: List[float] = []
    actionability_scores: List[float] = []
    inventory: List[Dict[str, Any]] = []

    for packet, result in scored:
        verdict_counts[result.verdict] = verdict_counts.get(result.verdict, 0) + 1

        if result.verdict != "PRODUCE":
            if (
                packet.case_identity.identity_confidence == "low"
                or "weak_identity" in result.risk_flags
            ):
                guards["weak_identity_blocks"] += 1
            if "document_only_hold" in result.reason_codes:
                guards["document_only_holds"] += 1
            if "claim_only_hold" in result.reason_codes:
                guards["claim_only_holds"] += 1
            if PROTECTED_RISK_FLAGS & set(result.risk_flags):
                guards["protected_or_pacer_blocked"] += 1
            if packet.case_identity.outcome_status not in CONCLUDED_OUTCOMES:
                guards["outcome_unconcluded_holds"] += 1
            if "no_verified_media" in result.risk_flags:
                guards["no_verified_media_blocks"] += 1

        media = [a for a in packet.verified_artifacts if _is_media_artifact(a)]
        documents = [a for a in packet.verified_artifacts if not _is_media_artifact(a)]
        for artifact in packet.verified_artifacts:
            by_type[artifact.artifact_type] = by_type.get(artifact.artifact_type, 0) + 1

        if not packet.verified_artifacts:
            no_artifact += 1
        elif media and not documents:
            media_only += 1
        elif documents and not media:
            document_only += 1
        if len({a.artifact_type for a in media}) >= 2:
            multi_media += 1
        if "artifact_portfolio_strong" in result.reason_codes:
            premium_cases += 1

        for flag in result.risk_flags:
            risk_counts[flag] = risk_counts.get(flag, 0) + 1
        for code in result.reason_codes:
            reason_counts[code] = reason_counts.get(code, 0) + 1

        input_type = packet.input.input_type or "unknown"
        input_types[input_type] = input_types.get(input_type, 0) + 1

        research_scores.append(result.research_completeness_score)
        production_scores.append(result.production_actionability_score)
        actionability_scores.append(result.actionability_score)

        if result.verdict == "PRODUCE":
            media_categories = sorted({a.artifact_type for a in media})
            inventory.append(
                {
                    "case_id": packet.case_id,
                    "production_actionability_score": result.production_actionability_score,
                    "media_categories": media_categories,
                    "reason_codes": list(result.reason_codes),
                }
            )

    inventory.sort(key=lambda entry: -entry["production_actionability_score"])

    return {
        "total_cases": len(scored),
        "verdict_counts": verdict_counts,
        "false_produce_guards": guards,
        "artifact_portfolio": {
            "by_artifact_type": dict(sorted(by_type.items())),
            "media_only_cases": media_only,
            "document_only_cases": document_only,
            "no_artifact_cases": no_artifact,
            "multi_media_cases": multi_media,
            "multi_artifact_premium_cases": premium_cases,
        },
        "score_distribution": {
            "research_completeness": _stats(research_scores),
            "production_actionability": _stats(production_scores),
            "actionability": _stats(actionability_scores),
        },
        "risk_flag_counts": dict(sorted(risk_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
        "reason_code_counts": dict(sorted(reason_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
        "input_type_breakdown": dict(sorted(input_types.items())),
        "produce_eligible_inventory": inventory,
        "media_quality": build_media_quality_report(packet_list),
    }


def _per_provider_zero() -> Dict[str, Any]:
    return {
        "calls": 0,
        "wallclock_seconds": 0.0,
        "estimated_cost_usd": 0.0,
        "result_count": 0,
        "source_records": 0,
        "verified_artifacts": 0,
        "endpoints": [],
        "errors": [],
    }


def _empty_live_yield_report() -> Dict[str, Any]:
    return {
        "total_runs": 0,
        "total_live_calls": 0,
        "total_estimated_cost_usd": 0.0,
        "total_wallclock_seconds": 0.0,
        "total_source_records": 0,
        "total_verified_artifacts": 0,
        "total_media_artifacts": 0,
        "total_document_artifacts": 0,
        "by_provider": {provider: _per_provider_zero() for provider in DEFAULT_API_CALLS},
        "verdict_counts": {"PRODUCE": 0, "HOLD": 0, "SKIP": 0, "unknown": 0},
        "warnings": [],
    }


def _empty_pilot_validation_scoreboard() -> Dict[str, Any]:
    return {
        "validation": {
            "total_entries": 0,
            "accuracy_pct": 0.0,
            "false_produce_count": 0,
            "guard_counters": {
                "document_only_produce_count": 0,
                "claim_only_produce_count": 0,
                "weak_identity_produce_count": 0,
                "protected_or_pacer_produce_count": 0,
            },
            "guard_counters_all_zero": True,
        },
        "pilots": {
            "total": 0,
            "ready_for_live": 0,
            "blocked": 0,
            "by_readiness_status": {},
            "blocked_pilots": [],
        },
        "connector_demand": {},
        "expected_artifact_types": {},
        "media_required_for_produce_count": 0,
        "total_planned_max_live_calls": 0,
        "warnings": [],
    }


def build_pilot_validation_scoreboard(
    *,
    validation_output: Optional[Mapping[str, Any]] = None,
    pilot_output: Optional[Mapping[str, Any]] = None,
    top_n_blocked: int = 10,
) -> Dict[str, Any]:
    """EVAL7 — Merged scoreboard over DATA2 validation runner output
    and PILOT2 readiness output.

    Pure: no network. Tolerates missing or empty inputs - either side
    may be ``None`` or carry an empty ``results`` / ``pilots`` list.

    Output fields:
    - ``validation``: total_entries, accuracy_pct, false_produce_count,
      guard_counters (all four), guard_counters_all_zero (bool)
    - ``pilots``: total, ready_for_live, blocked, by_readiness_status,
      blocked_pilots (top_n_blocked entries with id / status /
      policy_violations / budget_violations / next_actions)
    - ``connector_demand``: count of pilots that include each connector
      in ``allowed_connectors`` (sorted by name asc)
    - ``expected_artifact_types``: count across all pilots'
      ``expected_minimum.artifact_types_desired``
    - ``media_required_for_produce_count``: how many pilots set
      ``media_required_for_produce=true`` in expected_minimum
    - ``total_planned_max_live_calls``: sum of pilot ``max_live_calls``
    - ``warnings``: deterministic, sorted list. Possible warnings:
      ``over_budget_pilot:<id>``, ``paid_connector_in_pilot:<id>``,
      ``missing_media_required_for_produce:<id>``,
      ``downloads_enabled_in_pilot:<id>``,
      ``scraping_enabled_in_pilot:<id>``,
      ``llm_enabled_in_pilot:<id>``,
      ``validation_false_produce``,
      ``validation_guard_counter_nonzero:<counter>``.
    """
    if not validation_output and not pilot_output:
        return _empty_pilot_validation_scoreboard()

    # ---- Validation roll-up ------------------------------------------------
    validation_metrics = build_validation_metrics_report(
        validation_output if validation_output is not None else {"results": []}
    )
    validation_section = {
        "total_entries": validation_metrics["total_entries"],
        "accuracy_pct": validation_metrics["verdict_accuracy"]["accuracy_pct"],
        "false_produce_count": validation_metrics["false_verdicts"][
            "false_produce_count"
        ],
        "guard_counters": dict(validation_metrics["guard_counters"]),
        "guard_counters_all_zero": all(
            v == 0 for v in validation_metrics["guard_counters"].values()
        ),
    }

    # ---- Pilot roll-up -----------------------------------------------------
    pilots_section = {
        "total": 0,
        "ready_for_live": 0,
        "blocked": 0,
        "by_readiness_status": {},
        "blocked_pilots": [],
    }
    connector_demand: Dict[str, int] = {}
    artifact_types: Dict[str, int] = {}
    media_required_count = 0
    total_planned_calls = 0
    warnings: List[str] = []

    pilot_results: List[Mapping[str, Any]] = []
    if pilot_output is not None and isinstance(pilot_output.get("results"), list):
        pilot_results = list(pilot_output["results"])
        summary = pilot_output.get("summary") or {}
        pilots_section["total"] = int(summary.get("total_pilots") or len(pilot_results))
        pilots_section["ready_for_live"] = int(summary.get("ready_count") or 0)
        pilots_section["blocked"] = int(summary.get("blocked_count") or 0)
        pilots_section["by_readiness_status"] = dict(
            sorted((summary.get("by_readiness_status") or {}).items())
        )

    blocked_entries: List[Dict[str, Any]] = []

    for r in pilot_results:
        pilot_id = r.get("id") or "<unknown>"
        status = r.get("readiness_status") or "unknown"

        for connector in r.get("allowed_connectors") or []:
            connector_demand[connector] = connector_demand.get(connector, 0) + 1

        expected_minimum = r.get("expected_minimum") or {}
        for artifact_type in expected_minimum.get("artifact_types_desired") or []:
            artifact_types[artifact_type] = artifact_types.get(artifact_type, 0) + 1
        if expected_minimum.get("media_required_for_produce"):
            media_required_count += 1

        try:
            total_planned_calls += int(r.get("max_live_calls") or 0)
        except (TypeError, ValueError):
            pass

        for v in r.get("budget_violations") or []:
            warnings.append(f"over_budget_pilot:{pilot_id}:{v}")
        for v in r.get("policy_violations") or []:
            if v.startswith("paid_connectors_listed:"):
                warnings.append(f"paid_connector_in_pilot:{pilot_id}")
            elif v == "media_required_for_produce_false":
                warnings.append(f"missing_media_required_for_produce:{pilot_id}")
            elif v == "allow_downloads_true":
                warnings.append(f"downloads_enabled_in_pilot:{pilot_id}")
            elif v == "allow_scraping_true":
                warnings.append(f"scraping_enabled_in_pilot:{pilot_id}")
            elif v == "allow_llm_true":
                warnings.append(f"llm_enabled_in_pilot:{pilot_id}")
            elif v.startswith("unknown_connectors_listed:"):
                warnings.append(f"unknown_connector_in_pilot:{pilot_id}")

        if status != "ready_for_live_smoke":
            blocked_entries.append(
                {
                    "id": pilot_id,
                    "status": status,
                    "policy_violations": list(r.get("policy_violations") or []),
                    "budget_violations": list(r.get("budget_violations") or []),
                    "next_actions": list(r.get("next_actions") or []),
                }
            )

    pilots_section["blocked_pilots"] = blocked_entries[:top_n_blocked]

    if validation_section["false_produce_count"] > 0:
        warnings.append("validation_false_produce")
    for counter, value in validation_section["guard_counters"].items():
        if value > 0:
            warnings.append(f"validation_guard_counter_nonzero:{counter}")

    warnings = sorted(set(warnings))

    return {
        "validation": validation_section,
        "pilots": pilots_section,
        "connector_demand": dict(sorted(connector_demand.items())),
        "expected_artifact_types": dict(sorted(artifact_types.items())),
        "media_required_for_produce_count": media_required_count,
        "total_planned_max_live_calls": total_planned_calls,
        "warnings": warnings,
    }


def _empty_validation_metrics() -> Dict[str, Any]:
    zero_stats = {"min": 0.0, "max": 0.0, "mean": 0.0, "median": 0.0, "p90": 0.0}
    zero_confusion = {"PRODUCE": 0, "HOLD": 0, "SKIP": 0, "missing": 0}
    return {
        "total_entries": 0,
        "verdict_accuracy": {
            "correct": 0,
            "incorrect": 0,
            "accuracy_pct": 0.0,
        },
        "verdict_confusion": {
            "PRODUCE": dict(zero_confusion),
            "HOLD": dict(zero_confusion),
            "SKIP": dict(zero_confusion),
        },
        "false_verdicts": {
            "false_produce_count": 0,
            "false_hold_count": 0,
            "false_skip_count": 0,
        },
        "guard_counters": {
            "document_only_produce_count": 0,
            "claim_only_produce_count": 0,
            "weak_identity_produce_count": 0,
            "protected_or_pacer_produce_count": 0,
        },
        "artifact_yield": {
            "total_verified_artifacts": 0,
            "total_media_artifacts": 0,
            "total_document_artifacts": 0,
            "media_artifact_rate": 0.0,
            "media_only_cases": 0,
            "document_only_cases": 0,
            "no_artifact_cases": 0,
            "by_input_type": {},
        },
        "scenario_counts": {
            "document_only_HOLD_count": 0,
            "claim_only_HOLD_count": 0,
            "weak_identity_blocked_count": 0,
            "protected_blocked_count": 0,
            "concluded_outcome_count": 0,
            "media_artifact_present_count": 0,
        },
        "score_distribution": {
            "research_completeness": dict(zero_stats),
            "production_actionability": dict(zero_stats),
            "actionability": dict(zero_stats),
        },
        "failure_examples": [],
        "top_next_actions": [],
        "top_risk_flags": [],
        "top_reason_codes": [],
    }


def _top_n(counts: Dict[str, int], top_n: int, *, key: str) -> List[Dict[str, Any]]:
    pairs = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return [{key: name, "count": count} for name, count in pairs[:top_n]]


def build_validation_metrics_report(
    validation_output: Mapping[str, Any],
    *,
    top_n: int = 5,
) -> Dict[str, Any]:
    """EVAL6 — Build a deterministic metrics report from the structured
    output of :func:`pipeline2_discovery.casegraph.validation.run_validation_manifest`.

    Pure: no network, no LLM. Counts and distributions are computed
    over the per-entry results in ``validation_output['results']``.
    The ``guard_counters`` field is taken from the validation summary
    when present and recomputed from the per-entry results when the
    summary is missing.

    Output is deterministic and JSON-serializable. Empty input
    (``results == []``) yields a fully-populated report with zero
    defaults so consumers can rely on key presence.
    """

    if not isinstance(validation_output, Mapping):
        return _empty_validation_metrics()

    results = list(validation_output.get("results") or [])
    if not results:
        return _empty_validation_metrics()

    correct = 0
    incorrect = 0
    confusion: Dict[str, Dict[str, int]] = {
        "PRODUCE": {"PRODUCE": 0, "HOLD": 0, "SKIP": 0, "missing": 0},
        "HOLD": {"PRODUCE": 0, "HOLD": 0, "SKIP": 0, "missing": 0},
        "SKIP": {"PRODUCE": 0, "HOLD": 0, "SKIP": 0, "missing": 0},
    }
    false_produce = 0
    false_hold = 0
    false_skip = 0

    total_artifacts = 0
    total_media = 0
    total_document = 0
    media_only = 0
    document_only = 0
    no_artifact = 0
    by_input_type: Dict[str, Dict[str, int]] = {}

    document_only_HOLD = 0
    claim_only_HOLD = 0
    weak_identity_blocked = 0
    protected_blocked = 0
    concluded_count = 0
    media_present_count = 0

    next_action_counts: Dict[str, int] = {}
    risk_flag_counts: Dict[str, int] = {}
    reason_code_counts: Dict[str, int] = {}

    research_scores: List[float] = []
    production_scores: List[float] = []
    actionability_scores: List[float] = []

    failure_examples: List[Dict[str, Any]] = []

    document_only_produce = 0
    claim_only_produce = 0
    weak_identity_produce = 0
    protected_pacer_produce = 0

    for r in results:
        expected = r.get("expected_verdict")
        actual = r.get("actual_verdict")
        if expected in confusion:
            cell = actual if actual in {"PRODUCE", "HOLD", "SKIP"} else "missing"
            confusion[expected][cell] += 1
        if expected is not None and actual == expected:
            correct += 1
        else:
            incorrect += 1

        if actual == "PRODUCE" and expected != "PRODUCE":
            false_produce += 1
        if actual == "HOLD" and expected != "HOLD":
            false_hold += 1
        if actual == "SKIP" and expected != "SKIP":
            false_skip += 1

        v = int(r.get("verified_artifact_count") or 0)
        m = int(r.get("media_artifact_count") or 0)
        d = int(r.get("document_artifact_count") or 0)
        total_artifacts += v
        total_media += m
        total_document += d
        if v == 0:
            no_artifact += 1
        elif m > 0 and d == 0:
            media_only += 1
        elif d > 0 and m == 0:
            document_only += 1

        input_type = str(r.get("input_type") or "unknown")
        slot = by_input_type.setdefault(
            input_type, {"verified": 0, "media": 0, "document": 0}
        )
        slot["verified"] += v
        slot["media"] += m
        slot["document"] += d

        if actual == "HOLD" and v > 0 and m == 0:
            document_only_HOLD += 1
        if actual == "HOLD" and v == 0:
            claim_only_HOLD += 1
        if r.get("identity_confidence") != "high" and actual != "PRODUCE":
            weak_identity_blocked += 1
        if (
            "protected_or_nonpublic" in (r.get("risk_flags") or [])
            or "pacer_or_paywalled" in (r.get("risk_flags") or [])
            or "protected_or_nonpublic_only" in (r.get("risk_flags") or [])
        ) and actual != "PRODUCE":
            protected_blocked += 1
        if str(r.get("outcome_status") or "") in {"sentenced", "closed", "convicted"}:
            concluded_count += 1
        if m > 0:
            media_present_count += 1

        if actual == "PRODUCE":
            if m == 0:
                document_only_produce += 1
            if v == 0:
                claim_only_produce += 1
            if r.get("identity_confidence") != "high":
                weak_identity_produce += 1
            risk_flags_here = set(r.get("risk_flags") or [])
            if risk_flags_here & {
                "protected_or_nonpublic",
                "pacer_or_paywalled",
                "protected_or_nonpublic_only",
            }:
                protected_pacer_produce += 1

        for code in r.get("reason_codes") or []:
            reason_code_counts[code] = reason_code_counts.get(code, 0) + 1
        for flag in r.get("risk_flags") or []:
            risk_flag_counts[flag] = risk_flag_counts.get(flag, 0) + 1
        for action in r.get("next_actions") or []:
            next_action_counts[action] = next_action_counts.get(action, 0) + 1

        research_scores.append(float(r.get("research_completeness_score") or 0.0))
        production_scores.append(float(r.get("production_actionability_score") or 0.0))
        actionability_scores.append(float(r.get("actionability_score") or 0.0))

        if not r.get("passed", True):
            failure_examples.append(
                {
                    "id": r.get("id"),
                    "expected_verdict": expected,
                    "actual_verdict": actual,
                    "fail_reasons": list(r.get("fail_reasons") or []),
                }
            )

    summary = (
        validation_output.get("summary")
        if isinstance(validation_output.get("summary"), Mapping)
        else None
    )
    if summary is not None:
        guard_counters = {
            "document_only_produce_count": int(summary.get("document_only_produce_count") or document_only_produce),
            "claim_only_produce_count": int(summary.get("claim_only_produce_count") or claim_only_produce),
            "weak_identity_produce_count": int(summary.get("weak_identity_produce_count") or weak_identity_produce),
            "protected_or_pacer_produce_count": int(
                summary.get("protected_or_pacer_produce_count") or protected_pacer_produce
            ),
        }
    else:
        guard_counters = {
            "document_only_produce_count": document_only_produce,
            "claim_only_produce_count": claim_only_produce,
            "weak_identity_produce_count": weak_identity_produce,
            "protected_or_pacer_produce_count": protected_pacer_produce,
        }

    total = len(results)
    accuracy_pct = round((correct / total) * 100, 2) if total > 0 else 0.0
    media_rate = round(total_media / total_artifacts, 4) if total_artifacts > 0 else 0.0

    return {
        "total_entries": total,
        "verdict_accuracy": {
            "correct": correct,
            "incorrect": incorrect,
            "accuracy_pct": accuracy_pct,
        },
        "verdict_confusion": confusion,
        "false_verdicts": {
            "false_produce_count": false_produce,
            "false_hold_count": false_hold,
            "false_skip_count": false_skip,
        },
        "guard_counters": guard_counters,
        "artifact_yield": {
            "total_verified_artifacts": total_artifacts,
            "total_media_artifacts": total_media,
            "total_document_artifacts": total_document,
            "media_artifact_rate": media_rate,
            "media_only_cases": media_only,
            "document_only_cases": document_only,
            "no_artifact_cases": no_artifact,
            "by_input_type": dict(sorted(by_input_type.items())),
        },
        "scenario_counts": {
            "document_only_HOLD_count": document_only_HOLD,
            "claim_only_HOLD_count": claim_only_HOLD,
            "weak_identity_blocked_count": weak_identity_blocked,
            "protected_blocked_count": protected_blocked,
            "concluded_outcome_count": concluded_count,
            "media_artifact_present_count": media_present_count,
        },
        "score_distribution": {
            "research_completeness": _stats(research_scores),
            "production_actionability": _stats(production_scores),
            "actionability": _stats(actionability_scores),
        },
        "failure_examples": failure_examples[:top_n],
        "top_next_actions": _top_n(next_action_counts, top_n, key="action"),
        "top_risk_flags": _top_n(risk_flag_counts, top_n, key="flag"),
        "top_reason_codes": _top_n(reason_code_counts, top_n, key="code"),
    }


def build_live_yield_report(
    ledger_entries: Iterable[RunLedgerEntry],
    *,
    per_connector_diagnostics: Optional[Iterable[Dict[str, Any]]] = None,
    expected_connectors: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Summarize live-smoke yield across a sequence of RunLedgerEntry rows.

    Inputs:
    - ``ledger_entries``: iterable of :class:`RunLedgerEntry` (typically
      from :data:`autoresearch/.runs/experiments.jsonl` or
      :func:`build_run_ledger_entry` calls).
    - ``per_connector_diagnostics`` (optional): iterable of dicts as
      returned by :meth:`LiveSmokeResult.to_diagnostics` /
      :meth:`MultiConnectorSmokeResult.to_diagnostics`. Used to
      attribute source-record / verified-artifact counts to specific
      providers — without this, only api_calls are attributable.
    - ``expected_connectors`` (optional): iterable of connector names
      a caller expected to see. Any connector listed here that did
      NOT make a call yields a ``missing_provider`` warning.

    Output: a deterministic, JSON-serializable dict shaped for
    downstream review and JSONL persistence. Keys covered include
    total_live_calls, total_estimated_cost_usd, total_wallclock_seconds,
    total_source_records, total_verified_artifacts, total_media_artifacts,
    total_document_artifacts, by_provider (per-provider rollup),
    verdict_counts, warnings.

    Warnings emitted:
    - ``zero_yield_provider:<name>`` — provider with calls > 0 but
      result_count == 0
    - ``unexpected_verified_artifacts_from_smoke:<count>`` — non-zero
      VerifiedArtifact count attributed to a smoke run (smokes alone
      should never create them)
    - ``unexpected_cost:<amount>`` — non-zero estimated cost (smokes
      against free providers should be $0)
    - ``missing_provider:<name>`` — caller-expected provider with no
      calls
    """

    entries = list(ledger_entries)
    diags = list(per_connector_diagnostics or [])

    if not entries and not diags:
        return _empty_live_yield_report()

    by_provider: Dict[str, Dict[str, Any]] = {
        provider: _per_provider_zero() for provider in DEFAULT_API_CALLS
    }

    total_live_calls = 0
    total_cost = 0.0
    total_wall = 0.0
    total_sources = 0
    total_artifacts = 0
    total_media = 0
    total_documents = 0
    verdict_counts: Dict[str, int] = {"PRODUCE": 0, "HOLD": 0, "SKIP": 0, "unknown": 0}
    warnings: List[str] = []

    for entry in entries:
        for provider, count in (entry.api_calls or {}).items():
            slot = by_provider.setdefault(provider, _per_provider_zero())
            slot["calls"] += int(count)
            total_live_calls += int(count)
        total_cost += float(entry.estimated_cost_usd or 0.0)
        total_wall += float(entry.wallclock_seconds or 0.0)
        total_sources += int(entry.source_record_count or 0)
        total_artifacts += int(entry.verified_artifact_count or 0)
        total_media += int(entry.media_artifact_count or 0)
        total_documents += int(entry.document_artifact_count or 0)
        verdict = entry.verdict or "unknown"
        verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1

    for diag in diags:
        connector = str(diag.get("connector") or "")
        if not connector:
            continue
        slot = by_provider.setdefault(connector, _per_provider_zero())
        slot["result_count"] += int(diag.get("result_count") or 0)
        slot["source_records"] += int(diag.get("result_count") or 0)
        slot["verified_artifacts"] += int(diag.get("verified_artifact_count") or 0)
        slot["wallclock_seconds"] = round(
            slot["wallclock_seconds"] + float(diag.get("wallclock_seconds") or 0.0), 4
        )
        slot["estimated_cost_usd"] = round(
            slot["estimated_cost_usd"] + float(diag.get("estimated_cost_usd") or 0.0), 4
        )
        endpoint = diag.get("endpoint")
        if endpoint and endpoint not in slot["endpoints"]:
            slot["endpoints"].append(endpoint)
        error = diag.get("error")
        if error:
            slot["errors"].append(str(error))

    # Cross-cut totals from diagnostics if provided.
    if diags:
        total_sources_from_diag = sum(int(d.get("result_count") or 0) for d in diags)
        total_artifacts_from_diag = sum(
            int(d.get("verified_artifact_count") or 0) for d in diags
        )
        # Diagnostics, when supplied, are the more granular source —
        # prefer them for totals so per-provider numbers stay consistent.
        total_sources = max(total_sources, total_sources_from_diag)
        total_artifacts = max(total_artifacts, total_artifacts_from_diag)

    # Warnings — deterministic order.
    for provider, slot in by_provider.items():
        if slot["calls"] > 0 and slot["result_count"] == 0 and slot["source_records"] == 0:
            warnings.append(f"zero_yield_provider:{provider}")
    if total_artifacts > 0:
        warnings.append(f"unexpected_verified_artifacts_from_smoke:{total_artifacts}")
    if total_cost > 0:
        warnings.append(f"unexpected_cost:{round(total_cost, 4)}")
    if expected_connectors:
        for expected in expected_connectors:
            if by_provider.get(expected, _per_provider_zero())["calls"] == 0:
                warnings.append(f"missing_provider:{expected}")
    warnings.sort()

    return {
        "total_runs": len(entries),
        "total_live_calls": total_live_calls,
        "total_estimated_cost_usd": round(total_cost, 4),
        "total_wallclock_seconds": round(total_wall, 4),
        "total_source_records": total_sources,
        "total_verified_artifacts": total_artifacts,
        "total_media_artifacts": total_media,
        "total_document_artifacts": total_documents,
        "by_provider": {
            provider: {**slot, "endpoints": list(slot["endpoints"]), "errors": list(slot["errors"])}
            for provider, slot in sorted(by_provider.items())
        },
        "verdict_counts": verdict_counts,
        "warnings": warnings,
    }
