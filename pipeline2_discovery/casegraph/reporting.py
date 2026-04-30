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
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .ledger import DEFAULT_API_CALLS, RunLedgerEntry
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
