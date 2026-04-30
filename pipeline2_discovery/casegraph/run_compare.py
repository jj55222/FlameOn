"""EVAL5 — pure no-live comparison of two CaseGraph run bundles.

Given two bundle dicts produced by :func:`pipeline2_discovery.casegraph.cli.build_run_bundle`
(or by ``cli.py --bundle-out``), this module emits a deterministic,
JSON-serializable diff covering:

- query plan differences (connector counts, added / removed connectors,
  added / removed query strings)
- source count differences (overall and per-API)
- provider yield differences (api_calls per provider)
- artifact yield differences (totals, per artifact_type, media vs
  document)
- cost / runtime deltas (from the ledger entries)
- verdict changes (whether the enriched run flipped HOLD ⇄ PRODUCE
  ⇄ SKIP)
- reason code / risk flag adds / removes
- why_not_produce summary (only emitted when the enriched verdict is
  not PRODUCE — surfaces the deterministic blockers from the
  enriched bundle's risk_flags + reason_codes + identity / outcome
  sections so reviewers can see at a glance why the run did not
  graduate)

The function is pure: it does not call the network, run any scorer,
or mutate either bundle. All inputs are read-only dicts; outputs are
plain dicts / lists / scalars suitable for ``json.dumps``.

Both inputs may have missing or ``None`` sections (e.g. a query-plan-
only bundle has no ``verified_artifacts`` data); the comparator falls
back to zero counts and empty deltas in that case rather than raising.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional

from .scoring import MEDIA_ARTIFACT_TYPES, MEDIA_FORMATS


CONCLUDED_OUTCOMES = frozenset({"sentenced", "closed", "convicted"})

# Blocking signals that downstream consumers care about most when
# reviewing why a run did not graduate to PRODUCE. We surface each
# distinct blocker as a flag in the why_not_produce summary so the
# caller can route accordingly.
BLOCKING_RISK_FLAGS = frozenset(
    {
        "weak_identity",
        "no_verified_media",
        "protected_or_nonpublic",
        "protected_or_nonpublic_only",
        "pacer_or_paywalled",
    }
)
BLOCKING_REASON_CODES = frozenset(
    {
        "document_only_hold",
        "claim_only_hold",
        "weak_identity_block",
        "outcome_not_concluded",
    }
)


def _is_media_artifact(artifact: Mapping[str, Any]) -> bool:
    return (
        artifact.get("artifact_type") in MEDIA_ARTIFACT_TYPES
        or artifact.get("format") in MEDIA_FORMATS
    )


def _bundle_query_plan(bundle: Mapping[str, Any]) -> Mapping[str, Any]:
    plan = bundle.get("query_plan") or {}
    if not isinstance(plan, Mapping):
        return {}
    return plan


def _connectors_in_plan(plan: Mapping[str, Any]) -> List[str]:
    plans = plan.get("plans") or []
    if not isinstance(plans, list):
        return []
    return [str(entry.get("connector") or "") for entry in plans if entry.get("connector")]


def _queries_in_plan(plan: Mapping[str, Any]) -> List[str]:
    queries: List[str] = []
    plans = plan.get("plans") or []
    if not isinstance(plans, list):
        return []
    for entry in plans:
        for query_dict in entry.get("queries") or []:
            text = query_dict.get("query")
            if text:
                queries.append(str(text))
    return queries


def _bundle_sources_by_api(bundle: Mapping[str, Any]) -> Dict[str, int]:
    summary = bundle.get("connector_summary") or {}
    if not isinstance(summary, Mapping):
        return {}
    by_api = summary.get("by_api") or {}
    if not isinstance(by_api, Mapping):
        return {}
    return {str(k): int(v) for k, v in by_api.items()}


def _bundle_total_sources(bundle: Mapping[str, Any]) -> int:
    summary = bundle.get("connector_summary") or {}
    if isinstance(summary, Mapping):
        total = summary.get("total_source_records")
        if isinstance(total, int):
            return total
    multi = bundle.get("multi_source_summary") or {}
    if isinstance(multi, Mapping):
        total = multi.get("total_source_records")
        if isinstance(total, int):
            return total
    return 0


def _bundle_provider_calls(bundle: Mapping[str, Any]) -> Dict[str, int]:
    led = bundle.get("ledger_entry") or {}
    if not isinstance(led, Mapping):
        return {}
    api_calls = led.get("api_calls") or {}
    if not isinstance(api_calls, Mapping):
        return {}
    return {str(k): int(v) for k, v in api_calls.items()}


def _bundle_verified_artifacts(bundle: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    artifacts = bundle.get("verified_artifacts") or []
    if not isinstance(artifacts, list):
        return []
    return [a for a in artifacts if isinstance(a, Mapping)]


def _bundle_verdict(bundle: Mapping[str, Any]) -> Optional[str]:
    result = bundle.get("result") or {}
    if isinstance(result, Mapping):
        verdict = result.get("verdict")
        if verdict:
            return str(verdict)
    return None


def _bundle_reason_codes(bundle: Mapping[str, Any]) -> List[str]:
    result = bundle.get("result") or {}
    if isinstance(result, Mapping):
        codes = result.get("reason_codes") or []
        if isinstance(codes, list):
            return [str(c) for c in codes]
    return []


def _bundle_risk_flags(bundle: Mapping[str, Any]) -> List[str]:
    result = bundle.get("result") or {}
    if isinstance(result, Mapping):
        flags = result.get("risk_flags") or []
        if isinstance(flags, list):
            return [str(f) for f in flags]
    # Fallback to the top-level risk_flags if no scored result exists.
    flags = bundle.get("risk_flags") or []
    if isinstance(flags, list):
        return [str(f) for f in flags]
    return []


def _bundle_cost(bundle: Mapping[str, Any]) -> float:
    led = bundle.get("ledger_entry") or {}
    if isinstance(led, Mapping):
        return float(led.get("estimated_cost_usd") or 0.0)
    return 0.0


def _bundle_runtime(bundle: Mapping[str, Any]) -> float:
    led = bundle.get("ledger_entry") or {}
    if isinstance(led, Mapping):
        return float(led.get("wallclock_seconds") or 0.0)
    return 0.0


def _bundle_identity_confidence(bundle: Mapping[str, Any]) -> Optional[str]:
    identity = bundle.get("identity") or {}
    if isinstance(identity, Mapping):
        value = identity.get("identity_confidence")
        if value:
            return str(value)
    return None


def _bundle_outcome_status(bundle: Mapping[str, Any]) -> Optional[str]:
    outcome = bundle.get("outcome") or {}
    if isinstance(outcome, Mapping):
        value = outcome.get("outcome_status")
        if value:
            return str(value)
    return None


def _set_diff(left: Iterable[str], right: Iterable[str]) -> Dict[str, List[str]]:
    left_set = set(left)
    right_set = set(right)
    return {
        "added": sorted(right_set - left_set),
        "removed": sorted(left_set - right_set),
        "shared": sorted(left_set & right_set),
    }


def _list_diff(left: Iterable[str], right: Iterable[str]) -> Dict[str, List[str]]:
    """Like _set_diff but also tracks duplicates by treating the two
    sides as multisets when computing what was *added*.

    Currently we treat them as sets — duplicate queries within one
    bundle don't add signal."""
    return _set_diff(left, right)


def _per_key_delta(left: Mapping[str, int], right: Mapping[str, int]) -> Dict[str, Dict[str, int]]:
    keys = set(left) | set(right)
    return {
        key: {
            "dry": int(left.get(key, 0)),
            "enriched": int(right.get(key, 0)),
            "delta": int(right.get(key, 0)) - int(left.get(key, 0)),
        }
        for key in sorted(keys)
    }


def _artifact_type_counts(artifacts: List[Mapping[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for artifact in artifacts:
        artifact_type = str(artifact.get("artifact_type") or "unknown")
        counts[artifact_type] = counts.get(artifact_type, 0) + 1
    return counts


def _media_document_counts(artifacts: List[Mapping[str, Any]]) -> Dict[str, int]:
    media = sum(1 for a in artifacts if _is_media_artifact(a))
    return {"media": media, "document": len(artifacts) - media}


def _why_not_produce(enriched: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    verdict = _bundle_verdict(enriched)
    if verdict == "PRODUCE":
        return None

    risk_flags = _bundle_risk_flags(enriched)
    reason_codes = _bundle_reason_codes(enriched)
    artifacts = _bundle_verified_artifacts(enriched)
    has_media = any(_is_media_artifact(a) for a in artifacts)
    identity_confidence = _bundle_identity_confidence(enriched)
    outcome_status = _bundle_outcome_status(enriched)

    blocking_risk = sorted(set(risk_flags) & BLOCKING_RISK_FLAGS)
    blocking_codes = sorted(set(reason_codes) & BLOCKING_REASON_CODES)

    return {
        "verdict": verdict or "unknown",
        "blocking_risk_flags": blocking_risk,
        "blocking_reason_codes": blocking_codes,
        "missing_media_artifacts": not has_media,
        "missing_high_identity": identity_confidence != "high",
        "missing_concluded_outcome": (
            outcome_status is not None and outcome_status not in CONCLUDED_OUTCOMES
        ),
    }


def compare_run_bundles(
    dry: Mapping[str, Any],
    enriched: Mapping[str, Any],
) -> Dict[str, Any]:
    """Compare a baseline ('dry') bundle to a comparison ('enriched')
    bundle and emit a structured, JSON-serializable diff.

    Both inputs are tolerant of missing / ``None`` sections: each
    accessor falls back to zero / empty values rather than raising.
    """

    dry_plan = _bundle_query_plan(dry)
    enr_plan = _bundle_query_plan(enriched)
    dry_connectors = _connectors_in_plan(dry_plan)
    enr_connectors = _connectors_in_plan(enr_plan)
    dry_queries = _queries_in_plan(dry_plan)
    enr_queries = _queries_in_plan(enr_plan)

    dry_by_api = _bundle_sources_by_api(dry)
    enr_by_api = _bundle_sources_by_api(enriched)
    dry_total_sources = _bundle_total_sources(dry)
    enr_total_sources = _bundle_total_sources(enriched)

    dry_calls = _bundle_provider_calls(dry)
    enr_calls = _bundle_provider_calls(enriched)

    dry_artifacts = _bundle_verified_artifacts(dry)
    enr_artifacts = _bundle_verified_artifacts(enriched)
    dry_type_counts = _artifact_type_counts(dry_artifacts)
    enr_type_counts = _artifact_type_counts(enr_artifacts)
    dry_md = _media_document_counts(dry_artifacts)
    enr_md = _media_document_counts(enr_artifacts)

    dry_cost = _bundle_cost(dry)
    enr_cost = _bundle_cost(enriched)
    dry_runtime = _bundle_runtime(dry)
    enr_runtime = _bundle_runtime(enriched)

    dry_verdict = _bundle_verdict(dry)
    enr_verdict = _bundle_verdict(enriched)

    return {
        "experiment_ids": {
            "dry": dry.get("experiment_id"),
            "enriched": enriched.get("experiment_id"),
        },
        "modes": {
            "dry": dry.get("mode"),
            "enriched": enriched.get("mode"),
        },
        "query_plan_delta": {
            "dry_connector_count": int(dry_plan.get("connector_count") or len(dry_connectors)),
            "enriched_connector_count": int(
                enr_plan.get("connector_count") or len(enr_connectors)
            ),
            "delta_connector_count": (
                int(enr_plan.get("connector_count") or len(enr_connectors))
                - int(dry_plan.get("connector_count") or len(dry_connectors))
            ),
            "connectors": _set_diff(dry_connectors, enr_connectors),
            "queries": _list_diff(dry_queries, enr_queries),
        },
        "source_count_delta": {
            "dry": dry_total_sources,
            "enriched": enr_total_sources,
            "delta": enr_total_sources - dry_total_sources,
            "by_api": _per_key_delta(dry_by_api, enr_by_api),
        },
        "provider_yield_delta": {
            "by_provider": _per_key_delta(dry_calls, enr_calls),
            "total_calls": {
                "dry": sum(dry_calls.values()),
                "enriched": sum(enr_calls.values()),
                "delta": sum(enr_calls.values()) - sum(dry_calls.values()),
            },
        },
        "artifact_yield_delta": {
            "dry_total": len(dry_artifacts),
            "enriched_total": len(enr_artifacts),
            "delta": len(enr_artifacts) - len(dry_artifacts),
            "by_type": _per_key_delta(dry_type_counts, enr_type_counts),
            "media_delta": {
                "dry": dry_md["media"],
                "enriched": enr_md["media"],
                "delta": enr_md["media"] - dry_md["media"],
            },
            "document_delta": {
                "dry": dry_md["document"],
                "enriched": enr_md["document"],
                "delta": enr_md["document"] - dry_md["document"],
            },
        },
        "cost_delta": {
            "dry": round(dry_cost, 4),
            "enriched": round(enr_cost, 4),
            "delta": round(enr_cost - dry_cost, 4),
        },
        "runtime_delta": {
            "dry": round(dry_runtime, 4),
            "enriched": round(enr_runtime, 4),
            "delta": round(enr_runtime - dry_runtime, 4),
        },
        "verdict_change": {
            "dry": dry_verdict,
            "enriched": enr_verdict,
            "changed": (dry_verdict is not None and enr_verdict is not None and dry_verdict != enr_verdict),
        },
        "reason_code_delta": _set_diff(
            _bundle_reason_codes(dry), _bundle_reason_codes(enriched)
        ),
        "risk_flag_delta": _set_diff(
            _bundle_risk_flags(dry), _bundle_risk_flags(enriched)
        ),
        "why_not_produce": _why_not_produce(enriched),
    }
