"""CAL2 - no-live calibration replay scoreboard.

The replay runner evaluates CAL1 profile rows against current CaseGraph
capabilities and PORTAL1 profile coverage. It does not attempt discovery;
it produces deterministic benchmark metrics and a failure taxonomy for the
next connector/portal work.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .calibration_profile import (
    CalibrationProfileReport,
    CalibrationProfileRow,
    SUPPORTED_LIVE_CONNECTORS,
    profile_calibration_set,
)
from .portal_profiles import PortalProfileManifest, load_portal_profiles


FAILURE_REASONS = (
    "missing_identity_seed",
    "missing_outcome_seed",
    "no_known_artifact_signal",
    "agency_ois_live_connector_missing",
    "portal_profile_missing",
    "supported_connector_missing",
    "claim_only_no_artifact_url",
    "document_only_expected",
    "generic_media_only",
    "tier_a_media_possible",
    "needs_firecrawl_known_url",
    "needs_seed_url_discovery",
)

FIRECRAWL_LIKELY_SOURCE_TYPES = {
    "agency_ois",
    "news",
    "official_gov",
    "pdf_document",
    "social",
    "web",
}

SUPPORTED_PORTAL_IDS = {
    "youtube_agency_channel",
    "muckrock_request",
    "documentcloud_search",
    "courtlistener_search",
}


@dataclass
class CalibrationReplayCase:
    case_id: int
    title: str
    identity_ready: bool
    outcome_ready: bool
    supported_live_path_available: bool
    likely_connector_path: List[str] = field(default_factory=list)
    portal_profiles_needed: List[str] = field(default_factory=list)
    failure_reasons: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CalibrationReplayResult:
    total_cases: int
    case_results: List[CalibrationReplayCase] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    failure_reason_counts: Dict[str, int] = field(default_factory=dict)
    top_next_work_items: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_cases": self.total_cases,
            "case_results": [case.to_dict() for case in self.case_results],
            "metrics": dict(self.metrics),
            "failure_reason_counts": dict(self.failure_reason_counts),
            "top_next_work_items": list(self.top_next_work_items),
        }


def run_calibration_replay(
    profile_report: Optional[CalibrationProfileReport] = None,
    portal_manifest: Optional[PortalProfileManifest] = None,
    *,
    repo_root: Optional[Path] = None,
) -> CalibrationReplayResult:
    profiles = profile_report or profile_calibration_set(repo_root=repo_root)
    portals = portal_manifest or load_portal_profiles(repo_root=repo_root)
    known_portals = set(portals.profile_ids)

    case_results = [_replay_case(row, known_portals) for row in profiles.profile_rows]
    metrics = _metrics(profiles.profile_rows, case_results)
    failure_counts = _failure_counts(case_results)
    top_items = _top_next_work_items(failure_counts)
    return CalibrationReplayResult(
        total_cases=len(case_results),
        case_results=case_results,
        metrics=metrics,
        failure_reason_counts=failure_counts,
        top_next_work_items=top_items,
    )


def _replay_case(row: CalibrationProfileRow, known_portals: Sequence[str]) -> CalibrationReplayCase:
    identity_ready = bool(row.title.strip())
    outcome_ready = row.outcome_status.lower() not in {"", "unknown", "unresolved"}
    missing_portals = [
        profile_id
        for profile_id in row.portal_profiles_needed
        if profile_id not in known_portals
    ]
    failures: List[str] = []

    if not identity_ready:
        failures.append("missing_identity_seed")
    if not outcome_ready:
        failures.append("missing_outcome_seed")
    if "no_known_artifact_signal" in row.risk_flags:
        failures.append("no_known_artifact_signal")
    if _has_agency_ois_gap(row):
        failures.append("agency_ois_live_connector_missing")
    if missing_portals:
        failures.append("portal_profile_missing")
    if not row.supported_live_path_available and row.expected_artifact_types:
        failures.append("supported_connector_missing")
    if row.expected_artifact_types and not row.known_source_urls:
        failures.append("claim_only_no_artifact_url")
    if "document_only_expected" in row.risk_flags:
        failures.append("document_only_expected")
    if "generic_media_only" in row.risk_flags:
        failures.append("generic_media_only")
    if "tier_a_primary_media_case" in row.benchmark_roles:
        failures.append("tier_a_media_possible")
    if _needs_firecrawl_known_url(row):
        failures.append("needs_firecrawl_known_url")
    if "needs_seed_url_discovery" in row.risk_flags:
        failures.append("needs_seed_url_discovery")

    failures = [reason for reason in FAILURE_REASONS if reason in failures]
    return CalibrationReplayCase(
        case_id=row.case_id,
        title=row.title,
        identity_ready=identity_ready,
        outcome_ready=outcome_ready,
        supported_live_path_available=row.supported_live_path_available,
        likely_connector_path=list(row.likely_connector_path),
        portal_profiles_needed=list(row.portal_profiles_needed),
        failure_reasons=failures,
        next_actions=_next_actions(failures),
    )


def _metrics(
    rows: Sequence[CalibrationProfileRow],
    case_results: Sequence[CalibrationReplayCase],
) -> Dict[str, Any]:
    return {
        "total_cases": len(rows),
        "profileable_cases": len(case_results),
        "identity_ready_count": sum(1 for case in case_results if case.identity_ready),
        "outcome_ready_count": sum(1 for case in case_results if case.outcome_ready),
        "document_artifact_expected_count": sum(1 for row in rows if "document_artifact_case" in row.benchmark_roles),
        "media_artifact_expected_count": sum(1 for row in rows if "media_artifact_case" in row.benchmark_roles),
        "tier_a_media_expected_count": sum(1 for row in rows if "tier_a_primary_media_case" in row.benchmark_roles),
        "supported_live_path_count": sum(1 for row in rows if row.supported_live_path_available),
        "unsupported_portal_profile_count": sum(
            1 for case in case_results if "portal_profile_missing" in case.failure_reasons
        ),
        "agency_ois_only_count": sum(1 for row in rows if _has_agency_ois_gap(row)),
        "youtube_supported_count": _count_connector(rows, "youtube"),
        "muckrock_supported_count": _count_connector(rows, "muckrock"),
        "documentcloud_supported_count": _count_connector(rows, "documentcloud"),
        "courtlistener_supported_count": _count_connector(rows, "courtlistener"),
        "likely_firecrawl_needed_count": sum(1 for row in rows if _needs_firecrawl_known_url(row)),
        "ready_for_portal_fetch_count": sum(
            1 for row in rows if _needs_firecrawl_known_url(row) and bool(row.known_source_urls)
        ),
        "needs_seed_url_discovery_count": sum(
            1 for row in rows if _needs_firecrawl_known_url(row) and not row.known_source_urls
        ),
        "failure_reason_counts": _failure_counts(case_results),
    }


def _failure_counts(case_results: Sequence[CalibrationReplayCase]) -> Dict[str, int]:
    counts = {reason: 0 for reason in FAILURE_REASONS}
    for case in case_results:
        for reason in case.failure_reasons:
            counts[reason] += 1
    return counts


def _count_connector(rows: Sequence[CalibrationProfileRow], connector: str) -> int:
    return sum(1 for row in rows if connector in row.likely_connector_path)


def _has_agency_ois_gap(row: CalibrationProfileRow) -> bool:
    return "agency_ois" in row.likely_connector_path and "agency_ois" not in SUPPORTED_LIVE_CONNECTORS


def _needs_firecrawl_known_url(row: CalibrationProfileRow) -> bool:
    if set(row.source_types_already_known) & FIRECRAWL_LIKELY_SOURCE_TYPES:
        return True
    return any(profile not in SUPPORTED_PORTAL_IDS for profile in row.portal_profiles_needed)


def _next_actions(failures: Sequence[str]) -> List[str]:
    actions: List[str] = []
    if "missing_outcome_seed" in failures:
        actions.append("add_outcome_corroboration_path")
    if "tier_a_media_possible" in failures:
        actions.append("verify_primary_media_artifact_url")
    if "agency_ois_live_connector_missing" in failures:
        actions.append("implement_seeded_agency_ois_connector")
    if "needs_firecrawl_known_url" in failures:
        actions.append("add_seeded_portal_fetch_profile")
    if "needs_seed_url_discovery" in failures:
        actions.append("add_seed_url_discovery_or_curated_seed")
    if "document_only_expected" in failures:
        actions.append("treat_as_research_hold_without_media")
    if "no_known_artifact_signal" in failures:
        actions.append("park_until_artifact_signal_exists")
    return list(dict.fromkeys(actions))


def _top_next_work_items(failure_counts: Dict[str, int]) -> List[Dict[str, Any]]:
    labels = {
        "missing_outcome_seed": "Outcome corroboration for calibration replay",
        "tier_a_media_possible": "Tier A primary-media verification path",
        "needs_firecrawl_known_url": "Seeded jurisdiction portal fetch profile",
        "agency_ois_live_connector_missing": "Agency OIS live connector",
        "needs_seed_url_discovery": "Seed URL discovery or curated seeds",
        "no_known_artifact_signal": "Negative/no-artifact benchmark handling",
    }
    ranked: List[Dict[str, Any]] = []
    for reason, count in sorted(failure_counts.items(), key=lambda item: (-item[1], item[0])):
        if count <= 0 or reason not in labels:
            continue
        ranked.append({"reason": reason, "count": count, "recommended_work": labels[reason]})
    return ranked[:5]


def replay_to_jsonable(result: CalibrationReplayResult) -> Dict[str, Any]:
    return result.to_dict()
