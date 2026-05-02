"""OUTCOME2 - no-live outcome corroboration seed planning.

The calibration profile intentionally leaves outcome status unknown
unless an existing deterministic seed proves it. This module does not
resolve those outcomes; it records where a bounded corroboration pass
should look next, using only calibration rows and portal-profile
metadata.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .calibration_profile import CalibrationProfileReport, CalibrationProfileRow, profile_calibration_set
from .portal_profiles import PortalProfileManifest, load_portal_profiles


SUPPORTED_OUTCOME_SOURCES = {
    "courtlistener",
    "DocumentCloud",
}


@dataclass
class OutcomeSeedPlan:
    case_id: int
    title: str
    jurisdiction: str
    state: Optional[str]
    agency: Optional[str]
    current_outcome_seed_status: str
    missing_outcome_reason: Optional[str]
    recommended_outcome_sources: List[str] = field(default_factory=list)
    suggested_deterministic_query_seeds: List[str] = field(default_factory=list)
    likely_portal_profile: str = "source_discovery_required"
    supported_live_path_available: bool = False
    priority: str = "low"
    blocker: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class OutcomeSeedPlanReport:
    total_cases: int
    unresolved_outcome_count: int
    outcome_plan_ready_count: int
    manual_seed_needed_count: int
    plans: List[OutcomeSeedPlan] = field(default_factory=list)
    source_counts: Dict[str, int] = field(default_factory=dict)
    priority_counts: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_cases": self.total_cases,
            "unresolved_outcome_count": self.unresolved_outcome_count,
            "outcome_plan_ready_count": self.outcome_plan_ready_count,
            "manual_seed_needed_count": self.manual_seed_needed_count,
            "plans": [plan.to_dict() for plan in self.plans],
            "source_counts": dict(self.source_counts),
            "priority_counts": dict(self.priority_counts),
        }


def build_outcome_seed_plan_report(
    profile_report: Optional[CalibrationProfileReport] = None,
    portal_manifest: Optional[PortalProfileManifest] = None,
    *,
    repo_root: Optional[Path] = None,
) -> OutcomeSeedPlanReport:
    """Create no-live outcome-corroboration plans for unknown outcomes."""

    profiles = profile_report or profile_calibration_set(repo_root=repo_root)
    manifest = portal_manifest or load_portal_profiles(repo_root=repo_root)
    known_portals = set(manifest.profile_ids)
    plans = [
        _plan_for_row(row, known_portals)
        for row in profiles.profile_rows
        if row.outcome_seed_status == "unknown"
    ]
    return OutcomeSeedPlanReport(
        total_cases=profiles.total_cases,
        unresolved_outcome_count=len(plans),
        outcome_plan_ready_count=sum(1 for plan in plans if _is_plan_ready(plan)),
        manual_seed_needed_count=sum(
            1 for plan in plans if plan.recommended_outcome_sources == ["manual seed needed"]
        ),
        plans=plans,
        source_counts=_source_counts(plans),
        priority_counts=_priority_counts(plans),
    )


def outcome_seed_plan_to_jsonable(report: OutcomeSeedPlanReport) -> Dict[str, Any]:
    return report.to_dict()


def _plan_for_row(row: CalibrationProfileRow, known_portals: Sequence[str]) -> OutcomeSeedPlan:
    sources = _recommended_sources(row)
    queries = _query_seeds(row)
    portal = _likely_portal_profile(row, known_portals)
    supported = _supported_live_path_available(row, sources)
    blocker = _blocker(row, sources=sources, queries=queries, portal=portal, known_portals=known_portals)

    return OutcomeSeedPlan(
        case_id=row.case_id,
        title=row.title,
        jurisdiction=row.jurisdiction,
        state=row.state,
        agency=row.agency,
        current_outcome_seed_status=row.outcome_seed_status,
        missing_outcome_reason=row.missing_outcome_reason,
        recommended_outcome_sources=sources,
        suggested_deterministic_query_seeds=queries,
        likely_portal_profile=portal,
        supported_live_path_available=supported,
        priority=_priority(row, sources=sources, supported=supported),
        blocker=blocker,
    )


def _recommended_sources(row: CalibrationProfileRow) -> List[str]:
    sources: List[str] = []
    source_types = set(row.source_types_already_known)
    portals = set(row.portal_profiles_needed)
    connectors = set(row.likely_connector_path)

    if "courtlistener" in connectors or "courtlistener" in source_types:
        sources.append("courtlistener")
    if source_types & {"court_public"} or "docket_docs" in row.expected_artifact_types or "court_case_detail" in portals:
        sources.append("court_docket_search")
    if row.state or row.jurisdiction:
        sources.append("county/state court portal")
    if source_types & {"official_gov", "agency_ois"}:
        sources.append("DA press release")
    if "news" in source_types:
        sources.append("local news")
    if "documentcloud" in connectors or "documentcloud" in source_types:
        sources.append("DocumentCloud")
    if row.known_source_urls and not (source_types & {"court_public", "news", "official_gov", "agency_ois", "documentcloud"}):
        sources.append("local news")
    if not sources:
        sources.append("manual seed needed")
    return list(dict.fromkeys(sources))


def _query_seeds(row: CalibrationProfileRow) -> List[str]:
    names = [part.strip() for part in row.title.split(",") if part.strip()]
    lead_name = names[0] if names else row.title.strip()
    if not lead_name:
        return []

    location = _compact_location(row.jurisdiction, row.state)
    base = " ".join(part for part in (lead_name, location) if part)
    terms = ("sentenced", "convicted", "court docket", "case status")
    queries = [f"{base} {term}".strip() for term in terms]
    if row.agency:
        queries.append(f"{lead_name} {row.agency} outcome")
    return list(dict.fromkeys(queries))


def _compact_location(jurisdiction: str, state: Optional[str]) -> str:
    pieces = [piece.strip() for piece in (jurisdiction or "").split(",") if piece.strip()]
    if state and state not in pieces:
        pieces.append(state)
    compact = " ".join(pieces[:2])
    return re.sub(r"\s+", " ", compact).strip()


def _likely_portal_profile(row: CalibrationProfileRow, known_portals: Sequence[str]) -> str:
    for candidate in ("courtlistener_search", "court_docket_search", "court_case_detail", "documentcloud_search"):
        if candidate in row.portal_profiles_needed and candidate in known_portals:
            return candidate
    if "agency_ois" in row.source_types_already_known and "da_critical_incident" in known_portals:
        return "da_critical_incident"
    if row.known_source_urls and row.needed_portal_profile in known_portals:
        return row.needed_portal_profile
    return "court_docket_search" if "court_docket_search" in known_portals else "source_discovery_required"


def _supported_live_path_available(row: CalibrationProfileRow, sources: Sequence[str]) -> bool:
    if row.supported_live_path_available:
        return True
    return bool(set(sources) & SUPPORTED_OUTCOME_SOURCES)


def _blocker(
    row: CalibrationProfileRow,
    *,
    sources: Sequence[str],
    queries: Sequence[str],
    portal: str,
    known_portals: Sequence[str],
) -> Optional[str]:
    if not queries:
        return "manual_seed_needed"
    if sources == ["manual seed needed"]:
        return "manual_seed_needed"
    if portal not in known_portals:
        return "portal_profile_missing"
    if not row.known_source_urls and not row.supported_live_path_available:
        return "needs_seed_url_discovery"
    return None


def _priority(row: CalibrationProfileRow, *, sources: Sequence[str], supported: bool) -> str:
    if "tier_a_primary_media_case" in row.benchmark_roles:
        return "high"
    if supported or set(sources) & {"courtlistener", "court_docket_search", "DocumentCloud"}:
        return "medium"
    return "low"


def _is_plan_ready(plan: OutcomeSeedPlan) -> bool:
    return bool(plan.suggested_deterministic_query_seeds) and plan.recommended_outcome_sources != ["manual seed needed"]


def _source_counts(plans: Iterable[OutcomeSeedPlan]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for plan in plans:
        for source in plan.recommended_outcome_sources:
            counts[source] = counts.get(source, 0) + 1
    return dict(sorted(counts.items()))


def _priority_counts(plans: Iterable[OutcomeSeedPlan]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for plan in plans:
        counts[plan.priority] = counts.get(plan.priority, 0) + 1
    return dict(sorted(counts.items()))
