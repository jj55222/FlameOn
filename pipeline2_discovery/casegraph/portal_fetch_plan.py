"""PORTAL2 - no-live seeded portal fetch planning.

This module maps calibration profile rows to bounded portal fetch plans.
It does not fetch pages, call Firecrawl, scrape, download, or authenticate;
it only records what a future controlled run would be allowed to attempt.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .calibration_profile import CalibrationProfileReport, CalibrationProfileRow, profile_calibration_set
from .calibration_replay import FIRECRAWL_LIKELY_SOURCE_TYPES, SUPPORTED_PORTAL_IDS
from .portal_profiles import PortalProfile, PortalProfileManifest, load_portal_profiles


@dataclass
class PortalFetchPlan:
    case_id: int
    title: str
    portal_profile_id: str
    seed_url: Optional[str]
    seed_url_exists: bool
    fetcher: Optional[str]
    max_pages: int
    max_links: int
    allowed_domain: Optional[str]
    expected_artifact_types: List[str] = field(default_factory=list)
    resolver_policy: Dict[str, Any] = field(default_factory=dict)
    needs_seed_url_discovery: bool = False
    blocked_reason: Optional[str] = None
    safety_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PortalFetchPlanReport:
    total_cases: int
    candidate_cases: int
    ready_for_portal_fetch_count: int
    needs_seed_url_discovery_count: int
    firecrawl_fetcher_count: int
    requests_fetcher_count: int
    plans: List[PortalFetchPlan] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_cases": self.total_cases,
            "candidate_cases": self.candidate_cases,
            "ready_for_portal_fetch_count": self.ready_for_portal_fetch_count,
            "needs_seed_url_discovery_count": self.needs_seed_url_discovery_count,
            "firecrawl_fetcher_count": self.firecrawl_fetcher_count,
            "requests_fetcher_count": self.requests_fetcher_count,
            "plans": [plan.to_dict() for plan in self.plans],
        }


def build_portal_fetch_plan_report(
    profile_report: Optional[CalibrationProfileReport] = None,
    portal_manifest: Optional[PortalProfileManifest] = None,
    *,
    repo_root: Optional[Path] = None,
) -> PortalFetchPlanReport:
    profiles = profile_report or profile_calibration_set(repo_root=repo_root)
    manifest = portal_manifest or load_portal_profiles(repo_root=repo_root)
    plans = [
        _plan_for_row(row, manifest)
        for row in profiles.profile_rows
        if _needs_seeded_portal_plan(row)
    ]
    ready = [plan for plan in plans if plan.seed_url_exists and not plan.blocked_reason]
    needs_seed = [plan for plan in plans if plan.needs_seed_url_discovery]
    return PortalFetchPlanReport(
        total_cases=profiles.total_cases,
        candidate_cases=len(plans),
        ready_for_portal_fetch_count=len(ready),
        needs_seed_url_discovery_count=len(needs_seed),
        firecrawl_fetcher_count=sum(1 for plan in ready if plan.fetcher == "firecrawl"),
        requests_fetcher_count=sum(1 for plan in ready if plan.fetcher == "requests"),
        plans=plans,
    )


def _needs_seeded_portal_plan(row: CalibrationProfileRow) -> bool:
    if set(row.source_types_already_known) & FIRECRAWL_LIKELY_SOURCE_TYPES:
        return True
    if any(profile not in SUPPORTED_PORTAL_IDS for profile in row.portal_profiles_needed):
        return True
    return False


def _plan_for_row(row: CalibrationProfileRow, manifest: PortalProfileManifest) -> PortalFetchPlan:
    profile_id = _select_portal_profile_id(row, manifest)
    profile = manifest.get(profile_id)
    seed_url = _select_seed_url(row)
    needs_seed = not bool(seed_url)
    blocked_reason = None
    if needs_seed:
        blocked_reason = "needs_seed_url_discovery"
    elif profile is None:
        blocked_reason = "portal_profile_missing"

    return PortalFetchPlan(
        case_id=row.case_id,
        title=row.title,
        portal_profile_id=profile_id,
        seed_url=seed_url,
        seed_url_exists=bool(seed_url),
        fetcher=_select_fetcher(profile, seed_url) if not blocked_reason else None,
        max_pages=profile.max_pages if profile else 0,
        max_links=profile.max_links if profile else 0,
        allowed_domain=_allowed_domain(profile, seed_url) if profile else None,
        expected_artifact_types=list(profile.expected_artifact_types if profile else row.expected_artifact_types),
        resolver_policy=dict(profile.resolver_policy if profile else {}),
        needs_seed_url_discovery=needs_seed,
        blocked_reason=blocked_reason,
        safety_flags=list(profile.safety_flags if profile else []),
    )


def _select_portal_profile_id(row: CalibrationProfileRow, manifest: PortalProfileManifest) -> str:
    source_types = set(row.source_types_already_known)
    if "agency_ois" in source_types:
        return "agency_ois_detail"
    if "official_gov" in source_types:
        return "city_critical_incident"
    if "pdf_document" in source_types:
        return "document_release_page"
    for profile_id in row.portal_profiles_needed:
        if manifest.get(profile_id):
            return profile_id
    if row.known_source_urls:
        return "document_release_page"
    return row.needed_portal_profile or "source_discovery_required"


def _select_seed_url(row: CalibrationProfileRow) -> Optional[str]:
    if not row.known_source_urls:
        return None
    source_types = set(row.source_types_already_known)
    if source_types & {"agency_ois", "official_gov", "news", "web", "social", "pdf_document"}:
        return row.known_source_urls[0]
    for url in row.known_source_urls:
        host = urlparse(url).netloc.lower()
        if "youtube" not in host and "youtu.be" not in host:
            return url
    return row.known_source_urls[0]


def _select_fetcher(profile: Optional[PortalProfile], seed_url: Optional[str]) -> Optional[str]:
    if profile is None or seed_url is None:
        return None
    if any("api" in fetcher for fetcher in profile.allowed_fetchers):
        return "requests"
    return "firecrawl"


def _allowed_domain(profile: Optional[PortalProfile], seed_url: Optional[str]) -> Optional[str]:
    if profile is None:
        return None
    if profile.allowed_domains:
        return profile.allowed_domains[0]
    if seed_url:
        return urlparse(seed_url).netloc.lower()
    return None


def portal_fetch_plan_to_jsonable(report: PortalFetchPlanReport) -> Dict[str, Any]:
    return report.to_dict()
