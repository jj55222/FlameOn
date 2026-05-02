"""PORTAL1 - jurisdiction/source portal profile manifest.

Portal profiles describe how future seeded portal work should be bounded:
which authority type a page represents, which metadata-only fetchers are
allowed, caps, protected URL patterns, and resolver expectations.

Pure/no-live: loading and validation only; this module does not fetch,
scrape, download, authenticate, or call external services.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional


REQUIRED_PROFILE_IDS = {
    "agency_ois_listing",
    "agency_ois_detail",
    "da_critical_incident",
    "city_critical_incident",
    "sheriff_critical_incident",
    "court_docket_search",
    "court_case_detail",
    "foia_request_page",
    "document_release_page",
    "youtube_agency_channel",
    "vimeo_agency_channel",
    "documentcloud_search",
    "muckrock_request",
    "courtlistener_search",
}

MAX_DEFAULT_PAGES = 3
MAX_DEFAULT_LINKS = 50
BLOCKED_ACCESS_PATTERNS = ("login", "signin", "auth", "private", "protected", "password", "paywall")


@dataclass
class PortalProfile:
    profile_id: str
    source_authority: str
    allowed_fetchers: List[str] = field(default_factory=list)
    max_pages: int = MAX_DEFAULT_PAGES
    max_links: int = MAX_DEFAULT_LINKS
    domain_policy: str = "seeded_domain_only"
    allowed_domains: List[str] = field(default_factory=list)
    blocked_url_patterns: List[str] = field(default_factory=list)
    protected_private_login_patterns: List[str] = field(default_factory=list)
    media_url_patterns: List[str] = field(default_factory=list)
    document_url_patterns: List[str] = field(default_factory=list)
    identity_patterns: List[str] = field(default_factory=list)
    outcome_patterns: List[str] = field(default_factory=list)
    artifact_claim_patterns: List[str] = field(default_factory=list)
    expected_artifact_types: List[str] = field(default_factory=list)
    resolver_policy: Dict[str, Any] = field(default_factory=dict)
    next_actions: List[str] = field(default_factory=list)
    safety_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PortalProfileManifest:
    version: int
    profiles: List[PortalProfile] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "profiles": [profile.to_dict() for profile in self.profiles],
            "warnings": list(self.warnings),
        }

    def get(self, profile_id: str) -> Optional[PortalProfile]:
        for profile in self.profiles:
            if profile.profile_id == profile_id:
                return profile
        return None

    @property
    def profile_ids(self) -> List[str]:
        return [profile.profile_id for profile in self.profiles]


def default_portal_profiles_path(repo_root: Optional[Path] = None) -> Path:
    root = Path(repo_root) if repo_root else Path(__file__).resolve().parents[2]
    return root / "tests" / "fixtures" / "portal_profiles" / "portal_profiles.json"


def load_portal_profiles(path: Optional[Path] = None, *, repo_root: Optional[Path] = None) -> PortalProfileManifest:
    manifest_path = Path(path) if path is not None else default_portal_profiles_path(repo_root)
    with manifest_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    profiles = [
        PortalProfile(**profile)
        for profile in data.get("profiles", [])
        if isinstance(profile, Mapping)
    ]
    manifest = PortalProfileManifest(
        version=int(data.get("version", 1)),
        profiles=profiles,
        warnings=list(data.get("warnings", [])),
    )
    manifest.warnings.extend(validate_portal_profiles(manifest))
    return manifest


def validate_portal_profiles(manifest: PortalProfileManifest) -> List[str]:
    warnings: List[str] = []
    ids = manifest.profile_ids
    duplicate_ids = sorted({profile_id for profile_id in ids if ids.count(profile_id) > 1})
    for profile_id in duplicate_ids:
        warnings.append(f"duplicate_profile_id:{profile_id}")

    missing = sorted(REQUIRED_PROFILE_IDS - set(ids))
    for profile_id in missing:
        warnings.append(f"missing_profile_id:{profile_id}")

    for profile in manifest.profiles:
        if profile.max_pages > MAX_DEFAULT_PAGES:
            warnings.append(f"max_pages_exceeds_cap:{profile.profile_id}")
        if profile.max_links > MAX_DEFAULT_LINKS:
            warnings.append(f"max_links_exceeds_cap:{profile.profile_id}")
        if _allows_downloads(profile):
            warnings.append(f"downloads_allowed:{profile.profile_id}")
        if _allows_private_or_login(profile):
            warnings.append(f"private_or_login_allowed:{profile.profile_id}")
        if not profile.blocked_url_patterns:
            warnings.append(f"missing_blocked_patterns:{profile.profile_id}")
        if not profile.protected_private_login_patterns:
            warnings.append(f"missing_protected_patterns:{profile.profile_id}")
        if not profile.resolver_policy.get("metadata_only", False):
            warnings.append(f"resolver_not_metadata_only:{profile.profile_id}")
        if not profile.resolver_policy.get("require_public_url", False):
            warnings.append(f"resolver_missing_public_url_requirement:{profile.profile_id}")
    return warnings


def profiles_for_expected_artifact(
    manifest: PortalProfileManifest,
    artifact_type: str,
) -> List[PortalProfile]:
    return [
        profile
        for profile in manifest.profiles
        if artifact_type in profile.expected_artifact_types
    ]


def _allows_downloads(profile: PortalProfile) -> bool:
    if profile.resolver_policy.get("allow_downloads") is True:
        return True
    return any("download" in fetcher.lower() for fetcher in profile.allowed_fetchers)


def _allows_private_or_login(profile: PortalProfile) -> bool:
    policy_allows_login = profile.resolver_policy.get("allow_login") is True
    required_patterns = set(BLOCKED_ACCESS_PATTERNS)
    blocked = {pattern.lower() for pattern in profile.blocked_url_patterns}
    protected = {pattern.lower() for pattern in profile.protected_private_login_patterns}
    return policy_allows_login or not required_patterns.issubset(blocked | protected)


def manifest_to_jsonable(manifest: PortalProfileManifest) -> Dict[str, Any]:
    return manifest.to_dict()

