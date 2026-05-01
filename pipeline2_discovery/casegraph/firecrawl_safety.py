"""FIRE1 - no-live safety wrapper for future seeded portal fetches.

This module validates whether a known URL could be fetched by a future
requests/Firecrawl pass. It never performs the fetch. The decision is a
pure diagnostic object suitable for tests, replay reports, and live-run
preflight checks.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import os
from pathlib import Path
from typing import Any, Dict, Mapping, Optional
from urllib.parse import urlparse

from .portal_profiles import BLOCKED_ACCESS_PATTERNS, PortalProfileManifest, load_portal_profiles


@dataclass
class PortalFetchSafetyRequest:
    url: str
    profile_id: str
    fetcher: str = "firecrawl"
    max_pages: int = 1
    max_links: int = 10
    known_url: bool = True
    dry_run: bool = True
    live_env_gate: bool = False
    broad_search_mode: bool = False
    allow_downloads: bool = False
    allow_private_or_login: bool = False
    allow_llm: bool = False
    download_intent: bool = False


@dataclass
class FetchSafetyDecision:
    fetch_allowed: bool
    blocked_reason: Optional[str]
    fetcher: str
    url: str
    profile_id: str
    max_pages: int
    max_links: int
    estimated_cost: float = 0.0
    safety_flags: list[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class KnownUrlLiveSmokeTarget:
    target_id: str
    url: str
    profile_id: str
    fetcher: str = "firecrawl"
    max_pages: int = 1
    max_links: int = 10


@dataclass
class KnownUrlLiveSmokeDecision:
    target_id: str
    execution_status: str
    skip_reason: Optional[str]
    required_env_vars: list[str]
    safety_preflight_status: str
    safety_decision: Dict[str, Any]
    live_call_allowed: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def evaluate_fetch_safety(
    request: PortalFetchSafetyRequest,
    portal_manifest: Optional[PortalProfileManifest] = None,
    *,
    repo_root: Optional[Path] = None,
) -> FetchSafetyDecision:
    manifest = portal_manifest or load_portal_profiles(repo_root=repo_root)
    profile = manifest.get(request.profile_id)
    flags: list[str] = []

    if profile is None:
        return _blocked(request, "portal_profile_missing", flags)

    flags.extend(profile.safety_flags)
    parsed = urlparse(request.url or "")
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return _blocked(request, "invalid_public_url", flags)
    if not request.known_url:
        return _blocked(request, "known_seed_url_required", flags)
    if request.broad_search_mode:
        return _blocked(request, "broad_search_not_allowed", flags)
    if request.allow_llm:
        return _blocked(request, "llm_extraction_not_allowed", flags)
    if request.allow_downloads or request.download_intent:
        return _blocked(request, "downloads_not_allowed", flags)
    if request.allow_private_or_login or _matches_protected_pattern(request.url, profile):
        return _blocked(request, "private_or_login_not_allowed", flags)
    if not request.dry_run and not request.live_env_gate:
        return _blocked(request, "live_env_gate_required", flags)
    if request.max_pages > profile.max_pages:
        return _blocked(request, "max_pages_exceeds_profile_cap", flags)
    if request.max_links > profile.max_links:
        return _blocked(request, "max_links_exceeds_profile_cap", flags)
    if not _fetcher_allowed(request.fetcher, profile.allowed_fetchers):
        return _blocked(request, "fetcher_not_allowed_by_profile", flags)
    if not _domain_allowed(parsed.netloc.lower(), profile.allowed_domains):
        return _blocked(request, "domain_not_allowed_by_profile", flags)

    flags = list(dict.fromkeys([*flags, "known_url_only", "no_network_in_safety_check"]))
    return FetchSafetyDecision(
        fetch_allowed=True,
        blocked_reason=None,
        fetcher=request.fetcher,
        url=request.url,
        profile_id=request.profile_id,
        max_pages=request.max_pages,
        max_links=request.max_links,
        estimated_cost=_estimated_cost(request),
        safety_flags=flags,
    )


def firecrawl_safety_to_jsonable(decision: FetchSafetyDecision) -> Dict[str, Any]:
    return decision.to_dict()


def evaluate_known_url_live_smoke_skeleton(
    target: KnownUrlLiveSmokeTarget,
    *,
    env: Optional[Mapping[str, str]] = None,
    portal_manifest: Optional[PortalProfileManifest] = None,
    repo_root: Optional[Path] = None,
) -> KnownUrlLiveSmokeDecision:
    """Default-off preflight for a future known-URL portal live smoke.

    This function never fetches. It only verifies that the explicit live
    gates are present and that FIRE1 safety would allow the target.
    """

    environment = os.environ if env is None else env
    required = ["FLAMEON_RUN_LIVE_CASEGRAPH", "FLAMEON_RUN_LIVE_PORTAL_FETCH"]
    missing = [name for name in required if environment.get(name) != "1"]
    if missing:
        return KnownUrlLiveSmokeDecision(
            target_id=target.target_id,
            execution_status="skipped",
            skip_reason="missing_env_gates:" + ",".join(missing),
            required_env_vars=required,
            safety_preflight_status="not_run",
            safety_decision={},
            live_call_allowed=False,
        )

    safety = evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url=target.url,
            profile_id=target.profile_id,
            fetcher=target.fetcher,
            max_pages=target.max_pages,
            max_links=target.max_links,
            known_url=True,
            dry_run=False,
            live_env_gate=True,
            broad_search_mode=False,
            allow_downloads=False,
            allow_private_or_login=False,
            allow_llm=False,
            download_intent=False,
        ),
        portal_manifest=portal_manifest,
        repo_root=repo_root,
    )
    if not safety.fetch_allowed:
        return KnownUrlLiveSmokeDecision(
            target_id=target.target_id,
            execution_status="blocked",
            skip_reason=safety.blocked_reason,
            required_env_vars=required,
            safety_preflight_status="blocked",
            safety_decision=safety.to_dict(),
            live_call_allowed=False,
        )
    return KnownUrlLiveSmokeDecision(
        target_id=target.target_id,
        execution_status="ready_for_future_live_fetch",
        skip_reason=None,
        required_env_vars=required,
        safety_preflight_status="allowed",
        safety_decision=safety.to_dict(),
        live_call_allowed=True,
    )


def known_url_live_smoke_to_jsonable(decision: KnownUrlLiveSmokeDecision) -> Dict[str, Any]:
    return decision.to_dict()


def _blocked(
    request: PortalFetchSafetyRequest,
    reason: str,
    flags: list[str],
) -> FetchSafetyDecision:
    return FetchSafetyDecision(
        fetch_allowed=False,
        blocked_reason=reason,
        fetcher=request.fetcher,
        url=request.url,
        profile_id=request.profile_id,
        max_pages=request.max_pages,
        max_links=request.max_links,
        estimated_cost=0.0,
        safety_flags=list(dict.fromkeys([*flags, reason])),
    )


def _matches_protected_pattern(url: str, profile) -> bool:
    lower = (url or "").lower()
    patterns = [
        *(profile.blocked_url_patterns or []),
        *(profile.protected_private_login_patterns or []),
        *BLOCKED_ACCESS_PATTERNS,
    ]
    return any(pattern.lower() in lower for pattern in patterns)


def _fetcher_allowed(fetcher: str, allowed_fetchers: list[str]) -> bool:
    allowed = {item.lower() for item in allowed_fetchers}
    if fetcher == "requests":
        return any(item.endswith("_api") or item == "api_metadata" for item in allowed)
    if fetcher == "firecrawl":
        return "seeded_html_metadata" in allowed
    return fetcher in allowed


def _domain_allowed(host: str, allowed_domains: list[str]) -> bool:
    if not allowed_domains:
        return True
    return any(host == allowed or host.endswith(f".{allowed}") for allowed in allowed_domains)


def _estimated_cost(request: PortalFetchSafetyRequest) -> float:
    if request.dry_run:
        return 0.0
    if request.fetcher == "firecrawl":
        return round(0.01 * max(request.max_pages, 1), 2)
    return 0.0
