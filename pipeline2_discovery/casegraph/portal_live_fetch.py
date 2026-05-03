"""PORTAL-LIVE-2 — Operator-triggered single-URL portal live fetch.

End-to-end orchestrator for the new ``--portal-live`` CLI mode:

  target fixture
    -> KnownUrlLiveSmokeTarget safety preflight (FIRE2)
    -> additional target.allowed_domains enforcement
    -> mocked / abstract fetch client
    -> save raw payload (when target.save_raw_payload)
    -> extract agency_ois-shaped payload
    -> save extracted payload (when target.save_extracted_payload)
    -> ready for the CLI to replay through build_portal_replay_payload

All real-network capability is delegated to the fetch client; the
orchestrator itself is pure aside from disk writes (gated by the
``payloads_dir`` argument so tests can redirect into ``tmp_path``).

The orchestrator does NOT call Firecrawl, does NOT touch live URLs,
and does NOT require any env var to construct. The
``MockFetchClient`` returns the target's canned ``mock_response``
verbatim. ``FirecrawlFetchClient`` and ``RequestsFetchClient`` are
skeleton fetchers that surface clear error results without making a
network call (their ``_scrape`` method is monkey-patched in tests).
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional
from urllib.parse import urlparse

from .firecrawl_safety import (
    KnownUrlLiveSmokeDecision,
    KnownUrlLiveSmokeTarget,
    evaluate_known_url_live_smoke_skeleton,
)
from .portal_fetch_client import (
    PortalFetchClient,
    PortalFetchResult,
    PortalLiveTarget,
    make_fetch_client,
)


_REQUIRED_TARGET_KEYS = ("target_id", "url", "profile_id")


def load_portal_live_target(path: Path) -> PortalLiveTarget:
    """Parse a target fixture JSON into a :class:`PortalLiveTarget`."""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"target fixture root is not a JSON object: {path}")
    missing = [key for key in _REQUIRED_TARGET_KEYS if key not in data]
    if missing:
        raise ValueError(
            f"target fixture missing required keys {missing}: {path}"
        )
    allowed_domains = data.get("allowed_domains") or []
    if not isinstance(allowed_domains, list):
        raise ValueError("target fixture allowed_domains must be a list")
    return PortalLiveTarget(
        target_id=str(data["target_id"]),
        url=str(data["url"]),
        profile_id=str(data["profile_id"]),
        fetcher=str(data.get("fetcher") or "mock"),
        max_pages=int(data.get("max_pages") or 1),
        max_links=int(data.get("max_links") or 5),
        allowed_domains=[str(d) for d in allowed_domains],
        expected_response_status=int(data.get("expected_response_status") or 200),
        save_raw_payload=bool(data.get("save_raw_payload", True)),
        save_extracted_payload=bool(data.get("save_extracted_payload", True)),
        replay_through_portal_replay=bool(
            data.get("replay_through_portal_replay", True)
        ),
        mock_response=data.get("mock_response") if isinstance(
            data.get("mock_response"), dict
        ) else None,
    )


@dataclass
class PortalLiveResult:
    """Outcome of a portal-live run. ``status`` is ``"completed"`` only
    when every preflight passed, the fetch returned successfully, and
    the extractor produced a replayable agency_ois-shaped payload.
    Any earlier blocker sets ``status = "blocked"`` plus a specific
    ``blocked_reason`` string. ``raw_payload_path`` and
    ``extracted_payload_path`` are populated only when the
    corresponding ``save_*`` flag was True AND the orchestrator got
    that far."""

    target: PortalLiveTarget
    safety_decision: KnownUrlLiveSmokeDecision
    target_domain_status: str
    fetch_result: Optional[PortalFetchResult]
    extracted_payload: Optional[Dict[str, Any]]
    raw_payload_path: Optional[Path]
    extracted_payload_path: Optional[Path]
    status: str
    blocked_reason: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "target": asdict(self.target),
            "safety_decision": self.safety_decision.to_dict(),
            "target_domain_status": self.target_domain_status,
            "fetch_result": self.fetch_result.to_dict() if self.fetch_result else None,
            "extracted_payload_present": self.extracted_payload is not None,
            "raw_payload_path": _relative_or_str(self.raw_payload_path),
            "extracted_payload_path": _relative_or_str(self.extracted_payload_path),
            "status": self.status,
            "blocked_reason": self.blocked_reason,
        }


def run_portal_live(
    target: PortalLiveTarget,
    *,
    fetch_client: Optional[PortalFetchClient] = None,
    env: Optional[Mapping[str, str]] = None,
    repo_root: Optional[Path] = None,
    payloads_dir: Optional[Path] = None,
    timestamp_provider: Optional[Callable[[], str]] = None,
) -> PortalLiveResult:
    """Execute the bounded single-URL portal live flow.

    Returns a :class:`PortalLiveResult` describing what happened. Never
    raises for safety/fetch failures — they surface as
    ``status = "blocked"`` plus a specific ``blocked_reason``. Disk
    writes only happen when the target opts in via
    ``save_raw_payload`` / ``save_extracted_payload``.
    """
    repo = Path(repo_root) if repo_root else _default_repo_root()
    payloads_root = (
        Path(payloads_dir) if payloads_dir else _default_payloads_dir(repo)
    )
    timestamp = (timestamp_provider or _utc_compact_timestamp)()

    safety_decision = evaluate_known_url_live_smoke_skeleton(
        KnownUrlLiveSmokeTarget(
            target_id=target.target_id,
            url=target.url,
            profile_id=target.profile_id,
            fetcher=_safety_fetcher(target.fetcher),
            max_pages=target.max_pages,
            max_links=target.max_links,
        ),
        env=env,
        repo_root=repo,
    )

    target_domain_status = _check_target_domain(target)

    if safety_decision.execution_status != "ready_for_future_live_fetch":
        return PortalLiveResult(
            target=target,
            safety_decision=safety_decision,
            target_domain_status=target_domain_status,
            fetch_result=None,
            extracted_payload=None,
            raw_payload_path=None,
            extracted_payload_path=None,
            status="blocked",
            blocked_reason=safety_decision.skip_reason or "safety_preflight_blocked",
        )
    if target_domain_status != "allowed":
        return PortalLiveResult(
            target=target,
            safety_decision=safety_decision,
            target_domain_status=target_domain_status,
            fetch_result=None,
            extracted_payload=None,
            raw_payload_path=None,
            extracted_payload_path=None,
            status="blocked",
            blocked_reason=target_domain_status,
        )

    client = fetch_client or make_fetch_client(target, env=env)
    fetch_result = client.fetch(target)

    if fetch_result.error or fetch_result.status_code != target.expected_response_status:
        blocked_reason = (
            fetch_result.error
            or f"unexpected_status_code:{fetch_result.status_code}"
        )
        return PortalLiveResult(
            target=target,
            safety_decision=safety_decision,
            target_domain_status=target_domain_status,
            fetch_result=fetch_result,
            extracted_payload=None,
            raw_payload_path=None,
            extracted_payload_path=None,
            status="blocked",
            blocked_reason=blocked_reason,
        )

    raw_path: Optional[Path] = None
    if target.save_raw_payload:
        raw_path = _save_payload(
            payloads_root,
            target,
            kind="raw",
            payload=fetch_result.raw_payload,
            timestamp=timestamp,
        )

    try:
        extracted = extract_to_agency_ois(fetch_result.raw_payload)
    except ValueError as exc:
        return PortalLiveResult(
            target=target,
            safety_decision=safety_decision,
            target_domain_status=target_domain_status,
            fetch_result=fetch_result,
            extracted_payload=None,
            raw_payload_path=raw_path,
            extracted_payload_path=None,
            status="blocked",
            blocked_reason=f"extract_failed:{exc}",
        )

    extracted_path: Optional[Path] = None
    if target.save_extracted_payload:
        extracted_path = _save_payload(
            payloads_root,
            target,
            kind="extracted",
            payload=extracted,
            timestamp=timestamp,
        )

    return PortalLiveResult(
        target=target,
        safety_decision=safety_decision,
        target_domain_status=target_domain_status,
        fetch_result=fetch_result,
        extracted_payload=extracted,
        raw_payload_path=raw_path,
        extracted_payload_path=extracted_path,
        status="completed",
        blocked_reason=None,
    )


def extract_to_agency_ois(raw: Mapping[str, Any]) -> Dict[str, Any]:
    """Validate and pass through a fetch raw payload into the
    agency_ois fixture shape that ``--portal-replay --fixture <path>``
    can consume.

    For the ``mock`` fetcher (this PR's only success path), the raw
    payload is already shaped like an agency_ois fixture; the
    extractor confirms the minimum required keys are present and
    returns a copy. Real fetchers (Firecrawl HTML / requests HTML)
    will need richer extraction in a follow-up PR.
    """
    if not isinstance(raw, Mapping):
        raise ValueError("raw payload must be a JSON object")
    has_page_type = bool(raw.get("page_type"))
    has_profile = bool(raw.get("portal_profile_id"))
    has_source_records = isinstance(raw.get("source_records"), list)
    if not (has_page_type or has_profile or has_source_records):
        raise ValueError(
            "extracted payload must declare page_type, portal_profile_id, "
            "or source_records (agency_ois shape required)"
        )
    return dict(raw)


def build_live_fetch_section(result: PortalLiveResult) -> Dict[str, Any]:
    """Operator-facing JSON section. Surfaces every diagnostic the
    operator should see — fetcher, paths, status, costs — without ever
    exposing API key contents."""
    fetch_result = result.fetch_result
    fetch_result_dict = fetch_result.to_dict() if fetch_result else None
    return {
        "target_id": result.target.target_id,
        "url": result.target.url,
        "profile_id": result.target.profile_id,
        "fetcher": result.target.fetcher,
        "raw_payload_path": _relative_or_str(result.raw_payload_path),
        "extracted_payload_path": _relative_or_str(result.extracted_payload_path),
        "status": result.status,
        "blocked_reason": result.blocked_reason,
        "status_code": fetch_result.status_code if fetch_result else None,
        "estimated_cost_usd": fetch_result.estimated_cost_usd if fetch_result else 0.0,
        "api_calls": dict(fetch_result.api_calls) if fetch_result else {},
        "wallclock_seconds": fetch_result.wallclock_seconds if fetch_result else 0.0,
        "safety_status": result.safety_decision.safety_preflight_status,
        "target_domain_status": result.target_domain_status,
        "replayed": (
            result.status == "completed"
            and result.target.replay_through_portal_replay
        ),
    }


def _safety_fetcher(target_fetcher: str) -> str:
    """The FIRE1 safety preflight only knows about ``firecrawl`` /
    ``requests``. Map the orchestrator's ``mock`` to ``firecrawl`` for
    the preflight (mock targets still need to satisfy the same
    profile/cap/auth gates as a real Firecrawl target would)."""
    if target_fetcher == "mock":
        return "firecrawl"
    return target_fetcher or "firecrawl"


def _check_target_domain(target: PortalLiveTarget) -> str:
    """Return ``"allowed"`` when the target URL's netloc is in the
    target-supplied allowlist; ``"target_allowed_domains_empty"`` when
    the allowlist is missing/empty (the target fixture must declare a
    non-empty allowlist for live work); ``"url_domain_not_in_target_allowlist"``
    when the URL host doesn't match any entry."""
    if not target.allowed_domains:
        return "target_allowed_domains_empty"
    netloc = urlparse(target.url).netloc.lower()
    for allowed in target.allowed_domains:
        allowed_l = allowed.strip().lower()
        if not allowed_l:
            continue
        if netloc == allowed_l or netloc.endswith(f".{allowed_l}"):
            return "allowed"
    return "url_domain_not_in_target_allowlist"


def _default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_payloads_dir(repo_root: Path) -> Path:
    return repo_root / "autoresearch" / ".runs" / "live_payloads"


def _utc_compact_timestamp() -> str:
    """ISO-style timestamp safe for filenames: ``2026-05-03T12-34-56Z``."""
    now = datetime.now(timezone.utc).replace(microsecond=0)
    return now.strftime("%Y-%m-%dT%H-%M-%SZ")


def _save_payload(
    directory: Path,
    target: PortalLiveTarget,
    *,
    kind: str,
    payload: Any,
    timestamp: str,
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    safe_target_id = "".join(
        c if c.isalnum() or c in "-_" else "_" for c in target.target_id
    )
    path = directory / f"{timestamp}_{safe_target_id}.{kind}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=False)
        f.write("\n")
    return path


def _relative_or_str(path: Optional[Path]) -> Optional[str]:
    if path is None:
        return None
    repo = _default_repo_root()
    try:
        return path.resolve().relative_to(repo).as_posix()
    except ValueError:
        return path.as_posix()


__all__ = [
    "PortalLiveResult",
    "build_live_fetch_section",
    "extract_to_agency_ois",
    "load_portal_live_target",
    "run_portal_live",
]
