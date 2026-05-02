"""PORTAL4 - integrated no-live portal dry replay.

Runs the dry portal path end-to-end for seeded plans:

calibration profile -> portal fetch plan -> safety preflight ->
mocked portal executor -> resolver-action diagnostics.

No live fetches, no Firecrawl calls, no scraping, and no downloads.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .firecrawl_safety import PortalFetchSafetyRequest, evaluate_fetch_safety
from .portal_executor import execute_mock_portal_plan
from .portal_fetch_plan import PortalFetchPlan, build_portal_fetch_plan_report
from .portal_profiles import PortalProfileManifest, load_portal_profiles


@dataclass
class PortalDryReplayCaseResult:
    case_id: int
    portal_profile_id: str
    fetch_plan_status: str
    safety_status: str
    executor_status: str
    source_records_count: int = 0
    artifact_claims_count: int = 0
    candidate_urls_count: int = 0
    rejected_urls_count: int = 0
    resolver_actions_count: int = 0
    blockers: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PortalDryReplayReport:
    total_plans: int
    executed_count: int
    blocked_count: int
    missing_payload_count: int
    case_results: List[PortalDryReplayCaseResult] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_plans": self.total_plans,
            "executed_count": self.executed_count,
            "blocked_count": self.blocked_count,
            "missing_payload_count": self.missing_payload_count,
            "case_results": [result.to_dict() for result in self.case_results],
        }


@dataclass
class PortalReplayManifestEntry:
    case_id: int
    portal_profile_id: str
    mocked_payload_fixture: str
    expected_source_records: int
    expected_artifact_claims: int
    expected_candidate_urls: int
    expected_rejected_urls: int
    expected_resolver_actions: int
    expected_blockers: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PortalReplayManifest:
    version: int
    entries: List[PortalReplayManifestEntry] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "entries": [entry.to_dict() for entry in self.entries],
        }


def build_portal_dry_replay_report(
    *,
    plans: Optional[Sequence[PortalFetchPlan]] = None,
    mocked_payloads_by_case_id: Optional[Mapping[int, Mapping[str, Any]]] = None,
    portal_manifest: Optional[PortalProfileManifest] = None,
    repo_root: Optional[Path] = None,
    limit: Optional[int] = None,
) -> PortalDryReplayReport:
    manifest = portal_manifest or load_portal_profiles(repo_root=repo_root)
    source_plans = list(plans) if plans is not None else build_portal_fetch_plan_report(
        portal_manifest=manifest,
        repo_root=repo_root,
    ).plans
    if limit is not None:
        source_plans = source_plans[:limit]
    payloads = mocked_payloads_by_case_id or {}
    results = [
        _run_plan(plan, payloads.get(plan.case_id), manifest=manifest)
        for plan in source_plans
    ]
    return PortalDryReplayReport(
        total_plans=len(results),
        executed_count=sum(1 for result in results if result.executor_status == "completed"),
        blocked_count=sum(1 for result in results if result.blockers),
        missing_payload_count=sum(1 for result in results if "mock_payload_missing" in result.blockers),
        case_results=results,
    )


def portal_dry_replay_to_jsonable(report: PortalDryReplayReport) -> Dict[str, Any]:
    return report.to_dict()


def default_portal_replay_manifest_path(repo_root: Optional[Path] = None) -> Path:
    root = Path(repo_root) if repo_root else Path(__file__).resolve().parents[2]
    return root / "tests" / "fixtures" / "portal_replay" / "portal_replay_manifest.json"


def load_portal_replay_manifest(
    path: Optional[Path] = None,
    *,
    repo_root: Optional[Path] = None,
) -> PortalReplayManifest:
    manifest_path = Path(path) if path else default_portal_replay_manifest_path(repo_root)
    with manifest_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return PortalReplayManifest(
        version=int(data.get("version", 1)),
        entries=[
            PortalReplayManifestEntry(**entry)
            for entry in data.get("entries", [])
            if isinstance(entry, Mapping)
        ],
    )


def run_portal_replay_manifest(
    manifest: Optional[PortalReplayManifest] = None,
    *,
    repo_root: Optional[Path] = None,
) -> PortalDryReplayReport:
    root = Path(repo_root) if repo_root else Path(__file__).resolve().parents[2]
    loaded = manifest or load_portal_replay_manifest(repo_root=root)
    plans = [_plan_for_manifest_entry(entry) for entry in loaded.entries]
    payloads = {
        entry.case_id: _load_payload_fixture(root, entry.mocked_payload_fixture)
        for entry in loaded.entries
    }
    return build_portal_dry_replay_report(
        plans=plans,
        mocked_payloads_by_case_id=payloads,
        repo_root=root,
    )


def _run_plan(
    plan: PortalFetchPlan,
    payload: Optional[Mapping[str, Any]],
    *,
    manifest: PortalProfileManifest,
) -> PortalDryReplayCaseResult:
    blockers: List[str] = []
    next_actions: List[str] = []
    if plan.blocked_reason:
        blockers.append(plan.blocked_reason)
        next_actions.append("Resolve blocked fetch plan before portal execution.")
        return PortalDryReplayCaseResult(
            case_id=plan.case_id,
            portal_profile_id=plan.portal_profile_id,
            fetch_plan_status="blocked",
            safety_status="not_run",
            executor_status="skipped",
            blockers=blockers,
            next_actions=next_actions,
        )

    safety = evaluate_fetch_safety(_safety_request_for(plan), portal_manifest=manifest)
    if not safety.fetch_allowed:
        blockers.append(safety.blocked_reason or "safety_preflight_blocked")
        next_actions.append("Fix safety preflight blocker before any live fetch.")
        return PortalDryReplayCaseResult(
            case_id=plan.case_id,
            portal_profile_id=plan.portal_profile_id,
            fetch_plan_status="ready",
            safety_status="blocked",
            executor_status="skipped",
            blockers=blockers,
            next_actions=next_actions,
        )

    if payload is None:
        blockers.append("mock_payload_missing")
        next_actions.append("Add mocked portal payload before dry executor replay.")
        return PortalDryReplayCaseResult(
            case_id=plan.case_id,
            portal_profile_id=plan.portal_profile_id,
            fetch_plan_status="ready",
            safety_status="allowed",
            executor_status="skipped",
            blockers=blockers,
            next_actions=next_actions,
        )

    execution = execute_mock_portal_plan(plan, payload, portal_manifest=manifest)
    blockers.extend(execution.risk_flags)
    next_actions.extend(execution.next_actions)
    return PortalDryReplayCaseResult(
        case_id=plan.case_id,
        portal_profile_id=plan.portal_profile_id,
        fetch_plan_status="ready",
        safety_status="allowed",
        executor_status=execution.execution_status,
        source_records_count=len(execution.extracted_source_records),
        artifact_claims_count=len(execution.artifact_claims),
        candidate_urls_count=len(execution.candidate_artifact_urls),
        rejected_urls_count=len(execution.rejected_urls),
        resolver_actions_count=len(execution.resolver_actions),
        blockers=list(dict.fromkeys(blockers)),
        next_actions=list(dict.fromkeys(next_actions)),
    )


def _plan_for_manifest_entry(entry: PortalReplayManifestEntry) -> PortalFetchPlan:
    max_links = 5 if entry.portal_profile_id == "youtube_agency_channel" else 25
    fetcher = "requests" if entry.portal_profile_id == "youtube_agency_channel" else "firecrawl"
    seed_url = (
        f"https://www.youtube.com/watch?v=portalReplay{entry.case_id}"
        if entry.portal_profile_id == "youtube_agency_channel"
        else f"https://www.phoenix.gov/police/critical-incidents/replay-{entry.case_id}"
    )
    return PortalFetchPlan(
        case_id=entry.case_id,
        title=f"portal_replay_case_{entry.case_id}",
        portal_profile_id=entry.portal_profile_id,
        seed_url=seed_url,
        seed_url_exists=True,
        fetcher=fetcher,
        max_pages=1,
        max_links=max_links,
        allowed_domain="www.phoenix.gov",
        expected_artifact_types=[],
        resolver_policy={
            "metadata_only": True,
            "require_public_url": True,
            "allow_downloads": False,
            "allow_scraping": False,
            "allow_login": False,
        },
        needs_seed_url_discovery=False,
        blocked_reason=None,
        safety_flags=["portal_replay_manifest"],
    )


def _load_payload_fixture(root: Path, relative_path: str) -> Mapping[str, Any]:
    path = root / relative_path
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, Mapping):
        return payload
    raise ValueError(f"portal replay payload fixture must be a JSON object: {path}")


def _safety_request_for(plan: PortalFetchPlan) -> PortalFetchSafetyRequest:
    return PortalFetchSafetyRequest(
        url=plan.seed_url or "",
        profile_id=plan.portal_profile_id,
        fetcher=plan.fetcher or "firecrawl",
        max_pages=plan.max_pages,
        max_links=plan.max_links,
        known_url=bool(plan.seed_url_exists),
        dry_run=True,
        live_env_gate=False,
        broad_search_mode=False,
        allow_downloads=False,
        allow_private_or_login=False,
        allow_llm=False,
        download_intent=False,
    )
