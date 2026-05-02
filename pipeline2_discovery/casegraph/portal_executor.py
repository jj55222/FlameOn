"""PORTAL3 - mocked seeded portal executor.

Consumes PORTAL2 dry fetch plans plus caller-supplied page payloads.
This module never fetches, scrapes, downloads, or calls Firecrawl. It
only routes mocked content through the same connector/claim discipline
used by agency-OIS fixtures and records what a future resolver would be
allowed to inspect.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from .claim_extraction import extract_artifact_claims
from .connectors.agency_ois import AgencyOISConnector
from .models import CaseIdentity, CaseInput, CasePacket, Jurisdiction, Scores, SourceRecord
from .portal_fetch_plan import PortalFetchPlan
from .portal_profiles import PortalProfileManifest, load_portal_profiles


@dataclass
class PortalExecutionResult:
    plan_id: str
    profile_id: str
    fetcher_requested: str | None
    mocked_fetch_status: str
    extracted_source_records: List[Dict[str, Any]] = field(default_factory=list)
    artifact_claims: List[Dict[str, Any]] = field(default_factory=list)
    candidate_artifact_urls: List[str] = field(default_factory=list)
    rejected_urls: List[str] = field(default_factory=list)
    resolver_actions: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)
    execution_status: str = "completed"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def execute_mock_portal_plan(
    plan: PortalFetchPlan,
    page_payload: Mapping[str, Any],
    *,
    portal_manifest: PortalProfileManifest | None = None,
) -> PortalExecutionResult:
    """Execute a seeded portal plan against mocked page content only."""

    manifest = portal_manifest or load_portal_profiles()
    profile = manifest.get(plan.portal_profile_id)
    plan_id = f"{plan.case_id}:{plan.portal_profile_id}"
    if plan.blocked_reason:
        return PortalExecutionResult(
            plan_id=plan_id,
            profile_id=plan.portal_profile_id,
            fetcher_requested=plan.fetcher,
            mocked_fetch_status="not_fetched",
            risk_flags=[plan.blocked_reason],
            next_actions=["Provide a public seed URL before executing this portal plan."],
            execution_status="blocked",
        )
    if profile is None:
        return PortalExecutionResult(
            plan_id=plan_id,
            profile_id=plan.portal_profile_id,
            fetcher_requested=plan.fetcher,
            mocked_fetch_status="not_fetched",
            risk_flags=["portal_profile_missing"],
            next_actions=["Add a portal profile before executing this portal plan."],
            execution_status="blocked",
        )

    records = _records_from_payload(page_payload)
    capped_records = _apply_link_cap(records, max_links=plan.max_links)
    packet = _packet_for(plan, capped_records)
    claim_result = extract_artifact_claims(packet)
    candidate_urls = _candidate_urls(capped_records)
    rejected_urls = _rejected_urls(capped_records)
    resolver_actions = _resolver_actions(candidate_urls, rejected_urls)
    risk_flags = list(dict.fromkeys([*claim_result.risk_flags, *_record_risk_flags(capped_records)]))
    next_actions = list(dict.fromkeys([*claim_result.next_actions, *_next_actions(candidate_urls, rejected_urls)]))

    return PortalExecutionResult(
        plan_id=plan_id,
        profile_id=plan.portal_profile_id,
        fetcher_requested=plan.fetcher,
        mocked_fetch_status="ok",
        extracted_source_records=[record.to_dict() for record in capped_records],
        artifact_claims=[asdict(claim) for claim in claim_result.artifact_claims],
        candidate_artifact_urls=candidate_urls,
        rejected_urls=rejected_urls,
        resolver_actions=resolver_actions,
        risk_flags=risk_flags,
        next_actions=next_actions,
        execution_status="completed",
    )


def execute_mock_portal_plans(
    plan_payloads: Iterable[tuple[PortalFetchPlan, Mapping[str, Any]]],
    *,
    portal_manifest: PortalProfileManifest | None = None,
) -> List[PortalExecutionResult]:
    manifest = portal_manifest or load_portal_profiles()
    return [
        execute_mock_portal_plan(plan, payload, portal_manifest=manifest)
        for plan, payload in plan_payloads
    ]


def portal_execution_to_jsonable(result: PortalExecutionResult) -> Dict[str, Any]:
    return result.to_dict()


def _records_from_payload(page_payload: Mapping[str, Any]) -> List[SourceRecord]:
    if isinstance(page_payload.get("source_records"), list):
        return [_source_record_from_mapping(item) for item in page_payload["source_records"]]
    profile_id = str(page_payload.get("portal_profile_id") or "")
    if profile_id in {"agency_ois_detail", "agency_ois_listing"} or page_payload.get("page_type"):
        return list(AgencyOISConnector([page_payload]).fetch(_case_input(page_payload)))
    return [_generic_page_record(page_payload)]


def _source_record_from_mapping(item: Mapping[str, Any]) -> SourceRecord:
    return SourceRecord(
        source_id=str(item.get("source_id") or "portal_manifest_source"),
        url=str(item.get("url") or ""),
        title=str(item.get("title") or ""),
        snippet=str(item.get("snippet") or ""),
        raw_text=str(item.get("raw_text") or item.get("snippet") or ""),
        source_type=str(item.get("source_type") or "portal_manifest_source"),
        source_authority=str(item.get("source_authority") or "unknown"),
        source_roles=list(item.get("source_roles") or []),
        api_name=item.get("api_name"),
        discovered_via=str(item.get("discovered_via") or "portal_replay_manifest"),
        case_input_id=item.get("case_input_id"),
        metadata=dict(item.get("metadata") or {}),
        cost_estimate=float(item.get("cost_estimate") or 0.0),
        confidence_signals=dict(item.get("confidence_signals") or {}),
        matched_case_fields=list(item.get("matched_case_fields") or []),
    )


def _case_input(page_payload: Mapping[str, Any]) -> CaseInput:
    subjects = page_payload.get("subjects") or []
    if isinstance(subjects, str):
        subjects = [subjects]
    return CaseInput(
        input_type="mock_portal",
        raw_input={"defendant_names": ", ".join(str(item) for item in subjects)},
        known_fields={"defendant_names": list(subjects)},
    )


def _generic_page_record(page_payload: Mapping[str, Any]) -> SourceRecord:
    return SourceRecord(
        source_id=f"portal_mock::{page_payload.get('url', 'page')}",
        url=str(page_payload.get("url") or ""),
        title=str(page_payload.get("title") or ""),
        snippet=str(page_payload.get("snippet") or page_payload.get("narrative") or ""),
        raw_text=str(page_payload.get("raw_text") or page_payload.get("narrative") or ""),
        source_type=str(page_payload.get("source_type") or "portal_mock"),
        source_authority=str(page_payload.get("source_authority") or "unknown"),
        source_roles=list(page_payload.get("source_roles") or []),
        api_name="portal_mock",
        discovered_via="mock_portal_executor",
        metadata=dict(page_payload.get("metadata") or {}),
    )


def _apply_link_cap(records: Sequence[SourceRecord], *, max_links: int) -> List[SourceRecord]:
    if max_links <= 0:
        return [record for record in records if record.metadata.get("fixture_kind") == "agency_page"]
    page_records = [record for record in records if record.metadata.get("fixture_kind") == "agency_page"]
    link_records = [record for record in records if record.metadata.get("fixture_kind") != "agency_page"]
    return [*page_records, *link_records[:max_links]]


def _packet_for(plan: PortalFetchPlan, records: Sequence[SourceRecord]) -> CasePacket:
    return CasePacket(
        case_id=f"portal_executor_{plan.case_id}",
        input=CaseInput(
            input_type="portal_plan",
            raw_input={"case_id": plan.case_id, "title": plan.title},
            known_fields={"title": plan.title},
        ),
        case_identity=CaseIdentity(
            defendant_names=[plan.title] if plan.title else [],
            jurisdiction=Jurisdiction(),
            identity_confidence="low",
        ),
        sources=list(records),
        artifact_claims=[],
        verified_artifacts=[],
        scores=Scores(),
        verdict="HOLD",
        next_actions=[],
        risk_flags=[],
    )


def _candidate_urls(records: Sequence[SourceRecord]) -> List[str]:
    return [
        record.url
        for record in records
        if "possible_artifact_source" in record.source_roles
        and "protected_or_nonpublic" not in (record.metadata or {}).get("risk_flags", [])
    ]


def _rejected_urls(records: Sequence[SourceRecord]) -> List[str]:
    return [
        record.url
        for record in records
        if "protected_or_nonpublic" in (record.metadata or {}).get("risk_flags", [])
    ]


def _record_risk_flags(records: Sequence[SourceRecord]) -> List[str]:
    flags: List[str] = []
    for record in records:
        flags.extend((record.metadata or {}).get("risk_flags", []))
    return list(dict.fromkeys(flags))


def _resolver_actions(candidate_urls: Sequence[str], rejected_urls: Sequence[str]) -> List[str]:
    actions = [f"candidate_ready_for_resolver:{url}" for url in candidate_urls]
    actions.extend(f"reject_protected_or_nonpublic:{url}" for url in rejected_urls)
    return actions


def _next_actions(candidate_urls: Sequence[str], rejected_urls: Sequence[str]) -> List[str]:
    actions: List[str] = []
    if candidate_urls:
        actions.append("Run metadata-only resolver on candidate public artifact URLs.")
    if rejected_urls:
        actions.append("Skip protected/private portal URLs; find a public release URL.")
    return actions
