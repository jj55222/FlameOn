from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Optional

from .claim_extraction import ClaimExtractionResult, extract_artifact_claims
from .identity import IdentityResolution, resolve_identity
from .inputs import StructuredInputParseResult, YouTubeInputParseResult
from .models import CaseIdentity, CaseInput, CasePacket, Jurisdiction, Scores, SourceRecord
from .outcome import OutcomeResolution, resolve_outcome
from .query_planner import (
    QueryPlanResult,
    plan_queries_from_structured_result,
    plan_queries_from_youtube_result,
)
from .resolvers import MuckRockFileResolution, resolve_muckrock_released_files
from .scoring import ActionabilityResult, score_case_packet


@dataclass
class WeakInputAssemblyResult:
    packet: CasePacket
    query_plan: QueryPlanResult
    identity_resolution: IdentityResolution
    outcome_resolution: OutcomeResolution
    claim_extraction: ClaimExtractionResult
    artifact_resolution: MuckRockFileResolution
    actionability: ActionabilityResult


@dataclass
class StructuredAssemblyResult:
    packet: CasePacket
    query_plan: QueryPlanResult
    identity_resolution: IdentityResolution
    outcome_resolution: OutcomeResolution
    claim_extraction: ClaimExtractionResult
    artifact_resolution: MuckRockFileResolution
    actionability: ActionabilityResult


def assemble_weak_input_case_packet(
    parsed: YouTubeInputParseResult,
    *,
    query_plan: Optional[QueryPlanResult] = None,
    sources: Optional[Iterable[SourceRecord]] = None,
    case_id: Optional[str] = None,
) -> WeakInputAssemblyResult:
    """Build a preliminary CasePacket from weak input and supplied mock sources.

    This function never calls connectors, fetches transcripts, downloads files,
    or treats weak YouTube text as a corroborating source. The deterministic
    identity/outcome/artifact gates decide all confidence and verification.
    """

    plan = query_plan or plan_queries_from_youtube_result(parsed)
    packet = CasePacket(
        case_id=case_id or _case_id(parsed.case_input),
        input=_packet_input(parsed.case_input, plan),
        case_identity=_candidate_identity(parsed.case_input),
        sources=list(sources or []),
        scores=Scores(),
        verdict="HOLD",
        next_actions=_next_actions(parsed.case_input, plan),
        risk_flags=_risk_flags(parsed, plan),
    )

    identity_resolution = resolve_identity(packet)
    outcome_resolution = resolve_outcome(packet)
    claim_extraction = extract_artifact_claims(packet)
    artifact_resolution = resolve_muckrock_released_files(packet)
    actionability = score_case_packet(packet)

    return WeakInputAssemblyResult(
        packet=packet,
        query_plan=plan,
        identity_resolution=identity_resolution,
        outcome_resolution=outcome_resolution,
        claim_extraction=claim_extraction,
        artifact_resolution=artifact_resolution,
        actionability=actionability,
    )


def assemble_structured_case_packet(
    parsed: StructuredInputParseResult,
    *,
    query_plan: Optional[QueryPlanResult] = None,
    sources: Optional[Iterable[SourceRecord]] = None,
    case_id: Optional[str] = None,
) -> StructuredAssemblyResult:
    """Build a preliminary CasePacket from a structured dataset row and supplied mock sources.

    Mirrors `assemble_weak_input_case_packet`: never calls connectors, downloads
    files, or treats the structured row as a corroborating source. Identity,
    outcome, claim, and artifact gates remain authoritative; structured rows
    only seed candidate fields and queries.
    """

    plan = query_plan or plan_queries_from_structured_result(parsed)
    packet = CasePacket(
        case_id=case_id or _structured_case_id(parsed),
        input=_structured_packet_input(parsed.case_input, plan),
        case_identity=_candidate_identity(parsed.case_input),
        sources=list(sources or []),
        scores=Scores(),
        verdict="HOLD",
        next_actions=_structured_next_actions(parsed.case_input, plan),
        risk_flags=_structured_risk_flags(parsed, plan),
    )

    identity_resolution = resolve_identity(packet)
    outcome_resolution = resolve_outcome(packet)
    claim_extraction = extract_artifact_claims(packet)
    artifact_resolution = resolve_muckrock_released_files(packet)
    actionability = score_case_packet(packet)

    return StructuredAssemblyResult(
        packet=packet,
        query_plan=plan,
        identity_resolution=identity_resolution,
        outcome_resolution=outcome_resolution,
        claim_extraction=claim_extraction,
        artifact_resolution=artifact_resolution,
        actionability=actionability,
    )


def _packet_input(case_input: CaseInput, plan: QueryPlanResult) -> CaseInput:
    planned_queries = [
        query.query
        for connector_plan in plan.plans
        for query in connector_plan.queries
    ]
    return CaseInput(
        input_type="youtube",
        raw_input=dict(case_input.raw_input),
        known_fields=dict(case_input.known_fields),
        missing_fields=list(case_input.missing_fields),
        candidate_queries=_dedupe([*case_input.candidate_queries, *planned_queries]),
    )


def _candidate_identity(case_input: CaseInput) -> CaseIdentity:
    fields = case_input.known_fields or {}
    jurisdiction = fields.get("jurisdiction") if isinstance(fields.get("jurisdiction"), dict) else {}
    return CaseIdentity(
        defendant_names=list(fields.get("defendant_names") or []),
        victim_names=list(fields.get("victim_names") or []),
        agency=fields.get("agency"),
        jurisdiction=Jurisdiction(
            city=jurisdiction.get("city"),
            county=jurisdiction.get("county"),
            state=jurisdiction.get("state"),
        ),
        incident_date=fields.get("incident_date"),
        case_numbers=list(fields.get("case_numbers") or []),
        charges=list(fields.get("charges") or []),
        outcome_status="unknown",
        identity_confidence="low",
        identity_anchors=[],
    )


def _next_actions(case_input: CaseInput, plan: QueryPlanResult) -> List[str]:
    actions = [
        "Corroborate weak-input candidate fields with identity_source records.",
        "Resolve artifact claims into public artifact URLs before production.",
    ]
    missing = _dedupe([
        *case_input.missing_fields,
        *[
            requirement
            for connector_plan in plan.plans
            for requirement in connector_plan.missing_field_requirements
        ],
    ])
    if missing:
        actions.append(f"Fill missing weak-input fields: {', '.join(missing)}.")
    return actions


def _risk_flags(parsed: YouTubeInputParseResult, plan: QueryPlanResult) -> List[str]:
    return _dedupe([
        "weak_input_preliminary_packet",
        "candidate_fields_not_identity_lock",
        *parsed.risk_flags,
        *plan.risk_flags,
    ])


def _case_id(case_input: CaseInput) -> str:
    fields = case_input.known_fields or {}
    parts = [
        "youtube",
        *(fields.get("defendant_names") or [])[:1],
        fields.get("agency"),
        (fields.get("jurisdiction") or {}).get("city") if isinstance(fields.get("jurisdiction"), dict) else None,
        fields.get("incident_date"),
    ]
    slug = "_".join(str(part) for part in parts if part)
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", slug).strip("_").lower()
    return slug or "youtube_weak_input"


def _structured_packet_input(case_input: CaseInput, plan: QueryPlanResult) -> CaseInput:
    planned_queries = [
        query.query
        for connector_plan in plan.plans
        for query in connector_plan.queries
    ]
    return CaseInput(
        input_type=case_input.input_type or "dataset_row",
        raw_input=dict(case_input.raw_input),
        known_fields=dict(case_input.known_fields),
        missing_fields=list(case_input.missing_fields),
        candidate_queries=_dedupe([*case_input.candidate_queries, *planned_queries]),
    )


def _structured_next_actions(case_input: CaseInput, plan: QueryPlanResult) -> List[str]:
    actions = [
        "Corroborate structured-row candidate fields with identity_source records.",
        "Resolve artifact claims into public artifact URLs before production.",
    ]
    missing = _dedupe([
        *case_input.missing_fields,
        *[
            requirement
            for connector_plan in plan.plans
            for requirement in connector_plan.missing_field_requirements
        ],
    ])
    if missing:
        actions.append(f"Fill missing structured-row fields: {', '.join(missing)}.")
    return actions


def _structured_risk_flags(parsed: StructuredInputParseResult, plan: QueryPlanResult) -> List[str]:
    return _dedupe([
        "structured_input_preliminary_packet",
        "candidate_fields_not_identity_lock",
        f"dataset:{parsed.dataset_name}",
        *parsed.risk_flags,
        *plan.risk_flags,
    ])


def _structured_case_id(parsed: StructuredInputParseResult) -> str:
    fields = parsed.case_input.known_fields or {}
    jurisdiction = fields.get("jurisdiction") if isinstance(fields.get("jurisdiction"), dict) else {}
    parts = [
        parsed.dataset_name or "structured",
        *(fields.get("defendant_names") or [])[:1],
        fields.get("agency"),
        jurisdiction.get("city") or jurisdiction.get("county"),
        jurisdiction.get("state"),
        fields.get("incident_date"),
    ]
    slug = "_".join(str(part) for part in parts if part)
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", slug).strip("_").lower()
    return slug or "structured_dataset_row"


def _dedupe(values: Iterable[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for value in values:
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    return result
