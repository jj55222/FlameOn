from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .inputs import StructuredInputParseResult, YouTubeInputParseResult
from .models import CaseInput


STATE_NAMES_BY_ABBR = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
}

ARTIFACT_QUERY_TERMS = {
    "bodycam": "bodycam",
    "dashcam": "dashcam",
    "dispatch_911": "\"911 call\"",
    "interrogation": "interrogation",
    "surveillance_video": "\"surveillance video\"",
    "court_video": "\"court video\"",
    "critical_incident_video": "\"critical incident video\"",
}


@dataclass
class PlannedQuery:
    query: str
    reason: str
    candidate_fields_used: List[str] = field(default_factory=list)
    expected_source_roles: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ConnectorQueryPlan:
    connector_name: str
    priority: int
    queries: List[PlannedQuery] = field(default_factory=list)
    missing_field_requirements: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    rationale: str = ""
    live_enabled: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class QueryPlanResult:
    case_input: CaseInput
    plans: List[ConnectorQueryPlan] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "case_input": asdict(self.case_input),
            "plans": [plan.to_dict() for plan in self.plans],
            "risk_flags": list(self.risk_flags),
        }


def plan_queries_from_youtube_result(parsed: YouTubeInputParseResult) -> QueryPlanResult:
    return plan_queries_for_case_input(parsed.case_input, source_risk_flags=parsed.risk_flags)


def plan_queries_from_structured_result(parsed: StructuredInputParseResult) -> QueryPlanResult:
    fields = parsed.case_input.known_fields or {}
    risk_flags = _dedupe([
        "structured_dataset_query_plan_only",
        "candidate_fields_not_identity_lock",
        *parsed.risk_flags,
    ])
    plans = [
        _structured_youtube_plan(fields),
        _structured_muckrock_plan(fields),
        _structured_courtlistener_plan(fields),
        _structured_documentcloud_plan(fields),
        _structured_future_broad_search_plan(fields),
    ]
    return QueryPlanResult(case_input=parsed.case_input, plans=plans, risk_flags=risk_flags)


def plan_queries_for_case_input(
    case_input: CaseInput,
    *,
    source_risk_flags: Optional[Sequence[str]] = None,
) -> QueryPlanResult:
    fields = case_input.known_fields or {}
    risk_flags = _dedupe([
        "weak_input_query_plan_only",
        "candidate_fields_not_identity_lock",
        *(source_risk_flags or []),
    ])
    plans = [
        _youtube_plan(fields),
        _muckrock_plan(fields),
        _courtlistener_plan(fields),
        _documentcloud_plan(fields),
        _future_broad_search_plan(fields),
    ]
    return QueryPlanResult(case_input=case_input, plans=plans, risk_flags=risk_flags)


def _structured_youtube_plan(fields: Dict[str, Any]) -> ConnectorQueryPlan:
    subject = _subject_name(fields)
    agency = _agency(fields)
    year = _incident_year(fields)
    queries = []
    if agency:
        queries.append(PlannedQuery(
            query=_join_query([_quote(agency), "critical incident video", year, subject]),
            reason="Search video metadata for agency-published critical incident video using structured row anchors.",
            candidate_fields_used=_structured_fields_used(fields),
            expected_source_roles=["possible_artifact_source", "claim_source"],
            risk_flags=["candidate_media_lead_only", "structured_dataset_candidate_only"],
        ))
    if subject:
        queries.append(PlannedQuery(
            query=_join_query([_quote(subject), agency, "bodycam", year]),
            reason="Search video metadata for bodycam leads tied to the structured subject and agency.",
            candidate_fields_used=_structured_fields_used(fields),
            expected_source_roles=["possible_artifact_source", "claim_source"],
            risk_flags=["candidate_media_lead_only", "structured_dataset_candidate_only"],
        ))
    return ConnectorQueryPlan(
        connector_name="youtube",
        priority=_priority(fields, base=95),
        queries=_dedupe_queries(queries),
        missing_field_requirements=[] if queries else ["subject_name", "agency", "incident_date"],
        risk_flags=["metadata_only", "no_downloads", "no_transcript_fetch"],
        rationale="Structured rows support focused agency/name/year video searches, but YouTube results still cannot verify artifacts.",
        live_enabled=False,
    )


def _structured_muckrock_plan(fields: Dict[str, Any]) -> ConnectorQueryPlan:
    subject = _subject_name(fields)
    agency = _agency(fields)
    year = _incident_year(fields)
    location = _location_terms(fields)
    queries = []
    if subject or agency:
        queries.append(PlannedQuery(
            query=_join_query([_quote(subject) if subject else None, agency, "bodycam records", year]),
            reason="Search FOIA metadata for bodycam or records requests tied to structured subject/agency anchors.",
            candidate_fields_used=_structured_fields_used(fields),
            expected_source_roles=["claim_source", "possible_artifact_source"],
            risk_flags=["claim_source_not_artifact_source", "structured_dataset_candidate_only"],
        ))
    if agency:
        queries.append(PlannedQuery(
            query=_join_query([_quote(agency), *location, "critical incident records", year]),
            reason="Search FOIA metadata for agency incident records and released-file leads.",
            candidate_fields_used=_structured_fields_used(fields),
            expected_source_roles=["claim_source", "possible_artifact_source"],
            risk_flags=["claims_require_resolver"],
        ))
    return ConnectorQueryPlan(
        connector_name="muckrock",
        priority=_priority(fields, base=82 if agency else 60),
        queries=_dedupe_queries(queries),
        missing_field_requirements=[] if agency else ["agency"],
        risk_flags=["metadata_only", "claims_require_resolver"],
        rationale="Use structured anchors to search FOIA/request metadata; public URLs still require deterministic resolution.",
        live_enabled=False,
    )


def _structured_courtlistener_plan(fields: Dict[str, Any]) -> ConnectorQueryPlan:
    subject = _subject_name(fields)
    location = _location_terms(fields)
    state_terms = _state_terms(fields)
    queries = []
    if subject:
        queries.append(PlannedQuery(
            query=_join_query([_quote(subject), *(state_terms or location)]),
            reason="Search court metadata for outcome corroboration using structured subject and state anchors.",
            candidate_fields_used=[field for field in ["subject_name", "jurisdiction"] if _structured_field_present(fields, field)],
            expected_source_roles=["identity_source", "outcome_source"],
            risk_flags=["candidate_identity_only", "outcome_verification_required"],
        ))
    return ConnectorQueryPlan(
        connector_name="courtlistener",
        priority=_priority(fields, base=88 if subject else 45),
        queries=queries,
        missing_field_requirements=[] if subject else ["subject_name"],
        risk_flags=["metadata_only", "court_source_can_corroborate_but_not_lock_alone"],
        rationale="CourtListener can corroborate identity/outcome, but structured-row anchors do not set final confidence.",
        live_enabled=False,
    )


def _structured_documentcloud_plan(fields: Dict[str, Any]) -> ConnectorQueryPlan:
    subject = _subject_name(fields)
    agency = _agency(fields)
    year = _incident_year(fields)
    queries = []
    query = _join_query([_quote(subject) if subject else None, agency, year, "bodycam", "critical incident video"])
    if query:
        queries.append(PlannedQuery(
            query=query,
            reason="Plan future DocumentCloud search for structured-row documents or media-adjacent public artifacts.",
            candidate_fields_used=_structured_fields_used(fields),
            expected_source_roles=["identity_source", "claim_source", "artifact_source"],
            risk_flags=["future_connector", "artifact_urls_still_require_resolver"],
        ))
    return ConnectorQueryPlan(
        connector_name="documentcloud",
        priority=_priority(fields, base=70),
        queries=queries,
        missing_field_requirements=[] if query else ["subject_name", "agency", "incident_date"],
        risk_flags=["future_connector", "no_live_call"],
        rationale="Represent DocumentCloud as a future target only; W2-lite performs no live document discovery.",
        live_enabled=False,
    )


def _structured_future_broad_search_plan(fields: Dict[str, Any]) -> ConnectorQueryPlan:
    subject = _subject_name(fields)
    agency = _agency(fields)
    year = _incident_year(fields)
    incident_type = fields.get("incident_type")
    query = _join_query([_quote(subject) if subject else None, agency, year, incident_type, "bodycam", "records"])
    queries = []
    if query:
        queries.append(PlannedQuery(
            query=query,
            reason="Plan future broad-search fallback for official/news corroboration after controlled connectors.",
            candidate_fields_used=_structured_fields_used(fields),
            expected_source_roles=["identity_source", "outcome_source", "claim_source"],
            risk_flags=["future_connector", "requires_explicit_opt_in"],
        ))
    return ConnectorQueryPlan(
        connector_name="future_brave_exa",
        priority=_priority(fields, base=40),
        queries=queries,
        missing_field_requirements=[] if query else ["case_anchors"],
        risk_flags=["future_connector", "no_live_call", "requires_explicit_opt_in"],
        rationale="Keep Brave/Exa as a future structured-row fallback only; W2-lite does not run broad search.",
        live_enabled=False,
    )


def _youtube_plan(fields: Dict[str, Any]) -> ConnectorQueryPlan:
    query = _join_query([
        *_identity_terms(fields),
        *_location_terms(fields),
        *_date_terms(fields),
        *_descriptor_terms(fields, limit=2),
        *_artifact_terms(fields, limit=3),
    ])
    queries = []
    if query:
        queries.append(PlannedQuery(
            query=query,
            reason="Search video metadata for candidate media leads using only weak-input anchors.",
            candidate_fields_used=_fields_used(fields, include_artifacts=True),
            expected_source_roles=["possible_artifact_source", "claim_source"],
            risk_flags=["candidate_media_lead_only"],
        ))
    return ConnectorQueryPlan(
        connector_name="youtube",
        priority=_priority(fields, base=90),
        queries=queries,
        missing_field_requirements=[] if query else ["defendant_names", "agency", "jurisdiction", "incident_date", "artifact_signals"],
        risk_flags=["metadata_only", "no_downloads", "no_transcript_fetch"],
        rationale="Use the YouTube connector only as a capped metadata lead source; it cannot verify artifacts.",
        live_enabled=False,
    )


def _muckrock_plan(fields: Dict[str, Any]) -> ConnectorQueryPlan:
    queries = []
    agency = _agency(fields)
    location = _location_terms(fields)
    dates = _date_terms(fields)
    descriptors = _descriptor_terms(fields, limit=1)
    artifacts = _artifact_terms(fields, limit=2)
    if agency or location:
        query = _join_query([agency, *location, *dates, *(artifacts or descriptors or ["records"])])
        queries.append(PlannedQuery(
            query=query,
            reason="Search FOIA request metadata for public-records productions or artifact claims tied to candidate agency/location/date.",
            candidate_fields_used=_fields_used(fields, include_artifacts=True),
            expected_source_roles=["claim_source", "possible_artifact_source"],
            risk_flags=["claim_source_not_artifact_source"],
        ))
    missing = []
    if not agency:
        missing.append("agency")
    if not location:
        missing.append("jurisdiction")
    return ConnectorQueryPlan(
        connector_name="muckrock",
        priority=_priority(fields, base=75 if agency else 55),
        queries=queries,
        missing_field_requirements=missing,
        risk_flags=["metadata_only", "claims_require_resolver"],
        rationale="Use MuckRock for FOIA/request metadata and released-file leads; query results are not verified artifacts.",
        live_enabled=False,
    )


def _courtlistener_plan(fields: Dict[str, Any]) -> ConnectorQueryPlan:
    queries = []
    case_numbers = _case_numbers(fields)
    defendants = _defendant_names(fields)
    location = _location_terms(fields)
    dates = _date_terms(fields)
    charges = _charges(fields)

    for case_number in case_numbers[:2]:
        queries.append(PlannedQuery(
            query=_quote(case_number),
            reason="Search court metadata by candidate case number.",
            candidate_fields_used=["case_numbers"],
            expected_source_roles=["identity_source", "outcome_source"],
            risk_flags=["candidate_case_number_only"],
        ))
    for defendant in defendants[:2]:
        query = _join_query([_quote(defendant), *location, *dates, *charges[:1]])
        queries.append(PlannedQuery(
            query=query,
            reason="Search court metadata for identity and outcome corroboration of a candidate defendant.",
            candidate_fields_used=_fields_used(fields),
            expected_source_roles=["identity_source", "outcome_source"],
            risk_flags=["candidate_identity_only"],
        ))

    missing = []
    if not defendants and not case_numbers:
        missing.extend(["defendant_names", "case_numbers"])
    return ConnectorQueryPlan(
        connector_name="courtlistener",
        priority=_priority(fields, base=85 if defendants or case_numbers else 45),
        queries=queries,
        missing_field_requirements=missing,
        risk_flags=["metadata_only", "court_source_can_corroborate_but_not_lock_alone"],
        rationale="Use CourtListener for court metadata corroboration; deterministic identity/outcome resolvers decide confidence later.",
        live_enabled=False,
    )


def _documentcloud_plan(fields: Dict[str, Any]) -> ConnectorQueryPlan:
    query = _join_query([
        *_identity_terms(fields),
        _agency(fields),
        *_location_terms(fields),
        *_date_terms(fields),
        *_descriptor_terms(fields, limit=1),
        *_artifact_terms(fields, limit=1),
    ])
    queries = []
    if query:
        queries.append(PlannedQuery(
            query=query,
            reason="Plan future DocumentCloud search for public documents or media-adjacent source pages.",
            candidate_fields_used=_fields_used(fields, include_artifacts=True),
            expected_source_roles=["identity_source", "claim_source", "artifact_source"],
            risk_flags=["future_connector", "artifact_urls_still_require_resolver"],
        ))
    return ConnectorQueryPlan(
        connector_name="documentcloud",
        priority=_priority(fields, base=65),
        queries=queries,
        missing_field_requirements=[] if query else ["defendant_names", "agency", "jurisdiction", "incident_date"],
        risk_flags=["future_connector", "no_live_call"],
        rationale="Keep a structured slot for future DocumentCloud discovery without executing it in H2-lite.",
        live_enabled=False,
    )


def _future_broad_search_plan(fields: Dict[str, Any]) -> ConnectorQueryPlan:
    query = _join_query([
        *_identity_terms(fields),
        _agency(fields),
        *_location_terms(fields),
        *_date_terms(fields),
        *_descriptor_terms(fields, limit=2),
        *_artifact_terms(fields, limit=3),
    ])
    queries = []
    if query:
        queries.append(PlannedQuery(
            query=query,
            reason="Plan future broad search fallback for news/official source discovery after controlled connectors are exhausted.",
            candidate_fields_used=_fields_used(fields, include_artifacts=True),
            expected_source_roles=["identity_source", "outcome_source", "claim_source"],
            risk_flags=["future_connector", "broad_search_requires_manual_or_opt_in_execution"],
        ))
    return ConnectorQueryPlan(
        connector_name="future_brave_exa",
        priority=_priority(fields, base=35),
        queries=queries,
        missing_field_requirements=[] if query else ["case_anchors"],
        risk_flags=["future_connector", "no_live_call", "requires_explicit_opt_in"],
        rationale="Represent Brave/Exa as a future query target only; H2-lite performs no broad live search.",
        live_enabled=False,
    )


def _fields_used(fields: Dict[str, Any], *, include_artifacts: bool = False) -> List[str]:
    used = []
    if _defendant_names(fields):
        used.append("defendant_names")
    if _agency(fields):
        used.append("agency")
    if any(_jurisdiction(fields).values()):
        used.append("jurisdiction")
    if _date_terms(fields):
        used.append("incident_date")
    if _case_numbers(fields):
        used.append("case_numbers")
    if _charges(fields):
        used.append("charges")
    if _descriptor_terms(fields):
        used.append("incident_descriptors")
    if include_artifacts and _artifact_terms(fields):
        used.append("artifact_signals")
    return used


def _structured_fields_used(fields: Dict[str, Any]) -> List[str]:
    return [
        field
        for field in ["subject_name", "agency", "jurisdiction", "incident_date", "incident_type", "cause"]
        if _structured_field_present(fields, field)
    ]


def _structured_field_present(fields: Dict[str, Any], field: str) -> bool:
    if field == "subject_name":
        return bool(_subject_name(fields))
    if field == "jurisdiction":
        return any(_jurisdiction(fields).values())
    return bool(fields.get(field))


def _priority(fields: Dict[str, Any], *, base: int) -> int:
    anchor_bonus = min(len(_fields_used(fields, include_artifacts=True)) * 3, 15)
    return max(0, min(100, base + anchor_bonus))


def _identity_terms(fields: Dict[str, Any]) -> List[str]:
    return [_quote(name) for name in _defendant_names(fields)[:2]]


def _defendant_names(fields: Dict[str, Any]) -> List[str]:
    return list(fields.get("defendant_names") or [])


def _subject_name(fields: Dict[str, Any]) -> Optional[str]:
    return fields.get("subject_name") or _first(_defendant_names(fields))


def _agency(fields: Dict[str, Any]) -> Optional[str]:
    return fields.get("agency") or None


def _jurisdiction(fields: Dict[str, Any]) -> Dict[str, Optional[str]]:
    jurisdiction = fields.get("jurisdiction") or {}
    return jurisdiction if isinstance(jurisdiction, dict) else {}


def _location_terms(fields: Dict[str, Any]) -> List[str]:
    jurisdiction = _jurisdiction(fields)
    terms = [jurisdiction.get("city"), jurisdiction.get("county")]
    state = jurisdiction.get("state")
    if state:
        terms.append(STATE_NAMES_BY_ABBR.get(state, state))
    return [term for term in terms if term]


def _date_terms(fields: Dict[str, Any]) -> List[str]:
    raw_dates = fields.get("incident_date_raw_candidates") or []
    if raw_dates:
        return [_quote(raw_dates[0])]
    incident_date = fields.get("incident_date")
    return [_quote(incident_date)] if incident_date else []


def _incident_year(fields: Dict[str, Any]) -> Optional[str]:
    incident_date = fields.get("incident_date")
    if incident_date and re.match(r"\d{4}", str(incident_date)):
        return str(incident_date)[:4]
    return None


def _state_terms(fields: Dict[str, Any]) -> List[str]:
    state = _jurisdiction(fields).get("state")
    if not state:
        return []
    return [STATE_NAMES_BY_ABBR.get(state, state)]


def _case_numbers(fields: Dict[str, Any]) -> List[str]:
    return list(fields.get("case_numbers") or [])


def _charges(fields: Dict[str, Any]) -> List[str]:
    return [_quote(charge) for charge in list(fields.get("charges") or [])[:2]]


def _descriptor_terms(fields: Dict[str, Any], *, limit: int = 2) -> List[str]:
    return [_quote(descriptor) for descriptor in list(fields.get("incident_descriptors") or [])[:limit]]


def _artifact_terms(fields: Dict[str, Any], *, limit: int = 2) -> List[str]:
    signals = fields.get("artifact_signals") or []
    return [ARTIFACT_QUERY_TERMS[signal] for signal in signals if signal in ARTIFACT_QUERY_TERMS][:limit]


def _quote(value: str) -> str:
    return f"\"{value}\""


def _join_query(parts: Iterable[Optional[str]]) -> str:
    return re.sub(r"\s+", " ", " ".join(part for part in parts if part)).strip()


def _first(values: Sequence[str]) -> Optional[str]:
    return values[0] if values else None


def _dedupe_queries(queries: Iterable[PlannedQuery]) -> List[PlannedQuery]:
    result: List[PlannedQuery] = []
    seen = set()
    for query in queries:
        if query.query and query.query not in seen:
            result.append(query)
            seen.add(query.query)
    return result


def _dedupe(values: Iterable[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for value in values:
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    return result
