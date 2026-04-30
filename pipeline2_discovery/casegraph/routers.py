from __future__ import annotations

import re
from typing import List, Optional

from .models import CaseIdentity, CaseInput, CasePacket, Jurisdiction, Scores


STATE_ABBREVS = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "district of columbia": "DC",
}


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def _slug(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_") or "unknown"


def _split_names(defendant_names: str) -> List[str]:
    names = [n.strip() for n in re.split(r",|;|\band\b", defendant_names or "") if n.strip()]
    return names or ([defendant_names.strip()] if defendant_names.strip() else [])


def parse_jurisdiction(jurisdiction: str) -> Jurisdiction:
    parts = [_clean(p) for p in (jurisdiction or "").split(",")]
    parts = [p for p in parts if p]
    city = parts[0] if parts else None
    county = None
    state = None

    if len(parts) >= 3:
        county = parts[1]
        state = parts[-1]
    elif len(parts) == 2:
        if "county" in parts[0].lower():
            county = parts[0]
            city = None
        state = parts[1]
    elif len(parts) == 1 and len(parts[0]) == 2:
        city = None
        state = parts[0].upper()

    if state:
        state = STATE_ABBREVS.get(state.lower(), state.upper() if len(state) == 2 else state)

    return Jurisdiction(city=city, county=county, state=state)


def _manual_candidate_queries(defendant_names: List[str], jurisdiction: Jurisdiction) -> List[str]:
    if not defendant_names:
        return []
    primary = defendant_names[0]
    city = jurisdiction.city or ""
    state = jurisdiction.state or ""
    queries = [
        f"\"{primary}\" \"{city}\" \"{state}\"".strip(),
        f"\"{primary}\" \"{city}\" court case".strip(),
        f"\"{primary}\" bodycam interrogation 911",
    ]
    if city:
        queries.append(f"\"{primary}\" {city} police")
    return [re.sub(r"\s+", " ", q).strip() for q in queries if q.strip()]


def route_manual_defendant_jurisdiction(defendant_names: str, jurisdiction: str) -> CasePacket:
    names = _split_names(defendant_names)
    parsed_jurisdiction = parse_jurisdiction(jurisdiction)
    missing_fields = [
        "victim_names",
        "agency",
        "incident_date",
        "case_numbers",
        "charges",
        "outcome_status",
    ]
    known_fields = {
        "defendant_names": names,
        "jurisdiction": {
            "city": parsed_jurisdiction.city,
            "county": parsed_jurisdiction.county,
            "state": parsed_jurisdiction.state,
        },
    }
    candidate_queries = _manual_candidate_queries(names, parsed_jurisdiction)
    primary_name = names[0] if names else "unknown"
    case_id_parts = [
        "manual",
        primary_name,
        parsed_jurisdiction.city or "",
        parsed_jurisdiction.state or "",
    ]
    case_id = _slug("_".join(part for part in case_id_parts if part))

    case_input = CaseInput(
        input_type="manual",
        raw_input={
            "defendant_names": defendant_names,
            "jurisdiction": jurisdiction,
        },
        known_fields=known_fields,
        missing_fields=missing_fields,
        candidate_queries=candidate_queries,
    )
    identity = CaseIdentity(
        defendant_names=names,
        jurisdiction=parsed_jurisdiction,
        identity_confidence="low",
        identity_anchors=[],
    )
    return CasePacket(
        case_id=case_id,
        input=case_input,
        case_identity=identity,
        scores=Scores(),
        verdict="HOLD",
        next_actions=[
            "Find identity_source corroborating defendant, jurisdiction, and incident details.",
            "Find outcome_source confirming charged, convicted, sentenced, closed, dismissed, or acquitted status.",
        ],
        risk_flags=[
            "identity_not_locked",
            "no_verified_artifacts",
        ],
    )
