from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

from ..models import CaseInput
from ..routers import STATE_ABBREVS


STATE_NAMES_BY_ABBR = {abbr: name.title() for name, abbr in STATE_ABBREVS.items()}

SUBJECT_NAME_KEYS = ["subject_name", "name", "person_name", "decedent_name", "victim_name"]
AGENCY_KEYS = ["agency", "agency_name", "department", "law_enforcement_agency"]
CITY_KEYS = ["city", "incident_city"]
COUNTY_KEYS = ["county", "incident_county"]
STATE_KEYS = ["state", "state_abbr", "incident_state"]
DATE_KEYS = ["incident_date", "date", "date_of_incident"]
INCIDENT_TYPE_KEYS = ["incident_type", "manner_of_death", "threat_type"]
CAUSE_KEYS = ["cause", "category", "cause_category", "armed", "weapon", "threat_level"]
SOURCE_URL_KEYS = ["source_url", "url", "article_url", "source"]
DATASET_KEYS = ["dataset", "dataset_name"]


@dataclass
class StructuredInputParseResult:
    case_input: CaseInput
    dataset_name: str = "wapo_uof"
    risk_flags: List[str] = field(default_factory=list)
    source_notes: List[str] = field(default_factory=list)


def parse_wapo_uof_case_input(row: Dict[str, Any]) -> StructuredInputParseResult:
    """Normalize a WaPo-style use-of-force row into candidate CaseInput anchors."""

    dataset_name = _clean(_first_present(row, DATASET_KEYS)) or "wapo_uof"
    subject_name = _clean(_first_present(row, SUBJECT_NAME_KEYS))
    agency = _clean(_first_present(row, AGENCY_KEYS))
    city = _clean(_first_present(row, CITY_KEYS))
    county = _normalize_county(_clean(_first_present(row, COUNTY_KEYS)))
    state = _normalize_state(_clean(_first_present(row, STATE_KEYS)))
    incident_date = _normalize_date(_first_present(row, DATE_KEYS))
    incident_type = _clean(_first_present(row, INCIDENT_TYPE_KEYS))
    cause = _clean(_first_present(row, CAUSE_KEYS))
    source_url = _clean(_first_present(row, SOURCE_URL_KEYS))
    demographics = _demographics(row)
    source_notes = _source_notes(dataset_name, source_url, row)

    known_fields = {
        "defendant_names": [subject_name] if subject_name else [],
        "subject_name": subject_name,
        "agency": agency,
        "jurisdiction": {
            "city": city,
            "county": county,
            "state": state,
        },
        "incident_date": incident_date,
        "incident_type": incident_type,
        "cause": cause,
        "source_url": source_url,
        "demographics": demographics,
        "dataset_name": dataset_name,
        "source_notes": source_notes,
    }
    missing_fields = _missing_fields(known_fields)
    risk_flags = _risk_flags(known_fields, missing_fields)
    candidate_queries = _candidate_queries(known_fields)

    return StructuredInputParseResult(
        case_input=CaseInput(
            input_type="dataset_row",
            raw_input=dict(row),
            known_fields=known_fields,
            missing_fields=missing_fields,
            candidate_queries=candidate_queries,
        ),
        dataset_name=dataset_name,
        risk_flags=risk_flags,
        source_notes=source_notes,
    )


def _first_present(row: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in row and row[key] not in [None, ""]:
            return row[key]
    return None


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def _normalize_county(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if value.lower().endswith("county"):
        return value
    return f"{value} County"


def _normalize_state(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    if len(value) == 2:
        return value.upper()
    return STATE_ABBREVS.get(value.lower(), value)


def _normalize_date(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    for fmt in ["%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"]:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return text


def _demographics(row: Dict[str, Any]) -> Dict[str, Any]:
    demographics: Dict[str, Any] = {}
    for key in ["age", "race", "gender"]:
        value = row.get(key)
        if value not in [None, ""]:
            demographics[key] = value
    return demographics


def _source_notes(dataset_name: str, source_url: Optional[str], row: Dict[str, Any]) -> List[str]:
    notes = [f"dataset:{dataset_name}"]
    if source_url:
        notes.append("source_url_present")
    if row.get("id"):
        notes.append(f"dataset_row_id:{row['id']}")
    return notes


def _missing_fields(fields: Dict[str, Any]) -> List[str]:
    missing: List[str] = []
    if not fields["defendant_names"]:
        missing.append("subject_name")
    if not fields["agency"]:
        missing.append("agency")
    jurisdiction = fields["jurisdiction"]
    if not any(jurisdiction.values()):
        missing.append("jurisdiction")
    else:
        if not jurisdiction.get("city"):
            missing.append("jurisdiction.city")
        if not jurisdiction.get("county"):
            missing.append("jurisdiction.county")
        if not jurisdiction.get("state"):
            missing.append("jurisdiction.state")
    if not fields["incident_date"]:
        missing.append("incident_date")
    if not fields["incident_type"]:
        missing.append("incident_type")
    if not fields["source_url"]:
        missing.append("source_url")
    missing.extend(["case_numbers", "charges", "outcome_status", "verified_artifacts"])
    return missing


def _risk_flags(fields: Dict[str, Any], missing_fields: List[str]) -> List[str]:
    risks = [
        "structured_dataset_candidate_only",
        "candidate_fields_not_identity_lock",
        "artifact_verification_required",
        "outcome_verification_required",
    ]
    if "subject_name" in missing_fields:
        risks.append("missing_subject_name")
    if "agency" in missing_fields:
        risks.append("missing_agency")
    if "jurisdiction" in missing_fields:
        risks.append("missing_location")
    if "incident_date" in missing_fields:
        risks.append("missing_incident_date")
    if "source_url" in missing_fields:
        risks.append("missing_source_url")
    return _dedupe(risks)


def _candidate_queries(fields: Dict[str, Any]) -> List[str]:
    subject_name = fields["subject_name"]
    agency = fields["agency"]
    jurisdiction = fields["jurisdiction"]
    city = jurisdiction.get("city")
    state = _state_display(jurisdiction.get("state"))
    incident_date = fields["incident_date"]
    year = incident_date[:4] if incident_date and re.match(r"\d{4}", incident_date) else None
    incident_type = fields["incident_type"]
    cause = fields["cause"]

    queries = []
    if subject_name:
        queries.append(_join_query([_quote(subject_name), agency, city, state, year, incident_type or cause]))
        queries.append(_join_query([_quote(subject_name), state, "police shooting", year]))
        queries.append(_join_query([_quote(subject_name), agency, "bodycam records"]))
    if agency:
        queries.append(_join_query([_quote(agency), "critical incident video", year, subject_name]))
        queries.append(_join_query([_quote(agency), "bodycam", "records", subject_name]))
    if city or state:
        queries.append(_join_query([city, state, year, incident_type or cause, "police shooting"]))
    return _dedupe([query for query in queries if query])[:8]


def _state_display(abbr: Optional[str]) -> Optional[str]:
    if not abbr:
        return None
    return STATE_NAMES_BY_ABBR.get(abbr, abbr)


def _quote(value: str) -> str:
    return f"\"{value}\""


def _join_query(parts: Iterable[Any]) -> str:
    return re.sub(r"\s+", " ", " ".join(str(part) for part in parts if part)).strip()


def _dedupe(values: Iterable[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for value in values:
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    return result
