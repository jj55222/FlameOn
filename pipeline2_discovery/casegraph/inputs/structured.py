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

# Fatal Encounters dataset uses long, parenthesized human-readable column
# names (the public FE CSV) plus a few snake_case aliases that researchers
# commonly use after pre-processing. Listed in priority order.
FATAL_ENCOUNTERS_SUBJECT_NAME_KEYS = [
    "Person",
    "Subject's name",
    "person",
    "subject_name",
    "name",
]
FATAL_ENCOUNTERS_AGENCY_KEYS = [
    "Agency or agencies involved",
    "agency_or_agencies_involved",
    "agency",
]
FATAL_ENCOUNTERS_CITY_KEYS = [
    "Location of death (city)",
    "location_of_death_city",
    "city",
]
FATAL_ENCOUNTERS_COUNTY_KEYS = [
    "Location of death (county)",
    "location_of_death_county",
    "county",
]
FATAL_ENCOUNTERS_STATE_KEYS = ["State", "state"]
FATAL_ENCOUNTERS_DATE_KEYS = [
    "Date of injury resulting in death (month/day/year)",
    "date_of_injury_resulting_in_death",
    "date_of_injury",
    "incident_date",
    "date",
]
FATAL_ENCOUNTERS_INCIDENT_TYPE_KEYS = [
    "Highest level of force",
    "highest_level_of_force",
    "incident_type",
]
FATAL_ENCOUNTERS_CAUSE_KEYS = ["Cause of death", "cause_of_death", "cause"]
FATAL_ENCOUNTERS_SOURCE_URL_KEYS = [
    "Link to news article or photo of official document",
    "url_of_news_article",
    "url_of_official_release",
    "source_url",
    "url",
]
FATAL_ENCOUNTERS_DEMOGRAPHIC_AGE_KEYS = ["age", "Age"]
FATAL_ENCOUNTERS_DEMOGRAPHIC_RACE_KEYS = ["race", "Race"]
FATAL_ENCOUNTERS_DEMOGRAPHIC_GENDER_KEYS = ["gender", "Gender"]

# Mapping Police Violence dataset uses victim-centric column names plus
# common snake_case aliases. MPV does not separate "level of force" from
# "cause of death", so the parser leaves incident_type empty unless the
# row supplies an explicit incident_type alias.
MPV_SUBJECT_NAME_KEYS = [
    "Victim's name",
    "victim_name",
    "victims_name",
    "name",
    "subject_name",
]
MPV_AGENCY_KEYS = [
    "Agency responsible for death",
    "agency_responsible_for_death",
    "agency_responsible",
    "agency",
]
MPV_CITY_KEYS = ["City", "city"]
MPV_COUNTY_KEYS = ["County", "county"]
MPV_STATE_KEYS = ["State", "state"]
MPV_DATE_KEYS = [
    "Date of Incident (month/day/year)",
    "Date of incident",
    "date_of_incident",
    "incident_date",
    "date",
]
MPV_INCIDENT_TYPE_KEYS = ["incident_type"]
MPV_CAUSE_KEYS = ["Cause of death", "cause_of_death", "cause"]
MPV_SOURCE_URL_KEYS = [
    "Link to article",
    "Link to news article or photo of official document",
    "URL of news article",
    "url_of_news_article",
    "link_to_news_article_or_photo_of_official_document",
    "source_url",
    "url",
]
MPV_DEMOGRAPHIC_AGE_KEYS = ["Victim's age", "victim_age", "age"]
MPV_DEMOGRAPHIC_RACE_KEYS = ["Victim's race", "victim_race", "race"]
MPV_DEMOGRAPHIC_GENDER_KEYS = ["Victim's gender", "victim_gender", "gender"]
MPV_BODY_CAMERA_KEYS = [
    "Body Camera (Source: WaPo)",
    "Body Camera",
    "body_camera",
]


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


def parse_fatal_encounters_case_input(row: Dict[str, Any]) -> StructuredInputParseResult:
    """Normalize a Fatal Encounters–style row into candidate CaseInput anchors.

    Mirrors `parse_wapo_uof_case_input` but reads Fatal Encounters column
    names (e.g. "Person", "Agency or agencies involved",
    "Location of death (city)") and falls back to common snake_case
    aliases. Like the WaPo parser, it never asserts identity, never
    creates VerifiedArtifacts, never sets a verdict, and never overrides
    outcome — every field stays a candidate anchor.
    """

    dataset_name = _clean(_first_present(row, DATASET_KEYS)) or "fatal_encounters"
    subject_name = _clean(_first_present(row, FATAL_ENCOUNTERS_SUBJECT_NAME_KEYS))
    agency = _clean(_first_present(row, FATAL_ENCOUNTERS_AGENCY_KEYS))
    city = _clean(_first_present(row, FATAL_ENCOUNTERS_CITY_KEYS))
    county = _normalize_county(_clean(_first_present(row, FATAL_ENCOUNTERS_COUNTY_KEYS)))
    state = _normalize_state(_clean(_first_present(row, FATAL_ENCOUNTERS_STATE_KEYS)))
    incident_date = _normalize_date(_first_present(row, FATAL_ENCOUNTERS_DATE_KEYS))
    incident_type = _clean(_first_present(row, FATAL_ENCOUNTERS_INCIDENT_TYPE_KEYS))
    cause = _clean(_first_present(row, FATAL_ENCOUNTERS_CAUSE_KEYS))
    source_url = _clean(_first_present(row, FATAL_ENCOUNTERS_SOURCE_URL_KEYS))
    demographics = _demographics_with_keys(
        row,
        age_keys=FATAL_ENCOUNTERS_DEMOGRAPHIC_AGE_KEYS,
        race_keys=FATAL_ENCOUNTERS_DEMOGRAPHIC_RACE_KEYS,
        gender_keys=FATAL_ENCOUNTERS_DEMOGRAPHIC_GENDER_KEYS,
    )
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


def parse_mapping_police_violence_case_input(row: Dict[str, Any]) -> StructuredInputParseResult:
    """Normalize a Mapping Police Violence row into candidate CaseInput anchors.

    Same shape as `parse_wapo_uof_case_input` and
    `parse_fatal_encounters_case_input`: dataset_row input_type, identical
    known_fields keys, dataset_name="mapping_police_violence". Reads MPV's
    victim-centric column names (e.g. "Victim's name", "Agency
    responsible for death", "Date of Incident (month/day/year)") plus
    common snake_case aliases.

    MPV records a "Body Camera" field per row (yes/no/unknown). The
    parser surfaces this as a `source_notes` entry only — never as an
    ArtifactClaim or VerifiedArtifact, since it is metadata about the
    incident, not a claim about a public artifact URL.
    """

    dataset_name = _clean(_first_present(row, DATASET_KEYS)) or "mapping_police_violence"
    subject_name = _clean(_first_present(row, MPV_SUBJECT_NAME_KEYS))
    agency = _clean(_first_present(row, MPV_AGENCY_KEYS))
    city = _clean(_first_present(row, MPV_CITY_KEYS))
    county = _normalize_county(_clean(_first_present(row, MPV_COUNTY_KEYS)))
    state = _normalize_state(_clean(_first_present(row, MPV_STATE_KEYS)))
    incident_date = _normalize_date(_first_present(row, MPV_DATE_KEYS))
    incident_type = _clean(_first_present(row, MPV_INCIDENT_TYPE_KEYS))
    cause = _clean(_first_present(row, MPV_CAUSE_KEYS))
    source_url = _clean(_first_present(row, MPV_SOURCE_URL_KEYS))
    body_camera = _clean(_first_present(row, MPV_BODY_CAMERA_KEYS))
    demographics = _demographics_with_keys(
        row,
        age_keys=MPV_DEMOGRAPHIC_AGE_KEYS,
        race_keys=MPV_DEMOGRAPHIC_RACE_KEYS,
        gender_keys=MPV_DEMOGRAPHIC_GENDER_KEYS,
    )
    source_notes = _source_notes(dataset_name, source_url, row)
    if body_camera:
        source_notes.append(f"body_camera_flag:{body_camera.lower()}")

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


def _demographics_with_keys(
    row: Dict[str, Any],
    *,
    age_keys: Iterable[str],
    race_keys: Iterable[str],
    gender_keys: Iterable[str],
) -> Dict[str, Any]:
    """Like `_demographics` but reads from configurable key lists.

    Used for datasets that capitalize column names (e.g. Fatal Encounters
    "Race"/"Gender"). The output keys are still normalized to lower-case
    snake_case so downstream consumers see one shape.
    """
    demographics: Dict[str, Any] = {}
    age = _first_present(row, age_keys)
    if age not in [None, ""]:
        demographics["age"] = age
    race = _first_present(row, race_keys)
    if race not in [None, ""]:
        demographics["race"] = race
    gender = _first_present(row, gender_keys)
    if gender not in [None, ""]:
        demographics["gender"] = gender
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
