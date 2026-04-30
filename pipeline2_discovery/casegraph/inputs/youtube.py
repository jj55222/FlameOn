from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from ..models import CaseInput
from ..routers import STATE_ABBREVS


MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

MONTH_NAMES = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}

DATE_RE = re.compile(
    r"\b(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
    r"\.?\s+(?P<day>[0-3]?\d)(?:st|nd|rd|th)?,?\s+(?P<year>(?:19|20)\d{2})\b",
    re.I,
)

NAME_TOKEN = r"(?:[A-Z][a-z]+|[A-Z]\.)"
FULL_NAME = rf"{NAME_TOKEN}(?:\s+{NAME_TOKEN}){{1,3}}"

DEFENDANT_PATTERNS = [
    re.compile(rf"\b(?:suspect|defendant|arrestee|subject)\s+(?:identified\s+as\s+|named\s+|is\s+|was\s+)?(?P<name>{FULL_NAME})\b"),
    re.compile(rf"\b(?:detectives|police|officers)\s+(?:interviewed|arrested|charged|detained)\s+(?P<name>{FULL_NAME})\b"),
    re.compile(rf"\b(?P<name>{FULL_NAME})\s+(?:was\s+)?(?:arrested|charged|convicted|sentenced)\b"),
]

VICTIM_PATTERNS = [
    re.compile(rf"\bvictim\s+(?:identified\s+as\s+|named\s+|was\s+)?(?P<name>{FULL_NAME})\b"),
    re.compile(rf"\b(?:killed|shot|stabbed|assaulted)\s+victim\s+(?P<name>{FULL_NAME})\b"),
]

AGENCY_PATTERNS = [
    re.compile(r"\b(?P<agency>[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,5}\s+Police Department)\b"),
    re.compile(r"\b(?P<agency>[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,5}\s+Sheriff(?:'s)? Office)\b"),
    re.compile(r"\b(?P<agency>[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,5}\s+Department of Public Safety)\b"),
    re.compile(r"\b(?P<agency>[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,5}\s+District Attorney(?:'s)? Office)\b"),
    re.compile(r"\b(?P<agency>[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,5}\s+State Attorney(?:'s)? Office)\b"),
]

CITY_STATE_RE = re.compile(
    r"\b(?:in|near|from|at)\s+(?P<city>[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,2}),\s+"
    r"(?P<state>[A-Z]{2}|[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\b"
)

COUNTY_RE = re.compile(r"\b(?P<county>[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,2}) County\b")

CASE_NUMBER_PATTERNS = [
    re.compile(r"\b(?:case\s*(?:no\.?|number|#)|docket\s*(?:no\.?|number|#))\s*[:#]?\s*(?P<number>[A-Z0-9][A-Z0-9-]{3,})\b", re.I),
    re.compile(r"\b(?P<number>(?:CR|CF|F|M)-?\d{2,4}-\d{3,8})\b", re.I),
]

CHARGE_RE = re.compile(
    r"\b(?:charged with|convicted of|sentenced for|arrested for|pleaded guilty to)\s+"
    r"(?P<charge>[A-Za-z][A-Za-z0-9 ,'-]{2,120})",
    re.I,
)

OUTCOME_PATTERNS = {
    "charged": [r"\bcharged with\b", r"\barrested for\b"],
    "convicted": [r"\bconvicted of\b", r"\bfound guilty\b", r"\bpleaded guilty\b"],
    "sentenced": [r"\bsentenced to\b", r"\bsentencing\b"],
    "closed": [r"\bcase closed\b", r"\bclosed case\b"],
    "dismissed": [r"\bdismissed\b"],
    "acquitted": [r"\bacquitted\b", r"\bfound not guilty\b"],
}

ARTIFACT_PATTERNS = {
    "bodycam": [
        r"\bbodycam\b",
        r"\bbody cam\b",
        r"\bbody camera\b",
        r"\bbody[- ]worn camera\b",
        r"\bBWC\b",
    ],
    "dashcam": [r"\bdashcam\b", r"\bdash cam\b"],
    "dispatch_911": [r"\b911 call\b", r"\b911 audio\b", r"\bdispatch audio\b", r"\bdispatch recording\b"],
    "interrogation": [r"\binterrogation\b", r"\bconfession\b", r"\bpolice interview\b", r"\bdetective interview\b"],
    "surveillance_video": [r"\bsurveillance video\b", r"\bsecurity video\b", r"\bsurveillance footage\b"],
    "court_video": [r"\bcourt video\b", r"\btrial video\b", r"\bsentencing video\b", r"\bcourt audio\b"],
    "critical_incident_video": [r"\bcritical incident video\b", r"\bofficial incident video\b"],
}

INCIDENT_DESCRIPTOR_PATTERNS = [
    ("physical disturbance", r"\bphysical disturbance\b"),
    ("disabled vehicle", r"\bdisabled vehicle\b"),
    ("vehicle hit curb", r"\bvehicle (?:hit|struck) (?:a )?curb\b"),
    ("crowd", r"\bcrowd\b"),
    ("traffic stop", r"\btraffic stop\b"),
    ("officer-involved shooting", r"\bofficer[- ]involved shooting\b"),
    ("domestic disturbance", r"\bdomestic disturbance\b"),
    ("pursuit", r"\bpursuit\b"),
    ("crash", r"\bcrash\b"),
    ("standoff", r"\bstandoff\b"),
]

STATE_NAMES_BY_ABBR = {abbr: name.title() for name, abbr in STATE_ABBREVS.items()}


@dataclass
class YouTubeInputParseResult:
    case_input: CaseInput
    candidate_fields: Dict[str, Any] = field(default_factory=dict)
    artifact_signals: List[str] = field(default_factory=list)
    outcome_terms: List[str] = field(default_factory=list)
    incident_descriptors: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    # Per-segment view of the conflict-prone fields (incident_date, agency,
    # location_city, defendant_names). Only populated when at least two
    # segments disagree. Each entry is keyed by segment name (title /
    # description / transcript) and holds the raw values that segment
    # contributed. The combined-text extraction in candidate_fields is
    # never silently overwritten — conflicts surface as risk flags here
    # so downstream gates can treat them as ambiguous until corroborated.
    segment_conflicts: Dict[str, Dict[str, List[str]]] = field(default_factory=dict)


def parse_youtube_case_input(payload: Dict[str, Any]) -> YouTubeInputParseResult:
    """Extract weak case-input candidates from static YouTube text fields only."""

    title = _coerce_text(payload.get("title"))
    description = _coerce_text(payload.get("description"))
    transcript = _combined_values(
        payload.get("transcript"),
        payload.get("transcript_text"),
        payload.get("captions"),
        payload.get("captions_text"),
    )
    channel = _coerce_text(payload.get("channel"))
    video_url = _coerce_text(payload.get("video_url") or payload.get("url"))
    published_date = _coerce_text(payload.get("published_date") or payload.get("upload_date"))
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}

    text = _combined_values(title, description, transcript, channel)
    segments = {"title": title, "description": description, "transcript": transcript}
    date_mentions = _extract_dates(text)
    defendants = _extract_names(text, DEFENDANT_PATTERNS)
    victims = _extract_names(text, VICTIM_PATTERNS)
    agencies = _extract_agencies(text)
    jurisdiction = _extract_jurisdiction(text, agencies)
    case_numbers = _extract_case_numbers(text)
    charges = _extract_charges(text)
    outcome_terms = _extract_outcome_terms(text)
    artifact_signals = _extract_artifact_signals(text)
    descriptors = _extract_incident_descriptors(text)
    segment_conflicts = _detect_segment_conflicts(segments)

    candidate_fields = _candidate_fields(
        defendant_names=defendants,
        victim_names=victims,
        agency=agencies[0] if agencies else None,
        jurisdiction=jurisdiction,
        date_mentions=date_mentions,
        case_numbers=case_numbers,
        charges=charges,
        outcome_terms=outcome_terms,
        artifact_signals=artifact_signals,
        incident_descriptors=descriptors,
    )
    candidate_queries = _candidate_queries(candidate_fields)
    missing_fields = _missing_fields(candidate_fields)
    risk_flags = _risk_flags(candidate_fields, missing_fields)
    for conflict_field in segment_conflicts:
        risk_flags.append(f"conflicting_{conflict_field}_across_segments")
    risk_flags = _dedupe(risk_flags)

    raw_input = {
        "title": title,
        "description": description,
        "transcript": transcript,
        "channel": channel,
        "video_url": video_url,
        "published_date": published_date,
        "metadata": metadata,
    }
    case_input = CaseInput(
        input_type="youtube_lead",
        raw_input=raw_input,
        known_fields=candidate_fields,
        missing_fields=missing_fields,
        candidate_queries=candidate_queries,
    )
    return YouTubeInputParseResult(
        case_input=case_input,
        candidate_fields=candidate_fields,
        artifact_signals=artifact_signals,
        outcome_terms=outcome_terms,
        incident_descriptors=descriptors,
        risk_flags=risk_flags,
        segment_conflicts=segment_conflicts,
    )


def _detect_segment_conflicts(segments: Dict[str, str]) -> Dict[str, Dict[str, List[str]]]:
    """Scan each segment (title / description / transcript) independently and
    flag fields where the segments disagree.

    Returns a dict keyed by field name (incident_date, agency, location_city,
    defendant_names). Each value is a per-segment view of what THAT segment
    contributed — preserves all candidates so callers can audit which
    segment introduced which value. Only populated for fields where two
    or more segments produced non-empty, mutually disjoint values.
    """
    conflicts: Dict[str, Dict[str, List[str]]] = {}

    per_segment_dates = {
        seg: [mention["date"] for mention in _extract_dates(text)]
        for seg, text in segments.items()
        if text
    }
    if _segments_disagree(per_segment_dates):
        conflicts["incident_date"] = per_segment_dates

    per_segment_agencies = {
        seg: _extract_agencies(text)
        for seg, text in segments.items()
        if text
    }
    if _segments_disagree(per_segment_agencies):
        conflicts["agency"] = per_segment_agencies

    per_segment_cities: Dict[str, List[str]] = {}
    for seg, text in segments.items():
        if not text:
            continue
        seg_jurisdiction = _extract_jurisdiction(text, _extract_agencies(text))
        city = seg_jurisdiction.get("city")
        per_segment_cities[seg] = [city] if city else []
    if _segments_disagree(per_segment_cities):
        conflicts["location_city"] = per_segment_cities

    per_segment_names = {
        seg: _extract_names(text, DEFENDANT_PATTERNS)
        for seg, text in segments.items()
        if text
    }
    if _segments_disagree(per_segment_names):
        conflicts["defendant_names"] = per_segment_names

    return conflicts


def _segments_disagree(per_segment: Dict[str, List[str]]) -> bool:
    """Return True iff at least two segments contributed non-empty value
    sets and the agreement is not unanimous.

    Disagreement: the union of all contributing segments is strictly
    bigger than at least one segment's contribution — i.e. some value
    appears in one segment but is missing from another segment that
    DID contribute something.
    """
    non_empty = {seg: set(values) for seg, values in per_segment.items() if values}
    if len(non_empty) < 2:
        return False
    union = set().union(*non_empty.values())
    return any(values != union for values in non_empty.values())


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return re.sub(r"\s+", " ", value).strip()
    if isinstance(value, dict):
        if "text" in value:
            return _coerce_text(value["text"])
        return _combined_values(*value.values())
    if isinstance(value, list):
        return _combined_values(*value)
    return re.sub(r"\s+", " ", str(value)).strip()


def _combined_values(*values: Any) -> str:
    return " ".join(_dedupe([text for text in (_coerce_text(value) for value in values) if text]))


def _extract_dates(text: str) -> List[Dict[str, str]]:
    mentions: List[Dict[str, str]] = []
    seen = set()
    for match in DATE_RE.finditer(text):
        month = MONTHS[match.group("month").lower()[:3]]
        day = int(match.group("day"))
        year = int(match.group("year"))
        try:
            parsed = date(year, month, day)
        except ValueError:
            continue
        iso_date = parsed.isoformat()
        if iso_date in seen:
            continue
        seen.add(iso_date)
        mentions.append({
            "date": iso_date,
            "raw": f"{MONTH_NAMES[month]} {day}, {year}",
        })
    return mentions


def _extract_names(text: str, patterns: Sequence[re.Pattern[str]]) -> List[str]:
    names: List[str] = []
    for pattern in patterns:
        for match in pattern.finditer(text):
            names.append(_clean_name(match.group("name")))
    return _dedupe([name for name in names if name])


def _clean_name(value: str) -> str:
    value = re.sub(r"\s+", " ", value).strip(" ,.;:-")
    blocked = {"Police Department", "Sheriff Office", "Sheriff's Office"}
    return "" if value in blocked else value


def _extract_agencies(text: str) -> List[str]:
    agencies: List[str] = []
    for pattern in AGENCY_PATTERNS:
        for match in pattern.finditer(text):
            agencies.append(re.sub(r"\s+", " ", match.group("agency")).strip())
    return _dedupe(agencies)


def _extract_jurisdiction(text: str, agencies: Sequence[str]) -> Dict[str, Optional[str]]:
    city: Optional[str] = None
    county: Optional[str] = None
    state: Optional[str] = None

    city_state_match = CITY_STATE_RE.search(text)
    if city_state_match:
        city = city_state_match.group("city")
        state = _normalize_state(city_state_match.group("state")) or state

    county_match = COUNTY_RE.search(text)
    if county_match:
        county = f"{county_match.group('county')} County"

    state = state or _find_state(text)

    for agency in agencies:
        if not city and agency.endswith(" Police Department"):
            agency_city = agency[: -len(" Police Department")].strip()
            if "County" not in agency_city:
                city = agency_city
        if not county and "County Sheriff" in agency:
            county = agency.split(" Sheriff", 1)[0].strip()

    return {"city": city, "county": county, "state": state}


def _find_state(text: str) -> Optional[str]:
    for name, abbr in STATE_ABBREVS.items():
        if re.search(rf"\b{re.escape(name)}\b", text, re.I):
            return abbr
    abbreviations = "|".join(sorted(set(STATE_ABBREVS.values())))
    match = re.search(rf",\s*(?P<abbr>{abbreviations})\b", text)
    return match.group("abbr") if match else None


def _normalize_state(value: str) -> Optional[str]:
    clean = value.strip()
    if len(clean) == 2 and clean.upper() in set(STATE_ABBREVS.values()):
        return clean.upper()
    return STATE_ABBREVS.get(clean.lower())


def _extract_case_numbers(text: str) -> List[str]:
    case_numbers: List[str] = []
    for pattern in CASE_NUMBER_PATTERNS:
        for match in pattern.finditer(text):
            case_numbers.append(match.group("number").upper())
    return _dedupe(case_numbers)


def _extract_charges(text: str) -> List[str]:
    charges: List[str] = []
    for match in CHARGE_RE.finditer(text):
        charge = re.split(r"\b(?:after|when|during|on|in)\b|[.;\n]", match.group("charge"), maxsplit=1, flags=re.I)[0]
        charge = re.sub(r"\s+", " ", charge).strip(" ,.;:-")
        if charge:
            charges.append(charge.lower())
    return _dedupe(charges)


def _extract_outcome_terms(text: str) -> List[str]:
    terms: List[str] = []
    for term, patterns in OUTCOME_PATTERNS.items():
        if any(re.search(pattern, text, re.I) for pattern in patterns):
            terms.append(term)
    return terms


def _extract_artifact_signals(text: str) -> List[str]:
    signals: List[str] = []
    for signal, patterns in ARTIFACT_PATTERNS.items():
        if any(re.search(pattern, text, re.I) for pattern in patterns):
            signals.append(signal)
    return signals


def _extract_incident_descriptors(text: str) -> List[str]:
    descriptors = [
        descriptor
        for descriptor, pattern in INCIDENT_DESCRIPTOR_PATTERNS
        if re.search(pattern, text, re.I)
    ]
    return _dedupe(descriptors)


def _candidate_fields(
    *,
    defendant_names: List[str],
    victim_names: List[str],
    agency: Optional[str],
    jurisdiction: Dict[str, Optional[str]],
    date_mentions: List[Dict[str, str]],
    case_numbers: List[str],
    charges: List[str],
    outcome_terms: List[str],
    artifact_signals: List[str],
    incident_descriptors: List[str],
) -> Dict[str, Any]:
    incident_date = date_mentions[0]["date"] if len(date_mentions) == 1 else None
    return {
        "defendant_names": defendant_names,
        "victim_names": victim_names,
        "agency": agency,
        "jurisdiction": jurisdiction,
        "incident_date": incident_date,
        "incident_date_candidates": [mention["date"] for mention in date_mentions],
        "incident_date_raw_candidates": [mention["raw"] for mention in date_mentions],
        "case_numbers": case_numbers,
        "charges": charges,
        "outcome_terms": outcome_terms,
        "incident_type": incident_descriptors[0] if incident_descriptors else None,
        "incident_descriptors": incident_descriptors,
        "artifact_signals": artifact_signals,
    }


def _missing_fields(fields: Dict[str, Any]) -> List[str]:
    missing: List[str] = []
    if not fields["defendant_names"]:
        missing.append("defendant_names")
    if not fields["victim_names"]:
        missing.append("victim_names")
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
    if not fields["case_numbers"]:
        missing.append("case_numbers")
    if not fields["charges"]:
        missing.append("charges")
    if not fields["outcome_terms"]:
        missing.append("outcome_status")
    return missing


def _risk_flags(fields: Dict[str, Any], missing_fields: Sequence[str]) -> List[str]:
    risk_flags = ["youtube_candidate_lead_only"]
    if "defendant_names" in missing_fields:
        risk_flags.append("missing_defendant_name")
    if "agency" in missing_fields:
        risk_flags.append("missing_agency")
    if "jurisdiction" in missing_fields:
        risk_flags.append("missing_location")
    if "incident_date" in missing_fields:
        risk_flags.append("missing_incident_date")
    if len(fields["incident_date_candidates"]) > 1:
        risk_flags.append("conflicting_incident_dates")
        risk_flags.append("ambiguous_incident_date")

    has_case_anchor = bool(
        fields["defendant_names"]
        or fields["agency"]
        or any(fields["jurisdiction"].values())
        or fields["incident_date"]
        or fields["case_numbers"]
        or fields["charges"]
    )
    if not has_case_anchor:
        risk_flags.append("no_case_anchors")
    if fields["artifact_signals"] and not has_case_anchor:
        risk_flags.append("artifact_signal_without_case_anchors")
    return _dedupe(risk_flags)


def _candidate_queries(fields: Dict[str, Any]) -> List[str]:
    queries: List[str] = []
    defendant = _first(fields["defendant_names"])
    agency = fields["agency"]
    jurisdiction = fields["jurisdiction"]
    city = jurisdiction.get("city")
    state = _state_display(jurisdiction.get("state"))
    raw_dates = fields["incident_date_raw_candidates"]
    date_text = _first(raw_dates)
    descriptors = fields["incident_descriptors"]
    artifacts = _artifact_query_terms(fields["artifact_signals"])

    if defendant:
        parts = [_quote(defendant)]
        if agency:
            parts.append(_quote(agency))
        elif city:
            parts.append(city)
        if state:
            parts.append(state)
        if date_text:
            parts.append(_quote(date_text))
        parts.extend(artifacts[:2] or ["case"])
        queries.append(_join_query(parts))

    if agency:
        parts = [_quote(agency)]
        if date_text:
            parts.append(_quote(date_text))
        if artifacts:
            parts.extend(artifacts[:2])
        elif descriptors:
            parts.append(_quote(descriptors[0]))
        else:
            parts.append("case")
        queries.append(_join_query(parts))

    if date_text and (state or city or descriptors):
        parts = [_quote(date_text)]
        if city:
            parts.append(city)
        if state:
            parts.append(state)
        parts.extend(_quote(descriptor) for descriptor in descriptors[:2])
        if not descriptors:
            parts.extend(artifacts[:1])
        queries.append(_join_query(parts))

    if not queries and artifacts:
        parts = artifacts[:2]
        if descriptors:
            parts.extend(_quote(descriptor) for descriptor in descriptors[:2])
        queries.append(_join_query(parts))

    return _dedupe([query for query in queries if query])[:8]


def _artifact_query_terms(signals: Sequence[str]) -> List[str]:
    terms = {
        "bodycam": "bodycam",
        "dashcam": "dashcam",
        "dispatch_911": "\"911 call\"",
        "interrogation": "interrogation",
        "surveillance_video": "\"surveillance video\"",
        "court_video": "\"court video\"",
        "critical_incident_video": "\"critical incident video\"",
    }
    return [terms[signal] for signal in signals if signal in terms]


def _state_display(abbr: Optional[str]) -> Optional[str]:
    if not abbr:
        return None
    return STATE_NAMES_BY_ABBR.get(abbr, abbr)


def _quote(value: str) -> str:
    return f"\"{value}\""


def _join_query(parts: Iterable[str]) -> str:
    return re.sub(r"\s+", " ", " ".join(part for part in parts if part)).strip()


def _first(values: Sequence[str]) -> Optional[str]:
    return values[0] if values else None


def _dedupe(values: Iterable[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for value in values:
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    return result
