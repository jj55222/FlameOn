from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, List, Set

from .models import CasePacket, SourceRecord


HIGH_IDENTITY_THRESHOLD = 80.0
MEDIUM_IDENTITY_THRESHOLD = 45.0
STRONG_AUTHORITIES = {"official", "court", "foia"}


@dataclass
class IdentityResolution:
    identity_score: float
    identity_confidence: str
    identity_anchors: List[str] = field(default_factory=list)
    missing_disambiguators: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)


def _text_for(source: SourceRecord) -> str:
    return " ".join(
        part
        for part in [source.title, source.snippet, source.raw_text, source.url]
        if part
    ).lower()


def _words(value: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", value.lower())


def _has_phrase(text: str, value: str | None) -> bool:
    if not value:
        return False
    words = _words(value)
    if not words:
        return False
    return re.search(r"\b" + r"\s+".join(re.escape(w) for w in words) + r"\b", text) is not None


def _last_name(name: str) -> str:
    suffixes = {"jr", "sr", "ii", "iii", "iv", "v"}
    parts = [p for p in _words(name) if p not in suffixes]
    return parts[-1] if parts else ""


def _anchor_sources(sources: Iterable[SourceRecord]) -> List[SourceRecord]:
    return [
        source
        for source in sources
        if "identity_source" in source.source_roles or source.source_authority in STRONG_AUTHORITIES
    ]


def _append_unique(values: List[str], new_values: Iterable[str]) -> List[str]:
    seen = set(values)
    for value in new_values:
        if value not in seen:
            values.append(value)
            seen.add(value)
    return values


def resolve_identity(packet: CasePacket) -> IdentityResolution:
    identity = packet.case_identity
    sources = _anchor_sources(packet.sources)
    searchable_sources = sources or packet.sources
    text_blob = "\n".join(_text_for(source) for source in searchable_sources)
    source_texts = [_text_for(source) for source in searchable_sources]

    anchors: Set[str] = set()
    risk_flags: Set[str] = set()
    missing_disambiguators: Set[str] = set()

    full_name_matched = False
    for name in identity.defendant_names:
        if _has_phrase(text_blob, name):
            anchors.add("full_name")
            full_name_matched = True
            break

    if not full_name_matched:
        for name in identity.defendant_names:
            last = _last_name(name)
            if last and re.search(rf"\b{re.escape(last)}\b", text_blob):
                anchors.add("last_name")
                break

    jurisdiction_matches = []
    juris = identity.jurisdiction
    if _has_phrase(text_blob, juris.city):
        anchors.add("city")
        jurisdiction_matches.append("city")
    if _has_phrase(text_blob, juris.county):
        anchors.add("county")
        jurisdiction_matches.append("county")
    if _has_phrase(text_blob, juris.state):
        anchors.add("state")
        jurisdiction_matches.append("state")

    if identity.agency and _has_phrase(text_blob, identity.agency):
        anchors.add("agency")
    if identity.incident_date and identity.incident_date.lower() in text_blob:
        anchors.add("incident_date")
    if any(case_number.lower() in text_blob for case_number in identity.case_numbers):
        anchors.add("case_number")
    if any(_has_phrase(text_blob, victim) for victim in identity.victim_names):
        anchors.add("victim_name")
    if any(source.source_authority in STRONG_AUTHORITIES for source in searchable_sources):
        anchors.add("source_authority")

    expected_locations = [
        location.lower()
        for location in [juris.city, juris.county, juris.state]
        if location
    ]
    for source_text in source_texts:
        if not any(_has_phrase(source_text, location) for location in expected_locations):
            if re.search(r"\b(california|florida|tennessee|arizona|washington|oregon|ohio|texas|new york)\b", source_text):
                risk_flags.add("conflicting_jurisdiction")
                break

    jurisdiction_anchor_count = len({"city", "county", "state"} & anchors)
    has_jurisdiction = jurisdiction_anchor_count >= 1
    disambiguators = {"agency", "incident_date", "case_number", "victim_name", "source_authority"} & anchors

    score = 0.0
    if "full_name" in anchors:
        score += 35.0
    elif "last_name" in anchors:
        score += 15.0
    score += min(25.0, jurisdiction_anchor_count * 10.0)
    if "agency" in anchors:
        score += 15.0
    if "incident_date" in anchors:
        score += 15.0
    if "case_number" in anchors:
        score += 20.0
    if "victim_name" in anchors:
        score += 12.0
    if "source_authority" in anchors:
        score += 15.0
    score = min(score, 100.0)

    if "full_name" in anchors and {"city"} <= anchors and not disambiguators:
        risk_flags.add("name_city_only")
    if "full_name" in anchors and has_jurisdiction and not disambiguators:
        risk_flags.add("missing_disambiguator")
        missing_disambiguators.update(["incident_date", "agency", "victim_name", "case_number", "source_authority"])
    if "last_name" in anchors and "full_name" not in anchors:
        risk_flags.add("weak_identity")
        risk_flags.add("common_name_risk")
    if not ({"full_name", "last_name"} & anchors) or not has_jurisdiction:
        risk_flags.add("insufficient_identity_anchors")
    if not anchors:
        risk_flags.add("weak_identity")

    can_high = (
        "full_name" in anchors
        and has_jurisdiction
        and bool(disambiguators)
        and "conflicting_jurisdiction" not in risk_flags
    )
    if can_high and score >= HIGH_IDENTITY_THRESHOLD:
        confidence = "high"
    elif score >= MEDIUM_IDENTITY_THRESHOLD and "conflicting_jurisdiction" not in risk_flags:
        confidence = "medium"
    else:
        confidence = "low"

    ordered_anchors = [
        anchor
        for anchor in [
            "full_name",
            "last_name",
            "city",
            "county",
            "state",
            "agency",
            "incident_date",
            "case_number",
            "victim_name",
            "source_authority",
        ]
        if anchor in anchors
    ]
    ordered_risks = [
        risk
        for risk in [
            "weak_identity",
            "name_city_only",
            "missing_disambiguator",
            "common_name_risk",
            "conflicting_jurisdiction",
            "insufficient_identity_anchors",
        ]
        if risk in risk_flags
    ]
    ordered_missing = [
        item
        for item in ["incident_date", "agency", "victim_name", "case_number", "source_authority"]
        if item in missing_disambiguators
    ]

    result = IdentityResolution(
        identity_score=round(score, 2),
        identity_confidence=confidence,
        identity_anchors=ordered_anchors,
        missing_disambiguators=ordered_missing,
        risk_flags=ordered_risks,
    )
    apply_identity_resolution(packet, result)
    return result


def apply_identity_resolution(packet: CasePacket, result: IdentityResolution) -> None:
    packet.scores.identity_score = result.identity_score
    packet.case_identity.identity_confidence = result.identity_confidence
    packet.case_identity.identity_anchors = list(result.identity_anchors)
    _append_unique(packet.risk_flags, result.risk_flags)
