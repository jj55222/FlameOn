from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set

from .models import CasePacket, SourceRecord


STRONG_AUTHORITIES = {"court", "official", "foia"}

OUTCOME_PATTERNS: Dict[str, List[str]] = {
    "sentenced": [
        r"\bsentenced to\b",
        r"\bwas sentenced\b",
        r"\bsentence imposed\b",
        r"\blife in prison\b",
        r"\b\d+\s+years in prison\b",
    ],
    "convicted": [
        r"\bconvicted\b",
        r"\bfound guilty\b",
        r"(?<!not\s)\bguilty verdict\b",
        r"\bpleaded guilty\b",
        r"\bpled guilty\b",
    ],
    "charged": [
        r"\bcharged with\b",
        r"\barrested for\b",
        r"\bindicted\b",
        r"\barraigned\b",
    ],
    "dismissed": [
        r"\bcharges dismissed\b",
        r"\bcase dismissed\b",
        r"\bdismissed by judge\b",
    ],
    "acquitted": [
        r"\bacquitted\b",
        r"\bfound not guilty\b",
        r"\bnot guilty verdict\b",
    ],
    "closed": [
        r"\bclosed case\b",
        r"\bfinal judgment\b",
        r"\bcase resolved\b",
    ],
}

OUTCOME_PRIORITY = {
    "unknown": 0,
    "charged": 10,
    "convicted": 20,
    "closed": 25,
    "sentenced": 30,
    "dismissed": 40,
    "acquitted": 40,
}


@dataclass
class OutcomeCandidate:
    status: str
    anchor: str
    source_id: str
    source_authority: str
    source_has_identity_anchor: bool


@dataclass
class OutcomeResolution:
    outcome_status: str
    outcome_score: float
    outcome_confidence: str
    outcome_anchors: List[str] = field(default_factory=list)
    supporting_sources: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)


def _text_for(source: SourceRecord) -> str:
    return " ".join(
        part
        for part in [source.title, source.snippet, source.raw_text, source.url]
        if part
    ).lower()


def _words(value: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", value.lower())


def _has_phrase(text: str, value: Optional[str]) -> bool:
    if not value:
        return False
    words = _words(value)
    if not words:
        return False
    return re.search(r"\b" + r"\s+".join(re.escape(w) for w in words) + r"\b", text) is not None


def _source_has_identity_anchor(packet: CasePacket, source: SourceRecord) -> bool:
    text = _text_for(source)
    identity = packet.case_identity
    has_name = any(_has_phrase(text, name) for name in identity.defendant_names)
    juris = identity.jurisdiction
    has_location = any(_has_phrase(text, value) for value in [juris.city, juris.county, juris.state])
    matched_fields = set(source.matched_case_fields)
    return (
        has_name and has_location
    ) or bool(
        matched_fields
        & {
            "defendant_full_name",
            "case_number",
            "agency",
            "victim_name",
            "incident_date",
        }
    )


def _find_candidates(packet: CasePacket) -> List[OutcomeCandidate]:
    candidates: List[OutcomeCandidate] = []
    for source in packet.sources:
        text = _text_for(source)
        source_has_identity_anchor = _source_has_identity_anchor(packet, source)
        for status, patterns in OUTCOME_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, text, re.I):
                    candidates.append(OutcomeCandidate(
                        status=status,
                        anchor=f"{status}:{pattern}",
                        source_id=source.source_id,
                        source_authority=source.source_authority,
                        source_has_identity_anchor=source_has_identity_anchor,
                    ))
                    break
    return candidates


def _choose_status(candidates: List[OutcomeCandidate]) -> str:
    if not candidates:
        return "unknown"
    statuses = {candidate.status for candidate in candidates}
    if "dismissed" in statuses:
        return "dismissed"
    if "acquitted" in statuses:
        return "acquitted"
    return max(statuses, key=lambda status: OUTCOME_PRIORITY[status])


def _append_unique(values: List[str], new_values: Iterable[str]) -> None:
    seen = set(values)
    for value in new_values:
        if value not in seen:
            values.append(value)
            seen.add(value)


def resolve_outcome(packet: CasePacket) -> OutcomeResolution:
    candidates = _find_candidates(packet)
    status = _choose_status(candidates)
    selected = [candidate for candidate in candidates if candidate.status == status]
    statuses = {candidate.status for candidate in candidates}
    identity_confidence = packet.case_identity.identity_confidence
    identity_is_low = identity_confidence == "low"
    identity_conflicted = "conflicting_jurisdiction" in packet.risk_flags
    has_strong_authority = any(candidate.source_authority in STRONG_AUTHORITIES for candidate in selected)
    has_identity_anchor = any(candidate.source_has_identity_anchor for candidate in selected)

    risk_flags: Set[str] = set()
    next_actions: List[str] = []
    if len(statuses) > 1:
        risk_flags.add("conflicting_outcome_signals")
    if identity_is_low:
        risk_flags.add("weak_identity")
        risk_flags.add("identity_unconfirmed")
    if identity_conflicted:
        risk_flags.add("conflicting_jurisdiction")
    if candidates and not has_identity_anchor:
        risk_flags.add("identity_unconfirmed")

    if status == "unknown":
        score = 0.0
        confidence = "low"
        next_actions.append("Find outcome_source confirming charged, convicted, sentenced, closed, dismissed, or acquitted status.")
    else:
        score = 35.0 + OUTCOME_PRIORITY[status]
        if has_strong_authority:
            score += 20.0
        if has_identity_anchor:
            score += 15.0
        if identity_confidence == "high":
            score += 15.0
        elif identity_confidence == "medium":
            score += 8.0
        if identity_is_low or identity_conflicted:
            score = min(score, 55.0)
        score = min(score, 100.0)

        if (
            score >= 80.0
            and not identity_is_low
            and not identity_conflicted
            and has_identity_anchor
            and (has_strong_authority or identity_confidence == "high")
        ):
            confidence = "high"
        elif score >= 55.0 and not identity_conflicted and has_identity_anchor:
            confidence = "medium"
        else:
            confidence = "low"

    anchors = [candidate.anchor for candidate in selected]
    supporting_sources = [candidate.source_id for candidate in selected]
    result = OutcomeResolution(
        outcome_status=status,
        outcome_score=round(score, 2),
        outcome_confidence=confidence,
        outcome_anchors=anchors,
        supporting_sources=supporting_sources,
        risk_flags=[
            risk
            for risk in [
                "weak_identity",
                "identity_unconfirmed",
                "conflicting_jurisdiction",
                "conflicting_outcome_signals",
            ]
            if risk in risk_flags
        ],
        next_actions=next_actions,
    )
    apply_outcome_resolution(packet, result)
    return result


def apply_outcome_resolution(packet: CasePacket, result: OutcomeResolution) -> None:
    packet.case_identity.outcome_status = result.outcome_status
    packet.scores.outcome_score = result.outcome_score
    _append_unique(packet.risk_flags, result.risk_flags)
    _append_unique(packet.next_actions, result.next_actions)
