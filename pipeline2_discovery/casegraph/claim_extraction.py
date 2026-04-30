from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Tuple

from .models import ArtifactClaim, CasePacket, SourceRecord


ARTIFACT_PATTERNS: Dict[str, List[str]] = {
    "bodycam": [
        r"\bbodycam\b",
        r"\bbody cam\b",
        r"\bbody camera\b",
        r"\bbody[- ]worn camera\b",
        r"\bBWC\b",
        r"\bdashcam\b",
        r"\bdash cam\b",
        r"\bofficer camera\b",
        r"\bcritical incident video\b",
        r"\bOIS video\b",
        r"\bofficer[- ]involved shooting video\b",
    ],
    "interrogation": [
        r"\binterrogation\b",
        r"\bconfession\b",
        r"\bpolice interview\b",
        r"\bdetective interview\b",
        r"\bcustodial interview\b",
        r"\binterview recording\b",
        r"\binterview video\b",
    ],
    "court_video": [
        r"\bcourt video\b",
        r"\btrial video\b",
        r"\bsentencing video\b",
        r"\bhearing video\b",
        r"\bcourtroom video\b",
        r"\bcourt audio\b",
        r"\bCourt TV\b",
    ],
    "dispatch_911": [
        r"\b911 call\b",
        r"\b911 audio\b",
        r"\bdispatch audio\b",
        r"\bemergency call\b",
        r"\bdispatch recording\b",
    ],
    "docket_docs": [
        r"\bdocket\b",
        r"\bcomplaint\b",
        r"\baffidavit\b",
        r"\bindictment\b",
        r"\bprobable cause\b",
        r"\bcharging document\b",
        r"\bcourt filing\b",
        r"\bpolice report\b",
        r"\bincident report\b",
        r"\bpublic records production\b",
        r"\bresponsive records\b",
    ],
    "surveillance_video": [
        r"\bsurveillance video\b",
        r"\bsecurity video\b",
    ],
    "audio": [
        r"\baudio recording\b",
    ],
}

LABEL_PATTERNS: Dict[str, List[str]] = {
    "artifact_withheld": [
        r"\bvideo was withheld\b",
        r"\brecords were denied\b",
        r"\bagency refused to release\b",
        r"\brefused to release\b",
        r"\bexempt from disclosure\b",
        r"\bredacted in full\b",
        r"\bno responsive records\b",
        r"\brequest rejected\b",
    ],
    "artifact_released": [
        r"\breleased (?:the )?.{0,80}(?:footage|video|audio|call|records|documents|files)\b",
        r"\b(?:footage|video|audio|call|records|documents|files).{0,80}\bwas released\b",
        r"\bpublished .{0,80}(?:video|footage|audio|records|documents)\b",
        r"\bposted .{0,80}(?:video|footage|audio|records|documents)\b",
        r"\brecords were produced\b",
        r"\bdocuments were released\b",
        r"\bproduction includes\b",
        r"\bproduced included\b",
        r"\battached files include\b",
    ],
    "artifact_requested": [
        r"\brequested .{0,80}(?:footage|video|audio|records|documents|report)\b",
        r"\bseeking .{0,80}(?:footage|video|audio|records|documents|report)\b",
        r"\brequest for .{0,80}(?:footage|video|audio|records|documents|report)\b",
        r"\bFOIA request for\b",
        r"\bpublic records request seeking\b",
        r"\basked for .{0,80}(?:reports|records|footage|video|audio)\b",
    ],
    "artifact_mentioned_only": [
        r"\bofficer activated\b",
        r"\bbodycam captured\b",
        r"\bcaptured the incident\b",
        r"\b911 caller reported\b",
        r"\bsurveillance video showed\b",
        r"\bcourt heard audio\b",
    ],
}

LABEL_CONFIDENCE = {
    "artifact_released": 0.75,
    "artifact_requested": 0.6,
    "artifact_withheld": 0.7,
    "artifact_mentioned_only": 0.45,
    "no_artifact": 0.0,
}


@dataclass
class ClaimExtractionResult:
    artifact_claims: List[ArtifactClaim] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)


def _source_text(source: SourceRecord) -> str:
    return " ".join(part for part in [source.title, source.snippet, source.raw_text] if part)


def _find_artifact_types(text: str) -> List[Tuple[str, re.Match[str]]]:
    matches: List[Tuple[str, re.Match[str]]] = []
    for artifact_type, patterns in ARTIFACT_PATTERNS.items():
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                matches.append((artifact_type, match))
                break
    return matches


def _claim_label(text: str) -> str:
    for label in ["artifact_withheld", "artifact_released", "artifact_requested", "artifact_mentioned_only"]:
        if any(re.search(pattern, text, re.I) for pattern in LABEL_PATTERNS[label]):
            return label
    return "artifact_mentioned_only"


def _snippet(text: str, match: Optional[re.Match[str]]) -> str:
    if not text:
        return ""
    if not match:
        return re.sub(r"\s+", " ", text[:260]).strip()
    start = max(0, match.start() - 120)
    end = min(len(text), match.end() + 160)
    return re.sub(r"\s+", " ", text[start:end]).strip()


def _claim_id(source_id: str, artifact_type: str, claim_label: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_]+", "_", f"{source_id}_{artifact_type}_{claim_label}")
    return f"claim_{safe.strip('_')}"


def _confidence(source: SourceRecord, claim_label: str) -> float:
    confidence = LABEL_CONFIDENCE[claim_label]
    if "claim_source" in source.source_roles:
        confidence += 0.05
    if source.source_authority in {"official", "court", "foia"}:
        confidence += 0.05
    return round(min(confidence, 0.95), 2)


def _withheld_side_effects() -> Tuple[List[str], List[str]]:
    return (
        ["artifact_withheld", "access_limited"],
        ["Follow up on withheld/request-denied artifact through records status or manual review."],
    )


def extract_artifact_claims(packet: CasePacket) -> ClaimExtractionResult:
    claims: List[ArtifactClaim] = []
    risk_flags: List[str] = []
    next_actions: List[str] = []
    seen: Set[Tuple[str, str, str]] = set()

    for source in packet.sources:
        text = _source_text(source)
        artifact_matches = _find_artifact_types(text)
        if not artifact_matches:
            continue
        label = _claim_label(text)
        for artifact_type, match in artifact_matches:
            key = (source.source_id, artifact_type, label)
            if key in seen:
                continue
            seen.add(key)
            claim_risks: List[str] = []
            claim_next_actions: List[str] = []
            if label == "artifact_withheld":
                claim_risks, claim_next_actions = _withheld_side_effects()
                risk_flags.extend(claim_risks)
                next_actions.extend(claim_next_actions)
            claims.append(ArtifactClaim(
                claim_id=_claim_id(source.source_id, artifact_type, label),
                artifact_type=artifact_type,
                claim_label=label,
                claim_source_id=source.source_id,
                claim_source_url=source.url,
                supporting_snippet=_snippet(text, match),
                claim_confidence=_confidence(source, label),
                risk_flags=claim_risks,
                next_actions=claim_next_actions,
            ))

    packet.artifact_claims.extend(claims)
    _append_unique(packet.risk_flags, risk_flags)
    _append_unique(packet.next_actions, next_actions)
    return ClaimExtractionResult(
        artifact_claims=claims,
        risk_flags=_dedupe(risk_flags),
        next_actions=_dedupe(next_actions),
    )


def _dedupe(values: Iterable[str]) -> List[str]:
    result: List[str] = []
    _append_unique(result, values)
    return result


def _append_unique(values: List[str], new_values: Iterable[str]) -> None:
    seen = set(values)
    for value in new_values:
        if value not in seen:
            values.append(value)
            seen.add(value)
