from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Dict, Iterable, List, Set

from .media_relevance import classify_media_relevance
from .models import ArtifactClaim, CasePacket, SourceRecord, VerifiedArtifact


CONCLUDED_OUTCOMES = {"sentenced", "closed", "convicted"}
MEDIA_ARTIFACT_TYPES = {
    "bodycam",
    "dash_cam",
    "interrogation",
    "court_video",
    "dispatch_911",
    "surveillance_video",
    "other_video",
    "audio",
}
DOCUMENT_ARTIFACT_TYPES = {"docket_docs", "document", "other"}
MEDIA_FORMATS = {"video", "audio"}
DOCUMENT_FORMATS = {"pdf", "document", "html"}
SEVERE_PRODUCTION_RISKS = {
    "conflicting_jurisdiction",
    "weak_identity",
    "protected_or_nonpublic_only",
    "identity_unconfirmed",
    "artifact_unverified",
}


@dataclass
class ActionabilityResult:
    research_completeness_score: float
    production_actionability_score: float
    actionability_score: float
    verdict: str
    component_scores: Dict[str, float] = field(default_factory=dict)
    artifact_category_counts: Dict[str, int] = field(default_factory=dict)
    risk_flags: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)
    reason_codes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


def _append_unique(items: List[str], values: Iterable[str]) -> None:
    seen = set(items)
    for value in values:
        if value and value not in seen:
            items.append(value)
            seen.add(value)


def _artifact_categories(artifacts: Iterable[VerifiedArtifact]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for artifact in artifacts:
        category = artifact.artifact_type
        counts[category] = counts.get(category, 0) + 1
    return counts


def _is_media_artifact(artifact: VerifiedArtifact) -> bool:
    return artifact.artifact_type in MEDIA_ARTIFACT_TYPES or artifact.format in MEDIA_FORMATS


def _is_document_artifact(artifact: VerifiedArtifact) -> bool:
    return artifact.artifact_type in DOCUMENT_ARTIFACT_TYPES or artifact.format in DOCUMENT_FORMATS


def _identity_score(confidence: str, *, research: bool) -> float:
    if confidence == "high":
        return 25.0 if research else 20.0
    if confidence == "medium":
        return 15.0 if research else 10.0
    return 5.0 if research else 0.0


def _outcome_score(status: str, *, research: bool) -> float:
    if research:
        return {
            "sentenced": 20.0,
            "closed": 20.0,
            "convicted": 18.0,
            "dismissed": 16.0,
            "acquitted": 16.0,
            "charged": 10.0,
            "unknown": 0.0,
        }.get(status, 0.0)
    return {
        "sentenced": 15.0,
        "closed": 15.0,
        "convicted": 13.0,
        "dismissed": 8.0,
        "acquitted": 8.0,
        "charged": 5.0,
        "unknown": 0.0,
    }.get(status, 0.0)


def _document_research_score(packet: CasePacket, document_artifacts: List[VerifiedArtifact]) -> float:
    identity = packet.case_identity
    score = 0.0
    if identity.case_numbers:
        score += 4.0
    if identity.charges:
        score += 3.0
    if identity.victim_names:
        score += 3.0
    if identity.agency:
        score += 3.0
    if identity.incident_date:
        score += 3.0
    if document_artifacts:
        score += 12.0 if len(document_artifacts) == 1 else 16.0
    if any(source.source_authority in {"court", "foia", "official"} for source in packet.sources):
        score += 4.0
    return min(score, 20.0)


def _source_quality_score(sources: List[SourceRecord]) -> float:
    authorities = {source.source_authority for source in sources if source.source_authority != "unknown"}
    roles = {role for source in sources for role in source.source_roles}
    officialish = authorities & {"official", "court", "foia"}
    score = min(len(authorities) * 3.0, 9.0)
    score += min(len(roles) * 1.5, 4.5)
    if officialish:
        score += 1.5
    return min(score, 15.0)


def _artifact_claim_score(claims: List[ArtifactClaim]) -> float:
    score = 0.0
    for claim in claims:
        if claim.claim_label == "artifact_released":
            score += 6.0
        elif claim.claim_label in {"artifact_requested", "artifact_withheld"}:
            score += 4.0
        elif claim.claim_label == "artifact_mentioned_only":
            score += 2.0
    return min(score, 10.0)


def _gap_clarity_score(packet: CasePacket) -> float:
    if packet.next_actions:
        return min(10.0, 4.0 + len(packet.next_actions) * 2.0)
    if not packet.input.missing_fields:
        return 10.0
    return 4.0


def _media_artifact_score(media_artifacts: List[VerifiedArtifact]) -> float:
    if not media_artifacts:
        return 0.0
    weights = {
        "bodycam": 26.0,
        "interrogation": 26.0,
        "court_video": 20.0,
        "dispatch_911": 18.0,
        "surveillance_video": 18.0,
        "dash_cam": 18.0,
        "other_video": 16.0,
        "audio": 14.0,
    }
    score = 0.0
    for artifact in media_artifacts:
        score += weights.get(artifact.artifact_type, 16.0 if artifact.format == "video" else 12.0)
        if artifact.downloadable:
            score += 2.0
        if artifact.source_authority in {"official", "court", "foia"}:
            score += 2.0
    return min(score, 40.0)


def _portfolio_score(media_artifacts: List[VerifiedArtifact], document_artifacts: List[VerifiedArtifact]) -> float:
    media_categories: Set[str] = {artifact.artifact_type for artifact in media_artifacts}
    if not media_categories:
        return 3.0 if document_artifacts else 0.0

    score = 6.0 if len(media_categories) == 1 else 12.0
    if len(media_categories) >= 3:
        score = 15.0

    premium_sets = [
        {"bodycam", "interrogation"},
        {"bodycam", "dispatch_911"},
        {"bodycam", "court_video"},
        {"interrogation", "court_video"},
        {"surveillance_video", "dispatch_911"},
    ]
    if any(combo <= media_categories for combo in premium_sets):
        score = 15.0
    elif document_artifacts:
        score = min(15.0, score + 2.0)
    return score


def _downstream_readiness_score(media_artifacts: List[VerifiedArtifact], document_artifacts: List[VerifiedArtifact]) -> float:
    score = 0.0
    if any(artifact.downloadable for artifact in media_artifacts):
        score += 4.0
    if any(artifact.source_url or artifact.artifact_url for artifact in media_artifacts):
        score += 2.0
    if any(artifact.format in MEDIA_FORMATS for artifact in media_artifacts):
        score += 1.0
    if any(artifact.matched_case_fields for artifact in media_artifacts):
        score += 1.0
    if document_artifacts:
        score += 2.0
    if media_artifacts and not any(artifact.requires_manual_download for artifact in media_artifacts):
        score += 1.0
    return min(score, 10.0)


def _reason_codes(
    packet: CasePacket,
    media_artifacts: List[VerifiedArtifact],
    document_artifacts: List[VerifiedArtifact],
    production_score: float,
) -> List[str]:
    identity = packet.case_identity
    codes: List[str] = []
    if identity.identity_confidence == "high":
        codes.append("high_identity")
    elif identity.identity_confidence == "medium":
        codes.append("medium_identity")
    else:
        codes.append("low_identity")

    if identity.outcome_status in CONCLUDED_OUTCOMES:
        codes.append("sentenced_or_convicted")
    elif identity.outcome_status != "unknown":
        codes.append("outcome_not_concluded")

    if media_artifacts:
        codes.append("media_artifact_present")
    if len(media_artifacts) >= 2:
        codes.append("multiple_media_artifacts")
    media_categories = {artifact.artifact_type for artifact in media_artifacts}
    if len(media_categories) >= 2 or _portfolio_score(media_artifacts, document_artifacts) >= 15.0:
        codes.append("artifact_portfolio_strong")
    if "bodycam" in media_categories:
        codes.append("bodycam_present")
    if "interrogation" in media_categories:
        codes.append("interrogation_present")
    if "dispatch_911" in media_categories:
        codes.append("dispatch_audio_present")
    if "court_video" in media_categories:
        codes.append("court_video_present")
    if document_artifacts:
        codes.append("supporting_documents_present")
    if document_artifacts and not media_artifacts:
        codes.append("document_only_hold")
    if packet.artifact_claims and not packet.verified_artifacts:
        codes.append("artifact_claim_unresolved")
    if not media_artifacts:
        codes.append("no_verified_media")
    if production_score >= 70.0:
        codes.append("production_score_threshold_met")
    return codes


def _generated_risks(packet: CasePacket, media_artifacts: List[VerifiedArtifact], document_artifacts: List[VerifiedArtifact]) -> List[str]:
    risks = list(packet.risk_flags)
    if packet.case_identity.identity_confidence == "low":
        _append_unique(risks, ["weak_identity", "identity_unconfirmed"])
    if not media_artifacts:
        _append_unique(risks, ["no_verified_media"])
    if "protected_or_nonpublic" in risks and not media_artifacts:
        _append_unique(risks, ["protected_or_nonpublic_only"])
    if packet.artifact_claims and not packet.verified_artifacts:
        _append_unique(risks, ["artifact_unverified"])
    if document_artifacts and not media_artifacts:
        _append_unique(risks, ["document_only"])
    return risks


def _next_actions(packet: CasePacket, media_artifacts: List[VerifiedArtifact]) -> List[str]:
    actions = list(packet.next_actions)
    if not media_artifacts:
        _append_unique(actions, ["Locate verified media/audio/video artifact before production."])
    if packet.artifact_claims and not packet.verified_artifacts:
        _append_unique(actions, ["Resolve artifact claim into a public artifact URL."])
    if packet.case_identity.identity_confidence != "high":
        _append_unique(actions, ["Corroborate identity with date, agency, victim, case number, or official source."])
    if packet.case_identity.outcome_status not in CONCLUDED_OUTCOMES:
        _append_unique(actions, ["Confirm concluded outcome before production."])
    return actions


def _add_media_quality_advisories(
    *,
    verdict: str,
    media_artifacts: List[VerifiedArtifact],
    risks: List[str],
    reasons: List[str],
    actions: List[str],
) -> None:
    """Append advisory-only media relevance warnings.

    These flags are deliberately added after the core verdict is
    computed so media-quality rollout cannot silently change existing
    PRODUCE/HOLD/SKIP behavior.
    """

    if not media_artifacts:
        return

    relevance = [classify_media_relevance(artifact) for artifact in media_artifacts]
    if any("media_query_artifact_type_mismatch" in r.risk_flags for r in relevance):
        _append_unique(risks, ["media_query_artifact_type_mismatch"])
        _append_unique(reasons, ["query_artifact_type_not_confirmed_by_metadata"])
        _append_unique(actions, ["manually_verify_media_relevance"])

    if verdict == "PRODUCE" and all(
        r.media_relevance_tier in {"C", "unknown"} for r in relevance
    ):
        _append_unique(risks, ["produce_based_on_weak_or_uncertain_media"])
        _append_unique(reasons, ["produce_based_on_weak_or_uncertain_media"])
        _append_unique(actions, ["manually_verify_media_relevance"])


def _verdict(
    packet: CasePacket,
    production_score: float,
    media_artifacts: List[VerifiedArtifact],
    document_artifacts: List[VerifiedArtifact],
    risks: List[str],
) -> str:
    identity = packet.case_identity
    severe_risks = SEVERE_PRODUCTION_RISKS & set(risks)
    if (
        identity.identity_confidence == "high"
        and identity.outcome_status in CONCLUDED_OUTCOMES
        and media_artifacts
        and production_score >= 70.0
        and not severe_risks
    ):
        return "PRODUCE"

    if "conflicting_jurisdiction" in risks:
        return "SKIP"

    promising = any([
        identity.identity_confidence == "high" and identity.outcome_status in CONCLUDED_OUTCOMES,
        bool(packet.artifact_claims),
        bool(media_artifacts),
        bool(document_artifacts) and identity.identity_confidence in {"medium", "high"},
    ])
    return "HOLD" if promising else "SKIP"


def score_case_packet(packet: CasePacket) -> ActionabilityResult:
    """Score CaseGraph research value separately from production readiness.

    The function is pure: it reads the CasePacket and returns a scoring result
    without changing packet scores, verdict, risk flags, or next actions.
    """

    media_artifacts = [artifact for artifact in packet.verified_artifacts if _is_media_artifact(artifact)]
    document_artifacts = [artifact for artifact in packet.verified_artifacts if _is_document_artifact(artifact) and not _is_media_artifact(artifact)]

    identity_research = _identity_score(packet.case_identity.identity_confidence, research=True)
    outcome_research = _outcome_score(packet.case_identity.outcome_status, research=True)
    documents_research = _document_research_score(packet, document_artifacts)
    source_quality = _source_quality_score(packet.sources)
    artifact_claims = _artifact_claim_score(packet.artifact_claims)
    gap_clarity = _gap_clarity_score(packet)
    research_score = round(min(
        identity_research + outcome_research + documents_research + source_quality + artifact_claims + gap_clarity,
        100.0,
    ), 2)

    identity_production = _identity_score(packet.case_identity.identity_confidence, research=False)
    outcome_production = _outcome_score(packet.case_identity.outcome_status, research=False)
    media_score = _media_artifact_score(media_artifacts)
    portfolio = _portfolio_score(media_artifacts, document_artifacts)
    readiness = _downstream_readiness_score(media_artifacts, document_artifacts)
    production_score = round(min(identity_production + outcome_production + media_score + portfolio + readiness, 100.0), 2)

    component_scores = {
        "identity": identity_production,
        "outcome": outcome_production,
        "artifact_claims": artifact_claims,
        "verified_media_artifacts": media_score,
        "verified_document_artifacts": min(len(document_artifacts) * 8.0, 20.0),
        "artifact_portfolio": portfolio,
        "downstream_readiness": readiness,
        "research_documents": documents_research,
        "source_quality_diversity": source_quality,
        "gap_clarity": gap_clarity,
    }
    categories = _artifact_categories(packet.verified_artifacts)
    risks = _generated_risks(packet, media_artifacts, document_artifacts)
    actions = _next_actions(packet, media_artifacts)
    verdict = _verdict(packet, production_score, media_artifacts, document_artifacts, risks)
    reasons = _reason_codes(packet, media_artifacts, document_artifacts, production_score)
    if verdict == "HOLD" and not media_artifacts and packet.artifact_claims:
        _append_unique(reasons, ["claim_only_hold"])
    if verdict == "HOLD" and document_artifacts and not media_artifacts:
        _append_unique(reasons, ["document_only_hold"])
    _add_media_quality_advisories(
        verdict=verdict,
        media_artifacts=media_artifacts,
        risks=risks,
        reasons=reasons,
        actions=actions,
    )

    aggregate = round((production_score * 0.65) + (research_score * 0.35), 2)
    return ActionabilityResult(
        research_completeness_score=research_score,
        production_actionability_score=production_score,
        actionability_score=aggregate,
        verdict=verdict,
        component_scores=component_scores,
        artifact_category_counts=categories,
        risk_flags=risks,
        next_actions=actions,
        reason_codes=reasons,
    )
