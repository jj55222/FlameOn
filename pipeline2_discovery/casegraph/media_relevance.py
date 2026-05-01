"""MEDIA4 - metadata-only media relevance grading.

This module grades already-verified media artifacts by production
relevance. It is advisory and deterministic: it never fetches URLs,
downloads files, scrapes pages, or reads transcripts. Inputs are the
metadata already attached to a VerifiedArtifact plus an optional
SourceRecord.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import urlparse

from .media_policy import classify_media_url
from .models import SourceRecord, VerifiedArtifact


PRIMARY_TERMS: Sequence[tuple[str, str]] = (
    ("bodycam", "bodycam"),
    ("body cam", "bodycam"),
    ("body-cam", "bodycam"),
    ("body worn", "bodycam"),
    ("body-worn", "bodycam"),
    ("bwc", "bodycam"),
    ("dashcam", "dashcam"),
    ("dash_cam", "dashcam"),
    ("dash cam", "dashcam"),
    ("dash-cam", "dashcam"),
    ("interrogation", "interrogation"),
    ("confession", "confession"),
    ("police interview", "police_interview"),
    ("detective interview", "police_interview"),
    ("surveillance", "surveillance"),
    ("surveillance_video", "surveillance"),
    ("cctv", "surveillance"),
    ("911", "dispatch_911"),
    ("dispatch", "dispatch_911"),
    ("critical incident", "critical_incident"),
    ("officer involved shooting", "official_critical_incident"),
    ("officer-involved shooting", "official_critical_incident"),
    ("ois", "official_critical_incident"),
    ("raw footage", "raw_footage"),
)

SECONDARY_TERMS: Sequence[tuple[str, str]] = (
    ("court video", "court_video"),
    ("court_video", "court_video"),
    ("courtroom video", "court_video"),
    ("courtroom footage", "courtroom_footage"),
    ("court footage", "courtroom_footage"),
    ("sentencing", "sentencing_video"),
    ("trial video", "trial_video"),
    ("trial_video", "trial_video"),
    ("trial footage", "trial_video"),
    ("raw courtroom", "raw_courtroom_recording"),
    ("courtroom recording", "raw_courtroom_recording"),
    ("jail interview", "jail_prison_interview"),
    ("prison interview", "jail_prison_interview"),
)

WEAK_TERMS: Sequence[tuple[str, str]] = (
    ("commentary", "commentary"),
    ("documentary", "documentary"),
    ("explainer", "explainer"),
    ("analysis", "analysis"),
    ("reaction", "reaction"),
    ("podcast", "podcast"),
    ("true crime", "true_crime"),
    ("news package", "reused_news_package"),
    ("reuploaded", "reused_news_package"),
    ("re-uploaded", "reused_news_package"),
)

OFFICIAL_TERMS = (
    "police department",
    "sheriff",
    "district attorney",
    "prosecutor",
    "department of public safety",
    "public safety",
    "critical incident",
    "official",
)

PROTECTED_RISKS = {"protected_or_nonpublic", "pacer_or_paywalled"}


@dataclass
class MediaRelevanceResult:
    artifact_id: str
    artifact_url: str
    media_relevance_tier: str
    media_relevance_score: float
    primary_source_likelihood: float
    official_source_likelihood: float
    reason_codes: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    needs_manual_review: bool = False
    matched_terms: List[str] = field(default_factory=list)
    mismatch_warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _append_unique(items: List[str], values: Iterable[str]) -> None:
    seen = set(items)
    for value in values:
        if value and value not in seen:
            items.append(value)
            seen.add(value)


def _flatten_metadata(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        out: List[str] = []
        for key, item in value.items():
            out.extend(_flatten_metadata(key))
            out.extend(_flatten_metadata(item))
        return out
    if isinstance(value, (list, tuple, set)):
        out = []
        for item in value:
            out.extend(_flatten_metadata(item))
        return out
    return [str(value)]


def _metadata_text(
    artifact: VerifiedArtifact,
    source: Optional[SourceRecord],
    *,
    include_query: bool,
) -> str:
    metadata = artifact.metadata or {}
    query_keys = {
        "query",
        "query_used",
        "search_query",
        "planned_query",
        "discovered_query",
    }
    parts: List[str] = [
        artifact.artifact_id,
        artifact.artifact_type,
        artifact.artifact_url,
        artifact.source_url or "",
        artifact.source_authority,
        artifact.format,
        artifact.verification_method,
        " ".join(artifact.matched_case_fields),
        " ".join(artifact.risk_flags),
    ]
    for key, value in metadata.items():
        if not include_query and str(key).lower() in query_keys:
            continue
        parts.extend(_flatten_metadata(key))
        parts.extend(_flatten_metadata(value))
    if source is not None:
        parts.extend(
            [
                source.url,
                source.title,
                source.snippet,
                source.source_type,
                source.source_authority,
                " ".join(source.source_roles),
                source.api_name or "",
                source.discovered_via,
                " ".join(source.matched_case_fields),
            ]
        )
        for key, value in source.metadata.items():
            if not include_query and str(key).lower() in query_keys:
                continue
            parts.extend(_flatten_metadata(key))
            parts.extend(_flatten_metadata(value))
    return " ".join(part for part in parts if part).lower()


def _query_text(artifact: VerifiedArtifact, source: Optional[SourceRecord]) -> str:
    metadata = artifact.metadata or {}
    keys = ("query", "query_used", "search_query", "planned_query", "discovered_query")
    parts: List[str] = []
    for key in keys:
        parts.extend(_flatten_metadata(metadata.get(key)))
    if source is not None:
        for key in keys:
            parts.extend(_flatten_metadata(source.metadata.get(key)))
        parts.append(source.discovered_via)
    return " ".join(part for part in parts if part).lower()


def _match_terms(text: str, terms: Sequence[tuple[str, str]]) -> List[str]:
    matched: List[str] = []
    for needle, label in terms:
        if needle in text:
            _append_unique(matched, [label])
    return matched


def _host(url: str) -> str:
    return urlparse(url or "").netloc.lower()


def _is_youtube_url(url: str) -> bool:
    host = _host(url)
    return host in {"youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be"}


def _official_source_likelihood(artifact: VerifiedArtifact, text: str) -> float:
    score = 0.0
    if artifact.source_authority in {"official", "court", "foia"}:
        score += 0.45
    host = _host(artifact.artifact_url) or _host(artifact.source_url or "")
    if host.endswith(".gov") or ".gov" in host:
        score += 0.35
    if any(term in text for term in OFFICIAL_TERMS):
        score += 0.25
    if "official" in artifact.risk_flags:
        score += 0.1
    return round(min(score, 1.0), 2)


def _base_result(
    artifact: VerifiedArtifact,
    *,
    tier: str,
    score: float,
    primary: float,
    official: float,
    reason_codes: List[str],
    risk_flags: List[str],
    manual: bool,
    matched_terms: List[str],
    warnings: List[str],
) -> MediaRelevanceResult:
    return MediaRelevanceResult(
        artifact_id=artifact.artifact_id,
        artifact_url=artifact.artifact_url,
        media_relevance_tier=tier,
        media_relevance_score=round(score, 2),
        primary_source_likelihood=round(primary, 2),
        official_source_likelihood=round(official, 2),
        reason_codes=reason_codes,
        risk_flags=risk_flags,
        needs_manual_review=manual,
        matched_terms=matched_terms,
        mismatch_warnings=warnings,
    )


def classify_media_relevance(
    artifact: VerifiedArtifact,
    *,
    source: Optional[SourceRecord] = None,
) -> MediaRelevanceResult:
    """Grade a media artifact's production relevance.

    This function is pure and metadata-only. A verified URL remains a
    VerifiedArtifact; this classifier only says whether that artifact
    looks like primary production media, strong secondary media, or
    weak/uncertain media.
    """

    text = _metadata_text(artifact, source, include_query=False)
    all_text = _metadata_text(artifact, source, include_query=True)
    query = _query_text(artifact, source)
    official = _official_source_likelihood(artifact, text)
    primary_terms = _match_terms(text, PRIMARY_TERMS)
    secondary_terms = _match_terms(text, SECONDARY_TERMS)
    weak_terms = _match_terms(text, WEAK_TERMS)
    query_primary_terms = _match_terms(query, PRIMARY_TERMS)

    reason_codes: List[str] = []
    risk_flags: List[str] = list(artifact.risk_flags)
    warnings: List[str] = []
    matched_terms: List[str] = []

    url_policy = classify_media_url(artifact.artifact_url, hint=artifact.artifact_type)
    _append_unique(risk_flags, url_policy.risk_flags)
    if PROTECTED_RISKS & set(risk_flags):
        _append_unique(reason_codes, ["protected_or_nonpublic_media"])
        _append_unique(risk_flags, ["media_not_publicly_accessible"])
        _append_unique(warnings, ["protected_or_nonpublic_url_not_production_ready"])
        return _base_result(
            artifact,
            tier="C",
            score=0.05,
            primary=0.0,
            official=official,
            reason_codes=reason_codes,
            risk_flags=risk_flags,
            manual=True,
            matched_terms=[],
            warnings=warnings,
        )

    if any(term in all_text for term in ("name collision", "unrelated", "different person", "wrong case")):
        _append_unique(risk_flags, ["possible_name_collision"])
        _append_unique(warnings, ["possible_name_collision"])

    if weak_terms:
        _append_unique(reason_codes, ["weak_or_derivative_media_terms"])
        _append_unique(matched_terms, weak_terms)

    if primary_terms:
        _append_unique(reason_codes, ["primary_production_media_terms"])
        _append_unique(matched_terms, primary_terms)
        score = 0.88
        primary = 0.88
        if official >= 0.7:
            score += 0.08
            _append_unique(reason_codes, ["official_source_likely"])
        if "raw_footage" in primary_terms and official >= 0.55:
            _append_unique(reason_codes, ["official_raw_footage_likely"])
        return _base_result(
            artifact,
            tier="A",
            score=min(score, 1.0),
            primary=primary,
            official=official,
            reason_codes=reason_codes,
            risk_flags=risk_flags,
            manual=bool(risk_flags),
            matched_terms=matched_terms,
            warnings=warnings,
        )

    if official >= 0.7 and any(term in text for term in ("critical incident", "raw footage", "officer involved")):
        _append_unique(reason_codes, ["official_critical_incident_media"])
        _append_unique(matched_terms, ["official_critical_incident"])
        return _base_result(
            artifact,
            tier="A",
            score=0.9,
            primary=0.82,
            official=official,
            reason_codes=reason_codes,
            risk_flags=risk_flags,
            manual=bool(risk_flags),
            matched_terms=matched_terms,
            warnings=warnings,
        )

    if secondary_terms:
        _append_unique(reason_codes, ["strong_secondary_media_terms"])
        _append_unique(matched_terms, secondary_terms)
        score = 0.68 + (0.08 if official >= 0.5 else 0.0)
        return _base_result(
            artifact,
            tier="B",
            score=min(score, 0.82),
            primary=0.45,
            official=official,
            reason_codes=reason_codes,
            risk_flags=risk_flags,
            manual=bool(risk_flags),
            matched_terms=matched_terms,
            warnings=warnings,
        )

    if query_primary_terms:
        _append_unique(warnings, ["media_query_artifact_type_mismatch"])
        _append_unique(risk_flags, ["media_query_artifact_type_mismatch"])
        _append_unique(reason_codes, ["query_artifact_type_not_confirmed_by_metadata"])

    if weak_terms:
        score = 0.22
    elif artifact.format in {"video", "audio"} or artifact.artifact_type in {"video_footage", "other_video", "audio"}:
        score = 0.3
        _append_unique(reason_codes, ["generic_media_without_specific_type"])
        if _is_youtube_url(artifact.artifact_url):
            _append_unique(risk_flags, ["generic_youtube_media"])
    else:
        score = 0.12
        _append_unique(reason_codes, ["media_relevance_unknown"])

    _append_unique(risk_flags, ["weak_or_uncertain_media"])
    _append_unique(warnings, ["manual_review_media_relevance"])
    return _base_result(
        artifact,
        tier="C" if artifact.format in {"video", "audio"} else "unknown",
        score=score,
        primary=0.1,
        official=official,
        reason_codes=reason_codes,
        risk_flags=risk_flags,
        manual=True,
        matched_terms=matched_terms,
        warnings=warnings,
    )


def classify_media_relevance_many(
    artifacts: Iterable[VerifiedArtifact],
    *,
    sources_by_id: Optional[Mapping[str, SourceRecord]] = None,
) -> List[MediaRelevanceResult]:
    """Classify a collection of artifacts in input order."""

    source_map = sources_by_id or {}
    results = []
    for artifact in artifacts:
        source = None
        source_id = str((artifact.metadata or {}).get("source_id") or "")
        if source_id:
            source = source_map.get(source_id)
        results.append(classify_media_relevance(artifact, source=source))
    return results
