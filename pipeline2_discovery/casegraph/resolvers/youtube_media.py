"""LIVE9 - YouTube public media resolver.

Metadata-only resolver for public YouTube watch URLs returned by the
YouTube connector. It never downloads media, fetches transcripts,
scrapes pages, or calls the network. It only graduates connector
SourceRecords that already carry a concrete public YouTube URL and a
``possible_artifact_source`` role.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, List, Sequence
from urllib.parse import parse_qs, urlparse

from ..models import CasePacket, SourceRecord, VerifiedArtifact


PROTECTED_MARKERS = (
    "login",
    "signin",
    "sign-in",
    "private",
    "auth=",
    "token=",
    "session=",
)

ARTIFACT_TERMS = (
    ("bodycam", "bodycam"),
    ("body cam", "bodycam"),
    ("body-worn camera", "bodycam"),
    ("body worn camera", "bodycam"),
    ("bwc", "bodycam"),
    ("dashcam", "dash_cam"),
    ("dash cam", "dash_cam"),
    ("interrogation", "interrogation"),
    ("confession", "interrogation"),
    ("police interview", "interrogation"),
    ("detective interview", "interrogation"),
    ("911", "dispatch_911"),
    ("dispatch", "dispatch_911"),
    ("surveillance", "surveillance_video"),
    ("cctv", "surveillance_video"),
    ("critical incident", "bodycam"),
    ("officer involved shooting", "bodycam"),
    ("officer-involved shooting", "bodycam"),
    ("ois", "bodycam"),
)


@dataclass
class YouTubeMediaResolution:
    verified_artifacts: List[VerifiedArtifact] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)
    inspected_source_ids: List[str] = field(default_factory=list)

    @property
    def verified_artifact_count(self) -> int:
        return len(self.verified_artifacts)

    @property
    def media_artifact_count(self) -> int:
        return len(self.verified_artifacts)

    @property
    def document_artifact_count(self) -> int:
        return 0


def _append_unique(items: List[str], values: Iterable[str]) -> None:
    seen = set(items)
    for value in values:
        if value and value not in seen:
            items.append(value)
            seen.add(value)


def _host(url: str) -> str:
    return urlparse(url or "").netloc.lower()


def _is_youtube_url(url: str) -> bool:
    return _host(url) in {"youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be"}


def _looks_protected(url: str) -> bool:
    lower = (url or "").lower()
    return any(marker in lower for marker in PROTECTED_MARKERS)


def _video_id(url: str, source: SourceRecord) -> str:
    metadata_id = str((source.metadata or {}).get("video_id") or "").strip()
    if metadata_id:
        return metadata_id
    parsed = urlparse(url)
    if parsed.netloc.lower() == "youtu.be":
        return parsed.path.strip("/")
    values = parse_qs(parsed.query).get("v") or []
    return values[0] if values else parsed.path.strip("/")


def _is_youtube_source(source: SourceRecord) -> bool:
    return source.api_name == "youtube_yt_dlp" or _is_youtube_url(source.url)


def _is_artifact_candidate(source: SourceRecord) -> bool:
    return "possible_artifact_source" in (source.source_roles or [])


def _source_text(source: SourceRecord, *, include_query: bool = True) -> str:
    metadata = source.metadata or {}
    parts = [
        source.title,
        source.snippet,
        source.raw_text,
        str(metadata.get("channel") or ""),
        str(metadata.get("uploader") or ""),
    ]
    if include_query:
        parts.append(source.discovered_via)
    return " ".join(part for part in parts if part).lower()


def _artifact_type(source: SourceRecord) -> tuple[str, List[str]]:
    # The search query is deliberately excluded here. A query for
    # "bodycam" must not make a generic returned video into bodycam.
    text = _source_text(source, include_query=False)
    matched: List[str] = []
    for needle, artifact_type in ARTIFACT_TERMS:
        if needle in text:
            _append_unique(matched, [needle])
            return artifact_type, matched
    return "other_video", matched


def _matched_case_fields(packet: CasePacket | None, source: SourceRecord) -> List[str]:
    fields = list(source.matched_case_fields or [])
    if packet is None:
        return fields
    text = _source_text(source, include_query=False)
    for name in packet.case_identity.defendant_names:
        lowered = name.lower()
        parts = [part for part in re.split(r"\s+", lowered) if part]
        first_last = len(parts) >= 2 and parts[0] in text and parts[-1] in text
        if lowered in text or first_last:
            _append_unique(fields, ["defendant_full_name"])
            break
    return fields


def _artifact_id(source: SourceRecord, index: int, video_id: str) -> str:
    seed = f"{source.source_id}_{video_id}_{index}".lower()
    safe = re.sub(r"[^a-z0-9]+", "_", seed).strip("_")
    return f"youtube_{safe[:80] or index}"


def _sources_from(packet_or_sources: CasePacket | Sequence[SourceRecord]) -> List[SourceRecord]:
    if isinstance(packet_or_sources, CasePacket):
        return list(packet_or_sources.sources)
    return list(packet_or_sources)


def resolve_youtube_media_sources(
    packet_or_sources: CasePacket | Sequence[SourceRecord],
) -> YouTubeMediaResolution:
    """Resolve public YouTube SourceRecords into media artifacts.

    The resolver is intentionally conservative about provenance: a
    YouTube URL can become a VerifiedArtifact only when a connector
    already returned it as a concrete public URL and marked it as a
    possible artifact source. Generic videos are allowed to graduate as
    ``other_video`` so media-quality scoring can flag them for manual
    review, but they are not classified as Tier A by this resolver.
    """

    packet = packet_or_sources if isinstance(packet_or_sources, CasePacket) else None
    sources = [
        source
        for source in _sources_from(packet_or_sources)
        if _is_youtube_source(source)
    ]
    result = YouTubeMediaResolution(
        inspected_source_ids=[source.source_id for source in sources]
    )

    existing_urls = set()
    if packet is not None:
        existing_urls = {artifact.artifact_url for artifact in packet.verified_artifacts}

    for source in sources:
        if not _is_artifact_candidate(source):
            continue
        url = str(source.url or "").strip()
        if not _is_youtube_url(url):
            continue
        if _looks_protected(url):
            _append_unique(result.risk_flags, ["protected_or_nonpublic"])
            _append_unique(result.next_actions, ["Skip protected/private YouTube URL."])
            continue
        if url in existing_urls:
            continue

        artifact_type, matched_terms = _artifact_type(source)
        risk_flags: List[str] = []
        if artifact_type == "other_video":
            risk_flags.extend(["generic_youtube_media", "media_relevance_unconfirmed"])

        video_id = _video_id(url, source)
        artifact = VerifiedArtifact(
            artifact_id=_artifact_id(source, len(result.verified_artifacts) + 1, video_id),
            artifact_type=artifact_type,
            artifact_url=url,
            source_authority=source.source_authority,
            downloadable=False,
            format="video",
            source_url=url,
            matched_case_fields=_matched_case_fields(packet, source),
            confidence=0.78 if artifact_type != "other_video" else 0.55,
            claim_source_url=url if "claim_source" in source.source_roles else None,
            duration_sec=source.metadata.get("duration") if source.metadata else None,
            requires_manual_download=False,
            verification_method="youtube_metadata_public_watch_url",
            risk_flags=risk_flags,
            metadata={
                "source_id": source.source_id,
                "video_id": video_id,
                "title": source.title,
                "channel": (source.metadata or {}).get("channel"),
                "query_used": source.discovered_via,
                "matched_youtube_terms": matched_terms,
            },
        )
        result.verified_artifacts.append(artifact)
        existing_urls.add(url)

    if packet is not None:
        packet.verified_artifacts.extend(result.verified_artifacts)
        _append_unique(packet.risk_flags, result.risk_flags)
        _append_unique(packet.next_actions, result.next_actions)

    return result
