"""LIVE8 — YouTube media resolver.

Pure metadata-only resolver. Turns concrete public YouTube / Vimeo
URLs surfaced by the YouTube connector into media
:class:`VerifiedArtifact` records via the central MEDIA1
classification policy.

Hard rules (mirror existing resolvers):

- never download files
- never scrape pages
- never fetch transcripts
- never follow login/auth/private/protected URLs - they are refused
  via the MEDIA1 policy and surface as ``protected_or_nonpublic``
  risk flags
- claim text without a concrete URL never produces a
  VerifiedArtifact
- the resolver only inspects sources from the YouTube connector
  (``api_name == "youtube"``) OR sources whose URL is on a known
  video host (youtube.com / youtu.be / vimeo.com); other connectors
  are ignored
- the resolver creates VerifiedArtifacts only - it does not score or
  set the verdict; PRODUCE / HOLD / SKIP gates still apply via
  :func:`score_case_packet`
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, List, Sequence
from urllib.parse import urlparse

from ..media_policy import classify_media_url
from ..models import CasePacket, SourceRecord, VerifiedArtifact


VIDEO_HOSTS = frozenset(
    {
        "youtube.com",
        "www.youtube.com",
        "m.youtube.com",
        "youtu.be",
        "vimeo.com",
        "www.vimeo.com",
        "player.vimeo.com",
    }
)


@dataclass
class YouTubeFileResolution:
    verified_artifacts: List[VerifiedArtifact] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)
    inspected_source_ids: List[str] = field(default_factory=list)


def _is_youtube_source(source: SourceRecord) -> bool:
    api_name = (source.api_name or "").lower()
    if api_name == "youtube" or api_name.startswith("youtube_"):
        return True
    url = (source.url or "").lower()
    if not url:
        return False
    host = urlparse(url).netloc.lower()
    return host in VIDEO_HOSTS


def _append_unique(items: List[str], values: Iterable[str]) -> None:
    seen = set(items)
    for value in values:
        if value and value not in seen:
            items.append(value)
            seen.add(value)


def _sources_from(
    packet_or_sources: CasePacket | Sequence[SourceRecord],
) -> List[SourceRecord]:
    if isinstance(packet_or_sources, CasePacket):
        return list(packet_or_sources.sources)
    return list(packet_or_sources)


def _hint_for(source: SourceRecord) -> str:
    metadata = source.metadata or {}
    title = source.title or ""
    snippet = source.snippet or ""
    description = str(metadata.get("description") or metadata.get("media_link_type") or "")
    # Prefer explicit metadata hints; fall back to title + snippet
    # text. The MEDIA1 hint mapping handles bodycam / dashcam /
    # interrogation / court_video / dispatch_911 etc.
    return " | ".join(part for part in (description, title, snippet) if part)


def _artifact_id(source: SourceRecord, index: int, url: str) -> str:
    seed = f"{source.source_id}_{index}_{urlparse(url).path}".lower()
    safe = re.sub(r"[^a-z0-9]+", "_", seed).strip("_")
    return f"youtube_{safe[:80] or index}"


def resolve_youtube_files(
    packet_or_sources: CasePacket | Sequence[SourceRecord],
) -> YouTubeFileResolution:
    """Resolve YouTube SourceRecords into media VerifiedArtifacts.

    Pure: never downloads, never scrapes, never fetches transcripts,
    never contacts the network. Uses the central MEDIA1 classifier
    so every resolver agrees on what counts as media. Only graduates
    sources carrying the ``possible_artifact_source`` role; claim-
    only sources never become artifacts.
    """
    sources = [s for s in _sources_from(packet_or_sources) if _is_youtube_source(s)]
    result = YouTubeFileResolution(
        inspected_source_ids=[s.source_id for s in sources]
    )

    existing_urls: set = set()
    if isinstance(packet_or_sources, CasePacket):
        existing_urls = {a.artifact_url for a in packet_or_sources.verified_artifacts}

    for source in sources:
        # Only sources flagged as possible_artifact_source by the
        # connector can graduate. This preserves the
        # claim_source != artifact_source rule even when the YouTube
        # connector emits sources with multiple roles.
        roles = list(source.source_roles or [])
        if "possible_artifact_source" not in roles and "artifact_source" not in roles:
            # YouTube connector typically tags video results with
            # 'artifact_source' for direct video URLs. If that
            # convention changes, claim-only YouTube refs still cannot
            # graduate.
            continue

        url = (source.url or "").strip()
        if not url:
            continue
        if url in existing_urls:
            continue

        cls = classify_media_url(url, hint=_hint_for(source))
        if cls.rejected:
            if "protected_or_nonpublic" in cls.risk_flags:
                _append_unique(result.risk_flags, ["protected_or_nonpublic"])
                _append_unique(
                    result.next_actions,
                    [
                        "Skip protected YouTube/Vimeo source; obtain public mirror "
                        "before graduating."
                    ],
                )
            elif "thumbnail_or_preview" in cls.risk_flags:
                _append_unique(result.risk_flags, ["thumbnail_or_preview_skipped"])
            continue

        if not cls.is_media:
            # YouTube/Vimeo non-media (e.g. channel page) - not a
            # candidate for graduation in this resolver.
            continue

        artifact_type = cls.artifact_type or "video_footage"
        format_ = cls.format or "video"

        artifact = VerifiedArtifact(
            artifact_id=_artifact_id(source, len(result.verified_artifacts) + 1, url),
            artifact_type=artifact_type,
            artifact_url=url,
            source_url=source.url,
            source_authority=source.source_authority or "third_party",
            downloadable=False,  # YouTube/Vimeo do not surface direct mp4 links
            format=format_,
            matched_case_fields=list(source.matched_case_fields),
            confidence=0.78 if source.matched_case_fields else 0.7,
            claim_source_url=source.url,
            verification_method=cls.verification_method or "youtube_metadata",
            risk_flags=[],
            metadata={
                "source_id": source.source_id,
                "host_page_url": source.url,
                "channel": (source.metadata or {}).get("channel"),
                "duration_seconds": (source.metadata or {}).get("duration_seconds"),
                "media_or_document": "media",
                "hint_used": cls.hint_used,
            },
        )
        result.verified_artifacts.append(artifact)
        existing_urls.add(url)

    if isinstance(packet_or_sources, CasePacket):
        packet_or_sources.verified_artifacts.extend(result.verified_artifacts)
        _append_unique(packet_or_sources.risk_flags, result.risk_flags)
        _append_unique(packet_or_sources.next_actions, result.next_actions)

    return result
