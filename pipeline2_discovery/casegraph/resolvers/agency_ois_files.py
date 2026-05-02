"""SOURCE2 — Agency OIS media/document resolver (mocked first).

Pure metadata-only resolver. Turns concrete public agency-OIS
SourceRecord URLs into media or document
:class:`VerifiedArtifact` records.

Hard rules (mirror the existing MuckRock / DocumentCloud / CourtListener
resolvers):

- never download files
- never OCR
- never scrape pages
- never follow login/auth/private/protected URLs - they are refused
  and surface as ``protected_or_nonpublic`` risk flags
- claim text without a concrete URL never produces a VerifiedArtifact
  (this is the non-negotiable ``claim_source != artifact_source``
  rule)
- the resolver only inspects sources from the agency-OIS connector
  (``api_name == "agency_ois"``); other connectors are ignored
- the resolver creates VerifiedArtifacts only - it does not score or
  set the verdict. PRODUCE / HOLD / SKIP gates still apply via
  :func:`score_case_packet`.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Sequence
from urllib.parse import urlparse

from ..models import CasePacket, SourceRecord, VerifiedArtifact


VIDEO_EXTENSIONS = {".mp4", ".mov", ".webm", ".m3u8"}
AUDIO_EXTENSIONS = {".mp3", ".wav", ".m4a"}
DOCUMENT_EXTENSIONS = {".pdf", ".doc", ".docx", ".rtf"}

VIDEO_HOSTS = {"youtube.com", "www.youtube.com", "youtu.be", "vimeo.com", "www.vimeo.com"}

PROTECTED_MARKERS = (
    "login",
    "signin",
    "sign-in",
    "/private/",
    "/restricted/",
    "auth=",
    "token=",
    "session=",
    "redirect=",
    "permission",
    "placeholder",
)


# Map agency-supplied link_type hints to canonical CaseGraph
# artifact_type values. Anything not in this map is inferred from the
# URL extension or host.
LINK_TYPE_TO_ARTIFACT_TYPE = {
    "bodycam_briefing": "bodycam",
    "bodycam": "bodycam",
    "bwc": "bodycam",
    "dashcam": "dashcam",
    "surveillance": "surveillance",
    "interrogation": "interrogation",
    "police_interview": "interrogation",
    "court_video": "court_video",
    "sentencing_video": "court_video",
    "trial_video": "court_video",
    "dispatch_911": "dispatch_911",
    "911_audio": "dispatch_911",
    "incident_report": "docket_docs",
    "incident_summary": "docket_docs",
    "ia_report": "docket_docs",
    "use_of_force_report": "docket_docs",
    "police_report": "docket_docs",
    "agency_document": "docket_docs",
}


@dataclass
class AgencyOISFileResolution:
    verified_artifacts: List[VerifiedArtifact] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)
    inspected_source_ids: List[str] = field(default_factory=list)


def _clean_url(url: Any) -> str:
    return str(url or "").strip().strip("()[]{}<>'\"")


def _looks_protected(url: str) -> bool:
    if not url:
        return True
    lower = url.lower()
    return any(marker in lower for marker in PROTECTED_MARKERS)


def _extension(url: str) -> str:
    path = urlparse(url).path.lower()
    for ext in VIDEO_EXTENSIONS | AUDIO_EXTENSIONS | DOCUMENT_EXTENSIONS:
        if path.endswith(ext):
            return ext
    return ""


def _host(url: str) -> str:
    return urlparse(url).netloc.lower()


def _is_video_host(url: str) -> bool:
    return _host(url) in VIDEO_HOSTS


def _is_acceptable_public_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    if _looks_protected(url):
        return False
    if _extension(url):
        return True
    if _is_video_host(url):
        return True
    return False


def _format_for_url(url: str) -> str:
    ext = _extension(url)
    if ext in VIDEO_EXTENSIONS:
        return "video"
    if ext in AUDIO_EXTENSIONS:
        return "audio"
    if ext == ".pdf":
        return "pdf"
    if ext in DOCUMENT_EXTENSIONS:
        return "document"
    if _is_video_host(url):
        return "video"
    return "html"


def _is_media_format(fmt: str) -> bool:
    return fmt in {"video", "audio"}


def _infer_artifact_type(*, url: str, link_type_hint: str, format_: str) -> str:
    """Choose canonical artifact_type. Prefer the agency's own
    link_type hint when it maps to a known type; otherwise derive
    deterministically from URL extension / host / format."""
    hint = (link_type_hint or "").strip().lower()
    if hint in LINK_TYPE_TO_ARTIFACT_TYPE:
        return LINK_TYPE_TO_ARTIFACT_TYPE[hint]
    if format_ == "audio":
        return "dispatch_911"
    if format_ == "video":
        # Use the schema-canonical generic-video type so graduated
        # artifacts validate against the CasePacket schema enum (which
        # includes "other_video" but not "video_footage").
        return "other_video"
    if format_ in {"pdf", "document"}:
        return "docket_docs"
    return "docket_docs"


def _is_agency_ois_source(source: SourceRecord) -> bool:
    if source.api_name == "agency_ois":
        return True
    if source.source_authority == "official" and source.source_type and (
        source.source_type.startswith("agency_media")
        or source.source_type.startswith("agency_document")
        or source.source_type.startswith("agency_claim")
        or source.source_type == "incident_detail"
        or source.source_type == "agency_listing"
    ):
        return True
    return False


def _is_artifact_candidate(source: SourceRecord) -> bool:
    """Only sources that the connector flagged as
    ``possible_artifact_source`` are candidates for graduation. This is
    the deterministic rope that keeps claim-only sources from ever
    becoming artifacts."""
    return "possible_artifact_source" in (source.source_roles or [])


def _link_type_hint(source: SourceRecord) -> str:
    md = source.metadata or {}
    return str(md.get("media_link_type") or md.get("document_link_type") or "")


def _confidence_for(source: SourceRecord, url: str, *, format_: str) -> float:
    confidence = 0.72
    if _extension(url):
        confidence += 0.08
    if _is_video_host(url):
        confidence += 0.05
    if format_ == "video":
        confidence += 0.03
    if source.matched_case_fields:
        confidence += 0.05
    return round(min(confidence, 0.93), 2)


def _verification_method(url: str) -> str:
    ext = _extension(url)
    if ext in VIDEO_EXTENSIONS:
        return "agency_ois_video_url"
    if ext in AUDIO_EXTENSIONS:
        return "agency_ois_audio_url"
    if ext in DOCUMENT_EXTENSIONS:
        return "agency_ois_document_url"
    if _is_video_host(url):
        return "agency_ois_video_host"
    return "agency_ois_metadata"


def _artifact_id(source: SourceRecord, index: int, url: str) -> str:
    seed = f"{source.source_id}_{index}_{urlparse(url).path}".lower()
    safe = re.sub(r"[^a-z0-9]+", "_", seed).strip("_")
    return f"agency_ois_{safe[:80] or index}"


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


def resolve_agency_ois_files(
    packet_or_sources: CasePacket | Sequence[SourceRecord],
) -> AgencyOISFileResolution:
    """Resolve agency-OIS SourceRecords into media or document
    VerifiedArtifacts.

    Pure: never downloads, never scrapes, never contacts the network.
    Claim-only sources (role ``claim_source`` without
    ``possible_artifact_source``) are skipped entirely - they cannot
    become artifacts. Protected/login/private URLs surface a
    ``protected_or_nonpublic`` risk flag and are NOT graduated.
    """

    sources = [
        source
        for source in _sources_from(packet_or_sources)
        if _is_agency_ois_source(source)
    ]
    result = AgencyOISFileResolution(
        inspected_source_ids=[source.source_id for source in sources]
    )

    existing_urls: set = set()
    if isinstance(packet_or_sources, CasePacket):
        existing_urls = {a.artifact_url for a in packet_or_sources.verified_artifacts}

    for source in sources:
        if not _is_artifact_candidate(source):
            # Pages / claims / listings - not artifact candidates.
            continue

        url = _clean_url(source.url)
        if not url:
            continue

        if _looks_protected(url):
            _append_unique(result.risk_flags, ["protected_or_nonpublic"])
            _append_unique(
                result.next_actions,
                [
                    "Skip protected agency portal link; obtain public mirror or "
                    "agency-released copy before graduating."
                ],
            )
            continue

        if not _is_acceptable_public_url(url):
            _append_unique(
                result.next_actions,
                [
                    "Inspect agency page for a concrete public media or document URL "
                    "before graduating."
                ],
            )
            continue

        if url in existing_urls:
            continue

        format_ = _format_for_url(url)
        link_hint = _link_type_hint(source)
        artifact_type = _infer_artifact_type(
            url=url, link_type_hint=link_hint, format_=format_
        )

        downloadable = bool(_extension(url) in (VIDEO_EXTENSIONS | AUDIO_EXTENSIONS | DOCUMENT_EXTENSIONS))

        artifact = VerifiedArtifact(
            artifact_id=_artifact_id(source, len(result.verified_artifacts) + 1, url),
            artifact_type=artifact_type,
            artifact_url=url,
            source_url=(source.metadata or {}).get("host_page_url") or source.url,
            source_authority="official",
            downloadable=downloadable,
            format=format_,
            matched_case_fields=list(source.matched_case_fields),
            confidence=_confidence_for(source, url, format_=format_),
            claim_source_url=source.url,
            verification_method=_verification_method(url),
            risk_flags=[],
            metadata={
                "source_id": source.source_id,
                "agency": (source.metadata or {}).get("agency"),
                "case_number": (source.metadata or {}).get("case_number"),
                "host_page_url": (source.metadata or {}).get("host_page_url"),
                "link_type_hint": link_hint or None,
                "media_or_document": "media" if _is_media_format(format_) else "document",
            },
        )
        result.verified_artifacts.append(artifact)
        existing_urls.add(url)

    if isinstance(packet_or_sources, CasePacket):
        packet_or_sources.verified_artifacts.extend(result.verified_artifacts)
        _append_unique(packet_or_sources.risk_flags, result.risk_flags)
        _append_unique(packet_or_sources.next_actions, result.next_actions)

    return result
