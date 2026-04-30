from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Sequence
from urllib.parse import urlparse

from ..models import CasePacket, SourceRecord, VerifiedArtifact


FILE_FORMATS = {
    ".pdf": "pdf",
    ".mp4": "video",
    ".mov": "video",
    ".webm": "video",
    ".mp3": "audio",
    ".wav": "audio",
    ".m4a": "audio",
    ".doc": "document",
    ".docx": "document",
}

PROTECTED_MARKERS = [
    "login",
    "signin",
    "sign-in",
    "private",
    "protected",
    "requires-login",
    "permission",
    "auth=",
    "token=",
    "session",
    "placeholder",
]

FILE_CONTAINER_KEYS = {
    "file",
    "files",
    "file_url",
    "file_urls",
    "attachment",
    "attachments",
    "document",
    "documents",
    "documentcloud",
    "links",
    "responsive_files",
    "released_files",
}


@dataclass
class MuckRockFileResolution:
    verified_artifacts: List[VerifiedArtifact] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)
    inspected_source_ids: List[str] = field(default_factory=list)


def _text(*parts: Any) -> str:
    return " ".join(str(part) for part in parts if part).strip()


def _clean_url(url: str) -> str:
    return str(url or "").strip().strip("()[]{}<>'\"")


def _extension(url: str) -> str:
    path = urlparse(url).path.lower()
    for ext in FILE_FORMATS:
        if path.endswith(ext):
            return ext
    return ""


def _looks_protected(url: str) -> bool:
    lower = url.lower()
    return any(marker in lower for marker in PROTECTED_MARKERS)


def _is_public_artifact_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    if _looks_protected(url):
        return False
    if _extension(url):
        return True
    lower_path = parsed.path.lower()
    host = parsed.netloc.lower()
    if "documentcloud.org" in host:
        return True
    if "muckrock.com" in host and any(part in lower_path for part in ["/files/", "/file/", "/documents/"]):
        return True
    return False


def _format_for_url(url: str) -> str:
    ext = _extension(url)
    if ext:
        return FILE_FORMATS[ext]
    host = urlparse(url).netloc.lower()
    if "documentcloud.org" in host:
        return "html"
    return "html" if urlparse(url).path else "unknown"


def _downloadable_for_url(url: str) -> bool:
    return bool(_extension(url))


def _infer_artifact_type(url: str, context: str) -> str:
    lower = _text(url, context).lower()
    if any(term in lower for term in ["bodycam", "body_cam", "body-cam", "body cam", "body camera", "body-worn", "bwc"]):
        return "bodycam"
    if any(term in lower for term in ["911", "dispatch", "emergency call", "emergency_call"]):
        return "dispatch_911"
    if re.search(r"\b(interrogation|confession|police interview|custodial interview)\b", lower):
        return "interrogation"
    if re.search(r"\b(court video|trial video|sentencing video|hearing video)\b", lower):
        return "court_video"
    if re.search(r"\b(complaint|affidavit|report|probable cause|docket|indictment|police report|incident report)\b", lower):
        return "docket_docs"

    artifact_format = _format_for_url(url)
    if artifact_format == "audio":
        return "audio"
    if artifact_format == "video":
        return "other_video"
    if artifact_format in {"pdf", "document", "html"}:
        return "document"
    return "unknown"


def _source_context(source: SourceRecord, link_data: Dict[str, Any] | None = None) -> str:
    link_data = link_data or {}
    metadata_text = " ".join(
        str(value)
        for key, value in link_data.items()
        if key in {"title", "name", "filename", "description", "label"} and value
    )
    return _text(source.title, source.snippet, source.raw_text, metadata_text)


def _collect_metadata_links(value: Any, *, parent_key: str = "") -> List[Dict[str, Any]]:
    links: List[Dict[str, Any]] = []
    parent_is_fileish = parent_key.lower() in FILE_CONTAINER_KEYS
    if isinstance(value, dict):
        for key, child in value.items():
            key_lower = str(key).lower()
            child_fileish = parent_is_fileish or key_lower in FILE_CONTAINER_KEYS
            if isinstance(child, str) and child.startswith(("http://", "https://")) and (child_fileish or key_lower in {"url", "absolute_url"}):
                links.append({"url": _clean_url(child), **{str(k): v for k, v in value.items() if isinstance(v, (str, int, float, bool))}})
            else:
                links.extend(_collect_metadata_links(child, parent_key=key_lower if child_fileish else key_lower))
    elif isinstance(value, list):
        for item in value:
            links.extend(_collect_metadata_links(item, parent_key=parent_key))
    elif isinstance(value, str) and parent_is_fileish:
        links.extend({"url": _clean_url(match)} for match in re.findall(r"https?://[^\s)>\]\"']+", value))
    return links


def _collect_source_links(source: SourceRecord) -> List[Dict[str, Any]]:
    links = _collect_metadata_links(source.metadata or {})
    for text in [source.raw_text, source.snippet]:
        for url in re.findall(r"https?://[^\s)>\]\"']+", text or ""):
            links.append({"url": _clean_url(url), "source": "text"})
    if _is_public_artifact_url(source.url):
        links.append({"url": source.url, "source": "source_url"})

    deduped: Dict[str, Dict[str, Any]] = {}
    for link in links:
        url = _clean_url(str(link.get("url") or ""))
        if url and url not in deduped:
            deduped[url] = {**link, "url": url}
    return list(deduped.values())


def _artifact_id(source: SourceRecord, index: int, url: str) -> str:
    seed = f"{source.source_id}_{index}_{urlparse(url).path}".lower()
    safe = re.sub(r"[^a-z0-9]+", "_", seed).strip("_")
    return f"muckrock_file_{safe[:80] or index}"


def _verification_method(url: str, link_data: Dict[str, Any]) -> str:
    if _extension(url):
        return "file_extension"
    if link_data.get("source") == "text":
        return "public_url_pattern"
    return "muckrock_metadata"


def _confidence_for(source: SourceRecord, artifact_type: str, url: str) -> float:
    confidence = 0.68
    if _extension(url):
        confidence += 0.08
    if artifact_type not in {"unknown", "document", "other_video", "audio"}:
        confidence += 0.08
    if source.source_authority == "foia":
        confidence += 0.08
    if source.matched_case_fields:
        confidence += 0.08
    return round(min(confidence, 0.92), 2)


def _append_unique(items: List[str], values: Iterable[str]) -> None:
    seen = set(items)
    for value in values:
        if value and value not in seen:
            items.append(value)
            seen.add(value)


def _sources_from(packet_or_sources: CasePacket | Sequence[SourceRecord]) -> List[SourceRecord]:
    if isinstance(packet_or_sources, CasePacket):
        return list(packet_or_sources.sources)
    return list(packet_or_sources)


def resolve_muckrock_released_files(packet_or_sources: CasePacket | Sequence[SourceRecord]) -> MuckRockFileResolution:
    """Resolve public MuckRock file/document links into verified artifacts.

    This resolver is deterministic and metadata-only. It never downloads files,
    follows pages, or treats request/claim text as artifact verification.
    """

    sources = [
        source
        for source in _sources_from(packet_or_sources)
        if source.api_name == "muckrock" or source.source_authority == "foia" or source.source_type == "foia_request"
    ]
    result = MuckRockFileResolution(inspected_source_ids=[source.source_id for source in sources])
    existing_urls = set()
    if isinstance(packet_or_sources, CasePacket):
        existing_urls = {artifact.artifact_url for artifact in packet_or_sources.verified_artifacts}

    for source in sources:
        links = _collect_source_links(source)
        public_links = [link for link in links if _is_public_artifact_url(str(link.get("url") or ""))]
        protected_links = [link for link in links if _looks_protected(str(link.get("url") or ""))]

        if protected_links:
            _append_unique(result.risk_flags, ["protected_or_nonpublic"])

        release_text = _text(source.title, source.snippet, source.raw_text).lower()
        if not public_links and any(term in release_text for term in ["produced", "released", "attached files include"]):
            _append_unique(result.next_actions, ["Inspect public MuckRock request page for released-file links."])

        for index, link in enumerate(public_links, start=1):
            url = str(link.get("url") or "")
            if url in existing_urls:
                continue
            context = _source_context(source, link)
            artifact_type = _infer_artifact_type(url, context)
            artifact = VerifiedArtifact(
                artifact_id=_artifact_id(source, index, url),
                artifact_type=artifact_type,
                artifact_url=url,
                source_url=source.url,
                source_authority="foia",
                downloadable=_downloadable_for_url(url),
                format=_format_for_url(url),
                matched_case_fields=list(source.matched_case_fields),
                confidence=_confidence_for(source, artifact_type, url),
                claim_source_url=source.url,
                verification_method=_verification_method(url, link),
                risk_flags=[],
                metadata={
                    "source_id": source.source_id,
                    "request_url": source.url,
                    "link_metadata": {k: v for k, v in link.items() if k != "url"},
                },
            )
            result.verified_artifacts.append(artifact)
            existing_urls.add(url)

    if isinstance(packet_or_sources, CasePacket):
        packet_or_sources.verified_artifacts.extend(result.verified_artifacts)
        _append_unique(packet_or_sources.risk_flags, result.risk_flags)
        _append_unique(packet_or_sources.next_actions, result.next_actions)

    return result
