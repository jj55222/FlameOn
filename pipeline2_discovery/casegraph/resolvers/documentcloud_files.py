"""F4b — Public DocumentCloud document resolver.

Turns concrete public DocumentCloud canonical URLs and public PDF/file
URLs found in DocumentCloud-style source metadata into
`VerifiedArtifact` document records.

Hard rules (mirrors muckrock_files.py):
- never download files
- never OCR
- never scrape pages
- never follow login/auth/private/protected URLs
- claim text without a concrete URL never produces a VerifiedArtifact
- DocumentCloud `access` values other than "public" are rejected
- the resolver only inspects sources from the DocumentCloud connector
  (api_name == "documentcloud", source_authority == "documentcloud",
  source_type == "documentcloud_document") OR sources whose URL is on
  the documentcloud.org domain
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Sequence
from urllib.parse import urlparse

from ..models import CasePacket, SourceRecord, VerifiedArtifact


FILE_FORMATS = {
    ".pdf": "pdf",
    ".doc": "document",
    ".docx": "document",
    ".rtf": "document",
    ".txt": "document",
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

# DocumentCloud-specific access values that are NOT public.
NONPUBLIC_ACCESS_VALUES = {"private", "organization", "invisible", "pending", "draft"}

DOC_KEYS = {"pdf_url", "canonical_url", "file_url", "download_url"}


@dataclass
class DocumentCloudFileResolution:
    verified_artifacts: List[VerifiedArtifact] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)
    inspected_source_ids: List[str] = field(default_factory=list)


def _clean_url(url: Any) -> str:
    return str(url or "").strip().strip("()[]{}<>'\"")


def _looks_protected(url: str) -> bool:
    lower = url.lower()
    return any(marker in lower for marker in PROTECTED_MARKERS)


def _extension(url: str) -> str:
    path = urlparse(url).path.lower()
    for ext in FILE_FORMATS:
        if path.endswith(ext):
            return ext
    return ""


def _is_documentcloud_canonical(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = parsed.netloc.lower()
    if not host:
        return False
    return "documentcloud.org" in host


def _is_documentcloud_pdf(url: str) -> bool:
    if not url.lower().endswith(".pdf"):
        return False
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    return "documentcloud.org" in host


def _is_acceptable_public_url(url: str) -> bool:
    """A URL is acceptable if it is on documentcloud.org (canonical or
    pdf path) OR has a recognized public document extension AND is not
    protected. PDFs from other public document hosts (e.g. agency
    websites) are also accepted when supplied via DocumentCloud
    metadata."""
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    if _looks_protected(url):
        return False
    if _is_documentcloud_canonical(url):
        return True
    if _extension(url):
        return True
    return False


def _format_for_url(url: str) -> str:
    ext = _extension(url)
    if ext:
        return FILE_FORMATS[ext]
    if _is_documentcloud_canonical(url):
        return "document"
    return "html"


def _downloadable_for_url(url: str) -> bool:
    return bool(_extension(url))


def _is_documentcloud_source(source: SourceRecord) -> bool:
    if source.api_name == "documentcloud":
        return True
    if source.source_authority == "documentcloud":
        return True
    if source.source_type == "documentcloud_document":
        return True
    if source.url and _is_documentcloud_canonical(source.url):
        return True
    metadata = source.metadata or {}
    if isinstance(metadata.get("canonical_url"), str) and _is_documentcloud_canonical(metadata["canonical_url"]):
        return True
    if isinstance(metadata.get("pdf_url"), str) and _is_documentcloud_pdf(metadata["pdf_url"]):
        return True
    return False


def _candidate_links(source: SourceRecord) -> List[Dict[str, Any]]:
    """Pull every candidate document URL from this source's metadata.

    Returns dicts with keys: url, key (which metadata field it came from),
    access (DocumentCloud access label, if known)."""
    metadata = source.metadata or {}
    access = metadata.get("access")
    candidates: List[Dict[str, Any]] = []

    for key in DOC_KEYS:
        value = metadata.get(key)
        if isinstance(value, str) and value:
            candidates.append({"url": _clean_url(value), "key": key, "access": access})

    # Some payloads place the document URL on a separate `links` array.
    links = metadata.get("links")
    if isinstance(links, list):
        for entry in links:
            if isinstance(entry, dict):
                url = entry.get("url") or entry.get("href")
                if isinstance(url, str) and url:
                    candidates.append({"url": _clean_url(url), "key": "links", "access": access})
            elif isinstance(entry, str):
                candidates.append({"url": _clean_url(entry), "key": "links", "access": access})

    # The source's own URL (the canonical URL set by the connector) is
    # also a candidate, but mark it so we know its provenance.
    if isinstance(source.url, str) and source.url:
        candidates.append({"url": _clean_url(source.url), "key": "source_url", "access": access})

    # Dedupe preserving first-seen entry.
    deduped: Dict[str, Dict[str, Any]] = {}
    for cand in candidates:
        url = cand["url"]
        if url and url not in deduped:
            deduped[url] = cand
    return list(deduped.values())


def _infer_artifact_type(url: str, source: SourceRecord) -> str:
    """DocumentCloud artifacts are documents — never video/audio. Map
    deterministic textual hints to specific document subtypes when they
    appear in the source title/snippet/metadata, otherwise default to
    `docket_docs`."""
    text = " ".join(
        str(part) for part in [source.title, source.snippet, source.raw_text, url] if part
    ).lower()
    if re.search(r"\b(complaint|charging document|indictment|affidavit|probable cause)\b", text):
        return "docket_docs"
    if re.search(r"\b(police report|incident report|use of force report)\b", text):
        return "docket_docs"
    if re.search(r"\b(sentencing|plea agreement|order|judgment|memorandum)\b", text):
        return "docket_docs"
    return "docket_docs"


def _artifact_id(source: SourceRecord, index: int, url: str) -> str:
    seed = f"{source.source_id}_{index}_{urlparse(url).path}".lower()
    safe = re.sub(r"[^a-z0-9]+", "_", seed).strip("_")
    return f"documentcloud_doc_{safe[:80] or index}"


def _confidence_for(source: SourceRecord, url: str) -> float:
    confidence = 0.7
    if _extension(url):
        confidence += 0.08
    if _is_documentcloud_canonical(url):
        confidence += 0.05
    if source.matched_case_fields:
        confidence += 0.07
    return round(min(confidence, 0.92), 2)


def _verification_method(url: str, link: Dict[str, Any]) -> str:
    if _extension(url):
        return "documentcloud_pdf_url"
    if _is_documentcloud_canonical(url):
        return "documentcloud_canonical_url"
    return "documentcloud_metadata"


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


def resolve_documentcloud_files(
    packet_or_sources: CasePacket | Sequence[SourceRecord],
) -> DocumentCloudFileResolution:
    """Resolve public DocumentCloud document URLs into VerifiedArtifacts.

    Deterministic and metadata-only. Never downloads. Never follows
    private/protected/non-public URLs. Claim text without a concrete
    URL is left as a claim — the resolver does not graduate it.
    """

    sources = [source for source in _sources_from(packet_or_sources) if _is_documentcloud_source(source)]
    result = DocumentCloudFileResolution(inspected_source_ids=[source.source_id for source in sources])

    existing_urls: set = set()
    if isinstance(packet_or_sources, CasePacket):
        existing_urls = {artifact.artifact_url for artifact in packet_or_sources.verified_artifacts}

    for source in sources:
        access = (source.metadata or {}).get("access")
        normalized_access = str(access).lower() if isinstance(access, str) else ""
        if normalized_access in NONPUBLIC_ACCESS_VALUES:
            _append_unique(
                result.risk_flags,
                ["protected_or_nonpublic", "documentcloud_nonpublic_access"],
            )
            _append_unique(
                result.next_actions,
                ["Manually request public copy of DocumentCloud document or skip non-public records."],
            )
            continue

        candidates = _candidate_links(source)
        public_candidates = [
            cand for cand in candidates if _is_acceptable_public_url(str(cand.get("url") or ""))
        ]
        protected_candidates = [
            cand for cand in candidates if _looks_protected(str(cand.get("url") or ""))
        ]

        if protected_candidates:
            _append_unique(result.risk_flags, ["protected_or_nonpublic"])

        release_text = " ".join(
            str(part) for part in [source.title, source.snippet, source.raw_text] if part
        ).lower()
        if not public_candidates and any(
            term in release_text for term in ["released", "produced", "documents were", "public records"]
        ):
            _append_unique(
                result.next_actions,
                ["Inspect public DocumentCloud landing page for canonical document URL."],
            )

        for index, link in enumerate(public_candidates, start=1):
            url = str(link.get("url") or "")
            if url in existing_urls:
                continue
            artifact_type = _infer_artifact_type(url, source)
            artifact = VerifiedArtifact(
                artifact_id=_artifact_id(source, index, url),
                artifact_type=artifact_type,
                artifact_url=url,
                source_url=source.url,
                source_authority="documentcloud",
                downloadable=_downloadable_for_url(url),
                format=_format_for_url(url),
                matched_case_fields=list(source.matched_case_fields),
                confidence=_confidence_for(source, url),
                claim_source_url=source.url,
                verification_method=_verification_method(url, link),
                risk_flags=[],
                metadata={
                    "source_id": source.source_id,
                    "documentcloud_id": (source.metadata or {}).get("document_id"),
                    "publisher": (source.metadata or {}).get("publisher"),
                    "published_date": (source.metadata or {}).get("published_date"),
                    "page_count": (source.metadata or {}).get("page_count"),
                    "language": (source.metadata or {}).get("language"),
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
