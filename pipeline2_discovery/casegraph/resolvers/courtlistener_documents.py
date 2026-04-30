"""F3b — CourtListener / RECAP public document metadata resolver.

Turns concrete public CourtListener opinion URLs and public RECAP
document URLs (download_url / filepath_ia / filepath_local rendered as
a courtlistener.com path) into `VerifiedArtifact` document records.

Hard rules:
- never download files
- never call PACER (ecf.uscourts.gov / pacer.uscourts.gov)
- never follow paywalled/login URLs
- claim text without a concrete URL never produces a VerifiedArtifact
- docket-only sources (just /docket/<id>/ landing pages) are NOT
  artifacts — only opinion pages and explicit RECAP document URLs are
- the resolver only inspects sources from the CourtListener connector
  (api_name == "courtlistener", source_authority == "court", or url
  on courtlistener.com)
- protected URL markers (login/auth/token/private) are rejected with
  a `protected_or_nonpublic` risk flag
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Sequence
from urllib.parse import urlparse

from ..models import CasePacket, SourceRecord, VerifiedArtifact


COURTLISTENER_HOSTS = {"www.courtlistener.com", "courtlistener.com"}

# Public CourtListener URL paths that point at viewable artifacts.
PUBLIC_OPINION_PATHS = ("/opinion/",)
# Per-document landing pages on CourtListener (each represents a single
# document in a docket / RECAP filing).
PUBLIC_RECAP_PATHS = ("/recap-document/", "/document/")
# /docket/<id>/<slug>/ alone is the docket landing page — NOT a document.
DOCKET_LANDING_PATTERN = re.compile(r"^/docket/\d+/[^/]+/?$", re.I)

# Per-document RECAP metadata keys we can read.
RECAP_DOCUMENT_KEYS = ("download_url", "filepath_ia", "filepath_local")
RECAP_NESTED_KEYS = ("recap_documents", "documents", "items")

# Document file extensions accepted as VerifiedArtifact formats.
FILE_FORMATS = {
    ".pdf": "pdf",
    ".doc": "document",
    ".docx": "document",
    ".rtf": "document",
    ".txt": "document",
}

# Protected / paywalled markers — reject on any match.
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

# PACER hosts: never resolve through a PACER URL — that's paid access.
PACER_HOSTS = (
    "ecf.uscourts.gov",
    "pacer.uscourts.gov",
    "pcl.uscourts.gov",
    "pacer.psc.uscourts.gov",
)
PACER_PATH_HINTS = ("/cgi-bin/", "/pacer/")


@dataclass
class CourtListenerDocumentResolution:
    verified_artifacts: List[VerifiedArtifact] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)
    inspected_source_ids: List[str] = field(default_factory=list)


def _clean_url(url: Any) -> str:
    return str(url or "").strip().strip("()[]{}<>'\"")


def _looks_protected(url: str) -> bool:
    lower = url.lower()
    return any(marker in lower for marker in PROTECTED_MARKERS)


def _looks_pacer(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if any(host == pacer_host or host.endswith("." + pacer_host) for pacer_host in PACER_HOSTS):
        return True
    path_lower = parsed.path.lower()
    return any(hint in path_lower for hint in PACER_PATH_HINTS)


def _extension(url: str) -> str:
    path = urlparse(url).path.lower()
    for ext in FILE_FORMATS:
        if path.endswith(ext):
            return ext
    return ""


def _format_for_url(url: str) -> str:
    ext = _extension(url)
    if ext:
        return FILE_FORMATS[ext]
    parsed = urlparse(url)
    if parsed.netloc.lower() in COURTLISTENER_HOSTS:
        return "document"
    return "html"


def _is_courtlistener_host(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = parsed.netloc.lower()
    return host in COURTLISTENER_HOSTS


def _is_courtlistener_source(source: SourceRecord) -> bool:
    if source.api_name == "courtlistener":
        return True
    if source.source_authority == "court" and source.source_type in {
        "court_opinion",
        "court_docket",
        "court_search_result",
    }:
        return True
    if source.url and _is_courtlistener_host(source.url):
        return True
    metadata = source.metadata or {}
    abs_url = metadata.get("absolute_url")
    if isinstance(abs_url, str) and _is_courtlistener_host(abs_url):
        return True
    return False


def _is_public_courtlistener_artifact(url: str) -> bool:
    """Public CourtListener artifact = an opinion page or a per-document
    RECAP page. A bare docket landing page is NOT an artifact."""
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or parsed.netloc.lower() not in COURTLISTENER_HOSTS:
        return False
    if _looks_protected(url):
        return False
    path = parsed.path or "/"
    if DOCKET_LANDING_PATTERN.match(path):
        return False
    if any(prefix in path for prefix in PUBLIC_OPINION_PATHS):
        return True
    if any(prefix in path for prefix in PUBLIC_RECAP_PATHS):
        return True
    return False


def _is_public_external_pdf(url: str) -> bool:
    """A non-CourtListener URL is acceptable only if it has a recognized
    public document extension and is not a PACER/protected URL."""
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    if _looks_pacer(url) or _looks_protected(url):
        return False
    return bool(_extension(url))


def _is_acceptable_document_url(url: str) -> bool:
    return _is_public_courtlistener_artifact(url) or _is_public_external_pdf(url)


def _candidate_links(source: SourceRecord) -> List[Dict[str, Any]]:
    metadata = source.metadata or {}
    candidates: List[Dict[str, Any]] = []

    if isinstance(metadata.get("absolute_url"), str):
        candidates.append({"url": _clean_url(metadata["absolute_url"]), "key": "absolute_url"})

    for key in RECAP_DOCUMENT_KEYS:
        value = metadata.get(key)
        if isinstance(value, str) and value:
            candidates.append({"url": _clean_url(value), "key": key})

    if isinstance(metadata.get("pdf_url"), str):
        candidates.append({"url": _clean_url(metadata["pdf_url"]), "key": "pdf_url"})

    for nested_key in RECAP_NESTED_KEYS:
        entries = metadata.get(nested_key)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if isinstance(entry, dict):
                for key in (*RECAP_DOCUMENT_KEYS, "absolute_url", "url", "pdf_url"):
                    val = entry.get(key)
                    if isinstance(val, str) and val:
                        candidates.append({"url": _clean_url(val), "key": f"{nested_key}.{key}"})

    if isinstance(source.url, str) and source.url:
        candidates.append({"url": _clean_url(source.url), "key": "source_url"})

    deduped: Dict[str, Dict[str, Any]] = {}
    for cand in candidates:
        url = cand["url"]
        if url and url not in deduped:
            deduped[url] = cand
    return list(deduped.values())


def _infer_artifact_type(url: str, source: SourceRecord) -> str:
    """CourtListener artifacts are court documents — never video/audio."""
    text = " ".join(
        str(part) for part in [source.title, source.snippet, source.raw_text, url] if part
    ).lower()
    if re.search(r"\b(complaint|charging document|indictment|affidavit|probable cause)\b", text):
        return "docket_docs"
    if re.search(r"\b(opinion|order|judgment|memorandum|decision|ruling)\b", text):
        return "docket_docs"
    if re.search(r"\b(sentencing|plea agreement|brief|motion|filing)\b", text):
        return "docket_docs"
    return "docket_docs"


def _verification_method(url: str, link: Dict[str, Any]) -> str:
    parsed = urlparse(url)
    path = parsed.path
    if parsed.netloc.lower() in COURTLISTENER_HOSTS:
        if any(prefix in path for prefix in PUBLIC_OPINION_PATHS):
            return "courtlistener_opinion"
        if any(prefix in path for prefix in PUBLIC_RECAP_PATHS):
            return "courtlistener_recap_document"
        return "courtlistener_metadata"
    if _extension(url):
        return "external_public_pdf"
    return "courtlistener_metadata"


def _artifact_id(source: SourceRecord, index: int, url: str) -> str:
    seed = f"{source.source_id}_{index}_{urlparse(url).path}".lower()
    safe = re.sub(r"[^a-z0-9]+", "_", seed).strip("_")
    return f"courtlistener_doc_{safe[:80] or index}"


def _confidence_for(source: SourceRecord, url: str) -> float:
    confidence = 0.7
    if _extension(url):
        confidence += 0.08
    parsed = urlparse(url)
    if parsed.netloc.lower() in COURTLISTENER_HOSTS:
        if any(prefix in parsed.path for prefix in PUBLIC_OPINION_PATHS):
            confidence += 0.07
        elif any(prefix in parsed.path for prefix in PUBLIC_RECAP_PATHS):
            confidence += 0.05
    if source.matched_case_fields:
        confidence += 0.05
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


def resolve_courtlistener_documents(
    packet_or_sources: CasePacket | Sequence[SourceRecord],
) -> CourtListenerDocumentResolution:
    """Resolve public CourtListener opinion / RECAP document URLs into
    VerifiedArtifacts. Deterministic, metadata-only. Never downloads,
    never follows PACER, never follows protected URLs. Docket-only
    landing pages are NOT artifacts.
    """

    sources = [source for source in _sources_from(packet_or_sources) if _is_courtlistener_source(source)]
    result = CourtListenerDocumentResolution(inspected_source_ids=[s.source_id for s in sources])

    existing_urls: set = set()
    if isinstance(packet_or_sources, CasePacket):
        existing_urls = {artifact.artifact_url for artifact in packet_or_sources.verified_artifacts}

    for source in sources:
        candidates = _candidate_links(source)
        urls = [str(c.get("url") or "") for c in candidates]

        if any(_looks_pacer(u) for u in urls):
            _append_unique(result.risk_flags, ["pacer_or_paywalled"])
            _append_unique(
                result.next_actions,
                ["Skip PACER-only filings; require a public RECAP / opinion URL."],
            )
        if any(_looks_protected(u) for u in urls):
            _append_unique(result.risk_flags, ["protected_or_nonpublic"])

        public_candidates = [
            cand for cand in candidates if _is_acceptable_document_url(str(cand.get("url") or ""))
        ]

        # Docket-only signal: CourtListener URL exists but it's a docket
        # landing page, not an opinion or RECAP document.
        if not public_candidates:
            for cand in candidates:
                url = str(cand.get("url") or "")
                if not url:
                    continue
                parsed = urlparse(url)
                if parsed.netloc.lower() in COURTLISTENER_HOSTS and DOCKET_LANDING_PATTERN.match(parsed.path):
                    _append_unique(
                        result.next_actions,
                        ["Locate a public opinion or RECAP document URL — docket landing page alone is not a verified document."],
                    )
                    break

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
                source_authority="court",
                downloadable=bool(_extension(url)),
                format=_format_for_url(url),
                matched_case_fields=list(source.matched_case_fields),
                confidence=_confidence_for(source, url),
                claim_source_url=source.url,
                verification_method=_verification_method(url, link),
                risk_flags=[],
                metadata={
                    "source_id": source.source_id,
                    "court": (source.metadata or {}).get("court"),
                    "docket_number": (source.metadata or {}).get("docket_number"),
                    "case_name": (source.metadata or {}).get("case_name"),
                    "date_filed": (source.metadata or {}).get("date_filed"),
                    "date_terminated": (source.metadata or {}).get("date_terminated"),
                    "search_type": (source.metadata or {}).get("search_type"),
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
