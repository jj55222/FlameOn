"""F4a — Capped, metadata-only DocumentCloud search connector.

Mirrors the contract used by the MuckRock and CourtListener connectors:
emits `SourceRecord`s only, never `VerifiedArtifact`s, never sets a
verdict or final confidence. This module makes a single GET to the
public DocumentCloud search endpoint when a real `requests.Session` is
supplied, but the test suite drives it with mock sessions exclusively
(no live calls during pytest).

Hard rules:
- never download files
- never scrape pages
- never call OCR or LLMs
- public DocumentCloud canonical_url / pdf_url are PRESERVED in
  metadata, but the connector itself does NOT mark them as
  VerifiedArtifacts — that work belongs to the F4b resolver.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests

from ..models import CaseInput, SourceRecord
from .base import SourceConnector


DOCUMENTCLOUD_BASE = "https://api.www.documentcloud.org/api/documents/search/"

CLAIM_TERMS = [
    "released",
    "produced",
    "documents released",
    "records produced",
    "obtained",
    "filed",
    "withheld",
    "denied",
    "rejected",
    "no responsive records",
    "exempt from disclosure",
    "redacted",
    "complaint",
    "affidavit",
    "indictment",
    "probable cause",
    "police report",
    "incident report",
    "use of force report",
]

OUTCOME_TERMS = [
    "convicted",
    "sentenced",
    "found guilty",
    "pleaded guilty",
    "pled guilty",
    "charged",
    "indicted",
    "arraigned",
    "acquitted",
    "found not guilty",
    "dismissed",
    "case closed",
    "final judgment",
]


class ConnectorError(RuntimeError):
    """Controlled connector failure for live API errors."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _text(*parts: Optional[str]) -> str:
    return " ".join(str(part) for part in parts if part).strip()


def _has_any(text: str, terms: List[str]) -> bool:
    lower = text.lower()
    return any(term in lower for term in terms)


def _has_claim_language(text: str) -> bool:
    return _has_any(text, CLAIM_TERMS)


def _has_outcome_language(text: str) -> bool:
    return _has_any(text, OUTCOME_TERMS)


def _has_identity_anchor(case_input: CaseInput, text: str) -> bool:
    lower = text.lower()
    known = case_input.known_fields or {}
    names = known.get("defendant_names") or []
    jurisdiction = known.get("jurisdiction") or {}
    has_name = any(name and str(name).lower() in lower for name in names)
    has_location_or_agency = any(
        value and str(value).lower() in lower
        for value in [
            jurisdiction.get("city"),
            jurisdiction.get("county"),
            jurisdiction.get("state"),
            known.get("agency"),
        ]
    )
    return bool(has_name and has_location_or_agency)


def _document_id(item: Dict[str, Any]) -> str:
    raw_id = (
        item.get("id")
        or item.get("pk")
        or item.get("canonical_url")
        or item.get("url")
        or item.get("title")
        or "unknown"
    )
    return re.sub(r"[^a-zA-Z0-9_]+", "_", str(raw_id)).strip("_") or "unknown"


def _canonical_url(item: Dict[str, Any]) -> str:
    url = str(item.get("canonical_url") or item.get("url") or "")
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"https://www.documentcloud.org{url}"
    return url


def _publisher(item: Dict[str, Any]) -> Optional[str]:
    for key in ("source", "publisher", "organization"):
        value = item.get(key)
        if isinstance(value, dict):
            for sub in ("name", "title", "slug"):
                if value.get(sub):
                    return str(value[sub])
        elif isinstance(value, str) and value:
            return value
    return None


def _published_date(item: Dict[str, Any]) -> Optional[str]:
    for key in ("published_at", "publish_at", "created_at", "date_published", "date"):
        value = item.get(key)
        if isinstance(value, str) and value:
            return value
    return None


class DocumentCloudConnector(SourceConnector):
    """Capped metadata-only DocumentCloud search connector."""

    name = "documentcloud"

    def __init__(
        self,
        session: Any = None,
        *,
        timeout: int = 12,
        base_url: str = DOCUMENTCLOUD_BASE,
        api_token: Optional[str] = None,
    ) -> None:
        self.session = session or requests.Session()
        self.timeout = timeout
        self.base_url = base_url
        self.api_token = api_token
        self.last_query: Optional[str] = None
        self.last_error: Optional[str] = None
        self.last_endpoint: Optional[str] = None
        self.last_status_code: Optional[int] = None
        self.last_params: Dict[str, Any] = {}

    def fetch(self, case_input: CaseInput) -> Iterable[SourceRecord]:
        return self.search(case_input)

    def build_queries(self, case_input: CaseInput) -> List[str]:
        known = case_input.known_fields or {}
        names = known.get("defendant_names") or []
        if names:
            return [str(names[0])]
        agency = known.get("agency")
        if agency:
            return [str(agency)]
        candidates = case_input.candidate_queries or []
        return candidates[:1]

    def search(self, case_input: CaseInput, *, max_results: int = 5, max_queries: int = 1) -> List[SourceRecord]:
        max_results = max(0, min(max_results, 10))
        max_queries = max(0, min(max_queries, 3))
        if max_results == 0 or max_queries == 0:
            return []

        records: List[SourceRecord] = []
        for query in self.build_queries(case_input)[:max_queries]:
            self.last_query = query
            self.last_endpoint = self.base_url
            self.last_error = None
            self.last_status_code = None
            self.last_params = self._build_params(query, max_results)
            try:
                response = self.session.get(
                    self.base_url,
                    params=self.last_params,
                    headers=self._headers(),
                    timeout=self.timeout,
                )
                self.last_status_code = getattr(response, "status_code", None)
                if self.last_status_code is not None and self.last_status_code >= 400:
                    self.last_error = f"HTTP {self.last_status_code}"
                    return []
                data = response.json()
            except Exception as exc:  # pragma: no cover - live network errors vary
                self.last_error = str(exc)
                return []
            results = self._extract_results(data)
            for item in results[:max_results]:
                if not isinstance(item, dict):
                    continue
                records.append(self._source_from_item(case_input, query, item))
        return records[:max_results]

    def _build_params(self, query: str, page_size: int) -> Dict[str, Any]:
        return {"q": query, "per_page": page_size, "format": "json"}

    def _headers(self) -> Dict[str, str]:
        token = self.api_token
        if token is None:
            token = os.environ.get("DOCUMENTCLOUD_API_TOKEN")
        if not token:
            return {}
        return {"Authorization": f"Token {token}"}

    @staticmethod
    def _extract_results(data: Any) -> List[Any]:
        if isinstance(data, dict):
            for key in ("results", "documents", "data"):
                value = data.get(key)
                if isinstance(value, list):
                    return value
        if isinstance(data, list):
            return data
        return []

    def _source_from_item(self, case_input: CaseInput, query: str, item: Dict[str, Any]) -> SourceRecord:
        document_id = _document_id(item)
        title = str(item.get("title") or item.get("name") or "")
        description = str(item.get("description") or item.get("summary") or "")
        publisher = _publisher(item)
        published_date = _published_date(item)
        canonical_url = _canonical_url(item)
        pdf_url = item.get("pdf_url") or item.get("file_url") or item.get("download_url")
        text_preview = str(item.get("text_preview") or item.get("text") or "")[:280]
        access = item.get("access")

        raw_text = _text(title, description, text_preview, publisher)
        identity_anchor = _has_identity_anchor(case_input, raw_text)
        claim_present = _has_claim_language(raw_text)
        outcome_present = _has_outcome_language(raw_text)

        roles: List[str] = []
        if identity_anchor:
            roles.append("identity_source")
        if outcome_present:
            roles.append("outcome_source")
        if claim_present:
            roles.append("claim_source")
        if not roles:
            # Fall back to claim_source: a document record about a case
            # is at minimum a candidate lead about claims/records, even
            # without obvious claim language.
            roles.append("claim_source")

        # NEVER assign artifact_source from a connector. Even when the
        # item carries a pdf_url/canonical_url, marking it as
        # `possible_artifact_source` so downstream resolvers can audit
        # candidate URLs without skipping the verification step.
        if pdf_url or canonical_url:
            roles.append("possible_artifact_source")

        return SourceRecord(
            source_id=f"documentcloud_{document_id}",
            url=canonical_url,
            title=title,
            snippet=description or text_preview,
            raw_text=raw_text,
            source_type="documentcloud_document",
            source_roles=list(dict.fromkeys(roles)),
            source_authority="documentcloud",
            api_name="documentcloud",
            discovered_via=query,
            retrieved_at=_utc_now(),
            case_input_id=case_input.raw_input.get("defendant_names") or case_input.input_type,
            metadata={
                "document_id": item.get("id") or item.get("pk"),
                "canonical_url": canonical_url or None,
                "pdf_url": pdf_url,
                "publisher": publisher,
                "published_date": published_date,
                "access": access,
                "page_count": item.get("page_count"),
                "language": item.get("language"),
                "text_preview": text_preview or None,
            },
            cost_estimate=0.0,
            confidence_signals={
                "claim_language_present": claim_present,
                "outcome_language_present": outcome_present,
                "identity_anchor_present": identity_anchor,
                "has_pdf_url": bool(pdf_url),
                "has_canonical_url": bool(canonical_url),
            },
            matched_case_fields=["defendant_full_name"] if identity_anchor else [],
        )
