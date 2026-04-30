from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests

from ..models import CaseInput, SourceRecord
from .base import SourceConnector


MUCKROCK_BASE = "https://www.muckrock.com/api_v2/requests/"
CLAIM_TERMS = [
    "bodycam",
    "body cam",
    "body camera",
    "body-worn camera",
    "911",
    "dispatch",
    "interrogation",
    "police interview",
    "records request",
    "public records",
    "foia",
    "released",
    "produced",
    "withheld",
    "denied",
    "no responsive records",
]


class ConnectorError(RuntimeError):
    """Controlled connector failure for live API errors."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _text(*parts: Optional[str]) -> str:
    return " ".join(str(part) for part in parts if part).strip()


def _has_claim_language(text: str) -> bool:
    lower = text.lower()
    return any(term in lower for term in CLAIM_TERMS)


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


def _request_id(item: Dict[str, Any]) -> str:
    raw_id = item.get("id") or item.get("pk") or item.get("absolute_url") or item.get("url") or item.get("title") or "unknown"
    return re.sub(r"[^a-zA-Z0-9_]+", "_", str(raw_id)).strip("_") or "unknown"


def _absolute_url(item: Dict[str, Any]) -> str:
    url = str(item.get("absolute_url") or item.get("url") or "")
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"https://www.muckrock.com{url}"
    return url


def _agency_name(item: Dict[str, Any]) -> Optional[str]:
    agency = item.get("agency")
    if isinstance(agency, dict):
        return agency.get("name") or agency.get("title")
    if isinstance(agency, str):
        return agency
    agencies = item.get("agencies")
    if isinstance(agencies, list) and agencies:
        first = agencies[0]
        if isinstance(first, dict):
            return first.get("name") or first.get("title")
        if isinstance(first, str):
            return first
    return None


class MuckRockConnector(SourceConnector):
    """Capped metadata-only MuckRock FOIA request connector."""

    name = "muckrock"

    def __init__(
        self,
        session: Any = None,
        *,
        timeout: int = 12,
        base_url: str = MUCKROCK_BASE,
        api_token: Optional[str] = None,
        status: Optional[str] = "done",
    ) -> None:
        self.session = session or requests.Session()
        self.timeout = timeout
        self.base_url = base_url
        self.api_token = api_token
        self.status = status
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
            return [f"{agency} bodycam"]
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
            for item in (data.get("results") or [])[:max_results]:
                records.append(self._source_from_item(case_input, query, item))
        return records[:max_results]

    def _build_params(self, query: str, page_size: int) -> Dict[str, Any]:
        params: Dict[str, Any] = {"format": "json", "search": query, "page_size": page_size}
        if self.status:
            params["status"] = self.status
        return params

    def _headers(self) -> Dict[str, str]:
        token = self.api_token
        if token is None:
            token = os.environ.get("MUCKROCK_API_TOKEN")
        if not token:
            return {}
        return {"Authorization": f"Token {token}"}

    def _source_from_item(self, case_input: CaseInput, query: str, item: Dict[str, Any]) -> SourceRecord:
        request_id = _request_id(item)
        title = str(item.get("title") or item.get("name") or "")
        description = str(item.get("description") or item.get("summary") or item.get("status") or "")
        agency = _agency_name(item)
        status = item.get("status")
        raw_text = _text(title, description, status, agency)
        roles = []
        if _has_identity_anchor(case_input, raw_text):
            roles.append("identity_source")
        if _has_claim_language(raw_text):
            roles.append("claim_source")
        if not roles:
            roles.append("claim_source")
        return SourceRecord(
            source_id=f"muckrock_{request_id}",
            url=_absolute_url(item),
            title=title,
            snippet=description,
            raw_text=raw_text,
            source_type="foia_request",
            source_roles=list(dict.fromkeys(roles)),
            source_authority="foia",
            api_name="muckrock",
            discovered_via=query,
            retrieved_at=_utc_now(),
            case_input_id=case_input.raw_input.get("defendant_names") or case_input.input_type,
            metadata={
                "request_id": item.get("id") or item.get("pk"),
                "status": status,
                "agency": agency,
                "date_submitted": item.get("date_submitted") or item.get("date_created"),
                "date_done": item.get("date_done") or item.get("date_completed"),
                "jurisdiction": item.get("jurisdiction"),
            },
            cost_estimate=0.0,
            confidence_signals={
                "claim_language_present": _has_claim_language(raw_text),
                "identity_anchor_present": _has_identity_anchor(case_input, raw_text),
            },
            matched_case_fields=["defendant_full_name"] if _has_identity_anchor(case_input, raw_text) else [],
        )
