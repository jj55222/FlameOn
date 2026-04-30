from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

import requests

from ..models import CaseInput, SourceRecord
from .base import SourceConnector


COURTLISTENER_SEARCH_ENDPOINT = "https://www.courtlistener.com/api/rest/v4/search/"
OUTCOME_TERMS = [
    "sentenced",
    "sentenced to",
    "convicted",
    "found guilty",
    "guilty verdict",
    "pleaded guilty",
    "pled guilty",
    "charged with",
    "indicted",
    "dismissed",
    "acquitted",
    "found not guilty",
]
CLAIM_TERMS = [
    "bodycam",
    "body cam",
    "body camera",
    "body-worn",
    "bwc",
    "dashcam",
    "911 audio",
    "dispatch audio",
    "interrogation",
    "police interview",
    "video exhibit",
    "audio exhibit",
    "surveillance video",
]


class ConnectorError(RuntimeError):
    """Controlled CourtListener connector failure."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _text(*parts: Any) -> str:
    return " ".join(str(part) for part in parts if part).strip()


def _clean_html(text: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", without_tags).strip()


def _absolute_url(url: str) -> str:
    url = str(url or "")
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"https://www.courtlistener.com{url}"
    return url


def _safe_id(value: Any) -> str:
    raw = str(value or "unknown")
    return re.sub(r"[^a-zA-Z0-9_]+", "_", raw).strip("_") or "unknown"


def _case_name(item: Dict[str, Any]) -> str:
    return str(item.get("caseName") or item.get("case_name") or item.get("caption") or item.get("name") or "")


def _docket_number(item: Dict[str, Any]) -> Optional[str]:
    return item.get("docketNumber") or item.get("docket_number") or item.get("caseNumber") or item.get("case_number")


def _source_type(search_type: str, item: Dict[str, Any]) -> str:
    absolute_url = str(item.get("absolute_url") or item.get("url") or "").lower()
    if search_type == "o" or "/opinion/" in absolute_url:
        return "court_opinion"
    if search_type in {"r", "d"} or "/docket/" in absolute_url:
        return "court_docket"
    return "court_search_result"


def _has_any(text: str, terms: Sequence[str]) -> bool:
    lower = text.lower()
    return any(term in lower for term in terms)


def _identity_matches(case_input: CaseInput, text: str) -> List[str]:
    lower = text.lower()
    known = case_input.known_fields or {}
    names = known.get("defendant_names") or []
    jurisdiction = known.get("jurisdiction") or {}
    matched: List[str] = []
    for name in names:
        name_text = str(name or "").strip()
        if name_text and name_text.lower() in lower:
            matched.append("defendant_full_name")
            break
        parts = [part for part in name_text.split() if len(part) > 2]
        if parts and parts[-1].lower() in lower:
            matched.append("defendant_last_name")
            break
    for field_name, value in [
        ("city", jurisdiction.get("city")),
        ("county", jurisdiction.get("county")),
        ("state", jurisdiction.get("state")),
        ("agency", known.get("agency")),
    ]:
        if value and str(value).lower() in lower:
            matched.append(field_name)
    return list(dict.fromkeys(matched))


class CourtListenerConnector(SourceConnector):
    """Capped metadata-only CourtListener search connector."""

    name = "courtlistener"

    def __init__(
        self,
        session: Any = None,
        *,
        timeout: int = 12,
        endpoint: str = COURTLISTENER_SEARCH_ENDPOINT,
        api_token: Optional[str] = None,
        search_types: Sequence[str] = ("r", "o"),
    ) -> None:
        self.session = session or requests.Session()
        self.timeout = timeout
        self.endpoint = endpoint
        self.api_token = api_token
        self.search_types = tuple(search_types)
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
            for search_type in self.search_types:
                if len(records) >= max_results:
                    break
                params = self._build_params(query, search_type, max_results)
                self.last_endpoint = self.endpoint
                self.last_params = params
                self.last_error = None
                self.last_status_code = None
                try:
                    response = self.session.get(
                        self.endpoint,
                        params=params,
                        headers=self._headers(),
                        timeout=self.timeout,
                    )
                    self.last_status_code = getattr(response, "status_code", None)
                    if self.last_status_code is not None and self.last_status_code >= 400:
                        self.last_error = f"HTTP {self.last_status_code}"
                        return records[:max_results]
                    data = response.json()
                except Exception as exc:  # pragma: no cover - live network errors vary
                    self.last_error = str(exc)
                    return records[:max_results]

                for item in (data.get("results") or [])[: max_results - len(records)]:
                    records.append(self._source_from_item(case_input, query, search_type, item))
        return records[:max_results]

    def _build_params(self, query: str, search_type: str, page_size: int) -> Dict[str, Any]:
        return {"q": query, "type": search_type, "format": "json", "page_size": page_size}

    def _headers(self) -> Dict[str, str]:
        token = self.api_token
        if token is None:
            token = os.environ.get("COURTLISTENER_API_KEY")
        if not token:
            return {}
        return {"Authorization": f"Token {token}"}

    def _source_from_item(self, case_input: CaseInput, query: str, search_type: str, item: Dict[str, Any]) -> SourceRecord:
        title = _case_name(item)
        snippet = _clean_html(str(item.get("snippet") or item.get("plain_text") or item.get("summary") or ""))
        docket_number = _docket_number(item)
        raw_text = _text(title, snippet, docket_number, item.get("court"))
        matched_fields = _identity_matches(case_input, raw_text)
        roles: List[str] = []
        if matched_fields:
            roles.append("identity_source")
        if _has_any(raw_text, OUTCOME_TERMS):
            roles.append("outcome_source")
        if _has_any(raw_text, CLAIM_TERMS):
            roles.append("claim_source")
        if not roles:
            roles.append("identity_source")

        source_type = _source_type(search_type, item)
        record_id = item.get("id") or item.get("cluster_id") or item.get("docket_id") or item.get("absolute_url") or title
        return SourceRecord(
            source_id=f"courtlistener_{_safe_id(record_id)}",
            url=_absolute_url(str(item.get("absolute_url") or item.get("url") or "")),
            title=title,
            snippet=snippet,
            raw_text=raw_text,
            source_type=source_type,
            source_roles=list(dict.fromkeys(roles)),
            source_authority="court",
            api_name="courtlistener",
            discovered_via=query,
            retrieved_at=_utc_now(),
            case_input_id=case_input.raw_input.get("defendant_names") or case_input.input_type,
            metadata={
                "court": item.get("court") or item.get("court_id") or item.get("court_citation_string"),
                "docket_number": docket_number,
                "case_number": docket_number,
                "case_name": title,
                "date_filed": item.get("dateFiled") or item.get("date_filed"),
                "date_terminated": item.get("dateTerminated") or item.get("date_terminated"),
                "absolute_url": _absolute_url(str(item.get("absolute_url") or item.get("url") or "")),
                "search_type": search_type,
                "cluster_id": item.get("cluster_id") or item.get("cluster"),
                "docket_id": item.get("docket_id") or item.get("docket"),
            },
            cost_estimate=0.0,
            confidence_signals={
                "identity_anchor_present": bool(matched_fields),
                "outcome_terms_present": _has_any(raw_text, OUTCOME_TERMS),
                "claim_terms_present": _has_any(raw_text, CLAIM_TERMS),
            },
            matched_case_fields=matched_fields,
        )
