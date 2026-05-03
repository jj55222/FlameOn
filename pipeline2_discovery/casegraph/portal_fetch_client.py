"""PORTAL-LIVE-1 — Pluggable portal fetch client abstraction.

Three fetchers are recognised today:

- ``mock`` (fully implemented): zero-network. Returns a target-supplied
  canned ``mock_response`` dict (or a thin default fallback). Used by
  the live-smoke scaffolding tests so the orchestrator can be exercised
  end-to-end without any HTTP dependency.
- ``firecrawl`` (skeleton): refuses unless ``FIRECRAWL_API_KEY`` is
  present in the environment. The actual Firecrawl SDK call is
  delegated to ``_scrape``, which tests monkey-patch. The default
  ``_scrape`` implementation raises ``NotImplementedError`` so a real
  fetch only happens when an explicit follow-up PR wires it in.
- ``requests`` (skeleton): always returns an error result; deferred to
  a follow-up PR.

The client never reads from disk, never logs, and never echoes the API
key. All API-key handling lives inside the Firecrawl client and never
flows back into the orchestrator's return value.
"""
from __future__ import annotations

import os
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Mapping, Optional, Protocol


@dataclass
class PortalLiveTarget:
    """Operator-supplied portal-live target spec (parsed from a target
    fixture). Captures everything the orchestrator needs to perform a
    bounded, single-URL live fetch.

    ``mock_response`` is consumed only by ``MockFetchClient``; real
    fetchers ignore it.
    """

    target_id: str
    url: str
    profile_id: str
    fetcher: str = "mock"
    max_pages: int = 1
    max_links: int = 5
    allowed_domains: list[str] = field(default_factory=list)
    expected_response_status: int = 200
    save_raw_payload: bool = True
    save_extracted_payload: bool = True
    replay_through_portal_replay: bool = True
    mock_response: Optional[Dict[str, Any]] = None


@dataclass
class PortalFetchResult:
    """Outcome of a single fetch attempt. Always JSON-serialisable.
    ``raw_payload`` is the verbatim shape the fetcher emitted (mock
    returns a dict; future Firecrawl/requests would also normalize to
    a JSON-friendly dict). ``error`` is None on success."""

    raw_payload: Dict[str, Any]
    status_code: int
    fetcher: str
    wallclock_seconds: float
    api_calls: Dict[str, int]
    estimated_cost_usd: float
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class PortalFetchClient(Protocol):
    """Minimal protocol every fetcher implements."""

    def fetch(self, target: PortalLiveTarget) -> PortalFetchResult:
        ...


class MockFetchClient:
    """Zero-network fetcher. Returns ``target.mock_response`` verbatim
    when supplied; otherwise a tiny default agency_ois-shaped payload
    so smoke tests can still exercise the full chain."""

    name = "mock"

    _DEFAULT_RESPONSE: Dict[str, Any] = {
        "page_type": "incident_detail",
        "agency": "Example Sheriff's Office",
        "url": "",
        "title": "Mock Incident Briefing",
        "narrative": (
            "Mock agency_ois-shaped payload returned by MockFetchClient. "
            "The orchestrator should be able to extract this as-is and "
            "feed it through the existing offline portal replay path."
        ),
        "subjects": ["Mock Subject"],
        "incident_date": "2024-01-01",
        "case_number": "MOCK-001",
        "outcome_text": "subject mocked 2024",
        "media_links": [],
        "document_links": [],
        "claims": [],
    }

    def fetch(self, target: PortalLiveTarget) -> PortalFetchResult:
        started = time.perf_counter()
        if target.mock_response is not None:
            payload = dict(target.mock_response)
        else:
            payload = dict(self._DEFAULT_RESPONSE)
            payload["url"] = target.url
        wallclock = round(time.perf_counter() - started, 6)
        return PortalFetchResult(
            raw_payload=payload,
            status_code=target.expected_response_status,
            fetcher=self.name,
            wallclock_seconds=wallclock,
            api_calls={"mock": 1},
            estimated_cost_usd=0.0,
            error=None,
        )


class FirecrawlFetchClient:
    """Skeleton Firecrawl client.

    The actual SDK call is delegated to ``_scrape``; the default
    implementation raises ``NotImplementedError`` so no real fetch
    happens unless a follow-up PR wires in the real client. Tests
    monkey-patch ``_scrape`` to inject canned responses.

    The API key is captured at construction and never echoed in any
    return value, error message, or log line. Errors related to the
    key only mention the env var name (``FIRECRAWL_API_KEY``), never
    the value.
    """

    name = "firecrawl"
    _COST_PER_PAGE_USD: float = 0.01

    def __init__(self, api_key: Optional[str] = None) -> None:
        self._api_key = api_key

    @property
    def has_api_key(self) -> bool:
        return bool(self._api_key)

    def fetch(self, target: PortalLiveTarget) -> PortalFetchResult:
        started = time.perf_counter()
        if not self.has_api_key:
            return PortalFetchResult(
                raw_payload={},
                status_code=0,
                fetcher=self.name,
                wallclock_seconds=round(time.perf_counter() - started, 6),
                api_calls={"firecrawl": 0},
                estimated_cost_usd=0.0,
                error="missing_FIRECRAWL_API_KEY",
            )
        try:
            payload = self._scrape(target.url, max_pages=target.max_pages)
        except NotImplementedError:
            return PortalFetchResult(
                raw_payload={},
                status_code=0,
                fetcher=self.name,
                wallclock_seconds=round(time.perf_counter() - started, 6),
                api_calls={"firecrawl": 0},
                estimated_cost_usd=0.0,
                error="firecrawl_live_call_deferred",
            )
        except Exception as exc:  # pragma: no cover - defensive for real client
            return PortalFetchResult(
                raw_payload={},
                status_code=0,
                fetcher=self.name,
                wallclock_seconds=round(time.perf_counter() - started, 6),
                api_calls={"firecrawl": 1},
                estimated_cost_usd=self._COST_PER_PAGE_USD * max(target.max_pages, 1),
                error=_redact_api_key(str(exc), self._api_key),
            )
        wallclock = round(time.perf_counter() - started, 6)
        return PortalFetchResult(
            raw_payload=dict(payload or {}),
            status_code=int(payload.get("status_code", target.expected_response_status))
            if isinstance(payload, dict)
            else target.expected_response_status,
            fetcher=self.name,
            wallclock_seconds=wallclock,
            api_calls={"firecrawl": 1},
            estimated_cost_usd=self._COST_PER_PAGE_USD * max(target.max_pages, 1),
            error=None,
        )

    def _scrape(self, url: str, *, max_pages: int) -> Dict[str, Any]:
        """Real Firecrawl call lives here. Default: refuse.

        A follow-up PR will replace this with a real ``FirecrawlApp.scrape``
        invocation. Tests monkey-patch this method to inject canned
        responses without touching the network.
        """
        raise NotImplementedError(
            "Firecrawl live scrape is deferred to a follow-up PR"
        )


class RequestsFetchClient:
    """Skeleton ``requests``-based fetcher. Deferred — always returns
    an error result so the orchestrator can surface the limitation
    cleanly without invoking any network call."""

    name = "requests"

    def fetch(self, target: PortalLiveTarget) -> PortalFetchResult:
        return PortalFetchResult(
            raw_payload={},
            status_code=0,
            fetcher=self.name,
            wallclock_seconds=0.0,
            api_calls={"requests": 0},
            estimated_cost_usd=0.0,
            error="requests_fetcher_not_yet_implemented",
        )


class UnknownFetchClient:
    """Fallback for unknown fetcher names. Always errors; never
    fetches."""

    def __init__(self, name: str) -> None:
        self.name = name

    def fetch(self, target: PortalLiveTarget) -> PortalFetchResult:
        return PortalFetchResult(
            raw_payload={},
            status_code=0,
            fetcher=self.name,
            wallclock_seconds=0.0,
            api_calls={},
            estimated_cost_usd=0.0,
            error=f"unknown_fetcher:{self.name}",
        )


def make_fetch_client(
    target: PortalLiveTarget,
    *,
    env: Optional[Mapping[str, str]] = None,
) -> PortalFetchClient:
    """Factory: pick the right fetcher for a target. The Firecrawl
    client is constructed with the API key from env (or None when
    missing); the orchestrator decides whether to call ``fetch``
    after running its own preflight."""
    environment = os.environ if env is None else env
    fetcher = (target.fetcher or "").lower()
    if fetcher == "mock":
        return MockFetchClient()
    if fetcher == "firecrawl":
        return FirecrawlFetchClient(api_key=environment.get("FIRECRAWL_API_KEY"))
    if fetcher == "requests":
        return RequestsFetchClient()
    return UnknownFetchClient(target.fetcher)


def _redact_api_key(text: str, api_key: Optional[str]) -> str:
    """Defensive redaction in case a future SDK error surfaces the
    key. ``text`` is replaced inline with the literal ``[redacted]``
    everywhere the key value appears."""
    if not api_key or not text:
        return text
    return text.replace(api_key, "[redacted]")


__all__ = [
    "FirecrawlFetchClient",
    "MockFetchClient",
    "PortalFetchClient",
    "PortalFetchResult",
    "PortalLiveTarget",
    "RequestsFetchClient",
    "UnknownFetchClient",
    "make_fetch_client",
]
