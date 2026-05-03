"""PORTAL-LIVE-1 — Portal fetch client unit tests.

Mock client is fully exercised. Firecrawl / requests clients are
asserted to refuse without explicit unblocking (env, API key,
follow-up scrape implementation). Every test asserts zero real
HTTP calls — the Firecrawl client's ``_scrape`` is monkey-patched in
the success case so no network is touched.
"""
from __future__ import annotations

import pytest

from pipeline2_discovery.casegraph.portal_fetch_client import (
    FirecrawlFetchClient,
    MockFetchClient,
    PortalFetchResult,
    PortalLiveTarget,
    RequestsFetchClient,
    UnknownFetchClient,
    make_fetch_client,
)


# A fake key used throughout. Leak detection in other tests asserts this
# string never appears in stdout / bundle / payload outputs.
FAKE_API_KEY = "test-key-not-real-do-not-leak-12345"


def _mock_target(**overrides):
    base = {
        "target_id": "test_target",
        "url": "https://example-public-sheriff.gov/critical-incidents/2024-EX-001",
        "profile_id": "agency_ois_detail",
        "fetcher": "mock",
        "max_pages": 1,
        "max_links": 5,
        "allowed_domains": ["example-public-sheriff.gov"],
        "expected_response_status": 200,
    }
    base.update(overrides)
    return PortalLiveTarget(**base)


# ---- MockFetchClient --------------------------------------------------


def test_mock_fetch_client_returns_canned_response():
    canned = {
        "page_type": "incident_detail",
        "agency": "Test Agency",
        "url": "https://example-public-sheriff.gov/x",
        "title": "Test",
    }
    target = _mock_target(mock_response=canned)
    result = MockFetchClient().fetch(target)

    assert result.error is None
    assert result.fetcher == "mock"
    assert result.status_code == 200
    assert result.raw_payload == canned
    assert result.api_calls == {"mock": 1}
    assert result.estimated_cost_usd == 0.0


def test_mock_fetch_client_falls_back_to_default_response_when_no_canned():
    target = _mock_target()
    result = MockFetchClient().fetch(target)

    assert result.error is None
    assert "page_type" in result.raw_payload
    assert result.raw_payload["url"] == target.url
    assert result.raw_payload["agency"] == "Example Sheriff's Office"


def test_mock_fetch_client_respects_expected_response_status():
    target = _mock_target(expected_response_status=204)
    result = MockFetchClient().fetch(target)

    assert result.status_code == 204


# ---- FirecrawlFetchClient (skeleton) ---------------------------------


def test_firecrawl_client_without_api_key_returns_missing_key_error():
    client = FirecrawlFetchClient(api_key=None)
    target = _mock_target(fetcher="firecrawl")
    result = client.fetch(target)

    assert result.error == "missing_FIRECRAWL_API_KEY"
    assert result.fetcher == "firecrawl"
    assert result.api_calls == {"firecrawl": 0}
    assert result.estimated_cost_usd == 0.0
    assert result.raw_payload == {}
    assert client.has_api_key is False


def test_firecrawl_client_with_api_key_default_scrape_returns_deferred_error():
    """The default ``_scrape`` raises NotImplementedError. The client
    surfaces a clear ``firecrawl_live_call_deferred`` error so tests
    can prove no network call was made."""
    client = FirecrawlFetchClient(api_key=FAKE_API_KEY)
    target = _mock_target(fetcher="firecrawl")
    result = client.fetch(target)

    assert result.error == "firecrawl_live_call_deferred"
    assert result.api_calls == {"firecrawl": 0}
    assert result.estimated_cost_usd == 0.0
    assert client.has_api_key is True


def test_firecrawl_client_with_monkey_patched_scrape_returns_success():
    """When a follow-up PR wires in the real Firecrawl SDK, this is
    the contract: ``_scrape`` returns a dict, the client returns a
    populated PortalFetchResult."""

    class _PatchedClient(FirecrawlFetchClient):
        def _scrape(self, url, *, max_pages):
            return {
                "page_type": "incident_detail",
                "agency": "Patched Agency",
                "url": url,
                "title": "Patched",
                "status_code": 200,
            }

    client = _PatchedClient(api_key=FAKE_API_KEY)
    target = _mock_target(fetcher="firecrawl", max_pages=1)
    result = client.fetch(target)

    assert result.error is None
    assert result.fetcher == "firecrawl"
    assert result.status_code == 200
    assert result.raw_payload["agency"] == "Patched Agency"
    assert result.api_calls == {"firecrawl": 1}
    assert result.estimated_cost_usd == 0.01


def test_firecrawl_client_redacts_api_key_in_error_messages():
    """Defensive: if the upstream SDK ever surfaces the key in an
    error string, the client must replace it with [redacted]
    before returning. Pre-empts a full-output leak detection scan."""

    class _LeakyClient(FirecrawlFetchClient):
        def _scrape(self, url, *, max_pages):
            raise RuntimeError(
                f"backend says key='{FAKE_API_KEY}' is invalid"
            )

    client = _LeakyClient(api_key=FAKE_API_KEY)
    target = _mock_target(fetcher="firecrawl")
    result = client.fetch(target)

    assert result.error is not None
    assert FAKE_API_KEY not in result.error
    assert "[redacted]" in result.error


# ---- RequestsFetchClient (skeleton) ----------------------------------


def test_requests_fetch_client_returns_not_yet_implemented():
    target = _mock_target(fetcher="requests")
    result = RequestsFetchClient().fetch(target)

    assert result.error == "requests_fetcher_not_yet_implemented"
    assert result.fetcher == "requests"
    assert result.api_calls == {"requests": 0}


# ---- UnknownFetchClient ----------------------------------------------


def test_unknown_fetch_client_surfaces_unknown_name():
    target = _mock_target(fetcher="madeup")
    result = UnknownFetchClient("madeup").fetch(target)

    assert result.error == "unknown_fetcher:madeup"
    assert result.fetcher == "madeup"


# ---- make_fetch_client factory ---------------------------------------


def test_make_fetch_client_dispatches_per_target_fetcher():
    assert isinstance(make_fetch_client(_mock_target(fetcher="mock")), MockFetchClient)
    assert isinstance(
        make_fetch_client(_mock_target(fetcher="firecrawl"), env={}),
        FirecrawlFetchClient,
    )
    assert isinstance(
        make_fetch_client(_mock_target(fetcher="requests")),
        RequestsFetchClient,
    )
    assert isinstance(
        make_fetch_client(_mock_target(fetcher="madeup")),
        UnknownFetchClient,
    )


def test_make_fetch_client_reads_firecrawl_api_key_from_env():
    target = _mock_target(fetcher="firecrawl")
    with_key = make_fetch_client(target, env={"FIRECRAWL_API_KEY": FAKE_API_KEY})
    no_key = make_fetch_client(target, env={})

    assert isinstance(with_key, FirecrawlFetchClient)
    assert isinstance(no_key, FirecrawlFetchClient)
    assert with_key.has_api_key is True
    assert no_key.has_api_key is False


# ---- Zero-network invariant ------------------------------------------


def test_fetch_clients_make_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    # Mock client, Firecrawl without key, Firecrawl with deferred default,
    # Requests skeleton, Unknown — none should hit the network.
    MockFetchClient().fetch(_mock_target())
    FirecrawlFetchClient(api_key=None).fetch(_mock_target(fetcher="firecrawl"))
    FirecrawlFetchClient(api_key=FAKE_API_KEY).fetch(_mock_target(fetcher="firecrawl"))
    RequestsFetchClient().fetch(_mock_target(fetcher="requests"))
    UnknownFetchClient("madeup").fetch(_mock_target(fetcher="madeup"))

    assert calls == [], (
        f"fetch clients triggered {len(calls)} live HTTP call(s); must be zero"
    )


def test_fetch_clients_never_leak_api_key_in_results():
    """Across every fetch path, the API key value must never appear in
    the result dict (errors, payloads, or any field)."""
    import json as _json

    for client in [
        MockFetchClient(),
        FirecrawlFetchClient(api_key=None),
        FirecrawlFetchClient(api_key=FAKE_API_KEY),
        RequestsFetchClient(),
        UnknownFetchClient("madeup"),
    ]:
        result = client.fetch(_mock_target(fetcher=client.name if hasattr(client, "name") else "mock"))
        text = _json.dumps(result.to_dict())
        assert FAKE_API_KEY not in text, (
            f"{type(client).__name__} leaked the API key into its result"
        )
