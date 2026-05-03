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


# ---- RequestsFetchClient (real, monkey-patched) ----------------------


class _FakeResponse:
    """Minimal stand-in for ``requests.Response`` — covers every
    attribute the requests fetcher actually reads."""

    def __init__(
        self,
        *,
        status_code: int = 200,
        headers=None,
        text: str = "",
        url: str = "",
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text
        self.url = url


def _patch_session_get(monkeypatch, factory):
    """Monkey-patch requests.Session.get to call ``factory(url, **kwargs)``
    and return its result. ``factory`` returns a _FakeResponse OR
    raises an exception for the timeout/connection paths."""
    import requests

    calls = []

    def fake_get(self, url, *args, **kwargs):
        calls.append({"url": url, "args": args, "kwargs": kwargs})
        return factory(url, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    return calls


def test_requests_fetch_client_returns_html_wrapper_on_success(monkeypatch):
    html = "<html><body>hi</body></html>"
    calls = _patch_session_get(
        monkeypatch,
        lambda url, **kwargs: _FakeResponse(
            status_code=200,
            headers={"Content-Type": "text/html; charset=utf-8"},
            text=html,
            url=url,
        ),
    )

    target = _mock_target(fetcher="requests")
    result = RequestsFetchClient().fetch(target)

    assert result.error is None
    assert result.fetcher == "requests"
    assert result.status_code == 200
    assert result.api_calls == {"requests": 1}
    assert result.estimated_cost_usd == 0.0
    assert result.raw_payload == {
        "content_type": "text/html",
        "url": target.url,
        "status_code": 200,
        "text": html,
    }
    assert len(calls) == 1, "must perform exactly one Session.get call"


def test_requests_fetch_client_calls_session_get_with_timeout_and_user_agent(monkeypatch):
    calls = _patch_session_get(
        monkeypatch,
        lambda url, **kwargs: _FakeResponse(
            status_code=200,
            headers={"Content-Type": "text/html"},
            text="",
            url=url,
        ),
    )

    target = _mock_target(fetcher="requests")
    RequestsFetchClient().fetch(target)

    assert len(calls) == 1
    captured = calls[0]
    assert captured["kwargs"]["timeout"] == 30.0
    assert captured["kwargs"]["allow_redirects"] is True
    headers = captured["kwargs"]["headers"]
    assert "User-Agent" in headers
    assert "FlameOn-CaseGraph-PortalLive" in headers["User-Agent"]


def test_requests_fetch_client_does_not_retry_on_failure(monkeypatch):
    """A single failure must surface immediately as a single call —
    no retry loop, no backoff, no second attempt."""
    import requests

    calls = _patch_session_get(
        monkeypatch,
        lambda url, **kwargs: (_ for _ in ()).throw(
            requests.exceptions.Timeout("boom")
        ),
    )

    target = _mock_target(fetcher="requests")
    result = RequestsFetchClient().fetch(target)

    assert result.error is not None
    assert result.error.startswith("timeout:")
    # api_calls counts the attempt even when it fails — operator visibility.
    assert result.api_calls == {"requests": 1}
    assert len(calls) == 1, "fetcher must not retry"


def test_requests_fetch_client_handles_connection_error(monkeypatch):
    import requests

    _patch_session_get(
        monkeypatch,
        lambda url, **kwargs: (_ for _ in ()).throw(
            requests.exceptions.ConnectionError("dns failed")
        ),
    )

    target = _mock_target(fetcher="requests")
    result = RequestsFetchClient().fetch(target)

    assert result.error == "connection_error:ConnectionError"
    assert result.status_code == 0
    assert result.raw_payload == {}


def test_requests_fetch_client_handles_generic_request_exception(monkeypatch):
    import requests

    _patch_session_get(
        monkeypatch,
        lambda url, **kwargs: (_ for _ in ()).throw(
            requests.exceptions.HTTPError("400 Bad")
        ),
    )

    target = _mock_target(fetcher="requests")
    result = RequestsFetchClient().fetch(target)

    assert result.error.startswith("request_failed:")
    assert "HTTPError" in result.error


def test_requests_fetch_client_handles_non_200_status_cleanly(monkeypatch):
    """A 404 / 500 isn't a fetcher error — the orchestrator surfaces
    it as ``unexpected_status_code`` when the target expected 200.
    The fetch client just returns the actual status faithfully."""
    _patch_session_get(
        monkeypatch,
        lambda url, **kwargs: _FakeResponse(
            status_code=404,
            headers={"Content-Type": "text/html"},
            text="not found",
            url=url,
        ),
    )

    target = _mock_target(fetcher="requests")
    result = RequestsFetchClient().fetch(target)

    assert result.error is None  # fetch itself succeeded
    assert result.status_code == 404
    assert result.raw_payload["status_code"] == 404
    assert result.raw_payload["text"] == "not found"


def test_requests_fetch_client_uses_custom_session_when_supplied(monkeypatch):
    """A constructor-supplied session is used verbatim — useful for
    tests that want to inject a fully-canned session object."""

    class _CountingSession:
        def __init__(self):
            self.calls = []

        def get(self, url, *args, **kwargs):
            self.calls.append((url, args, kwargs))
            return _FakeResponse(
                status_code=200,
                headers={"Content-Type": "text/html"},
                text="<html></html>",
                url=url,
            )

    session = _CountingSession()
    target = _mock_target(fetcher="requests")
    result = RequestsFetchClient(session=session).fetch(target)

    assert result.error is None
    assert result.status_code == 200
    assert len(session.calls) == 1


def test_requests_fetch_client_strips_charset_from_content_type(monkeypatch):
    _patch_session_get(
        monkeypatch,
        lambda url, **kwargs: _FakeResponse(
            status_code=200,
            headers={"Content-Type": "text/html; charset=UTF-8"},
            text="",
            url=url,
        ),
    )

    target = _mock_target(fetcher="requests")
    result = RequestsFetchClient().fetch(target)

    assert result.raw_payload["content_type"] == "text/html"


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
    """Every non-requests fetcher must touch zero HTTP. The requests
    fetcher does call ``Session.get``, but only via the monkey-patched
    fake — the real network must never be reached. ``calls`` only
    counts wrapper invocations (no real downstream call)."""
    import requests

    fake_calls = []

    def fake_get(self, url, *args, **kwargs):
        fake_calls.append({"url": url, "args": args, "kwargs": kwargs})
        return _FakeResponse(
            status_code=200,
            headers={"Content-Type": "text/html"},
            text="<html></html>",
            url=url,
        )

    monkeypatch.setattr(requests.Session, "get", fake_get)

    # Mock + Firecrawl-without-key + Firecrawl-deferred-default + Unknown:
    # zero HTTP attempts.
    MockFetchClient().fetch(_mock_target())
    FirecrawlFetchClient(api_key=None).fetch(_mock_target(fetcher="firecrawl"))
    FirecrawlFetchClient(api_key=FAKE_API_KEY).fetch(_mock_target(fetcher="firecrawl"))
    UnknownFetchClient("madeup").fetch(_mock_target(fetcher="madeup"))
    assert fake_calls == [], (
        f"non-requests fetchers triggered {len(fake_calls)} HTTP call(s); "
        "must be zero"
    )

    # Requests fetcher with monkey-patched session: exactly one wrapper
    # invocation, never a real network round-trip.
    RequestsFetchClient().fetch(_mock_target(fetcher="requests"))
    assert len(fake_calls) == 1, (
        "requests fetcher should make exactly one Session.get call (mocked)"
    )


def test_fetch_clients_never_leak_api_key_in_results(monkeypatch):
    """Across every fetch path, the API key value must never appear in
    the result dict (errors, payloads, or any field)."""
    import json as _json
    import requests

    monkeypatch.setattr(
        requests.Session,
        "get",
        lambda self, url, *args, **kwargs: _FakeResponse(
            status_code=200,
            headers={"Content-Type": "text/html"},
            text="",
            url=url,
        ),
    )

    for client in [
        MockFetchClient(),
        FirecrawlFetchClient(api_key=None),
        FirecrawlFetchClient(api_key=FAKE_API_KEY),
        RequestsFetchClient(),
        UnknownFetchClient("madeup"),
    ]:
        target_fetcher = getattr(client, "name", "mock")
        result = client.fetch(_mock_target(fetcher=target_fetcher))
        text = _json.dumps(result.to_dict())
        assert FAKE_API_KEY not in text, (
            f"{type(client).__name__} leaked the API key into its result"
        )
