"""FIRE1 - no-live Firecrawl/request safety wrapper tests."""
import json
from pathlib import Path

from pipeline2_discovery.casegraph.firecrawl_safety import (
    PortalFetchSafetyRequest,
    evaluate_fetch_safety,
    firecrawl_safety_to_jsonable,
)


ROOT = Path(__file__).resolve().parents[1]


def test_safety_rejects_live_fetch_without_env_gate():
    decision = evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url="https://www.phoenix.gov/police/critical-incidents/2024-OIS-050",
            profile_id="agency_ois_detail",
            fetcher="firecrawl",
            max_pages=1,
            max_links=5,
            dry_run=False,
            live_env_gate=False,
        ),
        repo_root=ROOT,
    )

    assert decision.fetch_allowed is False
    assert decision.blocked_reason == "live_env_gate_required"


def test_safety_accepts_known_official_public_url_in_dry_mode():
    decision = evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url="https://www.phoenix.gov/police/critical-incidents/2024-OIS-050",
            profile_id="agency_ois_detail",
            fetcher="firecrawl",
            max_pages=1,
            max_links=5,
        ),
        repo_root=ROOT,
    )

    assert decision.fetch_allowed is True
    assert decision.blocked_reason is None
    assert decision.estimated_cost == 0.0
    assert "known_url_only" in decision.safety_flags


def test_safety_rejects_unknown_url_and_disallowed_domain():
    unknown = evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url="https://www.phoenix.gov/police/critical-incidents/2024-OIS-050",
            profile_id="agency_ois_detail",
            known_url=False,
        ),
        repo_root=ROOT,
    )
    wrong_domain = evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url="https://example.com/watch?v=abc",
            profile_id="youtube_agency_channel",
            fetcher="requests",
            max_pages=1,
            max_links=5,
        ),
        repo_root=ROOT,
    )

    assert unknown.blocked_reason == "known_seed_url_required"
    assert wrong_domain.blocked_reason == "domain_not_allowed_by_profile"


def test_safety_rejects_broad_search_and_llm_modes():
    broad = evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url="https://www.phoenix.gov/police/critical-incidents",
            profile_id="agency_ois_listing",
            broad_search_mode=True,
        ),
        repo_root=ROOT,
    )
    llm = evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url="https://www.phoenix.gov/police/critical-incidents",
            profile_id="agency_ois_listing",
            allow_llm=True,
        ),
        repo_root=ROOT,
    )

    assert broad.blocked_reason == "broad_search_not_allowed"
    assert llm.blocked_reason == "llm_extraction_not_allowed"


def test_safety_rejects_profile_cap_overrides():
    pages = evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url="https://www.phoenix.gov/police/critical-incidents/2024-OIS-050",
            profile_id="agency_ois_detail",
            max_pages=99,
        ),
        repo_root=ROOT,
    )
    links = evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url="https://www.phoenix.gov/police/critical-incidents/2024-OIS-050",
            profile_id="agency_ois_detail",
            max_pages=1,
            max_links=99,
        ),
        repo_root=ROOT,
    )

    assert pages.blocked_reason == "max_pages_exceeds_profile_cap"
    assert links.blocked_reason == "max_links_exceeds_profile_cap"


def test_safety_rejects_download_private_and_login_intent():
    download = evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url="https://www.phoenix.gov/police/critical-incidents/2024-OIS-050",
            profile_id="agency_ois_detail",
            download_intent=True,
        ),
        repo_root=ROOT,
    )
    login = evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url="https://www.phoenix.gov/login?redirect=/police/critical-incidents",
            profile_id="agency_ois_detail",
        ),
        repo_root=ROOT,
    )

    assert download.blocked_reason == "downloads_not_allowed"
    assert login.blocked_reason == "private_or_login_not_allowed"


def test_safety_rejects_fetchers_not_allowed_by_profile():
    decision = evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url="https://www.youtube.com/watch?v=officialBWC050",
            profile_id="youtube_agency_channel",
            fetcher="firecrawl",
            max_pages=1,
            max_links=5,
        ),
        repo_root=ROOT,
    )

    assert decision.fetch_allowed is False
    assert decision.blocked_reason == "fetcher_not_allowed_by_profile"


def test_safety_decision_is_json_serializable():
    decision = evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url="https://www.phoenix.gov/police/critical-incidents/2024-OIS-050",
            profile_id="agency_ois_detail",
            max_pages=1,
            max_links=5,
        ),
        repo_root=ROOT,
    )

    encoded = json.dumps(firecrawl_safety_to_jsonable(decision), sort_keys=True)
    decoded = json.loads(encoded)

    assert decoded["fetch_allowed"] is True
    assert decoded["profile_id"] == "agency_ois_detail"


def test_safety_wrapper_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url="https://www.phoenix.gov/police/critical-incidents/2024-OIS-050",
            profile_id="agency_ois_detail",
        ),
        repo_root=ROOT,
    )
    assert calls == []


def test_safety_allows_requests_against_seeded_html_metadata_profile():
    """A ``requests``-fetcher target against an HTML profile is
    allowed when env gates are set: the requests fetcher is strictly
    weaker than Firecrawl scraping (which the same profile already
    permits), so the second acceptance path in ``_fetcher_allowed``
    lets operators pick the cheaper, JS-free path for known-static
    pages."""
    decision = evaluate_fetch_safety(
        PortalFetchSafetyRequest(
            url="https://www.phoenix.gov/police/critical-incidents/2024-OIS-050",
            profile_id="agency_ois_detail",
            fetcher="requests",
            max_pages=1,
            max_links=5,
            known_url=True,
            dry_run=False,
            live_env_gate=True,
        ),
        repo_root=ROOT,
    )

    assert decision.fetch_allowed is True
    assert decision.blocked_reason is None


def test_fetcher_allowed_predicate_is_scoped_to_known_tokens():
    """Direct unit test of the predicate. ``requests`` is accepted
    only when the profile's allowed_fetchers list contains either an
    ``*_api`` token or ``seeded_html_metadata``; anything else is
    rejected. ``firecrawl`` and unknown fetcher names follow the
    pre-existing rules unchanged."""
    from pipeline2_discovery.casegraph.firecrawl_safety import _fetcher_allowed

    # requests: API-style token allowed.
    assert _fetcher_allowed("requests", ["youtube_metadata_api"]) is True
    assert _fetcher_allowed("requests", ["api_metadata"]) is True
    # requests: seeded_html_metadata allowed (new path).
    assert _fetcher_allowed("requests", ["seeded_html_metadata"]) is True
    # requests: neither token → rejected.
    assert _fetcher_allowed("requests", ["only_some_other_token"]) is False
    assert _fetcher_allowed("requests", []) is False

    # firecrawl: requires seeded_html_metadata exactly.
    assert _fetcher_allowed("firecrawl", ["seeded_html_metadata"]) is True
    assert _fetcher_allowed("firecrawl", ["youtube_metadata_api"]) is False
    assert _fetcher_allowed("firecrawl", []) is False

    # Unknown fetcher: literal-match only.
    assert _fetcher_allowed("custom_x", ["custom_x"]) is True
    assert _fetcher_allowed("custom_x", ["seeded_html_metadata"]) is False
