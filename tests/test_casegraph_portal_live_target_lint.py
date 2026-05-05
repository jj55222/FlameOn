"""Zero-network tests for ``portal_live_target_lint``.

Covers the URL allowlist, rejection reasons, fragment normalization,
and custom target_id safety. Anchors the contract so the next time
the host allowlist or path regex is touched, behavior changes are
caught at test time.
"""
from __future__ import annotations

import pytest

from pipeline2_discovery.casegraph.portal_live_target_lint import (
    PORTAL_LIVE_TARGET_HOSTS,
    lint_candidate,
)


# ---- accepts ---------------------------------------------------------


@pytest.mark.parametrize(
    "url,expected_target_id",
    [
        (
            "https://www.phoenix.gov/newsroom/police-department-news/3369.html",
            "phoenix_pd_newsroom_3369",
        ),
        (
            "https://www.phoenix.gov/newsroom/police-department-news/3286.html",
            "phoenix_pd_newsroom_3286",
        ),
        # Slug-style id (URL form Phoenix uses for some 2025 CIBs).
        (
            "https://www.phoenix.gov/newsroom/police-department-news/critical-incident-briefing-may-25-2025.html",
            "phoenix_pd_newsroom_critical-incident-briefing-may-25-2025",
        ),
    ],
)
def test_lint_accepts_phoenix_newsroom_article_detail_urls(url, expected_target_id):
    decision = lint_candidate(url)
    assert decision.accepted is True
    assert decision.reason == "ok"
    assert decision.host == "www.phoenix.gov"
    assert decision.normalized_url == url
    assert decision.target_id_suggestion == expected_target_id


def test_lint_strips_fragment_from_normalized_url():
    """Fragments are non-actionable for the fetcher and must be
    silently stripped so dedup-by-normalized-URL works correctly."""
    decision = lint_candidate(
        "https://www.phoenix.gov/newsroom/police-department-news/3369.html#video"
    )
    assert decision.accepted is True
    assert decision.normalized_url == (
        "https://www.phoenix.gov/newsroom/police-department-news/3369.html"
    )


# ---- rejects ---------------------------------------------------------


def test_lint_rejects_non_https_scheme():
    decision = lint_candidate(
        "http://www.phoenix.gov/newsroom/police-department-news/3369.html"
    )
    assert decision.accepted is False
    assert decision.reason == "non_https_scheme"


def test_lint_rejects_muckrock_host():
    decision = lint_candidate("https://www.muckrock.com/foi/phoenix/12345/")
    assert decision.accepted is False
    assert decision.reason == "host_not_in_allowlist"
    assert decision.host == "www.muckrock.com"


def test_lint_rejects_news_aggregator_host():
    decision = lint_candidate("https://www.washingtonpost.com/some-article")
    assert decision.accepted is False
    assert decision.reason == "host_not_in_allowlist"


@pytest.mark.parametrize(
    "url",
    [
        "https://twitter.com/phoenixpolice",
        "https://x.com/phoenixpolice",
        "https://www.facebook.com/phoenixpolice",
        "https://www.youtube.com/watch?v=15yKyn6BprE",
    ],
)
def test_lint_rejects_social_and_video_hosts(url):
    decision = lint_candidate(url)
    assert decision.accepted is False
    assert decision.reason == "host_not_in_allowlist"


def test_lint_rejects_phoenix_search_path():
    """A search URL on phoenix.gov hits both the query-string check
    and the denylisted-path check; either rejection is acceptable as
    long as the result is rejection."""
    decision = lint_candidate(
        "https://www.phoenix.gov/search?q=critical+incident"
    )
    assert decision.accepted is False
    assert decision.reason in ("query_string_not_allowed", "denylisted_path")


def test_lint_rejects_phoenix_login_path():
    decision = lint_candidate("https://www.phoenix.gov/login")
    assert decision.accepted is False
    assert decision.reason == "denylisted_path"


def test_lint_rejects_phoenix_pdf_url():
    decision = lint_candidate(
        "https://www.phoenix.gov/newsroom/police-department-news/file.pdf"
    )
    assert decision.accepted is False
    assert decision.reason == "denylisted_extension:.pdf"


def test_lint_rejects_phoenix_url_with_query_string():
    decision = lint_candidate(
        "https://www.phoenix.gov/newsroom/police-department-news/3369.html?utm_source=x"
    )
    assert decision.accepted is False
    assert decision.reason == "query_string_not_allowed"


def test_lint_rejects_phoenix_path_outside_newsroom():
    decision = lint_candidate("https://www.phoenix.gov/about-us.html")
    assert decision.accepted is False
    assert decision.reason == "path_pattern_mismatch"


def test_lint_rejects_phoenix_newsroom_outside_police_dept():
    decision = lint_candidate(
        "https://www.phoenix.gov/newsroom/parks/event-3286.html"
    )
    assert decision.accepted is False
    assert decision.reason == "path_pattern_mismatch"


def test_lint_rejects_empty_url():
    decision = lint_candidate("")
    assert decision.accepted is False
    assert decision.reason == "empty_url"


def test_lint_rejects_none_url():
    decision = lint_candidate(None)  # type: ignore[arg-type]
    assert decision.accepted is False
    assert decision.reason == "empty_url"


def test_lint_rejects_url_missing_host():
    decision = lint_candidate("https:///newsroom/police-department-news/x.html")
    assert decision.accepted is False
    # urlparse can produce different pieces depending on the malformed
    # input; either missing_host or host_not_in_allowlist is acceptable
    # as long as the URL is rejected.
    assert decision.reason in ("missing_host", "host_not_in_allowlist")


# ---- custom target_id ------------------------------------------------


def test_lint_rejects_unsafe_custom_target_id_path_traversal():
    decision = lint_candidate(
        "https://www.phoenix.gov/newsroom/police-department-news/3369.html",
        custom_target_id="../../etc/passwd",
    )
    assert decision.accepted is False
    assert decision.reason == "unsafe_target_id"


def test_lint_rejects_unsafe_custom_target_id_with_whitespace():
    decision = lint_candidate(
        "https://www.phoenix.gov/newsroom/police-department-news/3369.html",
        custom_target_id="phoenix pd 3369",
    )
    assert decision.accepted is False
    assert decision.reason == "unsafe_target_id"


def test_lint_rejects_unsafe_custom_target_id_with_shell_metachars():
    decision = lint_candidate(
        "https://www.phoenix.gov/newsroom/police-department-news/3369.html",
        custom_target_id="phoenix_pd;rm -rf /",
    )
    assert decision.accepted is False
    assert decision.reason == "unsafe_target_id"


def test_lint_accepts_safe_custom_target_id():
    decision = lint_candidate(
        "https://www.phoenix.gov/newsroom/police-department-news/3369.html",
        custom_target_id="phoenix_pd_2025_02_12_higley_cib",
    )
    assert decision.accepted is True
    assert decision.target_id_suggestion == "phoenix_pd_2025_02_12_higley_cib"


# ---- allowlist surface -----------------------------------------------


def test_phoenix_is_the_only_allowed_host_for_this_pr():
    """Future PRs may extend this list; for now any change here must
    be matched by per-host path patterns and lint behavior."""
    assert PORTAL_LIVE_TARGET_HOSTS == ("www.phoenix.gov",)


# ---- zero-network ----------------------------------------------------


def test_lint_makes_zero_network_calls(monkeypatch):
    import requests

    def fail_get(self, *args, **kwargs):
        raise AssertionError("lint must never touch the network")

    monkeypatch.setattr(requests.Session, "get", fail_get)
    lint_candidate(
        "https://www.phoenix.gov/newsroom/police-department-news/3369.html"
    )
    lint_candidate("https://www.muckrock.com/x")
    lint_candidate("not even a url")
