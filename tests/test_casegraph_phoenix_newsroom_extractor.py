"""PHOENIX-EXTRACTOR-1 — Phoenix Newsroom article-detail HTML extractor tests.

Drives the per-template extractor against a real Phoenix Newsroom
HTML page captured by PR #21's first manual fetch-only smoke. The
saved HTML is committed at ``tests/fixtures/portal_live_html/
phoenix_newsroom_3286.html`` so these tests run zero-network.

The extractor's contract:
- gates strictly on URL host (``www.phoenix.gov``) AND the AEM
  ``<body class="article-detail page basicpage">`` marker
- emits an agency_ois-shaped payload that the existing portal-replay
  path consumes
- raises ``ValueError`` with stable ``phoenix_newsroom_*`` prefixes
  when expected markers are missing
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph.extractors.phoenix_newsroom_html import (
    _html_mentions_bwc,
    extract_phoenix_newsroom_to_agency_ois,
    is_phoenix_newsroom_article_detail,
)


ROOT = Path(__file__).resolve().parents[1]
PHOENIX_HTML_FIXTURE = (
    ROOT / "tests" / "fixtures" / "portal_live_html" / "phoenix_newsroom_3286.html"
)
PHOENIX_URL = "https://www.phoenix.gov/newsroom/police-department-news/3286.html"


@pytest.fixture(scope="module")
def phoenix_html() -> str:
    assert PHOENIX_HTML_FIXTURE.exists(), (
        f"missing committed HTML fixture: {PHOENIX_HTML_FIXTURE}"
    )
    return PHOENIX_HTML_FIXTURE.read_text(encoding="utf-8")


# ---- gate predicate -------------------------------------------------


def test_is_phoenix_newsroom_article_detail_accepts_real_page(phoenix_html):
    assert is_phoenix_newsroom_article_detail(phoenix_html, PHOENIX_URL) is True


def test_is_phoenix_newsroom_article_detail_rejects_non_phoenix_host(phoenix_html):
    assert (
        is_phoenix_newsroom_article_detail(
            phoenix_html, "https://www.example.gov/x"
        )
        is False
    )


def test_is_phoenix_newsroom_article_detail_rejects_phoenix_host_without_marker():
    """A page on www.phoenix.gov that is NOT an article-detail (no
    ``article-detail`` body class) must be rejected so the dispatcher
    falls through to other extractors / the marker-block fallback."""
    minimal = (
        "<html><body class=\"page basicpage\"><h1>landing</h1></body></html>"
    )
    assert (
        is_phoenix_newsroom_article_detail(minimal, PHOENIX_URL) is False
    )


def test_is_phoenix_newsroom_article_detail_rejects_empty_or_none_inputs():
    assert is_phoenix_newsroom_article_detail("", PHOENIX_URL) is False
    assert is_phoenix_newsroom_article_detail("<html></html>", "") is False
    # Defensive: extractor should not crash on None-ish args.
    assert is_phoenix_newsroom_article_detail(None, PHOENIX_URL) is False  # type: ignore[arg-type]


# ---- title ----------------------------------------------------------


def test_extractor_pulls_clean_title_without_site_suffix(phoenix_html):
    out = extract_phoenix_newsroom_to_agency_ois(phoenix_html, PHOENIX_URL)
    assert (
        out["title"]
        == "Critical Incident Briefing - November 5th - 3rd Street and Clarendon"
    )
    assert "City of Phoenix" not in out["title"]


# ---- dates ----------------------------------------------------------


def test_extractor_release_date_is_2024_11_20_via_narrative(phoenix_html):
    out = extract_phoenix_newsroom_to_agency_ois(phoenix_html, PHOENIX_URL)
    # Release date isn't a top-level field in the agency_ois schema;
    # the extractor surfaces it inside the narrative blurb so identity
    # scoring can phrase-match it.
    assert "Release date: 2024-11-20" in out["narrative"]


def test_extractor_incident_date_inferred_2024_11_05(phoenix_html):
    out = extract_phoenix_newsroom_to_agency_ois(phoenix_html, PHOENIX_URL)
    assert out["incident_date"] == "2024-11-05"


def test_extractor_does_not_pick_up_read_next_dates(phoenix_html):
    """Regression: the page also contains ``cmp-byline`` blocks for
    "Read next..." cards (April 16, 2026 etc.). The extractor must
    cap its byline scan at the first ``cmp-article__cards`` container
    so the article's own release date wins."""
    out = extract_phoenix_newsroom_to_agency_ois(phoenix_html, PHOENIX_URL)
    assert "April 16, 2026" not in out["narrative"]
    assert "April 03, 2026" not in out["narrative"]
    assert "Release date: 2024-11-20" in out["narrative"]


# ---- media links (YouTube canonicalisation) -------------------------


def test_extractor_canonicalizes_youtube_iframe_to_watch_url(phoenix_html):
    out = extract_phoenix_newsroom_to_agency_ois(phoenix_html, PHOENIX_URL)
    media_urls = [m["url"] for m in out["media_links"]]
    assert "https://www.youtube.com/watch?v=15yKyn6BprE" in media_urls
    # Embed URL must be canonicalised; the resolver expects watch?v=...
    assert not any("/embed/" in u for u in media_urls)


def test_extractor_marks_youtube_video_as_bodycam_briefing(phoenix_html):
    out = extract_phoenix_newsroom_to_agency_ois(phoenix_html, PHOENIX_URL)
    assert out["media_links"][0]["type"] == "bodycam_briefing"
    assert "Phoenix Newsroom" in out["media_links"][0]["label"]


def test_extractor_dedupes_repeated_youtube_iframes():
    """Defensive: a hand-built minimal page with the same YouTube ID
    in two iframes should produce only one media link."""
    html = (
        '<html><body class="article-detail page basicpage">'
        '<title>Test</title>'
        '<meta property="og:title" content="Test"/>'
        '<iframe src="https://www.youtube.com/embed/abc123XY?x=1"></iframe>'
        '<iframe src="https://www.youtube.com/embed/abc123XY?x=2"></iframe>'
        '<div class="cmp-byline"><p>November 20, 2024</p></div>'
        '</body></html>'
    )
    out = extract_phoenix_newsroom_to_agency_ois(html, PHOENIX_URL)
    assert len(out["media_links"]) == 1
    assert out["media_links"][0]["url"] == "https://www.youtube.com/watch?v=abc123XY"


# ---- agency / jurisdiction --------------------------------------------


def test_extractor_sets_agency_to_phoenix_police_department(phoenix_html):
    out = extract_phoenix_newsroom_to_agency_ois(phoenix_html, PHOENIX_URL)
    assert out["agency"] == "Phoenix Police Department"
    assert out["agency_url_root"] == "https://www.phoenix.gov"


def test_extractor_keeps_subjects_empty_for_unidentified_cib(phoenix_html):
    """The Phoenix CIB does not name the subject pre-adjudication.
    The extractor must NOT invent a defendant; downstream identity
    scoring will land below HIGH for this case (verdict HOLD)."""
    out = extract_phoenix_newsroom_to_agency_ois(phoenix_html, PHOENIX_URL)
    assert out["subjects"] == []


# ---- agency_ois shape compatibility ----------------------------------


def test_extractor_emits_payload_replay_path_accepts(phoenix_html):
    """The extracted payload must satisfy ``extract_to_agency_ois``'s
    direct-passthrough check (page_type / portal_profile_id /
    source_records). Locks in the contract that lets ``--portal-replay
    --fixture <extracted>`` consume the file directly."""
    from pipeline2_discovery.casegraph.portal_live_fetch import extract_to_agency_ois

    extracted = extract_phoenix_newsroom_to_agency_ois(phoenix_html, PHOENIX_URL)
    # Round-trip: re-run through the dispatcher; should pass-through.
    again = extract_to_agency_ois(extracted)
    assert again == extracted


def test_extractor_payload_has_all_canonical_agency_ois_keys(phoenix_html):
    out = extract_phoenix_newsroom_to_agency_ois(phoenix_html, PHOENIX_URL)
    for key in (
        "page_type",
        "agency",
        "agency_url_root",
        "url",
        "title",
        "narrative",
        "subjects",
        "incident_date",
        "case_number",
        "outcome_text",
        "media_links",
        "document_links",
        "claims",
    ):
        assert key in out, f"missing canonical agency_ois key {key!r}"
    assert out["page_type"] == "incident_detail"


# ---- error paths ----------------------------------------------------


def test_extractor_rejects_non_phoenix_url_with_clear_marker():
    html = '<body class="article-detail"></body>'
    with pytest.raises(ValueError, match="phoenix_newsroom_marker_missing"):
        extract_phoenix_newsroom_to_agency_ois(
            html, "https://www.example.gov/some/article"
        )


def test_extractor_rejects_phoenix_url_without_article_detail_body():
    html = '<body class="page basicpage"></body>'
    with pytest.raises(ValueError, match="phoenix_newsroom_marker_missing"):
        extract_phoenix_newsroom_to_agency_ois(html, PHOENIX_URL)


def test_extractor_rejects_phoenix_article_detail_without_title():
    """A bare article-detail page with no ``<title>`` and no ``og:title``
    fails fast with a stable error prefix."""
    html = (
        '<html><body class="article-detail page basicpage">'
        '<iframe src="https://www.youtube.com/embed/abc123XY"></iframe>'
        '</body></html>'
    )
    with pytest.raises(ValueError, match="phoenix_newsroom_title_missing"):
        extract_phoenix_newsroom_to_agency_ois(html, PHOENIX_URL)


# ---- zero-network --------------------------------------------------


def test_extractor_makes_zero_network_calls(monkeypatch, phoenix_html):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("extractor should never touch the network")

    monkeypatch.setattr(requests.Session, "get", fake_get)
    extract_phoenix_newsroom_to_agency_ois(phoenix_html, PHOENIX_URL)
    assert calls == []


# ---- token-of-fact: identity-scoring phrases in narrative -----------


def test_extractor_narrative_contains_phrases_for_identity_anchoring(phoenix_html):
    """The narrative is the operator-readable bridge that lets identity
    scoring's phrase-matcher find ``case_identity.agency`` /
    ``incident_date`` after enrichment."""
    out = extract_phoenix_newsroom_to_agency_ois(phoenix_html, PHOENIX_URL)
    n = out["narrative"]
    assert "Phoenix Police Department" in n
    assert "2024-11-05" in n
    assert "Body-Worn Camera" in n or "BWC" in n


# ---- BWC source-faithfulness ----------------------------------------


@pytest.mark.parametrize(
    "snippet",
    [
        "BWC",
        "Body-Worn Camera",
        "Body-worn camera",
        "body worn camera",
        "body-worn camera",
        "BODY-WORN CAMERA",
        "Officer activated his bwc footage during the stop.",
    ],
)
def test_html_mentions_bwc_detects_each_phrase_variant(snippet):
    """All five spellings the extractor honours, plus all-caps and a
    lowercase BWC, must register as a BWC mention."""
    assert _html_mentions_bwc(f"<p>{snippet}</p>") is True


@pytest.mark.parametrize(
    "snippet",
    [
        "",
        "<p>This page is about traffic safety.</p>",
        # Word-boundary guard: do not match BWC embedded inside another token.
        "<p>NCBwCNxxx is a random identifier with no boundary.</p>",
        # No false-positive on partial phrases.
        "<p>The body of the report</p>",
    ],
)
def test_html_mentions_bwc_returns_false_when_absent(snippet):
    assert _html_mentions_bwc(snippet) is False


def test_html_mentions_bwc_handles_none_and_non_string():
    assert _html_mentions_bwc(None) is False  # type: ignore[arg-type]
    assert _html_mentions_bwc(123) is False  # type: ignore[arg-type]


def test_extractor_narrative_includes_bwc_when_source_mentions_it(phoenix_html):
    """3286.html source HTML mentions BWC, so the narrative may (and
    does) describe the briefing video as Body-Worn Camera footage."""
    out = extract_phoenix_newsroom_to_agency_ois(phoenix_html, PHOENIX_URL)
    n = out["narrative"]
    assert "Body-Worn Camera (BWC)" in n
    assert "official City of Phoenix Newsroom" in n


# Synthetic Phoenix article-detail snippet modeled on 3369.html
# (Critical Incident Briefing - February 12, 2025 - 2400 S Higley Rd.):
#   - same body class as the real page (extractor gate passes)
#   - og:title in the same exact format as 3369.html
#   - first cmp-byline = "February 26, 2025" (release)
#   - one YouTube iframe (gAoDdfgTSi8 — the real video on 3369.html)
#   - cmp-article__cards "Read next" container with a trailing byline
#     that the extractor must NOT pick (regression-anchored on the
#     observed 3369.html structure)
#   - DELIBERATELY contains no BWC / Body-Worn Camera language
PHOENIX_3369_BWC_ABSENT_SNIPPET = (
    '<html><head>'
    '<title>Critical Incident Briefing - February 12, 2025 - '
    '2400 S Higley Rd. | City of Phoenix</title>'
    '<meta property="og:title" '
    'content="Critical Incident Briefing - February 12, 2025 - '
    '2400 S Higley Rd."/>'
    '</head>'
    '<body class="article-detail page basicpage">'
    '<h1>Critical Incident Briefing - February 12, 2025 - '
    '2400 S Higley Rd.</h1>'
    '<div class="cmp-byline"><p>February 26, 2025</p></div>'
    '<iframe src="https://www.youtube.com/embed/gAoDdfgTSi8'
    '?enablejsapi=1&amp;showinfo=0&amp;rel=0"></iframe>'
    '<div class="cmp-article cmp-article__cards">'
    '<div class="cmp-byline"><p>April 16, 2026</p></div>'
    '</div>'
    '</body></html>'
)


def test_extractor_narrative_omits_bwc_when_source_does_not_mention_it():
    """3369.html source HTML contains none of: BWC, Body-Worn Camera,
    Body-worn camera, body worn camera, or body-worn camera. The
    narrative must not invent BWC content for that page. Surfaced by
    the second Phoenix CIB live smoke (PR #24)."""
    out = extract_phoenix_newsroom_to_agency_ois(
        PHOENIX_3369_BWC_ABSENT_SNIPPET,
        "https://www.phoenix.gov/newsroom/police-department-news/3369.html",
    )
    n = out["narrative"]
    for forbidden in (
        "BWC",
        "Body-Worn Camera",
        "Body-worn camera",
        "body worn camera",
        "body-worn camera",
    ):
        assert forbidden not in n, (
            f"narrative for BWC-absent Phoenix page must not contain "
            f"{forbidden!r}; got narrative: {n!r}"
        )
    # Neutral closing sentence still anchors the publishing channel for
    # downstream identity scoring.
    assert "official City of Phoenix Newsroom" in n


def test_extractor_bwc_absent_page_still_parses_title_dates_and_media():
    """The BWC-absent snippet must still extract title, dates, and the
    YouTube media link cleanly. Anchors the contract that the BWC
    conditional did not regress any other parser."""
    out = extract_phoenix_newsroom_to_agency_ois(
        PHOENIX_3369_BWC_ABSENT_SNIPPET,
        "https://www.phoenix.gov/newsroom/police-department-news/3369.html",
    )
    assert (
        out["title"]
        == "Critical Incident Briefing - February 12, 2025 - 2400 S Higley Rd."
    )
    assert out["incident_date"] == "2025-02-12"
    assert "Release date: 2025-02-26" in out["narrative"]
    assert out["media_links"][0]["url"] == (
        "https://www.youtube.com/watch?v=gAoDdfgTSi8"
    )
    assert out["media_links"][0]["type"] == "bodycam_briefing"
    # Subjects honestly empty; identity stays sub-HIGH for unidentified
    # CIBs across both BWC-present and BWC-absent variants.
    assert out["subjects"] == []


def test_extractor_bwc_absent_page_skips_read_next_byline():
    """Regression: the BWC-absent snippet has a "Read next..." byline
    of April 16, 2026 inside cmp-article__cards. The extractor must
    cap its byline scan at the first cmp-article__cards container so
    the article's own release date wins. Mirrors the existing 3286
    regression at a different incident date."""
    out = extract_phoenix_newsroom_to_agency_ois(
        PHOENIX_3369_BWC_ABSENT_SNIPPET,
        "https://www.phoenix.gov/newsroom/police-department-news/3369.html",
    )
    assert "April 16, 2026" not in out["narrative"]
    assert "Release date: 2025-02-26" in out["narrative"]
