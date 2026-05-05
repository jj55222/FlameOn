"""Phoenix Newsroom article-detail HTML extractor.

Targets exactly one template: City of Phoenix Newsroom article-detail
pages served by Adobe Experience Manager (AEM) under
``https://www.phoenix.gov/newsroom/...``. The Phoenix Police
Department publishes Critical Incident Briefings on this template.

The extractor is deliberately narrow:

  - ``is_phoenix_newsroom_article_detail`` gates on URL host + the
    ``<body class="article-detail page basicpage">`` AEM marker. Any
    page that does not match both is rejected up-front so the
    orchestrator can fall through to other extractors.
  - ``extract_phoenix_newsroom_to_agency_ois`` parses the title,
    release date (first ``cmp-byline`` block before any
    ``cmp-article__cards`` "Read next" container), inferred incident
    date (month-day from the title + year from the release date),
    embedded YouTube videos (canonicalized to ``watch?v=...`` URLs),
    and emits an agency_ois-shaped payload.

Anti-markers (verified against the saved 2024-11-05 CIB page):

  - The phrase "Body-worn camera" does NOT appear; the page uses
    ``BWC`` only. Do not rely on the long form.
  - No ``<script type="application/ld+json">`` block. No schema.org
    structured data to fall back on.
  - Multiple ``cmp-byline`` blocks exist on the page; only the first
    one (above the ``cmp-article cmp-article__cards`` "Read next"
    container) is the article's own publication date.

The extractor never fetches and never imports any networked module.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse


# ---- gating ----------------------------------------------------------


PHOENIX_NEWSROOM_HOSTS = ("www.phoenix.gov",)

_ARTICLE_DETAIL_BODY_RE = re.compile(
    r'<body[^>]*\bclass="[^"]*\barticle-detail\b[^"]*"',
    re.IGNORECASE,
)


def is_phoenix_newsroom_article_detail(html: str, source_url: str) -> bool:
    """True iff ``html`` is a Phoenix Newsroom AEM article-detail page
    served from the Phoenix Police Department newsroom area.

    Both conditions must hold:
      - the URL host is ``www.phoenix.gov``
      - the HTML contains a ``<body class="article-detail ...">`` tag

    This is intentionally narrow: a generic ``www.phoenix.gov`` page
    that is NOT an article-detail (e.g. a listing index, a department
    landing page) will not match and the dispatcher will fall through.
    """
    if not html or not isinstance(html, str):
        return False
    if not source_url:
        return False
    host = urlparse(source_url).netloc.lower()
    if host not in PHOENIX_NEWSROOM_HOSTS:
        return False
    return bool(_ARTICLE_DETAIL_BODY_RE.search(html))


# ---- extractor -------------------------------------------------------


def extract_phoenix_newsroom_to_agency_ois(
    html: str, source_url: str
) -> Dict[str, Any]:
    """Convert a Phoenix Newsroom article-detail HTML page into the
    canonical agency_ois payload shape that
    ``--portal-replay --fixture <path>`` consumes.

    Raises ``ValueError`` with a stable ``phoenix_newsroom_*`` prefix
    when an expected marker is missing, so the orchestrator can
    surface a clean ``blocked_reason`` and the operator knows which
    field failed extraction.
    """
    if not is_phoenix_newsroom_article_detail(html, source_url):
        raise ValueError(
            "phoenix_newsroom_marker_missing: HTML is not a Phoenix Newsroom "
            "article-detail page (host or body class did not match)"
        )

    title = _extract_title(html)
    if not title:
        raise ValueError(
            "phoenix_newsroom_title_missing: could not parse <title> or og:title"
        )

    release_date = _extract_release_date(html)  # ISO yyyy-mm-dd or None
    incident_date = _extract_incident_date(title, release_date)  # or None
    media_links = _extract_media_links(html)
    bwc_mentioned = _html_mentions_bwc(html)

    return {
        "page_type": "incident_detail",
        "agency": "Phoenix Police Department",
        "agency_url_root": "https://www.phoenix.gov",
        "url": source_url,
        "title": title,
        "narrative": _build_narrative(
            title=title,
            release_date=release_date,
            incident_date=incident_date,
            bwc_mentioned=bwc_mentioned,
        ),
        "subjects": [],
        "incident_date": incident_date,
        "case_number": "",
        "outcome_text": None,
        "media_links": media_links,
        "document_links": [],
        "claims": [],
    }


# ---- title -----------------------------------------------------------


_OG_TITLE_RE = re.compile(
    r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_HEAD_TITLE_RE = re.compile(
    r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL
)
_CITY_OF_PHOENIX_SUFFIX = "| City of Phoenix"


def _extract_title(html: str) -> Optional[str]:
    """Prefer ``og:title`` (clean, no site suffix); fall back to
    ``<title>`` minus the trailing ``| City of Phoenix`` site name."""
    m = _OG_TITLE_RE.search(html)
    if m:
        return m.group(1).strip() or None
    m = _HEAD_TITLE_RE.search(html)
    if m:
        title = m.group(1).strip()
        if title.endswith(_CITY_OF_PHOENIX_SUFFIX):
            title = title[: -len(_CITY_OF_PHOENIX_SUFFIX)].rstrip(" |\t").strip()
        return title or None
    return None


# ---- release date (first cmp-byline before "Read next" cards) -------


_READ_NEXT_CONTAINER_RE = re.compile(
    r'cmp-article\s+cmp-article__cards', re.IGNORECASE
)
_BYLINE_BLOCK_RE = re.compile(
    r'<div[^>]*\bclass="[^"]*\bcmp-byline\b[^"]*"[^>]*>(.*?)</div>',
    re.IGNORECASE | re.DOTALL,
)
_BYLINE_INNER_DATE_RE = re.compile(
    r"<p[^>]*>\s*(?:Posted\s+on\s+)?([^<]+?)\s*</p>",
    re.IGNORECASE,
)


def _extract_release_date(html: str) -> Optional[str]:
    """Return the article's publication / release date as an
    ISO-8601 ``yyyy-mm-dd`` string, or ``None`` if not parseable.

    The page renders ``<div class="cmp-byline"><p>November 20, 2024</p></div>``
    above the article body. ``cmp-byline`` blocks also appear on each
    "Read next..." card further down; we explicitly cap the search at
    the first ``cmp-article__cards`` container."""
    cap = _READ_NEXT_CONTAINER_RE.search(html)
    cutoff = cap.start() if cap else len(html)
    for block in _BYLINE_BLOCK_RE.finditer(html, 0, cutoff):
        inner = block.group(1)
        date_m = _BYLINE_INNER_DATE_RE.search(inner)
        if date_m:
            iso = _normalize_long_date(date_m.group(1).strip())
            if iso:
                return iso
    return None


# ---- incident date (title month-day + release year) -----------------


_MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}

_LONG_DATE_RE = re.compile(
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})",
    re.IGNORECASE,
)
_TITLE_MONTH_DAY_RE = re.compile(
    r"(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|"
    r"Aug|Sep|Sept|Oct|Nov|Dec)\s+(?P<day>\d{1,2})(?:st|nd|rd|th)?",
    re.IGNORECASE,
)


def _normalize_long_date(text: str) -> Optional[str]:
    """``"November 20, 2024"`` → ``"2024-11-20"``."""
    if not text:
        return None
    m = _LONG_DATE_RE.match(text.strip())
    if not m:
        return None
    month = _MONTH_NAMES.get(m.group("month").lower())
    if not month:
        return None
    day = int(m.group("day"))
    if not (1 <= day <= 31):
        return None
    return f"{m.group('year')}-{month:02d}-{day:02d}"


def _extract_incident_date(
    title: Optional[str], release_date: Optional[str]
) -> Optional[str]:
    """Phoenix CIB titles encode the incident month-day:
    ``"Critical Incident Briefing - November 5th - 3rd Street and Clarendon"``.
    The year is inferred from the release date. Returns ``None`` if
    either piece is missing."""
    if not title or not release_date:
        return None
    m = _TITLE_MONTH_DAY_RE.search(title)
    if not m:
        return None
    month = _MONTH_NAMES.get(m.group("month").lower())
    if not month:
        return None
    day = int(m.group("day"))
    if not (1 <= day <= 31):
        return None
    year = release_date[:4]
    if not (year.isdigit() and len(year) == 4):
        return None
    return f"{year}-{month:02d}-{day:02d}"


# ---- media links (embedded YouTube) ----------------------------------


_IFRAME_SRC_RE = re.compile(
    r'<iframe[^>]+src=["\']([^"\']+)["\']', re.IGNORECASE
)
_YOUTUBE_EMBED_ID_RE = re.compile(
    r"(?:youtube\.com|youtube-nocookie\.com)/embed/([A-Za-z0-9_-]{6,})",
    re.IGNORECASE,
)


def _extract_media_links(html: str) -> List[Dict[str, str]]:
    """Find embedded YouTube videos and emit canonical ``watch?v=<ID>``
    URLs typed as ``bodycam_briefing`` so the existing agency_ois
    resolver graduates them as bodycam media."""
    seen_ids: List[str] = []
    media_links: List[Dict[str, str]] = []
    for m in _IFRAME_SRC_RE.finditer(html):
        src = m.group(1).replace("&amp;", "&")
        yt_id = _extract_youtube_id(src)
        if yt_id and yt_id not in seen_ids:
            seen_ids.append(yt_id)
            media_links.append(
                {
                    "url": f"https://www.youtube.com/watch?v={yt_id}",
                    "label": "Critical Incident Briefing video (Phoenix Newsroom)",
                    "type": "bodycam_briefing",
                }
            )
    return media_links


def _extract_youtube_id(src: str) -> Optional[str]:
    """Pull a YouTube video ID out of an embed URL. Returns None for
    non-YouTube iframes."""
    m = _YOUTUBE_EMBED_ID_RE.search(src or "")
    return m.group(1) if m else None


# ---- narrative ------------------------------------------------------


_BWC_PRESENCE_RE = re.compile(
    r"\b(?:BWC|body[-\s]worn\s+camera)\b",
    re.IGNORECASE,
)


def _html_mentions_bwc(html: str) -> bool:
    """True iff the source HTML literally mentions Body-Worn Camera /
    BWC language. Matches the bare acronym ``BWC`` or any case
    variation of ``body-worn camera`` / ``body worn camera``.

    Used to guard the narrative builder so the BWC sentence is only
    emitted when the source page actually documents BWC footage. Phoenix
    CIB pages vary: ``3286.html`` mentions BWC; ``3369.html`` does not.
    The narrative must not invent source-page text either way.
    """
    if not html or not isinstance(html, str):
        return False
    return bool(_BWC_PRESENCE_RE.search(html))


def _build_narrative(
    *,
    title: str,
    release_date: Optional[str],
    incident_date: Optional[str],
    bwc_mentioned: bool,
) -> str:
    """Concise narrative blurb that surfaces the agency, the page
    title, and the parsed dates. Identity scoring later phrase-matches
    these against ``case_identity.agency`` / ``incident_date`` /
    ``case_numbers``, so this string is the operator-readable bridge
    between the saved HTML and the assembled CasePacket.

    The closing sentence is conditional on whether the source HTML
    actually mentions BWC / Body-Worn Camera, so the narrative never
    invents source-page text. Pages without BWC language get a neutral
    publishing-channel sentence instead.
    """
    parts = ["Phoenix Police Department Critical Incident Briefing."]
    parts.append(f"Title: {title}.")
    if incident_date:
        parts.append(f"Incident date: {incident_date}.")
    if release_date:
        parts.append(f"Release date: {release_date}.")
    if bwc_mentioned:
        parts.append(
            "The briefing video includes Body-Worn Camera (BWC) footage and is "
            "published on the official City of Phoenix Newsroom."
        )
    else:
        parts.append(
            "The briefing video is published on the official City of "
            "Phoenix Newsroom."
        )
    return " ".join(parts)


__all__ = [
    "extract_phoenix_newsroom_to_agency_ois",
    "is_phoenix_newsroom_article_detail",
]
