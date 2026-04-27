"""
jurisdiction_filter.py — demote sources whose geographic context conflicts
with the case's jurisdiction.

Purpose: targets the recurring false-HIGH failure mode where INSUFFICIENT
cases (e.g. Braulio Gonzalez/Miami, Joshua Carrier/Colorado Springs) accumulate
30-50 sources from name collisions in OTHER cities, get high_relevance >= 3,
and trigger HIGH confidence — when ground truth says LOW.

Approach:
  For each source, scan description + URL for US city/state mentions.
  - If we find OTHER state(s) and the case's state is also mentioned: soft demote (×0.7)
    — could be a comparison or reference to multiple jurisdictions.
  - If we find OTHER state(s) and case's state is NOT mentioned: hard demote (×0.2)
    — the source is almost certainly about a different place / different person.
  - Otherwise: leave unchanged.

Demotion only — sources stay in the pool (preserves recall). The score drop
pushes false-positive sources below the high_relevance threshold (0.5),
removing their contribution to assess_confidence's HIGH-tier triggers.

Toggle: FLAMEON_USE_JURISDICTION_FILTER=0 to disable. Default ON.

Why rule-based first: deterministic, $0/case, easy to reason about. An
LLM-based version (one call per case to classify cross-jurisdictional
sources) is a possible follow-up if rule-based misses subtle cases.
"""

import re

# All 50 states + DC, lowercase. Used for whole-word matches against source text.
US_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west virginia", "wisconsin", "wyoming", "district of columbia",
}

# Major US cities mapped to their state, for cross-state collision detection.
# Coverage: top 30+ population centers + the cities present in calibration data.
# Adding a city here means name-matching against it will tag the source with
# its state. NOT exhaustive — we prefer false-negatives (missed conflicts)
# over false-positives (incorrect demotions of legitimate sources).
MAJOR_CITIES_BY_STATE = {
    "alabama": {"birmingham", "montgomery", "huntsville", "mobile"},
    "arizona": {"phoenix", "tucson", "mesa", "chandler", "scottsdale", "glendale", "tempe", "peoria", "buckeye"},
    "arkansas": {"little rock"},
    "california": {"los angeles", "san diego", "san jose", "san francisco", "fresno",
                   "sacramento", "long beach", "oakland", "bakersfield", "anaheim",
                   "santa ana", "riverside", "stockton", "irvine", "berkeley", "fremont"},
    "colorado": {"denver", "colorado springs", "aurora", "fort collins", "lakewood", "boulder"},
    "connecticut": {"hartford", "new haven", "stamford", "bridgeport"},
    "delaware": {"wilmington"},
    "florida": {"jacksonville", "miami", "tampa", "orlando", "st petersburg", "hialeah",
                "tallahassee", "fort lauderdale", "port st lucie", "cape coral", "aventura"},
    "georgia": {"atlanta", "columbus", "augusta", "savannah", "macon"},
    "hawaii": {"honolulu"},
    "idaho": {"boise"},
    "illinois": {"chicago", "aurora", "springfield", "peoria", "naperville", "joliet"},
    "indiana": {"indianapolis", "fort wayne", "evansville", "south bend"},
    "iowa": {"des moines", "cedar rapids", "davenport"},
    "kansas": {"wichita", "topeka", "kansas city", "overland park"},
    "kentucky": {"louisville", "lexington"},
    "louisiana": {"new orleans", "baton rouge", "shreveport"},
    "maryland": {"baltimore"},
    "massachusetts": {"boston", "worcester", "springfield", "cambridge"},
    "michigan": {"detroit", "grand rapids", "warren", "ann arbor", "lansing", "flint"},
    "minnesota": {"minneapolis", "st paul"},
    "mississippi": {"jackson"},
    "missouri": {"kansas city", "st louis", "springfield"},
    "nebraska": {"omaha", "lincoln"},
    "nevada": {"las vegas", "reno", "henderson"},
    "new jersey": {"newark", "jersey city", "paterson"},
    "new mexico": {"albuquerque", "santa fe"},
    "new york": {"new york", "brooklyn", "queens", "buffalo", "rochester", "syracuse", "albany"},
    "north carolina": {"charlotte", "raleigh", "greensboro", "durham"},
    "ohio": {"columbus", "cleveland", "cincinnati", "toledo", "akron", "dayton"},
    "oklahoma": {"oklahoma city", "tulsa", "norman"},
    "oregon": {"portland", "salem", "eugene"},
    "pennsylvania": {"philadelphia", "pittsburgh", "allentown"},
    "south carolina": {"charleston", "columbia"},
    "tennessee": {"nashville", "memphis", "knoxville", "chattanooga"},
    "texas": {"houston", "san antonio", "dallas", "austin", "fort worth", "el paso",
              "arlington", "corpus christi", "plano", "lubbock", "irving"},
    "utah": {"salt lake city", "provo"},
    "virginia": {"virginia beach", "richmond", "norfolk", "arlington", "alexandria"},
    "washington": {"seattle", "spokane", "tacoma", "bellevue"},
    "wisconsin": {"milwaukee", "madison"},
}

# Reverse lookup: city → state. Used to identify which state a city mention implies.
_CITY_TO_STATE = {}
for _state, _cities in MAJOR_CITIES_BY_STATE.items():
    for _c in _cities:
        # If a city is in multiple states (e.g. Springfield, Aurora), keep the largest.
        # The list above is roughly population-ordered so first wins is ~OK.
        _CITY_TO_STATE.setdefault(_c, _state)

# State abbrev → full name. Used to expand 2-letter abbreviations in source text.
STATE_ABBREV = {
    "AL": "alabama", "AK": "alaska", "AZ": "arizona", "AR": "arkansas",
    "CA": "california", "CO": "colorado", "CT": "connecticut", "DE": "delaware",
    "FL": "florida", "GA": "georgia", "HI": "hawaii", "ID": "idaho",
    "IL": "illinois", "IN": "indiana", "IA": "iowa", "KS": "kansas",
    "KY": "kentucky", "LA": "louisiana", "ME": "maine", "MD": "maryland",
    "MA": "massachusetts", "MI": "michigan", "MN": "minnesota", "MS": "mississippi",
    "MO": "missouri", "MT": "montana", "NE": "nebraska", "NV": "nevada",
    "NH": "new hampshire", "NJ": "new jersey", "NM": "new mexico", "NY": "new york",
    "NC": "north carolina", "ND": "north dakota", "OH": "ohio", "OK": "oklahoma",
    "OR": "oregon", "PA": "pennsylvania", "RI": "rhode island",
    "SC": "south carolina", "SD": "south dakota", "TN": "tennessee", "TX": "texas",
    "UT": "utah", "VT": "vermont", "VA": "virginia", "WA": "washington",
    "WV": "west virginia", "WI": "wisconsin", "WY": "wyoming", "DC": "district of columbia",
}
# Abbreviations only count when in standard "City, ST" or " ST " contexts —
# avoids false matches against words like "txt", "atx" that contain state codes.
_ABBREV_PATTERN = re.compile(r",\s*([A-Z]{2})\b|\s([A-Z]{2})\s")

# National-tier news/legal-publication domains that routinely cover cases from
# multiple states. Their coverage of one case often mentions OTHER places by
# context (precedent, comparison, prior reporting) — that's not a name-collision
# signal. We avoid hard-demoting these to ×0.2; they get the soft ×0.7 instead.
NATIONAL_NEWS_DOMAINS = {
    "nytimes.com", "washingtonpost.com", "wsj.com", "usatoday.com",
    "cnn.com", "foxnews.com", "nbcnews.com", "cbsnews.com", "abcnews.go.com",
    "msnbc.com", "bbc.com", "bbc.co.uk", "reuters.com", "apnews.com",
    "bloomberg.com", "newsweek.com", "time.com", "theguardian.com",
    "huffpost.com", "vice.com", "vox.com", "theatlantic.com", "newyorker.com",
    "politico.com", "thehill.com", "dailybeast.com", "businessinsider.com",
    "courttv.com", "lawandcrime.com", "abovethelaw.com",
    "courtlistener.com", "casetext.com", "justia.com", "findlaw.com",
    "pacer.gov", "documentcloud.org", "scribd.com",
    "youtube.com", "youtu.be",  # platform domain — content origin is in the channel/title
    "wikipedia.org", "en.wikipedia.org",
    "reddit.com", "old.reddit.com",
    "podcasts.apple.com", "spotify.com",  # podcast platforms
}


def _is_national_news(url):
    """True if the URL is from a national-tier publication or cross-jurisdictional platform."""
    if not url:
        return False
    url_lower = url.lower()
    return any(d in url_lower for d in NATIONAL_NEWS_DOMAINS)


def _detect_states_in_text(text):
    """Return set of US state names mentioned in text (lowercase)."""
    text_lower = text.lower()
    found = set()
    # Full state names (whole-word match)
    for state in US_STATES:
        if re.search(rf"\b{re.escape(state)}\b", text_lower):
            found.add(state)
    # Major-city → state implication
    for city, state in _CITY_TO_STATE.items():
        if re.search(rf"\b{re.escape(city)}\b", text_lower):
            found.add(state)
    # State abbreviations in standard contexts
    for m in _ABBREV_PATTERN.finditer(text):  # use ORIGINAL case for abbreviations
        abbrev = m.group(1) or m.group(2)
        if abbrev and abbrev in STATE_ABBREV:
            found.add(STATE_ABBREV[abbrev])
    return found


def _normalize_state(state_str):
    """Convert various state representations to lowercase full name."""
    if not state_str:
        return ""
    s = state_str.strip()
    if s.upper() in STATE_ABBREV:
        return STATE_ABBREV[s.upper()]
    s_lower = s.lower()
    if s_lower in US_STATES:
        return s_lower
    # Strip stray punctuation/parentheticals like "Florida (federal jurisdiction)"
    s_clean = re.sub(r"\s*\([^)]*\)\s*", " ", s_lower).strip()
    if s_clean in US_STATES:
        return s_clean
    return s_lower  # best effort — matchers will still try


def detect_conflict(source, case_state, case_city):
    """
    Return a relevance multiplier for a single source based on jurisdiction
    conflict detection.

    Returns:
        1.0  no conflict (default — keep score as-is)
        0.7  soft conflict — source mentions BOTH case state and another state
             (likely a comparison or multi-jurisdiction context)
        0.2  hard conflict — source mentions another state and NOT the case state
             (almost certainly about a different place / wrong person)
    """
    text = (source.get("description", "") + " " + source.get("url", "")).lower()
    if not text.strip():
        return 1.0

    states_in_source = _detect_states_in_text(
        source.get("description", "") + " " + source.get("url", "")
    )
    if not states_in_source:
        return 1.0  # No geographic signal — leave alone

    case_state_norm = case_state.lower() if case_state else ""
    case_city_lower = case_city.lower() if case_city else ""

    # Did the source reference the case's jurisdiction at all?
    case_match = False
    if case_state_norm and case_state_norm in states_in_source:
        case_match = True
    if case_city_lower and re.search(rf"\b{re.escape(case_city_lower)}\b", text):
        case_match = True

    other_states = states_in_source - {case_state_norm}
    if not other_states:
        return 1.0  # Only matched on case state — fine

    if case_match:
        return 0.7  # Soft demote: ambiguous (mentions both)
    return 0.2  # Hard demote: about another place


def apply_jurisdiction_filter(sources, jurisdiction, parse_jurisdiction_fn=None):
    """
    Adjust relevance_score on each source based on geographic conflict.
    Sources are NOT removed — only down-weighted, preserving recall.

    Adds diagnostic fields on demoted sources:
      _jurisdiction_filter_mult: the multiplier applied (0.2 or 0.7)
      _jurisdiction_pre_score: the pre-filter relevance_score

    Args:
        sources: list of source dicts with `relevance_score`, `description`, `url`
        jurisdiction: case jurisdiction string (e.g. "Miami, Miami-Dade, Florida")
        parse_jurisdiction_fn: optional callable to parse jurisdiction. Defaults to
            a built-in parser; pass research.parse_jurisdiction for consistency.

    Returns:
        The same sources list with relevance_scores adjusted in place.
    """
    if not sources or not jurisdiction:
        return sources

    if parse_jurisdiction_fn is not None:
        parsed = parse_jurisdiction_fn(jurisdiction)
    else:
        # Minimal fallback parser — just splits on commas
        parts = [p.strip() for p in jurisdiction.split(",")]
        parsed = {
            "city": parts[0] if parts else "",
            "state": parts[-1] if len(parts) >= 2 else "",
        }

    case_state = _normalize_state(parsed.get("state", ""))
    case_city = parsed.get("city", "")
    if not case_state and not case_city:
        return sources  # Can't filter without a known jurisdiction

    n_demoted = 0
    for src in sources:
        mult = detect_conflict(src, case_state, case_city)
        if mult < 1.0:
            src["_jurisdiction_filter_mult"] = mult
            src["_jurisdiction_pre_score"] = src.get("relevance_score", 0.0)
            src["relevance_score"] = src.get("relevance_score", 0.0) * mult
            n_demoted += 1

    # Annotate the result for debug
    if n_demoted > 0:
        sources[0].setdefault("_filter_run_stats", {})
        sources[0]["_filter_run_stats"] = {
            "n_demoted": n_demoted,
            "n_total": len(sources),
            "case_state": case_state,
            "case_city": case_city,
        }
    return sources
