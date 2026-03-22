"""Stage 3A — Link Discovery (State-Aware Asset Miner).

Only runs after validation_status = validated_closed.
Discovers and inventories links. Does NOT download anything.

Now includes Phase 0 (Case Search Enrichment) which queries
CourtListener to extract case numbers, then uses those numbers
for surgical state-portal artifact queries.
"""

import re
import time
from dataclasses import asdict

import requests

from .case_search import (
    _is_operation_name,
    build_case_number_queries,
    build_direct_portal_urls,
    enrich_case,
    extract_case_numbers,
)
from .logger import get_logger
from .models import CaseCandidate, DiscoveredLink, LinkInventory, SourceRank

log = get_logger()

# --- State-specific court portal search strategies ---

STATE_SEARCH_STRATEGIES = {
    "FL": {
        "primary_target": "County Clerk of Courts",
        "portal_domains": [
            "civitekflorida.com",
            "myfloridacounty.com",
            "myeclerk.com",
        ],
        "asset_goals": ["Judgment and Sentence", "Information/Indictment"],
        "query_templates": [
            '"{name}" site:myfloridacounty.com',
            '"{name}" {county} clerk court records florida',
            '"{name}" {county} florida "judgment and sentence"',
            '"{name}" {county} florida court case',
        ],
    },
    "OH": {
        "primary_target": "Municipal and Common Pleas Courts",
        "portal_domains": [
            "courtclerk.org",
            "clerk.cuyahogacounty.us",
            "courtrecords.ohio.gov",
        ],
        "asset_goals": ["Final Appealable Order", "Sentencing Entry"],
        "query_templates": [
            '"{name}" {county} ohio clerk courts case',
            '"{name}" {county} ohio "sentencing entry"',
            '"{name}" ohio common pleas court case',
            '"{name}" {city} ohio municipal court docket',
        ],
    },
    "AZ": {
        "primary_target": "Arizona Judicial Branch (eAccess)",
        "portal_domains": [
            "azcourts.gov",
            "superiorcourt.maricopa.gov",
            "tucsonaz.gov/courts",
        ],
        "asset_goals": ["Minute Entry", "Indictment"],
        "query_templates": [
            '"{name}" site:azcourts.gov',
            '"{name}" {county} arizona court case docket',
            '"{name}" maricopa superior court docket',
            '"{name}" arizona "minute entry" sentencing',
        ],
    },
    "TX": {
        "primary_target": "District Clerk (Felony) / County Clerk (Misdemeanor)",
        "portal_domains": [
            "hcdistrictclerk.com",
            "bexar.org",
            "tarrantcounty.com",
        ],
        "asset_goals": ["Judgment of Conviction"],
        "query_templates": [
            '"{name}" {county} texas district clerk criminal',
            '"{name}" {county} texas "judgment of conviction"',
            '"{name}" texas court case criminal records',
            '"{name}" {county} texas court docket',
        ],
    },
}

# County mappings for major cities
CITY_TO_COUNTY = {
    # Florida
    "miami": "Miami-Dade",
    "jacksonville": "Duval",
    "orlando": "Orange",
    "west palm beach": "Palm Beach",
    "fort myers": "Lee",
    "defuniak springs": "Walton",
    "bartow": "Polk",
    "deland": "Volusia",
    "new port richey": "Pasco",
    "tallahassee": "Leon",
    "tampa": "Hillsborough",
    "largo": "Pinellas",
    # Arizona
    "phoenix": "Maricopa",
    "tucson": "Pima",
    "mesa": "Maricopa",
    "tempe": "Maricopa",
    "glendale": "Maricopa",
    "scottsdale": "Maricopa",
    "chandler": "Maricopa",
    "florence": "Pinal",
    # Texas
    "houston": "Harris",
    "dallas": "Dallas",
    "fort worth": "Tarrant",
    "san antonio": "Bexar",
    "austin": "Travis",
    "arlington": "Tarrant",
    "lubbock": "Lubbock",
    "garland": "Dallas",
    # Ohio
    "cleveland": "Cuyahoga",
    "cincinnati": "Hamilton",
    "toledo": "Lucas",
    "akron": "Summit",
    "dayton": "Montgomery",
    "columbus": "Franklin",
    "canton": "Stark",
    "springfield": "Clark",
}


def _get_county(city: str) -> str:
    """Look up the county for a known city."""
    return CITY_TO_COUNTY.get(city.lower().strip(), "")


def _brave_search(query: str, api_key: str, count: int = 10) -> list[dict]:
    """Execute a Brave Search API query with retry on rate limits."""
    import time as _time

    # Global rate limiter: ensure at least 1.1s between calls
    now = _time.monotonic()
    last = getattr(_brave_search, "_last_call", 0)
    if now - last < 1.1:
        _time.sleep(1.1 - (now - last))
    _brave_search._last_call = _time.monotonic()

    max_retries = 3
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(
                "https://api.search.brave.com/res/v1/web/search",
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip",
                    "X-Subscription-Token": api_key,
                },
                params={"q": query, "count": count},
                timeout=15,
            )
            if resp.status_code == 429 and attempt < max_retries:
                wait = 2 ** (attempt + 1)  # 2s, 4s, 8s
                log.warning("Brave rate-limited (429), retrying in %ds...", wait)
                _time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            results = []
            for item in data.get("web", {}).get("results", []):
                results.append({
                    "title": item.get("title", ""),
                    "url": item.get("url", ""),
                    "description": item.get("description", ""),
                })
            return results
        except Exception as e:
            if attempt < max_retries and "429" in str(e):
                wait = 2 ** (attempt + 1)
                log.warning("Brave rate-limited, retrying in %ds...", wait)
                _time.sleep(wait)
                continue
            log.error("Brave search failed for query '%s': %s", query, e)
            return []
    return []


# URLs that should be excluded entirely — never useful for case research
JUNK_URL_DOMAINS = [
    "linkedin.com", "instagram.com", "tiktok.com", "pinterest.com",
    "yelp.com", "glassdoor.com", "indeed.com", "zillow.com",
    # Never useful for criminal case research
    "imdb.com", "gettyimages.com", "wikipedia.org", "legacy.com",
    "honorstates.org", "pulitzercenter.org", "ancestry.com",
    "findagrave.com", "whitepages.com", "spokeo.com", "beenverified.com",
]


def _is_junk_url(url: str) -> bool:
    """Check if a URL is from a domain that never has useful case data."""
    url_lower = url.lower()
    return any(d in url_lower for d in JUNK_URL_DOMAINS)


def _classify_link(url: str, title: str, description: str) -> tuple[str, str]:
    """Classify a discovered link into (source_class, link_type)."""
    url_lower = url.lower()
    text = f"{title} {description}".lower()

    # Source class
    if any(d in url_lower for d in [".gov", "courts.", "judiciary."]):
        source_class = SourceRank.COURT_GOV.value
    elif any(d in url_lower for d in ["clerk.", "docket", "courtrecords", "caseinfo", "civiltek", "myfloridacounty"]):
        source_class = SourceRank.COUNTY_CLERK.value
    elif any(d in url_lower for d in ["sheriff", "police", "pd.org"]):
        source_class = SourceRank.LE_RELEASE.value
    elif any(d in url_lower for d in [
        "reddit.com", "twitter.com", "x.com", "facebook.com", "wikipedia.org",
        "instagram.com", "linkedin.com", "tiktok.com", "youtube.com",
    ]):
        source_class = SourceRank.OTHER.value
    else:
        source_class = SourceRank.LOCAL_NEWS.value

    # Link type
    if any(w in text for w in ["docket", "case search", "case number", "case detail"]):
        link_type = "court_docket"
    elif any(w in text for w in ["sentencing order", "sentencing entry", "judgment and sentence", "judgment of conviction"]):
        link_type = "sentencing_order"
    elif any(w in text for w in ["judgment", "final order", "final appealable"]):
        link_type = "judgment"
    elif any(w in text for w in ["indictment", "information", "grand jury"]):
        link_type = "indictment"
    elif any(w in text for w in ["minute entry"]):
        link_type = "minute_entry"
    elif any(w in text for w in ["interrogation", "interview room", "confession"]):
        link_type = "interrogation"
    elif any(w in text for w in ["body cam", "bodycam", "bwc", "body-worn", "critical incident"]):
        # Only classify as bwc_video if the URL is on a video-hosting domain;
        # otherwise it's a news article that merely mentions BWC footage.
        _VIDEO_DOMAINS = ["youtube.com", "youtu.be", "vimeo.com", "dailymotion.com", "rumble.com"]
        _VIDEO_EXTS = [".mp4", ".m3u8", ".webm", ".avi", ".mov"]
        _VIDEO_PATHS = ["/video/", "/media/", "/videos/", "/watch/", "/player/"]
        is_video_url = (
            any(d in url_lower for d in _VIDEO_DOMAINS)
            or any(url_lower.endswith(ext) for ext in _VIDEO_EXTS)
            or (any(d in url_lower for d in [".gov", "sheriff", "police"]) and any(p in url_lower for p in _VIDEO_PATHS))
        )
        link_type = "bwc_video" if is_video_url else "news_article"
    elif any(w in text for w in ["foia", "public records"]):
        link_type = "foia"
    elif url_lower.endswith(".pdf"):
        link_type = "pdf_document"
    else:
        link_type = "news_article"

    return source_class, link_type


# ---------------------------------------------------------------------------
# State-relevance check for corroboration (prevents wrong-person matches)
# ---------------------------------------------------------------------------

# Map state abbreviations to identifiers commonly found in .gov URLs and text.
# Includes abbreviation, full name, and common URL fragments.
_STATE_IDENTIFIERS: dict[str, list[str]] = {
    "AL": ["alabama", "/al/", ".al."],
    "AK": ["alaska", "/ak/", ".ak."],
    "AZ": ["arizona", "/az/", ".az.", "maricopa", "pima", "coconino"],
    "AR": ["arkansas", "/ar/", ".ar."],
    "CA": ["california", "/ca/", ".ca.", "losangeles", "sandiego", "sacramento"],
    "CO": ["colorado", "/co/", ".co."],
    "CT": ["connecticut", "/ct/", ".ct."],
    "DE": ["delaware", "/de/", ".de."],
    "FL": ["florida", "/fl/", ".fl.", "miami", "broward", "hillsborough", "orange",
           "duval", "palm beach", "pinellas", "myfloridacounty"],
    "GA": ["georgia", "/ga/", ".ga."],
    "HI": ["hawaii", "/hi/", ".hi."],
    "ID": ["idaho", "/id/", ".id."],
    "IL": ["illinois", "/il/", ".il.", "cook county"],
    "IN": ["indiana", "/in/", ".in."],
    "IA": ["iowa", "/ia/", ".ia."],
    "KS": ["kansas", "/ks/", ".ks."],
    "KY": ["kentucky", "/ky/", ".ky."],
    "LA": ["louisiana", "/la/", ".la."],
    "ME": ["maine", "/me/", ".me."],
    "MD": ["maryland", "/md/", ".md."],
    "MA": ["massachusetts", "/ma/", ".ma."],
    "MI": ["michigan", "/mi/", ".mi."],
    "MN": ["minnesota", "/mn/", ".mn."],
    "MS": ["mississippi", "/ms/", ".ms."],
    "MO": ["missouri", "/mo/", ".mo."],
    "MT": ["montana", "/mt/", ".mt."],
    "NE": ["nebraska", "/ne/", ".ne."],
    "NV": ["nevada", "/nv/", ".nv."],
    "NH": ["newhampshire", "new hampshire", "/nh/", ".nh."],
    "NJ": ["newjersey", "new jersey", "/nj/", ".nj."],
    "NM": ["newmexico", "new mexico", "/nm/", ".nm."],
    "NY": ["newyork", "new york", "/ny/", ".ny."],
    "NC": ["northcarolina", "north carolina", "/nc/", ".nc."],
    "ND": ["northdakota", "north dakota", "/nd/", ".nd."],
    "OH": ["ohio", "/oh/", ".oh.", "franklin", "cuyahoga", "hamilton"],
    "OK": ["oklahoma", "/ok/", ".ok."],
    "OR": ["oregon", "/or/", ".or."],
    "PA": ["pennsylvania", "/pa/", ".pa."],
    "RI": ["rhodeisland", "rhode island", "/ri/", ".ri."],
    "SC": ["southcarolina", "south carolina", "/sc/", ".sc."],
    "SD": ["southdakota", "south dakota", "/sd/", ".sd."],
    "TN": ["tennessee", "/tn/", ".tn."],
    "TX": ["texas", "/tx/", ".tx.", "harris county", "dallas", "tarrant", "bexar", "travis"],
    "UT": ["utah", "/ut/", ".ut."],
    "VT": ["vermont", "/vt/", ".vt."],
    "VA": ["virginia", "/va/", ".va."],
    "WA": ["washington", "/wa/", ".wa.", "snohomish", "kingcounty", "pierce"],
    "WV": ["westvirginia", "west virginia", "/wv/", ".wv."],
    "WI": ["wisconsin", "/wi/", ".wi."],
    "WY": ["wyoming", "/wy/", ".wy."],
}

# Federal domains that are state-neutral (never flag as wrong-state)
_FEDERAL_DOMAINS = [
    "justice.gov", "uscourts.gov", "courtlistener.com", "ice.gov",
    "fbi.gov", "dea.gov", "atf.gov", "bop.gov", "usmarshals.gov",
    "pacer.gov", "supremecourt.gov",
]


def _check_state_relevance(
    url: str, title: str, description: str, candidate_state: str,
) -> tuple[bool, str]:
    """Check whether a discovered link is relevant to the candidate's state.

    Returns (is_relevant, reason). A link is flagged as irrelevant when it
    clearly belongs to a DIFFERENT state than the candidate's.

    Federal domains (justice.gov, uscourts.gov, etc.) get additional scrutiny:
    if the snippet mentions a specific state/city that doesn't match, we flag it.
    """
    url_lower = url.lower()
    text_lower = f"{title} {description}".lower()
    candidate_state = candidate_state.upper()

    # Non-.gov links don't get official corroboration anyway — skip check
    is_gov = any(d in url_lower for d in [".gov", "courts.", "judiciary.", "courtlistener.com"])
    if not is_gov:
        return True, "non-gov"

    # Get the candidate state's identifiers
    own_ids = _STATE_IDENTIFIERS.get(candidate_state, [])

    # Check if this is a federal domain
    is_federal = any(fd in url_lower for fd in _FEDERAL_DOMAINS)

    if not is_federal:
        # State/county .gov domain — check if URL belongs to a different state
        for other_state, other_ids in _STATE_IDENTIFIERS.items():
            if other_state == candidate_state:
                continue
            for oid in other_ids:
                if oid in url_lower:
                    # URL contains another state's identifier
                    # But also check if it's a substring match for our state
                    if not any(mid in url_lower for mid in own_ids):
                        return False, f"URL belongs to {other_state} ('{oid}' in URL), case is in {candidate_state}"
        return True, "state-url-ok"

    # Federal domain — also check URL for US Attorney district codes (usao-sdga = GA, usao-ndtx = TX, etc.)
    # Format: usao-{n|s|m|e|w|c|d}{d|}{state_abbrev}  (e.g., usao-sdga, usao-ndtx, usao-edny)
    usao_match = re.search(r"usao-[newscdm]{1,2}([a-z]{2})", url_lower)
    if usao_match:
        district_state = usao_match.group(1).upper()
        if district_state != candidate_state and district_state in _STATE_IDENTIFIERS:
            return False, f"Federal URL is USAO district for {district_state}, case is in {candidate_state}"

    # Check if the snippet text mentions a different state
    # Only flag if we find ANOTHER state mentioned AND our state is NOT mentioned
    own_state_mentioned = any(mid in text_lower for mid in own_ids)

    if own_state_mentioned:
        return True, "federal-own-state-in-text"

    # Check if another state is clearly mentioned in the text
    for other_state, other_ids in _STATE_IDENTIFIERS.items():
        if other_state == candidate_state:
            continue
        # Only check full state names to avoid false positives from abbreviation fragments
        state_names = [oid for oid in other_ids if len(oid) > 4 and "/" not in oid and "." not in oid]
        for sname in state_names:
            if sname in text_lower:
                return False, f"Federal source mentions '{sname}' ({other_state}), not {candidate_state}"

    # CourtListener: if neither our state nor any other state is mentioned,
    # require at least our city/county in the text to accept as relevant.
    # Generic matches like "Wright v. State" with no location are too ambiguous.
    if "courtlistener.com" in url_lower:
        return False, f"CourtListener link does not mention {candidate_state} or any identifiable jurisdiction"

    return True, "federal-no-other-state"


def discover_court_links(
    candidate: CaseCandidate,
    brave_api_key: str,
    rate_limit: float = 1.0,
) -> list[DiscoveredLink]:
    """Discover court/docket links using state-aware search strategies."""
    state = candidate.state.upper()
    strategy = STATE_SEARCH_STRATEGIES.get(state)
    if not strategy:
        log.warning("No search strategy for state %s", state)
        return []

    name = candidate.suspect_name
    if not name:
        log.warning("No suspect name for court link discovery: %s", candidate.case_id)
        return []

    if _is_operation_name(name):
        log.info("Skipping court link discovery for operation name: %s (%s)", name, candidate.case_id)
        return []

    county = _get_county(candidate.city) if candidate.city else ""
    city = candidate.city or ""

    links = []
    seen_urls = set()

    for template in strategy["query_templates"]:
        query = template.format(name=name, county=county, city=city)
        if not county:
            query = query.replace("  ", " ").strip()

        log.debug("Court discovery query: %s", query)
        results = _brave_search(query, brave_api_key)

        for r in results:
            url = r["url"]
            if url in seen_urls or _is_junk_url(url):
                continue
            seen_urls.add(url)

            source_class, link_type = _classify_link(url, r["title"], r["description"])

            # Recommend download for court/clerk PDFs and dockets
            download_rec = source_class in [SourceRank.COURT_GOV.value, SourceRank.COUNTY_CLERK.value]
            official = source_class in [SourceRank.COURT_GOV.value, SourceRank.COUNTY_CLERK.value]

            # State-relevance gate: don't mark as official if it's about a different state
            if official:
                relevant, reason = _check_state_relevance(url, r["title"], r["description"], state)
                if not relevant:
                    log.warning(
                        "[corroboration-rejected] %s for %s: %s | url=%s",
                        candidate.case_id, name, reason, url,
                    )
                    official = False
                    download_rec = False

            links.append(DiscoveredLink(
                url=url,
                source_class=source_class,
                link_type=link_type,
                notes=f"{r['title']}: {r['description'][:200]}",
                download_recommended=download_rec,
                official_corroboration=official,
            ))

        time.sleep(rate_limit)

    return links


def discover_news_links(
    candidate: CaseCandidate,
    brave_api_key: str,
    rate_limit: float = 1.0,
) -> list[DiscoveredLink]:
    """Discover local news links tied to the same case."""
    name = candidate.suspect_name
    city = candidate.city
    state = candidate.state

    is_op = _is_operation_name(name)
    if is_op:
        log.info("Operation-name news search (not a person): %s (%s)", name, candidate.case_id)

    queries = []
    if name and city:
        queries.append(f'"{name}" {city} sentenced convicted')
        queries.append(f'"{name}" {city} case verdict')
    elif name and state:
        queries.append(f'"{name}" {state} sentenced convicted')

    links = []
    seen_urls = set()

    for query in queries[:2]:
        log.debug("News discovery query: %s", query)
        results = _brave_search(query, brave_api_key)

        for r in results:
            url = r["url"]
            if url in seen_urls or _is_junk_url(url):
                continue
            seen_urls.add(url)

            source_class, link_type = _classify_link(url, r["title"], r["description"])

            links.append(DiscoveredLink(
                url=url,
                source_class=source_class,
                link_type=link_type,
                notes=f"{r['title']}: {r['description'][:200]}",
                download_recommended=False,
            ))

        time.sleep(rate_limit)

    return links


def discover_bwc_interrogation_links(
    candidate: CaseCandidate,
    brave_api_key: str,
    rate_limit: float = 1.0,
) -> list[DiscoveredLink]:
    """Discover BWC/interrogation footage links."""
    name = candidate.suspect_name
    agency = candidate.agency_name

    is_op = _is_operation_name(name)
    if is_op:
        log.info("Operation-name BWC search (not a person): %s (%s)", name, candidate.case_id)

    queries = []
    if name:
        queries.append(f'"{name}" interrogation raw footage')
        if agency:
            queries.append(f'{agency} "Critical Incident" "{name}"')
        queries.append(f'"{name}" body camera footage')

    links = []
    seen_urls = set()

    for query in queries[:2]:
        log.debug("BWC/interrogation discovery query: %s", query)
        results = _brave_search(query, brave_api_key)

        for r in results:
            url = r["url"]
            if url in seen_urls or _is_junk_url(url):
                continue
            seen_urls.add(url)

            source_class, link_type = _classify_link(url, r["title"], r["description"])

            links.append(DiscoveredLink(
                url=url,
                source_class=source_class,
                link_type=link_type,
                notes=f"{r['title']}: {r['description'][:200]}",
                download_recommended=(link_type in ["bwc_video", "interrogation"]),
            ))

        time.sleep(rate_limit)

    return links


def discover_case_number_links(
    candidate: CaseCandidate,
    case_numbers: list[str],
    docket_numbers: list[str],
    brave_api_key: str,
    rate_limit: float = 1.0,
) -> list[DiscoveredLink]:
    """Run case-number-enhanced Brave queries targeting state portals.

    Uses case numbers extracted from CourtListener to build
    surgical queries that hit state court portals directly.
    """
    queries = build_case_number_queries(candidate, case_numbers, docket_numbers)
    if not queries:
        return []

    links = []
    seen_urls = set()

    for query in queries:
        log.debug("Case-number query: %s", query)
        results = _brave_search(query, brave_api_key)

        for r in results:
            url = r["url"]
            if url in seen_urls or _is_junk_url(url):
                continue
            seen_urls.add(url)

            source_class, link_type = _classify_link(url, r["title"], r["description"])

            download_rec = source_class in [SourceRank.COURT_GOV.value, SourceRank.COUNTY_CLERK.value]
            official = source_class in [SourceRank.COURT_GOV.value, SourceRank.COUNTY_CLERK.value]

            # State-relevance gate
            if official:
                relevant, reason = _check_state_relevance(
                    url, r["title"], r["description"], candidate.state,
                )
                if not relevant:
                    log.warning(
                        "[corroboration-rejected] %s: %s | url=%s",
                        candidate.case_id, reason, url,
                    )
                    official = False
                    download_rec = False

            links.append(DiscoveredLink(
                url=url,
                source_class=source_class,
                link_type=link_type,
                notes=f"[case-num] {r['title']}: {r['description'][:200]}",
                download_recommended=download_rec,
                official_corroboration=official,
            ))

        time.sleep(rate_limit)

    return links


def extract_individual_names_from_news(
    news_links: list[DiscoveredLink],
    operation_name: str,
) -> list[str]:
    """Extract individual defendant names from news snippets about an operation.

    News articles about operations like "Operation Community Shield" often
    mention individual defendants by name. This function scans the snippet
    text (stored in link.notes) for name patterns.

    Returns deduplicated list of extracted names.
    """
    if not news_links:
        return []

    # Combine all snippet text
    text = " ".join(
        link.notes if isinstance(link, DiscoveredLink) else link.get("notes", "")
        for link in news_links
    )

    # Patterns for individual names in news about operations/stings
    name_patterns = [
        # "John Doe, 34, was charged" / "John Doe, 34, of City"
        r"([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,}),?\s+\d{1,2},?\s+(?:was|were|of|has|is|charged|arrested|sentenced|convicted|pleaded)",
        # "arrested John Doe" / "charged John Doe"
        r"(?:arrested|charged|convicted|sentenced|indicted)\s+([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})",
        # "defendants X, Y, and Z" / "suspects X, Y, and Z"
        r"(?:defendants?|suspects?|individuals?)\s+([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})",
    ]

    names = []
    seen = set()

    # Clean the operation name for comparison
    op_lower = operation_name.lower().strip() if operation_name else ""

    for pattern in name_patterns:
        for match in re.finditer(pattern, text):
            name = match.group(1).strip()
            name_lower = name.lower()
            # Skip if it matches the operation name
            if op_lower and name_lower in op_lower:
                continue
            # Skip common false positives
            if name_lower in ("the police", "the sheriff", "the court", "the state",
                              "the department", "the office", "the suspect"):
                continue
            parts = name.split()
            if len(parts) >= 2 and len(name) > 5 and name_lower not in seen:
                seen.add(name_lower)
                names.append(name)

    return names


def run_discovery(
    candidate: CaseCandidate,
    brave_api_key: str,
    rate_limit: float = 1.0,
    courtlistener_api_key: str = "",
    caselaw_api_key: str = "",
) -> LinkInventory:
    """Run full link discovery for a validated closed case.

    Phases:
        0. Case Search Enrichment — CourtListener API queries to extract
           case numbers, docket numbers, citations, and direct document
           links from the RECAP archive.
        1. Court/docket links — state-aware Brave searches (existing).
        1b. Case-number queries — surgical Brave queries using case numbers
            from Phase 0 (NEW).
        2. News articles.
        3. BWC/interrogation footage.

    Returns a LinkInventory with all discovered links.
    """
    log.info("Starting link discovery for %s (%s)", candidate.case_id, candidate.suspect_name)

    all_links = []
    enrichment_links = []
    case_num_links = []

    # Phase 0: Case Search Enrichment (CourtListener)
    enrichment = {"case_numbers": [], "docket_numbers": [], "citations": []}
    if courtlistener_api_key or caselaw_api_key:
        log.info("Phase 0: Case search enrichment (CourtListener)")
        enrichment = enrich_case(
            candidate,
            courtlistener_api_key=courtlistener_api_key,
            caselaw_api_key=caselaw_api_key,
            rate_limit=max(rate_limit, 2.0),  # Be conservative with API limits
        )

        # Collect all links from enrichment
        for key in ["cl_opinion_links", "cl_docket_links", "cl_document_links", "cap_links"]:
            enrichment_links.extend(enrichment.get(key, []))

        all_links.extend(enrichment_links)
        log.info(
            "Phase 0 complete: %d links, %d case numbers, %d docket numbers",
            len(enrichment_links),
            len(enrichment.get("case_numbers", [])),
            len(enrichment.get("docket_numbers", [])),
        )

    # Phase 0b: Direct portal URL construction (zero API cost)
    case_numbers = enrichment.get("case_numbers", [])
    docket_numbers = enrichment.get("docket_numbers", [])
    # Extract docket IDs from CL docket links for federal URL construction
    docket_ids = []
    for dl in enrichment.get("cl_docket_links", []):
        url = dl.url if isinstance(dl, DiscoveredLink) else dl.get("url", "")
        # Extract docket ID from CourtListener URL: /docket/12345/...
        if "/docket/" in url:
            parts = url.split("/docket/")
            if len(parts) > 1:
                did = parts[1].strip("/").split("/")[0]
                if did.isdigit():
                    docket_ids.append(int(did))

    direct_urls = build_direct_portal_urls(
        candidate, case_numbers, docket_numbers, docket_ids=docket_ids,
    )
    all_links.extend(direct_urls)
    log.info("Phase 0b complete: %d direct portal URLs constructed", len(direct_urls))

    # Phase 1: Court/docket links via Brave (highest priority)
    court_links = discover_court_links(candidate, brave_api_key, rate_limit)
    all_links.extend(court_links)
    log.info("Found %d court/docket links", len(court_links))

    # Phase 1b: Case-number-enhanced queries (if we have case numbers)
    if case_numbers or docket_numbers:
        log.info("Phase 1b: Case-number-enhanced queries (%d numbers)", len(case_numbers) + len(docket_numbers))
        case_num_links = discover_case_number_links(
            candidate, case_numbers, docket_numbers, brave_api_key, rate_limit,
        )
        all_links.extend(case_num_links)
        log.info("Found %d case-number links", len(case_num_links))

    # Phase 2: News articles
    news_links = discover_news_links(candidate, brave_api_key, rate_limit)
    all_links.extend(news_links)
    log.info("Found %d news links", len(news_links))

    # Phase 2b: Extract case numbers from news snippets and loop back.
    # This solves the "Phase 0b never fires" problem — most state-level
    # cases aren't in CourtListener, but news articles often mention
    # case numbers like "Case No. CR2024-001234".
    if not case_numbers and not docket_numbers:
        news_text = " ".join(
            link.notes if isinstance(link, DiscoveredLink) else link.get("notes", "")
            for link in news_links
        )
        news_case_numbers = extract_case_numbers(news_text)
        if news_case_numbers:
            log.info(
                "Phase 2b: Extracted %d case numbers from news snippets: %s",
                len(news_case_numbers), news_case_numbers,
            )

            # Run Phase 0b with news-extracted case numbers
            news_direct_urls = build_direct_portal_urls(
                candidate, news_case_numbers, [], docket_ids=[],
            )
            all_links.extend(news_direct_urls)
            log.info("Phase 2b→0b: %d direct portal URLs from news case numbers", len(news_direct_urls))

            # Run Phase 1b with news-extracted case numbers
            news_case_num_links = discover_case_number_links(
                candidate, news_case_numbers, [], brave_api_key, rate_limit,
            )
            all_links.extend(news_case_num_links)
            log.info("Phase 2b→1b: %d case-number links from news case numbers", len(news_case_num_links))

    # Phase 3: BWC/interrogation footage
    bwc_links = discover_bwc_interrogation_links(candidate, brave_api_key, rate_limit)
    all_links.extend(bwc_links)
    log.info("Found %d BWC/interrogation links", len(bwc_links))

    # Deduplicate by URL across all phases
    seen_urls = set()
    deduped = []
    for link in all_links:
        url = link.url if isinstance(link, DiscoveredLink) else link.get("url", "")
        if url not in seen_urls:
            seen_urls.add(url)
            deduped.append(link)
    all_links = deduped

    # Sort by source quality
    rank_order = {
        SourceRank.COURT_GOV.value: 0,
        SourceRank.COUNTY_CLERK.value: 1,
        SourceRank.LOCAL_NEWS.value: 2,
        SourceRank.LE_RELEASE.value: 3,
        SourceRank.OTHER.value: 4,
    }
    all_links.sort(key=lambda link: rank_order.get(
        link.source_class if isinstance(link, DiscoveredLink) else link.get("source_class", ""),
        5,
    ))

    inventory = LinkInventory(
        case_id=candidate.case_id,
        links=[asdict(link) if isinstance(link, DiscoveredLink) else link for link in all_links],
    )

    # Store enrichment metadata alongside inventory
    inventory.enrichment = {
        "case_numbers": enrichment.get("case_numbers", []),
        "docket_numbers": enrichment.get("docket_numbers", []),
        "citations": enrichment.get("citations", []),
    }

    # For operation cases, extract individual defendant names from news snippets.
    # These can be used to spawn sub-cases or update the case record.
    if _is_operation_name(candidate.suspect_name or ""):
        individual_names = extract_individual_names_from_news(
            news_links, candidate.suspect_name,
        )
        inventory.enrichment["individual_names"] = individual_names
        if individual_names:
            log.info(
                "Operation '%s': extracted %d individual defendant names from news: %s",
                candidate.suspect_name, len(individual_names), individual_names,
            )

    log.info(
        "Discovery complete for %s: %d total links (%d enrichment, %d direct-url, %d court, %d case-num, %d news, %d bwc)",
        candidate.case_id,
        len(all_links),
        len(enrichment_links),
        len(direct_urls),
        len(court_links),
        len(case_num_links),
        len(news_links),
        len(bwc_links),
    )

    return inventory
