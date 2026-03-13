"""Stage 3A — Link Discovery (State-Aware Asset Miner).

Only runs after validation_status = validated_closed.
Discovers and inventories links. Does NOT download anything.
"""

import time
from dataclasses import asdict

import requests

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
    """Execute a Brave Search API query."""
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
        log.error("Brave search failed for query '%s': %s", query, e)
        return []


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
    elif any(d in url_lower for d in ["reddit.com", "twitter.com", "facebook.com", "wikipedia.org"]):
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
        link_type = "bwc_video"
    elif any(w in text for w in ["foia", "public records"]):
        link_type = "foia"
    elif url_lower.endswith(".pdf"):
        link_type = "pdf_document"
    else:
        link_type = "news_article"

    return source_class, link_type


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
            if url in seen_urls:
                continue
            seen_urls.add(url)

            source_class, link_type = _classify_link(url, r["title"], r["description"])

            # Recommend download for court/clerk PDFs and dockets
            download_rec = source_class in [SourceRank.COURT_GOV.value, SourceRank.COUNTY_CLERK.value]
            official = source_class in [SourceRank.COURT_GOV.value, SourceRank.COUNTY_CLERK.value]

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
            if url in seen_urls:
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
            if url in seen_urls:
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


def run_discovery(
    candidate: CaseCandidate,
    brave_api_key: str,
    rate_limit: float = 1.0,
) -> LinkInventory:
    """Run full link discovery for a validated closed case.

    Returns a LinkInventory with all discovered links.
    """
    log.info("Starting link discovery for %s (%s)", candidate.case_id, candidate.suspect_name)

    all_links = []

    # Phase 1: Court/docket links (highest priority)
    court_links = discover_court_links(candidate, brave_api_key, rate_limit)
    all_links.extend(court_links)
    log.info("Found %d court/docket links", len(court_links))

    # Phase 2: News articles
    news_links = discover_news_links(candidate, brave_api_key, rate_limit)
    all_links.extend(news_links)
    log.info("Found %d news links", len(news_links))

    # Phase 3: BWC/interrogation footage
    bwc_links = discover_bwc_interrogation_links(candidate, brave_api_key, rate_limit)
    all_links.extend(bwc_links)
    log.info("Found %d BWC/interrogation links", len(bwc_links))

    # Sort by source quality
    rank_order = {
        SourceRank.COURT_GOV.value: 0,
        SourceRank.COUNTY_CLERK.value: 1,
        SourceRank.LOCAL_NEWS.value: 2,
        SourceRank.LE_RELEASE.value: 3,
        SourceRank.OTHER.value: 4,
    }
    all_links.sort(key=lambda link: rank_order.get(link.source_class, 5))

    inventory = LinkInventory(
        case_id=candidate.case_id,
        links=[asdict(link) for link in all_links],
    )

    log.info(
        "Discovery complete for %s: %d total links (%d court, %d news, %d bwc)",
        candidate.case_id,
        len(all_links),
        len(court_links),
        len(news_links),
        len(bwc_links),
    )

    return inventory
