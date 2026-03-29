"""
research.py — FlameOn AutoResearch Agent Sandbox
=================================================
THIS IS THE ONLY FILE THE AGENT MODIFIES.

Contains the research methodology: query construction, source discovery,
relevance validation, and confidence assessment.

Phase 1 APIs:
  - MuckRock (FOIA requests)
  - CourtListener (court dockets, opinions, oral arguments)
  - YouTube Data API v3 (bodycam/interrogation footage)
  - Brave Search API (news coverage, case mentions, general discovery)

Required interface:
    research_case(defendant_names: str, jurisdiction: str) -> dict

Environment variables (set in Colab or .env):
    BRAVE_API_KEY         — required
    COURTLISTENER_API_KEY — required (free at courtlistener.com/sign-in/)
    MUCKROCK_API_TOKEN    — optional (public read works without auth)
    (YouTube: no key needed — uses youtube-search-python, free/unlimited)
"""

import os
import requests
import time
import re
from urllib.parse import quote_plus, urlparse

# ──────────────────────────────────────────────────────────────
# API Configuration
# ──────────────────────────────────────────────────────────────

BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY", "")
COURTLISTENER_API_KEY = os.environ.get("COURTLISTENER_API_KEY", "")
MUCKROCK_API_TOKEN = os.environ.get("MUCKROCK_API_TOKEN", "")

MUCKROCK_BASE = "https://www.muckrock.com/api_v2/"
COURTLISTENER_BASE = "https://www.courtlistener.com/api/rest/v4/"
BRAVE_BASE = "https://api.search.brave.com/res/v1/web/search"

REQUEST_TIMEOUT = 15

# ──────────────────────────────────────────────────────────────
# Budget caps — prevent runaway API spending
# ──────────────────────────────────────────────────────────────
# YouTube: 10,000 free units/day. Each search = 100 units.
# Set max searches per run to stay under budget.
# NOTE: youtube-search-python is free/unlimited, but we still cap
# to avoid hammering YouTube and getting rate-limited.
YOUTUBE_MAX_CALLS_PER_RUN = 200      # free but be polite
BRAVE_MAX_CALLS_PER_RUN = 150        # ~150 calls, well within free tier
COURTLISTENER_MAX_CALLS_PER_RUN = 80 # free but slow (5/min)

_api_call_counts = {"youtube": 0, "brave": 0, "courtlistener": 0, "muckrock": 0}

def check_budget(api):
    """Returns True if we're within budget for this API."""
    caps = {
        "youtube": YOUTUBE_MAX_CALLS_PER_RUN,
        "brave": BRAVE_MAX_CALLS_PER_RUN,
        "courtlistener": COURTLISTENER_MAX_CALLS_PER_RUN,
        "muckrock": 999,  # free, no cap needed
    }
    return _api_call_counts.get(api, 0) < caps.get(api, 999)

def log_call(api):
    """Track an API call."""
    _api_call_counts[api] = _api_call_counts.get(api, 0) + 1

def get_budget_report():
    """Return summary of API calls made."""
    return {api: count for api, count in _api_call_counts.items() if count > 0}

def reset_budget():
    """Reset call counts (call at start of each evaluate.py run)."""
    global _api_call_counts
    _api_call_counts = {"youtube": 0, "brave": 0, "courtlistener": 0, "muckrock": 0}

# Rate limiting — tracks last call time per API
_last_call = {"muckrock": 0, "courtlistener": 0, "youtube": 0, "brave": 0}

def rate_limit(api, delay):
    """Enforce minimum delay between calls to an API."""
    elapsed = time.time() - _last_call[api]
    if elapsed < delay:
        time.sleep(delay - elapsed)
    _last_call[api] = time.time()

# Evidence type keywords — agent should iterate on these
EVIDENCE_KEYWORDS = {
    "bodycam": ["body camera", "body cam", "bodycam", "BWC", "body-worn camera", "body worn", "officer camera"],
    "interrogation": ["interrogation", "confession", "interview recording", "interview video", "custodial interview", "police interview", "detective interview"],
    "court_video": ["court video", "trial video", "hearing video", "sentencing video", "court tv", "court audio", "oral argument", "courtroom video", "trial footage"],
    "docket_docs": ["docket", "complaint", "affidavit", "indictment", "motion", "court filing", "case number", "criminal complaint", "probable cause"],
    "dispatch_911": ["911 call", "dispatch audio", "911 audio", "emergency call", "dispatch recording", "911 recording"],
}


# ──────────────────────────────────────────────────────────────
# Name / jurisdiction parsing helpers
# ──────────────────────────────────────────────────────────────

def parse_names(defendant_names):
    """Split defendant names and extract primary + last name."""
    names = [n.strip() for n in defendant_names.split(",") if n.strip()]
    primary = names[0] if names else defendant_names
    parts = primary.split()
    last = parts[-1] if parts else ""
    # Handle titles like "Dr."
    first_parts = [p for p in parts if p not in ("Dr.", "Mr.", "Mrs.", "Ms.", "Jr.", "Sr.", "III", "II")]
    return {
        "all_names": names,
        "primary": primary,
        "last_name": last,
        "clean_primary": " ".join(first_parts),
    }

def parse_jurisdiction(jurisdiction):
    """Extract city, county, state from jurisdiction string."""
    if not jurisdiction:
        return {"city": "", "county": "", "state": "", "state_abbrev": "", "raw": ""}
    parts = [p.strip() for p in jurisdiction.split(",")]
    city = parts[0] if len(parts) >= 1 else ""
    state = parts[-1].strip() if len(parts) >= 2 else ""
    county = parts[1].strip() if len(parts) >= 3 else ""

    state_abbrevs = {
        "California": "CA", "Florida": "FL", "Arizona": "AZ",
        "Tennessee": "TN", "Oregon": "OR", "Ohio": "OH",
        "Colorado": "CO", "Washington": "WA", "Oklahoma": "OK",
        "Alabama": "AL", "South Carolina": "SC",
    }
    state_abbrev = state_abbrevs.get(state, state)

    return {"city": city, "county": county, "state": state,
            "state_abbrev": state_abbrev, "raw": jurisdiction}


# ──────────────────────────────────────────────────────────────
# MuckRock API
# ──────────────────────────────────────────────────────────────

def query_muckrock(search_term, status="done", page_size=10):
    """Query MuckRock FOIA API for completed requests."""
    if not check_budget("muckrock"):
        return []
    rate_limit("muckrock", 1.1)
    log_call("muckrock")
    headers = {}
    if MUCKROCK_API_TOKEN:
        headers["Authorization"] = f"Token {MUCKROCK_API_TOKEN}"
    try:
        resp = requests.get(
            f"{MUCKROCK_BASE}foia/",
            params={"format": "json", "search": search_term,
                    "status": status, "page_size": page_size},
            headers=headers, timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception:
        return []

def search_muckrock(names, jurisdiction):
    """Build and execute MuckRock queries. Returns source list."""
    sources = []
    n = parse_names(names)
    j = parse_jurisdiction(jurisdiction)
    queries = []
    if j["city"] and n["clean_primary"]:
        queries.append(f"{n['clean_primary']} {j['city']}")
    if j["state_abbrev"] and n["clean_primary"]:
        queries.append(f"{n['clean_primary']} {j['state_abbrev']}")
    if n["clean_primary"]:
        queries.append(n["clean_primary"])

    seen_urls = set()
    for query in queries[:3]:
        results = query_muckrock(query)
        for r in results:
            url = r.get("absolute_url") or r.get("url", "")
            if not url or url in seen_urls:
                continue
            title = (r.get("title", "") or "").lower()
            desc = (r.get("description", "") or "").lower()
            combined = f"{title} {desc}"
            relevance = 0.0
            if n["clean_primary"].lower() in combined:
                relevance = 0.9
            elif n["last_name"].lower() in combined and len(n["last_name"]) > 3:
                relevance = 0.5
            elif j["city"].lower() in combined and any(kw in combined for kw in ["shooting", "bodycam", "police", "homicide"]):
                relevance = 0.3
            if relevance >= 0.3:
                seen_urls.add(url)
                sources.append({
                    "url": url, "type": "muckrock_foia",
                    "relevance_score": relevance,
                    "description": r.get("title", ""), "api": "muckrock",
                })
    return sources


# ──────────────────────────────────────────────────────────────
# CourtListener API
# ──────────────────────────────────────────────────────────────

def query_courtlistener_dockets(search_term, page_size=5):
    """Search CourtListener docket database."""
    if not COURTLISTENER_API_KEY:
        return []
    if not check_budget("courtlistener"):
        return []
    rate_limit("courtlistener", 3.0)  # 5 req/min = 12s strict, but bursts OK
    log_call("courtlistener")
    try:
        resp = requests.get(
            f"{COURTLISTENER_BASE}search/",
            params={"q": search_term, "type": "r", "format": "json", "page_size": page_size},
            headers={"Authorization": f"Token {COURTLISTENER_API_KEY}"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception:
        return []

def query_courtlistener_opinions(search_term, page_size=5):
    """Search CourtListener opinions/case law."""
    if not COURTLISTENER_API_KEY:
        return []
    if not check_budget("courtlistener"):
        return []
    rate_limit("courtlistener", 3.0)
    log_call("courtlistener")
    try:
        resp = requests.get(
            f"{COURTLISTENER_BASE}search/",
            params={"q": search_term, "type": "o", "format": "json", "page_size": page_size},
            headers={"Authorization": f"Token {COURTLISTENER_API_KEY}"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception:
        return []

def search_courtlistener(names, jurisdiction):
    """Build and execute CourtListener queries. Returns source list."""
    sources = []
    n = parse_names(names)
    j = parse_jurisdiction(jurisdiction)
    seen_urls = set()
    queries = []
    if n["clean_primary"]:
        queries.append(n["clean_primary"])
    if n["last_name"] and j["state_abbrev"]:
        queries.append(f"{n['clean_primary']} {j['state_abbrev']}")

    for query in queries[:2]:
        for r in query_courtlistener_dockets(query):
            case_name = r.get("caseName", "") or r.get("case_name", "")
            docket_url = r.get("absolute_url", "")
            if docket_url and not docket_url.startswith("http"):
                docket_url = f"https://www.courtlistener.com{docket_url}"
            if not docket_url or docket_url in seen_urls:
                continue
            case_lower = case_name.lower()
            relevance = 0.0
            if n["clean_primary"].lower() in case_lower:
                relevance = 0.9
            elif n["last_name"].lower() in case_lower:
                relevance = 0.8
            if relevance >= 0.5:
                seen_urls.add(docket_url)
                sources.append({
                    "url": docket_url, "type": "court_docket",
                    "relevance_score": relevance,
                    "description": case_name, "api": "courtlistener",
                })

        for r in query_courtlistener_opinions(query):
            case_name = r.get("caseName", "") or r.get("case_name", "")
            opinion_url = r.get("absolute_url", "")
            if opinion_url and not opinion_url.startswith("http"):
                opinion_url = f"https://www.courtlistener.com{opinion_url}"
            if not opinion_url or opinion_url in seen_urls:
                continue
            case_lower = case_name.lower()
            relevance = 0.0
            if n["clean_primary"].lower() in case_lower:
                relevance = 0.85
            elif n["last_name"].lower() in case_lower:
                relevance = 0.7
            if relevance >= 0.5:
                seen_urls.add(opinion_url)
                sources.append({
                    "url": opinion_url, "type": "court_opinion",
                    "relevance_score": relevance,
                    "description": case_name, "api": "courtlistener",
                })
    return sources


# ──────────────────────────────────────────────────────────────
# YouTube Search (FREE — no API key, no quota)
# ──────────────────────────────────────────────────────────────
# Uses youtube-search-python which hits YouTube's internal InnerTube API.
# pip install youtube-search-python
# Zero cost. Unlimited searches. No API key needed.

def search_youtube(names, jurisdiction):
    """
    Search YouTube for case footage using youtube-search-python.
    Costs $0. No API key. No quota.
    """
    try:
        from youtubesearchpython import VideosSearch
    except ImportError:
        return []

    sources = []
    n = parse_names(names)
    j = parse_jurisdiction(jurisdiction)
    seen_ids = set()

    credible_channels = {
        "policeactivity", "realworldpolice", "bodycamwatch",
        "lawcrimetrial", "lawcrimenetwork", "courttv",
        "courtroomconsequences", "jaxsheriff", "phoenixpolice",
        "seattlepolice", "austinpolice", "houstonpolice",
        "orangecountysheriff", "mesapolice", "aurorapolice",
    }

    entertainment_flags = [
        "movie", "trailer", "tv show", "series", "episode",
        "music video", "official audio", "lyrics", "anime",
        "gameplay", "reaction", "prank",
    ]

    queries = []
    if n["clean_primary"]:
        queries.append(f"{n['clean_primary']} bodycam")
        queries.append(f"{n['clean_primary']} interrogation")
        queries.append(f"{n['clean_primary']} court trial")
    if n["clean_primary"] and j["city"]:
        queries.append(f"{n['clean_primary']} {j['city']} police")
    if n["clean_primary"]:
        queries.append(f"{n['clean_primary']} 911 call")

    for query in queries[:5]:
        try:
            search = VideosSearch(query, limit=3)
            results = search.result().get("result", [])
        except Exception:
            continue

        for item in results:
            video_id = item.get("id", "")
            if not video_id or video_id in seen_ids:
                continue

            title = item.get("title", "")
            channel = item.get("channel", {}).get("name", "")
            # Description may be truncated but good enough for classification
            desc_snippets = item.get("descriptionSnippet", [])
            description = " ".join(
                seg.get("text", "") for seg in (desc_snippets or [])
            )
            combined = f"{title} {description}".lower()

            relevance = _score_youtube_relevance(
                n, combined, title, channel, credible_channels, entertainment_flags
            )
            if relevance >= 0.3:
                seen_ids.add(video_id)
                sources.append(_build_youtube_source(
                    video_id, title, channel, combined, relevance
                ))

    return sources

def _score_youtube_relevance(n, combined, title, channel, credible_channels, entertainment_flags):
    """Score how relevant a YouTube video is to our case."""
    relevance = 0.0
    if n["clean_primary"].lower() in title.lower():
        relevance = 0.9
    elif n["last_name"].lower() in title.lower() and len(n["last_name"]) > 3:
        relevance = 0.6
    elif n["clean_primary"].lower() in combined:
        relevance = 0.5
    elif n["last_name"].lower() in combined and len(n["last_name"]) > 3:
        relevance = 0.35

    channel_slug = re.sub(r'[^a-z0-9]', '', channel.lower())
    if channel_slug in credible_channels:
        relevance = min(relevance + 0.2, 1.0)

    if any(flag in combined for flag in entertainment_flags):
        relevance = 0.0

    return relevance

def _build_youtube_source(video_id, title, channel, combined, relevance):
    """Build a source dict from YouTube video data."""
    url = f"https://www.youtube.com/watch?v={video_id}"
    etype = "general_footage"
    if any(kw in combined for kw in ["bodycam", "body cam", "body camera", "bwc", "body worn"]):
        etype = "bodycam_footage"
    elif any(kw in combined for kw in ["interrogation", "confession", "interview", "custodial"]):
        etype = "interrogation_footage"
    elif any(kw in combined for kw in ["trial", "court", "hearing", "sentencing", "verdict"]):
        etype = "court_footage"
    elif any(kw in combined for kw in ["911", "dispatch", "emergency call"]):
        etype = "dispatch_audio"
    return {
        "url": url, "type": etype, "relevance_score": relevance,
        "description": title, "channel": channel, "api": "youtube_free",
    }


# ──────────────────────────────────────────────────────────────
# Brave Search API
# ──────────────────────────────────────────────────────────────

def query_brave(search_term, count=5):
    """Search Brave Web Search API."""
    if not BRAVE_API_KEY:
        return []
    if not check_budget("brave"):
        return []
    rate_limit("brave", 1.1)
    log_call("brave")
    try:
        resp = requests.get(
            BRAVE_BASE,
            params={"q": search_term, "count": count},
            headers={"X-Subscription-Token": BRAVE_API_KEY, "Accept": "application/json"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("web", {}).get("results", [])
    except Exception:
        return []

def search_brave(names, jurisdiction):
    """Use Brave Search for news, court records, footage links."""
    sources = []
    n = parse_names(names)
    j = parse_jurisdiction(jurisdiction)
    seen_urls = set()

    queries = []
    if n["clean_primary"] and j["city"]:
        queries.append(f'"{n["clean_primary"]}" {j["city"]} case')
    if n["clean_primary"] and j["state_abbrev"]:
        queries.append(f'"{n["clean_primary"]}" {j["state_abbrev"]} court')
    if n["clean_primary"]:
        queries.append(f'"{n["clean_primary"]}" bodycam OR interrogation OR sentencing')
    if n["clean_primary"]:
        queries.append(f'"{n["clean_primary"]}" site:courtlistener.com OR site:casetext.com OR site:justia.com')

    evidence_domain_map = {
        "courtlistener.com": "court_docket", "casetext.com": "court_docket",
        "justia.com": "court_docket", "findlaw.com": "court_opinion",
        "docketbird.com": "court_docket", "unicourt.com": "court_docket",
        "pacermonitor.com": "court_docket", "youtube.com": "video_footage",
        "courttv.com": "court_footage", "muckrock.com": "foia_request",
        "documentcloud.org": "foia_document",
    }

    skip_domains = {
        "imdb.com", "tvguide.com", "spotify.com", "invubu.com",
        "viberate.com", "soapcentral.com", "tiktok.com",
        "reddit.com", "facebook.com", "instagram.com", "pinterest.com",
    }

    for query in queries[:4]:
        results = query_brave(query, count=5)
        for r in results:
            url = r.get("url", "")
            title = r.get("title", "")
            description = r.get("description", "")
            if not url or url in seen_urls:
                continue
            try:
                domain = urlparse(url).netloc.replace("www.", "")
            except Exception:
                continue
            if domain in skip_domains:
                continue

            combined = f"{title} {description}".lower()
            relevance = 0.0
            if n["clean_primary"].lower() in combined:
                relevance = 0.8
            elif n["last_name"].lower() in combined and len(n["last_name"]) > 4:
                if j["city"].lower() in combined or j["state_abbrev"].lower() in combined:
                    relevance = 0.5
                else:
                    relevance = 0.3

            if relevance > 0 and j["city"]:
                if j["city"].lower() not in combined and j["state_abbrev"].lower() not in combined:
                    relevance *= 0.7

            if relevance < 0.25:
                continue

            seen_urls.add(url)
            source_type = "news_article"
            for d, stype in evidence_domain_map.items():
                if d in domain:
                    source_type = stype
                    break
            sources.append({
                "url": url, "type": source_type, "relevance_score": relevance,
                "description": title, "api": "brave",
            })
    return sources


# ──────────────────────────────────────────────────────────────
# Evidence detection
# ──────────────────────────────────────────────────────────────

def detect_evidence_types(sources):
    """Determine which evidence types are present."""
    evidence = {
        "bodycam": False, "interrogation": False, "court_video": False,
        "docket_docs": False, "dispatch_911": False,
    }
    type_to_evidence = {
        "bodycam_footage": "bodycam", "interrogation_footage": "interrogation",
        "court_footage": "court_video", "court_docket": "docket_docs",
        "court_opinion": "docket_docs", "dispatch_audio": "dispatch_911",
    }
    for s in sources:
        stype = s.get("type", "")
        if stype in type_to_evidence:
            evidence[type_to_evidence[stype]] = True

    all_text = " ".join(
        f"{s.get('description', '')} {s.get('url', '')}" for s in sources
    ).lower()
    for etype, keywords in EVIDENCE_KEYWORDS.items():
        if evidence[etype]:
            continue
        for kw in keywords:
            if kw.lower() in all_text:
                evidence[etype] = True
                break

    docket_domains = ["courtlistener", "casetext", "justia", "findlaw",
                      "pacer", "docketbird", "unicourt", "trellis"]
    for s in sources:
        url = s.get("url", "").lower()
        if any(d in url for d in docket_domains):
            evidence["docket_docs"] = True
            break
    return evidence


# ──────────────────────────────────────────────────────────────
# Confidence assessment
# ──────────────────────────────────────────────────────────────

def assess_confidence(sources, evidence):
    """Confidence based on evidence breadth, source quality, API diversity."""
    evidence_count = sum(1 for v in evidence.values() if v)
    high_relevance = sum(1 for s in sources if s.get("relevance_score", 0) >= 0.5)
    api_diversity = len(set(s.get("api", "") for s in sources))

    if evidence_count >= 3 and high_relevance >= 3 and api_diversity >= 2:
        return "high"
    elif evidence_count >= 2 and high_relevance >= 1:
        return "medium"
    elif evidence_count >= 1 or high_relevance >= 1:
        return "medium"
    else:
        return "low"


# ──────────────────────────────────────────────────────────────
# Main research function — THE INTERFACE evaluate.py calls
# ──────────────────────────────────────────────────────────────

def research_case(defendant_names, jurisdiction):
    """
    Given a defendant name and jurisdiction, research the case using
    all available structured APIs and return findings.
    """
    all_sources = []
    notes = []

    notes.append("=== MuckRock FOIA ===")
    mr_sources = search_muckrock(defendant_names, jurisdiction)
    notes.append(f"  Found {len(mr_sources)} FOIA results")
    all_sources.extend(mr_sources)

    notes.append("=== CourtListener ===")
    cl_sources = search_courtlistener(defendant_names, jurisdiction)
    notes.append(f"  Found {len(cl_sources)} court records")
    all_sources.extend(cl_sources)

    notes.append("=== Brave Search ===")
    brave_sources = search_brave(defendant_names, jurisdiction)
    notes.append(f"  Found {len(brave_sources)} web results")
    all_sources.extend(brave_sources)

    # YouTube — free, no API key, uses youtube-search-python
    notes.append("=== YouTube (free) ===")
    yt_sources = search_youtube(defendant_names, jurisdiction)
    notes.append(f"  Found {len(yt_sources)} videos")
    all_sources.extend(yt_sources)

    # Deduplicate by URL
    seen = set()
    deduped = []
    for s in all_sources:
        url = s.get("url", "")
        if url and url not in seen:
            seen.add(url)
            deduped.append(s)
    all_sources = deduped
    all_sources.sort(key=lambda s: s.get("relevance_score", 0), reverse=True)

    evidence = detect_evidence_types(all_sources)
    confidence = assess_confidence(all_sources, evidence)

    notes.append(f"\n=== Summary ===")
    notes.append(f"  Total sources: {len(all_sources)}")
    notes.append(f"  Evidence: {evidence}")
    notes.append(f"  Confidence: {confidence}")
    notes.append(f"  API budget used: {get_budget_report()}")

    return {
        "evidence_found": evidence,
        "sources_found": all_sources,
        "confidence": confidence,
        "research_notes": "\n".join(notes),
    }
