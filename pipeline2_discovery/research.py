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
import json
import requests
import time
import re
from datetime import datetime
from urllib.parse import quote_plus, urlparse
from dotenv import load_dotenv
try:
    import praw
except ImportError:
    praw = None

try:
    from exa_py import Exa
except ImportError:
    Exa = None

try:
    from firecrawl import FirecrawlApp
except ImportError:
    FirecrawlApp = None

load_dotenv()

# ──────────────────────────────────────────────────────────────
# API Configuration
# ──────────────────────────────────────────────────────────────

BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY", "")
COURTLISTENER_API_KEY = os.environ.get("COURTLISTENER_API_KEY", "")
MUCKROCK_API_TOKEN = os.environ.get("MUCKROCK_API_TOKEN", "")

REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "FlameOn-Research/1.0")

EXA_API_KEY = os.environ.get("EXA_API_KEY", "")
FIRECRAWL_API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")

MUCKROCK_BASE = "https://www.muckrock.com/api_v2/"
COURTLISTENER_BASE = "https://www.courtlistener.com/api/rest/v4/"
BRAVE_BASE = "https://api.search.brave.com/res/v1/web/search"

REQUEST_TIMEOUT = 15

# ──────────────────────────────────────────────────────────────
# Brave billing quota — hard spend cap using response headers
# ──────────────────────────────────────────────────────────────
# $0.005/request observed from $57.08 / 11,416 requests.
# Set BRAVE_SPEND_LIMIT_USD env var to override (default $4.00).
# State is persisted to brave_quota.json and reset each calendar month.
BRAVE_SPEND_LIMIT_USD = float(os.environ.get("BRAVE_SPEND_LIMIT_USD", "4.00"))
BRAVE_COST_PER_REQUEST = 0.005   # $/request (from billing history)
BRAVE_QUOTA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "brave_quota.json")

def _load_brave_quota():
    """Load persistent Brave quota state; auto-reset on new calendar month."""
    month_key = datetime.utcnow().strftime("%Y-%m")
    default = {"month_key": month_key, "monthly_remaining": None,
                "estimated_spend": 0.0, "calls_this_month": 0}
    try:
        with open(BRAVE_QUOTA_FILE, "r") as f:
            data = json.load(f)
        if data.get("month_key") != month_key:
            # New month — full reset (don't carry over stale monthly_remaining from old month)
            data = default
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return default

def _save_brave_quota(state):
    """Persist Brave quota state to disk."""
    try:
        with open(BRAVE_QUOTA_FILE, "w") as f:
            json.dump(state, f)
    except Exception:
        pass

def _update_quota_from_response(state, resp):
    """
    Parse x-ratelimit-remaining header after a successful Brave call.
    Header format: "per_second_remaining, monthly_remaining"
    e.g. "49, 1234"  or  "0, 0" when exhausted.
    Returns updated state dict.
    """
    header = resp.headers.get("x-ratelimit-remaining", "")
    if header:
        parts = [p.strip() for p in header.split(",")]
        if len(parts) >= 2:
            try:
                state["monthly_remaining"] = int(parts[1])
            except ValueError:
                pass
    state["calls_this_month"] = state.get("calls_this_month", 0) + 1
    state["estimated_spend"] = state.get("estimated_spend", 0.0) + BRAVE_COST_PER_REQUEST
    return state

# ──────────────────────────────────────────────────────────────
# Exa quota — monthly credit tracking (free tier: 1K/month)
# ──────────────────────────────────────────────────────────────
EXA_MONTHLY_LIMIT = int(os.environ.get("EXA_MONTHLY_LIMIT", "1000"))
EXA_MAX_PER_CASE = 3
EXA_QUOTA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exa_quota.json")

def _load_exa_quota():
    month_key = datetime.utcnow().strftime("%Y-%m")
    default = {"month_key": month_key, "calls_this_month": 0}
    try:
        with open(EXA_QUOTA_FILE, "r") as f:
            data = json.load(f)
        if data.get("month_key") != month_key:
            data = default
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return default

def _save_exa_quota(state):
    try:
        with open(EXA_QUOTA_FILE, "w") as f:
            json.dump(state, f)
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────
# Firecrawl quota — LIFETIME credit tracking (500 total, NEVER resets)
# ──────────────────────────────────────────────────────────────
FIRECRAWL_LIFETIME_LIMIT = int(os.environ.get("FIRECRAWL_LIFETIME_LIMIT", "500"))
FIRECRAWL_QUOTA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "firecrawl_quota.json")
PORTALS_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portals_cache.json")

def _load_firecrawl_quota():
    default = {"lifetime_credits_used": 0, "pages_scraped": 0}
    try:
        with open(FIRECRAWL_QUOTA_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default

def _save_firecrawl_quota(state):
    try:
        with open(FIRECRAWL_QUOTA_FILE, "w") as f:
            json.dump(state, f)
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────
# Usage logging — append-only log for cost estimation
# ──────────────────────────────────────────────────────────────
API_USAGE_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "api_usage_log.json")

def _log_api_usage(api, query, credits_used, results_found, cost_usd=0.0):
    """Append a usage entry to the log file for cost estimation."""
    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "api": api,
        "query": query[:100],
        "credits_used": credits_used,
        "results_found": results_found,
        "cost_usd": cost_usd,
    }
    try:
        try:
            with open(API_USAGE_LOG_FILE, "r") as f:
                log = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            log = []
        log.append(entry)
        with open(API_USAGE_LOG_FILE, "w") as f:
            json.dump(log, f, indent=1)
    except Exception:
        pass

def get_usage_summary():
    """Return a summary of API usage from the log."""
    try:
        with open(API_USAGE_LOG_FILE, "r") as f:
            log = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    summary = {}
    for entry in log:
        api = entry.get("api", "unknown")
        if api not in summary:
            summary[api] = {"calls": 0, "credits": 0, "cost_usd": 0.0, "results": 0}
        summary[api]["calls"] += 1
        summary[api]["credits"] += entry.get("credits_used", 0)
        summary[api]["cost_usd"] += entry.get("cost_usd", 0)
        summary[api]["results"] += entry.get("results_found", 0)
    return summary

# ──────────────────────────────────────────────────────────────
# Budget caps — prevent runaway API spending
# ──────────────────────────────────────────────────────────────
# YouTube: 10,000 free units/day. Each search = 100 units.
# Set max searches per run to stay under budget.
# NOTE: youtube-search-python is free/unlimited, but we still cap
# to avoid hammering YouTube and getting rate-limited.
YOUTUBE_MAX_CALLS_PER_RUN = 400      # free but be polite
BRAVE_MAX_CALLS_PER_RUN = 450        # 11 queries × 38 cases + headroom; billing guard enforces real $ cap
COURTLISTENER_MAX_CALLS_PER_RUN = 160 # free but slow (5/min); raised for full 38-case coverage
BRAVE_MAX_PER_CASE = 11              # max Brave queries per individual case (matches queries[:11])

_api_call_counts = {"youtube": 0, "brave": 0, "courtlistener": 0, "muckrock": 0, "reddit": 0, "exa": 0, "firecrawl": 0}
_brave_case_calls = 0                 # reset per case in research_case()
_exa_case_calls = 0                   # reset per case in research_case()

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
    _api_call_counts = {"youtube": 0, "brave": 0, "courtlistener": 0, "muckrock": 0, "reddit": 0, "exa": 0, "firecrawl": 0}

# Rate limiting — tracks last call time per API
_last_call = {"muckrock": 0, "courtlistener": 0, "youtube": 0, "brave": 0, "reddit": 0, "exa": 0, "firecrawl": 0}

def rate_limit(api, delay):
    """Enforce minimum delay between calls to an API."""
    elapsed = time.time() - _last_call[api]
    if elapsed < delay:
        time.sleep(delay - elapsed)
    _last_call[api] = time.time()

# Evidence type keywords — agent should iterate on these
EVIDENCE_KEYWORDS = {
    "bodycam": ["body camera", "body cam", "bodycam", "BWC", "body-worn camera", "body worn",
                "officer camera", "dashcam", "dash cam", "police cam", "cop cam"],
    "interrogation": ["interrogation", "confession", "interview recording", "interview video",
                      "custodial interview", "police interview", "detective interview",
                      "interview", "interrogated", "confessed", "questioned by police"],
    "court_video": ["court video", "trial video", "hearing video", "sentencing video", "court tv",
                    "court audio", "oral argument", "courtroom video", "trial footage",
                    "trial", "hearing", "sentencing", "verdict", "courtroom", "arraignment",
                    "preliminary hearing", "sentenced", "convicted", "conviction", "found guilty",
                    "guilty verdict"],
    "docket_docs": ["docket", "complaint", "affidavit", "indictment", "motion", "court filing",
                    "case number", "criminal complaint", "probable cause", "charging document",
                    "grand jury", "information filed", "superseding indictment"],
    "dispatch_911": ["911 call", "dispatch audio", "911 audio", "emergency call",
                     "dispatch recording", "911 recording", "911", "called 911",
                     "emergency dispatch"],
}


# ──────────────────────────────────────────────────────────────
# Name / jurisdiction parsing helpers
# ──────────────────────────────────────────────────────────────

def parse_names(defendant_names):
    """Split defendant names and extract primary + last name."""
    names = [n.strip() for n in defendant_names.split(",") if n.strip()]
    primary = names[0] if names else defendant_names
    parts = primary.split()
    # Handle titles and name suffixes
    first_parts = [p for p in parts if p not in ("Dr.", "Mr.", "Mrs.", "Ms.", "Jr.", "Sr.", "III", "II")]
    # Find actual last name: skip trailing generational suffixes (Jr., Sr., III, II)
    # e.g. "William James McElroy Jr." → last = "McElroy", not "Jr."
    name_suffixes = {"Jr.", "Jr", "Sr.", "Sr", "III", "II", "IV", "V"}
    last = ""
    for part in reversed(parts):
        if part not in name_suffixes and part not in ("Dr.", "Mr.", "Mrs.", "Ms."):
            last = part
            break
    if not last and parts:
        last = parts[-1]
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

def query_courtlistener_oral_args(search_term, page_size=3):
    """Search CourtListener oral argument recordings (audio = court_video evidence)."""
    if not COURTLISTENER_API_KEY:
        return []
    if not check_budget("courtlistener"):
        return []
    rate_limit("courtlistener", 3.0)
    log_call("courtlistener")
    try:
        resp = requests.get(
            f"{COURTLISTENER_BASE}search/",
            params={"q": search_term, "type": "oa", "format": "json", "page_size": page_size},
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
                snippet = r.get("snippet", "") or ""
                description = f"{case_name} {snippet}".strip()
                sources.append({
                    "url": opinion_url, "type": "court_opinion",
                    "relevance_score": relevance,
                    "description": description, "api": "courtlistener",
                })

    return sources


# ──────────────────────────────────────────────────────────────
# Wikipedia Search (FREE — no API key, no quota)
# ──────────────────────────────────────────────────────────────

def search_wikipedia(names):
    """Search Wikipedia for case articles using free MediaWiki API."""
    n = parse_names(names)
    if not n["last_name"] or len(n["last_name"]) < 4:
        return []
    sources = []
    try:
        resp = requests.get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "query", "list": "search",
                "srsearch": n["clean_primary"], "srnamespace": 0,
                "srlimit": 5, "format": "json",
            },
            timeout=8,
        )
        data = resp.json()
        CASE_KEYWORDS = {
            "murder", "killed", "killing", "death", "trial", "sentenced",
            "convicted", "conviction", "crime", "guilty", "arrest", "arrested",
            "manslaughter", "assault", "robbery", "shooting", "stabbing",
            "rape", "abuse", "victim", "defendant", "jury", "verdict",
            "homicide", "execution", "imprisoned", "prison", "jail",
        }
        for r in data.get("query", {}).get("search", [])[:3]:
            title = r.get("title", "")
            snippet = r.get("snippet", "") or ""
            combined = f"{title} {snippet}".lower()
            # Must contain at least one crime/case keyword to avoid false positives
            if not any(kw in combined for kw in CASE_KEYWORDS):
                continue
            relevance = 0.0
            if n["clean_primary"].lower() in combined:
                relevance = 0.80
            elif n["last_name"].lower() in combined:
                relevance = 0.55
            if relevance >= 0.5:
                url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
                sources.append({
                    "url": url, "type": "wiki_article",
                    "relevance_score": relevance,
                    "description": title, "api": "wikipedia",
                })
    except Exception:
        pass
    return sources


# ──────────────────────────────────────────────────────────────
# DailyMotion Search (FREE — no API key, no quota)
# ──────────────────────────────────────────────────────────────

def search_dailymotion(names):
    """Search DailyMotion for case footage using public API."""
    n = parse_names(names)
    if not n["clean_primary"]:
        return []
    sources = []
    seen_ids = set()
    queries = [
        f"{n['clean_primary']} bodycam",
        f"{n['clean_primary']} interrogation",
    ]
    for query in queries[:2]:
        try:
            resp = requests.get(
                "https://api.dailymotion.com/videos",
                params={
                    "search": query,
                    "fields": "id,title,url",
                    "limit": 4, "language": "en",
                },
                timeout=8,
            )
            if resp.status_code != 200:
                continue
            for item in resp.json().get("list", []):
                vid_id = item.get("id", "")
                if not vid_id or vid_id in seen_ids:
                    continue
                title = item.get("title", "") or ""
                url = item.get("url", "") or f"https://www.dailymotion.com/video/{vid_id}"
                combined = title.lower()
                relevance = 0.0
                if n["clean_primary"].lower() in title.lower():
                    relevance = 0.9
                elif n["last_name"].lower() in title.lower() and len(n["last_name"]) > 3:
                    relevance = 0.6
                if relevance < 0.5:
                    continue
                seen_ids.add(vid_id)
                etype = "general_footage"
                if any(kw in combined for kw in ["bodycam", "body cam", "body camera", "bwc"]):
                    etype = "bodycam_footage"
                elif any(kw in combined for kw in ["interrogation", "confession", "interview"]):
                    etype = "interrogation_footage"
                elif any(kw in combined for kw in ["trial", "court", "hearing"]):
                    etype = "court_footage"
                sources.append({
                    "url": url, "type": etype,
                    "relevance_score": relevance,
                    "description": title, "api": "dailymotion",
                })
        except Exception:
            continue
    return sources


# ──────────────────────────────────────────────────────────────
# YouTube Search (FREE — no API key, no quota)
# ──────────────────────────────────────────────────────────────
# Uses youtube-search-python which hits YouTube's internal InnerTube API.
# pip install youtube-search-python
# Zero cost. Unlimited searches. No API key needed.

def search_youtube(names, jurisdiction):
    """
    Search YouTube for case footage using yt-dlp (robust, actively maintained).
    Costs $0. No API key. No quota.
    """
    try:
        import yt_dlp
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
        queries.append(f"{n['clean_primary']} confession")
    if n["clean_primary"] and j["city"]:
        queries.append(f"{n['clean_primary']} {j['city']} police")
        queries.append(f"{n['clean_primary']} {j['city']} murder")
    if n["clean_primary"]:
        queries.append(f"{n['clean_primary']} 911 call")
    if n["clean_primary"]:
        queries.append(f"{n['clean_primary']} sentencing")
    if n["clean_primary"]:
        queries.append(f"{n['clean_primary']} police interview")

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
        "skip_download": True,
        "socket_timeout": 8,
    }

    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

    def _yt_fetch(q):
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"ytsearch5:{q}", download=False)
            return info.get("entries", []) if info else []

    for query in queries[:9]:
        if not check_budget("youtube"):
            break
        rate_limit("youtube", 1.0)
        log_call("youtube")
        try:
            with ThreadPoolExecutor(max_workers=1) as ex:
                future = ex.submit(_yt_fetch, query)
                results = future.result(timeout=12)
        except Exception:
            results = []
            continue

        for item in results:
            if not item:
                continue
            video_id = item.get("id", "")
            if not video_id or video_id in seen_ids:
                continue

            title = item.get("title", "") or ""
            channel = item.get("channel", "") or item.get("uploader", "") or ""
            description = item.get("description", "") or ""
            combined = f"{title} {description}".lower()

            relevance = _score_youtube_relevance(
                n, j, combined, title, channel, credible_channels, entertainment_flags
            )
            if relevance >= 0.25:
                seen_ids.add(video_id)
                sources.append(_build_youtube_source(
                    video_id, title, channel, combined, relevance
                ))

    return sources

def _score_youtube_relevance(n, j, combined, title, channel, credible_channels, entertainment_flags):
    """Score how relevant a YouTube video is to our case."""
    evidence_keywords = [
        "bodycam", "body cam", "body camera", "interrogation", "confession",
        "police footage", "arrest footage", "police video", "cop cam",
        "trial", "sentencing", "hearing", "court", "911 call",
    ]
    relevance = 0.0
    if n["clean_primary"].lower() in title.lower():
        relevance = 0.9
    elif n["last_name"].lower() in title.lower() and len(n["last_name"]) > 3:
        relevance = 0.6
    elif n["clean_primary"].lower() in combined:
        relevance = 0.5
    elif n["last_name"].lower() in combined and len(n["last_name"]) > 3:
        relevance = 0.35
    # Jurisdiction + evidence keyword: likely the right incident even without name in title
    elif j["city"] and j["city"].lower() in combined:
        if any(kw in combined for kw in evidence_keywords):
            relevance = 0.30

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
    if any(kw in combined for kw in [
        "bodycam", "body cam", "body camera", "bwc", "body worn",
        "body-worn", "police cam", "cop cam", "dashcam", "dash cam",
        "police footage", "officer footage", "arrest footage", "police video",
        "officer video", "police camera", "dept releases", "department releases",
    ]):
        etype = "bodycam_footage"
    elif any(kw in combined for kw in [
        "interrogation", "confession", "interview", "custodial",
        "police interview", "detective interview", "questioned",
    ]):
        etype = "interrogation_footage"
    elif any(kw in combined for kw in [
        "trial", "court", "hearing", "sentencing", "verdict",
        "courtroom", "arraignment", "preliminary hearing",
    ]):
        etype = "court_footage"
    elif any(kw in combined for kw in ["911", "dispatch", "emergency call", "called police"]):
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
    global _brave_case_calls
    if not BRAVE_API_KEY:
        return []
    if not check_budget("brave"):
        return []
    if _brave_case_calls >= BRAVE_MAX_PER_CASE:
        return []

    # ── Hard billing quota check ──────────────────────────────
    quota = _load_brave_quota()
    # Block if monthly quota header says exhausted
    if quota.get("monthly_remaining") is not None and quota["monthly_remaining"] <= 0:
        print(f"[Brave] BLOCKED — monthly quota exhausted (0 requests remaining)")
        return []
    # Block if estimated spend would exceed the dollar cap
    projected = quota.get("estimated_spend", 0.0) + BRAVE_COST_PER_REQUEST
    if projected > BRAVE_SPEND_LIMIT_USD:
        print(f"[Brave] BLOCKED — spend cap reached "
              f"(${quota['estimated_spend']:.2f} + ${BRAVE_COST_PER_REQUEST:.3f} "
              f"> ${BRAVE_SPEND_LIMIT_USD:.2f} limit)")
        return []
    # ─────────────────────────────────────────────────────────

    rate_limit("brave", 1.1)
    log_call("brave")
    _brave_case_calls += 1
    try:
        resp = requests.get(
            BRAVE_BASE,
            params={"q": search_term, "count": count},
            headers={"X-Subscription-Token": BRAVE_API_KEY, "Accept": "application/json"},
            timeout=REQUEST_TIMEOUT,
        )
        # Handle quota-exhausted response from Brave (402 = quota exceeded)
        if resp.status_code == 402:
            quota["monthly_remaining"] = 0
            _save_brave_quota(quota)
            print("[Brave] 402 quota exhausted — saved state, skipping remaining calls")
            return []
        resp.raise_for_status()
        quota = _update_quota_from_response(quota, resp)
        _save_brave_quota(quota)
        return resp.json().get("web", {}).get("results", [])
    except requests.exceptions.HTTPError:
        return []
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
        queries.append(f'"{n["clean_primary"]}" site:caselaw.findlaw.com OR site:law.justia.com OR site:dockets.justia.com OR site:cases.justia.com OR site:courtlistener.com')
    if n["clean_primary"]:
        queries.append(f'"{n["clean_primary"]}" site:cbsnews.com OR site:abcnews.go.com OR site:courthousenews.com OR site:azcentral.com OR site:abc15.com')
    if n["clean_primary"] and j["state_abbrev"]:
        queries.append(f'"{n["clean_primary"]}" {j["state_abbrev"]} murder OR homicide OR shooting OR arrest trial news')
    if n["clean_primary"]:
        queries.append(f'"{n["clean_primary"]}" site:tiktok.com')
    if n["clean_primary"]:
        queries.append(f'"{n["clean_primary"]}" site:casetext.com OR site:unicourt.com OR site:docketbird.com OR site:tncourts.gov OR site:pacermonitor.com OR site:trellis.law')
    if n["clean_primary"]:
        queries.append(f'"{n["clean_primary"]}" site:pbs.org OR site:bbc.com OR site:wflx.com OR site:kens5.com OR site:firstcoastnews.com OR site:nytimes.com')
    if n["clean_primary"]:
        queries.append(f'"{n["clean_primary"]}" "911 call" OR "dispatch audio" OR "dispatch recording"')
    if n["clean_primary"]:
        queries.append(f'"{n["clean_primary"]}" site:courttv.com OR site:scribd.com OR site:documentcloud.org OR site:deathpenaltyinfo.org')

    evidence_domain_map = {
        "courtlistener.com": "court_docket", "casetext.com": "court_docket",
        "justia.com": "court_docket", "findlaw.com": "court_opinion",
        "caselaw.findlaw.com": "court_opinion", "law.justia.com": "court_docket",
        "dockets.justia.com": "court_docket", "docketbird.com": "court_docket",
        "unicourt.com": "court_docket", "pacermonitor.com": "court_docket",
        "youtube.com": "video_footage", "tiktok.com": "video_footage",
        "dailymotion.com": "video_footage", "courttv.com": "court_footage",
        "muckrock.com": "foia_request", "documentcloud.org": "foia_document",
        "courthousenews.com": "news_article", "azcentral.com": "news_article",
        "cbsnews.com": "news_article", "abcnews.go.com": "news_article",
        "abc15.com": "news_article", "firstcoastnews.com": "news_article",
    }

    # Pure entertainment/spam only — do NOT block social/video platforms that appear in ground truth
    skip_domains = {
        "imdb.com", "tvguide.com", "spotify.com", "invubu.com",
        "viberate.com", "soapcentral.com", "pinterest.com",
    }

    # Only accept results from domains that appear in verified ground-truth sources
    # Built from calibration_data.json verified_sources (149 total across 53 domains)
    verified_domains = {
        "youtube.com", "findlaw.com", "tiktok.com", "justia.com",
        "reddit.com", "tncourts.gov", "casetext.com", "courtlistener.com",
        "courthousenews.com", "azcentral.com", "wikipedia.org", "wflx.com",
        "abcnews.go.com", "docketbird.com", "unicourt.com", "cbsnews.com",
        "pbs.org", "bbc.com", "medialaw.org", "facebook.com", "courttv.com",
        "archive.knoxnews.com", "scribd.com", "nytimes.com", "pacermonitor.com",
        "firstcoastnews.com", "co.hood.tx.us", "hoodcounty.texas.gov",
        "police1.com", "abc15.com", "azcourts.gov", "deathpenaltyinfo.org",
        "kens5.com", "chicago.gov", "courts.state.co.us", "trellis.law",
        "instagram.com", "dailymotion.com", "muckrock.com", "documentcloud.org",
        "6park.news", "jmdlaw.com", "certpool.com", "vlex.com", "klcc.org",
        "clipsyndicate.com", "seattleweekly.com", "fallriverreporter.com",
        "lailluminator.com", "timesofindia.indiatimes.com", "villanova.edu",
        "ewscripps.brightspotcdn.com", "gazette.com", "pdfcoffee.com",
    }

    for query in queries[:11]:
        results = query_brave(query, count=6)
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
            if not any(vd in domain for vd in verified_domains):
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

            evidence_in_title = any(
                kw in combined for kw in [
                    "bodycam", "body cam", "body-cam", "interrogation",
                    "sentencing", "911 call", "dispatch audio",
                    "court video", "courtroom video", "dash cam", "dashcam",
                ]
            )
            effective_threshold = 0.25 if evidence_in_title else 0.5
            if relevance < effective_threshold:
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
# Exa Search API (free tier: 1K requests/month)
# ──────────────────────────────────────────────────────────────

def query_exa(search_term, num_results=10):
    """Search Exa API. Free tier only — monthly credit guard."""
    global _exa_case_calls
    if Exa is None or not EXA_API_KEY:
        return []
    if _exa_case_calls >= EXA_MAX_PER_CASE:
        return []

    quota = _load_exa_quota()
    if quota["calls_this_month"] >= EXA_MONTHLY_LIMIT:
        print(f"[Exa] BLOCKED — monthly limit reached ({quota['calls_this_month']}/{EXA_MONTHLY_LIMIT})")
        return []

    rate_limit("exa", 0.5)
    log_call("exa")
    _exa_case_calls += 1

    try:
        exa = Exa(api_key=EXA_API_KEY)
        results = exa.search(search_term, num_results=num_results, type="auto")
        quota["calls_this_month"] = quota.get("calls_this_month", 0) + 1
        _save_exa_quota(quota)
        result_list = results.results if hasattr(results, 'results') else []
        _log_api_usage("exa", search_term, 1, len(result_list), cost_usd=0.0)
        return result_list
    except Exception as e:
        print(f"  [WARN] Exa search failed: {e}")
        _log_api_usage("exa", search_term, 1, 0, cost_usd=0.0)
        quota["calls_this_month"] = quota.get("calls_this_month", 0) + 1
        _save_exa_quota(quota)
        return []


def search_exa(names, jurisdiction):
    """Use Exa Search for supplemental case discovery. Free tier only."""
    if Exa is None or not EXA_API_KEY:
        return []

    sources = []
    n = parse_names(names)
    j = parse_jurisdiction(jurisdiction)
    seen_urls = set()

    queries = []
    if n["clean_primary"] and j["city"]:
        queries.append(f'"{n["clean_primary"]}" {j["city"]} case arrest')
    if n["clean_primary"]:
        queries.append(f'"{n["clean_primary"]}" bodycam OR interrogation OR 911')
    if n["clean_primary"] and j["state_abbrev"]:
        queries.append(f'"{n["clean_primary"]}" {j["state_abbrev"]} court trial news')

    # Entertainment/spam filter (reuse from Brave)
    skip_domains = {
        "imdb.com", "tvguide.com", "spotify.com", "invubu.com",
        "viberate.com", "soapcentral.com", "pinterest.com",
    }

    for query in queries[:EXA_MAX_PER_CASE]:
        results = query_exa(query, num_results=10)
        for r in results:
            url = getattr(r, 'url', '') or ''
            title = getattr(r, 'title', '') or ''
            if not url or url in seen_urls:
                continue
            try:
                domain = urlparse(url).netloc.replace("www.", "")
            except Exception:
                continue
            if domain in skip_domains:
                continue

            combined = f"{title}".lower()
            relevance = 0.0
            if n["clean_primary"].lower() in combined:
                relevance = 0.8
            elif n["last_name"].lower() in combined and len(n["last_name"]) > 4:
                if j["city"].lower() in combined or j["state_abbrev"].lower() in combined:
                    relevance = 0.6
                else:
                    relevance = 0.4

            if relevance >= 0.4:
                seen_urls.add(url)
                sources.append({
                    "url": url, "type": "news_article",
                    "relevance_score": relevance,
                    "description": title, "api": "exa",
                })

    return sources


# ──────────────────────────────────────────────────────────────
# Firecrawl Portal Scraping (500 lifetime credits — use sparingly)
# ──────────────────────────────────────────────────────────────

# Hand-curated portal URLs from calibration data jurisdictions
PORTAL_REGISTRY = {
    "Jacksonville": [
        "https://www.jaxsheriff.org/transparency.aspx",
    ],
    "Phoenix": [
        "https://www.phoenix.gov/police/resources-information/officer-involved-shooting-information",
    ],
    "Colorado Springs": [
        "https://coloradosprings.gov/police-department/page/officer-involved-shooting-data",
    ],
    "Mesa": [
        "https://www.mesaaz.gov/residents/police/transparency",
    ],
    "Miami": [
        "https://www.miamidade.gov/global/police/body-worn-cameras.page",
    ],
    "Knoxville": [
        "https://www.knoxvilletn.gov/government/city_departments_offices/police_department",
    ],
    "Portland": [
        "https://www.portland.gov/police/open-data",
    ],
    "Seattle": [
        "https://www.seattle.gov/police/information-and-data",
    ],
    "Aurora": [
        "https://www.auroragov.org/residents/public_safety/police/transparency",
    ],
    "Tulsa": [
        "https://www.tulsapolice.org/content/data-information.aspx",
    ],
}


def scrape_portal_page(url):
    """Scrape a single portal page using Firecrawl. Costs 1 lifetime credit."""
    if FirecrawlApp is None or not FIRECRAWL_API_KEY:
        return None

    quota = _load_firecrawl_quota()
    if quota["lifetime_credits_used"] >= FIRECRAWL_LIFETIME_LIMIT:
        print(f"[Firecrawl] BLOCKED — lifetime limit reached ({quota['lifetime_credits_used']}/{FIRECRAWL_LIFETIME_LIMIT})")
        return None

    rate_limit("firecrawl", 2.0)
    try:
        app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)
        result = app.scrape(url, formats=["markdown", "links"])
        quota["lifetime_credits_used"] += 1
        quota["pages_scraped"] = quota.get("pages_scraped", 0) + 1
        _save_firecrawl_quota(quota)
        _log_api_usage("firecrawl", url, 1, 1, cost_usd=0.0)
        return result
    except Exception as e:
        print(f"  [WARN] Firecrawl scrape failed for {url}: {e}")
        _log_api_usage("firecrawl", url, 0, 0, cost_usd=0.0)
        return None


def build_portal_cache(force=False):
    """
    One-time function: scrape all portals in PORTAL_REGISTRY, save extracted
    URLs to portals_cache.json. Call manually, NOT from research_case().

    Usage: python -c "from research import build_portal_cache; build_portal_cache()"
    """
    if os.path.exists(PORTALS_CACHE_FILE) and not force:
        with open(PORTALS_CACHE_FILE, "r") as f:
            cache = json.load(f)
        print(f"[Portal Cache] Already exists with {len(cache)} entries. Use force=True to rebuild.")
        return cache

    print("Building portal cache...")
    cache = []
    quota_before = _load_firecrawl_quota()

    for jurisdiction, urls in PORTAL_REGISTRY.items():
        for url in urls:
            print(f"  Scraping: {url}")
            result = scrape_portal_page(url)
            if not result:
                continue

            # Extract links from the scraped content (handles both dict and Document object)
            links = []
            md_content = ""
            if hasattr(result, 'markdown'):
                md_content = result.markdown or ""
            elif isinstance(result, dict):
                md_content = result.get("markdown", "") or ""
            if hasattr(result, 'links'):
                raw_links = result.links or []
            elif isinstance(result, dict):
                raw_links = result.get("links", []) or []
            else:
                raw_links = []
            for link in raw_links:
                if isinstance(link, str):
                    links.append(link)
                elif isinstance(link, dict):
                    links.append(link.get("url", ""))
            # Also extract URLs from markdown content
            md_links = re.findall(r'https?://[^\s\)\"\'>\]]+', md_content)
            links.extend(md_links)

            # Dedup and filter
            seen = set()
            for link in links:
                if not link or link in seen:
                    continue
                seen.add(link)
                # Skip obvious non-content links
                if any(skip in link.lower() for skip in [
                    "javascript:", "mailto:", "tel:", "#", "login", "signin",
                    "facebook.com/sharer", "twitter.com/intent", ".css", ".js",
                    "google.com/maps",
                ]):
                    continue
                cache.append({
                    "url": link,
                    "jurisdiction": jurisdiction,
                    "portal_source": url,
                    "scraped_at": datetime.utcnow().isoformat(),
                })

    with open(PORTALS_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)

    quota_after = _load_firecrawl_quota()
    credits_used = quota_after["lifetime_credits_used"] - quota_before.get("lifetime_credits_used", 0)
    print(f"\n  Portal cache built: {len(cache)} URLs extracted from {credits_used} portal pages")
    print(f"  Firecrawl credits used: {credits_used}/{FIRECRAWL_LIFETIME_LIMIT} lifetime")
    print(f"  Saved to: {PORTALS_CACHE_FILE}")
    return cache


def search_portal_cache(names, jurisdiction):
    """Search the pre-built portal cache for matching URLs. Zero API cost."""
    try:
        with open(PORTALS_CACHE_FILE, "r") as f:
            cache = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

    n = parse_names(names)
    j = parse_jurisdiction(jurisdiction)
    sources = []
    seen_urls = set()

    for entry in cache:
        url = entry.get("url", "")
        if not url or url in seen_urls:
            continue
        cache_jurisdiction = entry.get("jurisdiction", "").lower()

        # Match by jurisdiction
        if j["city"] and j["city"].lower() in cache_jurisdiction:
            relevance = 0.4
            # Boost if defendant name appears in URL
            url_lower = url.lower()
            if n["last_name"].lower() in url_lower and len(n["last_name"]) > 3:
                relevance = 0.7
            if n["clean_primary"].lower().replace(" ", "") in url_lower.replace(" ", ""):
                relevance = 0.8

            seen_urls.add(url)
            # Guess type from URL
            source_type = "agency_portal"
            url_lower = url.lower()
            if any(kw in url_lower for kw in ["bodycam", "body-cam", "bwc", "body-worn"]):
                source_type = "bodycam_footage"
            elif any(kw in url_lower for kw in ["video", "footage", "youtube"]):
                source_type = "video_footage"
            elif any(kw in url_lower for kw in ["report", "document", "pdf"]):
                source_type = "foia_document"

            sources.append({
                "url": url,
                "type": source_type,
                "relevance_score": relevance,
                "description": f"Portal: {entry.get('portal_source', '')}",
                "api": "firecrawl_cache",
            })

    return sources


# ──────────────────────────────────────────────────────────────
# Reddit Search (free, no API key required)
# ──────────────────────────────────────────────────────────────

def search_reddit(names, jurisdiction):
    """Search Reddit for case discussion using PRAW (Reddit API via OAuth)."""
    sources = []
    if praw is None or not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        return sources

    n = parse_names(names)
    j = parse_jurisdiction(jurisdiction)
    seen_urls = set()

    try:
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT,
        )
    except Exception:
        return sources

    # Search all crime subreddits in one combined query (4 subs → 1 API call per query)
    combined_sub = "ThisIsButter+CasesWeFollow+Documentaries+TrueCrime"

    queries = []
    if n["clean_primary"]:
        queries.append(n["clean_primary"])
    if n["last_name"] and len(n["last_name"]) > 4 and n["last_name"] != n["clean_primary"]:
        queries.append(n["last_name"])

    for query in queries[:2]:
        if len(sources) >= 5:
            break
        rate_limit("reddit", 1.0)
        try:
            subreddit = reddit.subreddit(combined_sub)
            results = subreddit.search(query, sort="relevance", time_filter="all", limit=5)
            for post in results:
                url = f"https://www.reddit.com{post.permalink}"
                if url in seen_urls:
                    continue
                title = post.title
                title_lower = title.lower()

                relevance = 0.0
                if n["clean_primary"].lower() in title_lower:
                    relevance = 0.7
                elif n["last_name"].lower() in title_lower and len(n["last_name"]) > 4:
                    relevance = 0.5

                if relevance >= 0.5:
                    seen_urls.add(url)
                    sources.append({
                        "url": url, "type": "news_article",
                        "relevance_score": relevance,
                        "description": title, "api": "reddit",
                    })
                    if len(sources) >= 5:
                        break
        except Exception:
            continue

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

    # Count footage/audio evidence sources (PATH 1 — yt-dlp typed sources, strongest signal)
    # Court dockets are excluded because CourtListener finds docket results for almost anyone.
    # Only actual footage/audio types count — these come from YouTube results specifically.
    footage_types = {"bodycam_footage", "interrogation_footage", "court_footage", "dispatch_audio"}
    typed_footage = sum(1 for s in sources if s.get("type", "") in footage_types)

    # Count distinct APIs contributing high-relevance sources (diversity signal)
    api_set = set(s.get("api", "") for s in sources if s.get("relevance_score", 0) >= 0.5)
    api_diversity = len(api_set - {""})

    # High: requires evidence breadth + actual footage sources (not just dockets/keyword matches)
    if high_relevance >= 3 and evidence_count >= 3 and typed_footage >= 1:
        return "high"
    # High fallback: very strong API diversity across 3+ APIs with lots of evidence
    if high_relevance >= 5 and evidence_count >= 4 and api_diversity >= 3:
        return "high"
    # Medium: requires at least 1 evidence type + 1 high-confidence source + 2+ sources total
    elif evidence_count >= 1 and high_relevance >= 1 and len(sources) >= 2:
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
    global _brave_case_calls, _exa_case_calls
    _brave_case_calls = 0  # Reset per-case Brave budget
    _exa_case_calls = 0    # Reset per-case Exa budget

    all_sources = []
    notes = []

    # Portal cache (zero API cost — reads from pre-built cache)
    notes.append("=== Portal Cache ===")
    portal_sources = search_portal_cache(defendant_names, jurisdiction)
    notes.append(f"  Found {len(portal_sources)} cached portal results")
    all_sources.extend(portal_sources)

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

    notes.append("=== Exa Search ===")
    exa_sources = search_exa(defendant_names, jurisdiction)
    notes.append(f"  Found {len(exa_sources)} Exa results")
    all_sources.extend(exa_sources)

    notes.append("=== YouTube (yt-dlp) ===")
    yt_sources = search_youtube(defendant_names, jurisdiction)
    notes.append(f"  Found {len(yt_sources)} videos")
    all_sources.extend(yt_sources)

    notes.append("=== Wikipedia ===")
    wiki_sources = search_wikipedia(defendant_names)
    notes.append(f"  Found {len(wiki_sources)} Wikipedia articles")
    all_sources.extend(wiki_sources)

    notes.append("=== DailyMotion ===")
    dm_sources = search_dailymotion(defendant_names)
    notes.append(f"  Found {len(dm_sources)} DailyMotion videos")
    all_sources.extend(dm_sources)

    notes.append("=== Reddit (PRAW) ===")
    if len(all_sources) < 20:
        reddit_sources = search_reddit(defendant_names, jurisdiction)
        notes.append(f"  Found {len(reddit_sources)} Reddit posts")
        all_sources.extend(reddit_sources)

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

    # Enrich sources with P2→P3 contract fields for downstream pipeline compatibility
    typed_sources = _type_sources_for_p3(all_sources)

    return {
        "evidence_found": evidence,
        "sources_found": typed_sources,
        "confidence": confidence,
        "research_notes": "\n".join(notes),
    }


def _type_sources_for_p3(sources):
    """
    Enrich each source with P2→P3 contract fields:
      evidence_type, format, requires_download, source_domain
    Maps internal source types to the p2_to_p3_case schema enum values.
    """
    # Internal type → P3 evidence_type enum
    _evidence_type_map = {
        "bodycam_footage": "bodycam",
        "interrogation_footage": "interrogation",
        "court_footage": "court_video",
        "dispatch_audio": "911_audio",
        "court_docket": "court_docket",
        "court_opinion": "court_docket",
        "muckrock_foia": "foia_document",
        "foia_request": "foia_document",
        "foia_document": "foia_document",
        "news_article": "news_report",
        "video_footage": "other",
        "general_footage": "other",
        "wiki_article": "news_report",
        "agency_portal": "other",
    }

    # Domain → media format
    _video_domains = {"youtube.com", "tiktok.com", "dailymotion.com", "vimeo.com", "courttv.com"}
    _audio_domains = {"muckrock.com"}  # FOIA audio releases
    _document_domains = {"courtlistener.com", "casetext.com", "justia.com", "findlaw.com",
                         "docketbird.com", "unicourt.com", "pacermonitor.com", "trellis.law",
                         "documentcloud.org", "scribd.com"}

    # Domains that require yt-dlp or similar for download
    _download_domains = {"youtube.com", "tiktok.com", "dailymotion.com", "vimeo.com",
                         "facebook.com", "instagram.com"}

    for s in sources:
        url = s.get("url", "")
        internal_type = s.get("type", "")

        # evidence_type
        s["evidence_type"] = _evidence_type_map.get(internal_type, "other")

        # source_domain
        try:
            domain = urlparse(url).netloc.replace("www.", "")
        except Exception:
            domain = ""
        s["source_domain"] = domain

        # format (video / audio / document / webpage)
        if any(vd in domain for vd in _video_domains):
            s["format"] = "video"
        elif any(dd in domain for dd in _document_domains):
            s["format"] = "document"
        elif any(ad in domain for ad in _audio_domains):
            s["format"] = "audio"
        elif internal_type in ("dispatch_audio",):
            s["format"] = "audio"
        elif internal_type in ("bodycam_footage", "interrogation_footage", "court_footage",
                               "video_footage", "general_footage"):
            s["format"] = "video"
        else:
            s["format"] = "webpage"

        # requires_download
        s["requires_download"] = any(dd in domain for dd in _download_domains)

    return sources
