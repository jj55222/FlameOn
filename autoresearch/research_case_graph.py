"""
research_case_graph.py — FlameOn Case-Graph Research Harness
=============================================================

A safer fork of autoresearch/research.py that changes the unit of work from
"flat search results" to a structured case graph:

1. Identity lock: prove the case/person before evidence hunting.
2. Artifact claims: treat evidence mentions as unverified claims.
3. Artifact resolvers: try to turn claims into actual public artifact URLs.
4. Compatibility export: preserve evaluate.py's expected keys:
   - evidence_found
   - sources_found
   - confidence

This file is intended to be tested side-by-side with research.py:

    cp research_case_graph.py research.py
    python evaluate.py --case 1 --verbose

or imported directly from a new evaluator.

Important: this script only inspects public pages/search results. It is not for
bypassing authentication, private Axon/Evidence.com portals, or protected evidence systems.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote_plus, urlparse

import requests

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

if load_dotenv:
    load_dotenv()

# ──────────────────────────────────────────────────────────────
# API configuration
# ──────────────────────────────────────────────────────────────

BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY", "")
COURTLISTENER_API_KEY = os.environ.get("COURTLISTENER_API_KEY", "")
MUCKROCK_API_TOKEN = os.environ.get("MUCKROCK_API_TOKEN", "")
FIRECRAWL_API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")

BRAVE_BASE = "https://api.search.brave.com/res/v1/web/search"
COURTLISTENER_BASE = "https://www.courtlistener.com/api/rest/v4/"
MUCKROCK_BASE = "https://www.muckrock.com/api_v2/"
FIRECRAWL_SCRAPE_URL = "https://api.firecrawl.dev/v1/scrape"

REQUEST_TIMEOUT = 15

# Keep this fork cheap by default. Let the old research.py do broad recall tests.
BRAVE_PER_CASE_LIMIT = int(os.environ.get("FLAMEON_BRAVE_PER_CASE_LIMIT", "6"))
SEARCH_RESULT_COUNT = int(os.environ.get("FLAMEON_SEARCH_RESULT_COUNT", "5"))
ENABLE_FIRECRAWL = os.environ.get("FLAMEON_ENABLE_FIRECRAWL", "0") == "1"

_api_counts = {"brave": 0, "courtlistener": 0, "muckrock": 0, "youtube": 0, "firecrawl": 0}
_last_call_at = {k: 0.0 for k in _api_counts}

EVIDENCE_TYPES = ["bodycam", "interrogation", "court_video", "docket_docs", "dispatch_911"]

STATE_ABBREVS = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY", "District of Columbia": "DC",
}

OFFICIAL_DOMAIN_HINTS = [
    ".gov", ".us", "police", "sheriff", "courts", "court", "clerk",
    "da.", "districtattorney", "prosecutor", "cityof", "county",
]

ARTIFACT_HOST_HINTS = [
    "youtube.com", "youtu.be", "vimeo.com", "muckrock.com", "documentcloud.org",
    "courtlistener.com", "nextrequest.com", "govqa", "justfoia", "publicrecords",
    "evidence.com", "axon", "dropbox.com", "box.com", "s3.amazonaws.com",
]

ENTERTAINMENT_OR_NOISE_DOMAINS = {
    "imdb.com", "spotify.com", "tvguide.com", "soapcentral.com", "genius.com",
    "fandom.com", "wikipedia.org", "amazon.com", "goodreads.com",
}

ARTIFACT_PATTERNS = {
    "bodycam": [
        r"body[- ]?cam", r"body[- ]?worn camera", r"\bBWC\b", r"dash[- ]?cam",
        r"critical incident video", r"officer[- ]?involved shooting video",
        r"police footage", r"arrest footage",
    ],
    "interrogation": [
        r"interrogation", r"confession", r"custodial interview", r"police interview",
        r"detective interview", r"interview recording", r"interview video",
    ],
    "court_video": [
        r"court video", r"court audio", r"trial video", r"sentencing video",
        r"hearing video", r"courtroom video", r"Court TV", r"oral argument",
    ],
    "docket_docs": [
        r"docket", r"complaint", r"affidavit", r"indictment", r"information filed",
        r"probable cause", r"charging document", r"case number", r"court filing",
    ],
    "dispatch_911": [
        r"911 call", r"911 audio", r"dispatch audio", r"dispatch recording",
        r"emergency call", r"called 911",
    ],
}

OUTCOME_PATTERNS = {
    "sentenced": [r"sentenced to", r"was sentenced", r"sentence imposed", r"life in prison", r"years in prison"],
    "convicted": [r"convicted", r"found guilty", r"guilty verdict", r"pleaded guilty", r"pled guilty"],
    "charged": [r"charged with", r"arrested", r"indicted", r"arraigned"],
}

CASE_NUMBER_RE = re.compile(r"\b(?:case\s*(?:no\.?|number)?\s*[:#]?\s*)?([A-Z]{0,4}\d{2,4}[- ]?[A-Z]{0,4}[- ]?\d{2,8}|\d{2,4}[- ]?[A-Z]{1,4}[- ]?\d{2,8})\b", re.I)
DATE_RE = re.compile(r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}\b", re.I)


# ──────────────────────────────────────────────────────────────
# Data model
# ──────────────────────────────────────────────────────────────

@dataclass
class Jurisdiction:
    raw: str
    city: str = ""
    county: str = ""
    state: str = ""
    state_abbrev: str = ""


@dataclass
class Source:
    url: str
    title: str = ""
    snippet: str = ""
    source_type: str = "web"
    api: str = ""
    relevance_score: float = 0.0
    authority: str = "unknown"  # official|court|foia|news|third_party|unknown
    matched_fields: List[str] = field(default_factory=list)

    def text(self) -> str:
        return f"{self.title} {self.snippet} {self.url}".strip()


@dataclass
class ArtifactClaim:
    artifact_type: str
    claim_source_url: str
    claim_text: str
    verification_status: str = "unverified"  # unverified|artifact_page|verified
    confidence: float = 0.0


@dataclass
class VerifiedArtifact:
    artifact_type: str
    artifact_url: str
    host: str
    source_authority: str
    downloadable: bool
    matched_case_fields: List[str] = field(default_factory=list)
    confidence: float = 0.0


@dataclass
class CaseIdentity:
    defendant_names: List[str]
    jurisdiction: Jurisdiction
    victim_names: List[str] = field(default_factory=list)
    agencies: List[str] = field(default_factory=list)
    incident_dates: List[str] = field(default_factory=list)
    sentencing_dates: List[str] = field(default_factory=list)
    charges: List[str] = field(default_factory=list)
    case_numbers: List[str] = field(default_factory=list)
    outcome_status: str = "unknown"  # unknown|charged|convicted|sentenced
    identity_confidence: str = "low"  # low|medium|high
    identity_anchors: List[str] = field(default_factory=list)


@dataclass
class CaseGraph:
    case_identity: CaseIdentity
    sources: List[Source] = field(default_factory=list)
    artifact_claims: List[ArtifactClaim] = field(default_factory=list)
    verified_artifacts: List[VerifiedArtifact] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def add_source(self, source: Source) -> None:
        if not source.url:
            return
        normalized = normalize_url(source.url)
        if any(normalize_url(s.url) == normalized for s in self.sources):
            return
        self.sources.append(source)

    def add_claim(self, claim: ArtifactClaim) -> None:
        key = (claim.artifact_type, normalize_url(claim.claim_source_url), claim.claim_text[:80].lower())
        for existing in self.artifact_claims:
            existing_key = (existing.artifact_type, normalize_url(existing.claim_source_url), existing.claim_text[:80].lower())
            if existing_key == key:
                return
        self.artifact_claims.append(claim)

    def add_verified_artifact(self, artifact: VerifiedArtifact) -> None:
        normalized = normalize_url(artifact.artifact_url)
        if any(normalize_url(a.artifact_url) == normalized and a.artifact_type == artifact.artifact_type for a in self.verified_artifacts):
            return
        self.verified_artifacts.append(artifact)

    def legacy_export(self) -> Dict[str, Any]:
        evidence_found = {etype: False for etype in EVIDENCE_TYPES}

        # Verified artifacts carry the most weight.
        for artifact in self.verified_artifacts:
            evidence_found[artifact.artifact_type] = True

        # Backward compatibility: a strong docket/court source can count as docket_docs.
        for source in self.sources:
            if source.source_type in {"court_docket", "court_opinion", "foia_request"} and source.relevance_score >= 0.70:
                evidence_found["docket_docs"] = True

        confidence = assess_graph_confidence(self)
        return {
            "evidence_found": evidence_found,
            "sources_found": [asdict(s) for s in self.sources],
            "confidence": confidence,
            "case_identity": asdict(self.case_identity),
            "artifact_claims": [asdict(c) for c in self.artifact_claims],
            "verified_artifacts": [asdict(a) for a in self.verified_artifacts],
            "artifact_status": artifact_status(self),
            "notes": self.notes,
            "budget_report": get_budget_report(),
        }


# ──────────────────────────────────────────────────────────────
# Utility helpers
# ──────────────────────────────────────────────────────────────

def reset_budget() -> None:
    global _api_counts, _last_call_at
    _api_counts = {"brave": 0, "courtlistener": 0, "muckrock": 0, "youtube": 0, "firecrawl": 0}
    _last_call_at = {k: 0.0 for k in _api_counts}


def get_budget_report() -> Dict[str, int]:
    return dict(_api_counts)


def rate_limit(api: str, delay: float) -> None:
    elapsed = time.time() - _last_call_at.get(api, 0.0)
    if elapsed < delay:
        time.sleep(delay - elapsed)
    _last_call_at[api] = time.time()


def normalize_url(url: str) -> str:
    if not url:
        return ""
    return url.strip().split("#")[0].rstrip("/")


def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def authority_for_url(url: str) -> str:
    d = domain_of(url)
    if not d:
        return "unknown"
    if "courtlistener" in d or "court" in d or "clerk" in d:
        return "court"
    if "muckrock" in d or "nextrequest" in d or "govqa" in d or "justfoia" in d or "publicrecords" in d:
        return "foia"
    if d.endswith(".gov") or any(h in d for h in OFFICIAL_DOMAIN_HINTS):
        return "official"
    if "youtube" in d or "youtu.be" in d or "vimeo" in d or "documentcloud" in d:
        return "third_party"
    if any(x in d for x in ["news", "nbc", "abc", "cbs", "fox", "cnn", "apnews", "courthousenews", "lawandcrime"]):
        return "news"
    return "unknown"


def parse_names(defendant_names: str) -> Dict[str, Any]:
    names = [n.strip() for n in re.split(r",|;|\band\b", defendant_names or "") if n.strip()]
    primary = names[0] if names else (defendant_names or "").strip()
    suffixes = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}
    parts = [p.strip() for p in primary.split() if p.strip()]
    cleaned = [p for p in parts if p.lower() not in suffixes]
    last = cleaned[-1] if cleaned else (parts[-1] if parts else "")
    return {
        "all_names": names or ([primary] if primary else []),
        "primary": primary,
        "clean_primary": " ".join(cleaned) or primary,
        "last_name": last,
    }


def parse_jurisdiction(jurisdiction: str) -> Jurisdiction:
    parts = [p.strip() for p in (jurisdiction or "").split(",") if p.strip()]
    city = parts[0] if parts else ""
    state = parts[-1] if len(parts) >= 2 else ""
    county = ""
    if len(parts) >= 3:
        county = parts[1]
    elif len(parts) == 2 and "county" in parts[0].lower():
        county = parts[0]
        city = ""
    state_abbrev = STATE_ABBREVS.get(state, state if len(state) == 2 else "")
    return Jurisdiction(raw=jurisdiction or "", city=city, county=county, state=state, state_abbrev=state_abbrev)


def source_matches_case(source: Source, names: Dict[str, Any], jurisdiction: Jurisdiction) -> Tuple[float, List[str]]:
    text = source.text().lower()
    score = 0.0
    matched: List[str] = []
    clean_name = names["clean_primary"].lower()
    last_name = names["last_name"].lower()

    if clean_name and clean_name in text:
        score += 0.50
        matched.append("defendant_full_name")
    elif last_name and len(last_name) > 3 and re.search(rf"\b{re.escape(last_name)}\b", text):
        score += 0.25
        matched.append("defendant_last_name")

    if jurisdiction.city and jurisdiction.city.lower() in text:
        score += 0.18
        matched.append("city")
    if jurisdiction.county and jurisdiction.county.lower().replace(" county", "") in text:
        score += 0.14
        matched.append("county")
    if jurisdiction.state and jurisdiction.state.lower() in text:
        score += 0.12
        matched.append("state")
    elif jurisdiction.state_abbrev and re.search(rf"\b{re.escape(jurisdiction.state_abbrev.lower())}\b", text):
        score += 0.08
        matched.append("state_abbrev")

    authority = authority_for_url(source.url)
    if authority in {"official", "court", "foia"}:
        score += 0.12
        matched.append(f"authority:{authority}")

    domain = domain_of(source.url)
    if any(noise in domain for noise in ENTERTAINMENT_OR_NOISE_DOMAINS):
        score -= 0.50
        matched.append("noise_domain")
    if any(flag in text for flag in ["movie", "trailer", "anime", "lyrics", "soundtrack", "gameplay"]):
        score -= 0.30
        matched.append("entertainment_text")

    return max(0.0, min(1.0, score)), matched


def compiled_patterns(patterns: Sequence[str]) -> Iterable[re.Pattern[str]]:
    for pattern in patterns:
        yield re.compile(pattern, re.I)


# ──────────────────────────────────────────────────────────────
# Discovery APIs
# ──────────────────────────────────────────────────────────────

def search_brave(query: str, count: int = SEARCH_RESULT_COUNT) -> List[Source]:
    if not BRAVE_API_KEY or _api_counts["brave"] >= BRAVE_PER_CASE_LIMIT:
        return []
    rate_limit("brave", 1.1)
    _api_counts["brave"] += 1
    try:
        resp = requests.get(
            BRAVE_BASE,
            params={"q": query, "count": count, "search_lang": "en", "safesearch": "moderate"},
            headers={"Accept": "application/json", "X-Subscription-Token": BRAVE_API_KEY},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code >= 400:
            return []
        data = resp.json()
    except Exception:
        return []

    results: List[Source] = []
    for item in data.get("web", {}).get("results", []) or []:
        url = item.get("url", "")
        if not url:
            continue
        results.append(Source(
            url=url,
            title=item.get("title", "") or "",
            snippet=item.get("description", "") or "",
            source_type="web_search",
            api="brave",
            authority=authority_for_url(url),
        ))
    return results


def search_courtlistener(names: Dict[str, Any], jurisdiction: Jurisdiction) -> List[Source]:
    if not COURTLISTENER_API_KEY:
        return []
    queries = [names["clean_primary"]]
    if jurisdiction.state_abbrev:
        queries.append(f"{names['clean_primary']} {jurisdiction.state_abbrev}")
    sources: List[Source] = []
    for query in [q for q in queries if q][:2]:
        for search_type, source_type in [("r", "court_docket"), ("o", "court_opinion")]:
            rate_limit("courtlistener", 3.0)
            _api_counts["courtlistener"] += 1
            try:
                resp = requests.get(
                    f"{COURTLISTENER_BASE}search/",
                    params={"q": query, "type": search_type, "format": "json", "page_size": 5},
                    headers={"Authorization": f"Token {COURTLISTENER_API_KEY}"},
                    timeout=REQUEST_TIMEOUT,
                )
                if resp.status_code >= 400:
                    continue
                data = resp.json()
            except Exception:
                continue
            for r in data.get("results", []) or []:
                title = r.get("caseName") or r.get("case_name") or ""
                url = r.get("absolute_url") or ""
                if url and not url.startswith("http"):
                    url = f"https://www.courtlistener.com{url}"
                if not url:
                    continue
                sources.append(Source(
                    url=url,
                    title=title,
                    snippet=r.get("snippet", "") or "",
                    source_type=source_type,
                    api="courtlistener",
                    authority="court",
                ))
    return sources


def search_muckrock(names: Dict[str, Any], jurisdiction: Jurisdiction) -> List[Source]:
    queries = [names["clean_primary"]]
    if jurisdiction.city:
        queries.append(f"{names['clean_primary']} {jurisdiction.city}")
    if jurisdiction.state_abbrev:
        queries.append(f"{names['clean_primary']} {jurisdiction.state_abbrev}")
    sources: List[Source] = []
    headers: Dict[str, str] = {}
    if MUCKROCK_API_TOKEN:
        headers["Authorization"] = f"Token {MUCKROCK_API_TOKEN}"

    for query in [q for q in queries if q][:3]:
        rate_limit("muckrock", 1.1)
        _api_counts["muckrock"] += 1
        try:
            resp = requests.get(
                f"{MUCKROCK_BASE}foia/",
                params={"format": "json", "search": query, "status": "done", "page_size": 8},
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code >= 400:
                continue
            data = resp.json()
        except Exception:
            continue
        for item in data.get("results", []) or []:
            url = item.get("absolute_url") or item.get("url") or ""
            if not url:
                continue
            sources.append(Source(
                url=url,
                title=item.get("title", "") or "",
                snippet=item.get("description", "") or "",
                source_type="foia_request",
                api="muckrock",
                authority="foia",
            ))
    return sources


def search_youtube(names: Dict[str, Any], jurisdiction: Jurisdiction) -> List[Source]:
    try:
        import yt_dlp  # type: ignore
    except Exception:
        return []

    queries = [
        f"{names['clean_primary']} bodycam",
        f"{names['clean_primary']} interrogation",
        f"{names['clean_primary']} sentencing",
    ]
    if jurisdiction.city:
        queries.append(f"{names['clean_primary']} {jurisdiction.city} police")

    ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": True, "skip_download": True, "socket_timeout": 8}
    sources: List[Source] = []
    seen_ids: set[str] = set()
    for query in [q for q in queries if q][:4]:
        rate_limit("youtube", 1.0)
        _api_counts["youtube"] += 1
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(f"ytsearch5:{query}", download=False)
        except Exception:
            continue
        for item in (info or {}).get("entries", []) or []:
            video_id = item.get("id", "")
            if not video_id or video_id in seen_ids:
                continue
            seen_ids.add(video_id)
            url = f"https://www.youtube.com/watch?v={video_id}"
            sources.append(Source(
                url=url,
                title=item.get("title", "") or "",
                snippet=item.get("description", "") or item.get("channel", "") or "",
                source_type="video_search",
                api="youtube",
                authority=authority_for_url(url),
            ))
    return sources


def firecrawl_extract(url: str) -> str:
    """Optional public-page extraction. Off by default to avoid credit burn."""
    if not ENABLE_FIRECRAWL or not FIRECRAWL_API_KEY:
        return ""
    rate_limit("firecrawl", 1.0)
    _api_counts["firecrawl"] += 1
    try:
        resp = requests.post(
            FIRECRAWL_SCRAPE_URL,
            headers={"Authorization": f"Bearer {FIRECRAWL_API_KEY}", "Content-Type": "application/json"},
            json={"url": url, "formats": ["markdown"], "onlyMainContent": True},
            timeout=25,
        )
        if resp.status_code >= 400:
            return ""
        data = resp.json()
        return (data.get("data", {}) or {}).get("markdown", "") or ""
    except Exception:
        return ""


# ──────────────────────────────────────────────────────────────
# Case graph extraction
# ──────────────────────────────────────────────────────────────

def discover_identity_sources(defendant_names: str, jurisdiction: str) -> CaseGraph:
    names = parse_names(defendant_names)
    juris = parse_jurisdiction(jurisdiction)
    identity = CaseIdentity(defendant_names=names["all_names"], jurisdiction=juris)
    graph = CaseGraph(case_identity=identity)

    queries = [
        f"\"{names['clean_primary']}\" {juris.city} {juris.state} charged convicted sentenced",
        f"\"{names['clean_primary']}\" {juris.city} police court case",
        f"\"{names['clean_primary']}\" {juris.state_abbrev} docket",
        f"\"{names['clean_primary']}\" bodycam interrogation 911",
    ]

    raw_sources: List[Source] = []
    raw_sources.extend(search_courtlistener(names, juris))
    raw_sources.extend(search_muckrock(names, juris))
    for q in [q for q in queries if q.strip()]:
        raw_sources.extend(search_brave(q))
    raw_sources.extend(search_youtube(names, juris))

    for source in raw_sources:
        score, matched = source_matches_case(source, names, juris)
        source.relevance_score = score
        source.matched_fields = matched
        source.authority = source.authority or authority_for_url(source.url)
        if score >= 0.35:
            graph.add_source(source)

    extract_identity_fields(graph, names)
    return graph


def extract_identity_fields(graph: CaseGraph, names: Dict[str, Any]) -> None:
    text_blob = "\n".join(s.text() for s in graph.sources)
    lower = text_blob.lower()
    identity = graph.case_identity

    for match in CASE_NUMBER_RE.findall(text_blob):
        cleaned = re.sub(r"\s+", "", match.strip())
        if 5 <= len(cleaned) <= 25 and cleaned not in identity.case_numbers:
            identity.case_numbers.append(cleaned)

    for d in DATE_RE.findall(text_blob):
        if d not in identity.incident_dates:
            identity.incident_dates.append(d)

    # Agency hints: not perfect, but useful as identity anchors.
    agency_patterns = [
        r"([A-Z][A-Za-z ]+ Police Department)",
        r"([A-Z][A-Za-z ]+ Sheriff's Office)",
        r"([A-Z][A-Za-z ]+ County Sheriff(?:'s)? Department)",
        r"([A-Z][A-Za-z ]+ District Attorney(?:'s)? Office)",
    ]
    for pattern in agency_patterns:
        for agency in re.findall(pattern, text_blob):
            agency = agency.strip()
            if 5 < len(agency) < 80 and agency not in identity.agencies:
                identity.agencies.append(agency)

    for status, patterns in OUTCOME_PATTERNS.items():
        if any(re.search(p, lower, re.I) for p in patterns):
            if status == "sentenced" or identity.outcome_status == "unknown":
                identity.outcome_status = status
            elif status == "convicted" and identity.outcome_status not in {"sentenced"}:
                identity.outcome_status = status
            elif status == "charged" and identity.outcome_status == "unknown":
                identity.outcome_status = status

    # Crude charge extraction from snippets/titles.
    for charge_word in ["murder", "homicide", "manslaughter", "robbery", "kidnapping", "assault", "shooting", "DUI", "burglary"]:
        if re.search(rf"\b{re.escape(charge_word)}\b", text_blob, re.I) and charge_word not in identity.charges:
            identity.charges.append(charge_word)

    anchors: List[str] = []
    full_name_hits = sum(1 for s in graph.sources if "defendant_full_name" in s.matched_fields)
    authority_hits = sum(1 for s in graph.sources if any(m.startswith("authority:") for m in s.matched_fields))
    jurisdiction_hits = sum(1 for s in graph.sources if any(m in s.matched_fields for m in ["city", "county", "state", "state_abbrev"]))

    if full_name_hits >= 1:
        anchors.append("full_name")
    if jurisdiction_hits >= 1:
        anchors.append("jurisdiction")
    if authority_hits >= 1:
        anchors.append("authoritative_source")
    if identity.case_numbers:
        anchors.append("case_number")
    if identity.agencies:
        anchors.append("agency")
    if identity.incident_dates:
        anchors.append("date")
    if identity.outcome_status != "unknown":
        anchors.append(f"outcome:{identity.outcome_status}")

    identity.identity_anchors = anchors
    if "full_name" in anchors and len(set(anchors)) >= 3:
        identity.identity_confidence = "high"
    elif ("full_name" in anchors and "jurisdiction" in anchors) or len(set(anchors)) >= 3:
        identity.identity_confidence = "medium"
    else:
        identity.identity_confidence = "low"

    if identity.identity_confidence == "low":
        graph.notes.append("identity_not_locked: only weak source agreement found")


def extract_artifact_claims(graph: CaseGraph) -> None:
    for source in graph.sources:
        text = source.text()
        if source.api == "firecrawl":
            expanded = text
        else:
            expanded_text = firecrawl_extract(source.url) if source.relevance_score >= 0.70 and source.authority in {"official", "foia", "court"} else ""
            expanded = f"{text}\n{expanded_text}" if expanded_text else text

        for artifact_type, patterns in ARTIFACT_PATTERNS.items():
            for pattern in compiled_patterns(patterns):
                match = pattern.search(expanded)
                if not match:
                    continue
                start = max(0, match.start() - 120)
                end = min(len(expanded), match.end() + 160)
                claim_text = re.sub(r"\s+", " ", expanded[start:end]).strip()
                confidence = 0.35 + min(source.relevance_score, 0.50)
                if source.authority in {"official", "court", "foia"}:
                    confidence += 0.10
                graph.add_claim(ArtifactClaim(
                    artifact_type=artifact_type,
                    claim_source_url=source.url,
                    claim_text=claim_text[:600],
                    confidence=min(confidence, 0.95),
                ))
                break


def resolve_artifact_claims(graph: CaseGraph) -> None:
    """Resolve claims into concrete public artifact URLs when the claim source itself or nearby sources are artifact hosts."""
    if not graph.artifact_claims:
        return

    candidate_sources = list(graph.sources)

    # Targeted search from the locked identity and claim type.
    names = parse_names(", ".join(graph.case_identity.defendant_names))
    juris = graph.case_identity.jurisdiction
    if graph.case_identity.identity_confidence != "low":
        for claim in graph.artifact_claims[:8]:
            query_bits = [f"\"{names['clean_primary']}\"", juris.city, claim.artifact_type.replace("_", " ")]
            if claim.artifact_type == "bodycam":
                query_bits.extend(["bodycam OR \"critical incident video\"", "YouTube"])
            elif claim.artifact_type == "interrogation":
                query_bits.extend(["interrogation OR confession", "video"])
            elif claim.artifact_type == "dispatch_911":
                query_bits.extend(["911 call", "audio"])
            elif claim.artifact_type == "court_video":
                query_bits.extend(["sentencing OR trial", "video"])
            elif claim.artifact_type == "docket_docs":
                query_bits.extend(["affidavit OR complaint OR docket", "pdf"])
            candidate_sources.extend(search_brave(" ".join([q for q in query_bits if q])))

    for claim in graph.artifact_claims:
        for source in candidate_sources:
            if not source.url:
                continue
            url = source.url
            d = domain_of(url)
            text = source.text().lower()
            artifact_type = infer_artifact_type_from_text(text, url) or claim.artifact_type
            if artifact_type != claim.artifact_type:
                continue
            if not is_artifact_host(url, text):
                continue
            score, matched = source_matches_case(source, names, juris)
            if score < 0.35 and graph.case_identity.identity_confidence != "high":
                continue
            downloadable = looks_downloadable(url, text)
            authority = authority_for_url(url)
            confidence = min(0.95, 0.35 + score + (0.15 if downloadable else 0.0) + (0.10 if authority in {"official", "court", "foia"} else 0.0))
            if confidence < 0.55:
                continue
            graph.add_verified_artifact(VerifiedArtifact(
                artifact_type=claim.artifact_type,
                artifact_url=url,
                host=d,
                source_authority=authority,
                downloadable=downloadable,
                matched_case_fields=matched,
                confidence=confidence,
            ))
            claim.verification_status = "verified" if downloadable or authority in {"official", "court", "foia"} else "artifact_page"


def infer_artifact_type_from_text(text: str, url: str = "") -> Optional[str]:
    combined = f"{text} {url}".lower()
    for artifact_type, patterns in ARTIFACT_PATTERNS.items():
        if any(re.search(pattern, combined, re.I) for pattern in patterns):
            return artifact_type
    return None


def is_artifact_host(url: str, text: str) -> bool:
    d = domain_of(url)
    combined = f"{url} {text}".lower()
    if any(h in d for h in ARTIFACT_HOST_HINTS):
        return True
    if any(ext in combined for ext in [".mp4", ".mp3", ".wav", ".m4a", ".pdf"]):
        return True
    if any(h in combined for h in ["download", "attachment", "production", "documents tab", "video"]):
        return True
    return False


def looks_downloadable(url: str, text: str) -> bool:
    combined = f"{url} {text}".lower()
    if any(ext in combined for ext in [".mp4", ".mp3", ".wav", ".m4a", ".pdf"]):
        return True
    if "youtube.com/watch" in combined or "youtu.be/" in combined or "vimeo.com/" in combined:
        return True
    if any(word in combined for word in ["download", "attachment", "production -", "redacted.mp4", "redacted.mp3"]):
        return True
    return False


def artifact_status(graph: CaseGraph) -> str:
    if any(a.downloadable for a in graph.verified_artifacts):
        return "downloadable"
    if graph.verified_artifacts:
        return "located"
    if graph.artifact_claims:
        return "claimed"
    return "none"


def assess_graph_confidence(graph: CaseGraph) -> str:
    identity = graph.case_identity.identity_confidence
    verified_count = len(graph.verified_artifacts)
    downloadable_count = sum(1 for a in graph.verified_artifacts if a.downloadable)
    claim_count = len(graph.artifact_claims)
    outcome = graph.case_identity.outcome_status

    # Be intentionally stricter than research.py: claims do not equal evidence.
    if identity == "high" and downloadable_count >= 1 and outcome in {"sentenced", "convicted", "charged"}:
        return "high"
    if identity in {"high", "medium"} and verified_count >= 1:
        return "medium"
    if identity in {"high", "medium"} and claim_count >= 2:
        return "medium"
    return "low"


# ──────────────────────────────────────────────────────────────
# Public interface
# ──────────────────────────────────────────────────────────────

def research_case(defendant_names: str, jurisdiction: str) -> Dict[str, Any]:
    """Main entry point compatible with evaluate.py."""
    reset_budget()
    graph = discover_identity_sources(defendant_names, jurisdiction)

    if graph.case_identity.identity_confidence == "low":
        # Still return the graph for debugging, but do not inflate evidence claims.
        return graph.legacy_export()

    extract_artifact_claims(graph)
    resolve_artifact_claims(graph)
    return graph.legacy_export()


# ──────────────────────────────────────────────────────────────
# CLI smoke test
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Case-graph research harness smoke test")
    parser.add_argument("--name", required=True, help="Defendant name")
    parser.add_argument("--jurisdiction", required=True, help="Jurisdiction string")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    args = parser.parse_args()

    result = research_case(args.name, args.jurisdiction)
    print(json.dumps(result, indent=2 if args.pretty else None, ensure_ascii=False))
