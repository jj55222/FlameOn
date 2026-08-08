"""
parsers/ — per-agency CIB/OIS publishing-page parsers.

Each module exposes a function `fetch_and_parse() -> list[dict]` returning
incident records with this minimum schema:

    {
      "agency": str,                   # e.g. "LAPD"
      "incident_id": str,              # agency-internal id, e.g. "F015-23"
      "incident_date": str,            # YYYY-MM-DD if known, else ""
      "incident_type": str,            # "OIS", "use_of_force", "in_custody_death", etc.
      "division": str,                 # geographic/precinct context (optional)
      "location": str,                 # address or city
      "subjects": [str],               # named subjects (defendant, suspect, decedent)
      "video_urls": [str],             # bodycam/CIB video URLs
      "documents": [str],              # PDFs, news posts, supplemental docs
      "summary": str,                  # one-line description
      "source_url": str,               # the agency page this came from
      "scraped_at": str,               # ISO timestamp
    }

A central cache (cib_cache/{agency}.json) holds these records. research.py
calls `search_cib_cache(name, jurisdiction)` to look up matches at zero
API cost — pure file read + name match.
"""

import json
import re
from pathlib import Path

CIB_CACHE_DIR = Path(__file__).parent.parent / "cib_cache"

# Agency → jurisdiction-keyword markers. A name match against a CIB record
# only counts if the caller's jurisdiction string contains one of these
# tokens for the record's agency. This is the cheap false-positive filter:
# names collide constantly ("Davis", "Smith", "Johnson"), but
# (agency, jurisdiction) pairs almost never do. Add new agencies here as
# B2-B4 parsers land.
AGENCY_JURISDICTION_MARKERS = {
    "LAPD":  ["los angeles", "lapd", " ca", "california"],
    "SDPD":  ["san diego", "sdpd", " ca", "california"],
    "LBPD":  ["long beach", "lbpd", " ca", "california"],
    "MESA":  ["mesa", "arizona", " az"],
}

_NAME_PUNCT_RE = re.compile(r"[^\w\s]")
_WS_RE = re.compile(r"\s+")
_PLACEHOLDER_RE = re.compile(r"^[A-Z]{2,5}$")  # LAPD uses "NTUD", etc. for unnamed
_SPLIT_RE = re.compile(r"[;,]+|\s+(?:and|aka|a\.k\.a\.)\s+", re.IGNORECASE)
_GENERATIONAL = {"jr", "sr", "ii", "iii", "iv"}

_cache_records = None  # in-process cache so evaluate.py's 38-case run reads once


def _normalize_name(s):
    s = _NAME_PUNCT_RE.sub(" ", (s or "").lower())
    return _WS_RE.sub(" ", s).strip()


def _name_tokens(name):
    """Tokenize a name string, dropping generational suffixes."""
    return [t for t in _normalize_name(name).split() if t not in _GENERATIONAL]


def _agency_matches_jurisdiction(agency, jurisdiction):
    if not jurisdiction:
        return False
    markers = AGENCY_JURISDICTION_MARKERS.get((agency or "").upper())
    if not markers:
        # Unknown agency — accept. New parsers should add an entry above
        # before going to production to keep the false-positive filter active.
        return True
    j_lower = " " + jurisdiction.lower() + " "
    return any(m in j_lower for m in markers)


def _name_matches(query_name, subject_names):
    """
    Strict-ish match. Both query and subject must overlap on at least 2
    tokens (typically first + last). Subjects with placeholder-only names
    ("NTUD", "OIS") do not match.
    """
    if not subject_names:
        return False
    real = [s for s in subject_names if s and not _PLACEHOLDER_RE.match(s.strip())]
    if not real:
        return False

    query_token_sets = []
    for q in _SPLIT_RE.split(query_name or ""):
        toks = _name_tokens(q)
        if len(toks) >= 2:
            query_token_sets.append(set(toks))
    if not query_token_sets:
        return False

    for subject in real:
        subj_tokens = set(_name_tokens(subject))
        for qts in query_token_sets:
            if len(qts & subj_tokens) >= 2:
                return True
    return False


def _load_cache(cache_dir):
    """Load all cib_cache/*.json except *_test.json. Cached in-process."""
    global _cache_records
    if _cache_records is not None and cache_dir == CIB_CACHE_DIR:
        return _cache_records
    cache_dir = Path(cache_dir)
    if not cache_dir.exists():
        records = []
    else:
        records = []
        for path in sorted(cache_dir.glob("*.json")):
            if path.stem.endswith("_test"):
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    records.extend(data)
            except (json.JSONDecodeError, OSError):
                continue
    if cache_dir == CIB_CACHE_DIR:
        _cache_records = records
    return records


def _record_to_sources(record):
    """Convert one CIB record into 0+ source dicts matching research.py's schema."""
    sources = []
    agency = record.get("agency", "")
    incident_id = record.get("incident_id", "")
    subjects = ", ".join(record.get("subjects") or [])
    summary = record.get("summary", "")
    desc = " | ".join(p for p in [agency, incident_id, subjects, summary] if p)

    for url in record.get("video_urls") or []:
        if not url:
            continue
        sources.append({
            "url": url,
            "type": "bodycam_footage",
            "description": desc,
            "relevance_score": 0.9,
            "api": "cib_cache",
        })
    for url in record.get("documents") or []:
        if not url:
            continue
        sources.append({
            "url": url,
            "type": "agency_disclosure",
            "description": desc,
            "relevance_score": 0.8,
            "api": "cib_cache",
        })
    return sources


def search_cib_cache(defendant_names, jurisdiction, cache_dir=None):
    """
    Look up a defendant in cached agency CIB/OIS publishing-page records.
    Zero API cost: file read + token match. Returns sources in the shape
    research.py expects (url/type/description/relevance_score/api).
    """
    records = _load_cache(cache_dir or CIB_CACHE_DIR)
    if not records:
        return []
    out = []
    for record in records:
        if not _agency_matches_jurisdiction(record.get("agency", ""), jurisdiction):
            continue
        if not _name_matches(defendant_names, record.get("subjects") or []):
            continue
        out.extend(_record_to_sources(record))
    return out


def reset_cib_cache():
    """Clear in-process cache (for tests / repeated runs that update files mid-process)."""
    global _cache_records
    _cache_records = None
