"""
identity_score.py — per-source identity verification for FlameOn P2 discovery.

Cherry-picked from the case-graph fork (autoresearch/research_case_graph.py).
Adds an `identity_score` and `identity_matched_fields` annotation to each source,
plus utilities for case-number extraction and multi-case disambiguation.

Why this exists:
  Our existing research.py pools all sources together, then any source mentioning
  an evidence keyword sets the corresponding evidence flag. This pollutes
  evidence detection when sources include cases about OTHER people with similar
  names — the recurring false-HIGH failure mode (Joshua Carrier, Braulio
  Gonzalez, Carmen Barahona, etc.).

  Per-source identity scoring lets assess_confidence count only sources that
  are PROVABLY about THIS defendant — typically the ones containing the
  defendant's FULL name (not just last name).

What it does NOT do:
  - Replace existing per-API relevance_score (preserves recall heuristics).
  - Modify discovery — pure post-processing annotation.

Three exports for research.py to consume:
  - apply_identity_scoring(sources, defendant_names, jurisdiction)
      In-place: adds `identity_score` and `identity_matched_fields` to each source.
  - has_full_name_match(source) -> bool
      Convenience predicate for assess_confidence.
  - count_distinct_case_numbers(sources, *, require_full_name=True) -> int
      Used by assess_confidence to detect multi-case ambiguity (same name,
      different cases — the same-city collision pattern).
"""

import re
from typing import Any, Dict, Iterable, List, Sequence, Tuple

# Whole-word case-number patterns. Conservative: anchored, length-bounded.
# Examples that match: "23CR1234", "F015-23", "CR-22-001234", "12-CR-456"
# Examples that don't: "1234" (too short for context), "ABC123XYZ" (no digits-then-dash pattern)
_CASE_NUMBER_PATTERNS = [
    re.compile(r"\b(?:case\s*(?:no\.?|number|#)?\s*[:#]?\s*)?([A-Z]{1,4}\d{2,4}[- ]?[A-Z]{0,4}[- ]?\d{2,8})\b", re.IGNORECASE),
    re.compile(r"\b(\d{2,4}[- ]?[A-Z]{1,4}[- ]?\d{2,8})\b"),
]

# Generational / honorific suffixes to drop when extracting last name.
_NAME_SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}
_NAME_HONORIFICS = {"dr.", "mr.", "mrs.", "ms.", "dr", "mr", "mrs", "ms"}

# Authority hints (subset of what the case-graph fork uses) — boost when matched.
_OFFICIAL_DOMAIN_HINTS = (".gov", ".us", "police", "sheriff", "courts", "court",
                          "clerk", "districtattorney", "prosecutor", "cityof", "county")
_FOIA_DOMAIN_HINTS = ("muckrock", "nextrequest", "govqa", "justfoia", "publicrecords")
_ENTERTAINMENT_DOMAINS = ("imdb.com", "spotify.com", "tvguide.com", "soapcentral.com",
                          "genius.com", "fandom.com", "amazon.com", "goodreads.com")
_ENTERTAINMENT_TEXT_FLAGS = ("movie", "trailer", "anime", "lyrics", "soundtrack", "gameplay")


def _parse_names(defendant_names: str) -> Dict[str, str]:
    """Return {primary, clean_primary, last_name} from a comma/semicolon name list."""
    raw = (defendant_names or "").strip()
    if not raw:
        return {"primary": "", "clean_primary": "", "last_name": ""}
    names = [n.strip() for n in re.split(r",|;|\band\b", raw) if n.strip()]
    primary = names[0] if names else raw
    parts = [p.strip() for p in primary.split() if p.strip()]
    cleaned = [p for p in parts if p.lower() not in _NAME_SUFFIXES and p.lower() not in _NAME_HONORIFICS]
    last = ""
    for p in reversed(cleaned or parts):
        if p.lower() not in _NAME_SUFFIXES:
            last = p
            break
    return {
        "primary": primary,
        "clean_primary": " ".join(cleaned) or primary,
        "last_name": last,
    }


def _parse_jurisdiction(jurisdiction: str) -> Dict[str, str]:
    """Extract city/county/state from a comma-delimited jurisdiction string."""
    parts = [p.strip() for p in (jurisdiction or "").split(",") if p.strip()]
    return {
        "city": parts[0] if parts else "",
        "county": parts[1] if len(parts) >= 3 else "",
        "state": parts[-1] if len(parts) >= 2 else "",
    }


def _domain_of(url: str) -> str:
    if not url:
        return ""
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def _authority_hint(url: str) -> str:
    """Return one of: court, foia, official, news, third_party, unknown."""
    d = _domain_of(url)
    if not d:
        return "unknown"
    if "courtlistener" in d or "court" in d or "clerk" in d:
        return "court"
    if any(h in d for h in _FOIA_DOMAIN_HINTS):
        return "foia"
    if d.endswith(".gov") or any(h in d for h in _OFFICIAL_DOMAIN_HINTS):
        return "official"
    if "youtube" in d or "youtu.be" in d or "vimeo" in d or "documentcloud" in d:
        return "third_party"
    return "unknown"


def score_source_identity(source: Dict[str, Any],
                           names: Dict[str, str],
                           jurisdiction: Dict[str, str]) -> Tuple[float, List[str]]:
    """
    Score a single source's identity match to (defendant, jurisdiction).
    Returns (score in [0,1], list of matched-field labels).

    The matched_fields list lets downstream consumers gate on specific signals:
        "defendant_full_name" — strongest single signal
        "defendant_last_name" — weak (high collision risk)
        "city" / "county" / "state" / "state_abbrev"
        "authority:court" / "authority:foia" / "authority:official"
        "noise_domain" / "entertainment_text" — penalty markers
    """
    text = (
        f"{source.get('title', '')} "
        f"{source.get('description', '') or source.get('snippet', '')} "
        f"{source.get('url', '')}"
    ).lower()
    score = 0.0
    matched: List[str] = []

    clean_name = (names.get("clean_primary") or "").lower()
    last_name = (names.get("last_name") or "").lower()

    if clean_name and clean_name in text:
        score += 0.50
        matched.append("defendant_full_name")
    elif last_name and len(last_name) > 3 and re.search(rf"\b{re.escape(last_name)}\b", text):
        score += 0.25
        matched.append("defendant_last_name")

    city = (jurisdiction.get("city") or "").lower()
    county = (jurisdiction.get("county") or "").lower().replace(" county", "")
    state = (jurisdiction.get("state") or "").lower()

    if city and city in text:
        score += 0.18
        matched.append("city")
    if county and len(county) > 2 and county in text:
        score += 0.14
        matched.append("county")
    if state and state in text:
        score += 0.12
        matched.append("state")

    authority = _authority_hint(source.get("url", ""))
    if authority in {"court", "foia", "official"}:
        score += 0.12
        matched.append(f"authority:{authority}")

    # Penalty patterns — entertainment/noise domains drag the score down
    domain = _domain_of(source.get("url", ""))
    if any(noise in domain for noise in _ENTERTAINMENT_DOMAINS):
        score -= 0.50
        matched.append("noise_domain")
    if any(flag in text for flag in _ENTERTAINMENT_TEXT_FLAGS):
        score -= 0.30
        matched.append("entertainment_text")

    return max(0.0, min(1.0, score)), matched


def apply_identity_scoring(sources: List[Dict[str, Any]],
                            defendant_names: str,
                            jurisdiction: str) -> List[Dict[str, Any]]:
    """
    Annotate every source with `identity_score` and `identity_matched_fields`.
    Mutates sources in place; returns the same list for chaining.

    Does NOT touch the existing `relevance_score` — preserves whatever
    per-API heuristics the search_* functions assigned. Identity scoring
    is purely additive and is consumed by assess_confidence.
    """
    if not sources:
        return sources
    names = _parse_names(defendant_names)
    juris = _parse_jurisdiction(jurisdiction)
    if not names["clean_primary"] and not names["last_name"]:
        return sources
    for src in sources:
        score, matched = score_source_identity(src, names, juris)
        src["identity_score"] = score
        src["identity_matched_fields"] = matched
    return sources


def has_full_name_match(source: Dict[str, Any]) -> bool:
    """True iff the source's text contains the defendant's full name."""
    return "defendant_full_name" in (source.get("identity_matched_fields") or [])


def extract_case_numbers(text: str) -> List[str]:
    """Pull plausible court / agency case numbers out of free text."""
    if not text:
        return []
    out: List[str] = []
    seen = set()
    for pattern in _CASE_NUMBER_PATTERNS:
        for m in pattern.finditer(text):
            cleaned = re.sub(r"\s+", "", m.group(1).upper())
            # Bound length and require at least one digit run
            if 5 <= len(cleaned) <= 25 and re.search(r"\d{2,}", cleaned) and cleaned not in seen:
                seen.add(cleaned)
                out.append(cleaned)
    return out


def count_distinct_case_numbers(sources: Sequence[Dict[str, Any]],
                                  *, require_full_name: bool = True) -> int:
    """
    Count the number of DISTINCT case numbers found across sources.

    require_full_name=True (default): only consider sources whose identity
    score includes "defendant_full_name". This is the right filter for
    detecting same-name-different-case collisions: if the agent finds 3
    separate case numbers all in sources that DO mention this defendant by
    full name, they're likely 3 separate proceedings (same person, different
    cases — e.g. Epstein) OR 3 different people sharing a common name. Either
    way it's a signal worth surfacing.
    """
    distinct = set()
    for src in sources:
        if require_full_name and not has_full_name_match(src):
            continue
        text_blob = " ".join([
            src.get("title", "") or "",
            src.get("description", "") or src.get("snippet", "") or "",
        ])
        for cn in extract_case_numbers(text_blob):
            distinct.add(cn)
    return len(distinct)
