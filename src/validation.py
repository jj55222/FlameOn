"""Stage 2 — Cheap Validation Gate.

Uses Brave Search API for cheap validation, then OpenRouter LLM to parse
search snippets for closure signals.

This is the most important gate in the system.
"""

import time

import requests

from .logger import get_logger
from .models import CaseCandidate, SourceRank, ValidationResult, ValidationStatus

log = get_logger()

# --- Closure signal keywords ---

STRONG_PASS_SIGNALS = [
    "sentenced to",
    "was sentenced",
    "has been sentenced",
    "received a sentence",
    "life sentence",
    "life in prison",
    "death sentence",
    "death penalty",
    "years in prison",
    "years imprisonment",
    "convicted and sentenced",
    "found guilty",
    "guilty verdict",
    "pleaded guilty",
    "pled guilty",
    "plea deal",
    "plea agreement",
    "years to life",
    "without parole",
    "consecutive sentences",
    "concurrent sentences",
    "prison term",
    "years behind bars",
    "judgment of conviction",
]

STRONG_REJECT_SIGNALS = [
    "arrested",
    "has been arrested",
    "was arrested",
    "charged with",
    "has been charged",
    "indicted",
    "in trial",
    "trial begins",
    "trial underway",
    "awaiting trial",
    "trial date set",
    "plea pending",
    "hearing scheduled",
    "preliminary hearing",
    "investigation continues",
    "investigation ongoing",
    "under investigation",
    "search continues",
    "at large",
    "manhunt",
    "wanted for",
    "bail set",
    "bond set",
    "competency hearing",
    "incompetent to stand trial",
]

# Domains that are generally credible for validation
CREDIBLE_DOMAINS = [
    ".gov",
    "courts.",
    "clerk.",
    "judiciary.",
    # Major news orgs and local affiliates
    "nytimes.com",
    "washingtonpost.com",
    "apnews.com",
    "reuters.com",
    "local10.com",
    "wsvn.com",
    "wplg.com",
    "clickorlando.com",
    "fox35orlando.com",
    "wesh.com",
    "news4jax.com",
    "firstcoastnews.com",
    "baynews9.com",
    "abcactionnews.com",
    "fox13news.com",
    "wtsp.com",
    "wfla.com",
    "abc15.com",
    "azcentral.com",
    "fox10phoenix.com",
    "12news.com",
    "kgun9.com",
    "kvoa.com",
    "khou.com",
    "abc13.com",
    "fox26houston.com",
    "click2houston.com",
    "dallasnews.com",
    "fox4news.com",
    "wfaa.com",
    "nbcdfw.com",
    "ksat.com",
    "news4sa.com",
    "cleveland.com",
    "cleveland19.com",
    "fox8.com",
    "wkyc.com",
    "fox19.com",
    "wcpo.com",
    "wlwt.com",
    "10tv.com",
    "nbc4i.com",
    "whio.com",
    "daytondailynews.com",
]

# Social media / unreliable sources — not sufficient alone
NON_CREDIBLE_DOMAINS = [
    "reddit.com",
    "twitter.com",
    "x.com",
    "facebook.com",
    "tiktok.com",
    "instagram.com",
    "youtube.com",
    "wikipedia.org",
    "quora.com",
]


def _classify_source(url: str) -> str:
    """Classify a URL into a source rank."""
    url_lower = url.lower()

    if any(d in url_lower for d in [".gov", "courts.", "judiciary.", "uscourts."]):
        return SourceRank.COURT_GOV.value
    if any(d in url_lower for d in ["clerk.", "docket", "courtrecords", "caseinfo"]):
        return SourceRank.COUNTY_CLERK.value
    if any(d in url_lower for d in NON_CREDIBLE_DOMAINS):
        return SourceRank.OTHER.value
    # Check known LE domains
    if any(d in url_lower for d in ["sheriff", "police", "pd.org", "so.org"]):
        return SourceRank.LE_RELEASE.value
    # Default: if it looks like news, classify as local news
    return SourceRank.LOCAL_NEWS.value


def _is_credible(url: str) -> bool:
    """Check if a URL is from a credible source (not social media / wiki)."""
    url_lower = url.lower()
    if any(d in url_lower for d in NON_CREDIBLE_DOMAINS):
        return False
    return True


def _build_validation_queries(candidate: CaseCandidate) -> list[str]:
    """Build search queries for validation, ordered by expected quality."""
    queries = []

    name = candidate.suspect_name
    city = candidate.city
    state = candidate.state
    agency = candidate.agency_name
    keywords = candidate.case_keywords

    if name:
        # Suspect-centered queries (preferred)
        if city:
            queries.append(f'"{name}" {city} sentencing')
            queries.append(f'"{name}" {city} convicted sentence')
        if agency:
            queries.append(f'"{name}" {agency} sentenced')
        if state:
            queries.append(f'"{name}" {state} sentencing')
    else:
        # Fallback: jurisdiction-based queries
        offense = ""
        if keywords:
            # Pick the most specific keyword
            for kw in ["murder", "homicide", "shooting", "robbery", "assault", "stabbing"]:
                if kw in keywords.lower():
                    offense = kw
                    break

        if city and offense:
            queries.append(f"{city} {offense} sentencing")
            if candidate.incident_date:
                queries.append(f"{city} {candidate.incident_date} {offense} conviction")
        if agency and offense:
            queries.append(f"{agency} {offense} sentencing")

    return queries[:3]  # max 3 queries per spec


def _brave_search(query: str, api_key: str, count: int = 5) -> list[dict]:
    """Execute a Brave Search API query. Returns list of result dicts."""
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


def _parse_closure_with_llm(
    snippets: str,
    suspect_name: str,
    openrouter_api_key: str,
    openrouter_model: str,
    openrouter_base_url: str,
) -> dict:
    """Use OpenRouter LLM to determine if search snippets indicate case closure.

    Returns dict with keys: status, note, best_source_url
    """
    name_context = f' for suspect "{suspect_name}"' if suspect_name else ""

    prompt = (
        f"You are analyzing search results{name_context} to determine if a criminal case is CLOSED "
        "(sentenced/convicted with final disposition) or still OPEN/PENDING.\n\n"
        "Search results:\n"
        f"{snippets}\n\n"
        "Respond with EXACTLY one of these three lines, followed by a brief explanation:\n"
        "CLOSED: [explanation of the sentencing/conviction found]\n"
        "OPEN: [explanation of why the case appears still open or pending]\n"
        "AMBIGUOUS: [explanation of why the status is unclear]\n\n"
        "Rules:\n"
        "- CLOSED requires clear evidence of sentencing (years imposed, life sentence, etc.)\n"
        "- Arrested, charged, indicted, or in trial = OPEN\n"
        "- If you're not sure, say AMBIGUOUS\n"
        "- Be conservative — when in doubt, do not mark as CLOSED"
    )

    try:
        resp = requests.post(
            f"{openrouter_base_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {openrouter_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": openrouter_model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 200,
                "temperature": 0,
            },
            timeout=20,
        )
        resp.raise_for_status()
        result = resp.json()["choices"][0]["message"]["content"].strip()

        if result.upper().startswith("CLOSED"):
            return {"status": "closed", "note": result}
        elif result.upper().startswith("OPEN"):
            return {"status": "open", "note": result}
        else:
            return {"status": "ambiguous", "note": result}

    except Exception as e:
        log.error("LLM validation parsing failed: %s", e)
        return {"status": "error", "note": f"LLM error: {e}"}


def validate_case(
    candidate: CaseCandidate,
    brave_api_key: str,
    openrouter_api_key: str,
    openrouter_model: str = "google/gemini-flash-1.5",
    openrouter_base_url: str = "https://openrouter.ai/api/v1",
    max_queries: int = 3,
    rate_limit: float = 1.0,
) -> ValidationResult:
    """Run the cheap validation gate on a case candidate.

    Returns a ValidationResult with the disposition.
    """
    # Skip validation when no suspect name — generic queries match wrong cases
    if not candidate.suspect_name:
        log.info(
            "MANUAL REVIEW (no suspect name): %s — skipping validation to avoid mismatch",
            candidate.case_id,
        )
        return ValidationResult(
            status=ValidationStatus.MANUAL_REVIEW.value,
            query_used="(skipped — no suspect name)",
            note="No suspect name extracted; skipping automated validation to avoid "
                 "matching unrelated cases. Full video description preserved for manual review.",
            manual_review_reason="No suspect name — automated validation skipped to prevent mismatch",
        )

    queries = _build_validation_queries(candidate)

    if not queries:
        log.warning("No validation queries could be built for %s", candidate.case_id)
        return ValidationResult(
            status=ValidationStatus.REJECTED_OPEN_OR_UNCONFIRMED.value,
            query_used="(none — insufficient fields)",
            note="Could not build any validation queries — insufficient extracted fields",
        )

    all_snippets = []
    best_source_url = ""
    best_source_rank = SourceRank.OTHER.value

    for query in queries[:max_queries]:
        log.debug("Validation query: %s", query)
        results = _brave_search(query, brave_api_key)

        if not results:
            time.sleep(rate_limit)
            continue

        for r in results:
            url = r.get("url", "")
            snippet_text = f"[{r.get('title', '')}] ({url}): {r.get('description', '')}"
            all_snippets.append(snippet_text)

            # Track best credible source
            if _is_credible(url):
                rank = _classify_source(url)
                rank_order = [
                    SourceRank.COURT_GOV.value,
                    SourceRank.COUNTY_CLERK.value,
                    SourceRank.LOCAL_NEWS.value,
                    SourceRank.LE_RELEASE.value,
                    SourceRank.OTHER.value,
                ]
                if rank_order.index(rank) < rank_order.index(best_source_rank):
                    best_source_rank = rank
                    best_source_url = url
                elif not best_source_url:
                    best_source_url = url

        time.sleep(rate_limit)

    if not all_snippets:
        log.info("No search results for %s — rejecting", candidate.case_id)
        return ValidationResult(
            status=ValidationStatus.REJECTED_OPEN_OR_UNCONFIRMED.value,
            query_used=queries[0] if queries else "",
            note="No search results found for any validation query",
        )

    # Combine snippets and send to LLM for analysis
    combined = "\n".join(all_snippets[:10])  # limit to 10 snippets
    if len(combined) > 2000:
        combined = combined[:2000]

    llm_result = _parse_closure_with_llm(
        combined,
        candidate.suspect_name,
        openrouter_api_key,
        openrouter_model,
        openrouter_base_url,
    )

    query_used = queries[0]

    if llm_result["status"] == "closed":
        log.info("VALIDATED CLOSED: %s — %s", candidate.case_id, llm_result["note"][:100])
        return ValidationResult(
            status=ValidationStatus.VALIDATED_CLOSED.value,
            query_used=query_used,
            source_url=best_source_url,
            source_rank=best_source_rank,
            note=llm_result["note"],
            raw_snippets=combined,
        )

    elif llm_result["status"] == "ambiguous":
        log.info("MANUAL REVIEW: %s — %s", candidate.case_id, llm_result["note"][:100])
        return ValidationResult(
            status=ValidationStatus.MANUAL_REVIEW.value,
            query_used=query_used,
            source_url=best_source_url,
            source_rank=best_source_rank,
            note=llm_result["note"],
            manual_review_reason=f"Ambiguous validation result: {llm_result['note'][:200]}",
            raw_snippets=combined,
        )

    else:  # open or error
        log.info("REJECTED: %s — %s", candidate.case_id, llm_result["note"][:100])
        return ValidationResult(
            status=ValidationStatus.REJECTED_OPEN_OR_UNCONFIRMED.value,
            query_used=query_used,
            source_url=best_source_url,
            source_rank=best_source_rank,
            note=llm_result["note"],
            raw_snippets=combined,
        )
