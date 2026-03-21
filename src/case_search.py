"""Case Search — CourtListener + case.law API integration.

Enriches discovery with:
  1. CourtListener: Federal docket search (RECAP archive), opinion search,
     case-number extraction from docket results.
  2. case.law (Harvard CAP): Appellate opinion lookup — confirms closure,
     extracts citations and case numbers.
  3. Case-number-enhanced Brave queries for surgical artifact retrieval
     from state court portals.

Designed to slot between Stage 2 (validation) and Stage 3A (discovery)
as an enrichment layer, or to run as part of Stage 3A itself.
"""

import re
import time

import requests

from .logger import get_logger
from .models import CaseCandidate, DiscoveredLink, SourceRank

log = get_logger()

# ---------------------------------------------------------------------------
# CourtListener API
# ---------------------------------------------------------------------------

CL_BASE = "https://www.courtlistener.com/api/rest/v4"

# Map FlameOn state codes to CourtListener court abbreviations (federal district courts)
STATE_TO_CL_COURTS = {
    "FL": ["flsd", "flmd", "flnd"],
    "TX": ["txsd", "txed", "txwd", "txnd"],
    "OH": ["ohsd", "ohnd"],
    "AZ": ["azd"],
}

# Map FlameOn state codes to CourtListener state court abbreviations
STATE_TO_CL_STATE_COURTS = {
    "FL": ["fla", "fladistctapp", "flacircctapp"],
    "TX": ["tex", "texapp", "texcrimapp"],
    "OH": ["ohio", "ohioctapp"],
    "AZ": ["ariz", "arizctapp"],
}


def _cl_headers(api_key: str) -> dict:
    return {"Authorization": f"Token {api_key}"} if api_key else {}


def _cl_get(endpoint: str, params: dict, api_key: str, timeout: int = 15) -> dict | None:
    """Make a GET request to CourtListener API."""
    try:
        resp = requests.get(
            f"{CL_BASE}/{endpoint}/",
            params=params,
            headers=_cl_headers(api_key),
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.error("CourtListener %s failed: %s", endpoint, e)
        return None


def search_courtlistener_opinions(
    suspect_name: str,
    state: str,
    api_key: str = "",
    rate_limit: float = 2.0,
) -> list[dict]:
    """Search CourtListener opinions (case law) for a suspect name.

    Returns list of dicts with keys: case_name, citation, court, date_filed,
    absolute_url, docket_number, opinion_id.
    """
    if not suspect_name:
        return []

    courts = STATE_TO_CL_STATE_COURTS.get(state.upper(), []) + STATE_TO_CL_COURTS.get(state.upper(), [])

    results = []
    seen_ids = set()

    # Search opinions by name
    params = {
        "q": f'"{suspect_name}"',
        "type": "o",  # opinions
        "order_by": "score desc",
        "page_size": 10,
    }
    if courts:
        params["court"] = " ".join(courts)

    data = _cl_get("search", params, api_key)
    if not data:
        return results

    for hit in data.get("results", []):
        oid = hit.get("id")
        if oid in seen_ids:
            continue
        seen_ids.add(oid)

        results.append({
            "case_name": hit.get("caseName", ""),
            "citation": hit.get("citation", [hit.get("sibling_ids", "")])[0] if hit.get("citation") else "",
            "court": hit.get("court", ""),
            "date_filed": hit.get("dateFiled", ""),
            "absolute_url": hit.get("absolute_url", ""),
            "docket_number": hit.get("docketNumber", ""),
            "opinion_id": oid,
            "snippet": hit.get("snippet", ""),
        })

    time.sleep(rate_limit)
    return results


def search_courtlistener_dockets(
    suspect_name: str,
    state: str,
    api_key: str = "",
    rate_limit: float = 2.0,
) -> list[dict]:
    """Search CourtListener RECAP dockets for a suspect name.

    Returns list of dicts with: case_name, docket_number, court, date_filed,
    date_terminated, docket_id, docket_url, document_count.
    """
    if not suspect_name:
        return []

    courts = STATE_TO_CL_COURTS.get(state.upper(), [])
    if not courts:
        return []

    results = []
    seen_ids = set()

    # Search RECAP dockets
    params = {
        "q": f'"{suspect_name}"',
        "type": "r",  # RECAP dockets
        "order_by": "score desc",
        "page_size": 10,
    }
    params["court"] = " ".join(courts)

    data = _cl_get("search", params, api_key)
    if not data:
        return results

    for hit in data.get("results", []):
        did = hit.get("docket_id")
        if did in seen_ids:
            continue
        seen_ids.add(did)

        results.append({
            "case_name": hit.get("caseName", ""),
            "docket_number": hit.get("docketNumber", ""),
            "court": hit.get("court", ""),
            "date_filed": hit.get("dateFiled", ""),
            "date_terminated": hit.get("dateTerminated", ""),
            "docket_id": did,
            "docket_url": hit.get("absolute_url", ""),
            "document_count": hit.get("document_count", 0),
            "snippet": hit.get("snippet", ""),
        })

    time.sleep(rate_limit)
    return results


def fetch_docket_documents(
    docket_id: int,
    api_key: str = "",
    rate_limit: float = 2.0,
) -> list[dict]:
    """Fetch document entries from a specific CourtListener docket.

    Returns list of dicts with: description, document_number, page_count,
    filepath_local (download URL if in RECAP), pacer_doc_id.
    """
    data = _cl_get(
        "recap-documents",
        {"docket_entry__docket": docket_id, "page_size": 50, "is_available": True},
        api_key,
    )
    if not data:
        return []

    docs = []
    for doc in data.get("results", []):
        filepath = doc.get("filepath_local", "")
        download_url = ""
        if filepath:
            download_url = f"https://storage.courtlistener.com/{filepath}"

        docs.append({
            "description": doc.get("description", ""),
            "document_number": doc.get("document_number", ""),
            "page_count": doc.get("page_count", 0),
            "download_url": download_url,
            "pacer_doc_id": doc.get("pacer_doc_id", ""),
            "date_created": doc.get("date_created", ""),
            "is_available": doc.get("is_available", False),
        })

    time.sleep(rate_limit)
    return docs


# ---------------------------------------------------------------------------
# case.law (Harvard Caselaw Access Project)
# ---------------------------------------------------------------------------

CAP_BASE = "https://api.case.law/v1"


def _cap_headers(api_key: str) -> dict:
    headers = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Token {api_key}"
    return headers


def search_caselaw(
    suspect_name: str,
    state: str,
    api_key: str = "",
    rate_limit: float = 2.0,
) -> list[dict]:
    """Search Harvard CAP for published opinions mentioning a suspect.

    Returns list of dicts with: case_name, citation, court, decision_date,
    docket_number, cap_id, frontend_url.
    """
    if not suspect_name:
        return []

    # Map FlameOn state codes to CAP jurisdiction slugs
    state_to_cap = {
        "FL": "fla",
        "TX": "tex",
        "OH": "ohio",
        "AZ": "ariz",
    }
    jurisdiction = state_to_cap.get(state.upper(), state.lower())

    results = []

    try:
        resp = requests.get(
            f"{CAP_BASE}/cases/",
            params={
                "search": f'"{suspect_name}"',
                "jurisdiction": jurisdiction,
                "ordering": "-decision_date",
                "page_size": 5,
            },
            headers=_cap_headers(api_key),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error("case.law search failed for '%s': %s", suspect_name, e)
        return results

    for case in data.get("results", []):
        citations = case.get("citations", [])
        cite_str = citations[0].get("cite", "") if citations else ""

        results.append({
            "case_name": case.get("name", ""),
            "citation": cite_str,
            "court": case.get("court", {}).get("name", ""),
            "decision_date": case.get("decision_date", ""),
            "docket_number": case.get("docket_number", ""),
            "cap_id": case.get("id", ""),
            "frontend_url": case.get("frontend_url", ""),
        })

    time.sleep(rate_limit)
    return results


# ---------------------------------------------------------------------------
# Case number extraction
# ---------------------------------------------------------------------------

# Common case number patterns across jurisdictions
CASE_NUMBER_PATTERNS = [
    # Federal: 1:23-cr-00456
    r"\b\d{1,2}:\d{2}-cr-\d{3,6}\b",
    # Federal civil: 1:23-cv-00456
    r"\b\d{1,2}:\d{2}-cv-\d{3,6}\b",
    # FL: 2023-CF-001234, 23-CF-1234
    r"\b\d{2,4}-CF-\d{3,6}\b",
    # FL: 2023-MM-001234 (misdemeanor)
    r"\b\d{2,4}-MM-\d{3,6}\b",
    # TX: varies, e.g. F-2023-1234-A
    r"\b[A-Z]-\d{4}-\d{3,6}-[A-Z]\b",
    # OH: CR-23-123456
    r"\bCR-\d{2}-\d{4,8}\b",
    # AZ: CR2023-001234
    r"\bCR\d{4}-\d{4,8}\b",
    # Generic: "Case No. XX-XXXX" or "No. XX-XXXX" — capture group 1 = number only
    r"(?:Case\s+)?No\.?\s*(\d{2,4}-\w{1,4}-?\d{3,8})",
]


def extract_case_numbers(text: str) -> list[str]:
    """Extract case numbers from text using common patterns."""
    numbers = []
    seen = set()
    for pattern in CASE_NUMBER_PATTERNS:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            # Prefer capture group 1 if present (strips prefix like "Case No.")
            num = (match.group(1) if match.lastindex and match.group(1) else match.group(0)).strip()
            if num.upper() not in seen:
                seen.add(num.upper())
                numbers.append(num)
    return numbers


# ---------------------------------------------------------------------------
# Enrichment: run all sources, extract case numbers, build enhanced queries
# ---------------------------------------------------------------------------

def enrich_case(
    candidate: CaseCandidate,
    courtlistener_api_key: str = "",
    caselaw_api_key: str = "",
    rate_limit: float = 2.0,
) -> dict:
    """Run CourtListener + case.law searches for a candidate.

    Returns enrichment dict with:
      - case_numbers: list of extracted case numbers
      - docket_numbers: list of docket numbers from CourtListener
      - citations: list of legal citations
      - cl_opinion_links: list of DiscoveredLink from CL opinions
      - cl_docket_links: list of DiscoveredLink from CL dockets
      - cl_document_links: list of DiscoveredLink from RECAP documents
      - cap_links: list of DiscoveredLink from case.law
    """
    name = candidate.suspect_name
    state = candidate.state

    enrichment = {
        "case_numbers": [],
        "docket_numbers": [],
        "citations": [],
        "cl_opinion_links": [],
        "cl_docket_links": [],
        "cl_document_links": [],
        "cap_links": [],
    }

    if not name:
        log.warning("No suspect name for case search enrichment: %s", candidate.case_id)
        return enrichment

    all_text = ""  # Accumulate text for case number extraction

    # --- CourtListener: Opinions ---
    log.info("Searching CourtListener opinions for '%s' in %s", name, state)
    cl_opinions = search_courtlistener_opinions(name, state, courtlistener_api_key, rate_limit)
    for op in cl_opinions:
        all_text += f" {op.get('docket_number', '')} {op.get('case_name', '')} {op.get('snippet', '')}"

        if op.get("citation"):
            enrichment["citations"].append(op["citation"])

        url = f"https://www.courtlistener.com{op['absolute_url']}" if op.get("absolute_url") else ""
        if url:
            enrichment["cl_opinion_links"].append(DiscoveredLink(
                url=url,
                source_class=SourceRank.COURT_GOV.value,
                link_type="court_opinion",
                notes=f"CourtListener opinion: {op.get('case_name', '')} ({op.get('date_filed', '')})",
                download_recommended=True,
                official_corroboration=True,
            ))

    log.info("CourtListener opinions: %d results", len(cl_opinions))

    # --- CourtListener: RECAP Dockets ---
    log.info("Searching CourtListener RECAP dockets for '%s' in %s", name, state)
    cl_dockets = search_courtlistener_dockets(name, state, courtlistener_api_key, rate_limit)
    for dk in cl_dockets:
        all_text += f" {dk.get('docket_number', '')} {dk.get('case_name', '')}"

        if dk.get("docket_number"):
            enrichment["docket_numbers"].append(dk["docket_number"])

        url = f"https://www.courtlistener.com{dk['docket_url']}" if dk.get("docket_url") else ""
        if url:
            enrichment["cl_docket_links"].append(DiscoveredLink(
                url=url,
                source_class=SourceRank.COURT_GOV.value,
                link_type="court_docket",
                notes=f"RECAP docket: {dk.get('case_name', '')} ({dk.get('docket_number', '')})",
                download_recommended=True,
                official_corroboration=True,
            ))

        # Fetch actual documents from dockets that have them
        if dk.get("docket_id") and dk.get("document_count", 0) > 0:
            docs = fetch_docket_documents(dk["docket_id"], courtlistener_api_key, rate_limit)
            for doc in docs:
                if doc.get("download_url"):
                    # Classify by description
                    desc_lower = doc.get("description", "").lower()
                    if any(w in desc_lower for w in ["sentence", "judgment", "conviction"]):
                        link_type = "sentencing_order"
                    elif any(w in desc_lower for w in ["indictment", "information", "grand jury"]):
                        link_type = "indictment"
                    elif any(w in desc_lower for w in ["plea", "agreement"]):
                        link_type = "judgment"
                    else:
                        link_type = "pdf_document"

                    enrichment["cl_document_links"].append(DiscoveredLink(
                        url=doc["download_url"],
                        source_class=SourceRank.COURT_GOV.value,
                        link_type=link_type,
                        notes=f"RECAP doc: {doc.get('description', '')} ({doc.get('page_count', '?')} pages)",
                        download_recommended=True,
                        official_corroboration=True,
                    ))

    log.info("CourtListener dockets: %d results, %d documents", len(cl_dockets), len(enrichment["cl_document_links"]))

    # --- case.law (Harvard CAP) ---
    log.info("Searching case.law for '%s' in %s", name, state)
    cap_results = search_caselaw(name, state, caselaw_api_key, rate_limit)
    for cap in cap_results:
        all_text += f" {cap.get('docket_number', '')} {cap.get('case_name', '')}"

        if cap.get("citation"):
            enrichment["citations"].append(cap["citation"])
        if cap.get("docket_number"):
            enrichment["docket_numbers"].append(cap["docket_number"])

        url = cap.get("frontend_url", "")
        if url:
            enrichment["cap_links"].append(DiscoveredLink(
                url=url,
                source_class=SourceRank.COURT_GOV.value,
                link_type="court_opinion",
                notes=f"case.law: {cap.get('case_name', '')} ({cap.get('decision_date', '')})",
                download_recommended=True,
                official_corroboration=True,
            ))

    log.info("case.law: %d results", len(cap_results))

    # --- Extract case numbers from all accumulated text ---
    enrichment["case_numbers"] = extract_case_numbers(all_text)
    # Dedupe docket numbers
    enrichment["docket_numbers"] = list(dict.fromkeys(enrichment["docket_numbers"]))
    enrichment["citations"] = list(dict.fromkeys(enrichment["citations"]))

    log.info(
        "Enrichment complete for %s: %d case numbers, %d docket numbers, %d citations, %d total links",
        candidate.case_id,
        len(enrichment["case_numbers"]),
        len(enrichment["docket_numbers"]),
        len(enrichment["citations"]),
        sum(len(enrichment[k]) for k in ["cl_opinion_links", "cl_docket_links", "cl_document_links", "cap_links"]),
    )

    return enrichment


# ---------------------------------------------------------------------------
# Case-number-enhanced Brave queries
# ---------------------------------------------------------------------------

# State portal site: targets for case-number-based searches
STATE_CASE_NUMBER_TARGETS = {
    "FL": [
        '"{case_num}" site:myfloridacounty.com',
        '"{case_num}" {county} florida clerk court filetype:pdf',
        '"{case_num}" "judgment and sentence" florida',
    ],
    "TX": [
        '"{case_num}" site:research.txcourts.gov',
        '"{case_num}" {county} texas district clerk',
        '"{case_num}" "judgment of conviction" texas',
    ],
    "OH": [
        '"{case_num}" site:courtrecords.ohio.gov',
        '"{case_num}" {county} ohio common pleas',
        '"{case_num}" "sentencing entry" ohio',
    ],
    "AZ": [
        '"{case_num}" site:azcourts.gov',
        '"{case_num}" maricopa superior court',
        '"{case_num}" "minute entry" arizona',
    ],
}


def build_case_number_queries(
    candidate: CaseCandidate,
    case_numbers: list[str],
    docket_numbers: list[str],
) -> list[str]:
    """Build surgical Brave queries using discovered case numbers.

    These are much more precise than name-only queries and target
    state court portals directly.
    """
    state = candidate.state.upper()
    templates = STATE_CASE_NUMBER_TARGETS.get(state, [])
    if not templates:
        return []

    county = ""
    if candidate.city:
        from .discovery import _get_county
        county = _get_county(candidate.city)

    queries = []
    seen = set()

    # Use both case numbers and docket numbers
    all_numbers = list(dict.fromkeys(case_numbers + docket_numbers))

    for num in all_numbers[:3]:  # Cap at 3 numbers to limit API calls
        for template in templates:
            q = template.format(case_num=num, county=county)
            if not county:
                q = q.replace("  ", " ").strip()
            if q not in seen:
                seen.add(q)
                queries.append(q)

    return queries
