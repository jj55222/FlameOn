"""Case Search — CourtListener API integration.

Enriches discovery with:
  1. CourtListener: Federal + state court opinion search, RECAP docket search,
     case-number extraction from docket results.
  2. Case-number-enhanced Brave queries for surgical artifact retrieval
     from state court portals.

Note: Harvard case.law (CAP) API was wound down in 2024. All CAP data has
been migrated to CourtListener by the Free Law Project. We now search CL
for both federal and state opinions, which covers the same corpus.

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

        docket_number = hit.get("docketNumber", "")

        # Skip civil cases — we only want criminal dockets.
        # Federal civil cases use "-cv-", criminal use "-cr-".
        if "-cv-" in docket_number:
            log.debug("Skipping civil docket: %s (%s)", docket_number, hit.get("caseName", ""))
            continue

        results.append({
            "case_name": hit.get("caseName", ""),
            "docket_number": docket_number,
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
# Published opinions (replaces deprecated case.law / Harvard CAP API)
# ---------------------------------------------------------------------------
# Harvard CAP's api.case.law/v1 was wound down in 2024. All CAP data is
# now hosted in CourtListener. We search CL opinions with a "criminal"
# filter to find published appellate opinions that confirm case closure.


def search_published_opinions(
    suspect_name: str,
    state: str,
    api_key: str = "",
    rate_limit: float = 2.0,
) -> list[dict]:
    """Search CourtListener for published opinions mentioning a suspect.

    Replaces the old case.law search. CL now contains all CAP data plus
    court-scraped opinions.

    Returns list of dicts with: case_name, citation, court, decision_date,
    docket_number, opinion_id, frontend_url.
    """
    if not suspect_name:
        return []

    # Search state appellate + supreme courts (where published opinions live)
    courts = STATE_TO_CL_STATE_COURTS.get(state.upper(), [])
    if not courts:
        return []

    params = {
        "q": f'"{suspect_name}" criminal',
        "type": "o",
        "court": " ".join(courts),
        "order_by": "score desc",
        "page_size": 5,
        "stat_Published": "on",
    }

    data = _cl_get("search", params, api_key)
    if not data:
        return []

    results = []
    for hit in data.get("results", []):
        results.append({
            "case_name": hit.get("caseName", ""),
            "citation": (hit.get("citation", [""]) or [""])[0] if isinstance(hit.get("citation"), list) else hit.get("citation", ""),
            "court": hit.get("court", ""),
            "decision_date": hit.get("dateFiled", ""),
            "docket_number": hit.get("docketNumber", ""),
            "opinion_id": hit.get("id", ""),
            "frontend_url": f"https://www.courtlistener.com{hit['absolute_url']}" if hit.get("absolute_url") else "",
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
    """Extract case numbers from text using common patterns.

    Filters out civil case numbers (-cv-) since we only want criminal cases.
    """
    numbers = []
    seen = set()
    for pattern in CASE_NUMBER_PATTERNS:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            # Prefer capture group 1 if present (strips prefix like "Case No.")
            num = (match.group(1) if match.lastindex and match.group(1) else match.group(0)).strip()
            # Skip civil case numbers — only criminal cases are relevant
            if "-cv-" in num.lower():
                continue
            if num.upper() not in seen:
                seen.add(num.upper())
                numbers.append(num)
    return numbers


# ---------------------------------------------------------------------------
# Enrichment: run all sources, extract case numbers, build enhanced queries
# ---------------------------------------------------------------------------

def _is_operation_name(name: str) -> bool:
    """Check if a suspect_name is actually an operation name, not a person."""
    return bool(re.match(r"(?i)^operation\s+", name))


def enrich_case(
    candidate: CaseCandidate,
    courtlistener_api_key: str = "",
    caselaw_api_key: str = "",
    rate_limit: float = 2.0,
) -> dict:
    """Run CourtListener searches for a candidate.

    Returns enrichment dict with:
      - case_numbers: list of extracted case numbers
      - docket_numbers: list of docket numbers from CourtListener
      - citations: list of legal citations
      - cl_opinion_links: list of DiscoveredLink from CL opinions
      - cl_docket_links: list of DiscoveredLink from CL dockets
      - cl_document_links: list of DiscoveredLink from RECAP documents
      - cap_links: list of DiscoveredLink from published opinion search (replaces case.law)
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

    # Operation names (e.g. "Operation Community Shield") are not person names —
    # CourtListener searches will return noise. Skip API enrichment entirely.
    if _is_operation_name(name):
        log.info("Skipping CourtListener enrichment for operation name: %s", name)
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

    # --- Published opinions (replaces deprecated case.law / Harvard CAP) ---
    log.info("Searching published opinions for '%s' in %s", name, state)
    pub_results = search_published_opinions(name, state, courtlistener_api_key, rate_limit)
    for pub in pub_results:
        all_text += f" {pub.get('docket_number', '')} {pub.get('case_name', '')}"

        if pub.get("citation"):
            enrichment["citations"].append(pub["citation"])
        if pub.get("docket_number"):
            enrichment["docket_numbers"].append(pub["docket_number"])

        url = pub.get("frontend_url", "")
        if url:
            enrichment["cap_links"].append(DiscoveredLink(
                url=url,
                source_class=SourceRank.COURT_GOV.value,
                link_type="court_opinion",
                notes=f"Published opinion: {pub.get('case_name', '')} ({pub.get('decision_date', '')})",
                download_recommended=True,
                official_corroboration=True,
            ))

    log.info("Published opinions: %d results", len(pub_results))

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


# ---------------------------------------------------------------------------
# Direct portal URL construction (Phase 0b — zero API cost)
# ---------------------------------------------------------------------------

# County-specific portal URL templates.
# Each entry: (portal_name, url_template, notes)
# Templates can use: {case_num}, {case_num_nodash}, {county}, {county_lower}
#
# These are best-effort guesses. Some will 404 — that's expected.
# The download stage validates them. Logging here lets us track hit rates.

COUNTY_PORTAL_URLS = {
    # --- Florida (per-county clerks) ---
    "FL": {
        "Miami-Dade": [
            ("Miami-Dade Clerk (CJIS)",
             "https://www2.miami-dadeclerk.com/cjis/CaseSearch/Details/?CaseNo={case_num_nodash}",
             "Form-based but sometimes accepts direct case number param"),
        ],
        "Broward": [
            ("Broward Clerk of Courts",
             "https://www.browardclerk.org/Web2/CaseSearch/Details/?caseid={case_num_nodash}",
             "Broward Web2 portal — case ID format may differ"),
        ],
        "Orange": [
            ("Orange County myeclerk",
             "https://myeclerk.myorangeclerk.com/Cases/Search?q={case_num}",
             "Orange County Odyssey-based search"),
        ],
        "Hillsborough": [
            ("Hillsborough Clerk",
             "https://pubrec10.hillsclerk.com/Unsecured/CaseDetail.aspx?CaseID={case_num_nodash}",
             "Hillsborough Odyssey portal"),
        ],
        "Duval": [
            ("Duval County Clerk",
             "https://core.duvalclerk.com/CoreCms.aspx?q={case_num}",
             "Duval CORE portal — may require session"),
        ],
        "Palm Beach": [
            ("Palm Beach Clerk",
             "https://applications.mypalmbeachclerk.com/eCaseView/search.do?searchType=CASE&caseNumber={case_num}",
             "Palm Beach eCaseView"),
        ],
        "Pinellas": [
            ("Pinellas Clerk",
             "https://ccmspa.pinellascounty.org/PublicAccess/CaseDetail.aspx?CaseID={case_num_nodash}",
             "Pinellas public access portal"),
        ],
    },
    # --- Arizona ---
    "AZ": {
        "Maricopa": [
            ("Maricopa Superior Court Docket",
             "https://www.superiorcourt.maricopa.gov/docket/CriminalCourtCases/caseInfo.asp?caseNumber={case_num}",
             "Direct docket lookup — works for CR-format case numbers"),
            ("Maricopa Clerk of Court",
             "https://www.clerkofcourt.maricopa.gov/records/electronic-court-records-ecr",
             "Landing page — no direct link, but logged for reference"),
        ],
        "Pima": [
            ("Pima County Consolidated Justice Court",
             "https://www.jp.pima.gov/",
             "Landing page only — no direct URL construction available"),
        ],
    },
    # --- Texas ---
    "TX": {
        "Harris": [
            ("Harris County District Clerk eDocs",
             "https://www.hcdistrictclerk.com/edocs/public/search.aspx?CaseNumber={case_num_nodash}",
             "Requires login — may not resolve directly but worth logging"),
        ],
        "Dallas": [
            ("Dallas County District Clerk",
             "https://www.dallascounty.org/department/distclerk/casesearch.php?casenumber={case_num_nodash}",
             "Dallas district clerk search"),
        ],
        "Tarrant": [
            ("Tarrant County District Clerk",
             "https://apps.tarrantcounty.com/DistrictClerk/CrimCaseSearch/CaseDetail.aspx?caseNumber={case_num_nodash}",
             "Tarrant Odyssey-based portal"),
        ],
        "Bexar": [
            ("Bexar County District Clerk",
             "https://www.bexar.org/2852/Search-Court-Records",
             "Landing page only — no direct URL construction"),
        ],
        "Travis": [
            ("Travis County District Clerk",
             "https://odyssey.traviscountytx.gov/default.aspx",
             "Odyssey portal — form-based, no direct URL"),
        ],
    },
    # --- Ohio ---
    "OH": {
        "Franklin": [
            ("Franklin County Common Pleas CIO",
             "https://fcdcfcjs.co.franklin.oh.us/CaseInformationOnline/caseDetail?caseID={case_num_nodash}",
             "Franklin County Case Information Online"),
        ],
        "Cuyahoga": [
            ("Cuyahoga County CP Docket",
             "https://cpdocket.cp.cuyahogacounty.gov/CaseDetail.aspx?CaseID={case_num_nodash}",
             "Cuyahoga Common Pleas docket — may require session"),
        ],
        "Hamilton": [
            ("Hamilton County Clerk",
             "https://www.courtclerk.org/records/case-search/?casenumber={case_num_nodash}",
             "Hamilton County Clerk of Courts"),
        ],
        "Summit": [
            ("Summit County Clerk",
             "https://www.summitohioprobate.com/",
             "Landing page only"),
        ],
        "Lucas": [
            ("Lucas County Clerk",
             "https://co.lucas.oh.us/1061/Online-Records-Search",
             "Landing page only"),
        ],
    },
}

# Statewide portals that work for any county in the state
STATEWIDE_PORTAL_URLS = {
    "AZ": [
        ("Arizona Judicial Branch (eAccess)",
         "https://apps.supremecourt.az.gov/publicaccess/caselookup.aspx",
         "Statewide eAccess — form-based, logged as reference"),
    ],
    "OH": [
        ("Ohio Supreme Court Public Docket",
         "https://www.supremecourt.ohio.gov/clerk/ecms/",
         "Supreme Court docket — appellate cases only"),
    ],
    "FL": [
        ("Florida Appellate Case Info (ACIS)",
         "https://acis.flcourts.gov/",
         "Appellate docket search — useful for appeals"),
    ],
    "TX": [
        ("Texas Courts Case Search",
         "https://search.txcourts.gov/CaseSearch.aspx?coa=cossup&s={case_num}",
         "Statewide appellate search — may accept case numbers"),
    ],
}

# Federal court URL templates (work when we have docket_id or case number)
FEDERAL_URL_TEMPLATES = [
    ("CourtListener Docket",
     "https://www.courtlistener.com/docket/{docket_id}/",
     "Direct link from enrichment docket_id — high confidence"),
    ("PACER (via RECAP)",
     "https://www.courtlistener.com/docket/{docket_id}/",
     "Same as above — RECAP mirrors PACER"),
]


def build_direct_portal_urls(
    candidate: CaseCandidate,
    case_numbers: list[str],
    docket_numbers: list[str],
    docket_ids: list[int | str] = None,
) -> list[DiscoveredLink]:
    """Construct direct court portal URLs from case metadata (Phase 0b).

    Zero API cost — these are URL guesses based on known portal patterns.
    Each URL is logged with its construction rationale so we can track
    hit/miss rates when the download stage attempts to fetch them.

    Returns list of DiscoveredLink with source_class=court_gov or county_clerk.
    """
    state = candidate.state.upper()
    city = (candidate.city or "").lower()

    # Resolve county from city
    from .discovery import _get_county
    county = _get_county(city) if city else ""

    links = []
    seen_urls = set()

    all_numbers = list(dict.fromkeys(case_numbers + docket_numbers))

    if not all_numbers and not docket_ids:
        log.info(
            "[Phase 0b] No case numbers or docket IDs for %s — skipping direct URL construction",
            candidate.case_id,
        )
        return links

    log.info(
        "[Phase 0b] Building direct portal URLs for %s: state=%s, county=%s, "
        "case_numbers=%s, docket_numbers=%s, docket_ids=%s",
        candidate.case_id, state, county or "(unknown)",
        case_numbers, docket_numbers, docket_ids or [],
    )

    # --- County-specific portals ---
    county_portals = COUNTY_PORTAL_URLS.get(state, {}).get(county, [])
    if county_portals:
        log.info("[Phase 0b] Found %d county portal templates for %s County, %s",
                 len(county_portals), county, state)
    else:
        log.info("[Phase 0b] No county portal templates for county=%s, state=%s",
                 county or "(unknown)", state)

    for case_num in all_numbers[:3]:
        # Strip dashes/spaces for portals that don't use them
        case_num_nodash = re.sub(r"[-\s/]", "", case_num)

        for portal_name, url_template, notes in county_portals:
            try:
                url = url_template.format(
                    case_num=case_num,
                    case_num_nodash=case_num_nodash,
                    county=county,
                    county_lower=county.lower().replace(" ", ""),
                )
            except (KeyError, IndexError):
                log.warning("[Phase 0b] Template format error: %s with case_num=%s",
                            portal_name, case_num)
                continue

            if url in seen_urls:
                continue
            seen_urls.add(url)

            log.info(
                "[Phase 0b] CONSTRUCTED: %s | portal=%s | case_num=%s | notes=%s",
                url, portal_name, case_num, notes,
            )

            links.append(DiscoveredLink(
                url=url,
                source_class=SourceRank.COUNTY_CLERK.value,
                link_type="court_docket",
                notes=f"[direct-url] {portal_name}: {case_num} — {notes}",
                download_recommended=True,
                official_corroboration=True,
            ))

        # --- Statewide portals ---
        for portal_name, url_template, notes in STATEWIDE_PORTAL_URLS.get(state, []):
            try:
                url = url_template.format(
                    case_num=case_num,
                    case_num_nodash=case_num_nodash,
                )
            except (KeyError, IndexError):
                continue

            if url in seen_urls:
                continue
            seen_urls.add(url)

            log.info(
                "[Phase 0b] STATEWIDE: %s | portal=%s | case_num=%s | notes=%s",
                url, portal_name, case_num, notes,
            )

            links.append(DiscoveredLink(
                url=url,
                source_class=SourceRank.COURT_GOV.value,
                link_type="court_docket",
                notes=f"[direct-url] {portal_name}: {case_num} — {notes}",
                download_recommended=False,  # landing pages aren't directly downloadable
                official_corroboration=False,
            ))

    # --- Federal docket links from CourtListener docket IDs ---
    if docket_ids:
        for did in docket_ids:
            url = f"https://www.courtlistener.com/docket/{did}/"
            if url in seen_urls:
                continue
            seen_urls.add(url)

            log.info(
                "[Phase 0b] FEDERAL: %s | docket_id=%s | source=CourtListener RECAP",
                url, did,
            )

            links.append(DiscoveredLink(
                url=url,
                source_class=SourceRank.COURT_GOV.value,
                link_type="court_docket",
                notes=f"[direct-url] CourtListener RECAP docket #{did}",
                download_recommended=True,
                official_corroboration=True,
            ))

    log.info(
        "[Phase 0b] Complete for %s: %d direct URLs constructed (%d county, %d state, %d federal)",
        candidate.case_id,
        len(links),
        sum(1 for l in links if l.source_class == SourceRank.COUNTY_CLERK.value),
        sum(1 for l in links if l.source_class == SourceRank.COURT_GOV.value and "RECAP" not in l.notes),
        sum(1 for l in links if "RECAP" in l.notes),
    )

    return links


# ---------------------------------------------------------------------------
# Case-number-enhanced Brave queries
# ---------------------------------------------------------------------------


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
