"""
portal_harnesses.py — Native scrapers for FOIA portal vendors
==============================================================

Replaces wasteful Firecrawl AI-extract calls (which cost 105 credits for
0 usable hits in `portal_position_log.json`) with vendor-specific HTML
parsers that use plain `requests` + stdlib `html.parser`. Zero Firecrawl
credits.

Vendor coverage (from JURISDICTION_PORTALS analysis — 32 agencies):
  - NextRequest     10 agencies (31%)  — public /documents + /requests archives
  - GovQA/mycusthelp 13 agencies (41%)  — knowledge-base search; intake-gated for full requests
  - Together         23/27 scrape-able agencies  (85%)

Each harness exposes:
    search(defendant, jurisdiction, limit=20) -> list[dict]
where dict matches research.py's internal source shape:
    {"url", "type", "relevance_score", "description", "api", "source_domain"}

CLI (for smoke-testing):
    python portal_harnesses.py --vendor nextrequest \\
        --base https://sfdpa.nextrequest.com --defendant "Edwards" --dry-run
    python portal_harnesses.py --vendor govqa \\
        --base https://sanfranciscopd.govqa.us --defendant "Edwards" --dry-run
    python portal_harnesses.py --from-registry "CA.san_francisco.dpa" --defendant "Edwards" --dry-run
"""

import argparse
import json
import re
import sys
import time
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import quote_plus, urljoin, urlparse

import requests


HERE = Path(__file__).parent.resolve()
DEFAULT_TIMEOUT = 20
DEFAULT_UA = "FlameOn-Research/1.0 (+research-agent; contact via MuckRock)"


# ─────────────────────────────────────────────────────────────
# HTML parsing helper (stdlib — bs4 not installed)
# ─────────────────────────────────────────────────────────────

class _LinkCollector(HTMLParser):
    """Collect (href, link_text) tuples from an HTML document."""
    def __init__(self):
        super().__init__()
        self._current_href = None
        self._current_text = []
        self.links = []  # list of (href, text)

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            href = None
            for k, v in attrs:
                if k.lower() == "href":
                    href = v
                    break
            self._current_href = href
            self._current_text = []

    def handle_data(self, data):
        if self._current_href is not None:
            self._current_text.append(data)

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self._current_href is not None:
            text = "".join(self._current_text).strip()
            self.links.append((self._current_href, text))
            self._current_href = None
            self._current_text = []


def _fetch(url, params=None, session=None):
    """HTTP GET with consistent UA + timeout. Returns (text, final_url) or (None, None)."""
    try:
        s = session or requests.Session()
        resp = s.get(
            url,
            params=params,
            headers={"User-Agent": DEFAULT_UA, "Accept": "text/html,application/xhtml+xml"},
            timeout=DEFAULT_TIMEOUT,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return None, None
        return resp.text, resp.url
    except requests.RequestException:
        return None, None


def _score_relevance(defendant, text):
    """Simple keyword-match relevance. Shared across harnesses."""
    if not text or not defendant:
        return 0.0
    tl = text.lower()
    dl = defendant.lower().strip()
    # Whole defendant name present
    if dl in tl:
        return 0.85
    # Last-name-only match (only useful if long enough to avoid common words)
    parts = dl.split()
    last = parts[-1] if parts else ""
    if len(last) >= 4 and last in tl:
        return 0.45
    return 0.0


# ─────────────────────────────────────────────────────────────
# Base harness
# ─────────────────────────────────────────────────────────────

class PortalHarness:
    """Base class. Subclass for each vendor."""
    vendor = "generic"
    api_tag = "portal"

    def __init__(self, base_url, session=None):
        self.base_url = base_url.rstrip("/")
        self.domain = urlparse(self.base_url).netloc.replace("www.", "")
        self.session = session or requests.Session()

    def search(self, defendant, jurisdiction="", limit=20):
        """Override in subclass. Returns list of source dicts."""
        raise NotImplementedError

    def _as_source(self, url, title, relevance, evidence_hint=None):
        """Normalize into research.py's internal source shape."""
        return {
            "url": url,
            "type": evidence_hint or "agency_portal",
            "relevance_score": relevance,
            "description": title,
            "api": self.api_tag,
            "source_domain": self.domain,
        }


# ─────────────────────────────────────────────────────────────
# NextRequest harness — hits the public /documents + /requests archive
# ─────────────────────────────────────────────────────────────

class NextRequestHarness(PortalHarness):
    """
    NextRequest is a JavaScript SPA — the rendered HTML is empty. But the
    SPA calls two public JSON endpoints that accept ?query=:
      GET /client/requests?query=<term>   → {total_count, requests:[...]}
      GET /client/documents?query=<term>  → {total_count, documents:[...]}

    No auth required. These return structured records (request_path,
    document_path, file_extension, highlights) that map cleanly to the
    p2_to_p3_case source contract.
    """
    vendor = "nextrequest"
    api_tag = "nextrequest_portal"

    def _get_json(self, path, params):
        try:
            resp = self.session.get(
                f"{self.base_url}{path}",
                params=params,
                headers={"User-Agent": DEFAULT_UA, "Accept": "application/json"},
                timeout=DEFAULT_TIMEOUT,
            )
            if resp.status_code != 200 or "json" not in resp.headers.get("content-type", "").lower():
                return None
            return resp.json()
        except (requests.RequestException, ValueError):
            return None

    def search(self, defendant, jurisdiction="", limit=20):
        sources = []
        seen = set()

        # Lane 1 — released documents (highest value: direct file URLs)
        docs = self._get_json("/client/documents", {"query": defendant})
        if docs:
            for d in docs.get("documents", []) or []:
                doc_path = d.get("document_path") or ""
                req_path = d.get("request_path") or ""
                # document_path is the stable anchor; request_path may also link the parent
                detail_path = doc_path or req_path
                if not detail_path:
                    continue
                abs_url = urljoin(self.base_url + "/", detail_path.lstrip("/"))
                if abs_url in seen:
                    continue
                title = (d.get("title") or "").strip()
                desc = (d.get("description") or "").strip()
                # Highlights (API-provided match highlights) give us a stronger relevance signal
                highlights = " ".join(d.get("highlights", []) or []).strip()
                blob = " ".join([title, desc, d.get("folder_name") or "", highlights])
                rel = _score_relevance(defendant, blob)
                if rel <= 0 and not highlights:
                    continue
                # Any document that came back on a ?query= call has already matched server-side
                rel = max(rel, 0.55)
                # File-extension → internal evidence type
                ext = (d.get("file_extension") or "").lower()
                if ext in ("mp4", "mov", "avi", "mkv", "webm"):
                    evidence_hint = "court_footage"
                elif ext in ("mp3", "wav", "m4a", "ogg"):
                    evidence_hint = "dispatch_audio"
                elif ext in ("pdf", "docx", "doc"):
                    evidence_hint = "foia_document"
                else:
                    evidence_hint = "foia_document"
                seen.add(abs_url)
                desc_str = f"{title} — {desc}" if desc else title
                sources.append(self._as_source(abs_url, desc_str[:180], rel, evidence_hint))
                if len(sources) >= limit:
                    return sources

        # Lane 2 — public requests archive (fewer direct files, but captures FOIA in progress)
        reqs = self._get_json("/client/requests", {"query": defendant})
        if reqs:
            for r in reqs.get("requests", []) or []:
                req_id = r.get("id")
                req_path = r.get("request_path") or (f"/requests/{req_id}" if req_id else "")
                if not req_path:
                    continue
                abs_url = urljoin(self.base_url + "/", req_path.lstrip("/"))
                if abs_url in seen:
                    continue
                text = r.get("request_text") or ""
                dept = r.get("department_names") or ""
                blob = " ".join([text, dept, str(req_id or "")])
                rel = _score_relevance(defendant, blob)
                # Server already filtered, so a miss on text still counts at low relevance
                rel = max(rel, 0.35)
                seen.add(abs_url)
                short_desc = (text or f"Request {req_id}").strip()[:180]
                sources.append(self._as_source(abs_url, short_desc, rel, "foia_request"))
                if len(sources) >= limit:
                    return sources

        return sources


# ─────────────────────────────────────────────────────────────
# GovQA harness — searches the knowledge-base search endpoint
# ─────────────────────────────────────────────────────────────

class GovQAHarness(PortalHarness):
    """
    GovQA (Granicus) is ASP.NET Web Forms. Search is a POST-back to the
    same SupportHome.aspx with hidden __VIEWSTATE + __RequestVerificationToken
    fields. The session token is embedded in the URL as `(S(xxxx))`.

    Works across govqa.us and mycusthelp.com (same platform, different DNS).
    Publishes an optional public knowledge base at SolutionsHome.aspx on
    some installs — that's simpler but not universal.
    """
    vendor = "govqa"
    api_tag = "govqa_portal"

    # Hidden fields we must echo back on the POST
    _POST_FIELDS = (
        "__VIEWSTATE", "__VIEWSTATEGENERATOR", "__VIEWSTATEENCRYPTED",
        "__EVENTTARGET", "__EVENTARGUMENT",
        "__RequestVerificationToken",
    )

    def _get_home(self):
        """Return (html, final_url_with_session_token) or (None, None)."""
        for p in ("/WEBAPP/_rs/SupportHome.aspx", "/webapp/_rs/supporthome.aspx"):
            text, final = _fetch(f"{self.base_url}{p}", session=self.session)
            if text and len(text) > 500 and "w_search_words" in text:
                return text, final
        return None, None

    def _extract_hidden(self, html):
        """Pull hidden form inputs from the ASP.NET form."""
        form = {}
        for name in self._POST_FIELDS:
            m = re.search(
                rf'name="{re.escape(name)}"[^>]*value="([^"]*)"', html, re.I,
            )
            if m:
                form[name] = m.group(1)
        return form

    def search(self, defendant, jurisdiction="", limit=20):
        sources = []
        seen = set()

        html, home_url = self._get_home()
        if not html or not home_url:
            return sources

        form = self._extract_hidden(html)
        if not form.get("__VIEWSTATE"):
            return sources  # No VIEWSTATE means we can't POST back

        # Include the search fields
        form["w_search_words"] = defendant
        form["w_search_btn"] = "Search"
        # Explicitly zero out event fields if not set
        form.setdefault("__EVENTTARGET", "")
        form.setdefault("__EVENTARGUMENT", "")

        try:
            resp = self.session.post(
                home_url,
                data=form,
                headers={
                    "User-Agent": DEFAULT_UA,
                    "Accept": "text/html,application/xhtml+xml",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": f"{urlparse(home_url).scheme}://{urlparse(home_url).netloc}",
                    "Referer": home_url,
                },
                timeout=DEFAULT_TIMEOUT,
                allow_redirects=True,
            )
            if resp.status_code != 200:
                return sources
            result_html = resp.text
        except requests.RequestException:
            return sources

        # Parse result anchors. Valid result targets:
        #   SolutionDisplay.aspx — knowledge base article (public)
        #   RequestArchive.aspx / RequestDisplay.aspx — gated, but title is in the anchor text
        parser = _LinkCollector()
        try:
            parser.feed(result_html)
        except Exception:
            return sources

        pattern = re.compile(r"(SolutionDisplay|RequestArchive|RequestDisplay)\.aspx", re.I)

        for href, link_text in parser.links:
            if not href:
                continue
            if not pattern.search(href):
                continue
            abs_url = urljoin(home_url, href) if not href.startswith("http") else href
            if abs_url in seen:
                continue
            rel = _score_relevance(defendant, link_text or "")
            # Server already filtered by the search term — give a floor
            rel = max(rel, 0.4)
            seen.add(abs_url)
            evidence_hint = "foia_document" if "SolutionDisplay" in href else "foia_request"
            sources.append(self._as_source(abs_url, (link_text or "")[:180], rel, evidence_hint))
            if len(sources) >= limit:
                break

        return sources


# ─────────────────────────────────────────────────────────────
# Dispatch by vendor name
# ─────────────────────────────────────────────────────────────

HARNESSES = {
    "nextrequest": NextRequestHarness,
    "govqa": GovQAHarness,
}


def harness_for_vendor(vendor):
    return HARNESSES.get(vendor)


def search_all_portals_for_jurisdiction(defendant, jurisdiction, limit=20, only_vendors=None):
    """
    Look up every registered agency whose jurisdiction matches, instantiate
    the right harness for each, and return a unified source list.

    This is the function that replaces the expensive `find_case_in_portal()`
    Firecrawl-extract pathway for jurisdictions with known portal shapes.
    """
    try:
        from research import JURISDICTION_PORTALS, get_portal_for_jurisdiction
    except ImportError:
        return []

    # Reuse the existing registry lookup
    try:
        entries = get_portal_for_jurisdiction(jurisdiction) or []
    except Exception:
        entries = []

    if not entries:
        # Fallback: naive substring match over JURISDICTION_PORTALS
        entries = []
        for key, info in JURISDICTION_PORTALS.items():
            jurs = [j.lower() for j in info.get("jurisdiction", [])]
            if any(j in (jurisdiction or "").lower() for j in jurs):
                entries.append(info)

    sources = []
    for entry in entries:
        portal_url = entry.get("portal_url")
        vendor = entry.get("portal_vendor")
        if not portal_url or not vendor:
            continue
        if only_vendors and vendor not in only_vendors:
            continue
        cls = HARNESSES.get(vendor)
        if cls is None:
            continue  # Not a supported vendor yet (e.g. justfoia, dynamics365)
        try:
            h = cls(portal_url)
            found = h.search(defendant, jurisdiction, limit=limit)
            if found:
                sources.extend(found)
        except Exception as e:
            print(f"  [WARN] {vendor} harness failed for {portal_url}: {e}", file=sys.stderr)

    return sources


# ─────────────────────────────────────────────────────────────
# CLI (smoke test + dry run)
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Native FOIA portal harnesses")
    parser.add_argument("--vendor", choices=sorted(HARNESSES.keys()), help="Which vendor to test")
    parser.add_argument("--base", help="Portal base URL (e.g. https://sfdpa.nextrequest.com)")
    parser.add_argument("--from-registry", help="Agency key from JURISDICTION_PORTALS (e.g. CA.san_francisco.dpa)")
    parser.add_argument("--defendant", required=True, help="Defendant name to search")
    parser.add_argument("--jurisdiction", default="", help="Jurisdiction hint (free-form)")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true", help="Do not make network calls; print plan")
    args = parser.parse_args()

    # Resolve base URL + vendor from registry if requested
    vendor = args.vendor
    base = args.base
    if args.from_registry:
        try:
            from research import JURISDICTION_PORTALS
        except ImportError as e:
            print(f"[ERR] could not import JURISDICTION_PORTALS from research.py: {e}")
            sys.exit(2)
        info = JURISDICTION_PORTALS.get(args.from_registry)
        if not info:
            print(f"[ERR] unknown registry key: {args.from_registry}")
            sys.exit(2)
        vendor = info.get("portal_vendor")
        base = info.get("portal_url")
        print(f"Registry lookup: vendor={vendor}  portal_url={base}")

    if not vendor or not base:
        print("[ERR] must specify either --vendor + --base, or --from-registry")
        sys.exit(2)
    if vendor not in HARNESSES:
        print(f"[ERR] vendor {vendor!r} has no harness yet. Supported: {sorted(HARNESSES)}")
        sys.exit(2)

    cls = HARNESSES[vendor]

    if args.dry_run:
        print(f"[DRY RUN] would instantiate {cls.__name__}(base={base!r})")
        print(f"[DRY RUN] would call .search(defendant={args.defendant!r}, "
              f"jurisdiction={args.jurisdiction!r}, limit={args.limit})")
        print(f"[DRY RUN] no HTTP calls made.")
        return

    h = cls(base)
    t0 = time.time()
    sources = h.search(args.defendant, args.jurisdiction, limit=args.limit)
    elapsed = time.time() - t0
    print(f"\n{len(sources)} results in {elapsed:.1f}s  (credits: 0 — native HTTP)")
    print(json.dumps(sources, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
