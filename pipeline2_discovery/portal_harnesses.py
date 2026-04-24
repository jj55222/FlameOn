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
    NextRequest public archives expose:
      /requests          — public request index (with optional ?query=)
      /documents         — released-document index (with optional ?query=)

    No auth required for browsing published records. Page is standard HTML
    with anchor tags pointing at `/requests/<id>` and `/documents/<id>`.
    """
    vendor = "nextrequest"
    api_tag = "nextrequest_portal"

    def search(self, defendant, jurisdiction="", limit=20):
        sources = []
        seen = set()

        for path in ("/requests", "/documents"):
            url = f"{self.base_url}{path}"
            text, _ = _fetch(url, params={"query": defendant}, session=self.session)
            if not text:
                continue

            parser = _LinkCollector()
            try:
                parser.feed(text)
            except Exception:
                continue

            for href, link_text in parser.links:
                if not href:
                    continue
                # Only keep detail-page anchors within this NextRequest instance
                if not re.search(r"^/(requests|documents)/\w", href):
                    continue
                abs_url = urljoin(self.base_url + "/", href.lstrip("/"))
                if abs_url in seen:
                    continue
                # Score: name match on link text OR slug
                text_blob = f"{link_text} {href}"
                rel = _score_relevance(defendant, text_blob)
                if rel <= 0:
                    continue
                seen.add(abs_url)
                evidence_hint = "foia_document" if "/documents/" in href else "foia_request"
                sources.append(self._as_source(abs_url, link_text[:150], rel, evidence_hint))
                if len(sources) >= limit:
                    return sources

        return sources


# ─────────────────────────────────────────────────────────────
# GovQA harness — searches the knowledge-base search endpoint
# ─────────────────────────────────────────────────────────────

class GovQAHarness(PortalHarness):
    """
    GovQA (Granicus) supports GET ?searchPhrase=... on its public entry point.
    Works across govqa.us and mycusthelp.com subdomains (same platform).

    Most GovQA installs gate full requests behind account login, but the
    search result page itself is public and lists matching request titles
    with /WEBAPP/_rs/(S(...))/RequestArchive.aspx?... detail links.
    """
    vendor = "govqa"
    api_tag = "govqa_portal"

    def search(self, defendant, jurisdiction="", limit=20):
        sources = []
        seen = set()

        # Several GovQA variants — try in order, first that returns HTML wins
        candidate_paths = [
            "/WEBAPP/_rs/SupportHome.aspx",
            "/webapp/_rs/supporthome.aspx",
            "/",
        ]

        page_text = None
        for p in candidate_paths:
            text, _ = _fetch(
                f"{self.base_url}{p}",
                params={"searchPhrase": defendant},
                session=self.session,
            )
            if text and len(text) > 500:
                page_text = text
                break

        if not page_text:
            return sources

        parser = _LinkCollector()
        try:
            parser.feed(page_text)
        except Exception:
            return sources

        # GovQA result links contain "/WEBAPP/" and either RequestArchive.aspx
        # or RequestDisplay.aspx. Anything else is navigation chrome.
        pattern = re.compile(r"WEBAPP.*(RequestArchive|RequestDisplay|SolutionDisplay)\.aspx", re.I)

        for href, link_text in parser.links:
            if not href or not pattern.search(href):
                continue
            abs_url = urljoin(self.base_url + "/", href.lstrip("/"))
            if abs_url in seen:
                continue
            rel = _score_relevance(defendant, f"{link_text} {href}")
            if rel <= 0:
                continue
            seen.add(abs_url)
            sources.append(self._as_source(abs_url, link_text[:150], rel, "foia_request"))
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
