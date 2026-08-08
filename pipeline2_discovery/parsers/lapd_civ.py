"""
lapd_civ.py — Parser for LAPD Critical Incident Videos pages.

Source: https://www.lapdonline.org/office-of-the-chief-of-police/professional-standards-bureau/critical-incident-videos/

Structure observed (April 2026):
  - Top-level index page links to year-specific subpages
    (2020, 2021, 2022, 2023, 2024 — slug pattern: <year>-o-i-s-shootings-and-critical-incidents/)
  - Each year subpage contains a table with incident rows:
      F### -YY     | OIS/H or OIS/NH or other | Division | Address | Subject-name link to /newsroom/<slug-with-name>/
  - Scattered throughout: YouTube/youtu.be video URLs (CIB videos, bodycam clips)

This parser:
  1. Fetches the index, discovers year subpages.
  2. For each year subpage: extracts the incident table + YouTube URLs.
  3. Tries to associate video URLs with incident rows by proximity in the HTML.
  4. Emits records matching the cib_cache schema (see parsers/__init__.py).

CLI:
    python lapd_civ.py --output ../cib_cache/lapd.json
    python lapd_civ.py --years 2022,2023,2024 --output ../cib_cache/lapd.json
    python lapd_civ.py --dry-run
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

INDEX_URL = "https://www.lapdonline.org/office-of-the-chief-of-police/professional-standards-bureau/critical-incident-videos/"
USER_AGENT = "FlameOn-Research/1.0 (LAPD CIV parser)"
TIMEOUT = 25
AGENCY = "LAPD"

# Incident ID patterns observed:
#   F015-23     fatal OIS
#   NHSF030-23  non-hit shooting
#   NRF064-23   non-shooting force
INCIDENT_ID_RE = re.compile(r"\b(?:F|NHSF|NRF|NHF)\s*\d{3}-\d{2}[A-Z]*\b", re.IGNORECASE)
# Type cells: OIS/H, OIS/NH, IOD (in-custody death), UOF, etc.
TYPE_RE = re.compile(r"\b(OIS/?[HN]?H?|UOF|IOD|IN[-\s]CUSTODY)\b", re.IGNORECASE)
NEWSROOM_LINK_RE = re.compile(
    r'<a[^>]+href=["\'](https?://[^"\']*lapdonline\.org/newsroom/[^"\']+)["\'][^>]*>([^<]+)</a>',
    re.IGNORECASE,
)
VIDEO_URL_RE = re.compile(
    r'https?://[^\s"\'<>]*?(?:youtube\.com/(?:watch\?v=|embed/|playlist\?list=)|youtu\.be/)[^\s"\'<>]+',
    re.IGNORECASE,
)
TABLE_ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")


def _strip_tags(html):
    return TAG_RE.sub(" ", html or "").replace("&nbsp;", " ").replace("&amp;", "&").strip()


def _collapse_ws(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def fetch(url):
    """HTTP GET with consistent UA."""
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        return r.text
    except requests.RequestException:
        return None


def discover_year_pages():
    """Return list of (year_int, full_url) for available year subpages."""
    html = fetch(INDEX_URL)
    if not html:
        return []
    out = []
    for m in re.finditer(
        r'href=["\'](https?://[^"\']*critical-incident-videos/(\d{4})-[^"\']+)["\']',
        html, re.IGNORECASE,
    ):
        url, year = m.group(1), int(m.group(2))
        if not url.endswith("/"):
            url += "/"
        if (year, url) not in out:
            out.append((year, url))
    return sorted(out, key=lambda x: x[0])


def extract_incidents_from_year_page(html, year_url, year):
    """Parse a year subpage's HTML table into incident records."""
    if not html:
        return []
    incidents = []
    # Per-row parse: find each table row, walk its cells
    rows = TABLE_ROW_RE.findall(html)
    for row_html in rows:
        cells = CELL_RE.findall(row_html)
        if len(cells) < 3:
            continue
        cell_texts = [_collapse_ws(_strip_tags(c)) for c in cells]
        # Find the incident_id cell (matches F### or NHSF###)
        id_match = None
        for c in cell_texts:
            m = INCIDENT_ID_RE.search(c)
            if m:
                id_match = m.group(0).replace(" ", "").upper()
                break
        if not id_match:
            continue
        # Type cell — look across all cells for OIS/UOF marker
        incident_type = ""
        for c in cell_texts:
            tm = TYPE_RE.search(c)
            if tm:
                incident_type = tm.group(0).upper()
                break
        # Subject name + newsroom link from anchor in the row
        subjects = []
        newsroom_url = ""
        for nm in NEWSROOM_LINK_RE.finditer(row_html):
            url = nm.group(1)
            text = _collapse_ws(nm.group(2))
            if not newsroom_url:
                newsroom_url = url
            if text and len(text) > 3 and text not in subjects:
                subjects.append(text)
        # Division + address heuristic: cells 1 and 2 (after id), filtering out type/url
        division = ""
        location = ""
        for c in cell_texts:
            if INCIDENT_ID_RE.search(c) or TYPE_RE.fullmatch(c.split()[0] if c.split() else ""):
                continue
            if not division and len(c) < 30 and not c.startswith("http"):
                division = c
            elif not location and len(c) < 100 and not c.startswith("http"):
                location = c
                break
        # Per-row video URLs
        video_urls = list(set(VIDEO_URL_RE.findall(row_html)))

        incidents.append({
            "agency": AGENCY,
            "incident_id": id_match,
            "incident_date": "",  # year-only on this page
            "incident_year": year,
            "incident_type": incident_type,
            "division": division,
            "location": location,
            "subjects": subjects,
            "video_urls": video_urls,
            "documents": [newsroom_url] if newsroom_url else [],
            "summary": _collapse_ws(" | ".join(c for c in cell_texts if c))[:200],
            "source_url": year_url,
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        })
    return incidents


def fetch_and_parse(years=None, throttle_sec=1.0):
    """
    Crawl LAPD CIV index + year subpages. Returns list of incident dicts.
    `years` filter: list of int year values, or None for all available.
    """
    year_pages = discover_year_pages()
    if not year_pages:
        return []
    if years:
        year_set = set(int(y) for y in years)
        year_pages = [(y, u) for y, u in year_pages if y in year_set]

    all_incidents = []
    for year, url in year_pages:
        html = fetch(url)
        if not html:
            continue
        incidents = extract_incidents_from_year_page(html, url, year)
        all_incidents.extend(incidents)
        time.sleep(throttle_sec)
    return all_incidents


def main():
    parser = argparse.ArgumentParser(description="LAPD Critical Incident Videos parser")
    here = Path(__file__).parent
    default_out = here.parent / "cib_cache" / "lapd.json"
    parser.add_argument("--output", default=str(default_out),
                        help=f"Output JSON path (default: {default_out})")
    parser.add_argument("--years", help="Comma-separated years to fetch (e.g. 2022,2023,2024). Default: all.")
    parser.add_argument("--throttle", type=float, default=1.0, help="Seconds between page fetches")
    parser.add_argument("--dry-run", action="store_true",
                        help="Discover year pages but skip fetching content")
    args = parser.parse_args()

    if args.dry_run:
        pages = discover_year_pages()
        print(f"[DRY RUN] Discovered {len(pages)} year subpages:")
        for y, u in pages:
            print(f"  {y}: {u}")
        return

    years = [int(y) for y in args.years.split(",")] if args.years else None
    print(f"Fetching LAPD CIV (years filter: {years or 'all'})")
    incidents = fetch_and_parse(years=years, throttle_sec=args.throttle)
    print(f"Extracted {len(incidents)} incident records")

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(incidents, f, indent=2, ensure_ascii=False)
    # Quick aggregate
    by_year = {}
    n_with_video = 0
    n_with_subject = 0
    for i in incidents:
        by_year[i.get("incident_year")] = by_year.get(i.get("incident_year"), 0) + 1
        if i.get("video_urls"):
            n_with_video += 1
        if i.get("subjects"):
            n_with_subject += 1
    print(f"  By year: {sorted(by_year.items())}")
    print(f"  With video URLs: {n_with_video}/{len(incidents)}")
    print(f"  With named subjects: {n_with_subject}/{len(incidents)}")
    print(f"  Saved to {out}")


if __name__ == "__main__":
    main()
