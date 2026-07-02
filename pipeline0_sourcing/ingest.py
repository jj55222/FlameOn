"""
ingest.py — pull raw crime-news signals from FREE, no-API-key sources.

Two OSINT sources, both keyless:
  * Google News RSS  — https://news.google.com/rss/search?q=<query>+when:<N>d
  * GDELT 2.0 Doc API — https://api.gdeltproject.org/api/v2/doc/doc  (ArtList JSON)

Each returns a normalized "signal" dict:
    {title, url, source, published, snippet, query, origin}

Network failures degrade gracefully (a dead source returns [] with a warning);
the pipeline keeps going on whatever it got. No key, no state, no side effects.

CLI:
    python ingest.py --terms "officer involved shooting" "in custody death" \
        --since 3 --limit 60 --out signals.json
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

# The EWU/Dr.Insanity worth model as search intent: serious crime + likely records.
DEFAULT_TERMS = [
    "officer involved shooting",
    "in custody death",
    "deputy involved shooting",
    "fatal police shooting",
    "police use of force investigation",
    "custodial death jail",
    "body camera footage released",
    "police pursuit death",
]

_UA = "FlameOn-P0-sourcing/0.1 (+https://github.com/jj55222/FlameOn)"
_GNEWS = "https://news.google.com/rss/search"
_GDELT = "https://api.gdeltproject.org/api/v2/doc/doc"

# GDELT's DOC 2.0 API rate-limits an unauthenticated client HARD: fanning our
# 8-term query set at it back-to-back 429s every call, silently dropping GDELT
# entirely. It asks for gentle polling — hold a minimum spacing between calls and
# back off on a 429. Google News RSS is more tolerant but also 429s under a fast
# fan-out, so we space it too (much shorter).
_GDELT_MIN_INTERVAL = 5.0        # seconds between GDELT calls (module-global clock)
_GNEWS_MIN_INTERVAL = 1.0        # seconds between Google News calls
_last_call = {"gdelt": 0.0, "google_news": 0.0}


def _throttle(source: str, min_interval: float) -> None:
    """Sleep just enough to keep `min_interval` between calls to `source`."""
    dt = time.monotonic() - _last_call.get(source, 0.0)
    if dt < min_interval:
        time.sleep(min_interval - dt)
    _last_call[source] = time.monotonic()


def _http_get(url: str, params: Dict, timeout: int = 20):
    if requests is None:
        raise RuntimeError("requests not installed")
    return requests.get(url, params=params, headers={"User-Agent": _UA}, timeout=timeout)


def fetch_google_news(term: str, since_days: int, limit: int) -> List[Dict]:
    """Google News RSS search. Keyless. Returns normalized signals."""
    q = f"{term} when:{since_days}d"
    params = {"q": q, "hl": "en-US", "gl": "US", "ceid": "US:en"}
    out: List[Dict] = []
    try:
        _throttle("google_news", _GNEWS_MIN_INTERVAL)
        r = _http_get(_GNEWS, params)
        r.raise_for_status()
        root = ET.fromstring(r.content)
    except Exception as e:  # noqa: BLE001 — degrade gracefully
        print(f"  [ingest] google-news '{term}' failed: {str(e)[:120]}", file=sys.stderr)
        return out
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        # Google News titles are "Headline - Source"; split off the source tail.
        src_el = item.find("source")
        source = (src_el.text or "").strip() if src_el is not None else ""
        headline = title
        if source and title.endswith(f" - {source}"):
            headline = title[: -(len(source) + 3)].strip()
        elif " - " in title:
            headline, source = title.rsplit(" - ", 1)
        desc = (item.findtext("description") or "").strip()
        out.append({
            "title": headline, "url": link, "source": source,
            "published": pub, "snippet": _strip_html(desc),
            "query": term, "origin": "google_news",
        })
        if len(out) >= limit:
            break
    return out


def fetch_gdelt(term: str, since_days: int, limit: int, max_retries: int = 3) -> List[Dict]:
    """GDELT 2.0 Doc API ArtList (JSON). Keyless. Returns normalized signals.

    GDELT 429s aggressively under fan-out; throttle to a min interval and retry a
    429 with growing backoff before giving up (degrading to [])."""
    params = {
        "query": f'"{term}" sourcecountry:US', "mode": "ArtList",
        "format": "json", "maxrecords": str(min(limit, 75)),
        "timespan": f"{since_days}d", "sort": "DateDesc",
    }
    out: List[Dict] = []
    data = None
    for attempt in range(max_retries):
        try:
            _throttle("gdelt", _GDELT_MIN_INTERVAL)
            r = _http_get(_GDELT, params)
            if r.status_code == 429:
                wait = _GDELT_MIN_INTERVAL * (attempt + 2)   # 10s, 15s, 20s
                print(f"  [ingest] gdelt '{term}' 429; backing off {wait:.0f}s "
                      f"({attempt + 1}/{max_retries})", file=sys.stderr)
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()          # GDELT sometimes returns non-JSON on overload
            break
        except Exception as e:  # noqa: BLE001 — degrade gracefully
            print(f"  [ingest] gdelt '{term}' failed: {str(e)[:120]}", file=sys.stderr)
            return out
    if data is None:
        print(f"  [ingest] gdelt '{term}' gave up after {max_retries} 429s", file=sys.stderr)
        return out
    for art in data.get("articles", []):
        out.append({
            "title": (art.get("title") or "").strip(),
            "url": art.get("url", ""),
            "source": art.get("domain", ""),
            "published": art.get("seendate", ""),
            "snippet": "",
            "query": term, "origin": "gdelt",
        })
        if len(out) >= limit:
            break
    return out


def _strip_html(s: str) -> str:
    import re
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s).replace("&nbsp;", " ").strip()


def fetch_signals(terms: List[str], since_days: int = 3, limit_per: int = 40,
                  sources=("google_news", "gdelt")) -> List[Dict]:
    """Fan out over terms x sources, return the merged, URL-deduped signal list."""
    signals: List[Dict] = []
    seen_urls = set()
    for term in terms:
        batch: List[Dict] = []
        if "google_news" in sources:
            batch += fetch_google_news(term, since_days, limit_per)
        if "gdelt" in sources:
            batch += fetch_gdelt(term, since_days, limit_per)
        for s in batch:
            u = s.get("url", "")
            if u and u in seen_urls:
                continue
            seen_urls.add(u)
            signals.append(s)
    return signals


def main() -> int:
    ap = argparse.ArgumentParser(description="Pull free crime-news signals (Google News RSS + GDELT).")
    ap.add_argument("--terms", nargs="+", default=DEFAULT_TERMS)
    ap.add_argument("--since", type=int, default=3, help="look-back window in days")
    ap.add_argument("--limit", type=int, default=40, help="max signals per term per source")
    ap.add_argument("--sources", nargs="+", default=["google_news", "gdelt"],
                    choices=["google_news", "gdelt"])
    ap.add_argument("--out", default=None, help="write signals JSON here (else stdout summary)")
    a = ap.parse_args()

    sigs = fetch_signals(a.terms, a.since, a.limit, tuple(a.sources))
    print(f"[ingest] {len(sigs)} signals from {len(a.terms)} terms x {a.sources} (last {a.since}d)")
    if a.out:
        Path(a.out).write_text(json.dumps(sigs, indent=2))
        print(f"[ingest] wrote {a.out}")
    else:
        for s in sigs[:20]:
            print(f"  - [{s['origin']}] {s['title'][:90]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
