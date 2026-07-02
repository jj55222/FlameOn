"""
Sacramento County Sheriff AB748/SB1421 released-cases harvester (Dropbox bundles).

Each released critical-incident case (OIS / UOF) is a Dropbox shared folder holding
the FULL evidence bundle — body-worn (BWC) + in-car (ICC) VIDEO, 911/radio AUDIO,
and IA/UOF REPORTS — i.e. the EWU full package. This scrapes
https://www.sacsheriff.com/pages/released_cases.php, recursively lists each case's
Dropbox folder via the shared-link listing endpoint, and emits one candidate per
case (direct dl=1 download URLs) in the shared registry schema so they flow into
``case_bundle_agg.py`` (and downstream pipeline3/4).

GOTCHAS (all handled here):
  * Dropbox folder contents load via an XHR POST to
    ``/list_shared_link_folder_entries`` with a CSRF token read from the ``t``
    cookie — GET the folder page first to mint the cookie.
  * Two link formats: new ``/scl/fo/<key>/<hash>?rlkey=<k>`` and old
    ``/sh/<key>/<hash>``.
  * Subfolders (Audio/, Video/, Reports/) must recurse: each dir entry's ``href``
    carries a NEW secure_hash plus a trailing sub_path — list THAT href, not the
    parent hash with a sub_path.
  * Per-file download URL = the entry ``href`` with ``dl=1`` (302 → CDN, and it is
    range-probeable, so ``validate_ab.py`` sees it live).
  * The released_cases.php anchor glues a ``title=`` attribute onto the URL — strip it.

RESUMABLE: a per-case ``_checked`` flag + incremental save; re-runs skip finished
cases. GENTLE by construction: sequential, rate-limited, with 429/5xx backoff
(Dropbox throttles aggressive listing).

Usage:
  python discovered_cases/sacso_harvest.py --max-cases 5      # quick test
  python discovered_cases/sacso_harvest.py                    # full crawl (resumable)
  python discovered_cases/sacso_harvest.py --refresh-index    # re-scrape the case list
"""
from __future__ import annotations

import argparse
import html as htmlmod
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

import requests

sys.path.insert(0, str(Path(__file__).parent))
from muckrock_harvest import AUDIO_EXTS, DOC_EXTS, VIDEO_EXTS, ext_of, score_text, slugify  # noqa: E402

ROOT = Path(__file__).parent
INDEX_URL = "https://www.sacsheriff.com/pages/released_cases.php"
LIST_ENDPOINT = "https://www.dropbox.com/list_shared_link_folder_entries"
OUT = ROOT / "sacso_candidates.json"
INDEX_CACHE = ROOT.parent / ".tmp" / "sacso" / "case_index.json"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff", ".heic", ".bmp"}
EXTRA_VIDEO_EXTS = {".wmv", ".asf", ".3gp", ".mts", ".m2ts"}
RATE_LIMIT_SEC = 0.6
MAX_DEPTH = 6


class Dropbox:
    """Minimal authenticated-session lister for Dropbox public shared folders."""

    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": UA})
        self._last = 0.0

    def _wait(self):
        gap = RATE_LIMIT_SEC - (time.monotonic() - self._last)
        if gap > 0:
            time.sleep(gap)
        self._last = time.monotonic()

    @staticmethod
    def parse_link(url: str) -> Optional[Tuple[str, str, str, str, str]]:
        """-> (link_type, link_key, secure_hash, rlkey, sub_path) or None."""
        p = urlparse(url)
        q = parse_qs(p.query)
        rlkey = (q.get("rlkey") or [""])[0]
        m = re.match(r"/scl/fo/([^/]+)/([^/]+)(/.*)?$", p.path)
        if m:
            return "s", m.group(1), m.group(2), rlkey, unquote(m.group(3) or "")
        m = re.match(r"/sh/([^/]+)/([^/]+)(/.*)?$", p.path)
        if m:
            return "s", m.group(1), m.group(2), rlkey, unquote(m.group(3) or "")
        return None

    def _mint_token(self, referer: str) -> Optional[str]:
        self._wait()
        self.s.get(referer, timeout=30)
        return self.s.cookies.get("t")

    def list_folder(self, url: str, *, retries: int = 4) -> List[Dict[str, Any]]:
        """List one shared-folder URL (single level). Follows Dropbox pagination."""
        parsed = self.parse_link(url)
        if not parsed:
            return []
        link_type, link_key, secure_hash, rlkey, sub_path = parsed
        token = self._mint_token(url)
        entries: List[Dict[str, Any]] = []
        voucher = None
        for _ in range(50):                      # pagination guard
            data = {"is_xhr": "true", "t": token, "link_key": link_key,
                    "link_type": link_type, "secure_hash": secure_hash,
                    "sub_path": sub_path}
            if rlkey:                    # old /sh/ links carry no rlkey; sending "" -> HTTP 500
                data["rlkey"] = rlkey
            if voucher:
                data["voucher"] = voucher
            for attempt in range(retries):
                self._wait()
                try:
                    r = self.s.post(LIST_ENDPOINT, data=data, timeout=40, headers={
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                        "Referer": url, "X-Requested-With": "XMLHttpRequest"})
                except requests.RequestException as e:
                    if attempt < retries - 1:
                        time.sleep(2 * (attempt + 1)); continue
                    print(f"    [dropbox] network fail {type(e).__name__}", file=sys.stderr)
                    return entries
                if r.status_code in (429, 500, 502, 503) and attempt < retries - 1:
                    time.sleep(3 * (attempt + 1)); continue
                if "json" not in r.headers.get("Content-Type", ""):
                    if attempt < retries - 1:
                        token = self._mint_token(url); time.sleep(1.5 * (attempt + 1)); continue
                    return entries
                break
            try:
                payload = r.json()
            except ValueError:
                return entries
            entries.extend(payload.get("entries") or [])
            if not payload.get("has_more_entries"):
                break
            voucher = payload.get("next_request_voucher")
            if not voucher:
                break
        return entries

    def walk(self, url: str, depth: int = 0) -> List[Dict[str, Any]]:
        """Recursively collect leaf file entries from a folder and its subfolders."""
        out: List[Dict[str, Any]] = []
        for e in self.list_folder(url):
            if e.get("is_dir"):
                if depth < MAX_DEPTH and e.get("href"):
                    out.extend(self.walk(e["href"], depth + 1))
            elif e.get("href"):
                out.append(e)
        return out


def download_url(href: str) -> str:
    if "dl=0" in href:
        return href.replace("dl=0", "dl=1")
    return href + ("&dl=1" if "?" in href else "?dl=1")


def classify(name: str) -> Tuple[str, str]:
    """-> (bucket, evidence_type) from filename."""
    e = ext_of(name)
    low = name.lower()
    if e in VIDEO_EXTS or e in EXTRA_VIDEO_EXTS:
        if any(k in low for k in ("bwc", "body worn", "body-worn", "bodycam")):
            return "media", "bodycam"
        if any(k in low for k in ("icc", "in car", "in-car", "dash")):
            return "media", "dashcam"
        if any(k in low for k in ("surv", "cctv", "security")):
            return "media", "surveillance"
        return "media", "video"
    if e in AUDIO_EXTS:
        if "911" in low:
            return "media", "911_audio"
        if "radio" in low or "dispatch" in low:
            return "media", "radio"
        return "media", "audio"
    if e in DOC_EXTS:
        return "doc", "documents"
    if e in PHOTO_EXTS:
        return "photo", "photos"
    return "other", "other"


def make_candidate(case: Dict[str, str], files: List[Dict[str, Any]]) -> Dict[str, Any]:
    media, docs, photos, others = [], [], [], []
    for f in files:
        name = f.get("filename") or ""
        href = f.get("href") or ""
        bucket, etype = classify(name)
        rec = {"name": name, "url": download_url(href), "browse_url": href,
               "ext": ext_of(name), "evidence_type": etype,
               "size_mb": round((f.get("bytes") or 0) / 1e6, 1)}
        {"media": media, "doc": docs, "photo": photos, "other": others}[bucket].append(rec)

    n_video = sum(1 for f in media if f["evidence_type"] in ("bodycam", "dashcam", "surveillance", "video"))
    n_audio = len(media) - n_video
    n_doc, n_photo = len(docs), len(photos)
    has_v, has_a, has_d = n_video > 0, n_audio > 0, n_doc > 0
    # Tier by the generic vet_bundles.tier_for rule for an unknown source.
    tier = "B" if (has_v and has_a and has_d) else ("C" if (has_v or has_a) else "D")

    report = case["report"]
    title = f"{report} {case.get('type','')} — {case.get('location','')}".strip(" —")
    blob = title + " " + " ".join(f.get("filename", "") for f in files)
    keyword_score, hits = score_text(blob)
    artifact_bonus = (6 if has_v else 0) + (5 if has_a else 0) + (5 if has_d else 0)

    return {
        "source": "sacso",
        "case_id": f"sacso_{slugify(report, 30)}",
        "agency": "Sacramento County Sheriff's Office",
        "title": title,
        "case_url": case["url"],
        "incident_date": case.get("date", ""),
        "incident_type": case.get("type", ""),
        "location": case.get("location", ""),
        "n_video": n_video, "n_audio": n_audio, "n_docs": n_doc, "n_photos": n_photo,
        "n_other": len(others),
        "has_video": has_v, "has_mp3": has_a, "has_pdf": has_d,
        "is_bwc": any(f["evidence_type"] == "bodycam" for f in media),
        "is_interrogation": False,
        "tier": tier,
        "downloadable": None,
        "artifact_kinds": sum([has_v, has_a, has_d]),
        "pivot": {"agency": "Sacramento County Sheriff's Office", "case_title": report,
                  "incident_type": case.get("type", ""), "location": case.get("location", "")},
        "titles": [f.get("filename", "") for f in files][:40],
        "media_files": media,
        "doc_files": docs,
        "photo_files": photos,
        "other_files": others,
        "urls": [f["url"] for f in media],
        "keyword_score": keyword_score,
        "artifact_bonus": artifact_bonus,
        "score": keyword_score + artifact_bonus,
        "hits": hits,
    }


def scrape_case_index(sess: requests.Session) -> List[Dict[str, str]]:
    html = sess.get(INDEX_URL, timeout=45).text
    cases: List[Dict[str, str]] = []
    seen = set()
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I):
        if "dropbox.com" not in tr:
            continue
        m = re.search(r"https://www\.dropbox\.com/(?:scl/fo|sh)/[^\s\"'<>]+", tr)
        if not m:
            continue
        url = htmlmod.unescape(m.group(0)).split("title=")[0].rstrip("&")
        cells = [re.sub(r"<[^>]+>", " ", c) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S | re.I)]
        cells = [re.sub(r"\s+", " ", htmlmod.unescape(c)).strip() for c in cells]
        report = next((c for c in reversed(cells) if re.search(r"\d", c)), "") or f"case{len(cases)}"
        if report in seen:
            continue
        seen.add(report)
        cases.append({
            "report": report,
            "date": cells[0] if cells else "",
            "location": cells[1] if len(cells) > 1 else "",
            "type": cells[2] if len(cells) > 2 else "",
            "url": url,
        })
    return cases


def load_out() -> Dict[str, Dict[str, Any]]:
    if not OUT.exists():
        return {}
    try:
        data = json.loads(OUT.read_text(encoding="utf-8"))
    except ValueError:
        return {}
    return {c["case_id"]: c for c in data if isinstance(c, dict) and c.get("case_id")}


def save_out(by_id: Dict[str, Dict[str, Any]]):
    ordered = sorted(by_id.values(), key=lambda c: (-(c.get("score") or 0), c["case_id"]))
    OUT.write_text(json.dumps(ordered, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--max-cases", type=int, default=0, help="cap cases crawled this run (0 = all)")
    ap.add_argument("--refresh-index", action="store_true", help="re-scrape released_cases.php")
    ap.add_argument("--retry-errors", action="store_true", help="re-attempt cases that errored before")
    ap.add_argument("--save-every", type=int, default=5)
    args = ap.parse_args()

    db = Dropbox()
    if args.refresh_index or not INDEX_CACHE.exists():
        cases = scrape_case_index(db.s)
        INDEX_CACHE.parent.mkdir(parents=True, exist_ok=True)
        INDEX_CACHE.write_text(json.dumps(cases, indent=2, ensure_ascii=False), encoding="utf-8")
    else:
        cases = json.loads(INDEX_CACHE.read_text(encoding="utf-8"))
    print(f"[index] {len(cases)} released cases with Dropbox folders")

    by_id = load_out()
    crawled = 0
    stats = {"kept": 0, "empty": 0, "error": 0, "skip": 0}
    for case in cases:
        cid = f"sacso_{slugify(case['report'], 30)}"
        prev = by_id.get(cid)
        if prev and prev.get("_checked") and not (prev.get("_error") and args.retry_errors):
            stats["skip"] += 1
            continue
        if args.max_cases and crawled >= args.max_cases:
            break
        crawled += 1
        try:
            files = db.walk(case["url"])
        except Exception as e:  # noqa: BLE001
            print(f"  [err] {cid}: {type(e).__name__}: {e}", file=sys.stderr)
            by_id[cid] = {"source": "sacso", "case_id": cid, "case_url": case["url"],
                          "_checked": True, "_error": str(e)[:200], "media_files": [], "n_files": 0}
            stats["error"] += 1
            continue
        if not files:
            by_id[cid] = {"source": "sacso", "case_id": cid, "case_url": case["url"],
                          "_checked": True, "_error": "no files listed", "media_files": []}
            stats["empty"] += 1
        else:
            cand = make_candidate(case, files)
            cand["_checked"] = True
            by_id[cid] = cand
            stats["kept"] += 1
            print(f"  [keep {cand['tier']}] {cid:<24} V{cand['n_video']} A{cand['n_audio']} "
                  f"D{cand['n_docs']} P{cand['n_photos']}  {case.get('type','')} {case.get('date','')}")
        if crawled % args.save_every == 0:
            save_out(by_id)
    save_out(by_id)

    real = [c for c in by_id.values() if c.get("media_files") or c.get("doc_files")]
    print("\n" + "=" * 60)
    print(f"crawled={crawled} kept={stats['kept']} empty={stats['empty']} "
          f"error={stats['error']} skipped(done)={stats['skip']}")
    print(f"total candidates in {OUT.name}: {len(real)} with files")
    from collections import Counter
    print("tiers:", dict(Counter(c.get("tier") for c in real)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
