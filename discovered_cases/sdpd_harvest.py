"""
San Diego PD SB16/SB1421/AB748 harvester — a VIDEO case source for FlameOn.

Unlike MuckRock (which skews to documents), this portal publishes the RAW
per-case bundle: bodycam/incident VIDEO + 911/radio AUDIO + Documents + Photos.
Emits candidates in the same schema as muckrock_harvest.py so they flow straight
into pipeline3/4.

Structure (reverse-engineered 2026-06-27):
  Index (server-rendered, all cases on one page):
    https://www.sandiego.gov/police/data-transparency/mandated-disclosures/sb16-sb1421-ab748
  -> per-case page:
    .../mandated-disclosures/case?id=<MM-DD-YYYY location>&cat=<Category>
  -> files hosted on an S3 bucket, linked from the case page:
    https://sdpdsb1421.sandiego.gov/<Category>/<year>/<case>/<Video|Audio|Documents|Photos>/<file>

GOTCHAS (all handled here):
  * The S3 bucket (sdpdsb1421.sandiego.gov) DENIES listing — no autoindex, no
    ListObjectsV2. You can only GET a known object key. So we scrape the case
    pages for the exact file URLs; we cannot enumerate the bucket directly.
  * The whole site 403s requests without a browser User-Agent.
  * Case-page hrefs contain a spurious literal NEWLINE mid-URL (after the case
    folder, before /Audio etc.) and legitimate SPACES in path segments. Clean =
    strip newlines/tabs, then encode spaces -> %20 (leave existing %xx/+ alone).
    Get this wrong and every file 403s.

Usage:
  python discovered_cases/sdpd_harvest.py                       # crawl + rank
  python discovered_cases/sdpd_harvest.py --download            # also fetch media
  python discovered_cases/sdpd_harvest.py --max-cases 5         # quick test
  python discovered_cases/sdpd_harvest.py --category "Officer Involved Shootings"
"""
from __future__ import annotations

import argparse
import html as htmlmod
import json
import re
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# Reuse the shared classifiers/scorers so SD candidates match MuckRock's schema.
sys.path.insert(0, str(Path(__file__).parent))
from muckrock_harvest import (  # noqa: E402
    score_text, detect_artifacts, slugify,
    VIDEO_EXTS, AUDIO_EXTS, DOC_EXTS, ext_of,
)

ROOT = Path(__file__).parent
BASE = "https://www.sandiego.gov"
INDEX_URL = f"{BASE}/police/data-transparency/mandated-disclosures/sb16-sb1421-ab748"
CASE_PATH = "/police/data-transparency/mandated-disclosures/case"
FILE_HOST = "sdpdsb1421.sandiego.gov"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
RATE_LIMIT_SEC = 0.4
TIMEOUT = 45

PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff", ".heic"}


class SDPD:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": UA, "Accept": "text/html"})
        self._last = 0.0

    def get(self, url: str, params: Optional[dict] = None) -> Optional[str]:
        wait = RATE_LIMIT_SEC - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)
        try:
            r = self.s.get(url, params=params, timeout=TIMEOUT)
            self._last = time.monotonic()
            r.raise_for_status()
            return r.text
        except Exception as e:  # noqa: BLE001
            print(f"  [warn] GET {url} {params or ''} failed: {e}", file=sys.stderr)
            return None

    def list_cases(self) -> List[Dict[str, str]]:
        """Parse the index into [{id, category, url}] case descriptors."""
        html = self.get(INDEX_URL)
        if not html:
            return []
        cases: Dict[tuple, Dict[str, str]] = {}
        for href in re.findall(r'href="(' + re.escape(CASE_PATH) + r'\?[^"]+)"', html):
            q = urllib.parse.urlparse(htmlmod.unescape(href)).query
            params = urllib.parse.parse_qs(q)
            cid = (params.get("id") or [""])[0].strip()
            cat = (params.get("cat") or [""])[0].strip()
            if not cid:
                continue
            key = (cid, cat)
            if key not in cases:
                cases[key] = {"id": cid, "category": cat,
                              "url": f"{BASE}{CASE_PATH}?id={urllib.parse.quote(cid)}"
                                     f"&cat={urllib.parse.quote(cat)}"}
        return list(cases.values())

    def files_for_case(self, case: Dict[str, str]) -> List[Dict[str, str]]:
        """Scrape a case page for its S3 file URLs (cleaned + classified)."""
        html = self.get(f"{BASE}{CASE_PATH}",
                         params={"id": case["id"], "cat": case["category"]})
        if not html:
            return []
        raw = re.findall(r'href="(https://' + re.escape(FILE_HOST) + r'/[^"]+)"',
                         html, re.DOTALL)
        files: Dict[str, Dict[str, str]] = {}
        for u in raw:
            url = clean_file_url(u)
            if url in files:
                continue
            files[url] = {"url": url, "name": file_name_of(url),
                          "ext": ext_of(url), "kind": kind_of(url)}
        return list(files.values())


def clean_file_url(u: str) -> str:
    """Strip the spurious mid-URL newline; encode legitimate spaces. Idempotent."""
    u = htmlmod.unescape(u)
    u = re.sub(r"[\n\r\t]+", "", u)   # remove ONLY the formatting whitespace
    u = u.replace(" ", "%20")         # encode real path spaces (keep %xx and +)
    return u


def file_name_of(url: str) -> str:
    tail = url.rsplit("/", 1)[-1]
    return urllib.parse.unquote(tail)


def kind_of(url: str) -> str:
    """Classify by the portal's folder convention, falling back to extension."""
    low = url.lower()
    if "/video/" in low:
        return "video"
    if "/audio/" in low:
        return "audio"
    if "/photos/" in low or "/photo/" in low:
        return "photo"
    if "/documents/" in low or "/docs/" in low:
        return "doc"
    e = ext_of(url)
    if e in VIDEO_EXTS:
        return "video"
    if e in AUDIO_EXTS:
        return "audio"
    if e in PHOTO_EXTS:
        return "photo"
    if e in DOC_EXTS:
        return "doc"
    return "other"


def make_candidate(case: Dict[str, str], files: List[Dict[str, str]]) -> Dict[str, Any]:
    buckets: Dict[str, List[Dict[str, str]]] = {"video": [], "audio": [], "doc": [], "photo": [], "other": []}
    for f in files:
        buckets[f["kind"]].append(f)

    title = f"{case['id']} ({case['category']})"
    blob = f"{title} " + " ".join(f["name"] for f in files)
    keyword_score, hits = score_text(blob)
    art = detect_artifacts(blob)

    n_vid, n_aud, n_doc = len(buckets["video"]), len(buckets["audio"]), len(buckets["doc"])
    # SD cases are inherently OIS/UoF incidents — treat them as BWC-bearing.
    is_bwc = art["bwc"] or bool(n_vid)
    is_interr = art["interrogation"]
    is_sb16 = True  # the whole portal IS the SB16/1421/AB748 release

    artifact_bonus = (6 if n_vid else 0) + (5 if n_aud else 0) + (5 if is_sb16 and n_doc else 0)
    kinds = sum([bool(n_vid), bool(n_aud), bool(n_doc), bool(is_interr)])
    bundle_bonus = {0: 0, 1: 0, 2: 3, 3: 7, 4: 12}[min(kinds, 4)]

    return {
        "source": "sdpd",
        "case_id": f"sdpd_{slugify(case['id'], 50)}",
        "title": title,
        "agency": "San Diego Police Department",
        "category": case["category"],
        "case_url": case["url"],
        "n_docs": n_doc, "n_video": n_vid, "n_audio": n_aud, "n_photos": len(buckets["photo"]),
        "has_video": bool(n_vid), "has_mp3": bool(n_aud), "has_pdf": bool(n_doc),
        "is_bwc": is_bwc, "is_interrogation": is_interr, "is_sb16": is_sb16,
        "artifact_kinds": kinds,
        "pivot": {"agency": "San Diego Police Department", "case_title": case["id"],
                  "category": case["category"]},
        "titles": [f["name"] for f in files][:25],
        "media_files": buckets["video"] + buckets["audio"],
        "doc_files": buckets["doc"][:25],
        "urls": [f["url"] for f in buckets["video"] + buckets["audio"]],
        "keyword_score": keyword_score,
        "artifact_bonus": artifact_bonus,
        "bundle_bonus": bundle_bonus,
        "score": keyword_score + artifact_bonus + bundle_bonus,
        "hits": hits,
    }


def download_media(candidates: List[dict], cache_dir: Path, sess: requests.Session) -> dict:
    cache_dir.mkdir(parents=True, exist_ok=True)
    manifest: Dict[str, List] = {}
    for c in candidates:
        entries = []
        case_dir = cache_dir / c["case_id"]
        case_dir.mkdir(exist_ok=True)
        for i, f in enumerate(c["media_files"]):
            dest = case_dir / f["name"]
            if dest.exists() and dest.stat().st_size > 0:
                entries.append([f["name"], str(dest), "cached"]); continue
            try:
                print(f"  [dl] {c['case_id']} <- {f['name'][:60]}")
                with sess.get(f["url"], stream=True, timeout=600) as r:
                    r.raise_for_status()
                    with open(dest, "wb") as fh:
                        for chunk in r.iter_content(1 << 20):
                            fh.write(chunk)
                entries.append([f["name"], str(dest), "downloaded"])
            except Exception as e:  # noqa: BLE001
                print(f"  [dl-fail] {f['url'][:80]}: {e}", file=sys.stderr)
                entries.append([f["name"], f["url"], f"failed: {e}"])
        manifest[c["case_id"]] = entries
    return manifest


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--category", default=None, help="only this category (e.g. 'Officer Involved Shootings')")
    ap.add_argument("--max-cases", type=int, default=0, help="cap cases crawled (0=all)")
    ap.add_argument("--require-video", action="store_true", help="keep only cases with video")
    ap.add_argument("--download", action="store_true", help="download media into the cache dir")
    ap.add_argument("--cache-dir", default=str(ROOT.parent / "pipeline3_audio" / "foia_cache"))
    ap.add_argument("--out", default=str(ROOT / "sdpd_candidates.json"))
    args = ap.parse_args()

    sd = SDPD()
    cases = sd.list_cases()
    if args.category:
        cases = [c for c in cases if c["category"].lower() == args.category.lower()]
    if args.max_cases:
        cases = cases[:args.max_cases]
    print(f"[index] {len(cases)} case(s) to crawl")

    candidates: List[dict] = []
    stats = {"crawled": 0, "no_files": 0, "kept": 0}
    for case in cases:
        stats["crawled"] += 1
        files = sd.files_for_case(case)
        if not files:
            stats["no_files"] += 1
            continue
        cand = make_candidate(case, files)
        if args.require_video and not cand["has_video"]:
            continue
        candidates.append(cand)
        stats["kept"] += 1
        tags = "".join(t for t, on in (("V", cand["has_video"]), ("A", cand["has_mp3"]),
                       ("D", cand["has_pdf"])) if on)
        print(f"  [keep {cand['score']:>3} {tags:<3}] {cand['case_id']}  "
              f"(vid={cand['n_video']} aud={cand['n_audio']} doc={cand['n_docs']}) {case['category']}")

    candidates.sort(key=lambda c: c["score"], reverse=True)
    Path(args.out).write_text(json.dumps(candidates, indent=2, ensure_ascii=False))

    vids = sum(c["n_video"] for c in candidates)
    print("\n" + "=" * 72)
    print(f"crawled={stats['crawled']} no-files={stats['no_files']} kept={stats['kept']} "
          f"| total video files={vids}")
    print(f"wrote {len(candidates)} candidates -> {args.out}")
    print("=" * 72)
    for i, c in enumerate(candidates[:15], 1):
        print(f"{i:2}. [{c['score']:3}] {c['case_id']}  vid={c['n_video']} aud={c['n_audio']} doc={c['n_docs']}")

    if args.download and candidates:
        print("\n[download] fetching media ...")
        man = download_media(candidates, Path(args.cache_dir), sd.s)
        mp = ROOT / "sdpd_download_manifest.json"
        mp.write_text(json.dumps(man, indent=2, ensure_ascii=False))
        print(f"[download] manifest -> {mp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
