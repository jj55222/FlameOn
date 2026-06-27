"""
Chicago COPA case portal harvester — oversight/legal-report bundle source.

COPA's case portal is a WordPress custom post type with ~3k case records and a
large attachment library. It is strongest as a SB1421/SB16-equivalent legal
record source: Final Summary Reports, nonconcurrence/concurrence letters,
request-for-review materials, and other oversight PDFs. COPA's Video Release
Policy says media and police reports are posted for VRP cases, but direct raw
video/audio is not consistently exposed as WordPress media. This harvester
therefore catalogs every attachment it can see and flags raw media only when it
is directly available.

Usage:
  python discovered_cases/copa_harvest.py --max-media-pages 20
  python discovered_cases/copa_harvest.py --max-pages 31 --max-media-pages 112
"""
from __future__ import annotations

import argparse
import html
import json
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

sys.path.insert(0, str(Path(__file__).parent))
from muckrock_harvest import (  # noqa: E402
    AUDIO_EXTS,
    DOC_EXTS,
    VIDEO_EXTS,
    detect_artifacts,
    ext_of,
    score_text,
    slugify,
)


ROOT = Path(__file__).parent
API = "https://www.chicagocopa.org/wp-json/wp/v2"
PORTAL = "https://www.chicagocopa.org/data-cases/case-portal/"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff"}


class COPAClient:
    def __init__(self, timeout: float = 45.0):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": UA, "Accept": "application/json"})
        self.timeout = timeout

    def get_json(self, path: str, params: Optional[dict] = None) -> tuple[Any, Dict[str, str]]:
        url = path if path.startswith("http") else f"{API}/{path.lstrip('/')}"
        r = self.s.get(url, params=params or {}, timeout=self.timeout)
        r.raise_for_status()
        return r.json(), dict(r.headers)


def header_int(headers: Dict[str, str], name: str, default: int) -> int:
    """Read a response header case-insensitively."""
    lname = name.lower()
    for k, v in headers.items():
        if k.lower() == lname:
            try:
                return int(v)
            except (TypeError, ValueError):
                return default
    return default


def strip_html(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def fetch_cases(client: COPAClient, max_pages: Optional[int] = None) -> Dict[int, Dict[str, Any]]:
    cases: Dict[int, Dict[str, Any]] = {}
    page = 1
    total_pages = None
    while True:
        data, headers = client.get_json(
            "case",
            {"per_page": 100, "page": page},
        )
        if total_pages is None:
            total_pages = header_int(headers, "X-WP-TotalPages", page)
        for c in data:
            title = strip_html((c.get("title") or {}).get("rendered", "")) or c.get("slug", "")
            cases[int(c["id"])] = {
                "wp_id": int(c["id"]),
                "case_number": title,
                "slug": c.get("slug", ""),
                "case_url": c.get("link", ""),
                "published_at": c.get("date", ""),
                "modified_at": c.get("modified", ""),
            }
        print(f"[cases] page {page}/{total_pages} ({len(cases)} cases)", flush=True)
        page += 1
        if page > total_pages:
            break
        if max_pages and page > max_pages:
            break
        time.sleep(0.1)
    return cases


def fetch_media(
    client: COPAClient,
    max_pages: Optional[int] = None,
    *,
    allow_partial: bool = False,
    retries: int = 0,
) -> List[Dict[str, Any]]:
    media: List[Dict[str, Any]] = []
    page = 1
    total_pages = None
    while True:
        data = []
        headers: Dict[str, str] = {}
        for attempt in range(retries + 1):
            try:
                data, headers = client.get_json(
                    "media",
                    {
                        "per_page": 100,
                        "page": page,
                    },
                )
                break
            except requests.RequestException as exc:
                if attempt < retries:
                    print(f"[media] page {page} failed ({exc}); retrying", file=sys.stderr, flush=True)
                    time.sleep(1.5)
                    continue
                if allow_partial:
                    print(f"[media] page {page} failed ({exc}); skipping", file=sys.stderr, flush=True)
                    if total_pages is None:
                        total_pages = max_pages or page
                    break
                raise
        if total_pages is None:
            total_pages = header_int(headers, "X-WP-TotalPages", page)
        for m in data:
            source_url = m.get("source_url") or ""
            title = strip_html((m.get("title") or {}).get("rendered", "")) or source_url.rsplit("/", 1)[-1]
            parent = m.get("post")
            if not parent:
                continue
            media.append(
                {
                    "wp_id": int(m["id"]),
                    "parent_id": int(parent),
                    "title": title,
                    "url": source_url,
                    "mime_type": m.get("mime_type") or "",
                    "media_type": m.get("media_type") or "",
                    "date": m.get("date", ""),
                    "modified": m.get("modified", ""),
                    "ext": ext_of(source_url or title),
                }
            )
        print(f"[media] page {page}/{total_pages} ({len(media)} attached files)", flush=True)
        page += 1
        if page > total_pages:
            break
        if max_pages and page > max_pages:
            break
        time.sleep(0.1)
    return media


def kind_of(f: Dict[str, Any]) -> str:
    mime = (f.get("mime_type") or "").lower()
    ext = f.get("ext") or ext_of(f.get("url") or f.get("title", ""))
    if mime.startswith("video/") or ext in VIDEO_EXTS:
        return "video"
    if mime.startswith("audio/") or ext in AUDIO_EXTS:
        return "audio"
    if mime == "application/pdf" or ext in DOC_EXTS:
        return "doc"
    if mime.startswith("image/") or ext in PHOTO_EXTS:
        return "photo"
    return "other"


def evidence_type(f: Dict[str, Any]) -> str:
    low = f.get("title", "").lower()
    mime = (f.get("mime_type") or "").lower()
    if mime.startswith("video/") or any(k in low for k in ("video", "body worn", "bwc", "dash", "cctv")):
        return "bodycam"
    if mime.startswith("audio/") or any(k in low for k in ("audio", "911", "radio", "interview")):
        return "interrogation" if "interview" in low else "911_audio"
    if any(k in low for k in ("final summary", "fsr", "report", "nonconcur", "concur", "request for review", "opinion")):
        return "documents"
    return "other"


def file_record(f: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": f.get("title", ""),
        "url": f.get("url", ""),
        "ext": f.get("ext") or ext_of(f.get("url") or f.get("title", "")),
        "mime_type": f.get("mime_type", ""),
        "evidence_type": evidence_type(f),
        "wp_id": f.get("wp_id"),
        "date": f.get("date", ""),
    }


def make_candidate(case: Dict[str, Any], files: List[Dict[str, Any]]) -> Dict[str, Any]:
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for f in files:
        buckets[kind_of(f)].append(file_record(f))

    title = case.get("case_number") or case.get("slug", "")
    titles = [f.get("title", "") for f in files]
    blob = f"{title} " + " ".join(titles)
    keyword_score, hits = score_text(blob)
    art = detect_artifacts(blob)

    n_video, n_audio, n_doc, n_photo = (
        len(buckets["video"]),
        len(buckets["audio"]),
        len(buckets["doc"]),
        len(buckets["photo"]),
    )
    has_interrogation = art["interrogation"] or any(
        f.get("evidence_type") == "interrogation" for f in buckets["audio"]
    )
    has_bwc = art["bwc"] or bool(n_video)
    has_pdf = bool(n_doc)

    artifact_bonus = (
        (6 if n_video else 0)
        + (5 if has_interrogation else 0)
        + (4 if n_audio and not has_interrogation else 0)
        + (5 if has_pdf else 0)
        + (1 if n_photo else 0)
    )
    kinds = sum([bool(n_video), bool(n_audio), bool(n_doc), bool(n_photo), bool(has_interrogation)])
    bundle_bonus = {0: 0, 1: 0, 2: 3, 3: 7, 4: 12, 5: 14}[min(kinds, 5)]

    case_id = f"copa_{slugify(title, 50)}"
    return {
        "source": "chicago_copa",
        "case_id": case_id,
        "wp_id": case.get("wp_id"),
        "title": f"Chicago COPA {title}",
        "case_number": title,
        "agency": "Chicago Civilian Office of Police Accountability",
        "case_url": case.get("case_url", ""),
        "portal_url": PORTAL,
        "published_at": case.get("published_at", ""),
        "modified_at": case.get("modified_at", ""),
        "n_docs": n_doc,
        "n_video": n_video,
        "n_audio": n_audio,
        "n_photos": n_photo,
        "n_other": len(buckets["other"]),
        "has_video": bool(n_video),
        "has_mp3": bool(n_audio),
        "has_pdf": has_pdf,
        "is_bwc": has_bwc,
        "is_interrogation": has_interrogation,
        "is_sb16": has_pdf,
        "artifact_kinds": kinds,
        "pivot": {
            "agency": "Chicago Civilian Office of Police Accountability",
            "case_title": title,
            "case_number": title,
        },
        "titles": titles[:50],
        "media_files": buckets["video"] + buckets["audio"],
        "doc_files": buckets["doc"],
        "photo_files": buckets["photo"],
        "other_files": buckets["other"],
        "urls": [f["url"] for f in buckets["video"] + buckets["audio"]],
        "keyword_score": keyword_score,
        "artifact_bonus": artifact_bonus,
        "bundle_bonus": bundle_bonus,
        "score": keyword_score + artifact_bonus + bundle_bonus,
        "hits": hits,
    }


def build_candidates(cases: Dict[int, Dict[str, Any]], media: Iterable[Dict[str, Any]], min_score: int = 0) -> List[Dict[str, Any]]:
    by_parent: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for f in media:
        if f.get("parent_id") in cases:
            by_parent[f["parent_id"]].append(f)
    candidates = [make_candidate(cases[parent], files) for parent, files in by_parent.items()]
    candidates = [c for c in candidates if c["score"] >= min_score]
    candidates.sort(
        key=lambda c: (
            -c["score"],
            -c["n_video"],
            -c["n_audio"],
            -c["n_docs"],
            c["case_number"],
        )
    )
    return candidates


def write_catalog(candidates: List[Dict[str, Any]], out_path: Path, data_path: Path, *, cases_seen: int, media_seen: int) -> None:
    total_video = sum(c["n_video"] for c in candidates)
    total_audio = sum(c["n_audio"] for c in candidates)
    total_docs = sum(c["n_docs"] for c in candidates)
    total_photos = sum(c["n_photos"] for c in candidates)
    rows = [
        "# Chicago COPA — Case Portal Legal / Media Attachment Catalog",
        "",
        "**Source:** Civilian Office of Police Accountability case portal",
        f"**Front-end index:** {PORTAL}",
        f"**WordPress API:** `{API}/case` and `{API}/media`",
        f"**Candidate data:** `{data_path.as_posix()}`",
        "",
        "> COPA is strongest as a legal/accountability report source. It has a Video Release",
        "> Policy, but this catalog only counts raw video/audio when those files are directly",
        "> exposed as WordPress media attachments. Use the case links for manual review of",
        "> any VRP context that is not exposed as a direct attachment.",
        "",
        "## Totals In This Crawl",
        "",
        f"- **{cases_seen} case records checked**, **{media_seen} attached files checked**",
        f"- **{len(candidates)} cases** with attached files",
        f"- **{total_video} video files**, **{total_audio} audio files**, **{total_docs} documents**, **{total_photos} photos/images**",
        "",
        "## All cases with attachments (ranked by artifact richness)",
        "",
        "| # | Score | Case | Vid | Aud | Doc | Img | Highlights | Browse |",
        "|---|------:|------|----:|----:|----:|----:|------------|--------|",
    ]
    for i, c in enumerate(candidates, 1):
        highlights = ", ".join(c.get("hits", [])[:4]).replace("|", "/")
        rows.append(
            f"| {i} | {c['score']} | {c['case_number']} | {c['n_video']} | {c['n_audio']} | "
            f"{c['n_docs']} | {c['n_photos']} | {highlights} | [link]({c['case_url']}) |"
        )

    rows.extend(["", "## Top bundles", ""])
    for c in candidates[:12]:
        rows.append(
            f"- **{c['case_number']}** — {c['n_video']} video, {c['n_audio']} audio, "
            f"{c['n_docs']} docs, {c['n_photos']} images, score {c['score']} · case_id `{c['case_id']}`"
        )
        for f in (c["media_files"] + c["doc_files"] + c["photo_files"])[:6]:
            rows.append(f"  - {f['name']}")
    rows.append("")
    out_path.write_text("\n".join(rows), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--max-pages", type=int, default=None, help="max case API pages (100 cases/page); default all")
    ap.add_argument("--max-media-pages", type=int, default=20, help="max media API pages (100 files/page); default 20 for a quick crawl")
    ap.add_argument("--min-score", type=int, default=0)
    ap.add_argument("--timeout", type=float, default=45.0, help="per-request timeout in seconds")
    ap.add_argument("--retries", type=int, default=0, help="retry failed media pages this many times")
    ap.add_argument("--allow-partial", action="store_true", help="skip media pages that still fail after retries")
    ap.add_argument("--out", default=str(ROOT / "copa_candidates.json"))
    ap.add_argument("--catalog", default=str(ROOT / "COPA_CASE_CATALOG.md"))
    args = ap.parse_args()

    client = COPAClient(timeout=args.timeout)
    cases = fetch_cases(client, args.max_pages)
    media = fetch_media(
        client,
        args.max_media_pages,
        allow_partial=args.allow_partial,
        retries=args.retries,
    )
    candidates = build_candidates(cases, media, args.min_score)

    out = Path(args.out)
    out.write_text(json.dumps(candidates, indent=2, ensure_ascii=False), encoding="utf-8")
    write_catalog(candidates, Path(args.catalog), out, cases_seen=len(cases), media_seen=len(media))

    print(f"cases: {len(cases)}")
    print(f"attached files: {len(media)}")
    print(f"candidates: {len(candidates)} -> {out}")
    print(f"catalog: {args.catalog}")
    for c in candidates[:15]:
        print(
            f"  [{c['score']:2}] {c['case_id']} "
            f"V={c['n_video']} A={c['n_audio']} D={c['n_docs']} I={c['n_photos']} "
            f"{c['case_number']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
