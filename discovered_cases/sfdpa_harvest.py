"""
San Francisco DPA / NextRequest harvester — interview + document bundle source.

SFDPA is the best current source for interrogation/interview-style audio:
DPA/IAD/Homicide interview MP3s, occasional BWC MP4 exhibits, and production
PDFs are published through NextRequest's public documents index.

LIVE by default — it crawls the NextRequest JSON API and enumerates the WHOLE
portal (~594 documents), grouping them into per-case bundles by the NextRequest
"folder_name", classifying files, scoring the bundle, and emitting candidates in
the same broad shape as the MuckRock/SDPD harvesters so the rest of the pipeline
reuses it untouched. (An earlier version parsed a hand-captured 50-row markdown
snapshot — ~9% of the portal; --from-capture still reads that for offline repro.)

API (reverse-engineered 2026-06-27):
  GET https://sfdpa.nextrequest.com/client/documents?page_size=50&page_number=N
    -> {"total_count": 594, "documents": [ {id, title, count(downloads),
        folder_name, file_extension, document_path, request_path, pretty_id,
        created_at, doc_date, description}, ... ]}
  Download: <document_path>/download  -> 302 redirect to a signed S3 object URL
            (follow redirects; the filename rides in content-disposition).

GOTCHAS (handled here):
  * Pagination is ``page_number`` (NOT ``page`` / ``offset`` — those are silently
    IGNORED and re-serve page 1, so a naive crawler loops on the first 50 forever).
  * ``page_size`` is capped at 50 (anything larger returns an empty document list).
  * Whole site wants a browser User-Agent.

Usage:
  python discovered_cases/sfdpa_harvest.py                       # live crawl + rank
  python discovered_cases/sfdpa_harvest.py --max-docs 60         # quick live test
  python discovered_cases/sfdpa_harvest.py --download --case-id sfdpa_0386-17
  python discovered_cases/sfdpa_harvest.py --from-capture discovered_cases/sfdpa_docs.json  # offline
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
DOC_INDEX = "https://sfdpa.nextrequest.com/documents"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)


def _clean_cell(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def parse_markdown_rows(md: str) -> List[Dict[str, Any]]:
    """Parse the NextRequest markdown table captured in `sfdpa_docs.json`."""
    rows: List[Dict[str, Any]] = []
    for line in md.splitlines():
        line = line.strip()
        if not line.startswith("| ["):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) < 7:
            continue
        doc_cell, req_cell, upload_date, downloads, folder, doc_date, desc = cells[:7]
        m_doc = re.search(r"\[([^\]]+)\]\(([^)]+)\)", doc_cell)
        if not m_doc:
            continue
        m_req = re.search(r"\[([^\]]+)\]\(([^)]+)\)", req_cell)
        title, url = m_doc.group(1).strip(), m_doc.group(2).strip()
        try:
            dl_count = int(downloads.strip())
        except ValueError:
            dl_count = 0
        rows.append(
            {
                "title": _clean_cell(title),
                "url": url,
                "download_url": nextrequest_download_url(url),
                "request": _clean_cell(m_req.group(1)) if m_req else "",
                "request_url": m_req.group(2).strip() if m_req else "",
                "upload_date": upload_date.strip(),
                "downloads": dl_count,
                "folder": _clean_cell(folder),
                "document_date": _clean_cell(doc_date),
                "description": _clean_cell(desc),
                "ext": ext_of(title),
            }
        )
    return rows


def nextrequest_download_url(url: str) -> str:
    return url.rstrip("/") + "/download"


def kind_of(row: Dict[str, Any]) -> str:
    title = row.get("title", "")
    low = title.lower()
    ext = row.get("ext") or ext_of(title)
    if ext in VIDEO_EXTS or any(k in low for k in ("bwc", "body worn", "body-worn", "bodycam", "footage")):
        return "video"
    if ext in AUDIO_EXTS or any(k in low for k in ("interview", "interrogation", "iad ", "dpa ")):
        return "audio"
    if ext in DOC_EXTS:
        return "doc"
    return "other"


def evidence_type(row: Dict[str, Any]) -> str:
    low = row.get("title", "").lower()
    if any(k in low for k in ("bwc", "body worn", "body-worn", "bodycam")):
        return "bodycam"
    if any(k in low for k in ("interview", "interrogation", "iad ", "dpa ", "homicide interview")):
        return "interrogation"
    if "production" in low or low.endswith(".pdf"):
        return "documents"
    return "other"


def file_record(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": row["title"],
        "url": row["download_url"],
        "browse_url": row["url"],
        "ext": row.get("ext") or ext_of(row["title"]),
        "evidence_type": evidence_type(row),
        "downloads": row.get("downloads", 0),
        "upload_date": row.get("upload_date", ""),
        "request": row.get("request", ""),
        "request_url": row.get("request_url", ""),
        "description": row.get("description", ""),
    }


def make_candidate(folder: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        buckets[kind_of(r)].append(file_record(r))

    titles = [r["title"] for r in rows]
    blob = f"{folder} " + " ".join(titles)
    keyword_score, hits = score_text(blob)
    art = detect_artifacts(blob)

    n_video = len(buckets["video"])
    n_audio = len(buckets["audio"])
    n_doc = len(buckets["doc"])
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
    )
    kinds = sum([bool(n_video), bool(n_audio), bool(n_doc), bool(has_interrogation)])
    bundle_bonus = {0: 0, 1: 0, 2: 3, 3: 7, 4: 12}[min(kinds, 4)]
    popularity_bonus = min(sum(r.get("downloads", 0) for r in rows) // 250, 5)

    case_id = f"sfdpa_{slugify(folder, 50)}"
    return {
        "source": "sfdpa_nextrequest",
        "case_id": case_id,
        "folder": folder,
        "title": f"SF DPA folder {folder}",
        "agency": "San Francisco Department of Police Accountability",
        "case_url": rows[0].get("request_url") or DOC_INDEX,
        "document_index_url": DOC_INDEX,
        "n_docs": n_doc,
        "n_video": n_video,
        "n_audio": n_audio,
        "n_other": len(buckets["other"]),
        "has_video": bool(n_video),
        "has_mp3": bool(n_audio),
        "has_pdf": has_pdf,
        "is_bwc": has_bwc,
        "is_interrogation": has_interrogation,
        "is_sb16": has_pdf,
        "artifact_kinds": kinds,
        "pivot": {
            "agency": "San Francisco Department of Police Accountability",
            "case_title": folder,
            "folder": folder,
        },
        "titles": titles[:50],
        "media_files": buckets["video"] + buckets["audio"],
        "doc_files": buckets["doc"],
        "other_files": buckets["other"],
        "urls": [f["url"] for f in buckets["video"] + buckets["audio"]],
        "keyword_score": keyword_score,
        "artifact_bonus": artifact_bonus,
        "bundle_bonus": bundle_bonus,
        "popularity_bonus": popularity_bonus,
        "score": keyword_score + artifact_bonus + bundle_bonus + popularity_bonus,
        "hits": hits,
        "total_downloads": sum(r.get("downloads", 0) for r in rows),
    }


def load_rows(index_path: Path) -> List[Dict[str, Any]]:
    data = json.loads(index_path.read_text(encoding="utf-8"))
    md = data[DOC_INDEX]["markdown"]
    return parse_markdown_rows(md)


def build_candidates(rows: Iterable[Dict[str, Any]], min_score: int = 0) -> List[Dict[str, Any]]:
    by_folder: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        folder = r.get("folder", "").strip()
        if folder:
            by_folder[folder].append(r)
    candidates = [make_candidate(folder, docs) for folder, docs in by_folder.items()]
    candidates = [c for c in candidates if c["score"] >= min_score]
    candidates.sort(
        key=lambda c: (
            -c["score"],
            -c["n_video"],
            -c["n_audio"],
            -c["n_docs"],
            c["folder"],
        )
    )
    return candidates


def write_catalog(candidates: List[Dict[str, Any]], out_path: Path, data_path: Path) -> None:
    total_video = sum(c["n_video"] for c in candidates)
    total_audio = sum(c["n_audio"] for c in candidates)
    total_docs = sum(c["n_docs"] for c in candidates)
    interview_cases = sum(1 for c in candidates if c["is_interrogation"])
    video_cases = sum(1 for c in candidates if c["has_video"])
    rows = [
        "# San Francisco DPA — NextRequest Interview / BWC / Document Catalog",
        "",
        "**Source:** San Francisco Department of Police Accountability public records",
        f"**Front-end index:** {DOC_INDEX}",
        f"**Crawled/snapshotted:** local `discovered_cases/sfdpa_docs.json`",
        f"**Candidate data:** `{data_path.as_posix()}`",
        "",
        "> This is a NextRequest public-document portal. Case folders often bundle DPA/IAD/Homicide",
        "> interview MP3s with production PDFs, and some folders include BWC MP4 exhibits. The JSON",
        "> uses `/download` URLs so a downloader can follow NextRequest's redirect to the file.",
        "",
        "## Totals",
        "",
        f"- **{len(candidates)} folders**, **{total_video} video files**, **{total_audio} audio files**, **{total_docs} documents**",
        f"- **{interview_cases} folders** include interview/interrogation-style audio",
        f"- **{video_cases} folders** include BWC/bodycam/video artifacts",
        "",
        "## All folders (ranked by artifact richness)",
        "",
        "| # | Score | Folder | Vid | Aud | Doc | Downloads | Highlights | Browse |",
        "|---|------:|--------|----:|----:|----:|----------:|------------|--------|",
    ]
    for i, c in enumerate(candidates, 1):
        highlights = ", ".join(c.get("hits", [])[:4]).replace("|", "/")
        browse = c.get("case_url") or DOC_INDEX
        rows.append(
            f"| {i} | {c['score']} | {c['folder']} | {c['n_video']} | {c['n_audio']} | "
            f"{c['n_docs']} | {c['total_downloads']} | {highlights} | [link]({browse}) |"
        )

    rows.extend(
        [
            "",
            "## Top bundles",
            "",
        ]
    )
    for c in candidates[:10]:
        rows.append(
            f"- **{c['folder']}** — {c['n_video']} video, {c['n_audio']} audio, "
            f"{c['n_docs']} docs, score {c['score']} · case_id `{c['case_id']}`"
        )
        for f in (c["media_files"] + c["doc_files"])[:5]:
            rows.append(f"  - {f['name']}")

    rows.extend(
        [
            "",
            "## How to pull a case",
            "",
            "```bash",
            "# print media download URLs for one folder",
            "python3 -c \"import json; d={c['case_id']:c for c in json.load(open('discovered_cases/sfdpa_candidates.json'))}; "
            "[print(m['url']) for m in d['sfdpa_0386-17']['media_files']]\"",
            "",
            "# download one case's media",
            "python discovered_cases/sfdpa_harvest.py --download --case-id sfdpa_0386-17",
            "```",
            "",
        ]
    )
    out_path.write_text("\n".join(rows), encoding="utf-8")


def download_media(candidates: List[Dict[str, Any]], cache_dir: Path) -> Dict[str, List[Any]]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    manifest: Dict[str, List[Any]] = {}
    for c in candidates:
        case_dir = cache_dir / c["case_id"]
        case_dir.mkdir(parents=True, exist_ok=True)
        entries = []
        for f in c.get("media_files", []):
            name = re.sub(r"[^A-Za-z0-9._ -]+", "_", f["name"]).strip(" .")
            dest = case_dir / name
            if dest.exists() and dest.stat().st_size > 0:
                entries.append([f["name"], str(dest), "cached"])
                continue
            try:
                print(f"  [dl] {c['case_id']} <- {f['name'][:80]}")
                with s.get(f["url"], stream=True, timeout=600, allow_redirects=True) as r:
                    r.raise_for_status()
                    with open(dest, "wb") as out:
                        for chunk in r.iter_content(chunk_size=1 << 20):
                            if chunk:
                                out.write(chunk)
                entries.append([f["name"], str(dest), "downloaded"])
                time.sleep(0.2)
            except Exception as e:  # noqa: BLE001
                entries.append([f["name"], str(dest), f"error: {type(e).__name__}: {e}"])
        manifest[c["case_id"]] = entries
    return manifest


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--index", default=str(ROOT / "sfdpa_docs.json"), help="captured NextRequest document index")
    ap.add_argument("--out", default=str(ROOT / "sfdpa_candidates.json"))
    ap.add_argument("--catalog", default=str(ROOT / "SFDPA_BUNDLE_CATALOG.md"))
    ap.add_argument("--min-score", type=int, default=0)
    ap.add_argument("--case-id", help="filter to a single generated case_id")
    ap.add_argument("--download", action="store_true", help="download media files for selected candidates")
    ap.add_argument("--cache-dir", default=str(ROOT.parent / "pipeline3_audio" / "foia_cache"))
    args = ap.parse_args()

    rows = load_rows(Path(args.index))
    candidates = build_candidates(rows, min_score=args.min_score)
    if args.case_id:
        candidates = [c for c in candidates if c["case_id"] == args.case_id]

    out = Path(args.out)
    out.write_text(json.dumps(candidates, indent=2, ensure_ascii=False), encoding="utf-8")
    write_catalog(candidates, Path(args.catalog), out)

    print(f"rows: {len(rows)}")
    print(f"candidates: {len(candidates)} -> {out}")
    print(f"catalog: {args.catalog}")
    for c in candidates[:12]:
        print(
            f"  [{c['score']:2}] {c['case_id']} "
            f"V={c['n_video']} A={c['n_audio']} D={c['n_docs']} "
            f"dl={c['total_downloads']} {c['folder']}"
        )

    if args.download and candidates:
        manifest = download_media(candidates, Path(args.cache_dir))
        man_path = ROOT / "sfdpa_download_manifest.json"
        man_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"download manifest: {man_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
