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
BASE = "https://sfdpa.nextrequest.com"
DOC_INDEX = f"{BASE}/documents"
DOC_API = f"{BASE}/client/documents"
PAGE_SIZE = 50          # NextRequest hard-caps page_size at 50 (larger -> empty)
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)


def fetch_documents_live(session: requests.Session, *, page_size: int = PAGE_SIZE,
                         max_docs: Optional[int] = None, sleep: float = 0.4,
                         verbose: bool = True) -> List[Dict[str, Any]]:
    """Enumerate the whole NextRequest document index via the JSON API. Pages with
    ``page_number`` (``page``/``offset`` are ignored by the server). Stops when a
    page is empty, the running total reaches ``total_count``, or ``max_docs`` is hit."""
    docs: List[Dict[str, Any]] = []
    page = 1
    total: Optional[int] = None
    while True:
        r = session.get(DOC_API, params={"page_size": page_size, "page_number": page}, timeout=60)
        r.raise_for_status()
        data = r.json()
        batch = data.get("documents") or []
        total = data.get("total_count", total)
        docs.extend(batch)
        if verbose:
            print(f"  [page {page}] +{len(batch)} ({len(docs)}/{total})")
        if not batch or (total and len(docs) >= total) or (max_docs and len(docs) >= max_docs):
            break
        page += 1
        time.sleep(sleep)
    return docs[:max_docs] if max_docs else docs


def api_doc_to_row(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Map one NextRequest API document to the internal row shape (the same shape
    ``parse_markdown_rows`` emits) so candidate-building is source-agnostic."""
    path = doc.get("document_path") or f"/documents/{doc.get('id')}"
    url = BASE + path
    req_path = doc.get("request_path") or ""
    title = _clean_cell(doc.get("title", ""))
    # The shared classifiers key on DOTTED extensions ('.pdf'); the API returns a
    # bare 'pdf'. Normalize, else every PDF misclassifies as 'other' (D=0).
    ext = (doc.get("file_extension") or "").strip().lower()
    if ext and not ext.startswith("."):
        ext = "." + ext
    ext = ext or ext_of(title)
    return {
        "title": title,
        "url": url,
        "download_url": nextrequest_download_url(url),
        "request": (doc.get("pretty_id") or "").strip(),
        "request_url": (BASE + req_path) if req_path else "",
        "upload_date": (doc.get("created_at") or "").strip(),
        "downloads": int(doc.get("count") or 0),
        "folder": _clean_cell(doc.get("folder_name") or ""),
        "document_date": (doc.get("doc_date") or "").strip() if doc.get("doc_date") else "",
        "description": _clean_cell(doc.get("description", "")),
        "ext": ext,
    }


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


def _selftest() -> int:
    """Offline guard for the classification invariant the API broke once: the
    shared EXTS sets are DOTTED, the API returns bare extensions. If this regresses,
    every PDF silently lands in 'other' and the legal-doc half of each bundle vanishes."""
    cases = [
        ({"id": 1, "title": "Production - 0658-08 Part A.pdf", "file_extension": "pdf",
          "folder_name": "0658-08", "document_path": "/documents/1", "request_path": "/requests/20-5",
          "pretty_id": "20-5", "count": 323}, ".pdf", "doc", "documents"),
        ({"id": 2, "title": "0386-17 DPA Interview - Ravelo.mp3", "file_extension": "mp3",
          "folder_name": "0386-17", "document_path": "/documents/2", "count": 279}, ".mp3", "audio", "interrogation"),
        ({"id": 3, "title": "0656-18 BWC Footage of Officer Nazar.mp4", "file_extension": "mp4",
          "folder_name": "0656-18", "document_path": "/documents/3", "count": 10}, ".mp4", "video", "bodycam"),
    ]
    for doc, want_ext, want_kind, want_ev in cases:
        row = api_doc_to_row(doc)
        assert row["ext"] == want_ext, f"ext: want {want_ext}, got {row['ext']}"
        assert kind_of(row) == want_kind, f"{want_ext}: kind want {want_kind}, got {kind_of(row)}"
        assert evidence_type(row) == want_ev, f"{want_ext}: evtype want {want_ev}, got {evidence_type(row)}"
        assert row["download_url"].endswith("/download"), "download URL must hit the /download redirect"
    # a folder mixing audio + a pdf -> a real multi-kind bundle, doc COUNTED
    cand = make_candidate("0658-08", [api_doc_to_row(c[0]) for c in cases])
    assert cand["n_docs"] == 1, f"pdf must count as a doc, got n_docs={cand['n_docs']}"
    assert cand["n_audio"] == 1 and cand["n_video"] == 1 and cand["artifact_kinds"] >= 3
    print("sfdpa_harvest selftest: OK  (pdf->doc, mp3->interrogation, mp4->bodycam; bundle keeps all 3)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    if "--selftest" in sys.argv:
        return _selftest()
    ap.add_argument("--from-capture", metavar="JSON", default=None,
                    help="parse a captured NextRequest markdown snapshot instead of crawling live (offline)")
    ap.add_argument("--max-docs", type=int, default=None, help="cap docs fetched (quick live test)")
    ap.add_argument("--page-size", type=int, default=PAGE_SIZE, help="NextRequest page size (max 50)")
    ap.add_argument("--sleep", type=float, default=0.4, help="pause between API pages (politeness)")
    ap.add_argument("--out", default=str(ROOT / "sfdpa_candidates.json"))
    ap.add_argument("--catalog", default=str(ROOT / "SFDPA_BUNDLE_CATALOG.md"))
    ap.add_argument("--min-score", type=int, default=0)
    ap.add_argument("--case-id", help="filter to a single generated case_id")
    ap.add_argument("--download", action="store_true", help="download media files for selected candidates")
    ap.add_argument("--cache-dir", default=str(ROOT.parent / "pipeline3_audio" / "foia_cache"))
    args = ap.parse_args()

    if args.from_capture:
        print(f"[sfdpa] offline: parsing capture {args.from_capture}")
        rows = load_rows(Path(args.from_capture))
    else:
        print(f"[sfdpa] live crawl: {DOC_API}")
        s = requests.Session()
        s.headers.update({"User-Agent": UA, "Accept": "application/json"})
        docs = fetch_documents_live(s, page_size=min(args.page_size, PAGE_SIZE),
                                    max_docs=args.max_docs, sleep=args.sleep)
        rows = [api_doc_to_row(d) for d in docs]
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
