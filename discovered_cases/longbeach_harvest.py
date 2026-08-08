"""
Long Beach PD Laserfiche harvester - SB1421/SB16-style case bundle source.

This portal is Laserfiche WebLink 10. It does not expose a static HTML index.
The browser app calls JSON endpoints behind a cookie check, so this crawler:

  1. opens Browse.aspx to establish WebLinkSession / AcceptsCookies cookies
  2. calls FolderListingService.aspx/GetFolderListing2
  3. walks PUBLIC PRA Documents:
       - Officer Involved Shootings
       - Use of Force
       - Sustained Sexual Assault and Dishonesty
  4. treats each category child folder as a case bundle

Usage:
  python discovered_cases/longbeach_harvest.py
  python discovered_cases/longbeach_harvest.py --category "Officer Involved Shootings"
  python discovered_cases/longbeach_harvest.py --max-cases 10
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
from collections import Counter, defaultdict
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
BASE = "https://citydocs.longbeach.gov/LBPDPublicDocs"
PORTAL = f"{BASE}/Browse.aspx"
REPO = "LBPD-PUBDOCS"
DB_PARAM = f"dbid=0&repo={urllib.parse.quote(REPO)}"
PRA_ROOT_ID = 8
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff", ".heic"}
MEDIA_FOLDER_HINTS = ("audio", "video", "bwc", "body", "dash", "cctv")
PHOTO_FOLDER_HINTS = ("photo", "photos", "image", "images")
DOC_FOLDER_HINTS = ("document", "documents", "press release", "policy", "report")


class LaserficheClient:
    def __init__(self, timeout: float = 45.0, rate_limit: float = 0.05):
        self.s = requests.Session()
        self.s.headers.update(
            {
                "User-Agent": UA,
                "Accept": "application/json, text/plain, */*",
            }
        )
        self.timeout = timeout
        self.rate_limit = rate_limit
        self._last = 0.0
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        r = self.s.get(PORTAL, timeout=self.timeout)
        r.raise_for_status()
        self._started = True

    def post_json(self, endpoint: str, payload: Dict[str, Any]) -> Any:
        self.start()
        wait = self.rate_limit - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)
        url = f"{BASE}/{endpoint}"
        r = self.s.post(
            url,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "X-Lf-Suppress-Login-Redirect": "1",
            },
            timeout=self.timeout,
        )
        self._last = time.monotonic()
        r.raise_for_status()
        data = r.json().get("data")
        if isinstance(data, dict) and data.get("failed"):
            raise RuntimeError(f"{endpoint} failed: {data.get('errMsg')}")
        return data

    def list_folder(self, folder_id: int, page_size: int = 1000) -> Dict[str, Any]:
        all_results: List[Dict[str, Any]] = []
        first_payload = {
            "repoName": REPO,
            "folderId": folder_id,
            "getNewListing": True,
            "start": 0,
            "end": page_size,
            "sortColumn": "Name",
            "sortAscending": True,
        }
        data = self.post_json("FolderListingService.aspx/GetFolderListing2", first_payload)
        total = int(data.get("totalEntries") or 0)
        page_results = clean_results(data.get("results") or [])
        all_results.extend(page_results)

        # WebLink caps responses at the UI page size, commonly 40 rows, even
        # when the requested end is much larger. Advance by actual rows seen.
        step = max(1, len(page_results))
        start = len(page_results)
        seen = {int(r["entryId"]) for r in page_results}
        while start < total:
            payload = dict(first_payload)
            payload.update({"getNewListing": False, "start": start, "end": min(start + step, total)})
            page = self.post_json("FolderListingService.aspx/GetFolderListing2", payload)
            page_results = clean_results(page.get("results") or [])
            if not page_results:
                break
            new_results = [r for r in page_results if int(r["entryId"]) not in seen]
            all_results.extend(new_results)
            seen.update(int(r["entryId"]) for r in new_results)
            start += len(page_results)

        data["results"] = all_results
        return data


def clean_results(results: Iterable[Optional[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    return [r for r in results if r and r.get("entryId")]


def browse_url(entry_id: int) -> str:
    return f"{BASE}/Browse.aspx?id={entry_id}&{DB_PARAM}"


def doc_view_url(entry_id: int) -> str:
    return f"{BASE}/DocView.aspx?id={entry_id}&{DB_PARAM}"


def download_url(entry_id: int) -> str:
    return f"{BASE}/ElectronicFile.aspx?docid={entry_id}&{DB_PARAM}"


def absolute_media_url(media_handler_url: Optional[str]) -> str:
    if not media_handler_url:
        return ""
    if media_handler_url.startswith("http"):
        return media_handler_url
    return f"{BASE}/{media_handler_url.lstrip('/')}"


def entry_dates(entry: Dict[str, Any]) -> tuple[str, str]:
    data = entry.get("data") or []
    created = data[10] if len(data) > 10 and data[10] else ""
    modified = data[11] if len(data) > 11 and data[11] else ""
    return created, modified


def path_segments(path: str) -> List[str]:
    return [p.strip() for p in path.replace("\\", "/").split("/") if p.strip()]


def folder_context(path: str) -> str:
    segs = [p.lower() for p in path_segments(path)]
    return " ".join(segs[-3:])


def kind_of(entry: Dict[str, Any], path: str) -> str:
    ext = "." + (entry.get("extension") or "").lower().lstrip(".")
    ctx = folder_context(path)
    name = (entry.get("name") or "").lower()
    mime = (entry.get("mediaMimeType") or "").lower()
    if mime.startswith("video/") or ext in VIDEO_EXTS:
        return "video"
    if mime.startswith("audio/") or ext in AUDIO_EXTS:
        return "audio"
    if any(h in ctx for h in PHOTO_FOLDER_HINTS) or ext in PHOTO_EXTS:
        return "photo"
    if ext in DOC_EXTS or any(h in ctx for h in DOC_FOLDER_HINTS):
        return "doc"
    if any(h in ctx or h in name for h in MEDIA_FOLDER_HINTS):
        return "media_other"
    return "other"


def evidence_type(entry: Dict[str, Any], path: str) -> str:
    kind = kind_of(entry, path)
    low = f"{path} {entry.get('name', '')}".lower()
    if kind == "video":
        if any(k in low for k in ("bwc", "body", "dash", "camera", "critical incident", "briefing")):
            return "bodycam_or_incident_video"
        return "video"
    if kind == "audio":
        if any(k in low for k in ("911", "radio", "dispatch", "wav")):
            return "911_radio_audio"
        if "interview" in low:
            return "interview_audio"
        return "audio"
    if kind == "photo":
        return "photos"
    return "documents"


def file_record(entry: Dict[str, Any], path: str) -> Dict[str, Any]:
    entry_id = int(entry["entryId"])
    created, modified = entry_dates(entry)
    media_url = absolute_media_url(entry.get("mediaHandlerUrl"))
    ext = "." + (entry.get("extension") or "").lower().lstrip(".")
    if ext == ".":
        ext = ""
    url = media_url or download_url(entry_id)
    return {
        "name": entry.get("name", ""),
        "entry_id": entry_id,
        "path": path,
        "ext": ext,
        "mime_type": entry.get("mediaMimeType") or "",
        "evidence_type": evidence_type(entry, path),
        "url": url,
        "download_url": download_url(entry_id),
        "browse_url": doc_view_url(entry_id),
        "media_handler_url": media_url,
        "created_at": created,
        "modified_at": modified,
    }


def walk_case_folder(
    client: LaserficheClient,
    folder_id: int,
    path: str,
    *,
    page_size: int,
    max_folders: Optional[int] = None,
) -> tuple[List[Dict[str, Any]], int]:
    files: List[Dict[str, Any]] = []
    stack = [(folder_id, path)]
    folders_seen = 0
    while stack:
        fid, current_path = stack.pop()
        folders_seen += 1
        if max_folders and folders_seen > max_folders:
            break
        listing = client.list_folder(fid, page_size=page_size)
        for entry in listing.get("results") or []:
            name = entry.get("name") or ""
            child_path = f"{current_path}/{name}"
            if entry.get("type") == 0:
                stack.append((int(entry["entryId"]), child_path))
            else:
                files.append(file_record(entry, child_path))
    return files, folders_seen


def case_folder_rows(client: LaserficheClient, category_filter: Optional[str], page_size: int) -> List[Dict[str, Any]]:
    root = client.list_folder(PRA_ROOT_ID, page_size=page_size)
    rows: List[Dict[str, Any]] = []
    for category in root.get("results") or []:
        if category.get("type") != 0:
            continue
        category_name = category.get("name") or ""
        if category_filter and category_filter.lower() not in category_name.lower():
            continue
        category_id = int(category["entryId"])
        listing = client.list_folder(category_id, page_size=page_size)
        for entry in listing.get("results") or []:
            name = entry.get("name") or ""
            if entry.get("type") == 0:
                rows.append(
                    {
                        "category": category_name,
                        "case_name": name,
                        "folder_id": int(entry["entryId"]),
                        "path": f"PUBLIC PRA Documents/{category_name}/{name}",
                    }
                )
            else:
                rows.append(
                    {
                        "category": category_name,
                        "case_name": name,
                        "folder_id": category_id,
                        "path": f"PUBLIC PRA Documents/{category_name}/{name}",
                        "direct_entry": entry,
                    }
                )
    return rows


def bucket_files(files: Iterable[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for f in files:
        buckets[f.get("kind", "other")].append(f)
    return buckets


def make_candidate(row: Dict[str, Any], files: List[Dict[str, Any]], folders_seen: int) -> Dict[str, Any]:
    for f in files:
        f["kind"] = kind_of({"extension": f.get("ext", ""), "mediaMimeType": f.get("mime_type", ""), "name": f.get("name", "")}, f["path"])
    buckets = bucket_files(files)
    case_name = row["case_name"]
    category = row["category"]
    blob = f"{case_name} {category} " + " ".join(f["name"] for f in files)
    keyword_score, hits = score_text(blob)
    art = detect_artifacts(blob)

    n_video = len(buckets["video"])
    n_audio = len(buckets["audio"])
    n_docs = len(buckets["doc"])
    n_photos = len(buckets["photo"])
    n_other = len(buckets["other"]) + len(buckets["media_other"])
    is_interrogation = art["interrogation"] or any("interview" in f.get("evidence_type", "") for f in files)
    is_bwc = art["bwc"] or bool(n_video)
    is_sb16 = True
    artifact_bonus = (
        (6 if n_video else 0)
        + (5 if n_audio else 0)
        + (5 if n_docs else 0)
        + (3 if n_photos else 0)
        + (5 if is_interrogation else 0)
    )
    kinds = sum([bool(n_video), bool(n_audio), bool(n_docs), bool(n_photos), bool(is_interrogation)])
    bundle_bonus = {0: 0, 1: 0, 2: 3, 3: 7, 4: 12, 5: 14}[min(kinds, 5)]
    case_id = f"longbeach_{slugify(category + '_' + case_name, 70)}"
    media_files = buckets["video"] + buckets["audio"] + buckets["media_other"]
    doc_files = buckets["doc"]
    photo_files = buckets["photo"]
    return {
        "source": "longbeach_laserfiche",
        "case_id": case_id,
        "title": f"Long Beach PD {case_name}",
        "agency": "Long Beach Police Department",
        "category": category,
        "case_name": case_name,
        "folder_id": row["folder_id"],
        "case_url": browse_url(int(row["folder_id"])),
        "portal_url": PORTAL,
        "path": row["path"],
        "folders_seen": folders_seen,
        "n_docs": n_docs,
        "n_video": n_video,
        "n_audio": n_audio,
        "n_photos": n_photos,
        "n_other": n_other,
        "has_video": bool(n_video),
        "has_mp3": bool(n_audio),
        "has_pdf": bool(n_docs),
        "is_bwc": is_bwc,
        "is_interrogation": is_interrogation,
        "is_sb16": is_sb16,
        "artifact_kinds": kinds,
        "pivot": {
            "agency": "Long Beach Police Department",
            "case_title": case_name,
            "category": category,
            "folder_id": row["folder_id"],
        },
        "titles": [f["name"] for f in files][:50],
        "media_files": media_files,
        "doc_files": doc_files,
        "photo_files": photo_files,
        "other_files": buckets["other"] + buckets["media_other"],
        "urls": [f["url"] for f in media_files],
        "keyword_score": keyword_score,
        "artifact_bonus": artifact_bonus,
        "bundle_bonus": bundle_bonus,
        "score": keyword_score + artifact_bonus + bundle_bonus,
        "hits": hits,
    }


def build_candidates(
    client: LaserficheClient,
    *,
    category: Optional[str],
    max_cases: Optional[int],
    page_size: int,
    min_score: int,
) -> List[Dict[str, Any]]:
    rows = case_folder_rows(client, category, page_size)
    if max_cases:
        rows = rows[:max_cases]
    candidates: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, 1):
        if "direct_entry" in row:
            files = [file_record(row["direct_entry"], row["path"])]
            folders_seen = 0
        else:
            files, folders_seen = walk_case_folder(client, int(row["folder_id"]), row["path"], page_size=page_size)
        c = make_candidate(row, files, folders_seen)
        if c["score"] >= min_score or files:
            candidates.append(c)
        print(
            f"[case {idx}/{len(rows)}] {row['category']} / {row['case_name']} "
            f"V={c['n_video']} A={c['n_audio']} D={c['n_docs']} P={c['n_photos']}",
            flush=True,
        )
    candidates.sort(
        key=lambda c: (
            -c["score"],
            -c["n_video"],
            -c["n_audio"],
            -c["n_docs"],
            -c["n_photos"],
            c["case_name"],
        )
    )
    return candidates


def write_catalog(candidates: List[Dict[str, Any]], out_path: Path, data_path: Path) -> None:
    totals = Counter()
    categories = Counter()
    for c in candidates:
        categories[c["category"]] += 1
        totals["video"] += c["n_video"]
        totals["audio"] += c["n_audio"]
        totals["docs"] += c["n_docs"]
        totals["photos"] += c["n_photos"]
        totals["other"] += c["n_other"]

    rows = [
        "# Long Beach PD - Laserfiche Case Bundle Catalog",
        "",
        f"**Source:** Long Beach Police Department Laserfiche public documents",
        f"**Front-end index:** {PORTAL}",
        f"**Repository:** `{REPO}`",
        f"**Candidate data:** `{data_path.as_posix()}`",
        "",
        "> WebLink requires cookies. The crawler opens `Browse.aspx` first, then calls",
        "> `FolderListingService.aspx/GetFolderListing2` with the same session. Direct",
        "> media/download URLs are saved, but browser/crawler sessions must keep cookies.",
        "",
        "## Totals",
        "",
        f"- **{len(candidates)} case folders**",
        f"- **{totals['video']} video files**, **{totals['audio']} audio files**, "
        f"**{totals['docs']} documents**, **{totals['photos']} photo/exhibit files**, "
        f"**{totals['other']} other files**",
        "- By category: "
        + ", ".join(f"{name} ({count})" for name, count in categories.most_common()),
        "",
        "## All cases (ranked by artifact richness)",
        "",
        "| # | Score | Category | Case | Vid | Aud | Doc | Photo | Browse |",
        "|---|------:|----------|------|----:|----:|----:|------:|--------|",
    ]
    for i, c in enumerate(candidates, 1):
        rows.append(
            f"| {i} | {c['score']} | {c['category']} | {c['case_name'].replace('|', '/')} | "
            f"{c['n_video']} | {c['n_audio']} | {c['n_docs']} | {c['n_photos']} | "
            f"[link]({c['case_url']}) |"
        )

    rows.extend(["", "## Top bundles", ""])
    for c in candidates[:15]:
        rows.append(
            f"- **{c['case_name']}** ({c['category']}) - {c['n_video']} video, "
            f"{c['n_audio']} audio, {c['n_docs']} docs, {c['n_photos']} photos, "
            f"score {c['score']} - case_id `{c['case_id']}`"
        )
        for f in (c["media_files"] + c["doc_files"] + c["photo_files"])[:8]:
            rows.append(f"  - {f['name']} ({f['ext'] or 'no ext'})")

    rows.extend(["", "## Richest raw bundles", ""])
    richest = sorted(
        candidates,
        key=lambda c: c["n_video"] + c["n_audio"] + c["n_docs"] + c["n_photos"],
        reverse=True,
    )
    for c in richest[:15]:
        total = c["n_video"] + c["n_audio"] + c["n_docs"] + c["n_photos"]
        rows.append(
            f"- **{c['case_name']}** ({c['category']}) - {total} files: "
            f"{c['n_video']} video, {c['n_audio']} audio, {c['n_docs']} docs, "
            f"{c['n_photos']} photos - case_id `{c['case_id']}`"
        )

    rows.extend(
        [
            "",
            "## How to pull one case",
            "",
            "Use the `case_id` from the table or top bundles, then print that case's direct file URLs:",
            "",
            "```bash",
            "python - <<'PY'",
            "import json",
            "from pathlib import Path",
            "",
            "case_id = 'longbeach_officer_involved_shootings_150001828_gonzales_joseph'",
            "data = json.loads(Path('discovered_cases/longbeach_candidates.json').read_text())",
            "case = next(c for c in data if c['case_id'] == case_id)",
            "for f in case['media_files'] + case['doc_files'] + case['photo_files']:",
            "    print(f['url'])",
            "PY",
            "```",
            "",
            "## Source verdict",
            "",
            "- Treat Long Beach as a high-value official bundle source.",
            "- It has case folders with video, audio, documents, and photo/exhibit PDFs.",
            "- Direct downloads require a live WebLink cookie session; use the harvester, not bare URL fetches.",
            "",
        ]
    )
    out_path.write_text("\n".join(rows), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--category", help="optional category substring, e.g. Officer Involved")
    ap.add_argument("--max-cases", type=int, default=None, help="limit case folders for quick tests")
    ap.add_argument("--min-score", type=int, default=0)
    ap.add_argument("--page-size", type=int, default=1000)
    ap.add_argument("--timeout", type=float, default=45.0)
    ap.add_argument("--rate-limit", type=float, default=0.05)
    ap.add_argument("--out", default=str(ROOT / "longbeach_candidates.json"))
    ap.add_argument("--catalog", default=str(ROOT / "LONGBEACH_LASERFICHE_CATALOG.md"))
    args = ap.parse_args()

    client = LaserficheClient(timeout=args.timeout, rate_limit=args.rate_limit)
    candidates = build_candidates(
        client,
        category=args.category,
        max_cases=args.max_cases,
        page_size=args.page_size,
        min_score=args.min_score,
    )

    out = Path(args.out)
    out.write_text(json.dumps(candidates, indent=2, ensure_ascii=False), encoding="utf-8")
    write_catalog(candidates, Path(args.catalog), out)

    totals = Counter()
    for c in candidates:
        totals["video"] += c["n_video"]
        totals["audio"] += c["n_audio"]
        totals["docs"] += c["n_docs"]
        totals["photos"] += c["n_photos"]
    print(f"candidates: {len(candidates)} -> {out}")
    print(f"catalog: {args.catalog}")
    print(
        f"totals: V={totals['video']} A={totals['audio']} "
        f"D={totals['docs']} P={totals['photos']}"
    )
    for c in candidates[:15]:
        print(
            f"  [{c['score']:2}] {c['case_id']} "
            f"V={c['n_video']} A={c['n_audio']} D={c['n_docs']} P={c['n_photos']} "
            f"{c['case_name']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
