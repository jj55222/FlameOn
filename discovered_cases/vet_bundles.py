"""Vet and clean case-bundle candidate registries without downloading media.

This tool cleans the per-source ``*_candidates.json`` files that feed
``case_bundle_agg.py``. It does not hand-edit the generated aggregate registry.

Default mode is a dry run: reclassify in memory, assign tentative tiers, and
write ``vet_report.md``. Add ``--verify`` to do range probes / Vimeo metadata
resolution, and add ``--write`` to rewrite the source candidate files in place.

Examples:
  python discovered_cases/vet_bundles.py --selftest
  python discovered_cases/vet_bundles.py --source longbeach_laserfiche --limit-bundles 5
  python discovered_cases/vet_bundles.py --source sdpd --verify --max-files 20
  python discovered_cases/vet_bundles.py --verify --write
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import requests

sys.path.insert(0, str(Path(__file__).parent))
from muckrock_harvest import AUDIO_EXTS, DOC_EXTS, VIDEO_EXTS, ext_of  # noqa: E402


ROOT = Path(__file__).parent
REPORT_OUT = ROOT / "vet_report.md"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

EXCLUDE = {
    "CASE_BUNDLE_AGG.json",
    "muckrock_candidates_probe.json",
    "candidates_ranked.json",
}
PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff", ".bmp", ".webp", ".heic"}
EXTRA_VIDEO_EXTS = {".asf", ".3gp", ".mts", ".m2ts"}
EXTRA_AUDIO_EXTS = {".amr", ".aiff", ".aif"}
ALL_VIDEO_EXTS = set(VIDEO_EXTS) | EXTRA_VIDEO_EXTS
ALL_AUDIO_EXTS = set(AUDIO_EXTS) | EXTRA_AUDIO_EXTS

VIDEO_TYPES = {"bodycam", "dashcam", "surveillance", "interrogation", "video", "media"}
AUDIO_TYPES = {"911_audio", "radio", "audio", "interrogation"}
ALLOWED_TYPES = VIDEO_TYPES | AUDIO_TYPES | {"documents", "document", "photos", "photo", "other"}
CASE_NOISE_EXT_RE = re.compile(r"\.(?:aspx|ashx)$", re.I)
VIMEO_RE = re.compile(r"https?://player\.vimeo\.com/video/\d+[^\"'<\s]*", re.I)


@dataclass
class ProbeResult:
    live: bool
    status_code: Optional[int] = None
    size_mb: Optional[float] = None
    content_type: str = ""
    checked_at: str = ""
    resolved_url: str = ""
    verify_method: str = "range"
    error: str = ""
    title: str = ""
    duration: Optional[float] = None


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def candidate_files(root: Path = ROOT) -> List[Path]:
    out = []
    for p in sorted(root.glob("*_candidates.json")):
        if p.name in EXCLUDE or "probe" in p.name or "ranked" in p.name:
            continue
        out.append(p)
    return out


def load_items(path: Path) -> Tuple[Any, List[Dict[str, Any]]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data, data
    if isinstance(data, dict):
        items = data.get("bundles") or data.get("candidates") or []
        return data, items if isinstance(items, list) else []
    return data, []


def dump_like(original: Any, items: List[Dict[str, Any]]) -> Any:
    if isinstance(original, list):
        return items
    if isinstance(original, dict):
        out = dict(original)
        key = "bundles" if "bundles" in out else "candidates"
        out[key] = items
        return out
    return items


def normalize_ext(value: str) -> str:
    value = (value or "").strip().lower()
    if not value:
        return ""
    if CASE_NOISE_EXT_RE.fullmatch(value):
        return ""
    if not value.startswith("."):
        value = "." + value
    if not re.fullmatch(r"\.[a-z0-9]{1,6}", value):
        return ""
    return value


def file_ext(f: Dict[str, Any]) -> str:
    return (
        normalize_ext(str(f.get("ext") or ""))
        or normalize_ext(ext_of(str(f.get("name") or "")))
        or normalize_ext(ext_of(str(f.get("url") or f.get("download_url") or "")))
        or normalize_ext(ext_of(str(f.get("download_url") or "")))
    )


def is_laserfiche_mediahandler(url: str) -> bool:
    return "citydocs.longbeach.gov" in url and "mediahandler.ashx" in url.lower()


def canonical_url(f: Dict[str, Any]) -> str:
    url = str(f.get("url") or "")
    download = str(f.get("download_url") or "")
    if download and (not url or is_laserfiche_mediahandler(url)):
        return download
    return url or download


def source_file_records(c: Dict[str, Any]) -> Iterable[Tuple[str, Dict[str, Any]]]:
    saw_structured = False
    for bucket in ("media_files", "doc_files", "photo_files", "other_files"):
        for f in c.get(bucket) or []:
            if isinstance(f, dict):
                saw_structured = True
                yield bucket, f
    if not saw_structured:
        for url in c.get("urls") or []:
            if url:
                yield "media_files", {"name": "", "url": url}


def bucket_for_file(f: Dict[str, Any], old_bucket: str) -> str:
    url = canonical_url(f)
    ext = file_ext(f)
    mime = str(f.get("mime_type") or f.get("content_type") or "").lower()
    old_type = str(f.get("evidence_type") or f.get("kind") or "").lower()
    pathish = " ".join(str(f.get(k) or "") for k in ("name", "path", "url")).lower()

    if VIMEO_RE.match(url):
        return "media_files"
    if old_bucket == "photo_files" or old_type in {"photo", "photos"} or "/photos/" in pathish:
        return "photo_files"
    if mime.startswith("video/") or ext in ALL_VIDEO_EXTS:
        return "media_files"
    if mime.startswith("audio/") or ext in ALL_AUDIO_EXTS:
        return "media_files"
    if mime.startswith("image/") or ext in PHOTO_EXTS:
        return "photo_files"
    if mime == "application/pdf" or ext in DOC_EXTS:
        return "doc_files"
    return "other_files"


def normalized_evidence_type(f: Dict[str, Any], new_bucket: str) -> str:
    text = " ".join(str(f.get(k) or "") for k in ("name", "path", "title", "evidence_type", "kind")).lower()
    ext = file_ext(f)
    mime = str(f.get("mime_type") or f.get("content_type") or "").lower()

    if new_bucket == "doc_files":
        return "documents"
    if new_bucket == "photo_files":
        return "photos"
    if new_bucket == "other_files":
        return "other"

    is_video = VIMEO_RE.match(canonical_url(f)) or mime.startswith("video/") or ext in ALL_VIDEO_EXTS
    is_audio = mime.startswith("audio/") or ext in ALL_AUDIO_EXTS
    if is_video:
        if any(k in text for k in ("bodycam", "body cam", "body-worn", "body worn", "bwc", "axon")):
            return "bodycam"
        if any(k in text for k in ("dashcam", "dash cam", "in-car", "in car")):
            return "dashcam"
        if any(k in text for k in ("surveillance", "cctv", "security camera")):
            return "surveillance"
        if any(k in text for k in ("interrogation", "interview")):
            return "interrogation"
        return "video"
    if is_audio:
        if "911" in text:
            return "911_audio"
        if "radio" in text:
            return "radio"
        if any(k in text for k in ("interrogation", "interview")):
            return "interrogation"
        return "audio"
    old = str(f.get("evidence_type") or f.get("kind") or "").strip()
    return old if old in ALLOWED_TYPES else "media"


def clean_file(f: Dict[str, Any], old_bucket: str) -> Tuple[str, Dict[str, Any]]:
    rec = dict(f)
    chosen = canonical_url(rec)
    original = rec.get("url")
    if chosen and chosen != original:
        rec.setdefault("vet_original_url", original)
        rec["url"] = chosen
    ext = file_ext(rec)
    if ext:
        rec["ext"] = ext
    bucket = bucket_for_file(rec, old_bucket)
    rec["evidence_type"] = normalized_evidence_type(rec, bucket)
    if bucket == "media_files":
        rec["kind"] = "video" if rec["evidence_type"] in VIDEO_TYPES else "audio"
    elif bucket == "doc_files":
        rec["kind"] = "doc"
    elif bucket == "photo_files":
        rec["kind"] = "photo"
    else:
        rec["kind"] = "other"
    return bucket, rec


def probe_range(session: requests.Session, url: str, timeout: float) -> ProbeResult:
    checked = now_iso()
    try:
        r = session.get(
            url,
            headers={"Range": "bytes=0-0"},
            allow_redirects=True,
            timeout=timeout,
            stream=True,
        )
        status = r.status_code
        ctype = r.headers.get("Content-Type", "")
        cr = r.headers.get("Content-Range", "")
        clen = r.headers.get("Content-Length", "")
        size_mb = None
        if "/" in cr:
            try:
                size_mb = int(cr.rsplit("/", 1)[-1]) / 1e6
            except ValueError:
                size_mb = None
        elif clen and status == 206:
            try:
                size_mb = int(clen) / 1e6
            except ValueError:
                size_mb = None
        live = status in (200, 206)
        if status >= 400:
            live = False
        r.close()
        return ProbeResult(
            live=live,
            status_code=status,
            size_mb=size_mb,
            content_type=ctype,
            checked_at=checked,
            resolved_url=r.url,
        )
    except requests.RequestException as exc:
        return ProbeResult(live=False, checked_at=checked, error=f"{type(exc).__name__}: {exc}")


def probe_vimeo(url: str, timeout: float) -> ProbeResult:
    checked = now_iso()
    try:
        cmd = ["yt-dlp", "--skip-download", "--print", "%(title)s|%(duration)s", url]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return ProbeResult(live=False, checked_at=checked, verify_method="yt-dlp", error=str(exc))
    out = (p.stdout or "").strip().splitlines()
    if p.returncode != 0 or not out:
        return ProbeResult(
            live=False,
            checked_at=checked,
            verify_method="yt-dlp",
            error=(p.stderr or "").strip()[:500],
        )
    title, _, dur_s = out[-1].partition("|")
    try:
        duration = float(dur_s) if dur_s else None
    except ValueError:
        duration = None
    return ProbeResult(
        live=True,
        checked_at=checked,
        verify_method="yt-dlp",
        content_type="video/vimeo",
        resolved_url=url,
        title=title,
        duration=duration,
    )


def apply_probe(rec: Dict[str, Any], result: ProbeResult) -> None:
    data = asdict(result)
    for key, value in data.items():
        if value not in ("", None):
            rec[key] = value
    rec["live"] = result.live
    rec["checked_at"] = result.checked_at


def file_is_live(rec: Dict[str, Any], verified: bool) -> bool:
    if not verified:
        return True
    return rec.get("live") is True


def count_kinds(c: Dict[str, Any], *, verified: bool) -> Dict[str, int]:
    media = c.get("media_files") or []
    return {
        "video": sum(1 for f in media if file_is_live(f, verified) and (f.get("kind") == "video" or f.get("evidence_type") in VIDEO_TYPES)),
        "audio": sum(1 for f in media if file_is_live(f, verified) and (f.get("kind") == "audio" or f.get("evidence_type") in AUDIO_TYPES)),
        "docs": sum(1 for f in c.get("doc_files") or [] if file_is_live(f, verified)),
        "photos": sum(1 for f in c.get("photo_files") or [] if file_is_live(f, verified)),
        "other": sum(1 for f in c.get("other_files") or [] if file_is_live(f, verified)),
    }


def tier_for(c: Dict[str, Any], source: str, *, verified: bool) -> str:
    k = count_kinds(c, verified=verified)
    has_video, has_audio, has_docs = bool(k["video"]), bool(k["audio"]), bool(k["docs"])
    live_files = sum(k.values())

    if verified and live_files == 0:
        return "D"
    if source == "sdpd":
        if has_video and has_audio and has_docs:
            return "A"
        if has_docs and (has_video or has_audio):
            return "B"
        return "D"
    if source == "sfdpa_nextrequest":
        return "B" if has_docs and (has_video or has_audio) else ("D" if has_docs and not (has_video or has_audio) else "C")
    if source == "longbeach_laserfiche":
        return "C" if has_video or has_audio else ("B" if has_docs else "D")
    if source == "chicago_copa":
        if has_video:
            return "C"
        if has_audio and has_docs:
            return "B"
        return "D"
    if source == "muckrock":
        if has_docs and (has_video or has_audio):
            return "B"
        if has_video or has_audio:
            return "C"
        return "D"
    if has_video and has_audio and has_docs:
        return "B"
    if has_video or has_audio:
        return "C"
    return "D"


def vet_candidate(
    c: Dict[str, Any],
    *,
    session: Optional[requests.Session] = None,
    verify: bool = False,
    timeout: float = 20.0,
    verify_budget: Optional[List[int]] = None,
) -> Dict[str, Any]:
    out = dict(c)
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    seen_urls = set()
    for old_bucket, f in source_file_records(c):
        bucket, rec = clean_file(f, old_bucket)
        url = rec.get("url") or rec.get("download_url") or ""
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        if verify and (verify_budget is None or verify_budget[0] > 0):
            if verify_budget is not None:
                verify_budget[0] -= 1
            result = probe_vimeo(url, timeout) if VIMEO_RE.match(url) else probe_range(session or requests.Session(), url, timeout)
            apply_probe(rec, result)
        buckets[bucket].append(rec)

    for key in ("media_files", "doc_files", "photo_files", "other_files"):
        out[key] = buckets.get(key, [])

    verified = verify
    counts = count_kinds(out, verified=verified)
    out["n_video"] = counts["video"]
    out["n_audio"] = counts["audio"]
    out["n_docs"] = counts["docs"]
    out["n_photos"] = counts["photos"]
    out["n_other"] = counts["other"]
    out["has_video"] = bool(counts["video"])
    out["has_mp3"] = bool(counts["audio"])
    out["has_pdf"] = bool(counts["docs"])
    out["urls"] = [f["url"] for f in out["media_files"] if f.get("url")]

    all_files = out["media_files"] + out["doc_files"] + out["photo_files"] + out["other_files"]
    live = sum(1 for f in all_files if f.get("live") is True)
    dead = sum(1 for f in all_files if f.get("live") is False)
    unknown = len(all_files) - live - dead
    downloadable = (live / (live + dead)) if (live + dead) else None
    source = out.get("source") or ""
    out["downloadable"] = downloadable
    out["tier"] = tier_for(out, source, verified=verified)
    out["vetting"] = {
        "verified": verified,
        "checked_at": now_iso() if verify else "",
        "live_files": live,
        "dead_files": dead,
        "unknown_files": unknown,
        "downloadable": downloadable,
        "notes": vet_notes(out, source, verified=verified),
    }
    return out


def vet_notes(c: Dict[str, Any], source: str, *, verified: bool) -> List[str]:
    notes = []
    if source == "longbeach_laserfiche" and any(
        f.get("vet_original_url") and is_laserfiche_mediahandler(str(f.get("vet_original_url")))
        for f in c.get("media_files") or []
    ):
        notes.append("Laserfiche mediahandler URL replaced with ElectronicFile download_url where available.")
    if source == "chicago_copa" and not c.get("media_files"):
        notes.append("COPA case has no captured video in candidate file; run copa_harvest.py --capture-vimeo to enrich.")
    if verified and c["vetting"]["dead_files"]:
        notes.append("One or more URLs failed verify-only probing.")
    return notes


def source_matches(items: Sequence[Dict[str, Any]], path: Path, selected: set[str]) -> bool:
    if not selected:
        return True
    stem = path.stem.replace("_candidates", "")
    sources = {str(i.get("source") or "") for i in items[:20]}
    return bool(selected & ({stem, path.name} | sources))


def report_markdown(rows: List[Dict[str, Any]], *, verified: bool, wrote: bool) -> str:
    by_source: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_source[row["source"]].append(row)
    lines = [
        "# Case Bundle Vet Report",
        "",
        f"- Mode: {'verify-only probes' if verified else 'classification/tier dry run'}",
        f"- Candidate files rewritten: {'yes' if wrote else 'no'}",
        f"- Generated: {now_iso()}",
        "",
        "| source | bundles | files | live | dead | unknown | A | B | C | D |",
        "|--------|--------:|------:|-----:|-----:|--------:|--:|--:|--:|--:|",
    ]
    for source in sorted(by_source):
        rs = by_source[source]
        tiers = Counter(r["tier"] for r in rs)
        lines.append(
            f"| {source} | {len(rs)} | {sum(r['files'] for r in rs)} | "
            f"{sum(r['live'] for r in rs)} | {sum(r['dead'] for r in rs)} | {sum(r['unknown'] for r in rs)} | "
            f"{tiers.get('A',0)} | {tiers.get('B',0)} | {tiers.get('C',0)} | {tiers.get('D',0)} |"
        )
    lines.extend(["", "## Notes", ""])
    for source in sorted(by_source):
        notes = Counter()
        for r in by_source[source]:
            for note in r.get("notes") or []:
                notes[note] += 1
        if not notes:
            continue
        lines.append(f"### {source}")
        for note, count in notes.most_common():
            lines.append(f"- {count} bundle(s): {note}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def summarize_candidate(c: Dict[str, Any]) -> Dict[str, Any]:
    files = (c.get("media_files") or []) + (c.get("doc_files") or []) + (c.get("photo_files") or []) + (c.get("other_files") or [])
    return {
        "source": c.get("source") or "",
        "tier": c.get("tier") or "",
        "files": len(files),
        "live": sum(1 for f in files if f.get("live") is True),
        "dead": sum(1 for f in files if f.get("live") is False),
        "unknown": sum(1 for f in files if "live" not in f),
        "notes": (c.get("vetting") or {}).get("notes") or [],
    }


def _selftest() -> int:
    assert normalize_ext("pdf") == ".pdf"
    assert normalize_ext(".MP4") == ".mp4"
    lb = {
        "source": "longbeach_laserfiche",
        "case_id": "lb1",
        "media_files": [
            {
                "name": "BWC 1",
                "ext": "mp4",
                "url": "https://citydocs.longbeach.gov/LBPDPublicDocs/mediahandler.ashx?id=1",
                "download_url": "https://citydocs.longbeach.gov/LBPDPublicDocs/ElectronicFile.aspx?docid=1",
            }
        ],
        "photo_files": [{"name": "Photos 1", "ext": "pdf", "url": "https://x/photos.pdf", "evidence_type": "photos"}],
    }
    clean = vet_candidate(lb)
    assert clean["media_files"][0]["url"].endswith("ElectronicFile.aspx?docid=1")
    assert clean["media_files"][0]["vet_original_url"].endswith("mediahandler.ashx?id=1")
    assert clean["media_files"][0]["ext"] == ".mp4"
    assert clean["n_video"] == 1 and clean["n_photos"] == 1 and clean["tier"] == "C"
    copa = {
        "source": "chicago_copa",
        "case_id": "c1",
        "doc_files": [{"name": "COPA RELEASES VIDEO", "url": "https://x/release.pdf", "ext": "pdf", "evidence_type": "bodycam"}],
    }
    clean = vet_candidate(copa)
    assert clean["doc_files"][0]["evidence_type"] == "documents"
    assert clean["n_docs"] == 1 and clean["tier"] == "D"
    print("vet_bundles selftest: OK")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", action="append", default=[], help="source id or candidate filename to process; repeatable")
    ap.add_argument("--verify", action="store_true", help="verify URLs with range probes / yt-dlp metadata only")
    ap.add_argument("--write", action="store_true", help="rewrite source *_candidates.json files in place")
    ap.add_argument("--limit-bundles", type=int, default=0, help="dry-run limit per source; blocked with --write")
    ap.add_argument("--max-files", type=int, default=0, help="maximum files to verify across the run; 0 means all")
    ap.add_argument("--timeout", type=float, default=20.0)
    ap.add_argument("--report", type=Path, default=REPORT_OUT)
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        return _selftest()
    if args.write and args.limit_bundles:
        ap.error("--write cannot be combined with --limit-bundles")

    selected = set(args.source)
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    verify_budget = [args.max_files] if args.verify and args.max_files else None
    report_rows: List[Dict[str, Any]] = []
    touched = []

    for path in candidate_files(ROOT):
        original, items = load_items(path)
        if not source_matches(items, path, selected):
            continue
        process_items = items[: args.limit_bundles] if args.limit_bundles else items
        cleaned = [
            vet_candidate(
                c,
                session=session,
                verify=args.verify,
                timeout=args.timeout,
                verify_budget=verify_budget,
            )
            for c in process_items
        ]
        report_rows.extend(summarize_candidate(c) for c in cleaned)
        if args.write:
            out_items = cleaned
            backup = path.with_suffix(path.suffix + ".bak")
            if not backup.exists():
                shutil.copy2(path, backup)
            path.write_text(json.dumps(dump_like(original, out_items), indent=2, ensure_ascii=False), encoding="utf-8")
            touched.append(path.name)
        print(f"[vet] {path.name}: {len(cleaned)} bundle(s)")

    args.report.write_text(report_markdown(report_rows, verified=args.verify, wrote=args.write), encoding="utf-8")
    print(f"[vet] report -> {args.report}")
    if touched:
        print(f"[vet] rewrote: {', '.join(touched)}")
        print("[vet] next: python discovered_cases/case_bundle_agg.py && python discovered_cases/case_bundle_agg.py --selftest")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
