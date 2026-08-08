"""P3.5 / GOAL_D D0 — stamp each case artifact with an absolute time.

A documentary needs to know WHEN each clip happened so it can bucket footage
into phases (pre-incident / incident / aftermath / investigation / outcome).
This module assigns every artifact an absolute ``start_epoch`` (UTC unix
seconds) using the cheapest reliable signal for its kind, degrading to an
honest ``none`` rather than guessing.

Strategies (per kind):
  bodycam / interrogation / court video  -> AXON burned-in clock OCR
                                            (reuses pov_triage.ocr_clock),
                                            then container creation_time.
  dashcam                                -> container creation_time
                                            (audio cross-correlation is D3).
  911 / radio / audio                    -> container creation_time, else a
                                            time in the filename, else none.
  doc (pdf)                              -> first date in the text (best-effort).
  anything unstampable                   -> start_epoch=None, source="none".

Pure stdlib + ffmpeg; easyocr only when a video needs OCR (best-effort).
Run with the global Python (static_ffmpeg + easyocr).

    python pipeline3_audio/timeline_stamp.py --basket .tmp/sac_poc/video \
        --out .tmp/sac_poc/timeline
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pov_triage as pt  # noqa: E402  (sibling module; reuse ffmpeg + OCR)

VIDEO_EXTS = {".mp4", ".mov", ".mkv", ".m4v", ".webm", ".avi"}
AUDIO_EXTS = {".mp3", ".wav", ".m4a", ".aac", ".flac"}
OCR_KINDS = {"bodycam", "interrogation", "court"}

# date/time embedded in a filename, e.g. 2023-04-18_0945 or 20230418T0945
_FNAME_DT = re.compile(
    r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})[ _T]?(\d{2})[:_-]?(\d{2})(?:[:_-]?(\d{2}))?"
)
_PDF_DATE = re.compile(
    r"\b(20\d{2})-(\d{2})-(\d{2})\b|"
    r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b"
)


_CAM_RE = re.compile(r"\b(bwc|icc|bodycam|dash|dashcam)[\s_\-]?(\d+[a-z]?)\b", re.I)


def _cam_tag(name: str) -> Optional[str]:
    m = _CAM_RE.search(name)
    return f"{m.group(1).upper()}-{m.group(2)}" if m else None


def detect_kind(path: Path) -> str:
    low = path.name.lower()
    toks = {t for t in re.split(r"[^a-z0-9]+", low) if t}
    suf = path.suffix.lower()
    if suf == ".pdf":
        return "doc"
    if "911" in low:
        return "911"
    if "radio" in low:
        return "radio"
    if {"bwc", "bodycam", "bodyworn"} & toks or re.search(r"body.?cam|body.?worn", low):
        return "bodycam"  # also catches SDPD "Officer1BodyCameraVideo" (one token)
    if {"icc", "dash", "dashcam"} & toks:
        return "dashcam"
    if {"interview", "interrogation", "dpa", "iad", "interrog"} & toks:
        return "interrogation"
    if {"court", "trial", "hearing"} & toks:
        return "court"
    if suf in AUDIO_EXTS:
        return "audio"
    if suf in VIDEO_EXTS:
        return "other_video"
    return "other"


def _probe_duration(path: Path) -> float:
    out = subprocess.run(
        [pt.FFPROBE, "-v", "error", "-show_entries", "format=duration",
         "-of", "default=nw=1:nk=1", str(path)],
        capture_output=True, text=True).stdout.strip()
    try:
        return float(out)
    except ValueError:
        return 0.0


def _metadata_epoch(path: Path) -> Optional[float]:
    out = subprocess.run(
        [pt.FFPROBE, "-v", "error", "-show_entries", "format_tags=creation_time",
         "-of", "default=nw=1:nk=1", str(path)],
        capture_output=True, text=True).stdout.strip()
    if not out:
        return None
    try:
        iso = out.replace("Z", "+00:00")
        d = dt.datetime.fromisoformat(iso)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.timestamp()
    except ValueError:
        return None


def _filename_epoch(name: str) -> Optional[float]:
    m = _FNAME_DT.search(name)
    if not m:
        return None
    y, mo, da, hh, mm, ss = m.groups()
    try:
        d = dt.datetime(int(y), int(mo), int(da), int(hh), int(mm),
                        int(ss or 0), tzinfo=dt.timezone.utc)
        return d.timestamp()
    except ValueError:
        return None


def _pdf_epoch(path: Path) -> Optional[float]:
    try:
        import pypdf  # best-effort; absent/broken -> None
        reader = pypdf.PdfReader(str(path))
        text = " ".join((pg.extract_text() or "") for pg in reader.pages[:3])
    except Exception:
        return None
    m = _PDF_DATE.search(text)
    if not m:
        return None
    try:
        if m.group(1):
            y, mo, da = m.group(1), m.group(2), m.group(3)
        else:
            mo, da, y = m.group(4), m.group(5), m.group(6)
        d = dt.datetime(int(y), int(mo), int(da), tzinfo=dt.timezone.utc)
        return d.timestamp()
    except ValueError:
        return None


def stamp_artifact(path: Path) -> Dict:
    kind = detect_kind(path)
    dur = _probe_duration(path)
    start_epoch: Optional[float] = None
    source, conf = "none", 0.0

    if kind in OCR_KINDS and dur > 0:
        ep, _s = pt.ocr_clock(str(path), dur, samples=8)
        if ep is not None:
            start_epoch, source, conf = ep, "axon_ocr", 0.85
    if start_epoch is None:
        ep = _metadata_epoch(path)
        if ep is not None:
            start_epoch, source, conf = ep, "metadata", 0.6
    if start_epoch is None:
        ep = _filename_epoch(path.name)
        if ep is not None:
            start_epoch, source, conf = ep, "filename", 0.5
    if start_epoch is None and kind == "doc":
        ep = _pdf_epoch(path)
        if ep is not None:
            start_epoch, source, conf = ep, "doc_date", 0.4

    def _iso(e: Optional[float]) -> Optional[str]:
        return dt.datetime.fromtimestamp(e, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if e else None

    return {
        "artifact_id": path.stem,
        "path": str(path),
        "kind": kind,
        "pov_label": (_cam_tag(path.name) or path.stem),
        "duration_sec": round(dur, 1),
        "start_epoch": start_epoch,
        "end_epoch": (start_epoch + dur) if start_epoch else None,
        "start_iso": _iso(start_epoch),
        "timestamp_source": source,
        "timestamp_confidence": conf,
        "hot_windows": [],
        "phase": None,
    }


def stamp_basket(basket: Path) -> List[Dict]:
    files = [p for p in sorted(basket.rglob("*"))
             if p.is_file() and p.suffix.lower() in (VIDEO_EXTS | AUDIO_EXTS | {".pdf"})]
    arts = []
    for i, p in enumerate(files):
        sys.stderr.write(f"  [{i+1}/{len(files)}] stamping {p.name} ...\n"); sys.stderr.flush()
        arts.append(stamp_artifact(p))
    return arts


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="GOAL_D D0 — stamp artifacts with absolute time")
    ap.add_argument("--basket", required=True, type=Path)
    ap.add_argument("--out", type=Path, default=Path(".tmp/timeline"))
    args = ap.parse_args(argv)

    pt.FFMPEG, pt.FFPROBE = pt._ff()
    arts = stamp_basket(args.basket)

    # Print as a timeline: stamped artifacts in time order, then the unstamped.
    stamped = sorted([a for a in arts if a["start_epoch"]], key=lambda a: a["start_epoch"])
    unstamped = [a for a in arts if not a["start_epoch"]]
    print(f"\n{'start (UTC)':21s}{'dur':>7s}  {'kind':13s}{'source':10s}  artifact")
    for a in stamped:
        print(f"{a['start_iso']:21s}{a['duration_sec']/60:6.1f}m  {a['kind']:13s}"
              f"{a['timestamp_source']:10s}  {a['artifact_id']}")
    if unstamped:
        print(f"\n  UNSTAMPED ({len(unstamped)}) — need a CAD log / metadata / D3 audio-xcorr:")
        for a in unstamped:
            print(f"    {a['kind']:13s} {a['artifact_id']}")

    args.out.mkdir(parents=True, exist_ok=True)
    out = args.out / "artifacts.json"
    out.write_text(json.dumps(arts, indent=2), encoding="utf-8")
    by_src: Dict[str, int] = {}
    for a in arts:
        by_src[a["timestamp_source"]] = by_src.get(a["timestamp_source"], 0) + 1
    print(f"\n[stamped] {len(stamped)}/{len(arts)} artifacts -> {out}   by source: {by_src}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
