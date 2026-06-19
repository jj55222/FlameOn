"""P3.5 — Multi-POV triage + targeted transcription.

For a case with many camera angles (bodycam / dashcam), full transcription is
the bottleneck (hours of whisper). This module finds the *action* cheaply,
BEFORE transcribing, so whisper only runs on the hot footage.

Signals (cheapest first, all optional-degrading):
  1. Audio onset-energy scan  (ffmpeg + numpy, no ML): sudden loud events —
     shouts, commands, struggle, gunshots — are transients; steady engine /
     road noise is loud but flat, so onset detection ignores it. Ranks POVs
     and finds hot windows. The AXON ~30-60s muted pre-event buffer produces a
     false "audio turns on" spike that is detected and suppressed.
  2. Impulsive-transient flag (DSP crest factor): sharp broadband hits
     (gunshot / door slam) — a cheap stand-in for PANNs gunshot detection.
  3. AXON burned-in clock OCR (easyocr, best-effort): per-POV wall-clock
     offset so a hot moment in one camera maps onto every other.

``--transcribe-hot`` then transcribes only the hot windows (padded by
``--pad-pre`` / ``--pad-post`` minutes so inciting moments / aftermath are
kept) of the top POVs, emitting p3_to_p4 transcripts that P4 consumes.

Pure stdlib + numpy + ffmpeg for the scan; faster_whisper only for
``--transcribe-hot``; easyocr only for ``--ocr``. Run with the global Python
(has static_ffmpeg + numpy + faster_whisper).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np


def _ff() -> Tuple[str, str]:
    try:
        import static_ffmpeg
        static_ffmpeg.add_paths()
    except Exception:
        pass
    ff, fp = shutil.which("ffmpeg"), shutil.which("ffprobe")
    if not ff or not fp:
        raise RuntimeError("ffmpeg/ffprobe not found (pip install static_ffmpeg)")
    return ff, fp


FFMPEG, FFPROBE = "", ""

VIDEO_EXTS = {".mp4", ".mov", ".mkv", ".m4v", ".webm", ".avi"}
SCAN_SR = 4000              # audio decode rate for the salience scan
WIN_SEC = 1.0              # analysis window
ONSET_DB = 6.0            # a "sudden loud event" = >6 dB jump between windows


# ---------------------------------------------------------------------------
# Audio decode + salience scan
# ---------------------------------------------------------------------------

def decode_audio(path: str, sr: int) -> np.ndarray:
    raw = subprocess.run(
        [FFMPEG, "-v", "error", "-i", path, "-ac", "1", "-ar", str(sr),
         "-f", "s16le", "-"],
        capture_output=True).stdout
    return np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0


@dataclass
class PovScan:
    name: str
    path: str
    duration_sec: float
    evidence_type: str
    activity_score: float          # sum of suppressed-onset energy
    impulse_score: float           # strength of sharpest transient
    audio_start_sec: float         # where audio kicks in after the muted buffer
    hot_windows: List[List[float]] = field(default_factory=list)   # [[start,end],...]
    impulses: List[float] = field(default_factory=list)            # impulsive event centers (s)
    wallclock_epoch_at_0: Optional[float] = None                   # OCR'd
    wallclock_str: Optional[str] = None


def _evidence_type(name: str) -> str:
    low = name.lower()
    if re.search(r"\b(bwc|bodycam|bodyworn)\b", low) or "bwc" in low:
        return "bodycam"
    if re.search(r"\b(icc|dash|dashcam)\b", low) or "icc" in low:
        return "dash_cam"
    if "911" in low:
        return "911_audio"
    return "bodycam"


def scan_salience(path: str) -> PovScan:
    name = Path(path).stem
    a = decode_audio(path, SCAN_SR)
    n = int(SCAN_SR * WIN_SEC)
    if len(a) < n:
        return PovScan(name, path, len(a) / SCAN_SR, _evidence_type(name), 0.0, 0.0, 0.0)
    nwin = len(a) // n
    w = a[: nwin * n].reshape(nwin, n)
    rms = np.sqrt((w ** 2).mean(axis=1) + 1e-12)
    peak = np.abs(w).max(axis=1)
    db = 20 * np.log10(rms + 1e-12)
    onset = np.clip(np.diff(db, prepend=db[0]), 0, None)
    crest = peak / (rms + 1e-9)          # impulsive-ness per window

    # Suppress the AXON muted-buffer "audio turns on" artifact: find where audio
    # first sustains above the noise floor, zero onsets up to just after it.
    floor = np.percentile(db, 10)
    active = db > (floor + 15)
    audio_start_idx = 0
    for i in range(len(active) - 3):
        if active[i] and active[i + 1] and active[i + 2]:
            audio_start_idx = i
            break
    onset_f = onset.copy()
    onset_f[: audio_start_idx + 2] = 0.0

    # Hot windows: 1s windows with a >ONSET_DB jump, merged (<=8s gap), with the
    # 1-min radio "fire for X" check-ins kept out by requiring local energy too.
    hot_idx = sorted(int(i) for i in np.where(onset_f > ONSET_DB)[0])
    spans: List[List[float]] = []
    for t in hot_idx:
        if spans and t - spans[-1][1] <= 8:
            spans[-1][1] = float(t)
        else:
            spans.append([float(t), float(t)])

    # Impulsive events (gunshot/door-slam proxy): high crest + high peak, after
    # the audio start. Report their centres.
    crest_f = crest.copy()
    crest_f[: audio_start_idx + 2] = 0.0
    imp_thr = max(8.0, np.percentile(crest_f[crest_f > 0], 99) if (crest_f > 0).any() else 8.0)
    impulses = [float(i) for i in np.where((crest_f >= imp_thr) & (peak > 0.2))[0]]
    impulse_score = float(crest_f.max()) if crest_f.size else 0.0

    return PovScan(
        name=name, path=path, duration_sec=len(a) / SCAN_SR,
        evidence_type=_evidence_type(name),
        activity_score=round(float(onset_f.sum()), 1),
        impulse_score=round(impulse_score, 1),
        audio_start_sec=round(audio_start_idx * WIN_SEC, 1),
        hot_windows=spans, impulses=impulses,
    )


def pad_merge(spans: List[List[float]], dur: float, pre: float, post: float) -> List[List[float]]:
    """Pad each hot span by pre/post seconds (clip to file) and merge overlaps."""
    padded = sorted([max(0.0, s - pre), min(dur, e + post)] for s, e in spans)
    out: List[List[float]] = []
    for s, e in padded:
        if out and s <= out[-1][1]:
            out[-1][1] = max(out[-1][1], e)
        else:
            out.append([s, e])
    return out


# ---------------------------------------------------------------------------
# AXON burned-in clock OCR (best-effort, easyocr)
# ---------------------------------------------------------------------------

# Tolerant: AXON OCR yields "2023-04-18" + "09: 47 :20" (spaces around colons,
# '_' for '-', '.' for ':'). Date and time can arrive as separate OCR fragments.
_DT_RE = re.compile(
    r"(20\d{2})[-_/.\s](\d{1,2})[-_/.\s](\d{1,2}).{0,12}?"
    r"(\d{1,2})\s*[:.]\s*(\d{2})\s*[:.]\s*(\d{2})"
)
_OCR_READER = None


def _get_reader():
    global _OCR_READER
    if _OCR_READER is None:
        import easyocr  # raises if not installed
        _OCR_READER = easyocr.Reader(["en"], gpu=False, verbose=False)
    return _OCR_READER


def _frame_png(path: str, t: float, out_png: str) -> bool:
    r = subprocess.run([FFMPEG, "-y", "-ss", str(t), "-i", path, "-frames:v", "1",
                        out_png], capture_output=True)
    return os.path.exists(out_png)


def ocr_clock(path: str, dur: float, samples: int = 8) -> Tuple[Optional[float], Optional[str]]:
    """Read the burned-in AXON timestamp at several frames; return
    (epoch_at_frame_time_0, human_str) or (None, None). Best-effort."""
    try:
        from PIL import Image
        reader = _get_reader()
    except Exception:
        return None, None
    import datetime as _dt
    tmp = Path(".tmp/_ocr_frames"); tmp.mkdir(parents=True, exist_ok=True)
    offsets, first_str = [], None
    for k in range(samples):
        t = dur * (k + 0.5) / samples
        png = str(tmp / f"f_{abs(hash(path)) % 9999}_{k}.png")
        if not _frame_png(path, t, png):
            continue
        try:
            img = Image.open(png).convert("RGB")
            W, H = img.size
            crop = np.array(img.crop((0, 0, W, int(H * 0.12))))   # top strip
            txt = " ".join(reader.readtext(crop, detail=0))
        except Exception:
            continue
        m = _DT_RE.search(txt.replace(" ", " "))
        if not m:
            continue
        try:
            y, mo, d, hh, mm, ss = (int(x) for x in m.groups())
            wall = _dt.datetime(y, mo, d, hh, mm, ss, tzinfo=_dt.timezone.utc)
            epoch = wall.timestamp()
            offsets.append(epoch - t)
            if first_str is None:
                first_str = wall.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, OverflowError):
            continue
    if not offsets:
        return None, None
    return float(np.median(offsets)), first_str


# ---------------------------------------------------------------------------
# Targeted transcription (faster_whisper on padded hot windows only)
# ---------------------------------------------------------------------------

def transcribe_windows(path: str, windows: List[List[float]], model,
                       case_id: str, evidence_type: str, dur: float) -> Dict:
    """Transcribe only the given windows; return a p3_to_p4 transcript dict
    with segment timestamps remapped to the ORIGINAL file timeline."""
    segments: List[Dict] = []
    tmp = Path(".tmp/_hot_clips"); tmp.mkdir(parents=True, exist_ok=True)
    for wi, (s, e) in enumerate(windows):
        wav = str(tmp / f"{case_id}_{Path(path).stem}_{wi}.wav")
        subprocess.run([FFMPEG, "-y", "-ss", str(s), "-t", str(e - s), "-i", path,
                        "-ac", "1", "-ar", "16000", wav], capture_output=True)
        if not os.path.exists(wav):
            continue
        segs, _info = model.transcribe(wav, vad_filter=True, language="en")
        for seg in segs:
            txt = (seg.text or "").strip()
            if not txt:
                continue
            segments.append({
                "start_sec": round(s + seg.start, 3),
                "end_sec": round(s + seg.end, 3),
                "text": txt,
                "confidence": round(float(np.exp(seg.avg_logprob)) if seg.avg_logprob else 0.0, 3),
            })
        try:
            os.remove(wav)
        except OSError:
            pass
    segments.sort(key=lambda x: x["start_sec"])
    return {
        "case_id": case_id,
        "source_evidence_type": evidence_type,
        "source_url": path,
        "transcript": segments,
        "silence_map": [],
        "original_duration_sec": round(dur, 3),
        "processed_duration_sec": round(sum(e - s for s, e in windows), 1),
        "speaker_count": None,
        "processing_metadata": {
            "whisper_model": "small", "whisper_backend": "faster_whisper",
            "triage": "pov_triage hot-window targeted transcription",
            "windows": windows,
        },
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def list_media(media_dir: Path) -> List[Path]:
    return [p for p in sorted(media_dir.rglob("*"))
            if p.is_file() and p.suffix.lower() in VIDEO_EXTS]


def run_triage(media_dir: Path, do_ocr: bool) -> List[PovScan]:
    scans: List[PovScan] = []
    files = list_media(media_dir)
    for i, p in enumerate(files):
        sys.stderr.write(f"  [{i+1}/{len(files)}] scanning {p.name} ...\n"); sys.stderr.flush()
        sc = scan_salience(str(p))
        if do_ocr:
            ep, s = ocr_clock(str(p), sc.duration_sec)
            sc.wallclock_epoch_at_0, sc.wallclock_str = ep, s
        scans.append(sc)
    scans.sort(key=lambda s: -s.activity_score)
    return scans


def print_table(scans: List[PovScan]) -> None:
    print(f"\n{'POV':12s}{'dur':>7s}{'activity':>9s}{'impulse':>8s}{'audio@':>7s}  "
          f"{'wallclock':19s}  hot_windows(s)")
    for s in scans:
        hw = ", ".join(f"{int(a)}-{int(b)}" for a, b in s.hot_windows[:6])
        wc = (s.wallclock_str or "")[:19]
        print(f"{s.name:12s}{s.duration_sec:6.0f}s{s.activity_score:9.0f}"
              f"{s.impulse_score:8.0f}{s.audio_start_sec:6.0f}s  {wc:19s}  {hw}")


def main(argv: Optional[List[str]] = None) -> int:
    global FFMPEG, FFPROBE
    ap = argparse.ArgumentParser(description="Multi-POV triage + targeted transcription")
    ap.add_argument("--media-dir", required=True, type=Path)
    ap.add_argument("--ocr", action="store_true", help="OCR the AXON burned-in clock (easyocr)")
    ap.add_argument("--transcribe-hot", action="store_true",
                    help="Transcribe only hot windows of the top POVs")
    ap.add_argument("--top", type=int, default=8, help="How many top POVs to transcribe")
    ap.add_argument("--min-activity", type=float, default=0.0,
                    help="Only transcribe POVs with activity_score >= this")
    ap.add_argument("--pad-pre", type=float, default=120.0, help="Seconds kept before each hot window")
    ap.add_argument("--pad-post", type=float, default=120.0, help="Seconds kept after each hot window")
    ap.add_argument("--case-id", default="case")
    ap.add_argument("--out", type=Path, default=Path(".tmp/triage"))
    ap.add_argument("--whisper-model", default="small")
    args = ap.parse_args(argv)

    FFMPEG, FFPROBE = _ff()
    args.out.mkdir(parents=True, exist_ok=True)

    scans = run_triage(args.media_dir, args.ocr)
    print_table(scans)
    triage_json = args.out / "triage.json"
    triage_json.write_text(json.dumps([asdict(s) for s in scans], indent=2), encoding="utf-8")
    print(f"\n[triage] {triage_json}")

    if not args.transcribe_hot:
        return 0

    # Select POVs to transcribe: top-N by activity, above the floor.
    selected = [s for s in scans if s.activity_score >= args.min_activity][: args.top]
    tdir = args.out / "transcripts"; tdir.mkdir(parents=True, exist_ok=True)
    print(f"\n[transcribe-hot] {len(selected)} POV(s); pad -{args.pad_pre:.0f}s/+{args.pad_post:.0f}s")
    from faster_whisper import WhisperModel
    model = WhisperModel(args.whisper_model, device="cpu", compute_type="int8")
    total_hot = 0.0
    for s in selected:
        windows = pad_merge(s.hot_windows, s.duration_sec, args.pad_pre, args.pad_post)
        hot = sum(e - st for st, e in windows)
        total_hot += hot
        print(f"  {s.name:12s} {hot/60:5.1f} min hot / {s.duration_sec/60:5.1f} min total "
              f"({len(windows)} window(s))")
        tr = transcribe_windows(s.path, windows, model, args.case_id, s.evidence_type, s.duration_sec)
        out = tdir / f"{args.case_id}_{s.name.lower().replace('-', '')}_transcript.json"
        out.write_text(json.dumps(tr, indent=2, ensure_ascii=False), encoding="utf-8")
    full = sum(s.duration_sec for s in selected)
    print(f"\n[transcribe-hot] {total_hot/60:.1f} min transcribed vs {full/60:.1f} min full "
          f"({100*total_hot/max(full,1):.0f}% of selected) -> {tdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
