"""
av_transients.py — impulsive-onset (gunshot / bang) detection + beat snapping.

Why: beat_miner grounds moments on TRANSCRIPT lines, but a gunshot is non-verbal —
so beats land on people *talking about* shots (callouts, retellings), never on the
bang. On Iona the operator caught the cut starting AFTER the gunfire ("cut to was
post the shots / car door"). This module finds the impulsive audio onset near a
beat's clip and nudges its in-point so the bang lands ~2-4s after the clip starts.

Pure stdlib DSP (short-window RMS energy-flux) + an ffmpeg decode for real media;
no numpy, no new heavy deps. Opt-in: nothing here runs unless --snap-transients is
passed. Detection is a cheap, robust energy-rise detector — good enough to place a
gunshot to within a beat, not a forensic classifier.
"""
from __future__ import annotations

import array
import math
import re
import statistics
import subprocess
import wave
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

# Beats eligible for snapping: the salient ones whose in-point a bang should anchor.
SNAP_FUNCTIONS = {"reveal", "emotional_peak", "peak", "tension_shift"}

# Force-ONSET cues (a bang is imminent), NOT bare "shot"/"shooting" which recur in
# medical aftermath — same discipline as the P6 PEAK detector (see STATE.md).
FORCE_ONSET_CUES = re.compile(
    r"shots?\s+fired|shots?\s+out|one\s+shot|drop\s+the\s+(?:gun|knife|weapon)|drop\s+it|"
    r"gun\s+gun|he'?s\s+got\s+a\s+gun|got\s+a\s+gun|open(?:ed)?\s+fire|\btaser\b|\bgunfire\b|"
    r"shots?\s+fired\s+shots?\s+fired", re.I)


# ---------------------------------------------------------------------------
# Decode
# ---------------------------------------------------------------------------

def read_wav(path) -> Tuple[List[float], int]:
    """Read a PCM wav → (mono float samples in [-1,1], sample_rate). Stdlib only
    (no ffmpeg) so tests can synthesize a wav and exercise the full detector."""
    with wave.open(str(path), "rb") as w:
        n, sw, ch, sr = w.getnframes(), w.getsampwidth(), w.getnchannels(), w.getframerate()
        raw = w.readframes(n)
    if sw != 2:                                   # only 16-bit PCM (what we decode to)
        raise ValueError(f"unsupported sample width {sw*8}-bit; expected 16-bit PCM")
    a = array.array("h")
    a.frombytes(raw)
    if ch > 1:                                    # downmix to mono
        a = array.array("h", [sum(a[i:i + ch]) // ch for i in range(0, len(a), ch)])
    return [v / 32768.0 for v in a], sr


def decode_pcm(media_path, start_sec: float, dur_sec: float, sr: int = 8000) -> Optional[List[float]]:
    """ffmpeg-decode a window of ANY media to mono 16-bit PCM → float samples.
    Returns None if ffmpeg is missing or the decode fails (snapping then no-ops)."""
    cmd = ["ffmpeg", "-nostdin", "-v", "error", "-ss", f"{max(0.0, start_sec):.3f}",
           "-t", f"{max(0.0, dur_sec):.3f}", "-i", str(media_path),
           "-ac", "1", "-ar", str(sr), "-f", "s16le", "pipe:1"]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=60)
    except (FileNotFoundError, subprocess.SubprocessError):
        return None
    if r.returncode != 0 or not r.stdout:
        return None
    a = array.array("h")
    a.frombytes(r.stdout[: len(r.stdout) - (len(r.stdout) % 2)])
    return [v / 32768.0 for v in a]


# ---------------------------------------------------------------------------
# Detect
# ---------------------------------------------------------------------------

def _frame_rms(samples: Sequence[float], sr: int, win_s: float, hop_s: float) -> List[Tuple[float, float]]:
    wl, hl = max(1, int(sr * win_s)), max(1, int(sr * hop_s))
    out: List[Tuple[float, float]] = []
    for i in range(0, len(samples), hl):
        frame = samples[i:i + wl]
        if not frame:
            break
        out.append((i / sr, math.sqrt(sum(v * v for v in frame) / len(frame))))
    return out


def detect_transients(samples: Sequence[float], sr: int, *, win_ms: float = 20.0,
                      hop_ms: float = 10.0, k: float = 3.0, min_strength: float = 0.02,
                      min_gap_ms: float = 80.0) -> List[Tuple[float, float]]:
    """Impulsive onsets as (time_sec, strength), strongest first.

    Onset = a positive spike in short-window RMS (energy flux) that is a local
    maximum and exceeds BOTH ``mean + k*std`` of the flux AND an absolute
    ``min_strength`` floor. The floor is what keeps ambient room noise from ever
    registering a bang (its flux is tiny); a real gunshot is a large RMS step."""
    frames = _frame_rms(samples, sr, win_ms / 1000.0, hop_ms / 1000.0)
    if len(frames) < 3:
        return []
    flux = [(frames[i][0], max(0.0, frames[i][1] - frames[i - 1][1])) for i in range(1, len(frames))]
    vals = [f for _, f in flux]
    thr = max(min_strength, statistics.fmean(vals) + k * (statistics.pstdev(vals) or 1e-9))

    cands: List[Tuple[float, float]] = []
    for i, (t, f) in enumerate(flux):
        prev = flux[i - 1][1] if i > 0 else 0.0
        nxt = flux[i + 1][1] if i + 1 < len(flux) else 0.0
        if f >= thr and f >= prev and f >= nxt:
            cands.append((t, f))
    cands.sort(key=lambda c: -c[1])
    # suppress near-duplicates within min_gap (keep the stronger)
    kept: List[Tuple[float, float]] = []
    for t, f in cands:
        if all(abs(t - kt) * 1000.0 >= min_gap_ms for kt, _ in kept):
            kept.append((t, f))
    return kept


def strongest_transient(media_path, start_sec: float, end_sec: float,
                        sr: int = 8000, **kw) -> Optional[Tuple[float, float]]:
    """Strongest impulsive onset (absolute time_sec, strength) in a media window,
    or None. Absolute time = start_sec + offset within the decoded window."""
    dur = max(0.0, end_sec - start_sec)
    if dur <= 0:
        return None
    samples = decode_pcm(media_path, start_sec, dur, sr=sr)
    if not samples:
        return None
    hits = detect_transients(samples, sr, **kw)
    if not hits:
        return None
    t_off, strength = hits[0]
    return start_sec + t_off, strength


# ---------------------------------------------------------------------------
# Snap
# ---------------------------------------------------------------------------

def snap_window(media_path, in_sec: float, out_sec: float, *, search_back: float = 8.0,
                search_fwd: float = 3.0, lead_sec: float = 3.0,
                sr: int = 8000) -> Optional[Tuple[float, float]]:
    """Find the bang near a clip's in-point and return a new ``(in_sec, out_sec)``
    so the transient lands ``lead_sec`` (2-4s) after the clip starts, preserving
    duration. Returns None when no clear transient is found (leave the beat as-is).

    The bang is typically BEFORE the speech-grounded in-point, so we search a window
    that reaches back before it."""
    lo = max(0.0, in_sec - search_back)
    hi = max(lo, min(out_sec, in_sec + search_fwd))
    hit = strongest_transient(media_path, lo, hi, sr=sr)
    if not hit:
        return None
    t, _strength = hit
    new_in = max(0.0, t - lead_sec)
    delta = new_in - in_sec
    return round(new_in, 3), round(max(new_in + 0.5, out_sec + delta), 3)


def is_snap_candidate(beat: Dict) -> bool:
    """A beat is eligible if it is a salient function AND its words cite a force
    ONSET (a bang is imminent) — so we only move clips that should open on impact."""
    if beat.get("function") not in SNAP_FUNCTIONS:
        return False
    text = " ".join([(beat.get("quote") or {}).get("text", ""),
                     (beat.get("narration_bridge") or {}).get("text", ""),
                     str(beat.get("description") or "")])
    return bool(FORCE_ONSET_CUES.search(text))


def snap_beats(beats: List[Dict], resolve_path: Callable[[str], Optional[str]],
               *, lead_sec: float = 3.0, sr: int = 8000, **kw) -> List[Dict]:
    """Snap every eligible beat's primary_asset in/out to its audio transient.
    MUTATES beats in place; returns the list of beats that were moved (for logging).
    ``resolve_path(asset_id)`` maps an asset id to a decodable media path or None."""
    moved: List[Dict] = []
    for b in beats:
        pa = b.get("primary_asset")
        if not pa or not is_snap_candidate(b):
            continue
        path = resolve_path(pa.get("asset_id", ""))
        if not path or not Path(path).exists():
            continue
        snapped = snap_window(path, float(pa.get("in_sec", 0.0)), float(pa.get("out_sec", 0.0)),
                              lead_sec=lead_sec, sr=sr, **kw)
        if not snapped:
            continue
        old_in = pa.get("in_sec")
        pa["in_sec"], pa["out_sec"] = snapped
        b["_snapped_from_in_sec"] = old_in
        moved.append(b)
    return moved
