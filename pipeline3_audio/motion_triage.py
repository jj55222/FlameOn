"""GOAL_D — find the violent MOMENT by motion, not sound.

A gunshot is the most violent MOTION event in the footage: the shooter's recoil,
officers diving for cover, the victim collapsing, everyone breaking into a run.
This computes a per-frame visual motion-energy curve for a clip (decode tiny +
grayscale, NO audio) and returns the densest sustained-motion window — the
candidate moment.

Why this beats the audio impulse proxy:
  * The firing officer's own AXON camera is MUTED at the trigger-pull (the
    ~30-60 s pre-event buffer) yet is visually at peak violence. Motion-energy
    sees exactly what audio cannot.
  * A car door / magazine click is a sharp SOUND but barely moves the frame; a
    shooting moves everything. Motion rejects the false positives that fooled
    the volley ranking (garage, evidence handling).

Per-frame energy subtracts each frame's mean first, so a global exposure/cloud
shift doesn't read as motion. The "moment" is the max-area window over a few
seconds, so a single hard cut (edited evidence clips) can't win — only sustained
motion does.

Local + free (ffmpeg + numpy). Disk-safe extract-one→scan→delete from the 42 GB
zip with per-file checkpoint/resume — same harness as zip_triage.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import zip_triage as zt   # free_gb (disk guard), reused harness conventions

VIDEO_EXTS = {".mp4", ".mov", ".mkv", ".m4v", ".avi", ".wmv"}


# ---------------------------------------------------------------------------
# Motion energy (decode → numpy)
# ---------------------------------------------------------------------------

def motion_energy(media: str, fps: float = 3.0, w: int = 96, h: int = 54,
                  ffmpeg: Optional[str] = None) -> Tuple[List[float], List[float]]:
    """Decode ``media`` tiny + grayscale at ``fps`` and return ``(times, energy)``
    where ``energy[i]`` is the mean absolute inter-frame difference (0-255) after
    removing each frame's DC level. ``len(energy) == n_frames - 1``."""
    if ffmpeg is None:
        import pov_triage as pt
        if not pt.FFMPEG:
            pt.FFMPEG, pt.FFPROBE = pt._ff()
        ffmpeg = pt.FFMPEG
    cmd = [ffmpeg, "-hide_banner", "-loglevel", "error", "-i", media, "-an",
           "-vf", f"fps={fps},scale={w}:{h},format=gray", "-f", "rawvideo", "-"]
    proc = subprocess.run(cmd, capture_output=True)
    buf = proc.stdout
    frame_bytes = w * h
    n = len(buf) // frame_bytes
    if n < 2:
        return [], []
    arr = np.frombuffer(buf[: n * frame_bytes], dtype=np.uint8).astype(np.float32)
    arr = arr.reshape(n, h * w)
    arr -= arr.mean(axis=1, keepdims=True)          # drop global brightness shifts
    diff = np.abs(arr[1:] - arr[:-1]).mean(axis=1)   # per-frame motion (len n-1)
    times = [round((i + 1) / fps, 3) for i in range(n - 1)]
    return times, [round(float(x), 3) for x in diff]


# ---------------------------------------------------------------------------
# Pure analysis (zero-decode, unit-tested)
# ---------------------------------------------------------------------------

def densest_motion(times: List[float], energy: List[float],
                   win_sec: float = 6.0) -> Dict:
    """The ``win_sec`` window with the most summed motion — the sustained violent
    burst (pure, O(n) two-pointer). Returns ``{start, area, peak, mean, salience}``.

    ``salience`` = the window's mean motion / the clip's overall mean motion: how
    much the moment stands out from THIS camera's own baseline. It is the rank key,
    not raw ``area`` — raw motion is dominated by camera ego-motion (a hand-panned
    evidence walkthrough out-moves a vehicle dashcam in a pursuit), so absolute
    motion mis-ranks cameras; contrast-against-baseline finds a camera that was
    calm and then suddenly violent. A lone cut can't win: the score integrates
    several seconds."""
    if not energy:
        return {"start": None, "area": 0.0, "peak": 0.0, "mean": 0.0, "salience": 0.0}
    t = np.asarray(times, dtype=float)
    e = np.asarray(energy, dtype=float)
    csum = np.concatenate([[0.0], np.cumsum(e)])
    best = {"start": float(t[0]), "area": -1.0, "peak": 0.0, "n": 1}
    j = 0
    for i in range(len(e)):
        if j < i:
            j = i
        while j < len(t) and t[j] < t[i] + win_sec:
            j += 1
        area = float(csum[j] - csum[i])
        if area > best["area"]:
            best = {"start": round(float(t[i]), 2), "area": round(area, 2),
                    "peak": round(float(e[i:j].max()) if j > i else 0.0, 2),
                    "n": max(1, j - i)}
    overall = float(e.mean()) + 1e-9
    best["salience"] = round((best["area"] / best["n"]) / overall, 2)
    best["mean"] = round(overall, 3)
    del best["n"]
    return best


def downsample(times: List[float], energy: List[float], k: int = 60) -> List[List[float]]:
    """Coarse ``[[t, energy], ...]`` profile (≤k points) for storage/plots (pure)."""
    if not energy:
        return []
    e = np.asarray(energy, dtype=float)
    t = np.asarray(times, dtype=float)
    if len(e) <= k:
        return [[round(float(a), 2), round(float(b), 3)] for a, b in zip(t, e)]
    idx = np.linspace(0, len(e) - 1, k).astype(int)
    bins = np.array_split(np.arange(len(e)), k)
    out = []
    for b in bins:
        if len(b):
            out.append([round(float(t[b[0]]), 2), round(float(e[b].max()), 3)])
    return out


def motion_profile(media: str, fps: float = 3.0, win_sec: float = 6.0,
                   ffmpeg: Optional[str] = None) -> Dict:
    """Full motion read of one clip: the densest window + the FULL energy series
    (kept at ``fps`` so motion_sync can align cameras sub-second; times are
    implicit, ``t[i] = (i + 1) / fps``)."""
    times, energy = motion_energy(media, fps=fps, ffmpeg=ffmpeg)
    d = densest_motion(times, energy, win_sec=win_sec)
    return {
        "duration_sec": round(times[-1], 1) if times else 0.0,
        "fps": fps,
        "motion_window": d,                 # {start, area, peak, mean, salience}
        "motion_salience": d["salience"],   # rank key — contrast vs the cam's baseline
        "motion_area": d["area"],
        "energy": energy,                   # full series at `fps` (for motion_sync)
    }


# ---------------------------------------------------------------------------
# Disk-safe triage from a zip (checkpoint/resume)
# ---------------------------------------------------------------------------

def triage_motion(zip_path: str, members: List[str], fps: float = 3.0,
                  win_sec: float = 6.0, min_free_gb: float = 3.0,
                  tmp_dir: str = ".tmp/_motion_triage",
                  checkpoint_path: Optional[str] = None) -> List[Dict]:
    import pov_triage as pt
    pt.FFMPEG, pt.FFPROBE = pt._ff()
    z = zipfile.ZipFile(zip_path)
    sizes = {i.filename: i.file_size for i in z.infolist()}
    tmp = Path(tmp_dir); tmp.mkdir(parents=True, exist_ok=True)

    results: List[Dict] = []
    done: set = set()
    if checkpoint_path and Path(checkpoint_path).exists():
        try:
            results = json.loads(Path(checkpoint_path).read_text(encoding="utf-8"))
            done = {r.get("member") for r in results}
            if done:
                sys.stderr.write(f"[motion] resume: {len(done)} done\n")
        except (ValueError, OSError):
            results, done = [], set()

    def flush() -> None:
        if checkpoint_path:
            p = Path(checkpoint_path); p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(results, indent=2), encoding="utf-8")

    for k, m in enumerate(members):
        if m in done:
            continue
        size_mb = sizes.get(m, 0) / 1e6
        if zt.free_gb(tmp_dir) < max(min_free_gb, size_mb / 1000 + 1):
            sys.stderr.write(f"[motion] ABORT: low disk before {m}\n")
            break
        local = tmp / Path(m).name
        sys.stderr.write(f"  [{k+1}/{len(members)}] {m} ({size_mb:.0f} MB) ...\n")
        sys.stderr.flush()
        try:
            with z.open(m) as s, open(local, "wb") as d:
                shutil.copyfileobj(s, d, 1 << 20)
            prof = motion_profile(str(local), fps=fps, win_sec=win_sec, ffmpeg=pt.FFMPEG)
            prof.update({"member": m, "size_mb": round(size_mb, 1)})
            results.append(prof)
            done.add(m)
            flush()
        except Exception as e:   # noqa: BLE001 — one bad clip shouldn't kill the run
            sys.stderr.write(f"     skip ({type(e).__name__}: {str(e)[:80]})\n")
        finally:
            if local.exists():
                local.unlink()

    results.sort(key=lambda r: -r.get("motion_salience", 0))  # most SALIENT moment first
    flush()
    return results


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="GOAL_D — rank POVs by visual MOTION (sound-free)")
    ap.add_argument("--zip", required=True)
    ap.add_argument("--members", nargs="*", default=None)
    ap.add_argument("--triage", default=None,
                    help="select candidates from a zip_triage_full.json (top --top-n by activity)")
    ap.add_argument("--top-n", type=int, default=40)
    ap.add_argument("--min-size-mb", type=float, default=15.0)
    ap.add_argument("--fps", type=float, default=3.0)
    ap.add_argument("--win-sec", type=float, default=6.0)
    ap.add_argument("--min-free-gb", type=float, default=3.0)
    ap.add_argument("--out", default=".tmp/ois_289964/motion_triage.json")
    args = ap.parse_args(argv)

    z = zipfile.ZipFile(args.zip)
    names_sizes = [(i.filename, i.file_size) for i in z.infolist() if not i.is_dir()]
    if args.members:
        want = set(args.members)
        members = [n for n, _ in names_sizes
                   if n in want or os.path.basename(n) in want]
    elif args.triage:
        tri = json.loads(Path(args.triage).read_text(encoding="utf-8"))
        members = [r["member"] for r in tri[:args.top_n]]
    else:
        vids = [(n, s) for n, s in names_sizes
                if Path(n).suffix.lower() in VIDEO_EXTS and s >= args.min_size_mb * 1e6]
        vids.sort(key=lambda x: -x[1])
        members = [n for n, _ in vids[:args.top_n]]

    print(f"[motion] {len(members)} member(s); free disk {zt.free_gb():.1f} GB")
    results = triage_motion(args.zip, members, fps=args.fps, win_sec=args.win_sec,
                            min_free_gb=args.min_free_gb, checkpoint_path=args.out)
    print(f"\n[motion] DONE. Most SALIENT moments (contrast vs each cam's baseline):")
    for r in results[:12]:
        w = r["motion_window"]
        print(f"  {Path(r['member']).name:12s} salience={r.get('motion_salience', 0):5.1f} "
              f"peak={w['peak']:6.1f} @ {w['start']}s  ({r['duration_sec']/60:.0f}min)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
