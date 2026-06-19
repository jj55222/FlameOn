"""P3.5 / GOAL_D D3 — stamp dashcams by audio cross-correlation.

Dashcams have no burned-in clock, so OCR can't time them. But a dashcam at the
scene shares loud acoustic events (radio traffic, sirens, shouted commands,
the OIS) with the clocked bodycams. This aligns each un-timed dashcam to the
best-matching clocked bodycam by cross-correlating their **energy envelopes**
(robust to different mics — it's the shared loud *events* that line up), then
derives the dashcam's absolute start time from the bodycam's known one.

Best-effort + guarded: a match is accepted only when the correlation peak is
strong AND the derived time is same-day and within a few hours of the incident
anchor. Otherwise the dashcam stays unstamped (D1 keeps it kind-inferred).

    python pipeline3_audio/timeline_align_dashcam.py \
        --artifacts .tmp/sac_poc/timeline/artifacts.json

Updates artifacts.json in place (timestamp_source="audio_xcorr"). Global Python.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pov_triage as pt  # noqa: E402

ENV_HZ = 10                 # energy-envelope sample rate (100 ms windows)
DECODE_SR = 8000
MIN_PEAK_Z = 6.0            # peak must stand this many sigma above the rest
SANITY_WINDOW_SEC = 4 * 3600   # derived time must be within this of the anchor


def envelope(path: str) -> np.ndarray:
    a = pt.decode_audio(path, DECODE_SR)
    n = int(DECODE_SR / ENV_HZ)
    if len(a) < n * 4:
        return np.zeros(0, dtype=np.float32)
    w = a[: len(a) // n * n].reshape(-1, n)
    env = np.log1p(np.sqrt((w ** 2).mean(axis=1) + 1e-9))
    env = (env - env.mean()) / (env.std() + 1e-9)
    return env.astype(np.float32)


def best_lag(ref: np.ndarray, sig: np.ndarray) -> Tuple[int, float]:
    """Cross-correlate; return (lag_samples, peak_z). Convention: ref[n] aligns
    with sig[n - lag], so derived dashcam_start = bodycam_start + lag/ENV_HZ."""
    if len(ref) < 4 or len(sig) < 4:
        return 0, 0.0
    cc = np.correlate(ref, sig, mode="full")            # length len(ref)+len(sig)-1
    lags = np.arange(-(len(sig) - 1), len(ref))
    k = int(cc.argmax())
    z = float((cc[k] - cc.mean()) / (cc.std() + 1e-9))
    return int(lags[k]), z


def align(arts: List[Dict]) -> Tuple[int, List[str]]:
    clocked = [a for a in arts if a.get("start_epoch") and a.get("timestamp_source") == "axon_ocr"]
    if not clocked:
        return 0, ["no clocked bodycams to align against"]
    anchor = min(a["start_epoch"] for a in clocked)
    bodycam_env = {a["artifact_id"]: (envelope(a["path"]), a["start_epoch"]) for a in clocked}

    notes: List[str] = []
    stamped = 0
    for a in arts:
        if a["kind"] != "dashcam" or (a.get("start_epoch") and a.get("timestamp_source") == "audio_xcorr"):
            continue
        env_d = envelope(a["path"])
        if env_d.size == 0:
            continue
        best = None
        for bid, (env_b, bep) in bodycam_env.items():
            if env_b.size == 0:
                continue
            lag, z = best_lag(env_b, env_d)
            derived = bep + lag / ENV_HZ
            if best is None or z > best[2]:
                best = (bid, lag, z, derived)
        if best is None:
            continue
        bid, lag, z, derived = best
        same_day = dt.datetime.fromtimestamp(derived, dt.timezone.utc).date() == \
            dt.datetime.fromtimestamp(anchor, dt.timezone.utc).date()
        in_window = abs(derived - anchor) <= SANITY_WINDOW_SEC
        if z >= MIN_PEAK_Z and same_day and in_window:
            a["start_epoch"] = derived
            a["end_epoch"] = derived + (a.get("duration_sec") or 0)
            a["start_iso"] = dt.datetime.fromtimestamp(derived, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            a["timestamp_source"] = "audio_xcorr"
            a["timestamp_confidence"] = round(min(0.75, z / 20), 2)
            a["aligned_to"] = bid
            stamped += 1
            notes.append(f"{a['artifact_id']} -> {a['start_iso']} (match {bid}, z={z:.1f})")
        else:
            notes.append(f"{a['artifact_id']} -> no confident match "
                         f"(best {bid} z={z:.1f}, same_day={same_day}, in_window={in_window})")
    return stamped, notes


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="GOAL_D D3 — align dashcams by audio xcorr")
    ap.add_argument("--artifacts", required=True, type=Path)
    args = ap.parse_args(argv)
    pt.FFMPEG, pt.FFPROBE = pt._ff()

    arts = json.loads(args.artifacts.read_text(encoding="utf-8"))
    stamped, notes = align(arts)
    for n in notes:
        print("  " + n)
    args.artifacts.write_text(json.dumps(arts, indent=2), encoding="utf-8")
    print(f"\n[D3] stamped {stamped} dashcam(s) by audio_xcorr -> {args.artifacts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
