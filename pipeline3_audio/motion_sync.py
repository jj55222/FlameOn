"""GOAL_D — synchronize cameras with NO clock: "everyone flinches at once".

The gunshot is ONE event recorded by many co-located cameras. Even with the AXON
clock stripped and the dashcam overlay absent, the cameras can be put on a common
timeline by the SHARED VISUAL REACTION: cross-correlate each camera's motion-energy
curve (from motion_triage) and the lag that lines up the synchronized
recoil/duck/run reveals the recording-start offset between them — exactly like
audio sync, but immune to the muted buffer that silences the firing officer's cam.

Once aligned, the SHOOTING is the instant of maximum summed motion across cameras
(the moment the most cameras react at once). A by-product is which cameras are
co-located (they cross-correlate strongly) and, for each, the LOCAL timecode of the
moment — i.e. exactly where to look in that file.

Pure numpy — no video, no network. Consumes motion_triage records (``energy`` +
``fps``).
"""
from __future__ import annotations

import itertools
from typing import Dict, List, Optional, Sequence

import numpy as np


def zscore(x: Sequence[float]) -> np.ndarray:
    a = np.asarray(x, dtype=float)
    s = a.std()
    return (a - a.mean()) / s if s > 1e-9 else a * 0.0


def xcorr(a: Sequence[float], b: Sequence[float],
          max_lag: Optional[int] = None) -> (int, float):
    """Best alignment lag of ``b`` onto ``a`` and its score in ~[0,1].
    ``lag`` is the integer shift to ADD to b's indices to line shared events up
    with a (so a's event index ≈ b's event index + lag). Both inputs are
    z-scored, so the score reflects shape agreement, not amplitude."""
    az, bz = zscore(a), zscore(b)
    if az.size == 0 or bz.size == 0:
        return 0, 0.0
    c = np.correlate(az, bz, mode="full")           # len = La+Lb-1
    lags = np.arange(c.size) - (bz.size - 1)         # c[k] aligns at lag=lags[k]
    if max_lag is not None:
        keep = np.abs(lags) <= max_lag
        c, lags = c[keep], lags[keep]
    k = int(np.argmax(c))
    score = float(c[k]) / np.sqrt(az.size * bz.size)
    return int(lags[k]), round(score, 4)


def align_offsets(series: List[Sequence[float]], min_score: float = 0.3,
                  max_lag: Optional[int] = None) -> List[Optional[int]]:
    """Place each series on a common index axis by greedy strongest-link growth.
    Returns ``offset[i]`` in SAMPLES (add to series i's local index → common index),
    or ``None`` for a series with no strong link to the aligned set (not co-located).
    The most-connected series anchors at offset 0."""
    n = len(series)
    if n == 0:
        return []
    z = [zscore(s) for s in series]
    pair: Dict = {}
    for i, j in itertools.combinations(range(n), 2):
        lag, score = xcorr(z[i], z[j], max_lag)
        pair[(i, j)] = (lag, score)        # shift j → align with i
        pair[(j, i)] = (-lag, score)       # and the reverse
    conn = [sum(pair[(i, j)][1] for j in range(n) if j != i) for i in range(n)]
    ref = int(np.argmax(conn))
    offset: List[Optional[int]] = [None] * n
    offset[ref] = 0
    placed = {ref}
    while len(placed) < n:
        best = None                        # (score, i_placed, j_new, lag)
        for i in placed:
            for j in range(n):
                if j in placed:
                    continue
                lag, score = pair[(i, j)]
                if score >= min_score and (best is None or score > best[0]):
                    best = (score, i, j, lag)
        if best is None:
            break                          # remaining series share no event
        _, i, j, lag = best
        offset[j] = offset[i] + lag
        placed.add(j)
    return offset


def stack_peak(series: List[Sequence[float]], offsets: List[Optional[int]],
               fps: float, min_cams: int = 2) -> Dict:
    """Sum the aligned (z-scored) curves on the common axis and return the peak —
    the shared moment. Prefers instants covered by ``>= min_cams`` cameras (a true
    shared event), falling back to raw if nothing overlaps that much."""
    placed = [(zscore(s), off) for s, off in zip(series, offsets) if off is not None]
    if not placed:
        return {"common_idx": None, "t_sec": None, "value": 0.0, "n_cams": 0, "coverage_max": 0}
    lo = min(off for _, off in placed)
    hi = max(off + len(s) for s, off in placed)
    acc = np.zeros(hi - lo)
    cov = np.zeros(hi - lo)
    for s, off in placed:
        a = off - lo
        acc[a:a + len(s)] += s
        cov[a:a + len(s)] += 1
    score = np.where(cov >= min_cams, acc, -np.inf)
    if not np.isfinite(score).any():
        score = acc
    k = int(np.argmax(score))
    common_idx = k + lo
    return {"common_idx": int(common_idx), "t_sec": round(common_idx / fps, 2),
            "value": round(float(acc[k]), 2), "n_cams": int(cov[k]),
            "coverage_max": int(cov.max())}


def sync_moment(records: List[Dict], energy_key: str = "energy", fps_key: str = "fps",
                min_score: float = 0.3, max_lag_sec: Optional[float] = None,
                min_cams: int = 2) -> Dict:
    """End-to-end: align motion_triage records and locate the shared moment.
    Returns the common-axis moment plus, for each co-located camera, the LOCAL
    timecode to look at (``common_idx - offset`` back in that file)."""
    series = [r.get(energy_key) or [] for r in records]
    fps = float(records[0].get(fps_key, 3.0)) if records else 3.0
    max_lag = int(max_lag_sec * fps) if max_lag_sec else None
    offsets = align_offsets(series, min_score=min_score, max_lag=max_lag)
    peak = stack_peak(series, offsets, fps, min_cams=min_cams)
    cams = []
    for r, s, off in zip(records, series, offsets):
        if off is None or peak["common_idx"] is None:
            cams.append({"member": r.get("member"), "offset_sec": None, "local_sec": None})
            continue
        local_idx = peak["common_idx"] - off
        local_ok = 0 <= local_idx < len(s)
        cams.append({"member": r.get("member"),
                     "offset_sec": round(off / fps, 2),
                     "local_sec": round(local_idx / fps, 2) if local_ok else None})
    n_aligned = sum(1 for o in offsets if o is not None)
    return {"moment": peak, "n_aligned": n_aligned, "n_total": len(records),
            "cameras": cams}
