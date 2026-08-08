"""Pure-numpy tests for motion_sync: cross-correlation alignment recovers the
recording offsets between cameras that share a motion event, and the stacked
peak lands on that shared moment."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import motion_sync as ms  # noqa: E402


def _bump(n, center, amp=6.0, width=3.0, phase=0.0):
    """A shared reaction spike at ``center`` plus a little per-camera texture."""
    t = np.arange(n)
    e = amp * np.exp(-0.5 * ((t - center) / width) ** 2) + 0.25 * np.sin(0.3 * t + phase)
    return list(e)


def test_xcorr_recovers_shift():
    a = _bump(200, 100, phase=0.0)
    b = _bump(200, 80, phase=1.3)            # b's spike sits 20 samples earlier
    lag, score = ms.xcorr(a, b)
    assert score > 0.3
    assert abs((80 + lag) - 100) <= 2        # adding lag to b lines it up with a


def test_align_offsets_brings_events_into_coincidence():
    centers = [100, 80, 130]
    series = [_bump(220, c, phase=i * 0.7) for i, c in enumerate(centers)]
    off = ms.align_offsets(series, min_score=0.3)
    assert all(o is not None for o in off)
    aligned = [c + o for c, o in zip(centers, off)]
    assert max(aligned) - min(aligned) <= 2  # all spikes coincide on the common axis


def test_unrelated_series_is_not_aligned():
    centers = [100, 105]                      # two co-located
    series = [_bump(220, c, phase=i) for i, c in enumerate(centers)]
    series.append(list(0.2 * np.sin(0.11 * np.arange(220))))  # pure noise, no spike
    off = ms.align_offsets(series, min_score=0.5)
    assert off[0] is not None and off[1] is not None
    assert off[2] is None                    # the unrelated camera stays unplaced


def test_sync_moment_reports_local_timecodes():
    centers = [100, 80, 130]
    recs = [{"member": f"{i}.mp4", "fps": 4.0, "energy": _bump(220, c, phase=i * 0.7)}
            for i, c in enumerate(centers)]
    out = ms.sync_moment(recs, min_score=0.3, min_cams=2)
    assert out["n_aligned"] == 3
    assert out["moment"]["coverage_max"] == 3
    # each camera's LOCAL moment index (= local_sec * fps) is its own spike center
    for c, cam in zip(centers, out["cameras"]):
        assert abs(cam["local_sec"] * 4.0 - c) <= 2
