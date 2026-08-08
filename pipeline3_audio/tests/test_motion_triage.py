"""Zero-decode tests for motion_triage pure analysis (densest window, downsample)."""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import motion_triage as mt  # noqa: E402


def _times(n, fps=4.0):
    return [round((i + 1) / fps, 3) for i in range(n)]


def test_densest_motion_finds_sustained_burst():
    energy = [0.1] * 40
    for i in range(20, 30):
        energy[i] = 3.0                     # sustained motion ~5.25..7.5 s
    d = mt.densest_motion(_times(40), energy, win_sec=2.0)
    assert 5.0 <= d["start"] <= 6.0
    assert d["peak"] == 3.0
    assert d["salience"] > 2.0              # the burst stands well above baseline


def test_densest_motion_salience_low_when_always_moving():
    # a hand-panned walkthrough: motion is high but UNIFORM → low salience
    d = mt.densest_motion(_times(40), [3.0] * 40, win_sec=2.0)
    assert d["salience"] < 1.5              # nothing stands out from the baseline


def test_densest_motion_prefers_sustained_over_brief():
    energy = [0.1] * 40
    energy[5] = 3.0                          # lone brief spike (same magnitude)
    for i in range(20, 30):
        energy[i] = 3.0                      # sustained burst
    d = mt.densest_motion(_times(40), energy, win_sec=2.0)
    assert d["start"] >= 4.0                 # the sustained region wins, not t≈1.5 s


def test_densest_motion_empty():
    assert mt.densest_motion([], [])["start"] is None


def test_downsample_caps_points():
    times = [round(i * 0.1, 1) for i in range(500)]
    energy = [float(i % 7) for i in range(500)]
    ds = mt.downsample(times, energy, k=50)
    assert len(ds) <= 50 and all(len(p) == 2 for p in ds)
