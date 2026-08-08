"""Zero-network tests for the YT-comment salience pipeline (pure core).

No yt-dlp, no network: parse timestamps, weight, bin into a curve, pick top
windows, and calibrate against predicted moments — all with fixtures.
"""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import yt_salience as ys  # noqa: E402


# --- timestamp parsing ------------------------------------------------------

def test_parse_timestamp_formats():
    assert ys.parse_timestamp("0:58 wow") == [58]
    assert ys.parse_timestamp("at 12:34 he says") == [12 * 60 + 34]
    assert ys.parse_timestamp("1:02:03 is the part") == [3723]


def test_parse_timestamp_multiple_and_none():
    assert ys.parse_timestamp("0:10 then 1:20 again") == [10, 80]
    assert ys.parse_timestamp("no timestamps here") == []


def test_parse_timestamp_rejects_non_timestamps():
    assert ys.parse_timestamp("score was 5:1") == []      # seconds must be [0-5]\d
    assert ys.parse_timestamp("call 5551234") == []        # embedded digits, no colon pair


def test_comment_weight_monotonic():
    assert ys.comment_weight(0) == 1.0
    assert ys.comment_weight(100) > ys.comment_weight(10) > ys.comment_weight(0)


# --- flatten + noise --------------------------------------------------------

def test_parse_comment_timestamps_weights_and_caps():
    comments = [
        {"text": "0:58 the dog!!", "like_count": 0},
        {"text": "chapters 0:00 1:00 2:00 3:00", "like_count": 9},   # capped at 3
        {"text": "first!", "like_count": 500},                        # noise -> dropped
        {"text": "no time", "like_count": 4},
    ]
    out = ys.parse_comment_timestamps(comments, max_per_comment=3)
    secs = sorted(s["sec"] for s in out)
    assert secs == [0, 58, 60, 120]            # 0:58 + first 3 of the chapter list
    assert all(s["weight"] >= 1.0 for s in out)


def test_noise_comment_dropped():
    assert ys.parse_comment_timestamps([{"text": "who's here in 2024 1:23", "like_count": 0}]) == []


# --- curve ------------------------------------------------------------------

def test_salience_curve_bins_and_includes_zeros():
    stamps = [{"sec": 2, "weight": 1.0}, {"sec": 3, "weight": 2.0}, {"sec": 58, "weight": 1.5}]
    curve = ys.salience_curve(stamps, duration_sec=60, bin_sec=5.0)
    assert len(curve) == 12
    assert curve[0]["weight"] == 3.0 and curve[0]["n"] == 2     # secs 2,3 -> bin 0
    assert curve[11]["weight"] == 1.5                            # sec 58 -> bin 11
    assert curve[5]["weight"] == 0.0                             # empty bin still present


def test_salience_curve_empty_duration():
    assert ys.salience_curve([{"sec": 1, "weight": 1}], 0) == []


# --- top windows ------------------------------------------------------------

def test_top_salient_windows_picks_peaks_and_pads():
    curve = [
        {"start": 0, "end": 5, "weight": 0.0, "n": 0},
        {"start": 5, "end": 10, "weight": 8.0, "n": 4},     # a peak
        {"start": 10, "end": 15, "weight": 0.5, "n": 1},
        {"start": 100, "end": 105, "weight": 6.0, "n": 3},  # another peak, far away
    ]
    wins = ys.top_salient_windows(curve, k=2, pad_sec=5.0, min_weight=1.0)
    assert len(wins) == 2
    assert wins[0]["start"] == 0.0 and wins[0]["end"] == 15.0   # 5-10 padded
    assert wins[1]["start"] == 95.0


def test_top_windows_merge_adjacent():
    curve = [
        {"start": 10, "end": 15, "weight": 5.0, "n": 2},
        {"start": 18, "end": 23, "weight": 4.0, "n": 2},   # within merge_gap after padding
    ]
    wins = ys.top_salient_windows(curve, k=5, pad_sec=2.0, min_weight=1.0, merge_gap_sec=10.0)
    assert len(wins) == 1
    assert wins[0]["weight"] == 9.0 and wins[0]["n"] == 4


# --- calibration ------------------------------------------------------------

def test_align_to_predicted_recall():
    salient = [{"start": 50, "end": 60, "weight": 5, "n": 3},
               {"start": 200, "end": 210, "weight": 4, "n": 2}]
    predicted = [{"timestamp_sec": 55}]            # hits the first window only
    cal = ys.align_to_predicted(salient, predicted, tol_sec=8.0)
    assert cal["salient_windows"] == 2
    assert cal["covered_by_prediction"] == 1
    assert cal["missed"] == 1
    assert cal["recall"] == 0.5
