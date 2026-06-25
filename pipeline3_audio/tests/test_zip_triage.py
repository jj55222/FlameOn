"""Zero-network tests for zip_triage member selection (pure)."""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import zip_triage as zt  # noqa: E402


NS = [("1.mp4", 2_300_000_000), ("2.mp4", 16_000_000), ("notes.pdf", 5_000_000),
      ("3.m4v", 500_000_000), ("thumb.mp4", 200_000), ("dir/", 0)]


def test_list_video_members_filters_non_video():
    vids = dict(zt.list_video_members(NS))
    assert set(vids) == {"1.mp4", "2.mp4", "3.m4v", "thumb.mp4"}   # pdf + dir excluded


def test_select_top_by_size_picks_largest_videos():
    sel = zt.select_members(NS, top_by_size=2)
    assert sel == ["1.mp4", "3.m4v"]            # the two biggest videos, largest first


def test_select_respects_min_size():
    sel = zt.select_members(NS, top_by_size=10, min_size_mb=1.0)
    assert "thumb.mp4" not in sel               # 0.2 MB dropped
    assert "1.mp4" in sel and "3.m4v" in sel


def test_select_explicit_members_win():
    sel = zt.select_members(NS, top_by_size=2, explicit=["2.mp4"])
    assert sel == ["2.mp4"]


def test_densest_cluster_finds_the_volley():
    # scattered transients early, then a tight volley at ~500s
    times = [30, 120, 280, 497, 499, 501, 503, 505, 509, 700, 900]
    c = zt.densest_cluster(times, win_sec=25.0)
    assert c["count"] == 6              # 497..509 within 25s
    assert 497 <= c["start"] <= 499


def test_densest_cluster_empty():
    assert zt.densest_cluster([])["count"] == 0
