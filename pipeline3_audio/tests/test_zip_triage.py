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
