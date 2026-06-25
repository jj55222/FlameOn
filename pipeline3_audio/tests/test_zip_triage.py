"""Zero-network tests for zip_triage member selection (pure)."""
from __future__ import annotations

import json
import sys
import zipfile
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


def test_clock_crossref_finds_recordings_rolling_at_incident():
    import datetime as dt
    base = dt.datetime(2017, 8, 30, 11, 30, tzinfo=dt.timezone.utc).timestamp()  # 11:30
    recs = [
        {"member": "a.mp4", "start_epoch": base, "duration_sec": 3600},          # 11:30-12:30 covers noon
        {"member": "b.mp4", "start_epoch": base - 7200, "duration_sec": 600},    # 9:30-9:40, no
        {"member": "c.mp4", "start_epoch": None, "duration_sec": 100},           # unclocked fixed cam
    ]
    hits = zt.clock_crossref(recs, "2017-08-30 12:00", slack_sec=60)
    assert [h["member"] for h in hits] == ["a.mp4"]
    assert hits[0]["_offset_from_incident_sec"] == 1800   # incident 30 min into the recording


def test_clock_crossref_bad_incident_returns_empty():
    assert zt.clock_crossref([{"start_epoch": 1.0, "duration_sec": 1}], "not a date") == []


def test_triage_zip_resume_skips_done_and_checkpoints(tmp_path, monkeypatch):
    """A crashed run leaves a checkpoint; the re-run must reload it, skip the
    already-triaged member (no re-OCR), process only the new one, and persist both.
    This is the fix for the clock-OCR job that died at 109/136 and wrote nothing."""
    import pov_triage as pt
    zp = tmp_path / "case.zip"
    with zipfile.ZipFile(zp, "w") as z:
        z.writestr("old.mp4", b"x" * 16)
        z.writestr("new.mp4", b"y" * 16)
    # prior checkpoint already holds old.mp4 (sentinel proves it is NOT re-processed)
    ckpt = tmp_path / "clock.json"
    ckpt.write_text(json.dumps([{"member": "old.mp4", "is_clocked": True,
                                 "sentinel": "ORIG"}]), encoding="utf-8")

    ocr_calls = []
    monkeypatch.setattr(pt, "_ff", lambda: ("ffmpeg", "ffprobe"))
    monkeypatch.setattr(zt, "_probe_duration", lambda p: 10.0)
    monkeypatch.setattr(pt, "ocr_clock",
                        lambda path, dur: (ocr_calls.append(path) or (123.0, "wc")))

    out = zt.triage_zip(str(zp), ["old.mp4", "new.mp4"], clock_only=True,
                        tmp_dir=str(tmp_path / "_t"),
                        checkpoint_path=str(ckpt), min_free_gb=0.0)

    by = {r["member"]: r for r in out}
    assert by["old.mp4"].get("sentinel") == "ORIG"      # resumed, never re-touched
    assert by["new.mp4"]["is_clocked"] is True          # new member triaged
    assert [Path(p).name for p in ocr_calls] == ["new.mp4"]   # OCR ran once, new only
    saved = json.loads(ckpt.read_text(encoding="utf-8"))      # checkpoint persisted both
    assert {r["member"] for r in saved} == {"old.mp4", "new.mp4"}
