"""Zero-network / zero-ffmpeg tests for the P6 cold-open climax-lift (D6).

``_is_video`` only inspects a path's suffix and ``build_paper_edit`` only reads
media durations via ``_duration`` — which is monkeypatched to a constant here —
so the whole paper-edit assembly runs without media files or ffmpeg.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import render_rough_cut as r  # noqa: E402


@pytest.fixture(autouse=True)
def _stub_duration(monkeypatch):
    """No ffprobe: every clip is 'long enough'."""
    monkeypatch.setattr(r, "_duration", lambda p: 600.0)


def _src(idx, name, etype="bodycam"):
    return r.Source(source_idx=idx, media_path=Path(f"{name}.mp4"),
                    evidence_type=etype, label=name)


def _moment(idx, importance, mtype, ts):
    return {"source_idx": idx, "importance": importance, "moment_type": mtype,
            "timestamp_sec": ts, "end_timestamp_sec": ts + 10,
            "description": f"{mtype} at {ts}", "transcript_excerpt": "quote"}


SOURCES = [_src(0, "BWC-1a"), _src(1, "BWC-2"), _src(2, "BWC-5a")]
MOMENTS = [
    _moment(0, "high", "reveal", 180),
    _moment(2, "critical", "emotional_peak", 85),     # the strongest hook
    _moment(1, "critical", "procedural_violation", 240),
    _moment(0, "medium", "tension_shift", 235),
]


def _verdict(moments=MOMENTS):
    return {"case_id": "vasquez_23117201", "verdict": "HOLD",
            "key_moments": moments, "transcript_refs": ["a", "b", "c"]}


# --- _pick_climax -----------------------------------------------------------

def test_pick_climax_prefers_critical_emotional_peak():
    m = r._pick_climax(MOMENTS, SOURCES)
    assert m is not None
    # two 'critical' moments; emotional_peak outranks procedural_violation
    assert m["moment_type"] == "emotional_peak"
    assert m["source_idx"] == 2


def test_pick_climax_skips_moments_without_video():
    # only the medium tension_shift maps to video; the criticals point nowhere
    audio_only_sources = [
        r.Source(0, None, "911_audio", "911"),
        r.Source(1, None, "911_audio", "911"),
        _src(2, "BWC-5a"),
    ]
    moments = [_moment(0, "critical", "emotional_peak", 85),
               _moment(2, "medium", "tension_shift", 200)]
    m = r._pick_climax(moments, audio_only_sources)
    assert m is not None and m["source_idx"] == 2   # the only playable one


def test_pick_climax_none_when_no_video():
    audio = [r.Source(0, None, "911_audio", "911")]
    assert r._pick_climax([_moment(0, "critical", "emotional_peak", 85)], audio) is None


# --- build_paper_edit cold-open modes ---------------------------------------

def _kinds(pe):
    return [(e["kind"], e.get("beat_role") or e.get("card_kind")) for e in pe["timeline"]]


def test_scene_set_is_default_and_opens_with_title_then_broll():
    pe = r.build_paper_edit(_verdict(), SOURCES, "Agency", [], cold_open="scene_set")
    seq = _kinds(pe)
    assert seq[0] == ("card", "title")
    # B-roll hook clips follow the title (scene_set behaviour, unchanged)
    assert ("clip", "hook") in seq[1:3]
    assert not any(e.get("beat_role") == "cold_open" for e in pe["timeline"])


def test_climax_puts_cold_open_clip_before_title():
    pe = r.build_paper_edit(_verdict(), SOURCES, "Agency", [], cold_open="climax")
    first = pe["timeline"][0]
    assert first["kind"] == "clip"
    assert first["beat_role"] == "cold_open"
    assert first["moment_type"] == "emotional_peak"
    assert first["cold_open"] is True
    # title card comes right after the tease
    assert pe["timeline"][1]["kind"] == "card"
    assert pe["timeline"][1]["card_kind"] == "title"
    # climax mode drops the scene_set B-roll hook
    assert not any(e.get("moment_type") == "scene_set" for e in pe["timeline"])


def test_climax_tease_is_short_and_within_media():
    pe = r.build_paper_edit(_verdict(), SOURCES, "Agency", [], cold_open="climax")
    teaser = pe["timeline"][0]
    length = teaser["out_sec"] - teaser["in_sec"]
    assert 0 < length <= r.COLD_OPEN_TEASE_SEC + 0.01


def test_none_mode_has_no_cold_open_and_no_broll():
    pe = r.build_paper_edit(_verdict(), SOURCES, "Agency", [], cold_open="none")
    assert pe["timeline"][0] == {"kind": "card", "card_kind": "title",
                                 "title": "Case 23117201", "subtitle": "Agency",
                                 "dur": r.TITLE_SEC}
    assert not any(e.get("beat_role") == "cold_open" for e in pe["timeline"])
    assert not any(e.get("moment_type") == "scene_set" for e in pe["timeline"])


def test_climax_falls_back_when_no_video_moment():
    # all-audio sources: climax can't find a video hook -> behaves like 'none'
    audio = [r.Source(0, None, "911_audio", "911")]
    v = {"case_id": "x_1", "verdict": "HOLD", "transcript_refs": ["a"],
         "key_moments": [_moment(0, "critical", "emotional_peak", 85)]}
    pe = r.build_paper_edit(v, audio, "Agency", [], cold_open="climax")
    assert pe["timeline"][0]["kind"] == "card"   # title, no cold-open clip
