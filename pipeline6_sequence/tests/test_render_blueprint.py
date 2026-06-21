"""Zero-network tests for the blueprint→paper_edit render adapter.

No ffmpeg, no media: ``blueprint_to_paper_edit`` is pure assembly. Asserts the
event order/shapes render_rough_cut expects — act header cards, narration cards,
clips (with quote) vs B-roll (no quote), gaps for non-playable primaries, and the
sourced outcome card.
"""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import render_blueprint as rb  # noqa: E402


def _shaped():
    """A small SHAPED blueprint: two acts, a quote beat, a B-roll beat, a
    narration bridge, and one beat whose primary asset is a (non-playable) doc."""
    return {
        "case_id": "vasquez_23117201", "agency": "Sacramento County Sheriff",
        "logline": "A K9 apprehension, told across the body cameras.",
        "incident": {"charges": ["PC 459", "PC 148"], "disposition": "supervisor-reviewed"},
        "asset_manifest": [
            {"asset_id": "v_bwc1a", "kind": "bodycam", "pov_label": "BWC-1a",
             "path": "BWC-1a.mp4", "duration_sec": 300.0},
            {"asset_id": "a_911_1", "kind": "911_audio", "pov_label": "911 (A)",
             "path": "911A.mp3", "duration_sec": 120.0},
            {"asset_id": "doc_uofform", "kind": "document", "pov_label": "23-1",
             "path": "doc.pdf", "duration_sec": None},
        ],
        "acts": [
            {"act_id": "act_cold_open", "title": "Cold Open", "function": "hook",
             "target_sec": 45.0, "thesis": None, "beat_ids": ["br00"]},
            {"act_id": "act_incident", "title": "The Incident", "function": "escalate",
             "target_sec": 600.0, "thesis": "The force unfolds in real time.",
             "beat_ids": ["b00", "bdoc"]},
        ],
        "beats": [
            # B-roll cold open (911 audio, no quote)
            {"beat_id": "br00", "act_id": "act_cold_open", "ordinal": 0, "function": "cold_open",
             "target_duration_sec": 12.0,
             "primary_asset": {"asset_id": "a_911_1", "in_sec": 0.0, "out_sec": 12.0},
             "quote": None, "lower_third": {"text": "911 Dispatch"},
             "narration_bridge": None, "source_refs": ["a_911_1@0"],
             "is_broll": True, "broll_note": "open on the call"},
            # quote beat with realized narration
            {"beat_id": "b00", "act_id": "act_incident", "ordinal": 1, "function": "emotional_peak",
             "target_duration_sec": 18.0,
             "primary_asset": {"asset_id": "v_bwc1a", "in_sec": 80.0, "out_sec": 98.0},
             "quote": {"text": "dog on bite!", "source": "v_bwc1a", "timecode": 85.0},
             "lower_third": {"text": "Officer"},
             "narration_bridge": {"needed": True, "text": "The K9 is deployed.",
                                  "source_facts": ["v_bwc1a@85"]},
             "description": "K9 deployment", "source_refs": ["v_bwc1a@85"]},
            # a beat whose primary asset is a document → not playable → gap
            {"beat_id": "bdoc", "act_id": "act_incident", "ordinal": 2, "function": "reveal",
             "target_duration_sec": 10.0,
             "primary_asset": {"asset_id": "doc_uofform", "in_sec": 0.0, "out_sec": 10.0},
             "quote": None, "lower_third": {"text": "Record"},
             "narration_bridge": None, "description": "the report", "source_refs": []},
        ],
        "metadata": {"built_by": "llm_shaped"},
    }


def _pe(**kw):
    return rb.blueprint_to_paper_edit(_shaped(), **kw)


def _kinds(pe):
    return [e["kind"] for e in pe["timeline"]]


def test_title_card_uses_logline():
    pe = _pe()
    assert pe["timeline"][0] == {"kind": "card", "card_kind": "title",
                                 "title": "A K9 apprehension, told across the body cameras.",
                                 "subtitle": "Sacramento County Sheriff", "dur": rb.rc.TITLE_SEC}


def test_cold_open_act_has_no_header_card_but_incident_does():
    pe = _pe()
    titles = [e.get("title") for e in pe["timeline"] if e.get("card_kind") == "phase"]
    assert "The Incident" in titles
    assert "Cold Open" not in titles   # cold open plays straight in, no header


def test_act_header_carries_thesis():
    pe = _pe()
    inc = next(e for e in pe["timeline"] if e.get("card_kind") == "phase" and e["title"] == "The Incident")
    assert inc["subtitle"] == "The force unfolds in real time."


def test_broll_beat_plays_footage_without_quote():
    pe = _pe()
    clips = [e for e in pe["timeline"] if e["kind"] == "clip"]
    broll = next(c for c in clips if "911A.mp3" in c["media"])
    assert broll["transcript_excerpt"] == ""           # B-roll never shows a quote
    assert broll["in_sec"] == 0.0 and broll["out_sec"] == 12.0
    assert "911 Dispatch" in broll["lower_third"]


def test_quote_beat_keeps_pinned_quote_and_credit():
    pe = _pe()
    clip = next(e for e in pe["timeline"] if e["kind"] == "clip" and "BWC-1a.mp4" in e["media"])
    assert clip["transcript_excerpt"] == "dog on bite!"
    assert "Courtesy Sacramento County Sheriff" in clip["lower_third"]
    assert "Officer" in clip["lower_third"]


def test_realized_narration_becomes_a_narration_event_before_its_clip():
    pe = _pe()
    kinds = _kinds(pe)
    n_idx = kinds.index("narration")
    assert pe["timeline"][n_idx]["text"] == "The K9 is deployed."
    # the narration sits immediately before the bwc clip it introduces
    assert "BWC-1a.mp4" in pe["timeline"][n_idx + 1]["media"]


def test_non_playable_primary_becomes_a_gap():
    pe = _pe()
    gaps = [e for e in pe["timeline"] if e["kind"] == "gap"]
    assert len(gaps) == 1
    assert "doc_uofform" in gaps[0]["reason"]
    assert pe["_inputs"]["gaps"] == 1
    assert pe["_inputs"]["clips"] == 2


def test_outcome_card_is_last_and_sourced():
    pe = _pe()
    last = pe["timeline"][-1]
    assert last["card_kind"] == "outcome"
    assert "PC 459" in last["subtitle"] and "PC 148" in last["subtitle"]
    assert "supervisor-reviewed" in last["subtitle"]


def test_skeleton_blueprint_without_logline_or_narration_still_renders():
    bp = _shaped()
    bp["logline"] = None
    bp["metadata"]["built_by"] = "skeleton"
    for b in bp["beats"]:
        b["narration_bridge"] = {"needed": True, "brief": "x"}  # brief, no realized text
    pe = rb.blueprint_to_paper_edit(bp)
    assert pe["timeline"][0]["title"].startswith("Case ")   # falls back to case id
    assert not any(e["kind"] == "narration" for e in pe["timeline"])  # brief != realized text


def test_loudest_window_start_skips_silence():
    # a muted lead-in (the AXON buffer), then a loud stretch
    levels = [-90, -90, -90, -90, -20, -15, -18, -90, -90]
    # the loudest 3-sec window is indices 4..6 (the loud run)
    assert rb.loudest_window_start(levels, 3) == 4


def test_loudest_window_start_edge_cases():
    assert rb.loudest_window_start([], 5) == 0
    assert rb.loudest_window_start([-30, -10], 5) == 0   # want > len → clamps
    assert rb.loudest_window_start([-10, -10, -10], 1) == 0   # flat → first


def test_audio_aware_off_by_default_leaves_broll_window():
    # default (no audio_aware) keeps the LLM's window untouched — pure, no ffmpeg
    pe = rb.blueprint_to_paper_edit(_shaped())
    broll = next(e for e in pe["timeline"] if e["kind"] == "clip" and "911A.mp3" in e["media"])
    assert broll["in_sec"] == 0.0 and broll["out_sec"] == 12.0


def test_media_dir_remap_used_when_manifest_path_absent(tmp_path):
    # manifest path doesn't exist; a same-named file under media_dir does
    (tmp_path / "BWC-1a.mp4").write_bytes(b"x")
    pe = rb.blueprint_to_paper_edit(_shaped(), media_dir=tmp_path)
    clip = next(e for e in pe["timeline"] if e["kind"] == "clip" and e["media"].endswith("BWC-1a.mp4"))
    assert str(tmp_path) in clip["media"]
