"""Zero-network tests for vision_scan — the pure helpers + mock-backend scan.

No ffmpeg, no media, no network: frame sampling is monkeypatched and the VLM is
a MockVisionBackend. Covers batching, tolerant event parsing (shape drift, bad
timecodes, taxonomy clamping, window filtering), cross-batch merge, and the
scan orchestration end to end.
"""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import vision_scan as vs  # noqa: E402


# --- batch ------------------------------------------------------------------

def test_batch_splits_evenly_and_remainder():
    assert vs.batch([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert vs.batch([], 3) == []
    assert vs.batch([1, 2], 0) == [[1], [2]]   # n<1 clamps to 1


# --- parse_events -----------------------------------------------------------

def test_parse_events_happy_path():
    raw = ('{"events":[{"timecode_sec":72.0,"event_type":"k9_deployment",'
           '"actor":"K9 handler","description":"officer releases the dog","confidence":0.9}]}')
    ev = vs.parse_events(raw)
    assert len(ev) == 1
    assert ev[0]["event_type"] == "k9_deployment"
    assert ev[0]["timecode_sec"] == 72.0
    assert ev[0]["confidence"] == 0.9


def test_parse_events_tolerates_bare_list_and_alt_keys():
    raw = '[{"t":10,"type":"taser","who":"Officer A","desc":"taser deployed"}]'
    ev = vs.parse_events(raw)
    assert ev[0]["event_type"] == "taser"
    assert ev[0]["timecode_sec"] == 10.0
    assert ev[0]["actor"] == "Officer A"


def test_parse_events_drops_bad_timecode_and_clamps_unknown_type():
    raw = ('{"events":[{"event_type":"k9_deployment"},'                 # no timecode -> dropped
           '{"timecode_sec":5,"event_type":"excessive force"},'         # unknown w/ "force"
           '{"timecode_sec":6,"event_type":"dancing"}]}')               # unknown -> scene
    ev = vs.parse_events(raw)
    assert len(ev) == 2
    assert ev[0]["event_type"] == "use_of_force_other"
    assert ev[1]["event_type"] == "scene"


def test_parse_events_window_filter_rejects_invented_timecodes():
    raw = ('{"events":[{"timecode_sec":50,"event_type":"strike"},'
           '{"timecode_sec":9999,"event_type":"strike"}]}')
    ev = vs.parse_events(raw, valid_window=(40.0, 60.0))
    assert [e["timecode_sec"] for e in ev] == [50.0]


def test_parse_events_garbage_returns_empty():
    assert vs.parse_events("not json at all") == []
    assert vs.parse_events("") == []


# --- merge_events -----------------------------------------------------------

def test_merge_collapses_same_type_within_gap_keeps_best():
    events = [
        {"timecode_sec": 70.0, "event_type": "k9_deployment", "actor": "", "description": "a", "confidence": 0.5},
        {"timecode_sec": 71.5, "event_type": "k9_deployment", "actor": "", "description": "b", "confidence": 0.8},
        {"timecode_sec": 90.0, "event_type": "k9_deployment", "actor": "", "description": "c", "confidence": 0.6},
    ]
    merged = vs.merge_events(events, gap_sec=3.0)
    assert len(merged) == 2                       # 70/71.5 collapse; 90 separate
    assert merged[0]["confidence"] == 0.8         # highest-confidence representative
    assert merged[0]["timecode_sec"] == 71.5


def test_merge_keeps_distinct_types_at_same_time():
    events = [
        {"timecode_sec": 50.0, "event_type": "k9_deployment", "actor": "", "description": "", "confidence": 0.7},
        {"timecode_sec": 50.0, "event_type": "foot_pursuit", "actor": "", "description": "", "confidence": 0.7},
    ]
    assert len(vs.merge_events(events)) == 2


# --- scan orchestration (mock backend, monkeypatched sampling) --------------

def test_scan_visual_events_end_to_end(monkeypatch):
    # fake frames spanning a window; the mock VLM "sees" a K9 deployment
    fake_frames = [(70.0, "f70.png"), (71.0, "f71.png"), (72.0, "f72.png")]
    monkeypatch.setattr(vs, "sample_frames", lambda *a, **k: fake_frames)
    reply = {"events": [{"timecode_sec": 71.0, "event_type": "k9_deployment",
                         "actor": "K9 handler", "description": "dog released", "confidence": 0.9}]}
    backend = vs.MockVisionBackend(reply)
    events = vs.scan_visual_events("BWC-5a.mp4", [[50.0, 90.0]], backend)
    assert len(events) == 1
    assert events[0]["event_type"] == "k9_deployment"
    assert backend.calls and backend.calls[0] == fake_frames   # frames reached the backend


def test_scan_returns_empty_when_no_frames(monkeypatch):
    monkeypatch.setattr(vs, "sample_frames", lambda *a, **k: [])
    assert vs.scan_visual_events("x.mp4", [[0, 1]], vs.MockVisionBackend()) == []
