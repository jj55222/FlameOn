"""Tests for av_transients.py — impulsive-onset detection + beat snapping (FIX 4).

The DSP is exercised on a SYNTHESIZED wav (silence + click + speech) with stdlib
only (no ffmpeg). The snap math and the beat-gating are unit-tested with the media
decode monkeypatched, so nothing here shells out.
"""
from __future__ import annotations

import array
import random
import sys
import wave
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import av_transients as AV  # noqa: E402


def _synth(sr=8000):
    """0.5s silence, a 5ms click at 0.5s, 0.2s silence, then 0.5s quiet speech."""
    random.seed(7)
    s = [0.0] * int(0.5 * sr)
    s += [0.95 if i % 2 == 0 else -0.95 for i in range(int(0.005 * sr))]   # the bang
    s += [0.0] * int(0.195 * sr)
    s += [random.uniform(-0.08, 0.08) for _ in range(int(0.5 * sr))]        # speech
    return s, sr


def _write_wav(path, samples, sr):
    with wave.open(str(path), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(sr)
        w.writeframes(array.array("h", [int(max(-1, min(1, v)) * 32767) for v in samples]).tobytes())


# --- detection (the acceptance: find the click) -----------------------------

def test_detect_transient_finds_click():
    samples, sr = _synth()
    hits = AV.detect_transients(samples, sr)
    assert hits, "no transient found"
    assert abs(hits[0][0] - 0.5) < 0.05           # strongest onset is the click at ~0.5s


def test_detect_via_wav_roundtrip(tmp_path):
    samples, sr = _synth()
    p = tmp_path / "click.wav"
    _write_wav(p, samples, sr)
    s2, sr2 = AV.read_wav(p)
    assert sr2 == sr
    hits = AV.detect_transients(s2, sr2)
    assert hits and abs(hits[0][0] - 0.5) < 0.05


def test_no_strong_transient_in_quiet_noise():
    random.seed(3)
    quiet = [random.uniform(-0.05, 0.05) for _ in range(8000)]
    # no impulsive step => no onset clears mean+k*std as a clean local max spike
    assert AV.detect_transients(quiet, 8000, k=4.0) == []


# --- gating -----------------------------------------------------------------

def test_is_snap_candidate_requires_function_and_onset_cue():
    def beat(fn, text):
        return {"function": fn, "quote": {"text": text}}
    assert AV.is_snap_candidate(beat("reveal", "shots fired, shots fired!"))
    assert AV.is_snap_candidate(beat("tension_shift", "drop the gun!"))
    assert not AV.is_snap_candidate(beat("scene_set", "shots fired"))          # wrong function
    assert not AV.is_snap_candidate(beat("reveal", "license and registration"))  # no onset cue
    # bare "shooting"/"he was shot" must NOT trigger (aftermath recurrence)
    assert not AV.is_snap_candidate(beat("reveal", "he was shot earlier"))


# --- snap math + beat mutation (media decode monkeypatched) -----------------

def test_snap_window_places_transient_lead_after_start(monkeypatch):
    # transient at t=100; a speech-grounded clip that starts at 105 (post-bang)
    monkeypatch.setattr(AV, "strongest_transient", lambda *a, **k: (100.0, 0.5))
    new_in, new_out = AV.snap_window("x.mp4", 105.0, 115.0, lead_sec=3.0)
    assert new_in == 97.0                    # 100 - 3s lead
    assert round(new_out - new_in, 1) == 10.0    # duration preserved
    assert 2.0 <= (100.0 - new_in) <= 4.0        # bang lands 2-4s into the clip


def test_snap_window_none_when_no_transient(monkeypatch):
    monkeypatch.setattr(AV, "strongest_transient", lambda *a, **k: None)
    assert AV.snap_window("x.mp4", 105.0, 115.0) is None


def test_snap_beats_moves_only_eligible(monkeypatch, tmp_path):
    media = tmp_path / "cam.wav"
    _write_wav(media, *_synth())
    monkeypatch.setattr(AV, "strongest_transient", lambda *a, **k: (100.0, 0.5))
    beats = [
        {"beat_id": "b0", "function": "reveal", "quote": {"text": "shots fired!"},
         "primary_asset": {"asset_id": "v_cam", "in_sec": 105.0, "out_sec": 115.0}},
        {"beat_id": "b1", "function": "scene_set", "quote": {"text": "shots fired!"},
         "primary_asset": {"asset_id": "v_cam", "in_sec": 10.0, "out_sec": 20.0}},
    ]
    moved = AV.snap_beats(beats, lambda aid: str(media))
    assert [b["beat_id"] for b in moved] == ["b0"]
    assert beats[0]["primary_asset"]["in_sec"] == 97.0            # snapped
    assert beats[1]["primary_asset"]["in_sec"] == 10.0           # untouched (wrong function)


def test_snap_beats_skips_missing_media(monkeypatch):
    monkeypatch.setattr(AV, "strongest_transient", lambda *a, **k: (100.0, 0.5))
    beats = [{"beat_id": "b0", "function": "reveal", "quote": {"text": "shots fired!"},
              "primary_asset": {"asset_id": "v_cam", "in_sec": 105.0, "out_sec": 115.0}}]
    moved = AV.snap_beats(beats, lambda aid: "/no/such/file.mp4")
    assert moved == [] and beats[0]["primary_asset"]["in_sec"] == 105.0
