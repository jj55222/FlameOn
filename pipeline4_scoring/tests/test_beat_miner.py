"""Zero-network tests for beat_miner per-file mining (the BWC under-recall fix).

Flattening every transcript into ONE proposal lets dense short audio (911 calls,
full of salience-5 cues) win the global top-K and drown long, chaotic BWC A-roll.
Mining each file with its own budget, then round-robining across evidence kinds,
guarantees the scarce footage a fair share. All heuristic (--mock), no LLM/network.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import beat_miner as bm  # noqa: E402


def _seg(t, text):
    return {"start_sec": t, "end_sec": t + 3, "text": text}


def _write(dir_: Path, name: str, segs: list) -> Path:
    p = dir_ / name
    p.write_text(json.dumps({"transcript": segs}), encoding="utf-8")
    return p


def _case(tmp_path: Path) -> Path:
    """A long noisy BWC transcript (many filler segments, low-salience commands) +
    a short clean 911 call (few dense high-salience cues) — the Iona shape in
    miniature."""
    d = tmp_path / "transcripts"
    d.mkdir()
    # long noisy bodycam: 20 filler segments + 3 low-salience command cues (sal 3)
    long_segs = [_seg(i * 5, f"uh copy that standby unit {i}") for i in range(20)]
    long_segs += [_seg(110, "everybody get back"),          # tension_shift 3
                  _seg(240, "show me your hands"),           # tension_shift 3
                  _seg(400, "stop moving right now")]        # tension_shift 3
    _write(d, "sdpd_Video_OfficerAndersonBodyWornCamera.json", long_segs)
    # short clean 911 call: dense high-salience cues (sal 4-5)
    _write(d, "sdpd_Audio_911Call1.json",
           [_seg(5, "shots fired shots fired"),              # reveal 5
            _seg(9, "oh my god please hurry"),               # emotional_peak 4
            _seg(30, "he's dead he's dead")])                # emotional_peak 4
    return d


def _sources(beats):
    return {b.get("artifact_id") for b in beats}


# --- the core acceptance: BOTH files surface -------------------------------

def test_mine_per_file_surfaces_both_long_and_short(tmp_path):
    d = _case(tmp_path)
    beats, n_files = bm.mine_per_file([str(d)], max_beats=10, per_file_max=5, mock=True)
    assert n_files == 2
    srcs = _sources(beats)
    assert any("Video" in s for s in srcs), f"BWC A-roll missing: {srcs}"
    assert any("911" in s for s in srcs), f"911 audio missing: {srcs}"


def test_legacy_flatten_crowds_out_low_salience_video(tmp_path):
    # the pre-fix path: one combined proposal, global top-K by salience. With a
    # tight budget the dense 911 cues win every slot and the BWC file is dropped.
    d = _case(tmp_path)
    segs = bm.load_segments([str(d)])
    blob, toks = bm.load_transcript_blob([str(d)])
    kept, _ = bm.ground_filter(bm.propose_mock(segs, 2), blob, toks, segs)
    srcs = _sources(bm.dedup(kept))
    assert not any("Video" in s for s in srcs)   # crowded out
    assert any("911" in s for s in srcs)


# --- units ------------------------------------------------------------------

def test_interleave_by_kind_orders_video_first(tmp_path):
    files = ["case_Audio_911Call1.json", "case_Audio_Dispatch.json",
             "case_Video_BodyWornCamera.json", "case_Audio_Interview.json"]
    ordered = [Path(f).name for f in bm.interleave_by_kind(files)]
    assert "Video" in ordered[0]           # scarce A-roll leads the rotation


def test_infer_kind_buckets():
    assert bm._infer_kind("x_Video_OfficerBodyWornCamera") == "video"
    assert bm._infer_kind("y_Audio_911Call3") == "911"
    assert bm._infer_kind("z_Audio_InterviewOfficer") == "interview"
    assert bm._infer_kind("w_Audio_RandomWav") == "audio"


def test_dedup_same_source_does_not_merge_across_files():
    # two reveals with the SAME file-relative start but DIFFERENT sources are not a
    # duplicate — their clocks are independent.
    a = {"moment_type": "reveal", "salience": 5, "start_sec": 100, "artifact_id": "cam_A"}
    b = {"moment_type": "reveal", "salience": 5, "start_sec": 101, "artifact_id": "cam_B"}
    assert len(bm.dedup([a, b], same_source=True)) == 2      # kept apart
    assert len(bm.dedup([dict(a), dict(b)])) == 1            # legacy: merged (old behavior)


def test_round_robin_one_per_file_per_round_and_cap():
    f1 = [{"salience": 5, "artifact_id": "f1", "moment_type": "reveal", "start_sec": 1},
          {"salience": 4, "artifact_id": "f1", "moment_type": "reveal", "start_sec": 9}]
    f2 = [{"salience": 3, "artifact_id": "f2", "moment_type": "reveal", "start_sec": 1}]
    got = bm._round_robin([f1, f2], cap=10)
    # round 1 takes one from each file before f1's surplus -> f2 represented at index 1
    assert got[0]["artifact_id"] == "f1" and got[1]["artifact_id"] == "f2"
    assert len(got) == 3
    assert len(bm._round_robin([f1, f2], cap=2)) == 2        # cap honored


def test_mine_per_file_respects_max_beats(tmp_path):
    d = _case(tmp_path)
    beats, _ = bm.mine_per_file([str(d)], max_beats=3, per_file_max=5, mock=True)
    assert len(beats) <= 3
