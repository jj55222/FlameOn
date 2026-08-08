"""Zero-network tests for find_shooting_pov: pure scoring/selection + the
cost-metered funnel loop (vision call injected, ffmpeg/disk stubbed)."""
from __future__ import annotations

import json
import sys
import zipfile
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import find_shooting_pov as fsp  # noqa: E402
import vision_scan as vs          # noqa: E402
import zip_triage as zt           # noqa: E402


# ---- pure helpers ---------------------------------------------------------

def test_shooting_score_weights_by_event_and_confidence():
    ev = [{"event_type": "firearm_discharge", "confidence": 1.0},   # 1.0 * 1.0
          {"event_type": "officer_down", "confidence": 0.5},        # 0.9 * 0.5
          {"event_type": "scene", "confidence": 1.0}]               # 0.0
    assert fsp.shooting_score(ev) == 1.45


def test_shooting_score_empty_is_zero():
    assert fsp.shooting_score([]) == 0.0


def test_candidate_windows_centers_on_volley_and_clamps():
    rec = {"shot_cluster": {"start": 100.0, "count": 6}, "duration_sec": 118.0,
           "hot_windows": [[101.0, 110.0]]}
    wins = fsp.candidate_windows(rec, pad=12.0, dur_cap=30.0)
    assert wins[0] == [88.0, 118.0]          # start-12 .. clamped to duration
    assert len(wins) == 1                     # hot_window overlaps the volley → not duplicated


def test_candidate_windows_falls_back_to_full_when_no_cluster():
    rec = {"shot_cluster": {"start": None, "count": 0}, "duration_sec": 10.0}
    assert fsp.candidate_windows(rec, dur_cap=30.0) == [[0.0, 10.0]]


def test_select_candidates_ranks_by_volley_then_skips():
    triage = [
        {"member": "a/7.mp4", "shot_cluster": {"count": 6}, "impulse_score": 10, "activity_score": 100},
        {"member": "8.mp4", "shot_cluster": {"count": 8}, "impulse_score": 20, "activity_score": 200},
        {"member": "9.mp4", "shot_cluster": {"count": 8}, "impulse_score": 5,  "activity_score": 50},
    ]
    sel = fsp.select_candidates(triage, top_n=2, skip={"9.mp4"})
    assert [s["member"] for s in sel] == ["8.mp4", "a/7.mp4"]   # 9 skipped; 8 (count8) before 7 (count6)


# ---- the funnel loop (vision injected, ffmpeg/disk stubbed) ----------------

def _triage_zip(tmp_path):
    zp = tmp_path / "case.zip"
    with zipfile.ZipFile(zp, "w") as z:
        z.writestr("7.mp4", b"x" * 16)
        z.writestr("8.mp4", b"y" * 16)
    triage = [
        {"member": "7.mp4", "size_mb": 50, "shot_cluster": {"start": 100, "count": 6},
         "duration_sec": 600, "impulse_score": 10, "activity_score": 100, "hot_windows": [[100, 120]]},
        {"member": "8.mp4", "size_mb": 60, "shot_cluster": {"start": 200, "count": 8},
         "duration_sec": 600, "impulse_score": 20, "activity_score": 200, "hot_windows": [[200, 220]]},
    ]
    tp = tmp_path / "triage.json"
    tp.write_text(json.dumps(triage), encoding="utf-8")
    return str(zp), str(tp)


def _stub_env(monkeypatch, tmp_path):
    # one fake frame; never opened because the vision call is injected
    monkeypatch.setattr(vs, "sample_frames",
                        lambda *a, **k: [(1.0, str(tmp_path / "f.png"))])
    monkeypatch.setattr(zt, "free_gb", lambda *a, **k: 999.0)


_DISCHARGE = ('{"events":[{"timecode_sec":1.0,"event_type":"firearm_discharge",'
              '"confidence":0.9}]}')


def test_find_pov_scores_accumulates_cost_and_checkpoints(tmp_path, monkeypatch):
    zp, tp = _triage_zip(tmp_path)
    _stub_env(monkeypatch, tmp_path)
    calls = {"n": 0}

    def call_fn(frames, user):
        calls["n"] += 1
        return _DISCHARGE, 0.002, 120, 40       # (text, cost_usd, in_tok, out_tok)

    out = tmp_path / "pov.json"
    res = fsp.find_pov(zp, tp, str(out), top_n=12, budget_usd=1.0,
                       call_fn=call_fn, tmp_dir=str(tmp_path / "_t"))

    assert calls["n"] == 2                                   # one vision call per candidate
    assert {r["member"] for r in res} == {"7.mp4", "8.mp4"}
    assert all(r["shooting_score"] == 0.9 for r in res)      # 1.0 weight * 0.9 conf
    assert round(sum(r["vision_cost_usd"] for r in res), 4) == 0.004
    saved = json.loads(out.read_text(encoding="utf-8"))      # checkpoint persisted
    assert {r["member"] for r in saved} == {"7.mp4", "8.mp4"}


def test_find_pov_hard_budget_stops_before_overspend(tmp_path, monkeypatch):
    zp, tp = _triage_zip(tmp_path)
    _stub_env(monkeypatch, tmp_path)

    def call_fn(frames, user):
        return _DISCHARGE, 0.002, 120, 40

    out = tmp_path / "pov.json"
    res = fsp.find_pov(zp, tp, str(out), top_n=12, budget_usd=0.0015,   # < one call
                       call_fn=call_fn, tmp_dir=str(tmp_path / "_t"))

    assert len(res) == 1                       # 8.mp4 (top volley) scored, then budget stop
    assert res[0]["member"] == "8.mp4"


def test_find_pov_resume_skips_already_scored(tmp_path, monkeypatch):
    zp, tp = _triage_zip(tmp_path)
    _stub_env(monkeypatch, tmp_path)
    out = tmp_path / "pov.json"
    out.write_text(json.dumps([{"member": "8.mp4", "shooting_score": 0.5,
                                "vision_cost_usd": 0.0}]), encoding="utf-8")
    calls = {"n": 0}

    def call_fn(frames, user):
        calls["n"] += 1
        return _DISCHARGE, 0.002, 120, 40

    res = fsp.find_pov(zp, tp, str(out), top_n=12, budget_usd=1.0,
                       call_fn=call_fn, tmp_dir=str(tmp_path / "_t"))
    assert calls["n"] == 1                                  # only 7.mp4 re-scored; 8.mp4 resumed
    assert {r["member"] for r in res} == {"7.mp4", "8.mp4"}
