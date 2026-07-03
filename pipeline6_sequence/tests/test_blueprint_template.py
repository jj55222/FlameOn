"""Tests for the template-driven act skeleton (FIX 3).

A --template imposes a fixed ordered act list with target %-spans and per-act
beat-function quotas (the SolvedFiles standoff/discovery skeletons) instead of the
generic phase-∝-beats split; absent the flag, build_acts() behaviour is unchanged.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pytest

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import blueprint as B  # noqa: E402
import make_documentary as MD  # noqa: E402


def _beat(bid, phase):
    return {"beat_id": bid, "act_id": f"act_{phase}", "_phase": phase}


_TMPL = {
    "template_id": "t_demo",
    "acts": [
        {"id": "open", "phase": "pre_incident", "span_pct": [0, 20], "beat_functions": ["scene_set"]},
        {"id": "mid", "phase": "incident", "span_pct": [20, 80], "beat_functions": ["tension_shift", "reveal"]},
        {"id": "end", "phase": "outcome", "span_pct": [80, 100], "beat_functions": ["verdict"]},
    ],
}


# --- span math (the core acceptance) ----------------------------------------

def test_template_acts_honor_span_pct():
    beats = [_beat("p0", "pre_incident")] + [_beat(f"i{i}", "incident") for i in range(6)] \
            + [_beat("o0", "outcome")]
    acts = B.build_acts_from_template(_TMPL, beats, 1000.0)
    got = {a["act_id"]: a["target_sec"] for a in acts}
    assert got["act_open"] == 200.0    # 20% of 1000
    assert got["act_mid"] == 600.0     # 60%
    assert got["act_end"] == 200.0     # 20%
    assert round(sum(got.values()), 1) == 1000.0     # full-coverage template


def test_template_carries_beat_function_quota():
    acts = B.build_acts_from_template(_TMPL, [_beat("i0", "incident")], 600.0)
    mid = next(a for a in acts if a["act_id"] == "act_mid")
    assert mid["beat_function_quota"] == ["tension_shift", "reveal"]
    assert mid["template_id"] == "t_demo"


def test_template_slots_and_retags_beats_by_phase():
    beats = [_beat("p0", "pre_incident"), _beat("i0", "incident"), _beat("i1", "incident"),
             _beat("o0", "outcome")]
    acts = B.build_acts_from_template(_TMPL, beats, 1000.0)
    by_id = {a["act_id"]: a for a in acts}
    assert by_id["act_open"]["beat_ids"] == ["p0"]
    assert by_id["act_mid"]["beat_ids"] == ["i0", "i1"]
    assert by_id["act_end"]["beat_ids"] == ["o0"]
    # beats were re-tagged so downstream groups under the skeleton (no orphans)
    assert beats[1]["act_id"] == "act_mid"


def test_same_phase_acts_split_beats_proportional_to_span():
    tmpl = {"template_id": "t2", "acts": [
        {"id": "early", "phase": "incident", "span_pct": [0, 25]},     # 1/4 weight
        {"id": "late", "phase": "incident", "span_pct": [25, 100]},    # 3/4 weight
    ]}
    beats = [_beat(f"i{i}", "incident") for i in range(8)]
    acts = B.build_acts_from_template(tmpl, beats, 1000.0)
    by_id = {a["act_id"]: a for a in acts}
    assert len(by_id["act_early"]["beat_ids"]) == 2      # 25% of 8
    assert len(by_id["act_late"]["beat_ids"]) == 6       # remainder, chronological
    assert by_id["act_early"]["beat_ids"] == ["i0", "i1"]


def test_beats_of_absent_phase_fall_back_to_nearest_act():
    # template has no 'aftermath' act; those beats must still land somewhere (no drop)
    beats = [_beat("a0", "aftermath"), _beat("i0", "incident")]
    acts = B.build_acts_from_template(_TMPL, beats, 1000.0)
    all_bids = [bid for a in acts for bid in a["beat_ids"]]
    assert set(all_bids) == {"a0", "i0"}


# --- loader -----------------------------------------------------------------

def test_load_template_by_name_and_path():
    t = B.load_template("solvedfiles_standoff")
    assert t["template_id"] == "solvedfiles_standoff" and len(t["acts"]) == 8
    p = B.TEMPLATES_DIR / "solvedfiles_discovery.json"
    assert B.load_template(str(p))["template_id"] == "solvedfiles_discovery"


def test_load_template_missing_raises():
    with pytest.raises(FileNotFoundError):
        B.load_template("no_such_template")


@pytest.mark.parametrize("name", ["solvedfiles_standoff", "solvedfiles_discovery"])
def test_shipped_templates_are_valid_and_cover_0_100(name):
    t = B.load_template(name)
    spans = [a["span_pct"] for a in t["acts"]]
    assert spans[0][0] == 0 and spans[-1][1] == 100
    for (s, e), (ns, ne) in zip(spans, spans[1:]):
        assert s < e and e == ns                     # contiguous, non-overlapping, ascending
    for a in t["acts"]:                              # every act declares a quota + canonical phase
        assert a.get("beat_functions")
        assert B._canon_phase(a["phase"]) in B._CANON_PHASES


# --- make_documentary wiring (dry-run prints the template step) -------------

def _md_ns(**over):
    ns = argparse.Namespace(
        basket="/tmp/fake", media_dir=None, case_id="demo", doc=None, no_align=True,
        phases="incident", kinds="bodycam", moments="beatminer", moments_model="m",
        skip_score=True, flagship=True, agency="A", target_runtime=600.0,
        template=None, shape_model="m", judge_mock=True, judge_model=None, cold_open=None)
    for k, v in over.items():
        setattr(ns, k, v)
    return ns


def test_make_documentary_dry_run_includes_template_step():
    steps = MD.build_plan(_md_ns(template="solvedfiles_standoff"))
    bp = next(s for s in steps if s.label.startswith("blueprint"))
    assert "template" in bp.label and "solvedfiles_standoff" in bp.label
    assert "--template" in bp.argv and "solvedfiles_standoff" in bp.argv


def test_make_documentary_no_template_is_unchanged():
    steps = MD.build_plan(_md_ns(template=None))
    bp = next(s for s in steps if s.label.startswith("blueprint"))
    assert bp.label == "blueprint"
    assert "--template" not in bp.argv
