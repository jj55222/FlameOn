"""Zero-network tests for the bundle judge.

The deterministic craft/coverage layer is pure; the LLM layer runs through a
MockJudgeBackend. Asserts the objective defects a human reviewer catches by hand
(replays, unsourced beats, runtime, missing substance, omissions) and that the
deterministic findings have downward veto power over the LLM's craft/substance.
"""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import judge as J  # noqa: E402


def _bundle(beats=None, **over):
    b = {"case_id": "c", "logline": "L", "target_runtime_sec": 1800.0,
         "acts": [{"title": "The Incident", "thesis": "x"}],
         "incident": {}, "metadata": {"planned_runtime_sec": 1700.0},
         "beats": beats if beats is not None else []}
    b.update(over)
    return b


def _beat(fn, asset="v_bwc5a", in_s=10, out_s=20, **kw):
    d = {"beat_id": kw.get("beat_id", fn), "act_id": kw.get("act_id", "act_incident"),
         "function": fn, "primary_asset": {"asset_id": asset, "in_sec": in_s, "out_sec": out_s},
         "quote": kw.get("quote"), "source": kw.get("source"), "is_broll": kw.get("is_broll", False),
         "importance": kw.get("importance")}
    return d


# --- craft ------------------------------------------------------------------

def test_craft_detects_replay_same_media_overlap():
    beats = [_beat("k9_deployment", in_s=53, out_s=68),
             _beat("takedown", in_s=55, out_s=70)]     # starts before prev ended -> replay
    cf = J.craft_findings(_bundle(beats))
    assert cf["replay_count"] == 1
    assert cf["replays"][0]["media"] == "v_bwc5a"


def test_craft_no_replay_when_contiguous():
    beats = [_beat("a", in_s=53, out_s=60), _beat("b", in_s=60, out_s=68)]
    assert J.craft_findings(_bundle(beats))["replay_count"] == 0


def test_craft_flags_unsourced_and_runtime():
    beats = [_beat("reveal"), {"beat_id": "x", "function": "gap", "primary_asset": None}]
    cf = J.craft_findings(_bundle(beats, metadata={"planned_runtime_sec": 200.0}))
    assert cf["unsourced_beats"] == ["x"] and cf["all_sourced"] is False
    assert cf["runtime_flag"] == "under"          # 200/1800


def test_craft_uses_paper_edit_when_given():
    pe = {"timeline": [{"kind": "clip", "media": "X.mp4", "in_sec": 0, "out_sec": 10},
                       {"kind": "clip", "media": "X.mp4", "in_sec": 3, "out_sec": 13}]}
    cf = J.craft_findings(_bundle([]), paper_edit=pe)
    assert cf["replay_count"] == 1


# --- coverage ---------------------------------------------------------------

def test_coverage_substance_and_salience_presence():
    beats = [_beat("procedural_violation"), _beat("emotional_peak"), _beat("k9_deployment")]
    cov = J.coverage_findings(_bundle(beats))
    assert cov["has_procedural"] is True
    assert cov["has_use_of_force"] is True
    assert "emotional_peak" in cov["salience_types_present"]


def test_coverage_flags_missing_procedural():
    cov = J.coverage_findings(_bundle([_beat("emotional_peak"), _beat("reveal")]))
    assert cov["has_procedural"] is False


def test_coverage_omissions_vs_pool():
    # the pool has a procedural violation the bundle never included
    bundle = _bundle([_beat("emotional_peak", in_s=80)])
    pool = [{"moment_type": "procedural_violation", "timestamp_sec": 240, "description": "alter the record"},
            {"moment_type": "emotional_peak", "timestamp_sec": 80}]
    cov = J.coverage_findings(bundle, pool=pool)
    assert any(o["type"] == "procedural_violation" for o in cov["omissions"])
    assert not any(o["type"] == "emotional_peak" for o in cov["omissions"])   # that one's present


# --- source_mix: classify off primary_asset kind, not b["source"] -----------

def test_source_mix_classifies_by_asset_manifest_kind():
    # A flagship-shaped bundle: footage lives in primary_asset (kinds bodycam/dashcam),
    # a record beat cites a document, an audio beat carries a quote. NONE set
    # beat["source"] and every beat has a quote — the old code scored this vision:0.
    manifest = [{"asset_id": "v_anderson_bwc", "kind": "bodycam"},
                {"asset_id": "cam_dash_2", "kind": "dashcam"},      # not v_ prefixed
                {"asset_id": "x_doc_p1", "kind": "document"},
                {"asset_id": "a_911_3", "kind": "911_audio"}]
    beats = [_beat("reveal", asset="v_anderson_bwc", quote={"text": "shots fired"}),
             _beat("scene_set", asset="cam_dash_2"),
             _beat("record", asset="x_doc_p1"),
             _beat("tension_shift", asset="a_911_3", quote={"text": "he has a gun"})]
    sm = J.coverage_findings(_bundle(beats, asset_manifest=manifest))["source_mix"]
    assert sm["vision"] == 2          # bodycam + dashcam, even w/ a quote and no source="vision"
    assert sm["document"] == 1
    assert sm["transcript"] == 1      # the 911 audio-with-quote


def test_source_mix_counts_broll_beat_as_broll_not_vision():
    manifest = [{"asset_id": "v_stock", "kind": "video"}]
    beats = [_beat("scene", asset="v_stock", is_broll=True)]
    sm = J.coverage_findings(_bundle(beats, asset_manifest=manifest))["source_mix"]
    assert sm["broll"] == 1 and sm["vision"] == 0


def test_source_mix_falls_back_to_v_prefix_without_manifest():
    # legacy bundles with no asset_manifest still classify v_* as footage
    sm = J.coverage_findings(_bundle([_beat("reveal", asset="v_bwc5a")]))["source_mix"]
    assert sm["vision"] == 1


def test_bundle_digest_reports_vision_source_to_llm():
    # the digest is what the LLM judges; a bodycam beat must read as "vision", not
    # "transcript" — otherwise the LLM is prompted into a false zero-visual critique.
    manifest = [{"asset_id": "v_cam", "kind": "bodycam"}]
    bundle = _bundle([_beat("reveal", asset="v_cam", quote={"text": "shots fired"})],
                     asset_manifest=manifest)
    assert J._bundle_digest(bundle)["beats"][0]["source"] == "vision"


# --- use-of-force read from beat text when no force FUNCTION is present ------

def test_use_of_force_detected_from_beat_text_without_force_function():
    # beat_miner labels a shooting reveal/tension_shift (never a force FUNCTION), so
    # use-of-force must be read from the words.
    beats = [_beat("reveal", quote={"text": "He's shooting at the officer!"}),
             _beat("tension_shift", asset="a_1", quote={"text": "keep your hands up"})]
    assert J.coverage_findings(_bundle(beats))["has_use_of_force"] is True


def test_use_of_force_not_triggered_by_benign_stop():
    calm = [_beat("scene_set", quote={"text": "license and registration please"})]
    assert J.coverage_findings(_bundle(calm))["has_use_of_force"] is False


# --- blend (deterministic veto) --------------------------------------------

def test_blend_penalizes_craft_for_replays():
    report = {"craft": {"replay_count": 2, "all_sourced": True, "runtime_flag": "ok"},
              "coverage": {"substance_types_present": ["reveal"]}}
    scores = J._blend_scores({"scores": {"craft": 0.95, "substance": 0.8, "salience": 0.7}}, report)
    assert scores["craft"] < 0.95            # replays drag craft down


def test_blend_floors_substance_when_absent():
    report = {"craft": {"replay_count": 0, "all_sourced": True, "runtime_flag": "ok"},
              "coverage": {"substance_types_present": []}}
    scores = J._blend_scores({"scores": {"substance": 0.9, "salience": 0.7, "craft": 0.8}}, report)
    assert scores["substance"] <= 0.4


# --- parse + orchestration --------------------------------------------------

def test_parse_verdict_strips_fences():
    raw = '```json\n{"verdict":"SHIP","scores":{"overall":0.9}}\n```'
    assert J.parse_verdict(raw)["verdict"] == "SHIP"


def test_judge_bundle_end_to_end_with_mock():
    beats = [_beat("procedural_violation", in_s=53, out_s=60),
             _beat("k9_deployment", in_s=60, out_s=68, source="vision")]
    bundle = _bundle(beats)
    backend = J.MockJudgeBackend({"verdict": "REVISE",
        "scores": {"substance": 0.8, "salience": 0.6, "craft": 0.9, "overall": 0.77},
        "strengths": ["covers the force"], "issues": [], "omissions": []})
    v = J.judge_bundle(bundle, backend)
    assert v["verdict"] == "REVISE"
    assert v["deterministic"]["coverage"]["has_procedural"] is True
    assert v["deterministic"]["craft"]["replay_count"] == 0
    assert "EDIT BUNDLE" in backend.last_prompt and "DETERMINISTIC REPORT" in backend.last_prompt


def test_coverage_tolerates_none_act_id():
    # front-anchored B-roll has act_id None — must not crash coverage
    beats = [{"beat_id": "br", "act_id": None, "function": "scene", "is_broll": True,
              "primary_asset": {"asset_id": "v_icc", "in_sec": 0, "out_sec": 10}}]
    cov = J.coverage_findings(_bundle(beats))
    assert cov["beat_count"] == 1


def test_judge_omissions_merge_pool_into_output():
    bundle = _bundle([_beat("emotional_peak", in_s=80)])
    pool = [{"moment_type": "procedural_violation", "timestamp_sec": 240}]
    v = J.judge_bundle(bundle, J.MockJudgeBackend(), pool=pool)
    assert "procedural_violation" in v["omissions"]
