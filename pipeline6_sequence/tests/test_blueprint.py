"""Zero-network tests for the deterministic blueprint builder (the rails).

No media, no ffmpeg, no network: durations come from the artifact fixtures and
``Source`` objects are constructed directly (bypassing media resolution). Asserts
the manifest, incident facts, chronological sourced beats, acts/runtime budget,
the integrity ledger, and the gaps walker.
"""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import blueprint as bp  # noqa: E402
import render_rough_cut as rc  # noqa: E402


# --- fixtures ---------------------------------------------------------------

EPOCH = 1_681_811_089.0   # arbitrary; same for both cams so order is by ts


def _artifacts():
    return [
        {"artifact_id": "BWC-1a", "path": "BWC-1a.mp4", "kind": "bodycam",
         "pov_label": "BWC-1a", "duration_sec": 300.0,
         "start_iso": "2023-04-18 09:44:49", "timestamp_source": "axon_ocr",
         "phase": "incident"},
        {"artifact_id": "BWC-2", "path": "BWC-2.mp4", "kind": "bodycam",
         "pov_label": "BWC-2", "duration_sec": 300.0,
         "start_iso": "2023-04-18 09:45:05", "timestamp_source": "axon_ocr",
         "phase": "incident"},
        {"artifact_id": "911 (A)", "path": "911A.mp3", "kind": "911",
         "pov_label": "911 (A)", "duration_sec": 120.0, "start_iso": None,
         "timestamp_source": None, "phase": "pre_incident"},
    ]


def _timeline():
    return {
        "anchor_iso": "2023-04-18 09:44:49", "anchor_epoch": EPOCH,
        "phases": {
            "pre_incident": [{"artifact_id": "911 (A)", "kind": "911"}],
            "incident": [{"artifact_id": "BWC-1a", "kind": "bodycam"},
                         {"artifact_id": "BWC-2", "kind": "bodycam"}],
        },
        "case_id": "vasquez_x",
    }


def _verdict():
    return {"case_id": "vasquez_x", "verdict": "HOLD",
            "transcript_refs": ["t0", "t1", "t2"],
            "key_moments": [
                {"source_idx": 0, "moment_type": "emotional_peak", "importance": "critical",
                 "timestamp_sec": 85.0, "end_timestamp_sec": 95.0,
                 "description": "K9 deployment", "transcript_excerpt": "dog on bite!"},
                {"source_idx": 1, "moment_type": "reveal", "importance": "high",
                 "timestamp_sec": 50.0, "end_timestamp_sec": 55.0,
                 "description": "admission", "transcript_excerpt": "I was scared"},
                {"source_idx": 2, "moment_type": "contradiction", "importance": "high",
                 "timestamp_sec": 10.0, "description": "no media here"},
            ]}


def _sources():
    return [
        rc.Source(0, Path("BWC-1a.mp4"), "bodycam", "Body-Worn Camera (BWC-1a)",
                  None, EPOCH, "incident"),
        rc.Source(1, Path("BWC-2.mp4"), "bodycam", "Body-Worn Camera (BWC-2)",
                  None, EPOCH, "incident"),
        rc.Source(2, None, "other", "Source", None, None, None),  # unsourced beat
    ]


def _doc():
    return {"doc_type": "uof_form", "ia_case_number": "23-117201",
            "doc_date": "04/18/2023", "incident_time": "09:44",
            "location": "800 Howe Ave", "charges": ["PC 459", "PC 148"],
            "people": ["Vasquez Sonny"],
            "disposition": {"summary": "Use-of-Force report; supervisor-reviewed."},
            "outcome_card": {"title": "Use of Force · #23-117201",
                             "subtitle": "supervisor-reviewed, no further investigation."}}


def _build(docs=None, target=1800.0):
    docs = [_doc()] if docs is None else docs
    ti = {"BWC-1a": {"phase": "incident", "start_epoch": EPOCH},
          "BWC-2": {"phase": "incident", "start_epoch": EPOCH},
          "911 (A)": {"phase": "pre_incident", "start_epoch": None}}
    return bp.build_blueprint(_artifacts(), _timeline(), _verdict(), docs,
                              ["d.json"] if docs else [], _sources(), ti,
                              "Sacramento County Sheriff", target_runtime=target)


# --- manifest ---------------------------------------------------------------

def test_manifest_types_and_ids():
    m = {a["asset_id"]: a for a in _build()["asset_manifest"]}
    assert m["v_bwc1a"]["kind"] == "bodycam"
    assert m["a_911_1"]["kind"] == "911_audio"
    assert m["doc_uofform"]["kind"] == "document"
    assert m["doc_uofform"]["doc_type"] == "uof_form"


def test_manifest_has_transcript_flag():
    m = {a["asset_id"]: a for a in _build()["asset_manifest"]}
    assert m["v_bwc1a"]["has_transcript"] is True    # source maps to it
    assert m["a_911_1"]["has_transcript"] is False   # no transcript source


# --- incident facts ---------------------------------------------------------

def test_incident_facts_from_doc():
    inc = _build()["incident"]
    assert inc["date"] == "04/18/2023"
    assert inc["charges"] == ["PC 459", "PC 148"]
    assert "supervisor-reviewed" in inc["disposition"]
    assert "Vasquez Sonny" in inc["subjects"]


def test_incident_date_falls_back_to_anchor_without_doc():
    inc = _build(docs=[])["incident"]
    assert inc["date"] == "2023-04-18"   # from timeline anchor_iso


# --- beats ------------------------------------------------------------------

def test_beats_one_per_moment_in_chronological_order():
    beats = _build()["beats"]
    assert len(beats) == 3
    # both cams share EPOCH, so order is by timestamp: 50 (reveal) then 85;
    # the no-media moment has no abs time and sorts last.
    assert [b["function"] for b in beats] == ["reveal", "emotional_peak", "contradiction"]
    assert [b["ordinal"] for b in beats] == [0, 1, 2]


def test_beat_primary_asset_and_quote_pinned():
    beats = _build()["beats"]
    peak = next(b for b in beats if b["function"] == "emotional_peak")
    assert peak["primary_asset"]["asset_id"] == "v_bwc1a"
    assert peak["quote"] == {"text": "dog on bite!", "source": "v_bwc1a", "timecode": 85.0}
    assert "v_bwc1a@85" in peak["source_refs"]


def test_unsourced_beat_has_no_primary_asset():
    beats = _build()["beats"]
    orphan = next(b for b in beats if b["function"] == "contradiction")
    assert orphan["primary_asset"] is None
    assert orphan["quote"] is None
    assert orphan["source_refs"] == []


# --- acts -------------------------------------------------------------------

def test_acts_include_cold_open_and_phase_acts():
    acts = {a["act_id"]: a for a in _build()["acts"]}
    assert acts["act_cold_open"]["function"] == "hook"
    assert "act_pre_incident" in acts
    assert "act_incident" in acts
    # investigation act exists because a document is present
    assert "act_investigation" in acts


def test_act_runtime_budget_respects_target():
    acts = _build(target=1800.0)["acts"]
    total = sum(a["target_sec"] for a in acts)
    # cold open + proportional content acts + floors; should be in a sane band
    assert 1500 <= total <= 2200


# --- integrity ledger -------------------------------------------------------

def test_ledger_sources_every_real_beat_and_doc_facts():
    bpd = _build()
    ledger = bpd["integrity_ledger"]
    assert any(e["claim"].startswith("charges") and e["ok"] for e in ledger)
    # the two sourced beats each contribute a footage + quote claim, all ok
    footage_ok = [e for e in ledger if e["claim"].startswith("footage") and e["ok"]]
    assert len(footage_ok) == 2


def test_ledger_flags_unsourced_beat():
    ledger = _build()["integrity_ledger"]
    bad = [e for e in ledger if not e["ok"]]
    assert len(bad) == 1
    assert bad[0]["claim"].startswith("footage")


# --- gaps + metadata --------------------------------------------------------

def test_gaps_flag_missing_stills_and_unsourced_beat():
    gaps = _build()["gaps"]
    missing = " ".join(g["missing"] for g in gaps)
    assert "stills" in missing            # no photo asset
    assert "no resolvable footage" in missing   # the orphan beat


def test_gaps_flag_missing_911_when_absent():
    # drop the 911 artifact -> a pre_incident dispatch gap should appear
    arts = [a for a in _artifacts() if a["kind"] != "911"]
    ti = {"BWC-1a": {"phase": "incident", "start_epoch": EPOCH},
          "BWC-2": {"phase": "incident", "start_epoch": EPOCH}}
    tl = _timeline()
    tl["phases"].pop("pre_incident")
    d = bp.build_blueprint(arts, tl, _verdict(), [_doc()], ["d.json"],
                           _sources(), ti, "Agency")
    assert any(g.get("kind") == "911_audio" for g in d["gaps"])


def test_metadata_runtime_math_and_unsourced_count():
    m = _build()["metadata"]
    assert m["unsourced_beats"] == 1
    assert m["built_by"] == "skeleton"
    assert m["planned_runtime_sec"] > 0
    assert m["runtime_vs_target_sec"] == round(m["planned_runtime_sec"] - 1800.0, 1)


def test_render_markdown_smoke():
    out = bp.render_markdown(_build())
    for section in ["Asset Manifest", "Narrative Spine", "Beat Sheet",
                    "Factual Integrity Ledger", "Gaps & Acquisition"]:
        assert section in out
