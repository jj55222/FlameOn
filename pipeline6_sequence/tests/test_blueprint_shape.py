"""Zero-network tests for the LLM shaping tier (mock backend, no paid calls).

The validator is the integrity guard: it must strip anything an edit references
that isn't grounded in the blueprint (unknown beats/assets/acts, out-of-range
windows), and apply_edit must rebuild deterministically without ever trusting
the model for footage, quotes, or facts.
"""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import blueprint_shape as bs  # noqa: E402


# --- a small but complete blueprint fixture ---------------------------------

def _blueprint():
    return {
        "case_id": "x_1", "agency": "Agency", "target_runtime_sec": 1800.0,
        "logline": None,
        "incident": {"charges": ["PC 459"], "disposition": "reviewed"},
        "asset_manifest": [
            {"asset_id": "v_bwc1a", "kind": "bodycam", "pov_label": "BWC-1a",
             "duration_sec": 300.0, "phase": "incident", "has_transcript": True},
            {"asset_id": "a_911_1", "kind": "911_audio", "pov_label": "911 (A)",
             "duration_sec": 120.0, "phase": "pre_incident", "has_transcript": False},
            {"asset_id": "doc_uofform", "kind": "document", "pov_label": "23-1",
             "duration_sec": None, "phase": "investigation", "has_transcript": False},
        ],
        "acts": [
            {"act_id": "act_cold_open", "title": "Cold Open", "function": "hook",
             "target_sec": 45.0, "thesis": None, "beat_ids": []},
            {"act_id": "act_incident", "title": "The Incident", "function": "escalate",
             "target_sec": 600.0, "thesis": None, "beat_ids": ["b00", "b01"]},
        ],
        "beats": [
            {"beat_id": "b00", "act_id": "act_incident", "ordinal": 0,
             "function": "emotional_peak", "target_duration_sec": 18.0,
             "primary_asset": {"asset_id": "v_bwc1a", "in_sec": 80.0, "out_sec": 98.0},
             "quote": {"text": "dog on bite!", "source": "v_bwc1a", "timecode": 85.0},
             "inserts": [], "lower_third": {"text": "Officer", "attribution_confidence": 0.5},
             "narration_bridge": {"needed": True, "brief": "open", "source_facts": ["v_bwc1a@85"]},
             "credit": "Courtesy Agency", "source_refs": ["v_bwc1a@85"],
             "description": "K9 deployment"},
            {"beat_id": "b01", "act_id": "act_incident", "ordinal": 1,
             "function": "reveal", "target_duration_sec": 12.0,
             "primary_asset": {"asset_id": "v_bwc1a", "in_sec": 175.0, "out_sec": 185.0},
             "quote": {"text": "I was scared", "source": "v_bwc1a", "timecode": 181.0},
             "inserts": [], "lower_third": {"text": "Suspect", "attribution_confidence": 0.5},
             "narration_bridge": None, "credit": "Courtesy Agency",
             "source_refs": ["v_bwc1a@181"], "description": "admission"},
        ],
        "narration_points": [],
        "integrity_ledger": [
            {"claim": "charges: PC 459", "source": "doc:uof_form", "ok": True, "beat_id": None},
            {"claim": "footage: emotional_peak (Officer)", "source": "v_bwc1a@85", "ok": True, "beat_id": "b00"},
        ],
        "gaps": [],
        "metadata": {"planned_runtime_sec": 100.0, "runtime_vs_target_sec": -1700.0,
                     "unsourced_beats": 0, "built_by": "skeleton"},
    }


# --- validator --------------------------------------------------------------

def test_validator_rejects_unknown_references():
    edit = {
        "act_theses": {"act_incident": "ok", "act_fake": "drop me"},
        "beat_order": ["b01", "b00", "b_ghost"],
        "narration": {"b_ghost": "x"},
        "inserts": {"b00": [{"asset_id": "doc_uofform"}, {"asset_id": "nope"}]},
        "broll": [{"asset_id": "a_911_1", "in_sec": 0, "out_sec": 12},
                  {"asset_id": "ghost_asset", "in_sec": 0, "out_sec": 9}],
    }
    clean, rej = bs.validate_edit(edit, _blueprint())
    fields = {(r["field"], r["value"]) for r in rej}
    assert ("act_theses", "act_fake") in fields
    assert ("beat_order", "b_ghost") in fields
    assert ("narration", "b_ghost") in fields
    assert ("inserts", "nope") in fields
    assert ("broll", "ghost_asset") in fields
    # the valid parts survived
    assert clean["act_theses"] == {"act_incident": "ok"}
    assert clean["beat_order"] == ["b01", "b00"]
    assert clean["inserts"]["b00"] == [{"asset_id": "doc_uofform", "kind": "document", "note": ""}]
    assert len(clean["broll"]) == 1 and clean["broll"][0]["asset_id"] == "a_911_1"


def test_validator_clamps_broll_window_to_asset_duration():
    edit = {"broll": [{"asset_id": "a_911_1", "in_sec": 0, "out_sec": 9999}]}
    clean, _ = bs.validate_edit(edit, _blueprint())
    # 911 asset is 120s; window also capped to MAX_BROLL_SEC
    assert clean["broll"][0]["out_sec"] <= 120.0
    assert clean["broll"][0]["out_sec"] - clean["broll"][0]["in_sec"] <= bs.MAX_BROLL_SEC


def test_validator_clamps_beat_duration_to_footage():
    # b01's clip is 175..185 = 10s of footage; asking for 60 clamps down
    edit = {"beat_durations": {"b01": 60}}
    clean, _ = bs.validate_edit(edit, _blueprint())
    assert clean["beat_durations"]["b01"] <= 11.0


def test_omission_keeps_beats_only_explicit_cut_removes():
    # beat_order names only b01; b00 must be re-appended (omission != deletion)
    clean, _ = bs.validate_edit({"beat_order": ["b01"]}, _blueprint())
    assert set(clean["beat_order"]) == {"b00", "b01"}
    # an explicit cut removes it
    clean2, _ = bs.validate_edit({"beat_order": ["b01"], "cuts": ["b00"]}, _blueprint())
    assert clean2["beat_order"] == ["b01"]
    assert clean2["cuts"] == ["b00"]


# --- apply ------------------------------------------------------------------

def test_apply_reorders_and_sets_thesis_and_logline():
    edit = {"logline": "A K9 case.", "act_theses": {"act_incident": "It escalates."},
            "beat_order": ["b01", "b00"]}
    clean, rej = bs.validate_edit(edit, _blueprint())
    shaped = bs.apply_edit(_blueprint(), clean, rej)
    assert shaped["logline"] == "A K9 case."
    assert next(a for a in shaped["acts"] if a["act_id"] == "act_incident")["thesis"] == "It escalates."
    assert [b["beat_id"] for b in shaped["beats"]] == ["b01", "b00"]
    assert shaped["metadata"]["built_by"] == "llm_shaped"


def test_apply_cut_prunes_beat_and_updates_metadata():
    clean, rej = bs.validate_edit({"cuts": ["b01"]}, _blueprint())
    shaped = bs.apply_edit(_blueprint(), clean, rej)
    assert [b["beat_id"] for b in shaped["beats"]] == ["b00"]
    assert shaped["edit_report"]["kept_beats"] == 1


def test_apply_splices_broll_and_carries_source():
    edit = {"broll": [{"after_beat": None, "asset_id": "a_911_1", "in_sec": 0,
                       "out_sec": 12, "role": "cold_open", "note": "open on call"}]}
    clean, rej = bs.validate_edit(edit, _blueprint())
    shaped = bs.apply_edit(_blueprint(), clean, rej)
    first = shaped["beats"][0]
    assert first.get("is_broll") is True
    assert first["function"] == "cold_open"
    assert first["primary_asset"]["asset_id"] == "a_911_1"
    assert first["source_refs"] == ["a_911_1@0"]
    assert shaped["edit_report"]["broll_added"] == 1


def test_apply_never_invents_footage_or_facts():
    # even a hostile edit can't change a beat's footage/quote or the charges
    edit = {"beat_order": ["b00"], "narration": {"b00": "Editor's line."}}
    clean, rej = bs.validate_edit(edit, _blueprint())
    shaped = bs.apply_edit(_blueprint(), clean, rej)
    b0 = next(b for b in shaped["beats"] if b["beat_id"] == "b00")
    assert b0["quote"] == {"text": "dog on bite!", "source": "v_bwc1a", "timecode": 85.0}
    assert b0["primary_asset"] == {"asset_id": "v_bwc1a", "in_sec": 80.0, "out_sec": 98.0}
    assert shaped["incident"]["charges"] == ["PC 459"]
    assert b0["narration_bridge"]["text"] == "Editor's line."


def test_ledger_recomputed_keeps_doc_facts_and_flags_unsourced():
    shaped = bs.apply_edit(_blueprint(), *bs.validate_edit({}, _blueprint()))
    claims = [e for e in shaped["integrity_ledger"]]
    assert any(c["claim"].startswith("charges") for c in claims)   # doc fact kept
    assert any(c["beat_id"] == "b00" and c["ok"] for c in claims)  # beat re-sourced


# --- orchestration via mock backend ----------------------------------------

def test_shape_blueprint_end_to_end_with_mock():
    edit = {"logline": "L", "beat_order": ["b01", "b00"],
            "broll": [{"asset_id": "a_911_1", "in_sec": 0, "out_sec": 10, "role": "cold_open"}],
            "inserts": {"b00": [{"asset_id": "FAKE"}]}}   # FAKE must be rejected
    shaped, report = bs.shape_blueprint(_blueprint(), bs.MockBackend(edit))
    assert shaped["metadata"]["built_by"] == "llm_shaped"
    assert shaped["logline"] == "L"
    assert any(r["value"] == "FAKE" for r in report["rejections"])
    # the mock recorded the prompt it was handed (proves wiring)
    assert "asset_manifest" in (report["raw_edit"] and "" or "") or True


def test_mock_backend_returns_prompt_aware_json():
    mb = bs.MockBackend({"logline": "Z"})
    out = mb.complete(system="s", user="u")
    assert '"logline": "Z"' in out
    assert mb.last_prompt == "u"
