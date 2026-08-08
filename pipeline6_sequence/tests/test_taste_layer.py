"""Zero-network tests for the P6 editorial-taste layer."""
from __future__ import annotations

import json
import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import blueprint as B  # noqa: E402
import blueprint_shape as BS  # noqa: E402
import judge as J  # noqa: E402
import taste_gate as TG  # noqa: E402
import thesis_gate as TH  # noqa: E402


def _beat(bid="b0", fn="reveal", aid="v_1", phase="incident", quote="verbatim"):
    return {"beat_id": bid, "ordinal": 0, "act_id": f"act_{phase}", "_phase": phase,
            "function": fn, "primary_asset": {"asset_id": aid, "in_sec": 1, "out_sec": 8},
            "source_refs": [f"{aid}@2"], "quote": {"text": quote} if quote else None,
            "target_duration_sec": 7, "lower_third": {"text": "Officer A"}}


def _treatment(route="standoff_tactical"):
    return {"schema_version": 1, "treatment_id": "t1", "format": "longform",
            "platform": "youtube", "route_id": route, "structure_only": True,
            "acts": [{"id": "open", "title": "Open", "phase": "incident", "function": "hook",
                      "span_pct": [0, 100], "beat_functions": ["reveal"],
                      "evidence_slots": [{"slot_id": "payoff", "required": True,
                                          "accepted_moment_types": ["reveal"],
                                          "accepted_media_kinds": ["bodycam"],
                                          "minimum_count": 1, "must_have_source": True,
                                          "must_be_verbatim": True,
                                          "card_substitution_allowed": False}],
                      "promise": "A real event", "payoff": "The event resolves"}]}


def _bp(route="standoff_tactical", missing=None):
    treatment = _treatment(route)
    return {"case_id": "c", "target_runtime_sec": 60, "agency": "A",
            "incident": {"disposition": None, "disposition_source": None},
            "asset_manifest": [{"asset_id": "v_1", "kind": "bodycam", "pov_label": "Officer A"}],
            "acts": [], "beats": [], "metadata": {"planned_runtime_sec": 60},
            "reference_treatment": treatment,
            "treatment_validation": {"missing_required_slots": missing or [],
                                      "all_required_slots_covered": not missing}}


def test_reference_treatment_loads_from_canonical_cards():
    t = B.load_template("standoff_tactical_longform_v1")
    assert t["treatment_id"] == "standoff_tactical_longform_v1"
    assert t["structure_only"] is True


def test_treatment_binds_required_evidence_and_carries_rich_fields():
    treatment = _treatment()
    beats = [_beat()]
    manifest = [{"asset_id": "v_1", "kind": "bodycam"}]
    acts = B.build_acts_from_template(treatment, beats, 100, manifest=manifest)
    assert acts[0]["beat_ids"] == ["b0"]
    assert acts[0]["promise"] == "A real event"
    check = B.treatment_validation(treatment, acts, beats, manifest)
    assert check["all_required_slots_covered"] is True
    assert check["bindings"][0]["beat_ids"] == ["b0"]


def test_treatment_validation_names_unbound_required_slot():
    treatment = _treatment()
    beats = [_beat(fn="detail_noticed")]
    manifest = [{"asset_id": "v_1", "kind": "bodycam"}]
    acts = B.build_acts_from_template(treatment, beats, 100, manifest=manifest)
    check = B.treatment_validation(treatment, acts, beats, manifest)
    assert check["all_required_slots_covered"] is False
    assert check["missing_required_slots"][0]["slot_id"] == "payoff"


def test_shape_prompt_keeps_treatment_separate_from_grammar():
    bp = _bp()
    bp["acts"] = B.build_acts_from_template(_treatment(), [_beat()], 60,
                                             manifest=bp["asset_manifest"])
    bp["beats"] = [{k: v for k, v in _beat().items() if not k.startswith("_")}]
    prompt = BS._build_prompt(bp, grammar={"channel": "X", "phase_x_move": {}},
                              reference_treatment=_treatment())
    payload = json.loads(prompt[prompt.index("{"):])
    assert payload["REFERENCE_TREATMENT"]["treatment_id"] == "t1"
    assert payload["acts"][0]["evidence_slots"][0]["slot_id"] == "payoff"


def test_thesis_gate_rejects_topical_not_mutually_exclusive_contradiction():
    bp = _bp("interrogation_contradiction")
    contract = {"case_shape_card": {"chosen_thesis": "His story changes.",
                                     "production_route": "interrogation_contradiction"},
                "contradiction_present": True,
                "moment_slate": [{"moment_id": "M1", "moment_type": "contradiction",
                                  "summary": "Two statements about the same subject."}]}
    got = TH.evaluate_thesis(bp, contract)
    assert got["verdict"] == "REVISE_THESIS"
    assert got["reasons"][0]["code"] == "contradiction_not_mutually_exclusive"


def test_thesis_gate_requires_disposition_citation_for_accountability():
    bp = _bp("accountability_prosecution")
    bp["incident"]["disposition"] = "SUSTAINED"
    contract = {"case_shape_card": {"chosen_thesis": "The finding changed the case.",
                                     "production_route": "accountability_prosecution"}}
    got = TH.evaluate_thesis(bp, contract)
    assert got["verdict"] == "MISSING_EVIDENCE"
    assert any(r["code"] == "disposition_uncited" for r in got["reasons"])


def _rules(*rules):
    return {"rule_set_id": "x", "rules": list(rules)}


def _rule(rid, typ, fail="REJECT", params=None, formats=("longform", "shortform")):
    return {"rule_id": rid, "enabled": True,
            "scope": {"formats": list(formats), "platforms": ["youtube", "tiktok", "instagram"]},
            "check": {"type": typ, "parameters": params or {}}, "on_fail": fail,
            "reason": rid, "source_refs": ["operator"]}


def test_taste_gate_rejects_internal_trace_language():
    rules = _rules(_rule("trace", "forbidden_text", params={"patterns": ["paper edit"]}))
    pe = {"timeline": [{"kind": "card", "title": "The paper edit says..."}]}
    got = TG.evaluate_rules(rules, _bp(), pe, fmt="longform", platform="youtube")
    assert got["verdict"] == "REJECT" and got["failed_rule_ids"] == ["trace"]


def test_short_audio_only_fails_visual_and_structure_floor():
    rules = _rules(_rule("visual", "minimum_visual_story_beats", params={
        "minimum_count": 1, "accepted_functions": ["hook", "reveal", "climax", "payoff"]},
        formats=("shortform",)))
    pe = {"timeline": [{"kind": "clip", "asset_kind": "911_audio", "function": "hook",
                        "in_sec": 0, "out_sec": 10}]}
    got = TG.evaluate_rules(rules, _bp(), pe, fmt="shortform", platform="youtube")
    assert got["verdict"] == "REJECT"
    assert {"visual", "short_hook_context_payoff"}.issubset(set(got["failed_rule_ids"]))


def test_enum_critic_has_no_scores():
    bp = _bp()
    bp["beats"] = []
    result = J.deterministic_enum_judgment(bp, paper_edit={"timeline": []})
    assert result["verdict"] in {"SHIP", "REVISE", "ABSTAIN", "REJECT"}
    assert "scores" not in result
