"""EVAL7 — pilot + validation scoreboard tests.

Asserts that ``build_pilot_validation_scoreboard`` :

- merges DATA2 validation runner output and PILOT2 readiness output
  into a single deterministic scoreboard
- exposes the canonical top-level shape (validation, pilots,
  connector_demand, expected_artifact_types,
  media_required_for_produce_count, total_planned_max_live_calls,
  warnings)
- handles None / empty input on either side without raising
- emits ``over_budget_pilot:<id>`` for budget violations
- emits ``paid_connector_in_pilot:<id>`` when a paid connector is
  listed
- emits ``missing_media_required_for_produce:<id>`` for the
  media-gate policy violation
- emits ``downloads_enabled_in_pilot``, ``scraping_enabled_in_pilot``,
  ``llm_enabled_in_pilot`` for those policy violations
- emits ``validation_false_produce`` when validation has any false
  PRODUCE
- emits ``validation_guard_counter_nonzero:<counter>`` when any guard
  counter is non-zero
- aggregates connector_demand (per-connector pilot count) and
  expected_artifact_types (per-type pilot count) deterministically
- counts media_required_for_produce_count correctly
- sums total_planned_max_live_calls correctly
- end-to-end against real run_validation_manifest +
  run_pilot_manifest output
- never makes a network call
- output is JSON-serializable round-trip
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    build_pilot_validation_scoreboard,
    run_pilot_manifest,
    run_validation_manifest,
)


ROOT = Path(__file__).resolve().parents[1]
VALIDATION_MANIFEST = ROOT / "tests" / "fixtures" / "validation_manifest.json"
PILOT_MANIFEST = ROOT / "tests" / "fixtures" / "pilot_cases" / "pilot_manifest.json"


REQUIRED_TOP_KEYS = (
    "validation",
    "pilots",
    "connector_demand",
    "expected_artifact_types",
    "media_required_for_produce_count",
    "total_planned_max_live_calls",
    "warnings",
)


def _make_pilot_result(
    *,
    id_,
    status="ready_for_live_smoke",
    allowed_connectors=None,
    artifact_types=None,
    media_required=True,
    max_live_calls=1,
    policy_violations=None,
    budget_violations=None,
    next_actions=None,
):
    return {
        "id": id_,
        "input_type": "manual",
        "seed_fixture_path": "tests/fixtures/casegraph_scenarios/media_rich_produce.json",
        "expected_minimum": {
            "identity_lock_required": True,
            "outcome_required": True,
            "media_required_for_produce": media_required,
            "artifact_types_desired": list(artifact_types or ["bodycam"]),
        },
        "expected_verdict_without_live": "PRODUCE",
        "actual_dry_verdict": "PRODUCE",
        "verdict_match": True,
        "research_completeness_score": 100.0,
        "production_actionability_score": 80.0,
        "actionability_score": 90.0,
        "verified_artifact_count": 3,
        "media_artifact_count": 2,
        "document_artifact_count": 1,
        "identity_confidence": "high",
        "outcome_status": "sentenced",
        "satisfied_gates": {},
        "missing_gates": [],
        "allowed_connectors": list(allowed_connectors or ["courtlistener"]),
        "max_live_calls": max_live_calls,
        "max_results_per_connector": 5,
        "policy_violations": list(policy_violations or []),
        "budget_violations": list(budget_violations or []),
        "readiness_status": status,
        "next_actions": list(next_actions or []),
    }


def _wrap_pilot_output(results):
    by_status = {}
    for r in results:
        by_status[r["readiness_status"]] = by_status.get(r["readiness_status"], 0) + 1
    return {
        "manifest_version": 1,
        "global_constraints": {},
        "total_pilots": len(results),
        "results": results,
        "summary": {
            "total_pilots": len(results),
            "ready_count": sum(
                1 for r in results if r["readiness_status"] == "ready_for_live_smoke"
            ),
            "blocked_count": sum(
                1 for r in results if r["readiness_status"] != "ready_for_live_smoke"
            ),
            "by_readiness_status": by_status,
            "any_paid_connectors": False,
            "any_downloads_enabled": False,
            "any_scraping_enabled": False,
            "any_llm_enabled": False,
            "any_missing_media_required": False,
            "total_planned_live_calls": sum(int(r["max_live_calls"] or 0) for r in results),
        },
    }


def _wrap_validation_output(results):
    return {
        "manifest_version": 1,
        "manifest_path": "synthetic",
        "total_entries": len(results),
        "results": results,
        "summary": {
            "total": len(results),
            "passed": sum(1 for r in results if r.get("passed", True)),
            "failed": sum(1 for r in results if not r.get("passed", True)),
            "verdict_counts": {"PRODUCE": 0, "HOLD": 0, "SKIP": 0},
            "expected_verdict_counts": {"PRODUCE": 0, "HOLD": 0, "SKIP": 0},
            "false_produce_count": 0,
            "document_only_produce_count": 0,
            "claim_only_produce_count": 0,
            "weak_identity_produce_count": 0,
            "protected_or_pacer_produce_count": 0,
        },
    }


# ---- Empty / shape --------------------------------------------------------


def test_scoreboard_empty_inputs_returns_zero_default():
    sb = build_pilot_validation_scoreboard()
    for key in REQUIRED_TOP_KEYS:
        assert key in sb
    assert sb["validation"]["total_entries"] == 0
    assert sb["pilots"]["total"] == 0
    assert sb["warnings"] == []
    assert sb["connector_demand"] == {}
    assert sb["expected_artifact_types"] == {}


def test_scoreboard_canonical_shape_with_real_data():
    val = run_validation_manifest(VALIDATION_MANIFEST)
    pilot = run_pilot_manifest(PILOT_MANIFEST)
    sb = build_pilot_validation_scoreboard(
        validation_output=val, pilot_output=pilot
    )
    for key in REQUIRED_TOP_KEYS:
        assert key in sb


# ---- Real data ------------------------------------------------------------


def test_scoreboard_real_data_pilots_all_ready_no_warnings():
    val = run_validation_manifest(VALIDATION_MANIFEST)
    pilot = run_pilot_manifest(PILOT_MANIFEST)
    sb = build_pilot_validation_scoreboard(validation_output=val, pilot_output=pilot)
    assert sb["validation"]["accuracy_pct"] == 100.0
    assert sb["validation"]["false_produce_count"] == 0
    assert sb["validation"]["guard_counters_all_zero"] is True
    assert sb["pilots"]["ready_for_live"] == sb["pilots"]["total"]
    assert sb["pilots"]["blocked"] == 0
    assert sb["pilots"]["blocked_pilots"] == []
    assert sb["warnings"] == []


def test_scoreboard_real_data_aggregates_connector_demand():
    val = run_validation_manifest(VALIDATION_MANIFEST)
    pilot = run_pilot_manifest(PILOT_MANIFEST)
    sb = build_pilot_validation_scoreboard(validation_output=val, pilot_output=pilot)
    # Every connector named must be in the free list (paid connectors
    # would have been flagged as a warning).
    for connector in sb["connector_demand"].keys():
        assert connector in {"courtlistener", "documentcloud", "muckrock", "youtube"}
    assert sum(sb["connector_demand"].values()) >= len(pilot["results"])


def test_scoreboard_real_data_aggregates_expected_artifact_types():
    val = run_validation_manifest(VALIDATION_MANIFEST)
    pilot = run_pilot_manifest(PILOT_MANIFEST)
    sb = build_pilot_validation_scoreboard(validation_output=val, pilot_output=pilot)
    # The pilot manifest declares artifact_types_desired across all
    # pilots; the scoreboard rolls them up.
    assert isinstance(sb["expected_artifact_types"], dict)
    assert sum(sb["expected_artifact_types"].values()) >= len(pilot["results"])


def test_scoreboard_real_data_media_required_count_matches_pilot_total():
    """Every pilot in the committed manifest declares
    media_required_for_produce=true; the scoreboard count must equal
    the pilot total."""
    val = run_validation_manifest(VALIDATION_MANIFEST)
    pilot = run_pilot_manifest(PILOT_MANIFEST)
    sb = build_pilot_validation_scoreboard(validation_output=val, pilot_output=pilot)
    assert sb["media_required_for_produce_count"] == sb["pilots"]["total"]


def test_scoreboard_real_data_total_planned_live_calls_sums():
    val = run_validation_manifest(VALIDATION_MANIFEST)
    pilot = run_pilot_manifest(PILOT_MANIFEST)
    sb = build_pilot_validation_scoreboard(validation_output=val, pilot_output=pilot)
    expected = sum(int(p["max_live_calls"] or 0) for p in pilot["results"])
    assert sb["total_planned_max_live_calls"] == expected


# ---- Warnings -------------------------------------------------------------


def test_scoreboard_flags_over_budget_pilot():
    pilots = [
        _make_pilot_result(
            id_="over_budget",
            status="blocked_invalid_budget",
            budget_violations=["max_live_calls_over_envelope:99>6"],
        ),
    ]
    sb = build_pilot_validation_scoreboard(
        validation_output=_wrap_validation_output([]),
        pilot_output=_wrap_pilot_output(pilots),
    )
    assert any(w.startswith("over_budget_pilot:over_budget") for w in sb["warnings"])
    assert sb["pilots"]["blocked"] == 1
    assert any(b["id"] == "over_budget" for b in sb["pilots"]["blocked_pilots"])


def test_scoreboard_flags_paid_connector_in_pilot():
    pilots = [
        _make_pilot_result(
            id_="leaked_paid",
            status="blocked_policy",
            policy_violations=["paid_connectors_listed:brave"],
        ),
    ]
    sb = build_pilot_validation_scoreboard(
        validation_output=_wrap_validation_output([]),
        pilot_output=_wrap_pilot_output(pilots),
    )
    assert "paid_connector_in_pilot:leaked_paid" in sb["warnings"]


def test_scoreboard_flags_missing_media_required_for_produce():
    pilots = [
        _make_pilot_result(
            id_="no_media_gate",
            status="blocked_policy",
            media_required=False,
            policy_violations=["media_required_for_produce_false"],
        ),
    ]
    sb = build_pilot_validation_scoreboard(
        validation_output=_wrap_validation_output([]),
        pilot_output=_wrap_pilot_output(pilots),
    )
    assert "missing_media_required_for_produce:no_media_gate" in sb["warnings"]
    assert sb["media_required_for_produce_count"] == 0


def test_scoreboard_flags_downloads_scraping_llm_enabled():
    pilots = [
        _make_pilot_result(
            id_="dl_pilot",
            status="blocked_policy",
            policy_violations=["allow_downloads_true"],
        ),
        _make_pilot_result(
            id_="scrape_pilot",
            status="blocked_policy",
            policy_violations=["allow_scraping_true"],
        ),
        _make_pilot_result(
            id_="llm_pilot",
            status="blocked_policy",
            policy_violations=["allow_llm_true"],
        ),
    ]
    sb = build_pilot_validation_scoreboard(
        validation_output=_wrap_validation_output([]),
        pilot_output=_wrap_pilot_output(pilots),
    )
    assert "downloads_enabled_in_pilot:dl_pilot" in sb["warnings"]
    assert "scraping_enabled_in_pilot:scrape_pilot" in sb["warnings"]
    assert "llm_enabled_in_pilot:llm_pilot" in sb["warnings"]


def test_scoreboard_flags_validation_false_produce():
    """A synthetic validation result where actual=PRODUCE but
    expected=HOLD should trigger validation_false_produce in
    warnings."""
    val_results = [
        {
            "id": "synthetic_false_produce",
            "fixture_path": "x",
            "expected_verdict": "HOLD",
            "actual_verdict": "PRODUCE",
            "passed": False,
            "fail_reasons": ["verdict mismatch"],
            "reason_codes": [],
            "risk_flags": [],
            "next_actions": [],
            "reason_code_matches": {
                "must_include_present": [],
                "must_include_missing": [],
                "must_not_include_present": [],
            },
            "risk_flag_matches": {
                "must_include_present": [],
                "must_include_missing": [],
                "must_not_include_present": [],
            },
            "research_completeness_score": 0.0,
            "production_actionability_score": 0.0,
            "actionability_score": 0.0,
            "verified_artifact_count": 1,
            "media_artifact_count": 1,
            "document_artifact_count": 0,
            "input_type": "manual",
            "identity_confidence": "high",
            "outcome_status": "sentenced",
            "bundle_path": None,
        },
    ]
    sb = build_pilot_validation_scoreboard(
        validation_output=_wrap_validation_output(val_results),
        pilot_output=_wrap_pilot_output([]),
    )
    assert "validation_false_produce" in sb["warnings"]


def test_scoreboard_flags_validation_guard_counter_nonzero():
    val_results = [
        {
            "id": "synthetic_doc_only_produce",
            "fixture_path": "x",
            "expected_verdict": "PRODUCE",
            "actual_verdict": "PRODUCE",
            "passed": True,
            "fail_reasons": [],
            "reason_codes": [],
            "risk_flags": [],
            "next_actions": [],
            "reason_code_matches": {
                "must_include_present": [],
                "must_include_missing": [],
                "must_not_include_present": [],
            },
            "risk_flag_matches": {
                "must_include_present": [],
                "must_include_missing": [],
                "must_not_include_present": [],
            },
            "research_completeness_score": 0.0,
            "production_actionability_score": 0.0,
            "actionability_score": 0.0,
            "verified_artifact_count": 2,
            "media_artifact_count": 0,
            "document_artifact_count": 2,
            "input_type": "manual",
            "identity_confidence": "high",
            "outcome_status": "sentenced",
            "bundle_path": None,
        },
    ]
    sb = build_pilot_validation_scoreboard(
        validation_output=_wrap_validation_output(val_results),
        pilot_output=_wrap_pilot_output([]),
    )
    # PRODUCE without any media should bump document_only_produce_count
    # in the metrics, which the scoreboard turns into a warning.
    assert any(
        w.startswith("validation_guard_counter_nonzero:document_only_produce_count")
        for w in sb["warnings"]
    )


def test_scoreboard_warnings_are_sorted_and_deduplicated():
    pilots = [
        _make_pilot_result(
            id_="b",
            status="blocked_policy",
            policy_violations=["allow_downloads_true"],
        ),
        _make_pilot_result(
            id_="a",
            status="blocked_policy",
            policy_violations=["allow_downloads_true"],
        ),
    ]
    sb = build_pilot_validation_scoreboard(
        validation_output=_wrap_validation_output([]),
        pilot_output=_wrap_pilot_output(pilots),
    )
    # Two distinct warnings, sorted ascending by id.
    expected = [
        "downloads_enabled_in_pilot:a",
        "downloads_enabled_in_pilot:b",
    ]
    actual = [w for w in sb["warnings"] if w.startswith("downloads_enabled_in_pilot:")]
    assert actual == expected


def test_scoreboard_blocked_pilots_capped_by_top_n_blocked():
    pilots = [
        _make_pilot_result(
            id_=f"blocked_{i}",
            status="blocked_invalid_budget",
            budget_violations=["max_live_calls_over_envelope:99>6"],
        )
        for i in range(15)
    ]
    sb = build_pilot_validation_scoreboard(
        validation_output=_wrap_validation_output([]),
        pilot_output=_wrap_pilot_output(pilots),
        top_n_blocked=5,
    )
    assert len(sb["pilots"]["blocked_pilots"]) == 5


# ---- JSON + network -------------------------------------------------------


def test_scoreboard_output_is_json_serializable():
    val = run_validation_manifest(VALIDATION_MANIFEST)
    pilot = run_pilot_manifest(PILOT_MANIFEST)
    sb = build_pilot_validation_scoreboard(validation_output=val, pilot_output=pilot)
    encoded = json.dumps(sb)
    decoded = json.loads(encoded)
    assert decoded == sb


def test_scoreboard_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    val = run_validation_manifest(VALIDATION_MANIFEST)
    pilot = run_pilot_manifest(PILOT_MANIFEST)
    build_pilot_validation_scoreboard(validation_output=val, pilot_output=pilot)
    assert calls == [], f"scoreboard triggered {len(calls)} live HTTP call(s)"
