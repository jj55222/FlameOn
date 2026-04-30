"""PILOT3 — known-case live-pilot selection tests.

Asserts that ``select_pilot_for_live_smoke``:

- selects the highest-scoring ready pilot deterministically
- never selects a pilot that is not ``ready_for_live_smoke``
- respects max_live_calls (lower budget ranks higher)
- prefers pilots that include a connector in PROVEN_ARTIFACT_CONNECTORS
- returns ``no_ready_pilot=True`` when no ready pilot exists
- emits a structured rationale list
- never makes a network call
- end-to-end against the committed pilot manifest selects the
  ``mpv_documentcloud_pilot`` (single connector, lowest budget,
  proven artifact-yield path)
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    run_pilot_manifest,
    select_pilot_for_live_smoke,
)
from pipeline2_discovery.casegraph.pilots import (
    PROVEN_ARTIFACT_CONNECTORS,
    READINESS_BUDGET,
    READINESS_POLICY,
    READINESS_READY,
)


ROOT = Path(__file__).resolve().parents[1]
PILOT_MANIFEST = ROOT / "tests" / "fixtures" / "pilot_cases" / "pilot_manifest.json"


def _make_pilot_result(
    *,
    id_,
    status=READINESS_READY,
    allowed_connectors=None,
    max_live_calls=1,
    expected_verdict_without_live="HOLD",
    actual_dry_verdict=None,
    missing_gates=None,
):
    if actual_dry_verdict is None:
        actual_dry_verdict = expected_verdict_without_live
    return {
        "id": id_,
        "input_type": "manual",
        "seed_fixture_path": "tests/fixtures/casegraph_scenarios/media_rich_produce.json",
        "expected_minimum": {
            "media_required_for_produce": True,
            "artifact_types_desired": ["bodycam"],
        },
        "expected_verdict_without_live": expected_verdict_without_live,
        "actual_dry_verdict": actual_dry_verdict,
        "verdict_match": expected_verdict_without_live == actual_dry_verdict,
        "missing_gates": list(missing_gates or []),
        "allowed_connectors": list(allowed_connectors or ["courtlistener"]),
        "max_live_calls": max_live_calls,
        "max_results_per_connector": 5,
        "policy_violations": [],
        "budget_violations": [],
        "readiness_status": status,
        "next_actions": [],
    }


def _wrap(results):
    return {
        "manifest_version": 1,
        "global_constraints": {},
        "total_pilots": len(results),
        "results": results,
        "summary": {
            "total_pilots": len(results),
            "ready_count": sum(1 for r in results if r["readiness_status"] == READINESS_READY),
            "blocked_count": sum(1 for r in results if r["readiness_status"] != READINESS_READY),
            "by_readiness_status": {},
            "any_paid_connectors": False,
            "any_downloads_enabled": False,
            "any_scraping_enabled": False,
            "any_llm_enabled": False,
            "any_missing_media_required": False,
            "total_planned_live_calls": sum(r["max_live_calls"] for r in results),
        },
    }


# ---- Real manifest --------------------------------------------------------


def test_real_manifest_selects_mpv_documentcloud_pilot():
    """The committed pilot manifest's first-attempt winner should be
    mpv_documentcloud_pilot: single connector documentcloud (proven
    artifact-yield path), lowest budget tied at 1, structured-row
    seed that defaults to SKIP without live."""
    out = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    assert out["selected_pilot_id"] == "mpv_documentcloud_pilot"
    assert out["allowed_connectors"] == ["documentcloud"]
    assert out["max_live_calls"] == 1
    assert out["max_results_per_connector"] == 5
    assert out["no_ready_pilot"] is False


def test_real_manifest_selection_rationale_is_structured():
    out = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    rationale = out["rationale"]
    assert isinstance(rationale, list)
    assert len(rationale) >= 2
    assert all(isinstance(r, str) for r in rationale)


def test_real_manifest_selection_score_positive():
    out = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    assert out["selection_score"] > 0


# ---- Synthetic ranking -----------------------------------------------------


def test_blocked_pilot_never_selected():
    """A blocked-policy pilot must never be selected, even if it would
    otherwise score higher."""
    pilots = [
        _make_pilot_result(id_="blocked_paid", status=READINESS_POLICY,
                           allowed_connectors=["documentcloud"], max_live_calls=1),
        _make_pilot_result(id_="ready_other", allowed_connectors=["courtlistener"],
                           max_live_calls=1),
    ]
    out = select_pilot_for_live_smoke(pilot_output=_wrap(pilots))
    assert out["selected_pilot_id"] == "ready_other"


def test_no_ready_pilot_returns_flag():
    pilots = [
        _make_pilot_result(id_="b1", status=READINESS_POLICY),
        _make_pilot_result(id_="b2", status=READINESS_BUDGET),
    ]
    out = select_pilot_for_live_smoke(pilot_output=_wrap(pilots))
    assert out["selected_pilot_id"] is None
    assert out["no_ready_pilot"] is True
    assert out["candidate_count"] == 0


def test_proven_artifact_connector_outranks_others():
    pilots = [
        _make_pilot_result(id_="cl_pilot", allowed_connectors=["courtlistener"],
                           max_live_calls=1),
        _make_pilot_result(id_="dc_pilot", allowed_connectors=["documentcloud"],
                           max_live_calls=1),
    ]
    out = select_pilot_for_live_smoke(pilot_output=_wrap(pilots))
    assert out["selected_pilot_id"] == "dc_pilot"


def test_lower_budget_outranks_higher_budget_when_equal_yield():
    pilots = [
        _make_pilot_result(id_="dc_two_calls",
                           allowed_connectors=["documentcloud", "courtlistener"],
                           max_live_calls=2),
        _make_pilot_result(id_="dc_one_call",
                           allowed_connectors=["documentcloud"],
                           max_live_calls=1),
    ]
    out = select_pilot_for_live_smoke(pilot_output=_wrap(pilots))
    assert out["selected_pilot_id"] == "dc_one_call"


def test_alphabetical_tiebreaker_when_scores_equal():
    pilots = [
        _make_pilot_result(id_="zebra", allowed_connectors=["documentcloud"],
                           max_live_calls=1),
        _make_pilot_result(id_="alpha", allowed_connectors=["documentcloud"],
                           max_live_calls=1),
    ]
    out = select_pilot_for_live_smoke(pilot_output=_wrap(pilots))
    assert out["selected_pilot_id"] == "alpha"


def test_selection_is_deterministic_across_calls():
    out_a = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    out_b = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    assert out_a == out_b


# ---- Budget / safety ------------------------------------------------------


def test_selected_pilot_respects_max_live_calls_cap():
    """The selected pilot's max_live_calls must be a small positive
    integer (we never pick something the live-safety hard caps would
    refuse)."""
    out = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    assert isinstance(out["max_live_calls"], int)
    assert 1 <= out["max_live_calls"] <= 6


def test_selected_pilot_respects_max_results_cap():
    out = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    assert isinstance(out["max_results_per_connector"], int)
    assert 1 <= out["max_results_per_connector"] <= 5


def test_proven_artifact_connectors_constant_includes_documentcloud():
    """Sanity: the constant the selector uses to break ties on
    artifact yield must include documentcloud (the only connector
    we have evidence yields against structured-row queries)."""
    assert "documentcloud" in PROVEN_ARTIFACT_CONNECTORS


# ---- Network --------------------------------------------------------------


def test_selection_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    assert calls == [], f"selector triggered {len(calls)} live HTTP call(s)"


def test_output_is_json_serializable():
    out = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    encoded = json.dumps(out)
    decoded = json.loads(encoded)
    assert decoded == out
