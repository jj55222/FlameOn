"""PILOT2 — pilot manifest no-live runner tests.

Asserts that ``run_pilot_manifest`` and ``assess_pilot``:

- load the committed pilot manifest and emit per-pilot readiness
  results plus an aggregate summary
- expose the canonical per-pilot shape
- correctly dispatch between CasePacket-shape fixtures and structured-
  row-shape fixtures for the seed (so structured pilots and
  manual/youtube pilots both score)
- mark every CURRENT pilot as ``ready_for_live_smoke`` (committed
  manifest is sound by construction; this guards against future
  drift)
- detect the four blocking states explicitly:
  * blocked_missing_fixture   - seed_fixture_path doesn't exist
  * blocked_policy            - paid connector / downloads /
                                scraping / LLM / missing-media-gate
                                / unknown connector
  * blocked_invalid_budget    - over-cap max_live_calls /
                                max_results_per_connector / too many
                                connectors / non-int values
  * blocked_verdict_drift     - actual dry verdict != pilot's
                                expected_verdict_without_live
- aggregate summary correctly counts ready vs blocked, paid /
  download / scrape / LLM presence, and total planned live calls
- output is JSON-serializable
- never makes a network call
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    assess_pilot,
    run_pilot_manifest,
)
from pipeline2_discovery.casegraph.pilots import (
    READINESS_BUDGET,
    READINESS_MISSING_FIXTURE,
    READINESS_POLICY,
    READINESS_READY,
    READINESS_VERDICT_DRIFT,
)


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "tests" / "fixtures" / "pilot_cases" / "pilot_manifest.json"


REQUIRED_RESULT_KEYS = (
    "id",
    "input_type",
    "seed_fixture_path",
    "expected_minimum",
    "expected_verdict_without_live",
    "actual_dry_verdict",
    "verdict_match",
    "research_completeness_score",
    "production_actionability_score",
    "actionability_score",
    "verified_artifact_count",
    "media_artifact_count",
    "document_artifact_count",
    "identity_confidence",
    "outcome_status",
    "satisfied_gates",
    "missing_gates",
    "allowed_connectors",
    "max_live_calls",
    "max_results_per_connector",
    "policy_violations",
    "budget_violations",
    "readiness_status",
    "next_actions",
)


def _base_pilot(**overrides):
    """Build a minimal valid pilot dict; tests override individual
    fields to exercise blocked paths."""
    base = {
        "id": "synthetic_pilot",
        "input_type": "manual",
        "seed_fixture_path": "tests/fixtures/casegraph_scenarios/media_rich_produce.json",
        "expected_minimum": {
            "identity_lock_required": True,
            "outcome_required": True,
            "media_required_for_produce": True,
            "artifact_types_desired": ["bodycam"],
        },
        "allowed_connectors": ["courtlistener"],
        "max_live_calls": 1,
        "max_results_per_connector": 5,
        "allow_resolvers": True,
        "allow_downloads": False,
        "allow_scraping": False,
        "allow_llm": False,
        "expected_verdict_without_live": "PRODUCE",
        "notes": "synthetic",
    }
    base.update(overrides)
    return base


def _wrap(pilots):
    return {
        "manifest_version": 1,
        "global_constraints": {
            "allow_paid_connectors": False,
            "allow_downloads": False,
            "allow_scraping": False,
            "allow_llm": False,
            "max_live_calls_total_default": 4,
            "max_results_per_connector_default": 5,
            "allowed_free_connectors": [
                "courtlistener",
                "documentcloud",
                "muckrock",
                "youtube",
            ],
            "paid_connectors_blocked": ["brave", "firecrawl"],
        },
        "pilots": pilots,
    }


# ---- Real manifest --------------------------------------------------------


def test_runner_loads_committed_manifest():
    out = run_pilot_manifest(MANIFEST_PATH)
    assert out["manifest_version"] == 1
    assert out["total_pilots"] >= 3
    assert out["manifest_path"] == str(MANIFEST_PATH)


def test_every_per_pilot_result_has_canonical_shape():
    out = run_pilot_manifest(MANIFEST_PATH)
    for result in out["results"]:
        for key in REQUIRED_RESULT_KEYS:
            assert key in result, (
                f"pilot {result.get('id')!r} missing key {key!r}"
            )


def test_every_current_pilot_is_ready_for_live_smoke():
    out = run_pilot_manifest(MANIFEST_PATH)
    blocked = [
        r for r in out["results"] if r["readiness_status"] != READINESS_READY
    ]
    assert not blocked, (
        "current pilots that are not ready_for_live_smoke:\n"
        + "\n".join(
            f"{r['id']}: status={r['readiness_status']} policy={r['policy_violations']} "
            f"budget={r['budget_violations']} verdict={r['actual_dry_verdict']}"
            for r in blocked
        )
    )
    summary = out["summary"]
    assert summary["ready_count"] == out["total_pilots"]
    assert summary["blocked_count"] == 0
    assert summary["any_paid_connectors"] is False
    assert summary["any_downloads_enabled"] is False
    assert summary["any_scraping_enabled"] is False
    assert summary["any_llm_enabled"] is False
    assert summary["any_missing_media_required"] is False


def test_runner_dispatches_both_casepacket_and_structured_seed_shapes():
    """Sanity: the committed pilot manifest mixes structured-row
    fixtures (e.g. wapo_uof_complete.json) and CasePacket fixtures
    (e.g. media_rich_produce.json) as seeds. The runner must score
    both shapes without raising."""
    out = run_pilot_manifest(MANIFEST_PATH)
    seen_paths = {r["seed_fixture_path"] for r in out["results"]}
    assert any("structured_inputs/" in p for p in seen_paths), (
        "expected at least one structured-row pilot in the committed manifest"
    )
    assert any("casegraph_scenarios/" in p for p in seen_paths), (
        "expected at least one CasePacket-shape pilot in the committed manifest"
    )
    for r in out["results"]:
        assert r["actual_dry_verdict"] in {"PRODUCE", "HOLD", "SKIP"}


# ---- Blocked: missing fixture ---------------------------------------------


def test_pilot_with_missing_fixture_is_blocked_missing_fixture():
    pilot = _base_pilot(seed_fixture_path="tests/fixtures/does_not_exist.json")
    out = run_pilot_manifest(manifest_dict=_wrap([pilot]))
    result = out["results"][0]
    assert result["readiness_status"] == READINESS_MISSING_FIXTURE
    assert any(
        a.startswith("create_or_correct_seed_fixture:") for a in result["next_actions"]
    )


# ---- Blocked: policy ------------------------------------------------------


def test_pilot_with_paid_connector_is_blocked_policy():
    pilot = _base_pilot(allowed_connectors=["courtlistener", "brave"])
    out = run_pilot_manifest(manifest_dict=_wrap([pilot]))
    result = out["results"][0]
    assert result["readiness_status"] == READINESS_POLICY
    assert any("paid_connectors_listed:" in v for v in result["policy_violations"])


def test_pilot_with_allow_downloads_true_is_blocked_policy():
    pilot = _base_pilot(allow_downloads=True)
    out = run_pilot_manifest(manifest_dict=_wrap([pilot]))
    result = out["results"][0]
    assert result["readiness_status"] == READINESS_POLICY
    assert "allow_downloads_true" in result["policy_violations"]


def test_pilot_with_allow_scraping_true_is_blocked_policy():
    pilot = _base_pilot(allow_scraping=True)
    out = run_pilot_manifest(manifest_dict=_wrap([pilot]))
    result = out["results"][0]
    assert result["readiness_status"] == READINESS_POLICY
    assert "allow_scraping_true" in result["policy_violations"]


def test_pilot_with_allow_llm_true_is_blocked_policy():
    pilot = _base_pilot(allow_llm=True)
    out = run_pilot_manifest(manifest_dict=_wrap([pilot]))
    result = out["results"][0]
    assert result["readiness_status"] == READINESS_POLICY
    assert "allow_llm_true" in result["policy_violations"]


def test_pilot_with_media_required_for_produce_false_is_blocked_policy():
    pilot = _base_pilot()
    pilot["expected_minimum"]["media_required_for_produce"] = False
    out = run_pilot_manifest(manifest_dict=_wrap([pilot]))
    result = out["results"][0]
    assert result["readiness_status"] == READINESS_POLICY
    assert "media_required_for_produce_false" in result["policy_violations"]


def test_pilot_with_unknown_connector_is_blocked_policy():
    pilot = _base_pilot(allowed_connectors=["courtlistener", "some_future_thing"])
    out = run_pilot_manifest(manifest_dict=_wrap([pilot]))
    result = out["results"][0]
    assert result["readiness_status"] == READINESS_POLICY
    assert any(
        "unknown_connectors_listed:" in v for v in result["policy_violations"]
    )


# ---- Blocked: budget ------------------------------------------------------


def test_pilot_with_max_live_calls_over_envelope_is_blocked_budget():
    pilot = _base_pilot(max_live_calls=99)
    out = run_pilot_manifest(manifest_dict=_wrap([pilot]))
    result = out["results"][0]
    assert result["readiness_status"] == READINESS_BUDGET
    assert any(
        "max_live_calls_over_envelope" in v for v in result["budget_violations"]
    )


def test_pilot_with_max_results_over_cap_is_blocked_budget():
    pilot = _base_pilot(max_results_per_connector=99)
    out = run_pilot_manifest(manifest_dict=_wrap([pilot]))
    result = out["results"][0]
    assert result["readiness_status"] == READINESS_BUDGET
    assert any(
        "max_results_per_connector_over_cap" in v for v in result["budget_violations"]
    )


def test_pilot_with_too_many_connectors_is_blocked_budget():
    pilot = _base_pilot(
        allowed_connectors=["courtlistener", "muckrock", "documentcloud"]
    )
    out = run_pilot_manifest(manifest_dict=_wrap([pilot]))
    result = out["results"][0]
    assert result["readiness_status"] == READINESS_BUDGET
    assert any(
        "allowed_connectors_over_cap" in v for v in result["budget_violations"]
    )


def test_pilot_with_non_int_max_live_calls_is_blocked_budget():
    pilot = _base_pilot(max_live_calls="oops")
    out = run_pilot_manifest(manifest_dict=_wrap([pilot]))
    result = out["results"][0]
    assert result["readiness_status"] == READINESS_BUDGET
    assert "max_live_calls_invalid_type" in result["budget_violations"]


# ---- Blocked: verdict drift -----------------------------------------------


def test_pilot_with_verdict_drift_is_blocked_verdict_drift():
    """media_rich_produce.json scores PRODUCE; if a pilot claims it
    should score HOLD without live, the runner must flag the drift."""
    pilot = _base_pilot(
        expected_verdict_without_live="HOLD",
    )
    out = run_pilot_manifest(manifest_dict=_wrap([pilot]))
    result = out["results"][0]
    assert result["readiness_status"] == READINESS_VERDICT_DRIFT
    assert result["actual_dry_verdict"] == "PRODUCE"
    assert result["verdict_match"] is False


# ---- Aggregate summary ----------------------------------------------------


def test_aggregate_counts_ready_and_blocked_correctly():
    pilots = [
        _base_pilot(id="ok"),
        _base_pilot(id="blocked_missing", seed_fixture_path="tests/fixtures/missing.json"),
        _base_pilot(id="blocked_policy", allow_downloads=True),
    ]
    out = run_pilot_manifest(manifest_dict=_wrap(pilots))
    summary = out["summary"]
    assert summary["total_pilots"] == 3
    assert summary["ready_count"] == 1
    assert summary["blocked_count"] == 2
    assert summary["by_readiness_status"][READINESS_READY] == 1
    assert summary["by_readiness_status"][READINESS_MISSING_FIXTURE] == 1
    assert summary["by_readiness_status"][READINESS_POLICY] == 1
    assert summary["any_downloads_enabled"] is True


def test_aggregate_total_planned_live_calls_sums_correctly():
    pilots = [
        _base_pilot(id="a", max_live_calls=1),
        _base_pilot(id="b", max_live_calls=2),
    ]
    out = run_pilot_manifest(manifest_dict=_wrap(pilots))
    assert out["summary"]["total_planned_live_calls"] == 3


# ---- JSON + network -------------------------------------------------------


def test_runner_output_is_json_serializable():
    out = run_pilot_manifest(MANIFEST_PATH)
    encoded = json.dumps(out)
    decoded = json.loads(encoded)
    assert decoded == out


def test_runner_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    run_pilot_manifest(MANIFEST_PATH)
    assert calls == [], (
        f"pilot runner triggered {len(calls)} live HTTP call(s); must be no-live"
    )


def test_assess_pilot_directly_returns_canonical_shape():
    pilot = _base_pilot()
    result = assess_pilot(pilot)
    for key in REQUIRED_RESULT_KEYS:
        assert key in result
