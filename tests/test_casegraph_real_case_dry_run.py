"""REAL2 — Real-case dry run + gate forecast tests.

Pure no-live. Asserts that the real-case pilot seed
(``tests/fixtures/pilot_cases/real_case_min_jian_guan.json``) flows
through the existing tooling deterministically before any live work
is authorized:

- a CasePacket dry bundle is generated under the ignored
  ``autoresearch/.runs/live7/`` directory
- the dry verdict is HOLD with the correct reason codes / risk
  flags (high_identity, sentenced_or_convicted, no_verified_media)
- the pilot selector picks the real-case pilot (score 132 over
  placeholders)
- the validation/pilot scoreboard remains clean (no warnings, all
  pilots ready)
- a gate forecast JSON is buildable and JSON-serializable, capturing
  identity / outcome / media gate state and the forecast for
  Endpoint v0 condition 3 sub-bullets
- zero network calls during the entire dry-run + forecast flow
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    build_pilot_validation_scoreboard,
    run_pilot_manifest,
    run_validation_manifest,
    score_case_packet,
    select_pilot_for_live_smoke,
)
from pipeline2_discovery.casegraph.cli import _load_fixture


ROOT = Path(__file__).resolve().parents[1]
SEED_PATH = ROOT / "tests" / "fixtures" / "pilot_cases" / "real_case_min_jian_guan.json"
PILOT_MANIFEST = ROOT / "tests" / "fixtures" / "pilot_cases" / "pilot_manifest.json"
LIVE7_DIR = ROOT / "autoresearch" / ".runs" / "live7"
DRY_BUNDLE_PATH = LIVE7_DIR / "real_case_dry_bundle.json"


def _build_gate_forecast() -> dict:
    """Pure forecast builder. Reads the seed, runs the pilot manifest
    + selector + scoreboard, and emits the gate forecast structure."""
    packet = _load_fixture(SEED_PATH)
    result = score_case_packet(packet)
    selection = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    pilot_out = run_pilot_manifest(PILOT_MANIFEST)
    val_out = run_validation_manifest()
    scoreboard = build_pilot_validation_scoreboard(
        validation_output=val_out, pilot_output=pilot_out
    )

    pilot_result = next(
        (
            r
            for r in pilot_out["results"]
            if r.get("id") == "real_case_min_jian_guan_pilot"
        ),
        None,
    )

    selected = selection.get("selected_pilot_id") == "real_case_min_jian_guan_pilot"
    likely_blockers = []
    if "no_verified_media" in result.risk_flags:
        likely_blockers.append(
            "media_gate: dry verdict HOLD due to no_verified_media; "
            "documentcloud likely yields documents only, not media; "
            "verdict will remain HOLD on best-case live yield"
        )
    if "media_artifact_present" in (pilot_result or {}).get("missing_gates", []):
        likely_blockers.append(
            "missing_gate:media_artifact_present - condition 3 'classified type' "
            "will be 'document' rather than 'media'"
        )

    return {
        "experiment_id": "REAL2-real-case-dry-run",
        "real_case_pilot_id": "real_case_min_jian_guan_pilot",
        "seed_fixture_path": "tests/fixtures/pilot_cases/real_case_min_jian_guan.json",
        "dry_bundle_path": str(DRY_BUNDLE_PATH.relative_to(ROOT)),
        "selected_for_live7": selected,
        "selection_score": selection.get("selection_score"),
        "expected_connectors": list(selection.get("allowed_connectors") or []),
        "max_live_calls": selection.get("max_live_calls"),
        "max_results_per_connector": selection.get("max_results_per_connector"),
        "candidate_count": selection.get("candidate_count"),
        "rationale": list(selection.get("rationale") or []),
        "dry_run_outcome": {
            "verdict": result.verdict,
            "reason_codes": list(result.reason_codes),
            "risk_flags": list(result.risk_flags),
            "research_completeness_score": result.research_completeness_score,
            "production_actionability_score": result.production_actionability_score,
            "actionability_score": result.actionability_score,
        },
        "gate_forecast": {
            "identity_state": (
                "high (pre-locked from seed anchors: full_name + jurisdiction "
                "+ agency + case_number)"
                if packet.case_identity.identity_confidence == "high"
                else f"{packet.case_identity.identity_confidence} (insufficient)"
            ),
            "outcome_state": (
                f"concluded ({packet.case_identity.outcome_status})"
                if packet.case_identity.outcome_status
                in {"sentenced", "closed", "convicted"}
                else f"unconcluded ({packet.case_identity.outcome_status})"
            ),
            "media_gate_state": (
                "OPEN: no media artifact yet; live data may add documents but "
                "is not expected to add media"
            ),
            "produce_eligible_with_documents_only": False,
            "produce_eligible_with_media": True,
            "expected_dry_verdict": "HOLD",
            "expected_live_verdict_with_documents": "HOLD",
            "expected_live_verdict_with_no_yield": "HOLD",
        },
        "endpoint_v0_forecast": {
            "cond1_validation_passes": (
                scoreboard["validation"]["accuracy_pct"] == 100.0
                and scoreboard["validation"]["guard_counters_all_zero"]
            ),
            "cond2_pilot_readiness": (
                scoreboard["pilots"]["ready_for_live"] >= 3
                and not scoreboard["warnings"]
            ),
            "cond3a_identity_locked_high": (
                packet.case_identity.identity_confidence == "high"
            ),
            "cond3b_concluded_outcome": (
                packet.case_identity.outcome_status
                in {"sentenced", "closed", "convicted"}
            ),
            "cond3c_verified_artifact_url": "depends_on_live_yield",
            "cond3d_artifact_type_classified": "depends_on_live_yield",
            "cond3e_run_bundle_written": DRY_BUNDLE_PATH.exists(),
            "cond3f_ledger_entry_will_be_generated": True,
            "cond3g_dry_vs_live_comparison_will_be_generated": True,
            "cond4_media_doc_distinction": True,
            "cond5_no_safety_gate_breaks_forecast": True,
        },
        "likely_blockers": likely_blockers,
        "scoreboard_warnings": list(scoreboard.get("warnings") or []),
    }


# ---- Dry bundle deliverable ---------------------------------------------


def test_real2_dry_bundle_exists_under_ignored_runs_dir():
    assert DRY_BUNDLE_PATH.exists(), (
        f"REAL2 dry bundle not found at {DRY_BUNDLE_PATH}. "
        "Generate it via: .venv/Scripts/python.exe -m "
        "pipeline2_discovery.casegraph.cli --fixture "
        "tests/fixtures/pilot_cases/real_case_min_jian_guan.json "
        "--bundle-out autoresearch/.runs/live7/real_case_dry_bundle.json "
        "--json --experiment-id REAL2-real-case-dry-run"
    )


def test_real2_dry_bundle_has_canonical_top_level_keys():
    bundle = json.loads(DRY_BUNDLE_PATH.read_text(encoding="utf-8"))
    for key in (
        "experiment_id",
        "mode",
        "wallclock_seconds",
        "input_summary",
        "identity",
        "outcome",
        "artifact_claims",
        "verified_artifacts",
        "result",
        "ledger_entry",
        "next_actions",
        "risk_flags",
    ):
        assert key in bundle, f"missing {key!r} in REAL2 dry bundle"
    assert bundle["experiment_id"] == "REAL2-real-case-dry-run"
    assert bundle["mode"] == "default"


def test_real2_dry_bundle_has_high_identity_and_concluded_outcome():
    bundle = json.loads(DRY_BUNDLE_PATH.read_text(encoding="utf-8"))
    assert bundle["identity"]["identity_confidence"] == "high"
    assert bundle["outcome"]["outcome_status"] == "sentenced"
    assert bundle["verified_artifacts"] == []


def test_real2_dry_bundle_verdict_is_hold():
    bundle = json.loads(DRY_BUNDLE_PATH.read_text(encoding="utf-8"))
    assert bundle["result"]["verdict"] == "HOLD"
    assert "no_verified_media" in bundle["result"]["risk_flags"]
    assert "high_identity" in bundle["result"]["reason_codes"]
    assert "sentenced_or_convicted" in bundle["result"]["reason_codes"]


# ---- Selector / scoreboard ----------------------------------------------


def test_real2_selector_picks_real_case_pilot_for_live7():
    out = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    assert out["selected_pilot_id"] == "real_case_min_jian_guan_pilot"
    assert out["allowed_connectors"] == ["documentcloud"]
    assert out["max_live_calls"] == 1
    assert out["max_results_per_connector"] == 5


def test_real2_pilot_scoreboard_remains_clean():
    val = run_validation_manifest()
    pilot = run_pilot_manifest(PILOT_MANIFEST)
    sb = build_pilot_validation_scoreboard(validation_output=val, pilot_output=pilot)
    assert sb["pilots"]["ready_for_live"] == sb["pilots"]["total"]
    assert sb["warnings"] == []


# ---- Gate forecast -------------------------------------------------------


def test_real2_gate_forecast_is_json_serializable():
    forecast = _build_gate_forecast()
    encoded = json.dumps(forecast)
    decoded = json.loads(encoded)
    assert decoded == forecast


def test_real2_gate_forecast_has_canonical_keys():
    forecast = _build_gate_forecast()
    for key in (
        "experiment_id",
        "real_case_pilot_id",
        "seed_fixture_path",
        "selected_for_live7",
        "expected_connectors",
        "max_live_calls",
        "max_results_per_connector",
        "rationale",
        "dry_run_outcome",
        "gate_forecast",
        "endpoint_v0_forecast",
        "likely_blockers",
    ):
        assert key in forecast, f"missing {key!r} in gate forecast"


def test_real2_gate_forecast_says_real_case_selected_for_live7():
    forecast = _build_gate_forecast()
    assert forecast["selected_for_live7"] is True
    assert forecast["real_case_pilot_id"] == "real_case_min_jian_guan_pilot"
    assert forecast["expected_connectors"] == ["documentcloud"]
    assert forecast["max_live_calls"] == 1
    assert forecast["max_results_per_connector"] == 5


def test_real2_gate_forecast_endpoint_v0_pre_live_state():
    forecast = _build_gate_forecast()
    v0 = forecast["endpoint_v0_forecast"]
    # Conditions 1, 2, 3a (identity), 3b (outcome) ARE met by the seed
    # alone, before any live call.
    assert v0["cond1_validation_passes"] is True
    assert v0["cond2_pilot_readiness"] is True
    assert v0["cond3a_identity_locked_high"] is True
    assert v0["cond3b_concluded_outcome"] is True
    # Conditions 3c / 3d depend on what the live yield actually
    # returns - the forecast names them "depends_on_live_yield"
    # rather than guessing.
    assert v0["cond3c_verified_artifact_url"] == "depends_on_live_yield"
    assert v0["cond3d_artifact_type_classified"] == "depends_on_live_yield"


def test_real2_gate_forecast_lists_likely_blockers():
    forecast = _build_gate_forecast()
    assert isinstance(forecast["likely_blockers"], list)
    assert any(
        "media_gate" in b or "media_artifact_present" in b
        for b in forecast["likely_blockers"]
    ), forecast["likely_blockers"]


# ---- Network -------------------------------------------------------------


def test_real2_dry_run_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    _load_fixture(SEED_PATH)
    select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    run_pilot_manifest(PILOT_MANIFEST)
    _build_gate_forecast()
    assert calls == [], f"REAL2 dry run made {len(calls)} live HTTP call(s)"
