"""PRIMARY2_REAL - real official-primary pilot scaffold tests.

The Endpoint v2 pilot must be a real calibration/test case, must use a
currently supported live connector, and must not PRODUCE in dry mode
without a verified media artifact.
"""
import json
from pathlib import Path

from pipeline2_discovery.casegraph import (
    mine_primary_media_candidates,
    run_pilot_manifest,
    score_case_packet,
)
from pipeline2_discovery.casegraph.cli import _load_fixture
from pipeline2_discovery.casegraph.pilots import select_primary_media_pilot_for_live_smoke


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "tests" / "fixtures" / "pilot_cases" / "pilot_manifest.json"
SEED_PATH = ROOT / "tests" / "fixtures" / "pilot_cases" / "primary_media_case_alan_champagne_youtube.json"
PILOT_ID = "primary_media_alan_champagne_youtube_pilot"


def _manifest():
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _pilot():
    pilots = _manifest()["pilots"]
    return next(pilot for pilot in pilots if pilot["id"] == PILOT_ID)


def test_real_primary_media_pilot_is_real_supported_and_tier_a_required():
    pilot = _pilot()

    assert pilot["is_real_case"] is True
    assert "Example" not in pilot["id"]
    assert pilot["seed_fixture_path"].endswith("primary_media_case_alan_champagne_youtube.json")
    assert pilot["allowed_connectors"] == ["youtube"]
    assert pilot["max_live_calls"] == 1
    assert pilot["max_results_per_connector"] == 5
    assert pilot["allow_downloads"] is False
    assert pilot["allow_scraping"] is False
    assert pilot["allow_llm"] is False

    expected = pilot["expected_minimum"]
    assert expected["media_required_for_produce"] is True
    assert expected["tier_a_media_required"] is True
    assert {"bodycam", "interrogation", "dispatch_911"} <= set(
        expected["artifact_types_desired"]
    )


def test_real_primary_media_seed_dry_run_holds_without_verified_media():
    packet = _load_fixture(SEED_PATH)
    result = score_case_packet(packet)

    assert packet.case_identity.defendant_names == ["Alan Matthew Champagne"]
    assert packet.case_identity.identity_confidence == "high"
    assert packet.case_identity.outcome_status == "convicted"
    assert packet.verified_artifacts == []
    assert result.verdict == "HOLD"
    assert "no_verified_media" in result.risk_flags
    assert "produce_based_on_weak_or_uncertain_media" not in result.risk_flags


def test_real_primary_media_pilot_is_derived_from_primary1_calibration_candidate():
    pilot = _pilot()
    report = mine_primary_media_candidates(
        paths=[ROOT / "autoresearch" / "calibration_data.json"],
        repo_root=ROOT,
    )
    candidates = {
        candidate.candidate_id: candidate
        for candidate in report.candidates
        if candidate.case_name == "Alan Matthew Champagne"
    }

    assert pilot["primary_media_candidate_id"] in candidates
    candidate = candidates[pilot["primary_media_candidate_id"]]
    assert candidate.case_name == "Alan Matthew Champagne"
    assert "youtube" in candidate.likely_connector_path
    assert {"bodycam", "interrogation", "dispatch_911"} <= set(
        candidate.media_signal_terms
    )


def test_real_primary_media_pilot_runner_marks_entry_ready_no_live():
    output = run_pilot_manifest(MANIFEST_PATH)
    result = next(item for item in output["results"] if item["id"] == PILOT_ID)

    assert result["readiness_status"] == "ready_for_live_smoke"
    assert result["is_real_case"] is True
    assert result["allowed_connectors"] == ["youtube"]
    assert result["actual_dry_verdict"] == "HOLD"
    assert result["verified_artifact_count"] == 0
    assert result["media_artifact_count"] == 0
    assert result["missing_gates"] == ["media_artifact_present"]
    assert result["expected_minimum"]["tier_a_media_required"] is True


def test_primary_media_selector_picks_real_supported_pilot():
    selection = select_primary_media_pilot_for_live_smoke(manifest_path=MANIFEST_PATH)

    assert selection["selected_pilot_id"] == PILOT_ID
    assert selection["allowed_connectors"] == ["youtube"]
    assert selection["max_live_calls"] == 1
    assert selection["max_results_per_connector"] == 5
    assert selection["expected_minimum"]["tier_a_media_required"] is True
    assert selection["no_ready_pilot"] is False


def test_primary_media_pilot_manifest_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)
    run_pilot_manifest(MANIFEST_PATH)
    select_primary_media_pilot_for_live_smoke(manifest_path=MANIFEST_PATH)
    assert calls == []
