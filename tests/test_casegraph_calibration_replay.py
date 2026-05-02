"""CAL2 - no-live calibration replay runner tests."""
import json
from pathlib import Path

from pipeline2_discovery.casegraph.calibration_replay import (
    FAILURE_REASONS,
    replay_to_jsonable,
    run_calibration_replay,
)


ROOT = Path(__file__).resolve().parents[1]


def test_replay_runs_over_all_calibration_profiles():
    result = run_calibration_replay(repo_root=ROOT)

    assert result.total_cases == 38
    assert len(result.case_results) == 38
    assert result.metrics["total_cases"] == 38
    assert result.metrics["profileable_cases"] == 38


def test_replay_scoreboard_counts_current_capabilities():
    result = run_calibration_replay(repo_root=ROOT)
    metrics = result.metrics

    assert metrics["identity_ready_count"] == 38
    assert metrics["outcome_ready_count"] == 11
    assert metrics["document_artifact_expected_count"] == 27
    assert metrics["media_artifact_expected_count"] == 27
    assert metrics["tier_a_media_expected_count"] == 26
    assert metrics["supported_live_path_count"] == 34
    assert metrics["youtube_supported_count"] == 27
    assert metrics["muckrock_supported_count"] == 5
    assert metrics["courtlistener_supported_count"] == 17
    assert metrics["ready_for_portal_fetch_count"] == 30
    assert metrics["needs_seed_url_discovery_count"] == 4
    assert metrics["outcome_plan_ready_count"] == 27
    assert metrics["outcome_corrob_attempted_count"] == 0
    assert metrics["outcome_corrob_corroborated_count"] == 0
    assert metrics["proposed_missing_outcome_seed_count"] == 27


def test_replay_failure_taxonomy_is_complete_and_deterministic():
    result = run_calibration_replay(repo_root=ROOT)
    counts = result.failure_reason_counts

    assert set(counts) == set(FAILURE_REASONS)
    assert counts["missing_identity_seed"] == 0
    assert counts["missing_outcome_seed"] == 27
    assert counts["tier_a_media_possible"] == 26
    assert counts["needs_firecrawl_known_url"] == 34
    assert counts["document_only_expected"] == 5
    assert counts["generic_media_only"] == 4
    assert counts["no_known_artifact_signal"] == 6


def test_replay_identifies_top_next_work_items():
    result = run_calibration_replay(repo_root=ROOT)
    top = result.top_next_work_items

    assert top[0]["reason"] == "needs_firecrawl_known_url"
    assert top[0]["count"] == 34
    assert top[1]["reason"] == "missing_outcome_seed"
    assert top[1]["count"] == 27
    assert top[2]["reason"] == "tier_a_media_possible"


def test_replay_case_results_include_next_actions():
    result = run_calibration_replay(repo_root=ROOT)

    alan = next(case for case in result.case_results if case.title == "Alan Matthew Champagne")
    assert alan.identity_ready is True
    assert alan.outcome_ready is True
    assert "tier_a_media_possible" in alan.failure_reasons
    assert "missing_outcome_seed" not in alan.failure_reasons
    assert "verify_primary_media_artifact_url" in alan.next_actions

    negative = next(case for case in result.case_results if "no_known_artifact_signal" in case.failure_reasons)
    assert "park_until_artifact_signal_exists" in negative.next_actions


def test_replay_result_is_json_serializable():
    result = run_calibration_replay(repo_root=ROOT)

    encoded = json.dumps(replay_to_jsonable(result), sort_keys=True)
    decoded = json.loads(encoded)

    assert decoded["total_cases"] == 38
    assert decoded["metrics"]["failure_reason_counts"]["missing_outcome_seed"] == 27
    assert "outcome_corrob_status" in decoded["case_results"][0]


def test_replay_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    run_calibration_replay(repo_root=ROOT)
    assert calls == []
