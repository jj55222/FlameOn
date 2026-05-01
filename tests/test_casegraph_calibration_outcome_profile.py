"""OUTCOME1 - deterministic calibration outcome seed profiler tests."""
import json
from pathlib import Path

from pipeline2_discovery.casegraph.calibration_profile import profile_calibration_set
from pipeline2_discovery.casegraph.calibration_replay import run_calibration_replay


ROOT = Path(__file__).resolve().parents[1]


def test_outcome_profiler_extracts_pilot_fixture_outcomes():
    report = profile_calibration_set(repo_root=ROOT)

    min_jian = next(row for row in report.profile_rows if row.title == "Min Jian Guan")
    alan = next(row for row in report.profile_rows if row.title == "Alan Matthew Champagne")

    assert min_jian.outcome_seed_status == "sentenced"
    assert min_jian.outcome_status == "sentenced"
    assert min_jian.outcome_confidence == 0.95
    assert "real_case_min_jian_guan.json" in min_jian.outcome_source_field

    assert alan.outcome_seed_status == "convicted"
    assert alan.outcome_status == "convicted"
    assert alan.outcome_confidence == 0.95
    assert "primary_media_case_alan_champagne_youtube.json" in alan.outcome_source_field


def test_outcome_profiler_extracts_deterministic_url_slug_outcomes():
    report = profile_calibration_set(repo_root=ROOT)

    jorge = next(row for row in report.profile_rows if row.title == "Jorge Barahona")
    william = next(row for row in report.profile_rows if row.title == "William James McElroy Jr.")

    assert jorge.outcome_seed_status == "convicted"
    assert jorge.outcome_source_field == "ground_truth.verified_sources[2]"
    assert jorge.recommended_corrob_source == "news article"
    assert william.outcome_seed_status == "sentenced"
    assert william.outcome_confidence >= 0.78


def test_outcome_profiler_preserves_unknown_when_no_deterministic_signal_exists():
    report = profile_calibration_set(repo_root=ROOT)

    keonte = next(row for row in report.profile_rows if row.title == "Keonte Gathron")

    assert keonte.outcome_seed_status == "unknown"
    assert keonte.outcome_status == "unknown"
    assert keonte.outcome_confidence == 0.0
    assert keonte.outcome_source_field is None
    assert keonte.missing_outcome_reason == "no_verified_source_url_text"
    assert keonte.recommended_corrob_source == "manual seed needed"


def test_outcome_profile_summary_reduces_missing_outcome_seeds():
    report = profile_calibration_set(repo_root=ROOT)
    replay = run_calibration_replay(repo_root=ROOT)

    assert report.summary["outcome_seed_ready_cases"] == 11
    assert report.summary["outcome_seed_status_counts"] == {
        "convicted": 4,
        "sentenced": 7,
        "unknown": 27,
    }
    assert replay.metrics["outcome_ready_count"] == 11
    assert replay.failure_reason_counts["missing_outcome_seed"] == 27


def test_outcome_profile_is_json_serializable():
    report = profile_calibration_set(repo_root=ROOT)

    encoded = json.dumps(report.to_dict(), sort_keys=True)
    decoded = json.loads(encoded)

    first_seeded = next(
        row for row in decoded["profile_rows"]
        if row["outcome_seed_status"] != "unknown"
    )
    assert first_seeded["outcome_source_field"]
    assert "outcome_seed_status_counts" in decoded["summary"]


def test_outcome_profiler_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    profile_calibration_set(repo_root=ROOT)
    assert calls == []

