"""OUTCOME4 - replay diagnostics for mocked outcome corroboration."""
import json
from pathlib import Path

from pipeline2_discovery.casegraph.calibration_replay import replay_to_jsonable, run_calibration_replay


ROOT = Path(__file__).resolve().parents[1]


def _payloads():
    return {
        4: {
            "source_type": "court_docket",
            "source_authority": "court",
            "text": "Court records in Knoxville, Tennessee show Christa Gail Pike was sentenced to death.",
        },
        24: {
            "source_type": "news",
            "source_authority": "news",
            "text": "Louis Broadway-Phillips of Mesa Arizona was convicted after trial.",
        },
        0: {
            "source_type": "news",
            "source_authority": "news",
            "text": "The Keonte Gathron case drew attention, but this article does not state a final outcome.",
        },
    }


def test_replay_integrates_mocked_outcome_corroboration_diagnostics():
    result = run_calibration_replay(repo_root=ROOT, outcome_corrob_payloads=_payloads())

    christa = next(case for case in result.case_results if case.case_id == 4)
    louis = next(case for case in result.case_results if case.case_id == 24)
    keonte = next(case for case in result.case_results if case.case_id == 0)

    assert christa.outcome_corrob_status == "corroborated_sentenced"
    assert christa.outcome_corrob_confidence >= 0.9
    assert christa.outcome_corrob_source_type == "court_docket"
    assert "sentenced" in christa.outcome_corrob_supporting_snippet.lower()

    assert louis.outcome_corrob_status == "corroborated_convicted"
    assert louis.outcome_corrob_confidence >= 0.7

    assert keonte.outcome_corrob_status == "ambiguous"
    assert keonte.outcome_corrob_confidence == 0.0


def test_replay_keeps_final_failure_gates_unchanged_and_adds_proposed_metric():
    result = run_calibration_replay(repo_root=ROOT, outcome_corrob_payloads=_payloads())

    assert result.failure_reason_counts["missing_outcome_seed"] == 27
    assert result.metrics["failure_reason_counts"]["missing_outcome_seed"] == 27
    assert result.metrics["outcome_corrob_attempted_count"] == 3
    assert result.metrics["outcome_corrob_corroborated_count"] == 2
    assert result.metrics["proposed_missing_outcome_seed_count"] == 25


def test_replay_marks_unknown_cases_without_payload_as_no_mock_payload():
    result = run_calibration_replay(repo_root=ROOT, outcome_corrob_payloads=_payloads())

    unknown_without_payload = next(
        case for case in result.case_results
        if case.case_id not in {0, 4, 24} and "missing_outcome_seed" in case.failure_reasons
    )

    assert unknown_without_payload.outcome_corrob_status == "no_mock_payload"
    assert unknown_without_payload.outcome_corrob_confidence == 0.0
    assert "Add mocked outcome corroboration payload" in " ".join(unknown_without_payload.next_actions)


def test_replay_does_not_attempt_outcome_corroboration_for_seeded_outcomes():
    result = run_calibration_replay(repo_root=ROOT, outcome_corrob_payloads=_payloads())

    alan = next(case for case in result.case_results if case.title == "Alan Matthew Champagne")

    assert alan.outcome_ready is True
    assert alan.outcome_corrob_status == "not_attempted"
    assert alan.outcome_corrob_source_type is None


def test_replay_with_outcome_diagnostics_is_json_serializable():
    result = run_calibration_replay(repo_root=ROOT, outcome_corrob_payloads=_payloads())

    encoded = json.dumps(replay_to_jsonable(result), sort_keys=True)
    decoded = json.loads(encoded)

    assert decoded["metrics"]["outcome_corrob_corroborated_count"] == 2
    assert any(
        case["outcome_corrob_status"] == "corroborated_sentenced"
        for case in decoded["case_results"]
    )


def test_replay_outcome_diagnostics_make_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    run_calibration_replay(repo_root=ROOT, outcome_corrob_payloads=_payloads())
    assert calls == []
