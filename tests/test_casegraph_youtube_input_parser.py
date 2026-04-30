import json
from pathlib import Path

from pipeline2_discovery.casegraph import parse_youtube_case_input


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "youtube_inputs"


def load_fixture(name):
    with (FIXTURES / name).open("r", encoding="utf-8") as f:
        return json.load(f)


def parse_fixture(name):
    return parse_youtube_case_input(load_fixture(name))


def test_florida_disturbance_extracts_partial_anchors_without_identity_lock():
    result = parse_fixture("florida_disturbance.json")
    case_input = result.case_input
    fields = case_input.known_fields

    assert case_input.input_type == "youtube_lead"
    assert fields["incident_date"] == "2024-04-21"
    assert fields["jurisdiction"]["state"] == "FL"
    assert fields["jurisdiction"]["city"] is None
    assert fields["defendant_names"] == []
    assert fields["agency"] is None
    assert {"physical disturbance", "disabled vehicle", "vehicle hit curb"} <= set(result.incident_descriptors)
    assert "defendant_names" in case_input.missing_fields
    assert "agency" in case_input.missing_fields
    assert "jurisdiction.city" in case_input.missing_fields
    assert "identity_confidence" not in fields
    assert "verdict" not in fields
    assert not hasattr(result, "verified_artifacts")


def test_transcript_with_suspect_agency_and_date_extracts_candidates_only():
    result = parse_fixture("transcript_suspect_agency_date.json")
    fields = result.case_input.known_fields

    assert fields["defendant_names"] == ["John Example"]
    assert fields["agency"] == "Phoenix Police Department"
    assert fields["jurisdiction"]["city"] == "Phoenix"
    assert fields["incident_date"] == "2022-05-12"
    assert "bodycam" in result.artifact_signals
    assert fields["incident_date_candidates"] == ["2022-05-12"]
    assert any(
        '"John Example"' in query
        and '"Phoenix Police Department"' in query
        and "bodycam" in query
        for query in result.case_input.candidate_queries
    )
    assert "identity_confidence" not in fields


def test_artifact_language_becomes_signals_not_verified_artifacts():
    result = parse_fixture("transcript_artifact_language.json")

    assert {"bodycam", "dispatch_911", "interrogation"} <= set(result.artifact_signals)
    assert result.case_input.known_fields["artifact_signals"] == result.artifact_signals
    assert not hasattr(result, "verified_artifacts")
    assert "verdict" not in result.case_input.known_fields


def test_noisy_clickbait_keeps_artifact_terms_weak():
    result = parse_fixture("noisy_clickbait.json")
    fields = result.case_input.known_fields

    assert fields["defendant_names"] == []
    assert fields["agency"] is None
    assert not any(fields["jurisdiction"].values())
    assert fields["incident_date"] is None
    assert {"bodycam", "interrogation"} <= set(result.artifact_signals)
    assert "no_case_anchors" in result.risk_flags
    assert "artifact_signal_without_case_anchors" in result.risk_flags
    assert "missing_defendant_name" in result.risk_flags


def test_partial_fields_generate_search_query_with_available_anchors():
    result = parse_fixture("partial_fields_query_generation.json")

    assert any(
        "April 21, 2024" in query
        and "Florida" in query
        and "disabled vehicle" in query
        and "physical disturbance" in query
        for query in result.case_input.candidate_queries
    )
    assert result.case_input.known_fields["defendant_names"] == []
    assert "missing_defendant_name" in result.risk_flags


def test_conflicting_dates_are_preserved_and_flagged():
    result = parse_fixture("conflicting_dates.json")
    fields = result.case_input.known_fields

    assert fields["incident_date"] is None
    assert fields["incident_date_candidates"] == ["2024-04-21", "2024-04-22"]
    assert fields["incident_date_raw_candidates"] == ["April 21, 2024", "April 22, 2024"]
    assert "conflicting_incident_dates" in result.risk_flags
    assert "ambiguous_incident_date" in result.risk_flags
