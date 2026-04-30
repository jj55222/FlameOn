import json
from pathlib import Path

from pipeline2_discovery.casegraph import parse_wapo_uof_case_input


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "structured_inputs"


def load_fixture(name):
    with (FIXTURES / name).open("r", encoding="utf-8") as f:
        return json.load(f)


def parse_fixture(name):
    return parse_wapo_uof_case_input(load_fixture(name))


def test_complete_wapo_row_extracts_candidate_anchors_only():
    result = parse_fixture("wapo_uof_complete.json")
    case_input = result.case_input
    fields = case_input.known_fields

    assert case_input.input_type == "dataset_row"
    assert fields["defendant_names"] == ["John Example"]
    assert fields["agency"] == "Phoenix Police Department"
    assert fields["jurisdiction"] == {"city": "Phoenix", "county": "Maricopa County", "state": "AZ"}
    assert fields["incident_date"] == "2022-05-12"
    assert fields["incident_type"] == "police shooting"
    assert fields["cause"] == "gun"
    assert fields["demographics"] == {"age": 34, "race": "W", "gender": "M"}
    assert "candidate_fields_not_identity_lock" in result.risk_flags
    assert "artifact_verification_required" in result.risk_flags
    assert "outcome_verification_required" in result.risk_flags
    assert "identity_confidence" not in fields
    assert "verified_artifacts" not in fields
    assert "verdict" not in fields


def test_complete_wapo_row_generates_specific_candidate_queries():
    result = parse_fixture("wapo_uof_complete.json")
    queries = "\n".join(result.case_input.candidate_queries)

    assert '"John Example"' in queries
    assert "Phoenix Police Department" in queries
    assert "Phoenix" in queries
    assert "Arizona" in queries
    assert "2022" in queries
    assert "critical incident video" in queries
    assert "bodycam records" in queries


def test_missing_fields_are_reported_without_inventing_name_or_city():
    result = parse_fixture("wapo_uof_missing_fields.json")
    fields = result.case_input.known_fields

    assert fields["defendant_names"] == []
    assert fields["subject_name"] is None
    assert fields["agency"] == "Example County Sheriff's Office"
    assert fields["jurisdiction"]["city"] is None
    assert fields["jurisdiction"]["state"] == "FL"
    assert fields["incident_date"] == "2024-04-21"
    assert "subject_name" in result.case_input.missing_fields
    assert "jurisdiction.city" in result.case_input.missing_fields
    assert "missing_subject_name" in result.risk_flags
    assert "missing_source_url" in result.risk_flags
    assert "Example Person" not in "\n".join(result.case_input.candidate_queries)


def test_alternate_wapo_keys_are_normalized():
    result = parse_fixture("wapo_uof_alt_keys.json")
    fields = result.case_input.known_fields

    assert fields["defendant_names"] == ["Maria Example"]
    assert fields["agency"] == "Example City Police Department"
    assert fields["jurisdiction"]["city"] == "Example City"
    assert fields["jurisdiction"]["state"] == "CA"
    assert fields["incident_date"] == "2021-05-03"
    assert fields["incident_type"] == "shot"
    assert "source_url_present" in result.source_notes


def test_parser_does_not_overclaim_outcome_or_artifacts():
    result = parse_fixture("wapo_uof_complete.json")
    fields = result.case_input.known_fields

    assert "outcome_status" in result.case_input.missing_fields
    assert "verified_artifacts" in result.case_input.missing_fields
    assert not hasattr(result, "verified_artifacts")
    assert not hasattr(result, "verdict")
    assert "PRODUCE" not in json.dumps(result.case_input.raw_input)
