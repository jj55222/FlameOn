"""W5-lite — Fatal Encounters row parser.

Proves that a Fatal Encounters–style structured row normalizes into the
same `CaseInput` shape as the WaPo UoF parser, while preserving the
no-PRODUCE / no-VerifiedArtifact / candidate-only guarantees.
"""
import json
from pathlib import Path

from pipeline2_discovery.casegraph import parse_fatal_encounters_case_input


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "structured_inputs"


def load_fixture(name):
    with (FIXTURES / name).open("r", encoding="utf-8") as f:
        return json.load(f)


def parse_fixture(name):
    return parse_fatal_encounters_case_input(load_fixture(name))


def test_complete_fe_row_extracts_candidate_anchors_only():
    result = parse_fixture("fatal_encounters_complete.json")
    case_input = result.case_input
    fields = case_input.known_fields

    assert case_input.input_type == "dataset_row"
    assert result.dataset_name == "fatal_encounters"
    assert fields["defendant_names"] == ["Maria Example"]
    assert fields["agency"] == "Tucson Police Department"
    assert fields["jurisdiction"] == {"city": "Tucson", "county": "Pima County", "state": "AZ"}
    assert fields["incident_date"] == "2021-06-14"
    assert fields["incident_type"] == "Lethal force"
    assert fields["cause"] == "Gunshot"
    assert fields["demographics"] == {"age": 28, "race": "Hispanic/Latino", "gender": "Female"}
    assert fields["source_url"].startswith("https://www.fatalencounters.org/")

    # Candidate anchors only — never an identity lock or verdict.
    assert "candidate_fields_not_identity_lock" in result.risk_flags
    assert "artifact_verification_required" in result.risk_flags
    assert "outcome_verification_required" in result.risk_flags
    assert "identity_confidence" not in fields
    assert "verified_artifacts" not in fields
    assert "verdict" not in fields


def test_complete_fe_row_generates_specific_candidate_queries():
    result = parse_fixture("fatal_encounters_complete.json")
    queries = "\n".join(result.case_input.candidate_queries)

    assert '"Maria Example"' in queries
    assert "Tucson Police Department" in queries
    assert "Tucson" in queries
    assert "Arizona" in queries
    assert "2021" in queries
    assert "critical incident video" in queries
    assert "bodycam records" in queries


def test_missing_fields_are_reported_without_inventing_name_or_city():
    result = parse_fixture("fatal_encounters_missing_fields.json")
    fields = result.case_input.known_fields

    assert fields["defendant_names"] == []
    assert fields["subject_name"] is None
    assert fields["agency"] == "Columbus Division of Police"
    assert fields["jurisdiction"]["city"] is None
    assert fields["jurisdiction"]["county"] is None
    assert fields["jurisdiction"]["state"] == "OH"
    assert fields["incident_date"] == "2023-11-22"

    assert "subject_name" in result.case_input.missing_fields
    assert "jurisdiction.city" in result.case_input.missing_fields
    assert "jurisdiction.county" in result.case_input.missing_fields
    assert "missing_subject_name" in result.risk_flags
    assert "missing_source_url" in result.risk_flags
    # Parser must not invent names absent from the row.
    assert "Maria Example" not in "\n".join(result.case_input.candidate_queries)


def test_alt_keys_are_normalized_to_same_shape():
    result = parse_fixture("fatal_encounters_alt_keys.json")
    fields = result.case_input.known_fields

    assert fields["defendant_names"] == ["Marcus Doe"]
    assert fields["agency"] == "Houston Police Department"
    assert fields["jurisdiction"]["city"] == "Houston"
    assert fields["jurisdiction"]["county"] == "Harris County"
    assert fields["jurisdiction"]["state"] == "TX"
    assert fields["incident_date"] == "2020-08-30"
    assert fields["incident_type"] == "Tasered"
    assert fields["cause"] == "Tasered"
    assert fields["source_url"].endswith("houston-marcus-doe")
    assert "source_url_present" in result.source_notes


def test_fe_parser_does_not_overclaim_outcome_or_artifacts():
    result = parse_fixture("fatal_encounters_complete.json")

    assert "outcome_status" in result.case_input.missing_fields
    assert "verified_artifacts" in result.case_input.missing_fields
    assert not hasattr(result, "verified_artifacts")
    assert not hasattr(result, "verdict")
    raw_dump = json.dumps(result.case_input.raw_input)
    # Source disposition fields are preserved verbatim, but no PRODUCE
    # verdict ever leaks from the parser.
    assert "PRODUCE" not in raw_dump


def test_fe_dataset_name_is_used_when_dataset_key_absent():
    """If the row carries no `dataset` key, parser still tags it as fatal_encounters."""
    row = {
        "Person": "Anonymous Example",
        "State": "CA",
        "Date of injury resulting in death (month/day/year)": "01/05/2019",
    }
    result = parse_fatal_encounters_case_input(row)
    assert result.dataset_name == "fatal_encounters"
    assert result.case_input.known_fields["dataset_name"] == "fatal_encounters"
