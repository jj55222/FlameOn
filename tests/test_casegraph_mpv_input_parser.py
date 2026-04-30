"""W6-lite — Mapping Police Violence row parser.

Proves that an MPV-style structured row normalizes into the same
`CaseInput` shape as the WaPo and Fatal Encounters parsers, while
preserving the no-PRODUCE / no-VerifiedArtifact / candidate-only
guarantees. MPV's per-row "Body Camera" flag is surfaced as a
`source_notes` entry only — never as an ArtifactClaim or VerifiedArtifact.
"""
import json
from pathlib import Path

from pipeline2_discovery.casegraph import parse_mapping_police_violence_case_input


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "structured_inputs"


def load_fixture(name):
    with (FIXTURES / name).open("r", encoding="utf-8") as f:
        return json.load(f)


def parse_fixture(name):
    return parse_mapping_police_violence_case_input(load_fixture(name))


def test_complete_mpv_row_extracts_candidate_anchors_only():
    result = parse_fixture("mpv_complete.json")
    case_input = result.case_input
    fields = case_input.known_fields

    assert case_input.input_type == "dataset_row"
    assert result.dataset_name == "mapping_police_violence"
    assert fields["defendant_names"] == ["Jordan Example"]
    assert fields["agency"] == "Memphis Police Department"
    assert fields["jurisdiction"] == {"city": "Memphis", "county": "Shelby County", "state": "TN"}
    assert fields["incident_date"] == "2022-07-30"
    # MPV does not separate level-of-force from cause: incident_type stays empty.
    assert fields["incident_type"] is None
    assert fields["cause"] == "Gunshot"
    assert fields["demographics"] == {"age": 33, "race": "Black", "gender": "Male"}
    assert fields["source_url"].startswith("https://www.example-news.test/")

    # Body-camera flag becomes a source note, not a claim or artifact.
    assert "body_camera_flag:yes" in result.source_notes

    # Candidate anchors only.
    assert "candidate_fields_not_identity_lock" in result.risk_flags
    assert "artifact_verification_required" in result.risk_flags
    assert "outcome_verification_required" in result.risk_flags
    assert "identity_confidence" not in fields
    assert "verified_artifacts" not in fields
    assert "verdict" not in fields


def test_complete_mpv_row_generates_specific_candidate_queries():
    result = parse_fixture("mpv_complete.json")
    queries = "\n".join(result.case_input.candidate_queries)

    assert '"Jordan Example"' in queries
    assert "Memphis Police Department" in queries
    assert "Memphis" in queries
    assert "Tennessee" in queries
    assert "2022" in queries
    assert "critical incident video" in queries
    assert "bodycam records" in queries


def test_missing_fields_are_reported_without_inventing_name_or_city():
    result = parse_fixture("mpv_missing_fields.json")
    fields = result.case_input.known_fields

    assert fields["defendant_names"] == []
    assert fields["subject_name"] is None
    assert fields["agency"] == "Las Vegas Metropolitan Police Department"
    assert fields["jurisdiction"]["city"] is None
    assert fields["jurisdiction"]["county"] is None
    assert fields["jurisdiction"]["state"] == "NV"
    assert fields["incident_date"] == "2024-01-14"

    assert "subject_name" in result.case_input.missing_fields
    assert "jurisdiction.city" in result.case_input.missing_fields
    assert "jurisdiction.county" in result.case_input.missing_fields
    assert "missing_subject_name" in result.risk_flags
    assert "missing_source_url" in result.risk_flags
    assert "Jordan Example" not in "\n".join(result.case_input.candidate_queries)


def test_alt_keys_are_normalized_to_same_shape():
    result = parse_fixture("mpv_alt_keys.json")
    fields = result.case_input.known_fields

    assert fields["defendant_names"] == ["Lina Doe"]
    assert fields["agency"] == "Seattle Police Department"
    assert fields["jurisdiction"]["city"] == "Seattle"
    assert fields["jurisdiction"]["county"] == "King County"
    assert fields["jurisdiction"]["state"] == "WA"
    assert fields["incident_date"] == "2023-09-12"
    assert fields["cause"] == "Tasered"
    assert fields["demographics"] == {"age": 45, "race": "Asian", "gender": "Female"}
    assert fields["source_url"].endswith("lina-doe")
    assert "source_url_present" in result.source_notes
    # body_camera="No" → flag preserved verbatim in source notes.
    assert "body_camera_flag:no" in result.source_notes


def test_mpv_parser_does_not_overclaim_outcome_or_artifacts():
    result = parse_fixture("mpv_complete.json")

    assert "outcome_status" in result.case_input.missing_fields
    assert "verified_artifacts" in result.case_input.missing_fields
    assert not hasattr(result, "verified_artifacts")
    assert not hasattr(result, "verdict")
    raw_dump = json.dumps(result.case_input.raw_input)
    assert "PRODUCE" not in raw_dump


def test_mpv_dataset_name_is_used_when_dataset_key_absent():
    """If the row carries no `dataset` key, parser still tags it as mapping_police_violence."""
    row = {
        "Victim's name": "Anonymous Example",
        "State": "MI",
        "Date of Incident (month/day/year)": "03/15/2018",
    }
    result = parse_mapping_police_violence_case_input(row)
    assert result.dataset_name == "mapping_police_violence"
    assert result.case_input.known_fields["dataset_name"] == "mapping_police_violence"


def test_body_camera_flag_does_not_become_artifact_claim_or_verified():
    """The 'Body Camera' field is a per-row metadata flag, never a claim about an artifact URL."""
    result = parse_fixture("mpv_complete.json")

    # Surfaced as a source note only.
    assert "body_camera_flag:yes" in result.source_notes
    # No artifact-level fields anywhere on the parse result.
    assert not hasattr(result, "artifact_claims")
    assert not hasattr(result, "verified_artifacts")
    # The flag does not pollute candidate queries (it's just metadata,
    # not a search anchor).
    assert "body_camera_flag" not in "\n".join(result.case_input.candidate_queries)
