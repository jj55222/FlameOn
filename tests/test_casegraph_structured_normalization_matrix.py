"""W7-lite — structured source normalization matrix.

Goal: prove that WaPo UoF, Fatal Encounters, and Mapping Police
Violence rows — three datasets with different column conventions —
normalize to the SAME `StructuredInputParseResult` / `CaseInput` shape.

This test is the contract that downstream CaseGraph code (query
planner, assembly, scoring) can depend on: regardless of which dataset
fed the input, the consumer only ever has to look at one schema.

The matrix also re-asserts the cross-cutting invariants:
- candidate anchors only (never an identity lock)
- no VerifiedArtifact created at parse time
- no PRODUCE verdict set
- outcome and verified_artifacts always reported as missing
- subject_name omitted (not invented) when the row lacks one
"""
import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    parse_fatal_encounters_case_input,
    parse_mapping_police_violence_case_input,
    parse_wapo_uof_case_input,
)


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "structured_inputs"


def load_fixture(name):
    with (FIXTURES / name).open("r", encoding="utf-8") as f:
        return json.load(f)


# (parser, complete_fixture, missing_fields_fixture, alt_keys_fixture, expected_dataset_name)
PARSER_MATRIX = [
    pytest.param(
        parse_wapo_uof_case_input,
        "wapo_uof_complete.json",
        "wapo_uof_missing_fields.json",
        "wapo_uof_alt_keys.json",
        "wapo_uof",
        id="wapo_uof",
    ),
    pytest.param(
        parse_fatal_encounters_case_input,
        "fatal_encounters_complete.json",
        "fatal_encounters_missing_fields.json",
        "fatal_encounters_alt_keys.json",
        "fatal_encounters",
        id="fatal_encounters",
    ),
    pytest.param(
        parse_mapping_police_violence_case_input,
        "mpv_complete.json",
        "mpv_missing_fields.json",
        "mpv_alt_keys.json",
        "mapping_police_violence",
        id="mapping_police_violence",
    ),
]


EXPECTED_KNOWN_FIELDS_KEYS = {
    "defendant_names",
    "subject_name",
    "agency",
    "jurisdiction",
    "incident_date",
    "incident_type",
    "cause",
    "source_url",
    "demographics",
    "dataset_name",
    "source_notes",
}

EXPECTED_JURISDICTION_KEYS = {"city", "county", "state"}

CORE_RISK_FLAGS = {
    "candidate_fields_not_identity_lock",
    "artifact_verification_required",
    "outcome_verification_required",
    "structured_dataset_candidate_only",
}


@pytest.mark.parametrize("parser, complete, missing, alt, expected_dataset", PARSER_MATRIX)
def test_input_type_is_dataset_row(parser, complete, missing, alt, expected_dataset):
    for fixture in (complete, missing, alt):
        result = parser(load_fixture(fixture))
        assert result.case_input.input_type == "dataset_row", (
            f"{expected_dataset} parser must use input_type='dataset_row' for {fixture}"
        )


@pytest.mark.parametrize("parser, complete, missing, alt, expected_dataset", PARSER_MATRIX)
def test_known_fields_keys_are_identical_across_datasets(parser, complete, missing, alt, expected_dataset):
    for fixture in (complete, missing, alt):
        result = parser(load_fixture(fixture))
        actual_keys = set(result.case_input.known_fields.keys())
        assert actual_keys == EXPECTED_KNOWN_FIELDS_KEYS, (
            f"{expected_dataset}/{fixture} known_fields key set differs: "
            f"missing={EXPECTED_KNOWN_FIELDS_KEYS - actual_keys}, "
            f"extra={actual_keys - EXPECTED_KNOWN_FIELDS_KEYS}"
        )


@pytest.mark.parametrize("parser, complete, missing, alt, expected_dataset", PARSER_MATRIX)
def test_jurisdiction_is_always_a_three_key_dict(parser, complete, missing, alt, expected_dataset):
    for fixture in (complete, missing, alt):
        result = parser(load_fixture(fixture))
        jurisdiction = result.case_input.known_fields["jurisdiction"]
        assert isinstance(jurisdiction, dict), f"{expected_dataset}/{fixture} jurisdiction must be a dict"
        assert set(jurisdiction.keys()) == EXPECTED_JURISDICTION_KEYS


@pytest.mark.parametrize("parser, complete, missing, alt, expected_dataset", PARSER_MATRIX)
def test_core_risk_flags_emitted_for_every_dataset(parser, complete, missing, alt, expected_dataset):
    for fixture in (complete, missing, alt):
        result = parser(load_fixture(fixture))
        risk_flags = set(result.risk_flags)
        for required_flag in CORE_RISK_FLAGS:
            assert required_flag in risk_flags, (
                f"{expected_dataset}/{fixture} missing required risk flag: {required_flag}"
            )


@pytest.mark.parametrize("parser, complete, missing, alt, expected_dataset", PARSER_MATRIX)
def test_dataset_name_is_consistent_in_known_fields(parser, complete, missing, alt, expected_dataset):
    for fixture in (complete, alt):
        result = parser(load_fixture(fixture))
        assert result.dataset_name == expected_dataset
        assert result.case_input.known_fields["dataset_name"] == expected_dataset


@pytest.mark.parametrize("parser, complete, missing, alt, expected_dataset", PARSER_MATRIX)
def test_outcome_and_verified_artifacts_are_always_missing(parser, complete, missing, alt, expected_dataset):
    """No structured-dataset parser may infer outcome or verify artifacts."""
    for fixture in (complete, missing, alt):
        result = parser(load_fixture(fixture))
        assert "outcome_status" in result.case_input.missing_fields
        assert "verified_artifacts" in result.case_input.missing_fields


@pytest.mark.parametrize("parser, complete, missing, alt, expected_dataset", PARSER_MATRIX)
def test_subject_name_is_never_invented(parser, complete, missing, alt, expected_dataset):
    """When the row lacks a subject name, defendant_names stays empty."""
    result = parser(load_fixture(missing))
    assert result.case_input.known_fields["defendant_names"] == []
    assert result.case_input.known_fields["subject_name"] is None
    assert "subject_name" in result.case_input.missing_fields
    assert "missing_subject_name" in result.risk_flags


@pytest.mark.parametrize("parser, complete, missing, alt, expected_dataset", PARSER_MATRIX)
def test_no_parser_returns_a_verdict_or_verified_artifacts(parser, complete, missing, alt, expected_dataset):
    """No structured parser may set a verdict or attach verified artifacts at parse time."""
    for fixture in (complete, missing, alt):
        result = parser(load_fixture(fixture))
        assert not hasattr(result, "verdict")
        assert not hasattr(result, "verified_artifacts")
        # And no PRODUCE leakage in serialized raw input either.
        assert "PRODUCE" not in json.dumps(result.case_input.raw_input)


def test_synthetic_same_incident_across_datasets_produces_consistent_anchors():
    """Same person, agency, location, date — represented in three different
    dataset row formats — should normalize to the same defendant_names,
    agency, jurisdiction, and incident_date.

    This is the strongest cross-dataset invariant: downstream gates can
    treat the normalized output identically regardless of source dataset.
    """
    wapo_row = {
        "subject_name": "John Doe",
        "agency": "Phoenix Police Department",
        "city": "Phoenix",
        "county": "Maricopa",
        "state": "Arizona",
        "incident_date": "2022-05-12",
        "incident_type": "police shooting",
        "cause": "gun",
    }
    fe_row = {
        "Person": "John Doe",
        "Agency or agencies involved": "Phoenix Police Department",
        "Location of death (city)": "Phoenix",
        "Location of death (county)": "Maricopa",
        "State": "AZ",
        "Date of injury resulting in death (month/day/year)": "05/12/2022",
        "Cause of death": "Gunshot",
    }
    mpv_row = {
        "Victim's name": "John Doe",
        "Agency responsible for death": "Phoenix Police Department",
        "City": "Phoenix",
        "County": "Maricopa",
        "State": "AZ",
        "Date of Incident (month/day/year)": "05/12/2022",
        "Cause of death": "Gunshot",
    }

    wapo = parse_wapo_uof_case_input(wapo_row).case_input.known_fields
    fe = parse_fatal_encounters_case_input(fe_row).case_input.known_fields
    mpv = parse_mapping_police_violence_case_input(mpv_row).case_input.known_fields

    for fields in (wapo, fe, mpv):
        assert fields["defendant_names"] == ["John Doe"]
        assert fields["agency"] == "Phoenix Police Department"
        assert fields["jurisdiction"] == {
            "city": "Phoenix",
            "county": "Maricopa County",
            "state": "AZ",
        }
        assert fields["incident_date"] == "2022-05-12"


def test_candidate_queries_are_emitted_by_every_parser_with_subject_name():
    """All parsers must produce at least one quoted-name candidate query
    when a subject name is present, so the downstream query planner has
    something to work with."""
    fixtures_with_name = [
        (parse_wapo_uof_case_input, "wapo_uof_complete.json"),
        (parse_fatal_encounters_case_input, "fatal_encounters_complete.json"),
        (parse_mapping_police_violence_case_input, "mpv_complete.json"),
    ]
    for parser, fixture in fixtures_with_name:
        result = parser(load_fixture(fixture))
        queries = result.case_input.candidate_queries
        assert queries, f"{fixture}: parser produced no candidate queries"
        # At least one query should quote the subject name.
        subject_name = result.case_input.known_fields["subject_name"]
        assert any(f'"{subject_name}"' in q for q in queries), (
            f"{fixture}: no candidate query quotes subject name {subject_name!r}"
        )
