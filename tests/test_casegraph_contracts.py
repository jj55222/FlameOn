import json
from pathlib import Path

from jsonschema import Draft7Validator


ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_PATH = ROOT / "schemas" / "contracts.json"
SAMPLE_PACKET_PATH = ROOT / "schemas" / "examples" / "case_packet_manual_minimal.json"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def validator_for(definition_name: str) -> Draft7Validator:
    schema = load_json(CONTRACTS_PATH)
    Draft7Validator.check_schema(schema)
    wrapped = {
        "$schema": schema["$schema"],
        **schema["definitions"][definition_name],
        "definitions": schema["definitions"],
    }
    Draft7Validator.check_schema(wrapped)
    return Draft7Validator(wrapped)


def assert_valid(definition_name: str, instance: dict) -> None:
    validator = validator_for(definition_name)
    errors = sorted(validator.iter_errors(instance), key=lambda e: list(e.path))
    assert errors == [], "\n".join(error.message for error in errors)


def test_contract_schema_is_valid_json_schema():
    schema = load_json(CONTRACTS_PATH)
    Draft7Validator.check_schema(schema)
    assert "case_packet" in schema["definitions"]
    assert "case_packet_legacy_evaluate_shape" in schema["definitions"]


def test_sample_case_packet_validates():
    packet = load_json(SAMPLE_PACKET_PATH)
    assert_valid("case_packet", packet)


def test_case_packet_requires_downstream_contract_fields():
    required = set(load_json(CONTRACTS_PATH)["definitions"]["case_packet"]["required"])
    assert required == {
        "case_id",
        "input",
        "case_identity",
        "sources",
        "artifact_claims",
        "verified_artifacts",
        "scores",
        "verdict",
        "next_actions",
        "risk_flags",
    }


def test_legacy_evaluate_compatible_shape_validates():
    legacy_output = {
        "evidence_found": {
            "bodycam": False,
            "interrogation": False,
            "court_video": False,
            "docket_docs": False,
            "dispatch_911": False,
        },
        "sources_found": [],
        "confidence": "low",
    }
    assert_valid("case_packet_legacy_evaluate_shape", legacy_output)
