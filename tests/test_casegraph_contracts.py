import json
from pathlib import Path

from jsonschema import Draft7Validator

from pipeline2_discovery.casegraph import (
    export_legacy_evaluate_result,
    export_p2_to_p3,
    export_p2_to_p4,
    export_p2_to_p5,
    route_manual_defendant_jurisdiction,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "schemas"
EXAMPLE_DIR = SCHEMA_DIR / "examples"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def validator_for(schema_path: Path) -> Draft7Validator:
    schema = load_json(schema_path)
    Draft7Validator.check_schema(schema)
    return Draft7Validator(schema)


def assert_valid(schema_path: Path, instance: dict) -> None:
    validator = validator_for(schema_path)
    errors = sorted(validator.iter_errors(instance), key=lambda e: list(e.path))
    assert errors == [], "\n".join(error.message for error in errors)


def assert_valid_contract_definition(definition_name: str, instance: dict) -> None:
    schema = load_json(SCHEMA_DIR / "contracts.json")
    Draft7Validator.check_schema(schema)
    wrapped = {
        "$schema": schema["$schema"],
        **schema["definitions"][definition_name],
        "definitions": schema["definitions"],
    }
    validator = Draft7Validator(wrapped)
    errors = sorted(validator.iter_errors(instance), key=lambda e: list(e.path))
    assert errors == [], "\n".join(error.message for error in errors)


def test_contract_files_are_valid_json_schemas():
    for schema_name in [
        "contracts.json",
        "p2_case_packet.schema.json",
        "p2_to_p3.schema.json",
        "p2_to_p4.schema.json",
        "p2_to_p5.schema.json",
    ]:
        Draft7Validator.check_schema(load_json(SCHEMA_DIR / schema_name))


def test_sample_case_packet_validates_against_split_schema():
    packet = load_json(EXAMPLE_DIR / "case_packet_manual_minimal.json")
    assert_valid(SCHEMA_DIR / "p2_case_packet.schema.json", packet)
    assert packet["input"]["input_type"] == "manual"
    assert "known_fields" in packet["input"]
    assert packet["case_identity"]["identity_confidence"] == "low"
    assert packet["verified_artifacts"] == []


def test_downstream_examples_validate():
    assert_valid(SCHEMA_DIR / "p2_to_p3.schema.json", load_json(EXAMPLE_DIR / "p2_to_p3_artifact.json"))
    assert_valid(SCHEMA_DIR / "p2_to_p4.schema.json", load_json(EXAMPLE_DIR / "p2_to_p4_context.json"))
    assert_valid(SCHEMA_DIR / "p2_to_p5.schema.json", load_json(EXAMPLE_DIR / "p2_to_p5_seed.json"))


def test_manual_router_populates_known_fields_and_queries_without_locking_identity():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    packet_dict = packet.to_dict()

    assert_valid(SCHEMA_DIR / "p2_case_packet.schema.json", packet_dict)
    assert packet_dict["case_id"] == "manual_min_jian_guan_san_francisco_ca"
    assert packet_dict["input"]["input_type"] == "manual"
    assert packet_dict["input"]["known_fields"]["defendant_names"] == ["Min Jian Guan"]
    assert packet_dict["input"]["known_fields"]["jurisdiction"] == {
        "city": "San Francisco",
        "county": "San Francisco",
        "state": "CA",
    }
    assert "incident_date" in packet_dict["input"]["missing_fields"]
    assert len(packet_dict["input"]["candidate_queries"]) >= 3
    assert packet_dict["case_identity"]["identity_confidence"] == "low"
    assert packet_dict["case_identity"]["identity_anchors"] == []
    assert packet_dict["sources"] == []
    assert packet_dict["artifact_claims"] == []
    assert packet_dict["verified_artifacts"] == []


def test_manual_router_downstream_exports_validate():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")

    assert export_p2_to_p3(packet) == []
    assert_valid(SCHEMA_DIR / "p2_to_p4.schema.json", export_p2_to_p4(packet))
    assert_valid(SCHEMA_DIR / "p2_to_p5.schema.json", export_p2_to_p5(packet))


def test_legacy_adapter_exports_old_shape_and_dry_hole_stays_low():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    legacy_output = export_legacy_evaluate_result(packet)

    assert_valid_contract_definition("case_packet_legacy_evaluate_shape", legacy_output)
    assert legacy_output == {
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
