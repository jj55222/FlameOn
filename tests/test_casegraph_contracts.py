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


# ---- score_result kwarg merges advisory signals (PR #12) ---------------
#
# The exporters accept an optional ActionabilityResult so freshly
# computed advisory risk_flags / next_actions reach the P4 / P5
# handoffs. score_case_packet stays pure; the packet is never mutated.
# When the kwarg is omitted, the exports are byte-identical to PR #11
# behavior — locked by the backwards-compat tests below.


class _FakeScoreResult:
    """Test-only stand-in for ActionabilityResult. The adapter only
    reads .risk_flags and .next_actions via getattr, so any object
    exposing those attributes is sufficient — keeps the contracts
    test free of the scoring import."""

    def __init__(self, risk_flags=None, next_actions=None):
        self.risk_flags = list(risk_flags or [])
        self.next_actions = list(next_actions or [])


def test_export_p2_to_p5_merges_score_result_advisories():
    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )
    score_result = _FakeScoreResult(
        risk_flags=[
            "outcome_not_concluded_advisory",
            "produce_with_pending_outcome",
        ],
        next_actions=[
            "Treat as production-ready with a pending-outcome caveat; "
            "verify outcome before publish.",
        ],
    )

    out = export_p2_to_p5(packet, score_result=score_result)

    # Schema still validates — the canonical fields are unconstrained
    # string arrays, so adding advisories doesn't break the contract.
    assert_valid(SCHEMA_DIR / "p2_to_p5.schema.json", out)
    risks = set(out["risk_flags"])
    actions_text = " ".join(out["next_actions"]).lower()
    assert "outcome_not_concluded_advisory" in risks
    assert "produce_with_pending_outcome" in risks
    assert "pending-outcome" in actions_text
    # Existing packet-level entries must be preserved at the front.
    assert out["risk_flags"][: len(packet.risk_flags)] == list(packet.risk_flags)


def test_export_p2_to_p4_merges_score_result_into_source_quality_notes():
    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )
    score_result = _FakeScoreResult(
        risk_flags=[
            "outcome_not_concluded_advisory",
            "produce_with_pending_outcome",
        ],
    )

    out = export_p2_to_p4(packet, score_result=score_result)

    assert_valid(SCHEMA_DIR / "p2_to_p4.schema.json", out)
    notes = set(out["source_quality_notes"])
    assert "outcome_not_concluded_advisory" in notes
    assert "produce_with_pending_outcome" in notes
    assert (
        out["source_quality_notes"][: len(packet.risk_flags)]
        == list(packet.risk_flags)
    )


def test_export_p2_to_p5_without_score_result_kwarg_is_backwards_compat():
    """Backwards-compat invariant: omitting the score_result kwarg
    must produce identical output to today's behavior. Direct callers
    in test_manual_router_downstream_exports_validate and external
    consumers continue to work unchanged."""
    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )

    no_kwarg = export_p2_to_p5(packet)
    explicit_none = export_p2_to_p5(packet, score_result=None)
    assert no_kwarg == explicit_none
    # Locks the legacy field shape: stored packet values only,
    # nothing from any score_result.
    assert no_kwarg["risk_flags"] == list(packet.risk_flags)
    assert no_kwarg["next_actions"] == list(packet.next_actions)


def test_export_p2_to_p4_without_score_result_kwarg_is_backwards_compat():
    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )

    no_kwarg = export_p2_to_p4(packet)
    explicit_none = export_p2_to_p4(packet, score_result=None)
    assert no_kwarg == explicit_none
    assert no_kwarg["source_quality_notes"] == list(packet.risk_flags)


def test_score_case_packet_remains_pure_after_handoff_export():
    """Score result advisories are merged into the EXPORT, never into
    the packet. Calling score_case_packet (via the public API) and
    then exporting must leave packet.risk_flags / packet.next_actions
    unchanged — the documented purity contract."""
    from pipeline2_discovery.casegraph import score_case_packet

    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )
    pre_risk_flags = list(packet.risk_flags)
    pre_next_actions = list(packet.next_actions)

    score_result = score_case_packet(packet)
    export_p2_to_p4(packet, score_result=score_result)
    export_p2_to_p5(packet, score_result=score_result)

    assert packet.risk_flags == pre_risk_flags
    assert packet.next_actions == pre_next_actions


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
