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
    """Test-only stand-in for ActionabilityResult. The adapter reads
    .verdict, .risk_flags, and .next_actions via getattr, so any
    object exposing those attributes is sufficient — keeps the
    contracts test free of the scoring import."""

    def __init__(self, risk_flags=None, next_actions=None, verdict=None):
        self.risk_flags = list(risk_flags or [])
        self.next_actions = list(next_actions or [])
        self.verdict = verdict


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
    # Verdict stays sourced from packet.verdict when no result threaded.
    assert no_kwarg["verdict"] == packet.verdict


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


# ---- score_result.verdict threading into P5 export --------------------
#
# Doctrine: scoring stays pure; the packet's stored verdict is the
# router default for portal-replay packets ("HOLD") regardless of the
# fresh scorer outcome. When a caller threads an ActionabilityResult,
# the P5 export should reflect the freshly computed verdict so
# downstream consumers don't see HOLD next to a PRODUCE production
# score. Without the kwarg, the export remains byte-identical to the
# pre-threading shape.


def test_export_p2_to_p5_uses_fresh_verdict_when_score_result_supplied():
    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )
    # The manual router pins packet.verdict to "HOLD" by default.
    assert packet.verdict == "HOLD"
    score_result = _FakeScoreResult(verdict="PRODUCE")

    out = export_p2_to_p5(packet, score_result=score_result)

    assert out["verdict"] == "PRODUCE", (
        "P5 export verdict must reflect score_result.verdict when threaded"
    )
    # Schema still validates the canonical PRODUCE/HOLD/SKIP enum.
    assert_valid(SCHEMA_DIR / "p2_to_p5.schema.json", out)
    # The packet itself must NOT be mutated by exporting.
    assert packet.verdict == "HOLD"


def test_export_p2_to_p5_falls_back_to_packet_verdict_when_score_result_lacks_verdict():
    """Defensive: a score_result without a populated verdict (None or
    empty string) must not silently overwrite the packet verdict — the
    export should fall back to the stored value."""
    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )
    score_result = _FakeScoreResult(verdict=None)

    out = export_p2_to_p5(packet, score_result=score_result)
    assert out["verdict"] == packet.verdict


def test_export_p2_to_p5_verdict_threading_works_for_skip():
    """The SKIP verdict path: scorer concludes SKIP for a
    conflicting-jurisdiction case. The export should surface SKIP
    rather than the packet's stored HOLD."""
    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )
    score_result = _FakeScoreResult(verdict="SKIP")

    out = export_p2_to_p5(packet, score_result=score_result)
    assert out["verdict"] == "SKIP"
    assert_valid(SCHEMA_DIR / "p2_to_p5.schema.json", out)


def test_export_p2_to_p4_verdict_unchanged_by_score_result():
    """P4 has no verdict field; threading score_result must not
    introduce one or otherwise alter the schema-required keys."""
    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )
    score_result = _FakeScoreResult(verdict="PRODUCE", risk_flags=["x"])

    out = export_p2_to_p4(packet, score_result=score_result)
    assert "verdict" not in out
    assert_valid(SCHEMA_DIR / "p2_to_p4.schema.json", out)


def test_export_p2_to_p5_verdict_does_not_mutate_packet_or_score_result():
    """Pure invariant: exporting must not touch either input."""
    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )
    pre_packet_verdict = packet.verdict
    pre_packet_risks = list(packet.risk_flags)
    pre_packet_actions = list(packet.next_actions)
    score_result = _FakeScoreResult(
        verdict="PRODUCE",
        risk_flags=["fresh_advisory"],
        next_actions=["fresh action"],
    )
    pre_score_verdict = score_result.verdict
    pre_score_risks = list(score_result.risk_flags)
    pre_score_actions = list(score_result.next_actions)

    export_p2_to_p5(packet, score_result=score_result)

    assert packet.verdict == pre_packet_verdict
    assert packet.risk_flags == pre_packet_risks
    assert packet.next_actions == pre_packet_actions
    assert score_result.verdict == pre_score_verdict
    assert score_result.risk_flags == pre_score_risks
    assert score_result.next_actions == pre_score_actions


# ---- Stale router-default risk-flag filter ---------------------------
#
# Doctrine: the manual router seeds packet.risk_flags with
# ["identity_not_locked", "no_verified_artifacts"] to mark a fresh
# manual case as uncorroborated. Once identity actually resolves to
# HIGH or a VerifiedArtifact graduates, those flags are stale. The
# filter strips them only from result/export views and never touches
# packet.risk_flags itself, preserving score_case_packet purity. Other
# flags pass through unchanged.


def _packet_with_high_identity_and_one_artifact():
    """Build a router packet, then directly set identity_confidence to
    'high' and append one VerifiedArtifact so the predicate's
    contradiction conditions are met. We mutate the packet here in the
    test only — production code never does this."""
    from pipeline2_discovery.casegraph.models import VerifiedArtifact

    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )
    packet.case_identity.identity_confidence = "high"
    packet.verified_artifacts.append(
        VerifiedArtifact(
            artifact_id="test_artifact_001",
            artifact_type="bodycam",
            artifact_url="https://example.gov/bwc.mp4",
            source_authority="official",
            downloadable=True,
            format="video",
        )
    )
    return packet


def test_is_stale_router_default_handles_known_router_flags():
    from pipeline2_discovery.casegraph.scoring import is_stale_router_default

    fresh = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )
    # identity=low, artifacts=[] → neither flag is stale.
    assert is_stale_router_default("identity_not_locked", fresh) is False
    assert is_stale_router_default("no_verified_artifacts", fresh) is False

    resolved = _packet_with_high_identity_and_one_artifact()
    # identity=high, artifacts has 1 → both flags are stale.
    assert is_stale_router_default("identity_not_locked", resolved) is True
    assert is_stale_router_default("no_verified_artifacts", resolved) is True


def test_is_stale_router_default_does_not_strip_unrelated_flags():
    from pipeline2_discovery.casegraph.scoring import is_stale_router_default

    resolved = _packet_with_high_identity_and_one_artifact()
    for unrelated in (
        "weak_identity",
        "protected_or_nonpublic",
        "protected_or_nonpublic_only",
        "conflicting_jurisdiction",
        "conflicting_outcome_signals",
        "artifact_unverified",
        "outcome_not_concluded_advisory",
        "produce_with_pending_outcome",
        "no_verified_media",
        "document_only",
        "weak_input_preliminary_packet",
        "dataset:wapo_uof",
    ):
        assert is_stale_router_default(unrelated, resolved) is False, (
            f"filter incorrectly classified {unrelated!r} as stale"
        )


def test_is_stale_router_default_medium_identity_does_not_strip():
    from pipeline2_discovery.casegraph.scoring import is_stale_router_default

    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )
    packet.case_identity.identity_confidence = "medium"
    # Medium ≠ "locked"; the router's framing still applies.
    assert is_stale_router_default("identity_not_locked", packet) is False


def test_is_stale_router_default_accepts_dict_or_dataclass():
    from pipeline2_discovery.casegraph.scoring import is_stale_router_default

    packet = _packet_with_high_identity_and_one_artifact()
    packet_dict = packet.to_dict()
    assert is_stale_router_default("identity_not_locked", packet_dict) is True
    assert is_stale_router_default("no_verified_artifacts", packet_dict) is True
    assert is_stale_router_default("identity_not_locked", packet) is True
    assert is_stale_router_default("no_verified_artifacts", packet) is True


def test_filter_stale_router_defaults_preserves_order_and_other_flags():
    from pipeline2_discovery.casegraph.scoring import filter_stale_router_defaults

    packet = _packet_with_high_identity_and_one_artifact()
    flags = [
        "identity_not_locked",
        "outcome_not_concluded_advisory",
        "no_verified_artifacts",
        "produce_with_pending_outcome",
    ]
    out = filter_stale_router_defaults(flags, packet)
    assert out == ["outcome_not_concluded_advisory", "produce_with_pending_outcome"]


def test_filter_stale_router_defaults_is_noop_for_fresh_router_packet():
    from pipeline2_discovery.casegraph.scoring import filter_stale_router_defaults

    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )
    flags = list(packet.risk_flags)
    assert filter_stale_router_defaults(flags, packet) == flags


def test_export_p2_to_p5_strips_stale_flags_when_packet_state_contradicts():
    packet = _packet_with_high_identity_and_one_artifact()
    score_result = _FakeScoreResult(
        verdict="PRODUCE",
        risk_flags=[
            "identity_not_locked",
            "no_verified_artifacts",
            "outcome_not_concluded_advisory",
        ],
    )

    out = export_p2_to_p5(packet, score_result=score_result)
    assert "identity_not_locked" not in out["risk_flags"]
    assert "no_verified_artifacts" not in out["risk_flags"]
    assert "outcome_not_concluded_advisory" in out["risk_flags"]
    assert_valid(SCHEMA_DIR / "p2_to_p5.schema.json", out)
    # Packet itself must stay untouched.
    assert "identity_not_locked" in packet.risk_flags
    assert "no_verified_artifacts" in packet.risk_flags


def test_export_p2_to_p4_strips_stale_flags_when_packet_state_contradicts():
    packet = _packet_with_high_identity_and_one_artifact()
    score_result = _FakeScoreResult(
        risk_flags=[
            "identity_not_locked",
            "no_verified_artifacts",
            "outcome_not_concluded_advisory",
        ],
    )

    out = export_p2_to_p4(packet, score_result=score_result)
    notes = out["source_quality_notes"]
    assert "identity_not_locked" not in notes
    assert "no_verified_artifacts" not in notes
    assert "outcome_not_concluded_advisory" in notes
    assert_valid(SCHEMA_DIR / "p2_to_p4.schema.json", out)


def test_export_p2_to_p5_keeps_stale_flags_when_packet_state_does_not_contradict():
    """A fresh manual-router packet still legitimately carries both
    flags — strip-conditions are not met. This locks the conservative
    scoping and keeps the schema example fixture (an 'empty manual
    seed') byte-identical."""
    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )

    out = export_p2_to_p5(packet)
    assert "identity_not_locked" in out["risk_flags"]
    assert "no_verified_artifacts" in out["risk_flags"]


def test_export_p2_to_p4_keeps_stale_flags_when_packet_state_does_not_contradict():
    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )

    out = export_p2_to_p4(packet)
    assert "identity_not_locked" in out["source_quality_notes"]
    assert "no_verified_artifacts" in out["source_quality_notes"]


def test_score_case_packet_result_risk_flags_excludes_stale_for_resolved_packet():
    from pipeline2_discovery.casegraph import score_case_packet

    packet = _packet_with_high_identity_and_one_artifact()
    result = score_case_packet(packet)
    assert "identity_not_locked" not in result.risk_flags
    assert "no_verified_artifacts" not in result.risk_flags
    # Packet was not mutated.
    assert "identity_not_locked" in packet.risk_flags
    assert "no_verified_artifacts" in packet.risk_flags


def test_score_case_packet_result_risk_flags_keeps_stale_for_unresolved_packet():
    from pipeline2_discovery.casegraph import score_case_packet

    packet = route_manual_defendant_jurisdiction(
        "Min Jian Guan", "San Francisco, San Francisco, CA"
    )
    result = score_case_packet(packet)
    # Identity is low and there are no artifacts — the flags still apply.
    assert "identity_not_locked" in result.risk_flags
    assert "no_verified_artifacts" in result.risk_flags
