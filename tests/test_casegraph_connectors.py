import json
from pathlib import Path

import pytest
from jsonschema import Draft7Validator

from pipeline2_discovery.casegraph import (
    CaseInput,
    CasePacket,
    MockSourceConnector,
    SourceConnector,
    SourceRecord,
    route_manual_defendant_jurisdiction,
    validate_connector_source_record,
)


ROOT = Path(__file__).resolve().parents[1]
CASE_PACKET_SCHEMA = ROOT / "schemas" / "p2_case_packet.schema.json"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def assert_valid_case_packet(packet: CasePacket) -> None:
    validator = Draft7Validator(load_json(CASE_PACKET_SCHEMA))
    errors = sorted(validator.iter_errors(packet.to_dict()), key=lambda e: list(e.path))
    assert errors == [], "\n".join(error.message for error in errors)


class StaticConnector(SourceConnector):
    name = "static_test"

    def fetch(self, case_input: CaseInput):
        yield SourceRecord(
            source_id="static_identity_001",
            url="https://example.org/case/min-jian-guan",
            title="Example identity source",
            snippet="Min Jian Guan case in San Francisco.",
            raw_text="Min Jian Guan case in San Francisco.",
            source_type="news",
            source_roles=["identity_source"],
            source_authority="news",
            api_name=None,
            discovered_via="static_fixture",
            retrieved_at="2026-04-29T23:30:00Z",
            case_input_id=case_input.raw_input.get("defendant_names"),
            metadata={"fixture": True},
            cost_estimate=0.0,
            confidence_signals={"matched_name": True, "matched_city": True},
            matched_case_fields=["defendant_full_name", "city"],
        )


def test_source_record_validates_with_connector_fields():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    source = StaticConnector().collect(packet.input)[0]

    assert source.source_id == "static_identity_001"
    assert source.source_roles == ["identity_source"]
    assert source.cost_estimate == 0.0
    validate_connector_source_record(source)


def test_connector_interface_collects_sources_without_final_decisions():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    sources = StaticConnector().collect(packet.input)
    packet.sources.extend(sources)

    assert packet.verdict == "HOLD"
    assert packet.case_identity.identity_confidence == "low"
    assert packet.verified_artifacts == []
    assert_valid_case_packet(packet)


def test_connector_source_record_rejects_final_confidence_signal():
    bad_source = SourceRecord(
        source_id="bad_001",
        url="https://example.org/bad",
        title="Bad source",
        snippet="",
        raw_text="",
        source_type="web",
        source_roles=["identity_source"],
        source_authority="unknown",
        api_name=None,
        discovered_via="static_fixture",
        retrieved_at="2026-04-29T23:30:00Z",
        case_input_id="bad",
        metadata={},
        cost_estimate=0.0,
        confidence_signals={"final_confidence": "high"},
        matched_case_fields=[],
    )

    with pytest.raises(ValueError, match="final-decision"):
        validate_connector_source_record(bad_source)


def test_mock_connector_returns_identity_claim_and_artifact_source_records():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    sources = MockSourceConnector().collect(packet.input)

    assert [source.source_id for source in sources] == [
        "mock_identity_001",
        "mock_claim_001",
        "mock_artifact_like_001",
    ]
    assert sources[0].source_roles == ["identity_source"]
    assert sources[1].source_roles == ["claim_source"]
    assert sources[2].source_roles == ["artifact_source"]
    assert all(source.cost_estimate == 0.0 for source in sources)


def test_mock_connector_sources_attach_without_creating_verified_artifacts():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    packet.sources.extend(MockSourceConnector().collect(packet.input))

    assert len(packet.sources) == 3
    assert any("claim_source" in source.source_roles for source in packet.sources)
    assert any("artifact_source" in source.source_roles for source in packet.sources)
    assert packet.artifact_claims == []
    assert packet.verified_artifacts == []
    assert packet.verdict == "HOLD"
    assert packet.case_identity.identity_confidence == "low"
    assert_valid_case_packet(packet)
