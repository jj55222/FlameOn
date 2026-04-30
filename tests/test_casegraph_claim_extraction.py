import json
from pathlib import Path

from jsonschema import Draft7Validator

from pipeline2_discovery.casegraph import (
    SourceRecord,
    extract_artifact_claims,
    route_manual_defendant_jurisdiction,
)


ROOT = Path(__file__).resolve().parents[1]
CASE_PACKET_SCHEMA = ROOT / "schemas" / "p2_case_packet.schema.json"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def assert_valid_packet(packet):
    validator = Draft7Validator(load_json(CASE_PACKET_SCHEMA))
    errors = sorted(validator.iter_errors(packet.to_dict()), key=lambda e: list(e.path))
    assert errors == [], "\n".join(error.message for error in errors)


def packet_with_source(text, *, source_roles=None, url="https://example.org/source", title="Fixture claim source"):
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    packet.sources.append(SourceRecord(
        source_id="claim_fixture",
        url=url,
        title=title,
        snippet=text,
        raw_text=text,
        source_type="news",
        source_roles=source_roles or ["claim_source"],
        source_authority="news",
        api_name=None,
        discovered_via="claim_fixture",
        retrieved_at="2026-04-30T00:00:00Z",
        case_input_id="fixture",
        metadata={},
        cost_estimate=0.0,
        confidence_signals={},
        matched_case_fields=["defendant_full_name"],
    ))
    return packet


def labels_and_types(packet):
    return {(claim.claim_label, claim.artifact_type) for claim in packet.artifact_claims}


def test_requested_bodycam_claim_does_not_verify_artifact():
    packet = packet_with_source("Public records request seeking bodycam footage and 911 audio.")

    result = extract_artifact_claims(packet)

    assert ("artifact_requested", "bodycam") in labels_and_types(packet)
    assert ("artifact_requested", "dispatch_911") in labels_and_types(packet)
    assert packet.verified_artifacts == []
    assert len(result.artifact_claims) == 2
    assert_valid_packet(packet)


def test_released_bodycam_claim_has_snippet_but_no_verified_artifact():
    packet = packet_with_source("Police released bodycam footage from the April incident.")

    extract_artifact_claims(packet)

    assert len(packet.artifact_claims) == 1
    claim = packet.artifact_claims[0]
    assert claim.claim_label == "artifact_released"
    assert claim.artifact_type == "bodycam"
    assert claim.supporting_snippet
    assert claim.claim_confidence >= 0.75
    assert packet.verified_artifacts == []
    assert_valid_packet(packet)


def test_withheld_video_claim_adds_risk_and_next_action():
    packet = packet_with_source("The agency refused to release the body camera video, citing an exemption.")

    result = extract_artifact_claims(packet)

    assert len(packet.artifact_claims) == 1
    claim = packet.artifact_claims[0]
    assert claim.claim_label == "artifact_withheld"
    assert claim.artifact_type == "bodycam"
    assert {"artifact_withheld", "access_limited"} <= set(packet.risk_flags)
    assert packet.next_actions
    assert result.next_actions
    assert packet.verified_artifacts == []
    assert_valid_packet(packet)


def test_mentioned_only_bodycam_claim_does_not_imply_release():
    packet = packet_with_source("The officer activated his bodycam before approaching the vehicle.")

    extract_artifact_claims(packet)

    assert len(packet.artifact_claims) == 1
    claim = packet.artifact_claims[0]
    assert claim.claim_label == "artifact_mentioned_only"
    assert claim.artifact_type == "bodycam"
    assert packet.verified_artifacts == []


def test_no_artifact_language_creates_no_claim():
    packet = packet_with_source("The defendant appeared in court on Monday.")

    result = extract_artifact_claims(packet)

    assert result.artifact_claims == []
    assert packet.artifact_claims == []
    assert packet.verified_artifacts == []
    assert_valid_packet(packet)


def test_multiple_artifact_types_from_production_language():
    packet = packet_with_source(
        "Records produced included the 911 call, body-worn camera video, and the probable cause affidavit."
    )

    extract_artifact_claims(packet)

    assert ("artifact_released", "dispatch_911") in labels_and_types(packet)
    assert ("artifact_released", "bodycam") in labels_and_types(packet)
    assert ("artifact_released", "docket_docs") in labels_and_types(packet)
    assert packet.verified_artifacts == []
    assert_valid_packet(packet)


def test_claim_source_with_artifact_looking_url_does_not_create_verified_artifact_or_upgrade_packet():
    packet = packet_with_source(
        "Police released bodycam footage.",
        source_roles=["claim_source"],
        url="https://www.youtube.com/watch?v=fixture",
        title="Bodycam video posted online",
    )

    extract_artifact_claims(packet)

    assert packet.artifact_claims
    assert packet.verified_artifacts == []
    assert packet.verdict == "HOLD"
    assert packet.case_identity.identity_confidence == "low"
    assert packet.scores.artifact_score == 0.0
    assert_valid_packet(packet)
