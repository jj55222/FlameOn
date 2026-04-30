import json
from copy import deepcopy
from pathlib import Path

from jsonschema import Draft7Validator

from pipeline2_discovery.casegraph import (
    ArtifactClaim,
    CaseIdentity,
    CaseInput,
    CasePacket,
    Jurisdiction,
    Scores,
    SourceRecord,
    VerifiedArtifact,
    score_case_packet,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "casegraph_scenarios"
CASE_PACKET_SCHEMA = ROOT / "schemas" / "p2_case_packet.schema.json"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def assert_valid_case_packet(data):
    validator = Draft7Validator(load_json(CASE_PACKET_SCHEMA))
    errors = sorted(validator.iter_errors(data), key=lambda e: list(e.path))
    assert errors == [], "\n".join(error.message for error in errors)


def packet_from_dict(data):
    identity_data = dict(data["case_identity"])
    identity_data["jurisdiction"] = Jurisdiction(**identity_data["jurisdiction"])
    return CasePacket(
        case_id=data["case_id"],
        input=CaseInput(**data["input"]),
        case_identity=CaseIdentity(**identity_data),
        sources=[SourceRecord(**source) for source in data["sources"]],
        artifact_claims=[ArtifactClaim(**claim) for claim in data["artifact_claims"]],
        verified_artifacts=[VerifiedArtifact(**artifact) for artifact in data["verified_artifacts"]],
        scores=Scores(**data["scores"]),
        verdict=data["verdict"],
        next_actions=list(data["next_actions"]),
        risk_flags=list(data["risk_flags"]),
    )


def load_packet(name):
    data = load_json(FIXTURE_DIR / name)
    assert_valid_case_packet(data)
    return packet_from_dict(data)


def score_fixture(name):
    packet = load_packet(name)
    before = deepcopy(packet.to_dict())
    result = score_case_packet(packet)
    assert packet.to_dict() == before
    return packet, result


def test_media_rich_fixture_produces():
    _, result = score_fixture("media_rich_produce.json")

    assert result.verdict == "PRODUCE"
    assert result.production_actionability_score >= 70
    assert "media_artifact_present" in result.reason_codes
    assert {"artifact_portfolio_strong", "multiple_media_artifacts"} & set(result.reason_codes)


def test_document_only_fixture_holds():
    _, result = score_fixture("document_only_hold.json")

    assert result.verdict == "HOLD"
    assert result.research_completeness_score > result.production_actionability_score
    assert {"document_only_hold", "no_verified_media"} & set(result.reason_codes)
    assert result.verdict != "PRODUCE"


def test_claim_only_fixture_holds_without_verified_artifact():
    packet, result = score_fixture("claim_only_hold.json")

    assert result.verdict == "HOLD"
    assert "artifact_claim_unresolved" in result.reason_codes
    assert len(packet.verified_artifacts) == 0
    assert result.verdict != "PRODUCE"


def test_weak_identity_media_fixture_is_blocked_from_produce():
    _, result = score_fixture("weak_identity_media_blocked.json")

    assert result.verdict != "PRODUCE"
    assert {"weak_identity", "identity_unconfirmed"} & set(result.risk_flags)
    assert result.production_actionability_score < 70


def test_charged_with_media_fixture_holds():
    _, result = score_fixture("charged_with_media_hold.json")

    assert result.verdict == "HOLD"
    assert "outcome_not_concluded" in result.reason_codes
    assert result.verdict != "PRODUCE"


def test_protected_nonpublic_fixture_is_blocked():
    _, result = score_fixture("protected_nonpublic_blocked.json")

    assert result.verdict != "PRODUCE"
    assert {"protected_or_nonpublic_only", "protected_or_nonpublic"} & set(result.risk_flags)
    assert result.artifact_category_counts == {}


def test_multi_artifact_premium_fixture_produces_above_media_rich_fixture():
    _, media_rich = score_fixture("media_rich_produce.json")
    _, premium = score_fixture("multi_artifact_premium_produce.json")

    assert premium.verdict == "PRODUCE"
    assert premium.production_actionability_score > media_rich.production_actionability_score
    assert len(premium.artifact_category_counts) >= 3
    assert {"artifact_portfolio_strong", "artifact_portfolio_premium"} & set(premium.reason_codes)


def test_transcript_candidate_name_without_corroboration_does_not_produce():
    packet, result = score_fixture("transcript_candidate_name_hold.json")

    assert packet.input.input_type == "youtube"
    assert packet.case_identity.identity_confidence == "low"
    assert result.verdict != "PRODUCE"
    assert {"weak_identity", "identity_unconfirmed"} & set(result.risk_flags)


def test_transcript_artifact_claim_without_verified_url_holds():
    packet, result = score_fixture("transcript_artifact_claim_hold.json")

    assert packet.artifact_claims
    assert packet.verified_artifacts == []
    assert result.verdict == "HOLD"
    assert "artifact_claim_unresolved" in result.reason_codes
    assert result.verdict != "PRODUCE"


def test_transcript_corroborated_with_verified_media_can_produce():
    _, result = score_fixture("transcript_corroborated_media_produce.json")

    assert result.verdict == "PRODUCE"
    assert "high_identity" in result.reason_codes
    assert "media_artifact_present" in result.reason_codes
    assert "bodycam_present" in result.reason_codes


def test_noisy_transcript_bodycam_language_does_not_produce():
    packet, result = score_fixture("transcript_noisy_bodycam_not_produce.json")

    assert packet.case_identity.identity_confidence == "low"
    assert result.verdict != "PRODUCE"
    assert {"weak_identity", "identity_unconfirmed"} & set(result.risk_flags)
    assert "no_verified_media" in result.risk_flags


def test_structured_wapo_row_only_does_not_produce():
    packet, result = score_fixture("structured_wapo_row_only_not_produce.json")

    assert packet.input.input_type == "dataset_row"
    assert packet.case_identity.identity_confidence == "low"
    assert packet.verified_artifacts == []
    assert result.verdict != "PRODUCE"
    assert {"weak_identity", "identity_unconfirmed"} & set(result.risk_flags)


def test_structured_official_bodycam_claim_without_verified_url_holds():
    packet, result = score_fixture("structured_official_bodycam_claim_hold.json")

    assert packet.sources
    assert packet.artifact_claims
    assert packet.verified_artifacts == []
    assert result.verdict == "HOLD"
    assert "artifact_claim_unresolved" in result.reason_codes
    assert result.verdict != "PRODUCE"


def test_structured_verified_bodycam_with_outcome_can_produce():
    _, result = score_fixture("structured_verified_bodycam_produce.json")

    assert result.verdict == "PRODUCE"
    assert "high_identity" in result.reason_codes
    assert "sentenced_or_convicted" in result.reason_codes
    assert "bodycam_present" in result.reason_codes


def test_structured_conflicting_corroboration_blocks_produce():
    _, result = score_fixture("structured_conflicting_source_not_produce.json")

    assert result.verdict != "PRODUCE"
    assert "conflicting_jurisdiction" in result.risk_flags
