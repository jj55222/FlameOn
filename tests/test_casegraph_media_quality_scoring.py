"""SCORE2 - media-quality advisory scoring tests."""
from __future__ import annotations

from pipeline2_discovery.casegraph import (
    ArtifactClaim,
    CaseIdentity,
    CaseInput,
    CasePacket,
    Jurisdiction,
    VerifiedArtifact,
    score_case_packet,
)


def packet(*, identity: str = "high", outcome: str = "sentenced") -> CasePacket:
    return CasePacket(
        case_id="media_quality_scoring_fixture",
        input=CaseInput(input_type="fixture", known_fields={"defendant_names": ["Jane Example"]}),
        case_identity=CaseIdentity(
            defendant_names=["Jane Example"],
            agency="Example Police Department",
            jurisdiction=Jurisdiction(city="Phoenix", state="AZ"),
            incident_date="2024-01-02",
            case_numbers=["CR-2024-001"],
            charges=["murder"],
            victim_names=["Victim Example"],
            outcome_status=outcome,
            identity_confidence=identity,
            identity_anchors=["full_name", "jurisdiction", "agency", "case_number"],
        ),
    )


def artifact(
    artifact_id: str,
    artifact_type: str,
    *,
    title: str,
    fmt: str = "video",
    source_authority: str = "official",
    metadata: dict | None = None,
) -> VerifiedArtifact:
    merged_metadata = {"title": title}
    if metadata:
        merged_metadata.update(metadata)
    return VerifiedArtifact(
        artifact_id=artifact_id,
        artifact_type=artifact_type,
        artifact_url=f"https://www.youtube.com/watch?v={artifact_id}",
        source_url=f"https://www.youtube.com/watch?v={artifact_id}",
        source_authority=source_authority,
        downloadable=True,
        format=fmt,
        matched_case_fields=["defendant_full_name", "agency"],
        confidence=0.88,
        verification_method="fixture",
        metadata=merged_metadata,
    )


def claim() -> ArtifactClaim:
    return ArtifactClaim(
        claim_id="claim_001",
        artifact_type="bodycam",
        claim_label="artifact_released",
        claim_source_id="source_001",
        claim_source_url="https://example.org/news/bodycam-released",
        supporting_snippet="Police released bodycam footage.",
        claim_confidence=0.82,
    )


def test_tier_a_media_produce_has_no_weak_media_warning():
    case = packet()
    case.verified_artifacts.append(artifact("bodycam001", "bodycam", title="Police bodycam BWC footage"))

    result = score_case_packet(case)

    assert result.verdict == "PRODUCE"
    assert "produce_based_on_weak_or_uncertain_media" not in result.risk_flags
    assert "manually_verify_media_relevance" not in result.next_actions


def test_tier_b_media_produce_has_no_weak_media_warning():
    case = packet()
    case.verified_artifacts.append(artifact("court001", "court_video", title="Sentencing court video"))

    result = score_case_packet(case)

    assert result.verdict == "PRODUCE"
    assert "produce_based_on_weak_or_uncertain_media" not in result.risk_flags
    assert "manually_verify_media_relevance" not in result.next_actions


def test_tier_c_only_media_produce_gets_advisory_warning():
    case = packet()
    case.verified_artifacts.append(artifact("generic001", "other_video", title="Court hearing update"))

    result = score_case_packet(case)

    assert result.verdict == "PRODUCE"
    assert "produce_based_on_weak_or_uncertain_media" in result.risk_flags
    assert "produce_based_on_weak_or_uncertain_media" in result.reason_codes
    assert "manually_verify_media_relevance" in result.next_actions


def test_unknown_or_uncertain_media_produce_gets_manual_review_action():
    case = packet()
    case.verified_artifacts.extend(
        [
            artifact("weak001", "other_video", title="General case update"),
            artifact("weak002", "other_video", title="Court archive upload"),
        ]
    )

    result = score_case_packet(case)

    assert result.verdict == "PRODUCE"
    assert "manually_verify_media_relevance" in result.next_actions
    assert "produce_based_on_weak_or_uncertain_media" in result.risk_flags


def test_bodycam_query_mismatch_warning_present():
    case = packet()
    case.verified_artifacts.append(
        artifact(
            "mismatch001",
            "other_video",
            title="Court hearing update",
            metadata={"query_used": "Jane Example bodycam"},
        )
    )

    result = score_case_packet(case)

    assert result.verdict == "PRODUCE"
    assert "media_query_artifact_type_mismatch" in result.risk_flags
    assert "query_artifact_type_not_confirmed_by_metadata" in result.reason_codes


def test_document_only_remains_hold():
    case = packet()
    case.verified_artifacts.append(
        VerifiedArtifact(
            artifact_id="doc001",
            artifact_type="docket_docs",
            artifact_url="https://example.gov/docket.pdf",
            source_authority="court",
            downloadable=True,
            format="pdf",
            matched_case_fields=["case_number"],
            confidence=0.9,
        )
    )

    result = score_case_packet(case)

    assert result.verdict == "HOLD"
    assert "document_only_hold" in result.reason_codes


def test_claim_only_remains_hold():
    case = packet()
    case.artifact_claims.append(claim())

    result = score_case_packet(case)

    assert result.verdict == "HOLD"
    assert "claim_only_hold" in result.reason_codes


def test_weak_identity_remains_not_produce():
    case = packet(identity="low")
    case.verified_artifacts.append(artifact("bodycam002", "bodycam", title="Police bodycam footage"))

    result = score_case_packet(case)

    assert result.verdict != "PRODUCE"
    assert "weak_identity" in result.risk_flags


def test_media_quality_scoring_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    case = packet()
    case.verified_artifacts.append(artifact("bodycam003", "bodycam", title="Police bodycam footage"))
    score_case_packet(case)

    assert calls == []
