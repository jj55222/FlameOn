from copy import deepcopy

from pipeline2_discovery.casegraph import (
    ArtifactClaim,
    SourceRecord,
    VerifiedArtifact,
    route_manual_defendant_jurisdiction,
    score_case_packet,
)


def base_packet(*, identity="high", outcome="sentenced", rich_context=False):
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    packet.case_identity.identity_confidence = identity
    packet.case_identity.identity_anchors = ["full_name", "jurisdiction", "agency"]
    packet.case_identity.outcome_status = outcome
    packet.risk_flags = []
    packet.next_actions = []
    if rich_context:
        packet.case_identity.case_numbers = ["SF-2024-001"]
        packet.case_identity.charges = ["murder"]
        packet.case_identity.victim_names = ["Example Victim"]
        packet.case_identity.agency = "San Francisco Police Department"
        packet.case_identity.incident_date = "2024-04-21"
    return packet


def artifact(artifact_id, artifact_type, *, fmt="video", authority="official", downloadable=True, risk_flags=None):
    ext = {
        "video": "mp4",
        "audio": "mp3",
        "pdf": "pdf",
        "document": "docx",
        "html": "html",
    }.get(fmt, "dat")
    return VerifiedArtifact(
        artifact_id=artifact_id,
        artifact_type=artifact_type,
        artifact_url=f"https://example.org/artifacts/{artifact_id}.{ext}",
        source_url="https://example.org/source",
        source_authority=authority,
        downloadable=downloadable,
        format=fmt,
        matched_case_fields=["defendant_full_name", "agency"],
        confidence=0.86,
        verification_method="file_extension",
        risk_flags=risk_flags or [],
        metadata={"fixture": True},
    )


def claim(label="artifact_released", artifact_type="bodycam"):
    return ArtifactClaim(
        claim_id="claim_001",
        artifact_type=artifact_type,
        claim_label=label,
        claim_source_id="source_claim",
        claim_source_url="https://example.org/news/bodycam-released",
        supporting_snippet="Police released bodycam footage.",
        claim_confidence=0.82,
    )


def source(source_id, *, authority="news", roles=None, source_type="news"):
    return SourceRecord(
        source_id=source_id,
        url=f"https://example.org/{source_id}",
        title=f"Fixture {source_id}",
        snippet="Fixture source.",
        raw_text="Fixture source.",
        source_type=source_type,
        source_roles=roles or ["identity_source"],
        source_authority=authority,
        api_name=None,
        discovered_via="fixture",
        retrieved_at="2026-04-30T00:00:00Z",
        case_input_id="fixture",
        metadata={},
        cost_estimate=0.0,
        confidence_signals={},
        matched_case_fields=["defendant_full_name"],
    )


def test_media_rich_case_produces_with_portfolio_strength():
    packet = base_packet(identity="high", outcome="sentenced", rich_context=True)
    packet.verified_artifacts.extend([
        artifact("bodycam_001", "bodycam", fmt="video", authority="official"),
        artifact("dispatch_001", "dispatch_911", fmt="audio", authority="official"),
        artifact("complaint_001", "docket_docs", fmt="pdf", authority="foia"),
    ])

    result = score_case_packet(packet)

    assert result.production_actionability_score >= 70
    assert result.verdict == "PRODUCE"
    assert "media_artifact_present" in result.reason_codes
    assert "artifact_portfolio_strong" in result.reason_codes
    assert result.artifact_category_counts["bodycam"] == 1
    assert result.artifact_category_counts["dispatch_911"] == 1


def test_single_strong_official_bodycam_can_produce_but_scores_below_media_rich_case():
    packet = base_packet(identity="high", outcome="sentenced")
    packet.verified_artifacts.append(artifact("bodycam_001", "bodycam", fmt="video", authority="official"))

    result = score_case_packet(packet)

    assert result.production_actionability_score >= 70
    assert result.production_actionability_score < 95
    assert result.verdict == "PRODUCE"
    assert "bodycam_present" in result.reason_codes


def test_document_only_case_defaults_to_hold_not_produce():
    packet = base_packet(identity="high", outcome="sentenced")
    packet.verified_artifacts.append(artifact("complaint_001", "docket_docs", fmt="pdf", authority="foia"))

    result = score_case_packet(packet)

    assert result.research_completeness_score >= 50
    assert result.production_actionability_score < 70
    assert result.verdict == "HOLD"
    assert "document_only_hold" in result.reason_codes
    assert "no_verified_media" in result.reason_codes
    assert "Locate verified media/audio/video artifact before production." in result.next_actions


def test_claim_only_case_holds_until_public_artifact_url_is_resolved():
    packet = base_packet(identity="high", outcome="sentenced")
    packet.artifact_claims.append(claim())

    result = score_case_packet(packet)

    assert packet.verified_artifacts == []
    assert result.verdict == "HOLD"
    assert "artifact_claim_unresolved" in result.reason_codes
    assert "claim_only_hold" in result.reason_codes
    assert "Resolve artifact claim into a public artifact URL." in result.next_actions


def test_weak_identity_blocks_produce_even_with_verified_media():
    packet = base_packet(identity="low", outcome="sentenced")
    packet.verified_artifacts.append(artifact("bodycam_001", "bodycam", fmt="video", authority="official"))

    result = score_case_packet(packet)

    assert result.verdict != "PRODUCE"
    assert {"weak_identity", "identity_unconfirmed"} <= set(result.risk_flags)


def test_charged_but_not_concluded_stays_hold_with_media():
    packet = base_packet(identity="high", outcome="charged")
    packet.verified_artifacts.append(artifact("bodycam_001", "bodycam", fmt="video", authority="official"))

    result = score_case_packet(packet)

    assert result.verdict == "HOLD"
    assert "outcome_not_concluded" in result.reason_codes


def test_protected_or_nonpublic_only_does_not_produce():
    packet = base_packet(identity="high", outcome="sentenced")
    packet.risk_flags.append("protected_or_nonpublic")

    result = score_case_packet(packet)

    assert result.verdict != "PRODUCE"
    assert "protected_or_nonpublic_only" in result.risk_flags


def test_no_artifacts_does_not_produce_and_points_to_artifact_discovery():
    packet = base_packet(identity="high", outcome="sentenced")

    result = score_case_packet(packet)

    assert result.verdict != "PRODUCE"
    assert "Locate verified media/audio/video artifact before production." in result.next_actions
    assert "no_verified_media" in result.risk_flags


def test_research_completeness_can_be_high_while_production_holds_for_documents_only():
    packet = base_packet(identity="high", outcome="sentenced", rich_context=True)
    packet.sources.extend([
        source("court_docket", authority="court", roles=["identity_source", "outcome_source"], source_type="court"),
        source("muckrock_request", authority="foia", roles=["claim_source"], source_type="foia_request"),
        source("news_context", authority="news", roles=["identity_source"], source_type="news"),
    ])
    packet.verified_artifacts.extend([
        artifact("complaint_001", "docket_docs", fmt="pdf", authority="foia"),
        artifact("affidavit_001", "document", fmt="pdf", authority="foia"),
    ])

    before = deepcopy(packet.to_dict())
    result = score_case_packet(packet)

    assert result.research_completeness_score > result.production_actionability_score
    assert result.research_completeness_score >= 80
    assert result.verdict == "HOLD"
    assert "document_only_hold" in result.reason_codes
    assert packet.to_dict() == before
