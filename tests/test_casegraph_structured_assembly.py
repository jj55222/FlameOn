"""W4-lite — structured-row mock CasePacket assembly.

Proves that a structured dataset row (e.g. WaPo UoF) flowing through
`assemble_structured_case_packet` honors the same gates as weak-input
assembly:

- structured row alone keeps identity low and verdict != PRODUCE
- corroborating identity_source records can lift identity / outcome
  but cannot create VerifiedArtifacts on their own
- claim_source records yield ArtifactClaims, never VerifiedArtifacts
- VerifiedArtifacts only appear when a resolver-style public artifact
  URL is supplied via `metadata.released_files`
- protected/private artifact URLs do not produce VerifiedArtifacts
"""
import json
from pathlib import Path

from pipeline2_discovery.casegraph import (
    SourceRecord,
    assemble_structured_case_packet,
    parse_wapo_uof_case_input,
    plan_queries_from_structured_result,
)


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "structured_inputs"


def load_fixture(name):
    with (FIXTURES / name).open("r", encoding="utf-8") as f:
        return json.load(f)


def parsed_fixture(name):
    return parse_wapo_uof_case_input(load_fixture(name))


def source(
    text,
    *,
    source_id="mock_source",
    url="https://example.test/source",
    title="Mock source",
    source_type="news",
    source_authority="news",
    source_roles=None,
    api_name=None,
    metadata=None,
    matched_case_fields=None,
):
    return SourceRecord(
        source_id=source_id,
        url=url,
        title=title,
        snippet=text,
        raw_text=text,
        source_type=source_type,
        source_authority=source_authority,
        source_roles=source_roles or [],
        api_name=api_name,
        discovered_via="mock_query",
        retrieved_at="2026-04-30T00:00:00Z",
        case_input_id="structured_input_fixture",
        metadata=metadata or {},
        cost_estimate=0.0,
        confidence_signals={},
        matched_case_fields=matched_case_fields or [],
    )


def test_structured_row_alone_stays_low_identity_and_not_produce():
    parsed = parsed_fixture("wapo_uof_complete.json")
    result = assemble_structured_case_packet(parsed)

    assert result.packet.input.input_type == "dataset_row"
    assert result.packet.case_identity.defendant_names == ["John Example"]
    assert result.packet.case_identity.agency == "Phoenix Police Department"
    assert result.packet.case_identity.jurisdiction.state == "AZ"
    assert result.packet.case_identity.incident_date == "2022-05-12"

    # Structured row carries candidate anchors only — never an identity lock.
    assert result.packet.case_identity.identity_confidence == "low"
    assert result.identity_resolution.identity_score == 0.0
    assert "structured_input_preliminary_packet" in result.packet.risk_flags
    assert "candidate_fields_not_identity_lock" in result.packet.risk_flags
    assert "dataset:wapo_uof" in result.packet.risk_flags

    # Outcome cannot be inferred from the row alone.
    assert result.packet.case_identity.outcome_status == "unknown"
    assert result.outcome_resolution.outcome_confidence == "low"

    # No artifacts; verdict cannot be PRODUCE.
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"


def test_structured_row_with_missing_fields_still_produces_query_plan_and_no_artifacts():
    parsed = parsed_fixture("wapo_uof_missing_fields.json")
    result = assemble_structured_case_packet(parsed)

    assert result.packet.case_identity.identity_confidence == "low"
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"

    # Query plan from structured parser is preserved on the packet input.
    planned = result.query_plan
    assert planned.plans, "structured row should produce at least one connector plan"
    assert any("structured" in flag or flag.startswith("dataset:") for flag in result.packet.risk_flags)


def test_corroborating_court_source_can_raise_identity_but_no_media_means_no_produce():
    parsed = parsed_fixture("wapo_uof_complete.json")
    court_source = source(
        "Court records identify John Example in Phoenix. Phoenix Police Department records list incident date 2022-05-12. John Example was sentenced to 5 years in prison.",
        source_id="mock_court_john_example",
        url="https://www.courtlistener.com/docket/structured-mock",
        title="John Example court docket",
        source_type="court_docket",
        source_authority="court",
        source_roles=["identity_source", "outcome_source"],
        api_name="courtlistener",
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )

    result = assemble_structured_case_packet(parsed, sources=[court_source])

    assert result.packet.case_identity.identity_confidence == "high"
    assert result.packet.case_identity.outcome_status == "sentenced"
    assert result.outcome_resolution.outcome_confidence == "high"

    # Corroboration alone never creates VerifiedArtifacts.
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"
    assert "no_verified_media" in result.actionability.risk_flags


def test_claim_source_for_bodycam_does_not_create_verified_artifact():
    parsed = parsed_fixture("wapo_uof_complete.json")
    claim_source = source(
        "The Phoenix Police Department released bodycam footage from the John Example incident.",
        source_id="mock_youtube_claim",
        url="https://www.youtube.com/watch?v=structured_claim",
        title="Bodycam released in John Example case",
        source_type="video",
        source_authority="third_party",
        source_roles=["claim_source", "possible_artifact_source"],
        api_name="youtube_yt_dlp",
        matched_case_fields=["defendant_full_name", "agency"],
    )

    result = assemble_structured_case_packet(parsed, sources=[claim_source])

    assert result.packet.artifact_claims, "claim_source should yield at least one ArtifactClaim"
    bodycam_claims = [c for c in result.packet.artifact_claims if c.artifact_type == "bodycam"]
    assert bodycam_claims, "bodycam claim should be extracted from the claim_source text"

    # The claim does not graduate into a VerifiedArtifact.
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"
    assert "artifact_claim_unresolved" in result.actionability.reason_codes


def test_verified_media_only_appears_from_supplied_resolver_metadata():
    parsed = parsed_fixture("wapo_uof_complete.json")
    court_source = source(
        "Court records identify John Example in Phoenix. Phoenix Police Department records list incident date 2022-05-12. John Example was sentenced to 5 years in prison.",
        source_id="mock_court_sentenced",
        url="https://www.courtlistener.com/docket/structured-sentenced",
        title="John Example sentenced docket",
        source_type="court_docket",
        source_authority="court",
        source_roles=["identity_source", "outcome_source"],
        api_name="courtlistener",
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )
    foia_source = source(
        "Records produced include bodycam footage for John Example.",
        source_id="mock_muckrock_files",
        url="https://www.muckrock.com/foi/phoenix-100/bodycam-john-example/",
        title="Phoenix bodycam records production",
        source_type="foia_request",
        source_authority="foia",
        source_roles=["claim_source", "possible_artifact_source"],
        api_name="muckrock",
        metadata={
            "released_files": [
                {
                    "url": "https://cdn.muckrock.com/foia_files/structured_bodycam_john_example.mp4",
                    "name": "bodycam john example",
                }
            ]
        },
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )

    result = assemble_structured_case_packet(parsed, sources=[court_source, foia_source])

    assert len(result.packet.verified_artifacts) == 1
    artifact = result.packet.verified_artifacts[0]
    assert artifact.artifact_type == "bodycam"
    assert artifact.format == "video"
    assert artifact.artifact_url.endswith(".mp4")
    assert result.artifact_resolution.verified_artifacts == [artifact]
    # With high identity, concluded outcome, and a verified bodycam, this row is PRODUCE-eligible.
    assert result.actionability.verdict == "PRODUCE"


def test_protected_metadata_url_does_not_create_verified_artifact():
    parsed = parsed_fixture("wapo_uof_complete.json")
    court_source = source(
        "Court records identify John Example in Phoenix. Phoenix Police Department records list incident date 2022-05-12. John Example was sentenced to 5 years in prison.",
        source_id="mock_court_protected_case",
        url="https://www.courtlistener.com/docket/structured-protected",
        title="John Example court docket",
        source_type="court_docket",
        source_authority="court",
        source_roles=["identity_source", "outcome_source"],
        api_name="courtlistener",
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )
    protected_foia = source(
        "Records produced include bodycam footage for John Example.",
        source_id="mock_muckrock_protected",
        url="https://www.muckrock.com/foi/phoenix-200/protected-bodycam/",
        title="Phoenix bodycam (protected)",
        source_type="foia_request",
        source_authority="foia",
        source_roles=["claim_source", "possible_artifact_source"],
        api_name="muckrock",
        metadata={
            "released_files": [
                {
                    "url": "https://cdn.muckrock.com/foia_files/login/private_bodycam.mp4",
                    "name": "bodycam protected",
                }
            ]
        },
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )

    result = assemble_structured_case_packet(parsed, sources=[court_source, protected_foia])

    assert result.packet.verified_artifacts == [], (
        "Protected/login-gated URLs must not become VerifiedArtifacts"
    )
    assert result.actionability.verdict != "PRODUCE"
    assert "no_verified_media" in result.actionability.risk_flags


def test_supplied_query_plan_is_reused_without_live_connector_calls():
    parsed = parsed_fixture("wapo_uof_complete.json")
    plan = plan_queries_from_structured_result(parsed)

    result = assemble_structured_case_packet(parsed, query_plan=plan, sources=[])

    assert result.query_plan is plan
    assert result.packet.sources == []
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"
    # Planned queries are surfaced on the case input for future connector calls.
    candidate_queries = result.packet.input.candidate_queries
    assert candidate_queries, "planned queries should be propagated onto the packet input"
