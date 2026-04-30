import json
from pathlib import Path

from pipeline2_discovery.casegraph import (
    SourceRecord,
    assemble_weak_input_case_packet,
    parse_youtube_case_input,
    plan_queries_from_youtube_result,
)


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "youtube_inputs"


def load_fixture(name):
    with (FIXTURES / name).open("r", encoding="utf-8") as f:
        return json.load(f)


def parsed_fixture(name):
    return parse_youtube_case_input(load_fixture(name))


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
        case_input_id="weak_input_fixture",
        metadata=metadata or {},
        cost_estimate=0.0,
        confidence_signals={},
        matched_case_fields=matched_case_fields or [],
    )


def test_weak_input_alone_stays_low_identity_and_not_produce():
    parsed = parsed_fixture("transcript_suspect_agency_date.json")
    result = assemble_weak_input_case_packet(parsed)

    assert result.packet.input.input_type == "youtube"
    assert result.packet.case_identity.defendant_names == ["John Example"]
    assert result.packet.case_identity.identity_confidence == "low"
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"
    assert result.identity_resolution.identity_confidence == "low"
    assert result.identity_resolution.identity_score == 0.0
    assert "weak_input_preliminary_packet" in result.packet.risk_flags


def test_mock_court_source_can_corroborate_identity_and_outcome():
    parsed = parsed_fixture("transcript_suspect_agency_date.json")
    court_source = source(
        "Court records identify John Example in Phoenix. Phoenix Police Department records list incident date 2022-05-12. John Example was sentenced to 5 years in prison.",
        source_id="mock_court_john_example",
        url="https://www.courtlistener.com/docket/mock",
        title="John Example court docket",
        source_type="court_docket",
        source_authority="court",
        source_roles=["identity_source", "outcome_source"],
        api_name="courtlistener",
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )

    result = assemble_weak_input_case_packet(parsed, sources=[court_source])

    assert result.packet.case_identity.identity_confidence == "high"
    assert result.packet.case_identity.outcome_status == "sentenced"
    assert result.outcome_resolution.outcome_confidence == "high"
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"
    assert "no_verified_media" in result.actionability.risk_flags


def test_artifact_claim_source_does_not_verify_artifact():
    parsed = parsed_fixture("transcript_suspect_agency_date.json")
    claim_source = source(
        "The Phoenix Police Department released bodycam footage from the John Example incident.",
        source_id="mock_youtube_claim",
        url="https://www.youtube.com/watch?v=claimonly",
        title="Bodycam released in John Example case",
        source_type="video",
        source_authority="third_party",
        source_roles=["claim_source", "possible_artifact_source"],
        api_name="youtube_yt_dlp",
        matched_case_fields=["defendant_full_name", "agency"],
    )

    result = assemble_weak_input_case_packet(parsed, sources=[claim_source])

    assert result.packet.artifact_claims
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"
    assert "artifact_claim_unresolved" in result.actionability.reason_codes


def test_mock_public_artifact_url_resolves_only_from_resolver_source():
    parsed = parsed_fixture("transcript_suspect_agency_date.json")
    court_source = source(
        "Court records identify John Example in Phoenix. Phoenix Police Department records list incident date 2022-05-12. John Example was sentenced to 5 years in prison.",
        source_id="mock_court_sentenced",
        url="https://www.courtlistener.com/docket/mock-sentenced",
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
                    "url": "https://cdn.muckrock.com/foia_files/bodycam_john_example.mp4",
                    "name": "bodycam john example",
                }
            ]
        },
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )

    result = assemble_weak_input_case_packet(parsed, sources=[court_source, foia_source])

    assert len(result.packet.verified_artifacts) == 1
    artifact = result.packet.verified_artifacts[0]
    assert artifact.artifact_type == "bodycam"
    assert artifact.format == "video"
    assert artifact.artifact_url.endswith(".mp4")
    assert result.artifact_resolution.verified_artifacts == [artifact]
    assert result.actionability.verdict == "PRODUCE"


def test_supplied_query_plan_is_reused_without_live_connector_calls():
    parsed = parsed_fixture("partial_fields_query_generation.json")
    query_plan = plan_queries_from_youtube_result(parsed)

    result = assemble_weak_input_case_packet(parsed, query_plan=query_plan, sources=[])

    assert result.query_plan is query_plan
    assert result.packet.sources == []
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"
