"""H4-lite — YouTube weak-input end-to-end fixture harness.

Wires the deterministic CaseGraph pipeline together for tests:

  YouTube title/description/transcript fixture
    → parse_youtube_case_input
    → plan_queries_from_youtube_result
    → FixtureMockConnector.collect (validates SourceRecord shape)
    → assemble_weak_input_case_packet
        → resolve_identity / resolve_outcome
        → extract_artifact_claims
        → resolve_muckrock_released_files
        → score_case_packet

No live APIs, no downloads, no LLMs. The mock connector is a
test-only `SourceConnector` whose `fetch()` returns a list given at
construction time — useful for asserting the full pipeline behavior
under different mock corroboration regimes.

Cross-cutting invariants asserted by this harness:
- weak YouTube input alone (no corroborating sources) cannot PRODUCE
- a claim_source for bodycam never graduates into a VerifiedArtifact
- only resolver-supplied public CDN file URLs (e.g. MuckRock
  released_files metadata) become VerifiedArtifacts
- PRODUCE requires high identity + concluded outcome + verified media
"""
import json
from pathlib import Path
from typing import Iterable, List

from pipeline2_discovery.casegraph import (
    SourceConnector,
    SourceRecord,
    assemble_weak_input_case_packet,
    parse_youtube_case_input,
    plan_queries_from_youtube_result,
    validate_connector_source_record,
)


YOUTUBE_FIXTURES = Path(__file__).resolve().parent / "fixtures" / "youtube_inputs"


def load_youtube_fixture(name: str):
    with (YOUTUBE_FIXTURES / name).open("r", encoding="utf-8") as f:
        return json.load(f)


class FixtureMockConnector(SourceConnector):
    """Test-only connector that returns a pre-built `SourceRecord` list.

    The records still flow through `validate_connector_source_record`
    (via `SourceConnector.collect`), which proves the fixture data
    respects the connector contract: no final-decision fields like
    `verified_artifacts`, `verdict`, etc.
    """

    name = "fixture_mock"

    def __init__(self, sources: List[SourceRecord]):
        self._sources = list(sources)

    def fetch(self, case_input) -> Iterable[SourceRecord]:
        return iter(self._sources)


def _source(text: str, **overrides) -> SourceRecord:
    defaults = dict(
        source_id="mock_source",
        url="https://example.test/source",
        title="Mock source",
        snippet=text,
        raw_text=text,
        source_type="news",
        source_authority="news",
        source_roles=[],
        api_name=None,
        discovered_via="mock_query",
        retrieved_at="2026-04-30T00:00:00Z",
        case_input_id="youtube_e2e_fixture",
        metadata={},
        cost_estimate=0.0,
        confidence_signals={},
        matched_case_fields=[],
    )
    defaults.update(overrides)
    return SourceRecord(**defaults)


def run_e2e(fixture_name: str, mock_sources: List[SourceRecord]):
    """End-to-end pipeline run: parse → plan → connector → assemble → score."""
    parsed = parse_youtube_case_input(load_youtube_fixture(fixture_name))
    plan = plan_queries_from_youtube_result(parsed)
    connector = FixtureMockConnector(sources=mock_sources)
    sources = connector.collect(parsed.case_input)
    return assemble_weak_input_case_packet(parsed, query_plan=plan, sources=sources)


def test_e2e_weak_youtube_alone_does_not_produce():
    result = run_e2e("transcript_suspect_agency_date.json", mock_sources=[])

    assert result.packet.case_identity.identity_confidence == "low"
    assert result.packet.case_identity.outcome_status == "unknown"
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict in ("HOLD", "SKIP")
    assert "weak_input_preliminary_packet" in result.packet.risk_flags


def test_e2e_corroborating_identity_source_lifts_identity_but_no_media_no_produce():
    court_source = _source(
        "Court records identify John Example in Phoenix. "
        "Phoenix Police Department records list incident date 2022-05-12.",
        source_id="mock_court_e2e",
        url="https://www.courtlistener.com/docket/e2e-mock",
        title="John Example court docket",
        source_type="court_docket",
        source_authority="court",
        source_roles=["identity_source"],
        api_name="courtlistener",
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )

    result = run_e2e("transcript_suspect_agency_date.json", mock_sources=[court_source])

    assert result.packet.case_identity.identity_confidence == "high"
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"
    assert "no_verified_media" in result.actionability.risk_flags


def test_e2e_bodycam_claim_source_does_not_create_verified_artifact():
    # A claim-only source carries claim_source but NOT
    # possible_artifact_source — it describes an artifact in text but
    # is not itself a candidate public artifact URL. Assembly must
    # extract an ArtifactClaim and never graduate a VerifiedArtifact.
    claim_source = _source(
        "The Phoenix Police Department released bodycam footage from the John Example incident.",
        source_id="mock_claim_e2e",
        url="https://www.youtube.com/watch?v=e2e_claimonly",
        title="Bodycam released in John Example case",
        source_type="video",
        source_authority="third_party",
        source_roles=["claim_source"],
        api_name="youtube_yt_dlp",
        matched_case_fields=["defendant_full_name", "agency"],
    )

    result = run_e2e("transcript_suspect_agency_date.json", mock_sources=[claim_source])

    bodycam_claims = [c for c in result.packet.artifact_claims if c.artifact_type == "bodycam"]
    assert bodycam_claims, "claim_source bodycam language should yield an ArtifactClaim"
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"
    assert "artifact_claim_unresolved" in result.actionability.reason_codes


def test_e2e_youtube_possible_artifact_source_graduates_through_assembly():
    # Sibling to the claim-only test above: the same YouTube URL with
    # possible_artifact_source role IS a candidate public artifact.
    # Assembly's orchestrator wiring must graduate it via the YouTube
    # media resolver into a VerifiedArtifact.
    artifact_source = _source(
        "Phoenix PD releases bodycam footage from the John Example incident.",
        source_id="mock_yt_artifact_e2e",
        url="https://www.youtube.com/watch?v=e2e_bodycam_artifact",
        title="Phoenix Police Department bodycam — John Example",
        source_type="video",
        source_authority="third_party",
        source_roles=["claim_source", "possible_artifact_source"],
        api_name="youtube_yt_dlp",
        metadata={"video_id": "e2e_bodycam_artifact", "channel": "Phoenix Police Department"},
        matched_case_fields=["defendant_full_name", "agency"],
    )

    result = run_e2e("transcript_suspect_agency_date.json", mock_sources=[artifact_source])

    youtube_artifacts = [
        a for a in result.packet.verified_artifacts
        if "youtube.com" in a.artifact_url
    ]
    assert youtube_artifacts, (
        "YouTube source with possible_artifact_source role should graduate via assembly"
    )
    assert youtube_artifacts[0].artifact_type == "bodycam"
    assert youtube_artifacts[0].format == "video"
    # Same artifact URL also lands on the assembly result's
    # ResolverOrchestrationResult (Option A: artifact_resolution is now
    # the orchestrator aggregate).
    assert any(
        a.artifact_url == youtube_artifacts[0].artifact_url
        for a in result.artifact_resolution.verified_artifacts
    )


def test_e2e_full_happy_path_with_verified_media_produces():
    court_source = _source(
        "Court records identify John Example in Phoenix. "
        "Phoenix Police Department records list incident date 2022-05-12. "
        "John Example was sentenced to 5 years in prison.",
        source_id="mock_court_full_e2e",
        url="https://www.courtlistener.com/docket/full-e2e",
        title="John Example sentenced docket",
        source_type="court_docket",
        source_authority="court",
        source_roles=["identity_source", "outcome_source"],
        api_name="courtlistener",
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )
    foia_source = _source(
        "Records produced include bodycam footage for John Example.",
        source_id="mock_muckrock_full_e2e",
        url="https://www.muckrock.com/foi/phoenix-300/bodycam-john-example/",
        title="Phoenix bodycam records production",
        source_type="foia_request",
        source_authority="foia",
        source_roles=["claim_source", "possible_artifact_source"],
        api_name="muckrock",
        metadata={
            "released_files": [
                {
                    "url": "https://cdn.muckrock.com/foia_files/full_e2e_bodycam.mp4",
                    "name": "bodycam john example",
                }
            ]
        },
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )

    result = run_e2e(
        "transcript_suspect_agency_date.json",
        mock_sources=[court_source, foia_source],
    )

    assert result.packet.case_identity.identity_confidence == "high"
    assert result.packet.case_identity.outcome_status == "sentenced"
    assert len(result.packet.verified_artifacts) == 1
    artifact = result.packet.verified_artifacts[0]
    assert artifact.artifact_type == "bodycam"
    assert artifact.format == "video"
    assert artifact.artifact_url.endswith(".mp4")
    assert result.actionability.verdict == "PRODUCE"


def test_e2e_query_plan_is_passed_through_to_packet_input():
    """The query planner output should land on the assembled CasePacket's
    candidate_queries so downstream consumers can replay it."""
    result = run_e2e("partial_fields_query_generation.json", mock_sources=[])

    assert result.query_plan.plans, "query planner should emit at least one connector plan"
    candidate_queries = result.packet.input.candidate_queries
    assert candidate_queries, "planned queries should be propagated onto packet input"
    # No PRODUCE without sources, regardless of how good the query plan looks.
    assert result.actionability.verdict != "PRODUCE"


def test_e2e_mock_connector_validates_source_records():
    """The connector contract rejects SourceRecords carrying final-decision
    fields. The harness exercises this via SourceConnector.collect()."""
    parsed = parse_youtube_case_input(load_youtube_fixture("transcript_suspect_agency_date.json"))
    bad_source = _source(
        "Mock source attempting to assert a verdict.",
        source_id="bad_source",
        metadata={"verdict": "PRODUCE"},  # forbidden by FORBIDDEN_SOURCE_FIELDS
    )
    connector = FixtureMockConnector(sources=[bad_source])
    raised = False
    try:
        connector.collect(parsed.case_input)
    except ValueError as exc:
        raised = True
        assert "verdict" in str(exc)
    assert raised, "FixtureMockConnector.collect must reject final-decision fields"


def test_e2e_florida_disturbance_fixture_alone_does_not_produce():
    """Run another YouTube fixture through the harness to prove the
    invariants hold across different parser inputs."""
    result = run_e2e("florida_disturbance.json", mock_sources=[])
    assert result.actionability.verdict != "PRODUCE"
    assert result.packet.verified_artifacts == []
    assert "weak_input_preliminary_packet" in result.packet.risk_flags
