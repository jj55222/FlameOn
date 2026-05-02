"""PIPE4 — multi-source CasePacket assembly.

Demonstrates that the existing ``assemble_structured_case_packet`` and
``assemble_weak_input_case_packet`` functions handle cross-connector
mock SourceRecords end-to-end through the deterministic gates:

  fixture
  -> parser
  -> query planner
  -> mocked SourceRecords (CourtListener / MuckRock / DocumentCloud
     shapes)
  -> identity resolver
  -> outcome resolver
  -> claim extractor
  -> resolver(s) only when concrete public URLs exist in metadata
  -> CasePacket
  -> score_case_packet

The tests assert all the cross-cutting invariants for a multi-source
flow:
- structured/weak fixture alone -> never PRODUCE
- court source corroborates identity/outcome but does not alone
  produce media artifacts
- claim_source with release language but no URL -> ArtifactClaim
  only, never VerifiedArtifact
- foia/documentcloud source with concrete public PDF URL ->
  VerifiedArtifact (document) but document-only stays HOLD
- foia source with concrete public CDN .mp4 URL -> VerifiedArtifact
  (media) and PRODUCE only when identity high + outcome concluded
- weak YouTube input + cross-connector mocks follows same gates
- no network calls anywhere
"""
import json
from pathlib import Path

from pipeline2_discovery.casegraph import (
    SourceRecord,
    assemble_structured_case_packet,
    assemble_weak_input_case_packet,
    extract_artifact_claims,
    parse_wapo_uof_case_input,
    parse_youtube_case_input,
    resolve_documentcloud_files,
    resolve_identity,
    resolve_muckrock_released_files,
    resolve_outcome,
    score_case_packet,
)


ROOT = Path(__file__).resolve().parents[1]
STRUCTURED_FIXTURE = ROOT / "tests" / "fixtures" / "structured_inputs" / "wapo_uof_complete.json"
YOUTUBE_FIXTURE = ROOT / "tests" / "fixtures" / "youtube_inputs" / "transcript_suspect_agency_date.json"


def load_structured():
    with STRUCTURED_FIXTURE.open("r", encoding="utf-8") as f:
        return parse_wapo_uof_case_input(json.load(f))


def load_youtube():
    with YOUTUBE_FIXTURE.open("r", encoding="utf-8") as f:
        return parse_youtube_case_input(json.load(f))


def _source(
    *,
    source_id,
    url,
    title,
    snippet,
    api_name,
    source_authority,
    source_type,
    source_roles,
    metadata=None,
    matched_case_fields=None,
    raw_text=None,
):
    return SourceRecord(
        source_id=source_id,
        url=url,
        title=title,
        snippet=snippet,
        raw_text=raw_text or snippet,
        source_type=source_type,
        source_roles=list(source_roles),
        source_authority=source_authority,
        api_name=api_name,
        discovered_via="mock_query",
        retrieved_at="2026-05-01T00:00:00Z",
        case_input_id="multisource_assembly_fixture",
        metadata=metadata or {},
        cost_estimate=0.0,
        confidence_signals={},
        matched_case_fields=list(matched_case_fields or []),
    )


def courtlistener_identity_outcome_source():
    return _source(
        source_id="cl_mock_identity",
        url="https://www.courtlistener.com/opinion/9100001/state-v-john-example/",
        title="State v. John Example",
        snippet=(
            "Court records identify John Example in Phoenix. The Phoenix Police "
            "Department investigation lists incident date 2022-05-12. The defendant "
            "was sentenced to 5 years in prison after pleading guilty."
        ),
        api_name="courtlistener",
        source_authority="court",
        source_type="court_opinion",
        source_roles=["identity_source", "outcome_source"],
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )


def muckrock_claim_only_source():
    return _source(
        source_id="mr_mock_claim_only",
        url="https://www.muckrock.com/foi/example/9200001/",
        title="Public records request for John Example bodycam",
        snippet="The Phoenix Police Department released bodycam footage from the John Example incident.",
        api_name="muckrock",
        source_authority="foia",
        source_type="foia_request",
        source_roles=["claim_source", "possible_artifact_source"],
        matched_case_fields=["defendant_full_name", "agency"],
    )


def muckrock_public_pdf_source():
    return _source(
        source_id="mr_mock_pdf",
        url="https://www.muckrock.com/foi/example/9300001/",
        title="Phoenix incident report production",
        snippet="Records produced include the use of force report and incident narrative.",
        api_name="muckrock",
        source_authority="foia",
        source_type="foia_request",
        source_roles=["claim_source", "possible_artifact_source"],
        metadata={
            "released_files": [
                {
                    "url": "https://cdn.muckrock.com/foia_files/incident_report_john_example.pdf",
                    "name": "incident_report",
                }
            ]
        },
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )


def muckrock_public_media_source():
    return _source(
        source_id="mr_mock_media",
        url="https://www.muckrock.com/foi/example/9400001/",
        title="Phoenix bodycam records production",
        snippet="Records produced include bodycam footage for John Example.",
        api_name="muckrock",
        source_authority="foia",
        source_type="foia_request",
        source_roles=["claim_source", "possible_artifact_source"],
        metadata={
            "released_files": [
                {
                    "url": "https://cdn.muckrock.com/foia_files/bodycam_john_example.mp4",
                    "name": "bodycam_john_example",
                }
            ]
        },
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )


def documentcloud_public_document_source():
    return _source(
        source_id="dc_mock_document",
        url="https://www.documentcloud.org/documents/9500001-phoenix-pd-john-example-incident",
        title="Phoenix PD incident report on John Example",
        snippet="Records produced include the incident report.",
        api_name="documentcloud",
        source_authority="documentcloud",
        source_type="documentcloud_document",
        source_roles=["claim_source", "possible_artifact_source"],
        metadata={
            "document_id": 9500001,
            "canonical_url": "https://www.documentcloud.org/documents/9500001-phoenix-pd-john-example-incident",
            "pdf_url": "https://s3.documentcloud.org/documents/9500001/phoenix-pd-john-example-incident.pdf",
            "publisher": "Arizona Republic",
            "access": "public",
        },
        matched_case_fields=["defendant_full_name", "agency"],
    )


# ---- Structured fixture × multi-source matrix ----------------------------


def test_structured_fixture_alone_does_not_produce():
    parsed = load_structured()
    result = assemble_structured_case_packet(parsed)

    assert result.packet.case_identity.identity_confidence == "low"
    assert result.packet.case_identity.outcome_status == "unknown"
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"


def test_structured_plus_court_source_lifts_outcome_but_no_media_artifacts():
    parsed = load_structured()
    result = assemble_structured_case_packet(
        parsed, sources=[courtlistener_identity_outcome_source()]
    )

    assert result.packet.case_identity.identity_confidence == "high"
    assert result.packet.case_identity.outcome_status == "sentenced"
    # Assembly chains the CourtListener resolver: a public /opinion/
    # URL graduates into a docket_docs document artifact. The court
    # source still does NOT yield a media artifact, so verdict stays
    # below PRODUCE and the no_verified_media risk flag survives.
    media_artifacts = [
        a
        for a in result.packet.verified_artifacts
        if a.artifact_type in {"bodycam", "interrogation", "court_video", "dispatch_911"}
    ]
    assert media_artifacts == [], "court source must not yield a media artifact"
    assert result.actionability.verdict != "PRODUCE"
    assert "no_verified_media" in result.actionability.risk_flags


def test_structured_plus_claim_only_muckrock_yields_artifactclaim_not_verifiedartifact():
    parsed = load_structured()
    result = assemble_structured_case_packet(
        parsed, sources=[muckrock_claim_only_source()]
    )

    bodycam_claims = [c for c in result.packet.artifact_claims if c.artifact_type == "bodycam"]
    assert bodycam_claims, "claim_source release language should yield an ArtifactClaim"
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"
    assert "artifact_claim_unresolved" in result.actionability.reason_codes


def test_structured_plus_documentcloud_public_pdf_yields_document_artifact_only_hold():
    """Document-only artifacts contribute to research completeness but
    do not unlock PRODUCE."""
    parsed = load_structured()
    sources = [
        courtlistener_identity_outcome_source(),
        documentcloud_public_document_source(),
    ]
    result = assemble_structured_case_packet(parsed, sources=sources)

    # Document-only resolver path: invoke DocumentCloud resolver
    # against the assembled packet. The assembly already chained the
    # MuckRock resolver — we add the DocumentCloud one explicitly here.
    resolve_documentcloud_files(result.packet)
    rescore = score_case_packet(result.packet)

    document_artifacts = [
        a for a in result.packet.verified_artifacts if a.artifact_type == "docket_docs"
    ]
    media_artifacts = [
        a for a in result.packet.verified_artifacts if a.artifact_type == "bodycam"
    ]
    assert document_artifacts, "DocumentCloud public PDF should yield a document artifact"
    assert media_artifacts == [], "no media artifact should be created from a document URL"
    # Document-only -> HOLD even with high identity + concluded outcome.
    assert rescore.verdict == "HOLD"
    assert "document_only_hold" in rescore.reason_codes


def test_structured_plus_muckrock_public_pdf_yields_document_artifact_only_hold():
    parsed = load_structured()
    sources = [
        courtlistener_identity_outcome_source(),
        muckrock_public_pdf_source(),
    ]
    result = assemble_structured_case_packet(parsed, sources=sources)

    document_artifacts = [
        a for a in result.packet.verified_artifacts if a.artifact_type == "docket_docs"
    ]
    media_artifacts = [
        a
        for a in result.packet.verified_artifacts
        if a.artifact_type in {"bodycam", "interrogation", "court_video", "dispatch_911"}
    ]
    assert document_artifacts, "MuckRock public PDF should yield a document artifact"
    assert media_artifacts == [], "MuckRock document-only should not create media"
    assert result.actionability.verdict == "HOLD"
    assert "document_only_hold" in result.actionability.reason_codes


def test_structured_plus_court_plus_muckrock_media_produces():
    """Identity high (from CourtListener) + outcome concluded (from
    CourtListener) + verified media (from MuckRock public CDN .mp4)
    -> PRODUCE."""
    parsed = load_structured()
    sources = [
        courtlistener_identity_outcome_source(),
        muckrock_public_media_source(),
    ]
    result = assemble_structured_case_packet(parsed, sources=sources)

    media_artifacts = [
        a for a in result.packet.verified_artifacts if a.artifact_type == "bodycam"
    ]
    assert media_artifacts, "MuckRock public CDN .mp4 should yield a media artifact"
    assert media_artifacts[0].format == "video"
    assert result.actionability.verdict == "PRODUCE"


def test_structured_plus_three_connector_sources_combine_correctly():
    """All three connector shapes together: court (identity/outcome),
    documentcloud (document artifact), muckrock (media artifact)
    -> PRODUCE with mixed portfolio."""
    parsed = load_structured()
    sources = [
        courtlistener_identity_outcome_source(),
        documentcloud_public_document_source(),
        muckrock_public_media_source(),
    ]
    result = assemble_structured_case_packet(parsed, sources=sources)
    resolve_documentcloud_files(result.packet)
    rescore = score_case_packet(result.packet)

    artifact_types = {a.artifact_type for a in result.packet.verified_artifacts}
    assert "bodycam" in artifact_types, "media artifact should be present"
    assert "docket_docs" in artifact_types, "document artifact should be present"
    # Verdict can still PRODUCE because the media artifact + high
    # identity + concluded outcome unlock it; the document artifact is
    # supplementary.
    assert rescore.verdict == "PRODUCE"
    assert "supporting_documents_present" in rescore.reason_codes


# ---- YouTube weak input × multi-source matrix ----------------------------


def test_weak_youtube_alone_does_not_produce():
    parsed = load_youtube()
    result = assemble_weak_input_case_packet(parsed)
    assert result.packet.case_identity.identity_confidence == "low"
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"


def test_weak_youtube_plus_court_plus_muckrock_media_produces_under_gates():
    parsed = load_youtube()
    sources = [
        courtlistener_identity_outcome_source(),
        muckrock_public_media_source(),
    ]
    result = assemble_weak_input_case_packet(parsed, sources=sources)

    media_artifacts = [
        a for a in result.packet.verified_artifacts if a.artifact_type == "bodycam"
    ]
    assert media_artifacts, "MuckRock public CDN .mp4 should yield a media artifact"
    # Under the YouTube weak-input flow, the same gates (identity high,
    # outcome concluded, media present, no severe risks) decide PRODUCE.
    assert result.actionability.verdict == "PRODUCE"


def test_weak_youtube_plus_claim_only_does_not_produce():
    parsed = load_youtube()
    sources = [
        courtlistener_identity_outcome_source(),
        muckrock_claim_only_source(),
    ]
    result = assemble_weak_input_case_packet(parsed, sources=sources)
    bodycam_claims = [c for c in result.packet.artifact_claims if c.artifact_type == "bodycam"]
    assert bodycam_claims, "claim should appear"
    # The CL /opinion/ URL graduates into a docket_docs document via
    # assembly's orchestrator wiring, but no media artifact graduates
    # (MuckRock claim-only has no released_files metadata). With media
    # missing, verdict stays below PRODUCE.
    media_artifacts = [
        a
        for a in result.packet.verified_artifacts
        if a.artifact_type in {"bodycam", "interrogation", "court_video", "dispatch_911"}
    ]
    assert media_artifacts == [], "no media artifact should graduate from claim-only inputs"
    assert result.actionability.verdict != "PRODUCE"


# ---- Network invariant ---------------------------------------------------


def test_multisource_assembly_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    parsed = load_structured()
    sources = [
        courtlistener_identity_outcome_source(),
        documentcloud_public_document_source(),
        muckrock_public_media_source(),
    ]
    result = assemble_structured_case_packet(parsed, sources=sources)
    resolve_documentcloud_files(result.packet)

    assert calls == [], f"multisource assembly made {len(calls)} live HTTP call(s)"


def test_multisource_assembly_does_not_create_verified_artifacts_from_claim_text_alone():
    """Even when sources have aggressive release language but ZERO
    metadata URLs, the assembly must not graduate them."""
    parsed = load_structured()
    claim_only_text = _source(
        source_id="claim_only_aggressive",
        url="https://example.test/no-metadata-url",
        title="Released bodycam, dispatch audio, and interrogation video",
        snippet=(
            "The agency released bodycam footage and dispatch audio. "
            "The interrogation video was published. Records were produced."
        ),
        api_name="news",
        source_authority="news",
        source_type="news",
        source_roles=["claim_source"],
        metadata={},  # NO URLs here — claim text only
    )
    result = assemble_structured_case_packet(parsed, sources=[claim_only_text])
    assert result.packet.artifact_claims  # claims fired
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"


# ---- Resolver ordering invariant ----------------------------------------


def test_running_resolvers_twice_is_idempotent_per_url():
    """Multi-source flows may run the same resolver more than once
    against an already-populated packet. Verified artifact URLs must
    dedupe — no duplicates land on packet.verified_artifacts."""
    parsed = load_structured()
    result = assemble_structured_case_packet(
        parsed, sources=[muckrock_public_media_source()]
    )
    initial = len(result.packet.verified_artifacts)
    assert initial >= 1

    # Re-run muckrock resolver on the same packet.
    resolve_muckrock_released_files(result.packet)
    after = len(result.packet.verified_artifacts)
    assert after == initial, "resolver re-run must not duplicate artifacts"


# ---- Assembly-level orchestrator wiring ---------------------------------


def test_assembly_graduates_documentcloud_artifact_without_explicit_resolver_call():
    """Assembly chains the DocumentCloud resolver via the orchestrator.
    A public DocumentCloud source must produce a VerifiedArtifact
    inside assembly itself — no manual resolve_documentcloud_files call
    required."""
    parsed = load_structured()
    sources = [
        courtlistener_identity_outcome_source(),
        documentcloud_public_document_source(),
    ]
    result = assemble_structured_case_packet(parsed, sources=sources)

    documentcloud_artifacts = [
        a for a in result.packet.verified_artifacts
        if a.source_authority == "documentcloud"
    ]
    assert documentcloud_artifacts, (
        "DocumentCloud public PDF should graduate inside assembly"
    )
    # Document-only -> HOLD even with high identity + concluded outcome.
    assert result.actionability.verdict == "HOLD"
    assert "document_only_hold" in result.actionability.reason_codes


def test_assembly_graduates_youtube_media_artifact_via_orchestrator():
    """Assembly chains the YouTube resolver via the orchestrator. A
    public YouTube source carrying possible_artifact_source role and a
    real watch URL must produce a VerifiedArtifact inside assembly
    itself."""
    parsed = load_structured()
    youtube_source = _source(
        source_id="yt_assembly_bodycam",
        url="https://www.youtube.com/watch?v=phx_bodycam_assembly",
        title="Phoenix Police Department bodycam — John Example",
        snippet="Official bodycam footage released by Phoenix PD.",
        api_name="youtube_yt_dlp",
        source_authority="third_party",
        source_type="video",
        source_roles=["claim_source", "possible_artifact_source"],
        metadata={
            "video_id": "phx_bodycam_assembly",
            "channel": "Phoenix Police Department",
        },
        matched_case_fields=["defendant_full_name", "agency"],
    )
    result = assemble_structured_case_packet(
        parsed,
        sources=[courtlistener_identity_outcome_source(), youtube_source],
    )

    youtube_artifacts = [
        a for a in result.packet.verified_artifacts
        if "youtube.com" in a.artifact_url
    ]
    assert youtube_artifacts, "YouTube source should graduate inside assembly"
    assert youtube_artifacts[0].artifact_type == "bodycam"
    assert youtube_artifacts[0].format == "video"


def test_assembly_dedupes_artifact_urls_across_resolvers():
    """The orchestrator wired into assembly dedupes artifact URLs
    across resolvers. A single URL surfaced by two resolver paths must
    yield only one VerifiedArtifact in the assembled packet."""
    parsed = load_structured()
    shared_url = "https://www.documentcloud.org/documents/9900001-cross-resolver-shared"
    muckrock_pointing_at_shared = _source(
        source_id="mr_pointing_at_dc",
        url="https://www.muckrock.com/foi/example/9900001/",
        title="MuckRock production referencing a DocumentCloud-hosted file",
        snippet="Records produced include a public DocumentCloud-hosted PDF.",
        api_name="muckrock",
        source_authority="foia",
        source_type="foia_request",
        source_roles=["claim_source", "possible_artifact_source"],
        metadata={"released_files": [{"url": shared_url, "name": "shared"}]},
    )
    documentcloud_with_same_url = _source(
        source_id="dc_with_same_url",
        url=shared_url,
        title="DocumentCloud document independently surfaced",
        snippet="Public document.",
        api_name="documentcloud",
        source_authority="documentcloud",
        source_type="documentcloud_document",
        source_roles=["claim_source", "possible_artifact_source"],
        metadata={"canonical_url": shared_url, "access": "public"},
    )

    result = assemble_structured_case_packet(
        parsed, sources=[muckrock_pointing_at_shared, documentcloud_with_same_url]
    )
    matching = [
        a for a in result.packet.verified_artifacts
        if a.artifact_url == shared_url
    ]
    assert len(matching) == 1, (
        "cross-resolver dedupe must collapse the shared URL to a single artifact; "
        f"got {[a.artifact_url for a in result.packet.verified_artifacts]}"
    )


def test_assembly_graduates_agency_ois_media_via_orchestrator():
    """Assembly chains the agency-OIS resolver via the orchestrator. A
    public agency-OIS bodycam source must graduate inside assembly
    itself, with source_authority="official" and the schema-canonical
    artifact_type derived from the agency's link_type hint."""
    parsed = load_structured()
    agency_source = _source(
        source_id="agency_ois::media::assembly::1",
        url="https://www.phoenix.gov/police/critical-incidents/2024-OIS-014/bwc.mp4",
        title="Phoenix PD bodycam release",
        snippet="Body-worn camera footage published by Phoenix PD.",
        api_name="agency_ois",
        source_authority="official",
        source_type="agency_media:bodycam_briefing",
        source_roles=["possible_artifact_source"],
        metadata={
            "agency": "Phoenix Police Department",
            "case_number": "2024-OIS-014",
            "media_link_type": "bodycam_briefing",
            "host_page_url": "https://www.phoenix.gov/police/critical-incidents/2024-OIS-014",
        },
        matched_case_fields=["agency"],
    )
    result = assemble_structured_case_packet(
        parsed, sources=[courtlistener_identity_outcome_source(), agency_source]
    )

    agency_media = [
        a for a in result.packet.verified_artifacts
        if a.source_authority == "official" and a.format == "video"
    ]
    assert agency_media, "agency-OIS media should graduate inside assembly"
    assert agency_media[0].artifact_type == "bodycam"
    assert agency_media[0].artifact_url.endswith(".mp4")
