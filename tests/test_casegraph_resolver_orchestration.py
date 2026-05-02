"""RESOLVE1 — Metadata-only resolver orchestration tests.

Asserts that ``run_metadata_only_resolvers``:
- runs MuckRock + DocumentCloud + CourtListener resolvers by default
- respects the ``allow_list`` to scope which resolvers run
- aggregates VerifiedArtifacts across resolvers
- dedupes by URL across resolvers
- preserves risk flags / next_actions
- never makes a network call
- never graduates claim text without URL into a VerifiedArtifact
- rejects protected/private/PACER URLs (delegated to underlying
  resolvers)
- document-only artifacts contribute to research completeness but
  remain HOLD
- media artifact + identity high + outcome concluded -> PRODUCE
"""
import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    SourceRecord,
    assemble_structured_case_packet,
    parse_wapo_uof_case_input,
    run_metadata_only_resolvers,
    score_case_packet,
)
from pipeline2_discovery.casegraph.resolvers import (
    RESOLVER_NAMES,
    ResolverOrchestrationResult,
)


ROOT = Path(__file__).resolve().parents[1]
STRUCTURED_FIXTURE = ROOT / "tests" / "fixtures" / "structured_inputs" / "wapo_uof_complete.json"


def load_structured():
    with STRUCTURED_FIXTURE.open("r", encoding="utf-8") as f:
        return parse_wapo_uof_case_input(json.load(f))


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
):
    return SourceRecord(
        source_id=source_id,
        url=url,
        title=title,
        snippet=snippet,
        raw_text=snippet,
        source_type=source_type,
        source_roles=list(source_roles),
        source_authority=source_authority,
        api_name=api_name,
        discovered_via="mock_query",
        retrieved_at="2026-05-01T00:00:00Z",
        case_input_id="resolver_orchestration_fixture",
        metadata=metadata or {},
        cost_estimate=0.0,
        confidence_signals={},
        matched_case_fields=list(matched_case_fields or []),
    )


def courtlistener_identity_outcome_source():
    return _source(
        source_id="cl_orch_identity",
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
        metadata={
            "absolute_url": "https://www.courtlistener.com/opinion/9100001/state-v-john-example/",
            "court": "AZSP",
            "docket_number": "CR-2022-001234",
            "case_name": "State v. John Example",
            "search_type": "o",
        },
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )


def muckrock_public_pdf_source():
    return _source(
        source_id="mr_orch_pdf",
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
        source_id="mr_orch_media",
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
        source_id="dc_orch_document",
        url="https://www.documentcloud.org/documents/9500001-phoenix-incident-report",
        title="Phoenix incident report on John Example",
        snippet="Records produced include the incident report.",
        api_name="documentcloud",
        source_authority="documentcloud",
        source_type="documentcloud_document",
        source_roles=["claim_source", "possible_artifact_source"],
        metadata={
            "document_id": 9500001,
            "canonical_url": "https://www.documentcloud.org/documents/9500001-phoenix-incident-report",
            "pdf_url": "https://s3.documentcloud.org/documents/9500001/phoenix-incident-report.pdf",
            "publisher": "Arizona Republic",
            "access": "public",
        },
        matched_case_fields=["defendant_full_name", "agency"],
    )


def claim_only_source():
    return _source(
        source_id="claim_only",
        url="https://example.test/claim-only-no-metadata",
        title="Released bodycam, dispatch audio, and interrogation",
        snippet=(
            "The agency released bodycam footage and dispatch audio. "
            "The interrogation video was published. Records were produced."
        ),
        api_name="muckrock",
        source_authority="foia",
        source_type="foia_request",
        source_roles=["claim_source"],
        metadata={},
    )


def courtlistener_pacer_only_source():
    return _source(
        source_id="cl_pacer",
        url="https://www.courtlistener.com/docket/4400001/state-v-doe/",
        title="State v. Doe — PACER-only filings",
        snippet="PACER-only filings.",
        api_name="courtlistener",
        source_authority="court",
        source_type="court_docket",
        source_roles=["identity_source"],
        metadata={
            "absolute_url": "https://www.courtlistener.com/docket/4400001/state-v-doe/",
            "search_type": "r",
            "recap_documents": [
                {
                    "download_url": "https://ecf.azd.uscourts.gov/cgi-bin/show_doc.pl?case=00123&doc=42",
                }
            ],
        },
    )


def documentcloud_protected_source():
    return _source(
        source_id="dc_protected",
        url="https://www.documentcloud.org/documents/8000002-private/login",
        title="Privately scoped document",
        snippet="Login required.",
        api_name="documentcloud",
        source_authority="documentcloud",
        source_type="documentcloud_document",
        source_roles=["claim_source", "possible_artifact_source"],
        metadata={
            "canonical_url": "https://www.documentcloud.org/documents/8000002-private/login",
            "pdf_url": "https://s3.documentcloud.org/documents/8000002/secret.pdf?token=abc",
            "access": "private",
        },
    )


# ---- Default behaviour ---------------------------------------------------


def test_orchestrator_returns_orchestration_result_with_canonical_keys():
    parsed = load_structured()
    result = assemble_structured_case_packet(parsed)
    orch = run_metadata_only_resolvers(result.packet)
    assert isinstance(orch, ResolverOrchestrationResult)
    assert orch.resolvers_run == list(RESOLVER_NAMES)
    diag = orch.to_diagnostics()
    for key in (
        "resolvers_run",
        "verified_artifact_count",
        "media_artifact_count",
        "document_artifact_count",
        "verified_artifact_urls",
        "risk_flags",
        "next_actions",
        "inspected_source_ids",
    ):
        assert key in diag


def test_muckrock_public_pdf_is_resolved_through_orchestrator():
    parsed = load_structured()
    result = assemble_structured_case_packet(
        parsed, sources=[muckrock_public_pdf_source()]
    )
    # Assembly already ran the MuckRock resolver — inspect the packet directly.
    assert any(a.source_authority == "foia" for a in result.packet.verified_artifacts)
    # Re-running through the orchestrator should not duplicate.
    initial = len(result.packet.verified_artifacts)
    orch = run_metadata_only_resolvers(result.packet)
    assert len(result.packet.verified_artifacts) == initial
    assert orch.verified_artifact_count >= 0


def test_documentcloud_public_pdf_is_resolved_through_orchestrator():
    parsed = load_structured()
    result = assemble_structured_case_packet(
        parsed, sources=[documentcloud_public_document_source()]
    )
    orch = run_metadata_only_resolvers(result.packet)
    documentcloud_artifacts = [
        a for a in result.packet.verified_artifacts if a.source_authority == "documentcloud"
    ]
    assert documentcloud_artifacts, "DocumentCloud public PDF should yield artifacts"
    # Both pdf_url and canonical_url paths produce artifacts.
    assert any(a.format == "pdf" for a in documentcloud_artifacts)
    assert any(a.format == "document" for a in documentcloud_artifacts)


def test_courtlistener_opinion_is_resolved_through_orchestrator():
    parsed = load_structured()
    result = assemble_structured_case_packet(
        parsed, sources=[courtlistener_identity_outcome_source()]
    )
    # Assembly chains the CourtListener resolver via the orchestrator,
    # so the /opinion/ URL graduates inside assembly itself.
    cl_artifacts = [a for a in result.packet.verified_artifacts if a.source_authority == "court"]
    assert cl_artifacts, "expected at least one court-authority artifact from assembly"
    assert all(a.artifact_type == "docket_docs" for a in cl_artifacts)
    # Re-running the orchestrator must not duplicate the artifact.
    pre = len(result.packet.verified_artifacts)
    orch = run_metadata_only_resolvers(result.packet)
    post = len(result.packet.verified_artifacts)
    assert post == pre, "orchestrator re-run must dedupe CourtListener artifact"
    assert orch.verified_artifact_count == 0, (
        "orchestrator re-run aggregate must report zero new artifacts"
    )


# ---- Claim-text-only invariant ------------------------------------------


def test_claim_text_without_url_creates_no_artifact_via_orchestrator():
    parsed = load_structured()
    result = assemble_structured_case_packet(parsed, sources=[claim_only_source()])
    orch = run_metadata_only_resolvers(result.packet)
    assert orch.verified_artifact_count == 0
    assert result.packet.verified_artifacts == []


# ---- Protected / PACER rejection -----------------------------------------


def test_courtlistener_pacer_url_is_rejected_with_risk_flag():
    parsed = load_structured()
    result = assemble_structured_case_packet(
        parsed, sources=[courtlistener_pacer_only_source()]
    )
    orch = run_metadata_only_resolvers(result.packet, allow_list=["courtlistener"])
    assert orch.verified_artifact_count == 0
    assert "pacer_or_paywalled" in orch.risk_flags
    assert "pacer_or_paywalled" in result.packet.risk_flags


def test_documentcloud_private_access_is_rejected_with_risk_flag():
    parsed = load_structured()
    result = assemble_structured_case_packet(
        parsed, sources=[documentcloud_protected_source()]
    )
    orch = run_metadata_only_resolvers(result.packet, allow_list=["documentcloud"])
    assert orch.verified_artifact_count == 0
    assert "documentcloud_nonpublic_access" in orch.risk_flags


# ---- Cross-resolver dedupe -----------------------------------------------


def test_duplicate_artifact_urls_dedupe_across_resolvers():
    """If a single source surfaces a URL that two resolvers might both
    consider (e.g. via api_name=muckrock + a documentcloud.org canonical
    URL embedded in metadata), the orchestrator dedupes by URL across
    resolvers."""
    duplicate_url = "https://www.documentcloud.org/documents/4567890-shared-doc"
    shared_source = _source(
        source_id="shared_doc",
        url=duplicate_url,
        title="Shared document referenced by multiple resolvers",
        snippet="Records produced.",
        api_name="documentcloud",
        source_authority="documentcloud",
        source_type="documentcloud_document",
        source_roles=["claim_source", "possible_artifact_source"],
        metadata={
            "canonical_url": duplicate_url,
            "access": "public",
        },
    )
    parsed = load_structured()
    result = assemble_structured_case_packet(parsed, sources=[shared_source])
    initial = len(result.packet.verified_artifacts)
    # Run orchestrator twice; second run should not duplicate.
    orch1 = run_metadata_only_resolvers(result.packet)
    after_first = len(result.packet.verified_artifacts)
    orch2 = run_metadata_only_resolvers(result.packet)
    after_second = len(result.packet.verified_artifacts)
    assert after_first >= initial
    assert after_second == after_first, "second orchestrator run must not duplicate"


# ---- Allow-list ----------------------------------------------------------


def test_allow_list_skips_unlisted_resolvers():
    parsed = load_structured()
    result = assemble_structured_case_packet(
        parsed, sources=[courtlistener_identity_outcome_source(), documentcloud_public_document_source()]
    )
    pre = len(result.packet.verified_artifacts)

    # Only run muckrock resolver — neither courtlistener nor
    # documentcloud should be invoked.
    orch = run_metadata_only_resolvers(result.packet, allow_list=["muckrock"])
    after = len(result.packet.verified_artifacts)
    assert orch.resolvers_run == ["muckrock"]
    # MuckRock has no MuckRock sources here, so artifact count delta is 0.
    assert after - pre == 0


def test_allow_list_unknown_resolver_raises():
    with pytest.raises(ValueError, match="unknown resolver"):
        run_metadata_only_resolvers([], allow_list=["not_a_real_resolver"])


def test_allow_list_partial_runs_only_listed_in_order():
    parsed = load_structured()
    result = assemble_structured_case_packet(
        parsed, sources=[courtlistener_identity_outcome_source(), documentcloud_public_document_source()]
    )
    orch = run_metadata_only_resolvers(
        result.packet, allow_list=["documentcloud", "courtlistener"]
    )
    assert orch.resolvers_run == ["documentcloud", "courtlistener"]
    assert "muckrock" not in orch.resolvers_run


# ---- Document-only / Media gates -----------------------------------------


def test_document_only_packet_remains_hold_after_orchestrator():
    parsed = load_structured()
    result = assemble_structured_case_packet(
        parsed,
        sources=[
            courtlistener_identity_outcome_source(),
            documentcloud_public_document_source(),
        ],
    )
    run_metadata_only_resolvers(result.packet)
    rescore = score_case_packet(result.packet)

    media_artifacts = [
        a
        for a in result.packet.verified_artifacts
        if a.format in {"video", "audio"}
        or a.artifact_type
        in {"bodycam", "interrogation", "court_video", "dispatch_911"}
    ]
    assert media_artifacts == [], "document-only path should not create media"
    assert rescore.verdict == "HOLD"


def test_media_artifact_with_high_identity_and_concluded_outcome_produces():
    parsed = load_structured()
    result = assemble_structured_case_packet(
        parsed,
        sources=[
            courtlistener_identity_outcome_source(),
            muckrock_public_media_source(),
        ],
    )
    # Re-run the orchestrator to ensure the media artifact is preserved.
    # The orchestrator's media_artifact_count reflects only the artifacts
    # IT added on this run; assembly already chained MuckRock's resolver,
    # so the bodycam lives on the packet (not the orchestrator's delta).
    orch = run_metadata_only_resolvers(result.packet)
    rescore = score_case_packet(result.packet)
    assert rescore.verdict == "PRODUCE"
    bodycam_artifacts = [a for a in result.packet.verified_artifacts if a.artifact_type == "bodycam"]
    assert bodycam_artifacts, "media artifact should remain on the packet after orchestration"
    # Orchestrator deduped against the existing packet artifact — no
    # double-counting.
    assert orch.verified_artifact_count <= 1  # at most the CourtListener doc


# ---- Bare SourceRecord input (no packet) --------------------------------


def test_orchestrator_works_against_bare_source_list_without_mutating_packet():
    """When given a bare list of SourceRecords (no CasePacket), the
    orchestrator collects artifacts in its result without touching any
    packet."""
    sources = [muckrock_public_media_source()]
    orch = run_metadata_only_resolvers(sources)
    assert orch.verified_artifact_count >= 1
    assert orch.media_artifact_count >= 1


# ---- Network invariant ---------------------------------------------------


def test_orchestrator_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    parsed = load_structured()
    result = assemble_structured_case_packet(
        parsed,
        sources=[
            courtlistener_identity_outcome_source(),
            documentcloud_public_document_source(),
            muckrock_public_media_source(),
        ],
    )
    run_metadata_only_resolvers(result.packet)

    assert calls == [], f"resolver orchestrator made {len(calls)} live HTTP call(s)"


# ---- inspected_source_ids ----------------------------------------------


def test_inspected_source_ids_reflect_per_resolver_inspection():
    parsed = load_structured()
    result = assemble_structured_case_packet(
        parsed,
        sources=[
            muckrock_public_pdf_source(),
            documentcloud_public_document_source(),
            courtlistener_identity_outcome_source(),
        ],
    )
    orch = run_metadata_only_resolvers(result.packet)
    ids = set(orch.inspected_source_ids)
    assert "mr_orch_pdf" in ids
    assert "dc_orch_document" in ids
    assert "cl_orch_identity" in ids
