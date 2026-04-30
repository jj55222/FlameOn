"""F4b — DocumentCloud public document resolver.

Asserts that:
- public DocumentCloud canonical URLs become document VerifiedArtifacts
- public PDF URLs become pdf-format VerifiedArtifacts
- claim text without a concrete URL produces NO VerifiedArtifact
- protected/login/auth URLs are rejected with risk flags
- DocumentCloud `access` values like 'private' / 'organization' /
  'pending' / 'draft' / 'invisible' reject the entire source
- non-DocumentCloud sources are NOT inspected by this resolver
- the resolver never downloads, never scrapes, never OCRs
- a document-only CasePacket with verified DocumentCloud artifacts
  remains HOLD (not PRODUCE) because no media artifact exists
"""
import json
from pathlib import Path

from pipeline2_discovery.casegraph import (
    SourceRecord,
    resolve_documentcloud_files,
    route_manual_defendant_jurisdiction,
    score_case_packet,
)


def _source(
    *,
    source_id="documentcloud_test",
    url="",
    title="Test document",
    snippet="",
    raw_text="",
    api_name="documentcloud",
    source_authority="documentcloud",
    source_type="documentcloud_document",
    source_roles=None,
    metadata=None,
    matched_case_fields=None,
):
    return SourceRecord(
        source_id=source_id,
        url=url,
        title=title,
        snippet=snippet,
        raw_text=raw_text or snippet or title,
        source_type=source_type,
        source_roles=source_roles or ["claim_source", "possible_artifact_source"],
        source_authority=source_authority,
        api_name=api_name,
        discovered_via="mock_query",
        retrieved_at="2026-04-30T00:00:00Z",
        case_input_id="documentcloud_resolver_fixture",
        metadata=metadata or {},
        cost_estimate=0.0,
        confidence_signals={},
        matched_case_fields=matched_case_fields or [],
    )


def _packet_with(sources):
    packet = route_manual_defendant_jurisdiction("John Example", "Phoenix, Maricopa, AZ")
    packet.sources.extend(sources)
    return packet


def test_public_pdf_url_creates_verified_pdf_artifact():
    pdf_source = _source(
        source_id="documentcloud_7000123",
        url="https://www.documentcloud.org/documents/7000123-phoenix-pd-john-example-incident-report",
        title="Phoenix PD incident report on John Example",
        snippet="Records produced included the incident report.",
        metadata={
            "document_id": 7000123,
            "canonical_url": "https://www.documentcloud.org/documents/7000123-phoenix-pd-john-example-incident-report",
            "pdf_url": "https://s3.documentcloud.org/documents/7000123/phoenix-pd-john-example-incident-report.pdf",
            "publisher": "Arizona Republic",
            "published_date": "2022-06-30T15:00:00Z",
            "page_count": 14,
            "language": "eng",
            "access": "public",
        },
        matched_case_fields=["defendant_full_name"],
    )
    packet = _packet_with([pdf_source])

    result = resolve_documentcloud_files(packet)

    # Two distinct candidate URLs (pdf + canonical) → two artifacts.
    pdf_artifacts = [a for a in result.verified_artifacts if a.format == "pdf"]
    document_artifacts = [a for a in result.verified_artifacts if a.format == "document"]
    assert len(pdf_artifacts) == 1
    assert pdf_artifacts[0].artifact_url.endswith(".pdf")
    assert pdf_artifacts[0].downloadable is True
    assert pdf_artifacts[0].artifact_type == "docket_docs"
    assert pdf_artifacts[0].source_authority == "documentcloud"
    assert pdf_artifacts[0].verification_method == "documentcloud_pdf_url"

    # Canonical URL also resolved as a document artifact.
    assert len(document_artifacts) == 1
    assert "documentcloud.org/documents/" in document_artifacts[0].artifact_url
    assert document_artifacts[0].downloadable is False
    assert document_artifacts[0].verification_method == "documentcloud_canonical_url"

    # Both artifacts were appended to the packet.
    assert len(packet.verified_artifacts) == 2


def test_canonical_url_only_creates_document_artifact():
    canonical_source = _source(
        source_id="documentcloud_7000124",
        url="https://www.documentcloud.org/documents/7000124-john-example-sentencing-memo",
        title="Sentencing memorandum filed in John Example case",
        snippet="The defendant was sentenced after pleading guilty.",
        metadata={
            "document_id": 7000124,
            "canonical_url": "https://www.documentcloud.org/documents/7000124-john-example-sentencing-memo",
            "pdf_url": None,
            "publisher": "Maricopa County Public Records",
            "access": "public",
        },
    )
    packet = _packet_with([canonical_source])

    result = resolve_documentcloud_files(packet)

    assert len(result.verified_artifacts) == 1
    artifact = result.verified_artifacts[0]
    assert artifact.format == "document"
    assert artifact.downloadable is False
    assert artifact.artifact_type == "docket_docs"
    assert artifact.verification_method == "documentcloud_canonical_url"


def test_released_text_without_url_creates_no_verified_artifact():
    """Source raw_text says 'documents released' but metadata carries
    NO canonical_url, NO pdf_url, NO source url. Resolver must not
    fabricate an artifact."""
    text_only = _source(
        source_id="documentcloud_text_only",
        url="",
        title="Public records announcement",
        snippet="The agency confirmed documents were released last week, but no URL was attached.",
        metadata={
            "document_id": None,
            "canonical_url": None,
            "pdf_url": None,
            "access": "public",
        },
    )
    packet = _packet_with([text_only])

    result = resolve_documentcloud_files(packet)

    assert result.verified_artifacts == []
    assert packet.verified_artifacts == []
    # And we leave a follow-up next_action.
    assert any("DocumentCloud" in action for action in result.next_actions)


def test_protected_url_is_rejected_with_risk_flag():
    protected_source = _source(
        source_id="documentcloud_protected",
        url="https://www.documentcloud.org/documents/8000001-protected/login",
        title="Protected document",
        snippet="Login required.",
        metadata={
            "document_id": 8000001,
            "canonical_url": "https://www.documentcloud.org/documents/8000001-protected/login",
            "pdf_url": "https://s3.documentcloud.org/documents/8000001/secret.pdf?token=abc",
            "access": "public",  # access label is public but URLs carry login/token markers
        },
    )
    packet = _packet_with([protected_source])

    result = resolve_documentcloud_files(packet)

    assert result.verified_artifacts == []
    assert "protected_or_nonpublic" in result.risk_flags
    assert "protected_or_nonpublic" in packet.risk_flags


def test_nonpublic_access_rejects_entire_source():
    """When DocumentCloud access != 'public', skip the source entirely
    even if URLs look fine."""
    private_source = _source(
        source_id="documentcloud_private_access",
        url="https://www.documentcloud.org/documents/8000002-private-doc",
        title="Privately scoped document",
        snippet="Not yet released to the public.",
        metadata={
            "document_id": 8000002,
            "canonical_url": "https://www.documentcloud.org/documents/8000002-private-doc",
            "pdf_url": "https://s3.documentcloud.org/documents/8000002/private-doc.pdf",
            "access": "private",
        },
    )
    packet = _packet_with([private_source])

    result = resolve_documentcloud_files(packet)

    assert result.verified_artifacts == []
    assert "documentcloud_nonpublic_access" in result.risk_flags
    assert any("DocumentCloud" in action for action in result.next_actions)


def test_organization_and_pending_access_also_rejected():
    """All non-public DocumentCloud access tiers are skipped."""
    for access in ("organization", "pending", "draft", "invisible"):
        source = _source(
            source_id=f"documentcloud_{access}",
            url=f"https://www.documentcloud.org/documents/9100/{access}",
            metadata={
                "canonical_url": f"https://www.documentcloud.org/documents/9100/{access}",
                "pdf_url": "https://s3.documentcloud.org/documents/9100/file.pdf",
                "access": access,
            },
        )
        packet = _packet_with([source])
        result = resolve_documentcloud_files(packet)
        assert result.verified_artifacts == [], f"access={access} should not produce VerifiedArtifact"
        assert "documentcloud_nonpublic_access" in result.risk_flags


def test_non_documentcloud_source_is_not_inspected():
    """The resolver only looks at DocumentCloud-flavored sources. A
    foia or news source does NOT trigger this resolver."""
    foia_source = _source(
        source_id="muckrock_001",
        url="https://www.muckrock.com/foi/example/",
        api_name="muckrock",
        source_authority="foia",
        source_type="foia_request",
        metadata={
            "request_id": 1,
            # Even if pdf_url is here, the resolver shouldn't touch it
            # because the source is not a DocumentCloud source.
            "pdf_url": "https://example.org/foia/file.pdf",
        },
    )
    packet = _packet_with([foia_source])

    result = resolve_documentcloud_files(packet)
    assert result.verified_artifacts == []
    assert result.inspected_source_ids == []


def test_document_only_packet_remains_hold_through_scoring():
    """Even with verified DocumentCloud artifacts (documents only) +
    high identity + concluded outcome, the verdict must be HOLD because
    no media artifact exists."""
    pdf_source = _source(
        source_id="documentcloud_doc_only_hold",
        url="https://www.documentcloud.org/documents/7000130-disposition-order",
        title="Disposition order in John Example case",
        snippet="Court records identify John Example in Phoenix. The defendant was sentenced to 5 years in prison.",
        raw_text="Court records identify John Example in Phoenix. Phoenix Police Department records list incident date 2022-05-12. The defendant John Example was sentenced to 5 years in prison.",
        source_roles=["identity_source", "outcome_source", "possible_artifact_source"],
        metadata={
            "document_id": 7000130,
            "canonical_url": "https://www.documentcloud.org/documents/7000130-disposition-order",
            "pdf_url": "https://s3.documentcloud.org/documents/7000130/disposition-order.pdf",
            "publisher": "Maricopa County Public Records",
            "access": "public",
        },
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )
    packet = _packet_with([pdf_source])

    # Resolve identity + outcome + claims first to give the scorer everything.
    from pipeline2_discovery.casegraph import (
        extract_artifact_claims,
        resolve_identity,
        resolve_outcome,
    )

    resolve_identity(packet)
    resolve_outcome(packet)
    extract_artifact_claims(packet)
    resolve_documentcloud_files(packet)

    assert len(packet.verified_artifacts) >= 1
    assert all(a.format in {"pdf", "document"} for a in packet.verified_artifacts)

    # Document-only -> HOLD even with high identity + concluded outcome.
    actionability = score_case_packet(packet)
    assert actionability.verdict == "HOLD"
    assert "document_only_hold" in actionability.reason_codes
    assert "no_verified_media" in actionability.risk_flags


def test_existing_verified_artifact_url_is_not_duplicated():
    """If the packet already carries a VerifiedArtifact with the same
    URL (e.g. from a prior resolver pass), the DocumentCloud resolver
    must not append a duplicate."""
    pdf_source = _source(
        source_id="documentcloud_dup",
        url="https://www.documentcloud.org/documents/7000131-no-dup",
        metadata={
            "document_id": 7000131,
            "canonical_url": "https://www.documentcloud.org/documents/7000131-no-dup",
            "pdf_url": "https://s3.documentcloud.org/documents/7000131/no-dup.pdf",
            "access": "public",
        },
    )
    packet = _packet_with([pdf_source])
    # First pass.
    first = resolve_documentcloud_files(packet)
    initial_count = len(packet.verified_artifacts)
    assert initial_count == 2  # canonical + pdf
    # Second pass on the same packet — must dedupe.
    second = resolve_documentcloud_files(packet)
    assert len(packet.verified_artifacts) == initial_count
    # The second call sees both URLs as existing and emits no new artifacts.
    assert second.verified_artifacts == []


def test_pdf_artifact_carries_documentcloud_metadata():
    pdf_source = _source(
        source_id="documentcloud_metadata_check",
        url="https://www.documentcloud.org/documents/7000132-metadata-check",
        title="Use of force report",
        snippet="Police use of force report regarding the encounter.",
        metadata={
            "document_id": 7000132,
            "canonical_url": "https://www.documentcloud.org/documents/7000132-metadata-check",
            "pdf_url": "https://s3.documentcloud.org/documents/7000132/use-of-force-report.pdf",
            "publisher": "Phoenix Public Records Office",
            "published_date": "2024-01-01T00:00:00Z",
            "page_count": 7,
            "language": "eng",
            "access": "public",
        },
        matched_case_fields=["defendant_full_name"],
    )
    packet = _packet_with([pdf_source])

    result = resolve_documentcloud_files(packet)

    pdf = next(a for a in result.verified_artifacts if a.format == "pdf")
    assert pdf.metadata["documentcloud_id"] == 7000132
    assert pdf.metadata["publisher"] == "Phoenix Public Records Office"
    assert pdf.metadata["page_count"] == 7
    assert pdf.metadata["language"] == "eng"
    assert pdf.confidence > 0.5
    assert pdf.confidence <= 0.92
