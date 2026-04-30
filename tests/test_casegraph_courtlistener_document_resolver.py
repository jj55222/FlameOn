"""F3b — CourtListener / RECAP public document metadata resolver.

Asserts that:
- public CourtListener opinion URLs become document VerifiedArtifacts
- public RECAP per-document URLs become document/pdf VerifiedArtifacts
  (when the underlying download_url has a .pdf extension and is not on
  a PACER host)
- a docket-only source (just /docket/<id>/<slug>/) creates NO
  VerifiedArtifact and emits a "find a public opinion or RECAP URL"
  next_action
- PACER URLs (ecf.uscourts.gov, pacer.uscourts.gov) are rejected with
  a `pacer_or_paywalled` risk flag
- protected/login URLs are rejected with `protected_or_nonpublic`
- non-CourtListener sources are not inspected by this resolver
- a document-only CasePacket (CourtListener documents but no media)
  remains HOLD through scoring
"""
from pipeline2_discovery.casegraph import (
    SourceRecord,
    extract_artifact_claims,
    resolve_courtlistener_documents,
    resolve_identity,
    resolve_outcome,
    route_manual_defendant_jurisdiction,
    score_case_packet,
)


def _source(
    *,
    source_id="courtlistener_test",
    url="",
    title="Court record",
    snippet="",
    raw_text="",
    api_name="courtlistener",
    source_authority="court",
    source_type="court_opinion",
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
        source_roles=source_roles or ["identity_source"],
        source_authority=source_authority,
        api_name=api_name,
        discovered_via="mock_query",
        retrieved_at="2026-04-30T00:00:00Z",
        case_input_id="courtlistener_resolver_fixture",
        metadata=metadata or {},
        cost_estimate=0.0,
        confidence_signals={},
        matched_case_fields=matched_case_fields or [],
    )


def _packet_with(sources):
    packet = route_manual_defendant_jurisdiction("John Example", "Phoenix, Maricopa, AZ")
    packet.sources.extend(sources)
    return packet


def test_public_courtlistener_opinion_url_creates_document_artifact():
    opinion = _source(
        source_id="courtlistener_111111",
        url="https://www.courtlistener.com/opinion/111111/state-v-john-example/",
        title="State v. John Example",
        snippet="Affirmed. The defendant was sentenced to 5 years in prison.",
        source_type="court_opinion",
        metadata={
            "court": "AZCA",
            "docket_number": "CR-2022-001234",
            "case_name": "State v. John Example",
            "date_filed": "2022-09-15",
            "absolute_url": "https://www.courtlistener.com/opinion/111111/state-v-john-example/",
            "search_type": "o",
        },
        matched_case_fields=["defendant_full_name"],
    )
    packet = _packet_with([opinion])

    result = resolve_courtlistener_documents(packet)

    assert len(result.verified_artifacts) == 1
    artifact = result.verified_artifacts[0]
    assert artifact.format == "document"
    assert artifact.artifact_type == "docket_docs"
    assert artifact.source_authority == "court"
    assert artifact.verification_method == "courtlistener_opinion"
    assert artifact.downloadable is False
    assert artifact.metadata["court"] == "AZCA"
    assert artifact.metadata["docket_number"] == "CR-2022-001234"


def test_public_recap_document_pdf_url_creates_pdf_artifact():
    recap_doc = _source(
        source_id="courtlistener_recap_222222",
        url="https://www.courtlistener.com/docket/200000/state-v-john-example/",
        title="State v. John Example — Indictment",
        snippet="Indictment filed in Maricopa County Superior Court.",
        source_type="court_docket",
        metadata={
            "court": "MARSP",
            "docket_number": "CR-2022-001234",
            "case_name": "State v. John Example",
            "absolute_url": "https://www.courtlistener.com/docket/200000/state-v-john-example/",
            "search_type": "r",
            "recap_documents": [
                {
                    "absolute_url": "https://www.courtlistener.com/recap-document/222222/indictment/",
                    "download_url": "https://storage.courtlistener.com/recap/gov.uscourts.azd.222222/gov.uscourts.azd.222222.1.0.pdf",
                    "filepath_ia": "https://archive.org/download/gov.uscourts.azd.222222/indictment.pdf",
                }
            ],
        },
    )
    packet = _packet_with([recap_doc])

    result = resolve_courtlistener_documents(packet)

    artifact_urls = [a.artifact_url for a in result.verified_artifacts]

    # The RECAP per-document landing page is a CourtListener public artifact.
    assert "https://www.courtlistener.com/recap-document/222222/indictment/" in artifact_urls
    # The download_url ends in .pdf and is not on a PACER host → accepted as external pdf.
    assert any(url.endswith(".pdf") and "storage.courtlistener.com" in url for url in artifact_urls)
    # Internet Archive backup also has .pdf extension → accepted.
    assert any(url.endswith(".pdf") and "archive.org" in url for url in artifact_urls)

    # The docket landing page itself is NOT an artifact.
    assert "https://www.courtlistener.com/docket/200000/state-v-john-example/" not in artifact_urls

    # All resolved artifacts use court source authority.
    assert all(a.source_authority == "court" for a in result.verified_artifacts)
    # All resolved artifacts are documents (no video/audio).
    assert all(a.artifact_type == "docket_docs" for a in result.verified_artifacts)


def test_docket_only_source_emits_no_verified_artifact_and_a_next_action():
    docket_only = _source(
        source_id="courtlistener_docket_only",
        url="https://www.courtlistener.com/docket/300000/state-v-john-example/",
        title="State v. John Example — docket",
        source_type="court_docket",
        metadata={
            "court": "AZ",
            "docket_number": "CR-2022-001234",
            "case_name": "State v. John Example",
            "absolute_url": "https://www.courtlistener.com/docket/300000/state-v-john-example/",
            "search_type": "r",
        },
    )
    packet = _packet_with([docket_only])

    result = resolve_courtlistener_documents(packet)

    assert result.verified_artifacts == []
    assert any("public opinion or RECAP" in action for action in result.next_actions)


def test_pacer_only_url_is_rejected_with_risk_flag():
    pacer_source = _source(
        source_id="courtlistener_pacer",
        url="https://www.courtlistener.com/docket/400000/state-v-john-example/",
        title="State v. John Example — PACER-only filings",
        source_type="court_docket",
        metadata={
            "court": "AZD",
            "docket_number": "2:22-cr-00123",
            "absolute_url": "https://www.courtlistener.com/docket/400000/state-v-john-example/",
            "search_type": "r",
            "recap_documents": [
                {
                    "download_url": "https://ecf.azd.uscourts.gov/cgi-bin/show_doc.pl?case=00123&doc=42",
                }
            ],
        },
    )
    packet = _packet_with([pacer_source])

    result = resolve_courtlistener_documents(packet)

    assert result.verified_artifacts == []
    assert "pacer_or_paywalled" in result.risk_flags
    assert "pacer_or_paywalled" in packet.risk_flags


def test_protected_url_is_rejected_with_risk_flag():
    protected = _source(
        source_id="courtlistener_protected",
        url="https://www.courtlistener.com/recap-document/500000/sealed/?token=abc",
        title="Sealed filing",
        source_type="court_docket",
        metadata={
            "absolute_url": "https://www.courtlistener.com/recap-document/500000/sealed/?token=abc",
            "search_type": "r",
        },
    )
    packet = _packet_with([protected])

    result = resolve_courtlistener_documents(packet)

    assert result.verified_artifacts == []
    assert "protected_or_nonpublic" in result.risk_flags


def test_non_courtlistener_source_is_not_inspected():
    foia_source = _source(
        source_id="muckrock_001",
        url="https://www.muckrock.com/foi/example/",
        api_name="muckrock",
        source_authority="foia",
        source_type="foia_request",
        metadata={
            "absolute_url": "https://www.muckrock.com/foi/example/",
            # Even with a courtlistener-shaped recap_documents block, the
            # resolver must skip the source because it isn't a CourtListener
            # source by api_name/authority/type/url.
            "recap_documents": [
                {"absolute_url": "https://www.courtlistener.com/recap-document/600000/x/"}
            ],
        },
    )
    packet = _packet_with([foia_source])

    result = resolve_courtlistener_documents(packet)
    assert result.verified_artifacts == []
    assert result.inspected_source_ids == []


def test_document_only_courtlistener_packet_remains_hold_through_scoring():
    opinion = _source(
        source_id="courtlistener_doc_only_700000",
        url="https://www.courtlistener.com/opinion/700000/state-v-john-example/",
        title="State v. John Example",
        snippet="Court records identify John Example in Phoenix. The defendant John Example was sentenced to 5 years in prison after pleading guilty.",
        raw_text="Court records identify John Example in Phoenix. Phoenix Police Department records list incident date 2022-05-12. The defendant John Example was sentenced to 5 years in prison after pleading guilty.",
        source_type="court_opinion",
        source_roles=["identity_source", "outcome_source"],
        metadata={
            "court": "AZCA",
            "docket_number": "CR-2022-001234",
            "case_name": "State v. John Example",
            "date_filed": "2022-09-15",
            "absolute_url": "https://www.courtlistener.com/opinion/700000/state-v-john-example/",
            "search_type": "o",
        },
        matched_case_fields=["defendant_full_name", "agency", "incident_date"],
    )
    packet = _packet_with([opinion])

    resolve_identity(packet)
    resolve_outcome(packet)
    extract_artifact_claims(packet)
    resolve_courtlistener_documents(packet)

    # Verified document artifact created.
    assert len(packet.verified_artifacts) == 1
    assert packet.verified_artifacts[0].format == "document"
    assert packet.verified_artifacts[0].artifact_type == "docket_docs"

    # Document-only ⇒ HOLD, never PRODUCE — the scoring rule fires
    # because no media artifact exists.
    actionability = score_case_packet(packet)
    assert actionability.verdict == "HOLD"
    assert "document_only_hold" in actionability.reason_codes
    assert "no_verified_media" in actionability.risk_flags


def test_existing_artifact_url_is_not_duplicated_on_second_resolve():
    opinion = _source(
        source_id="courtlistener_dup",
        url="https://www.courtlistener.com/opinion/800000/no-dup/",
        title="State v. Doe — opinion",
        source_type="court_opinion",
        metadata={
            "absolute_url": "https://www.courtlistener.com/opinion/800000/no-dup/",
            "search_type": "o",
        },
    )
    packet = _packet_with([opinion])

    first = resolve_courtlistener_documents(packet)
    initial = len(packet.verified_artifacts)
    assert initial == 1

    second = resolve_courtlistener_documents(packet)
    assert len(packet.verified_artifacts) == initial
    assert second.verified_artifacts == []


def test_external_public_pdf_outside_courtlistener_is_accepted_when_referenced():
    """If the CourtListener metadata references an external public PDF
    (e.g. an Internet Archive backup) and it's not a PACER host, the
    resolver still accepts it via the external_public_pdf path."""
    opinion = _source(
        source_id="courtlistener_ia_backup",
        url="https://www.courtlistener.com/opinion/900000/state-v-doe/",
        title="State v. Doe",
        source_type="court_opinion",
        metadata={
            "absolute_url": "https://www.courtlistener.com/opinion/900000/state-v-doe/",
            "filepath_ia": "https://archive.org/download/state-v-doe/order.pdf",
            "search_type": "o",
        },
    )
    packet = _packet_with([opinion])

    result = resolve_courtlistener_documents(packet)
    urls = [a.artifact_url for a in result.verified_artifacts]
    assert "https://archive.org/download/state-v-doe/order.pdf" in urls
    # And the external pdf is downloadable.
    pdf_artifact = next(a for a in result.verified_artifacts if a.artifact_url.endswith(".pdf"))
    assert pdf_artifact.format == "pdf"
    assert pdf_artifact.downloadable is True
    assert pdf_artifact.verification_method == "external_public_pdf"


def test_pacer_uscourts_gov_subdomain_is_rejected():
    """Any .uscourts.gov host is treated as PACER-flavored. The resolver
    must reject the entire URL set rather than accept it as a download."""
    source = _source(
        source_id="courtlistener_pacer_subdomain",
        url="https://www.courtlistener.com/docket/950000/x/",
        source_type="court_docket",
        metadata={
            "absolute_url": "https://www.courtlistener.com/docket/950000/x/",
            "search_type": "r",
            "recap_documents": [
                {"download_url": "https://pacer.psc.uscourts.gov/document/12345.pdf"}
            ],
        },
    )
    packet = _packet_with([source])

    result = resolve_courtlistener_documents(packet)
    assert result.verified_artifacts == []
    assert "pacer_or_paywalled" in result.risk_flags


def test_recap_document_pdf_artifact_carries_courtlistener_metadata():
    recap_doc = _source(
        source_id="courtlistener_recap_meta",
        url="https://www.courtlistener.com/docket/1000000/state-v-john-example/",
        title="State v. John Example — RECAP Doc 1",
        source_type="court_docket",
        metadata={
            "court": "AZD",
            "docket_number": "2:22-cr-00123",
            "case_name": "State v. John Example",
            "date_filed": "2022-08-01",
            "absolute_url": "https://www.courtlistener.com/docket/1000000/state-v-john-example/",
            "search_type": "r",
            "recap_documents": [
                {
                    "absolute_url": "https://www.courtlistener.com/recap-document/1000001/state-v-john-example-doc-1/",
                    "download_url": "https://storage.courtlistener.com/recap/gov.uscourts.azd.1000001.1.0.pdf",
                }
            ],
        },
    )
    packet = _packet_with([recap_doc])

    result = resolve_courtlistener_documents(packet)
    assert len(result.verified_artifacts) >= 2
    pdf_artifact = next(a for a in result.verified_artifacts if a.format == "pdf")
    assert pdf_artifact.metadata["court"] == "AZD"
    assert pdf_artifact.metadata["docket_number"] == "2:22-cr-00123"
    assert pdf_artifact.metadata["case_name"] == "State v. John Example"
    assert 0.5 < pdf_artifact.confidence <= 0.92
