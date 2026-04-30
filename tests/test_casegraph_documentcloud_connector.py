"""F4a — DocumentCloud connector wrapper (mocked, metadata-only).

Asserts that the DocumentCloud connector:
- emits SourceRecords only (never VerifiedArtifacts, never verdicts)
- assigns identity_source / outcome_source / claim_source roles from
  deterministic text signals
- preserves DocumentCloud-style metadata (document_id, canonical_url,
  pdf_url, publisher, published date, page_count, language)
- never assigns `artifact_source` (only `possible_artifact_source` when
  a candidate URL exists, leaving verification to the F4b resolver)
- respects max_results / max_queries hard caps
- handles missing URL/title gracefully
- is exercised entirely with FakeSession (no live API calls in pytest)
- live opt-in test gated by FLAMEON_RUN_LIVE_DOCUMENTCLOUD env var
"""
import json
import os
import time
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    DocumentCloudConnector,
    extract_artifact_claims,
    route_manual_defendant_jurisdiction,
)
from pipeline2_discovery.casegraph.connectors.documentcloud import DOCUMENTCLOUD_BASE


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "documentcloud"


def load_fixture(name):
    with (FIXTURES / name).open("r", encoding="utf-8") as f:
        return json.load(f)


class FakeResponse:
    def __init__(self, payload=None, status_code=200, exc=None):
        self.payload = payload if payload is not None else {"results": []}
        self.status_code = status_code
        self.exc = exc

    def json(self):
        if self.exc:
            raise self.exc
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, *, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        return self.response


def test_mocked_release_document_creates_documentcloud_source_record():
    packet = route_manual_defendant_jurisdiction("John Example", "Phoenix, Maricopa, AZ")
    session = FakeSession(FakeResponse(load_fixture("release_with_anchors.json")))
    connector = DocumentCloudConnector(session=session)

    sources = connector.search(packet.input, max_results=5, max_queries=1)

    assert len(sources) == 1
    source = sources[0]
    assert source.source_id == "documentcloud_7000123"
    assert source.source_type == "documentcloud_document"
    assert source.source_authority == "documentcloud"
    assert source.api_name == "documentcloud"
    assert source.discovered_via == "John Example"

    # identity_source from name + jurisdiction match in raw_text.
    assert "identity_source" in source.source_roles
    # claim_source from "Records produced" / "documents released" language.
    assert "claim_source" in source.source_roles
    # possible_artifact_source because canonical_url + pdf_url are present.
    assert "possible_artifact_source" in source.source_roles
    # NEVER full artifact_source — verification belongs to the F4b resolver.
    assert "artifact_source" not in source.source_roles

    # Metadata preservation.
    assert source.metadata["document_id"] == 7000123
    assert source.metadata["canonical_url"].startswith("https://www.documentcloud.org/documents/")
    assert source.metadata["pdf_url"].endswith(".pdf")
    assert source.metadata["publisher"] == "Arizona Republic"
    assert source.metadata["published_date"] == "2022-06-30T15:00:00Z"
    assert source.metadata["page_count"] == 14
    assert source.metadata["language"] == "eng"
    assert source.metadata["access"] == "public"

    # Confidence signals are diagnostic only — never imply verification.
    assert source.confidence_signals["claim_language_present"] is True
    assert source.confidence_signals["identity_anchor_present"] is True
    assert source.confidence_signals["has_pdf_url"] is True

    # Connector NEVER touches the case packet's artifact slot.
    assert packet.verified_artifacts == []
    assert packet.verdict == "HOLD"


def test_outcome_language_assigns_outcome_source_role():
    packet = route_manual_defendant_jurisdiction("John Example", "Phoenix, Maricopa, AZ")
    session = FakeSession(FakeResponse(load_fixture("outcome_disposition.json")))
    connector = DocumentCloudConnector(session=session)

    sources = connector.search(packet.input, max_results=5, max_queries=1)
    assert len(sources) == 1
    source = sources[0]

    assert "outcome_source" in source.source_roles
    assert "identity_source" in source.source_roles
    assert source.confidence_signals["outcome_language_present"] is True
    # Connector still does NOT assert outcome_status itself — the
    # outcome resolver reads source text in a downstream phase.
    assert packet.case_identity.outcome_status == "unknown"
    assert packet.verified_artifacts == []


def test_endpoint_params_and_optional_auth_header(monkeypatch):
    packet = route_manual_defendant_jurisdiction("John Example", "Phoenix, Maricopa, AZ")
    monkeypatch.delenv("DOCUMENTCLOUD_API_TOKEN", raising=False)
    session = FakeSession(FakeResponse({"results": []}))
    connector = DocumentCloudConnector(session=session)

    sources = connector.search(packet.input, max_results=4, max_queries=1)

    assert sources == []
    assert session.calls[0]["url"] == DOCUMENTCLOUD_BASE
    assert session.calls[0]["params"] == {
        "q": "John Example",
        "per_page": 4,
        "format": "json",
    }
    assert session.calls[0]["headers"] == {}
    assert connector.last_endpoint == DOCUMENTCLOUD_BASE
    assert connector.last_status_code == 200

    monkeypatch.setenv("DOCUMENTCLOUD_API_TOKEN", "test-doc-token")
    token_session = FakeSession(FakeResponse({"results": []}))
    token_connector = DocumentCloudConnector(session=token_session)
    token_connector.search(packet.input, max_results=1, max_queries=1)
    assert token_session.calls[0]["headers"] == {"Authorization": "Token test-doc-token"}


def test_missing_url_and_missing_title_handled_gracefully():
    packet = route_manual_defendant_jurisdiction("John Example", "Phoenix, Maricopa, AZ")
    session = FakeSession(FakeResponse(load_fixture("bare_metadata_no_anchors.json")))
    connector = DocumentCloudConnector(session=session)

    sources = connector.search(packet.input, max_results=5, max_queries=1)

    assert len(sources) == 2
    # First record has canonical_url but no pdf_url.
    first = sources[0]
    assert first.metadata["pdf_url"] is None
    assert first.metadata["canonical_url"].endswith("generic-policy-memo")
    # Second record has no canonical_url and no pdf_url — connector still
    # builds a SourceRecord with empty url and the title field set to
    # whatever was provided.
    second = sources[1]
    assert second.url == ""
    assert second.title == "Untitled draft"
    # No identity anchors (no name/jurisdiction in text), no outcome,
    # no claim language ⇒ falls back to claim_source role.
    assert "identity_source" not in second.source_roles
    assert "outcome_source" not in second.source_roles
    assert "claim_source" in second.source_roles
    assert "artifact_source" not in second.source_roles
    # No URL ⇒ no possible_artifact_source.
    assert "possible_artifact_source" not in second.source_roles


def test_documentcloud_connector_hard_caps_results():
    packet = route_manual_defendant_jurisdiction("John Example", "Phoenix, Maricopa, AZ")
    bulk = {
        "results": [
            {
                "id": 8000000 + i,
                "title": f"Document {i}",
                "description": "",
                "canonical_url": f"https://www.documentcloud.org/documents/{8000000 + i}-doc-{i}",
                "access": "public",
            }
            for i in range(12)
        ]
    }
    session = FakeSession(FakeResponse(bulk))
    connector = DocumentCloudConnector(session=session)

    sources = connector.search(packet.input, max_results=3, max_queries=1)

    assert len(sources) == 3
    assert session.calls[0]["params"]["per_page"] == 3
    # max_queries is also clamped: even with overflow, only 1 GET fired.
    assert len(session.calls) == 1


def test_documentcloud_404_returns_empty_with_diagnostics():
    packet = route_manual_defendant_jurisdiction("John Example", "Phoenix, Maricopa, AZ")
    connector = DocumentCloudConnector(session=FakeSession(FakeResponse(status_code=404)))

    sources = connector.search(packet.input, max_results=5, max_queries=1)

    assert sources == []
    assert connector.last_error == "HTTP 404"
    assert connector.last_status_code == 404
    assert connector.last_endpoint == DOCUMENTCLOUD_BASE


def test_connector_never_creates_verified_artifacts_or_verdicts_even_with_release_text():
    """Even when fixture text says 'documents released' and metadata
    has both a canonical_url and a pdf_url, the connector must NOT
    create VerifiedArtifacts or set a verdict. That work belongs to
    the F4b resolver."""
    packet = route_manual_defendant_jurisdiction("John Example", "Phoenix, Maricopa, AZ")
    session = FakeSession(FakeResponse(load_fixture("release_with_anchors.json")))
    connector = DocumentCloudConnector(session=session)

    packet.sources.extend(connector.search(packet.input, max_results=5, max_queries=1))

    # Connector emitted a claim_source — claim extractor will fire a
    # "released" claim from the raw_text, but no VerifiedArtifact yet.
    extract_artifact_claims(packet)
    assert any(claim.claim_label == "artifact_released" for claim in packet.artifact_claims)
    assert packet.verified_artifacts == []
    assert packet.verdict == "HOLD"


def test_results_field_alternative_keys():
    """DocumentCloud responses sometimes nest results under "documents"
    or return a bare list. The connector tolerates both."""
    packet = route_manual_defendant_jurisdiction("John Example", "Phoenix, Maricopa, AZ")

    nested_payload = {
        "documents": [
            {
                "id": 9000001,
                "title": "Nested under documents key",
                "canonical_url": "https://www.documentcloud.org/documents/9000001-nested",
                "access": "public",
            }
        ]
    }
    session = FakeSession(FakeResponse(nested_payload))
    sources = DocumentCloudConnector(session=session).search(packet.input, max_results=2, max_queries=1)
    assert len(sources) == 1
    assert sources[0].source_id == "documentcloud_9000001"

    bare_list_payload = [
        {
            "id": 9000002,
            "title": "Bare list response",
            "canonical_url": "https://www.documentcloud.org/documents/9000002-bare",
            "access": "public",
        }
    ]
    session2 = FakeSession(FakeResponse(bare_list_payload))
    sources2 = DocumentCloudConnector(session=session2).search(packet.input, max_results=2, max_queries=1)
    assert len(sources2) == 1
    assert sources2[0].source_id == "documentcloud_9000002"


@pytest.mark.skipif(
    os.environ.get("FLAMEON_RUN_LIVE_DOCUMENTCLOUD") != "1",
    reason="live DocumentCloud smoke is opt-in",
)
def test_live_documentcloud_smoke_metadata_only():
    """Opt-in only: actual GET to DocumentCloud's public search endpoint.
    Default-skipped. The connector still emits SourceRecords only and
    never creates VerifiedArtifacts."""
    packet = route_manual_defendant_jurisdiction("bodycam", "")
    packet.input.known_fields["defendant_names"] = ["bodycam"]
    connector = DocumentCloudConnector()
    started = time.perf_counter()
    sources = connector.search(packet.input, max_results=3, max_queries=1)
    runtime = time.perf_counter() - started

    print(f"LIVE_DOCUMENTCLOUD_ENDPOINT={connector.last_endpoint or ''}")
    print(f"LIVE_DOCUMENTCLOUD_STATUS_CODE={connector.last_status_code if connector.last_status_code is not None else ''}")
    print(f"LIVE_DOCUMENTCLOUD_RESULT_COUNT={len(sources)}")
    print(f"LIVE_DOCUMENTCLOUD_RUNTIME_SECONDS={runtime:.2f}")

    assert connector.last_query == "bodycam"
    assert len(sources) <= 3
    assert packet.verified_artifacts == []
    assert packet.verdict == "HOLD"
