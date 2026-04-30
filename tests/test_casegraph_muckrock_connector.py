import json
import os
import time
from pathlib import Path

from jsonschema import Draft7Validator
import pytest

from pipeline2_discovery.casegraph import (
    MuckRockConnector,
    extract_artifact_claims,
    route_manual_defendant_jurisdiction,
)
from pipeline2_discovery.casegraph.connectors.muckrock import MUCKROCK_BASE


ROOT = Path(__file__).resolve().parents[1]
CASE_PACKET_SCHEMA = ROOT / "schemas" / "p2_case_packet.schema.json"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def assert_valid_packet(packet):
    validator = Draft7Validator(load_json(CASE_PACKET_SCHEMA))
    errors = sorted(validator.iter_errors(packet.to_dict()), key=lambda e: list(e.path))
    assert errors == [], "\n".join(error.message for error in errors)


class FakeResponse:
    def __init__(self, payload=None, status_code=200, exc=None):
        self.payload = payload or {"results": []}
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


def request_result(idx, *, title, description, status="done", agency="Example Police Department"):
    return {
        "id": idx,
        "title": title,
        "description": description,
        "status": status,
        "absolute_url": f"https://www.muckrock.com/foi/example/request-{idx}/",
        "agency": {"name": agency},
        "date_submitted": "2026-04-01",
        "date_done": "2026-04-20",
        "jurisdiction": "California",
    }


def test_mocked_request_creates_foia_source_record():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    session = FakeSession(FakeResponse({
        "results": [
            request_result(
                101,
                title="Request for bodycam footage and 911 audio",
                description="Records request seeking body-worn camera footage for Min Jian Guan in San Francisco.",
            )
        ]
    }))
    connector = MuckRockConnector(session=session)

    sources = connector.search(packet.input, max_results=5, max_queries=1)

    assert len(sources) == 1
    source = sources[0]
    assert source.source_id == "muckrock_101"
    assert source.source_type == "foia_request"
    assert source.source_authority == "foia"
    assert "claim_source" in source.source_roles
    assert "identity_source" in source.source_roles
    assert "artifact_source" not in source.source_roles
    assert source.api_name == "muckrock"
    assert source.discovered_via == "Min Jian Guan"
    assert source.metadata["request_id"] == 101
    assert source.metadata["status"] == "done"
    assert source.metadata["agency"] == "Example Police Department"
    assert packet.verified_artifacts == []


def test_endpoint_params_and_auth_header_are_diagnostic(monkeypatch):
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    monkeypatch.delenv("MUCKROCK_API_TOKEN", raising=False)
    session = FakeSession(FakeResponse({"results": []}))
    connector = MuckRockConnector(session=session)

    sources = connector.search(packet.input, max_results=4, max_queries=1)

    assert sources == []
    assert session.calls[0]["url"] == MUCKROCK_BASE
    assert session.calls[0]["params"] == {
        "format": "json",
        "search": "Min Jian Guan",
        "page_size": 4,
        "status": "done",
    }
    assert session.calls[0]["headers"] == {}
    assert connector.last_endpoint == MUCKROCK_BASE
    assert connector.last_status_code == 200

    monkeypatch.setenv("MUCKROCK_API_TOKEN", "test-token")
    token_session = FakeSession(FakeResponse({"results": []}))
    token_connector = MuckRockConnector(session=token_session)

    token_connector.search(packet.input, max_results=1, max_queries=1)

    assert token_session.calls[0]["headers"] == {"Authorization": "Token test-token"}


def test_released_language_is_preserved_for_claim_extractor():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    session = FakeSession(FakeResponse({
        "results": [
            request_result(
                102,
                title="Documents released for Min Jian Guan",
                description="Responsive records produced included bodycam footage and documents released.",
            )
        ]
    }))
    connector = MuckRockConnector(session=session)
    packet.sources.extend(connector.search(packet.input, max_results=5, max_queries=1))

    assert "documents released" in packet.sources[0].raw_text.lower()
    extract_artifact_claims(packet)

    assert any(claim.claim_label == "artifact_released" for claim in packet.artifact_claims)
    assert packet.verified_artifacts == []
    assert_valid_packet(packet)


def test_withheld_language_is_preserved_without_verification():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    session = FakeSession(FakeResponse({
        "results": [
            request_result(
                103,
                title="Body camera video request denied",
                description="The agency refused to release the body camera video and records withheld under exemption.",
                status="rejected",
            )
        ]
    }))
    connector = MuckRockConnector(session=session)
    packet.sources.extend(connector.search(packet.input, max_results=5, max_queries=1))

    assert "records withheld" in packet.sources[0].raw_text.lower()
    assert packet.verified_artifacts == []
    assert_valid_packet(packet)


def test_muckrock_connector_hard_caps_results():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    results = [
        request_result(i, title=f"Request {i} bodycam", description="Public records request seeking bodycam footage.")
        for i in range(10)
    ]
    session = FakeSession(FakeResponse({"results": results}))
    connector = MuckRockConnector(session=session)

    sources = connector.search(packet.input, max_results=3, max_queries=1)

    assert len(sources) == 3
    assert session.calls[0]["params"]["page_size"] == 3


def test_muckrock_404_gracefully_returns_empty_with_diagnostics():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    connector = MuckRockConnector(session=FakeSession(FakeResponse(status_code=404)))

    sources = connector.search(packet.input, max_results=5, max_queries=1)

    assert sources == []
    assert connector.last_error == "HTTP 404"
    assert connector.last_status_code == 404
    assert connector.last_endpoint == MUCKROCK_BASE


@pytest.mark.skipif(os.environ.get("FLAMEON_RUN_LIVE_MUCKROCK") != "1", reason="live MuckRock smoke is opt-in")
def test_live_muckrock_smoke_metadata_only():
    packet = route_manual_defendant_jurisdiction("bodycam", "")
    packet.input.known_fields["defendant_names"] = ["bodycam"]
    connector = MuckRockConnector()
    started = time.perf_counter()
    sources = connector.search(packet.input, max_results=5, max_queries=1)
    runtime = time.perf_counter() - started
    packet.sources.extend(sources)

    print(f"LIVE_MUCKROCK_ENDPOINT={connector.last_endpoint or ''}")
    print(f"LIVE_MUCKROCK_QUERY={connector.last_query}")
    print(f"LIVE_MUCKROCK_STATUS_CODE={connector.last_status_code if connector.last_status_code is not None else ''}")
    print(f"LIVE_MUCKROCK_RESULT_COUNT={len(sources)}")
    print(f"LIVE_MUCKROCK_RUNTIME_SECONDS={runtime:.2f}")
    print(f"LIVE_MUCKROCK_ERROR={connector.last_error or ''}")

    assert connector.last_query == "bodycam"
    assert len(sources) <= 5
    assert packet.verified_artifacts == []
    assert packet.verdict == "HOLD"
    assert packet.case_identity.identity_confidence == "low"
