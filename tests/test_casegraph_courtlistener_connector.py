import json
import os
import time
from pathlib import Path

from jsonschema import Draft7Validator
import pytest

from pipeline2_discovery.casegraph import (
    CourtListenerConnector,
    route_manual_defendant_jurisdiction,
)
from pipeline2_discovery.casegraph.connectors.courtlistener import COURTLISTENER_SEARCH_ENDPOINT


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
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, *, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def docket_result(idx=101, *, snippet="Defendant was sentenced to 10 years."):
    return {
        "id": idx,
        "caseName": "State v. Min Jian Guan",
        "docketNumber": "CR-12345",
        "absolute_url": "/docket/12345/state-v-min-jian-guan/",
        "snippet": snippet,
        "court": "California Superior Court",
        "dateFiled": "2024-01-02",
        "dateTerminated": "2025-03-04",
        "docket_id": idx,
    }


def opinion_result(idx=202, *, case_name="People v. Example", snippet="The defendant was convicted and found guilty."):
    return {
        "id": idx,
        "caseName": case_name,
        "absolute_url": "/opinion/202/people-v-example/",
        "snippet": snippet,
        "court": "Cal.",
        "dateFiled": "2025-05-06",
        "cluster_id": idx,
    }


def test_mocked_docket_result_maps_to_source_record():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    session = FakeSession([FakeResponse({"results": [docket_result()]})])
    connector = CourtListenerConnector(session=session, search_types=("r",))

    sources = connector.search(packet.input, max_results=5, max_queries=1)
    packet.sources.extend(sources)

    assert len(sources) == 1
    source = sources[0]
    assert source.source_type == "court_docket"
    assert source.source_authority == "court"
    assert "identity_source" in source.source_roles
    assert "outcome_source" in source.source_roles
    assert "artifact_source" not in source.source_roles
    assert source.metadata["docket_number"] == "CR-12345"
    assert source.metadata["case_name"] == "State v. Min Jian Guan"
    assert source.metadata["absolute_url"].startswith("https://www.courtlistener.com/docket/")
    assert source.api_name == "courtlistener"
    assert packet.verified_artifacts == []
    assert_valid_packet(packet)


def test_mocked_opinion_result_maps_to_source_record():
    packet = route_manual_defendant_jurisdiction("Example", "Los Angeles, Los Angeles, CA")
    session = FakeSession([FakeResponse({"results": [opinion_result()]})])
    connector = CourtListenerConnector(session=session, search_types=("o",))

    sources = connector.search(packet.input, max_results=5, max_queries=1)

    assert len(sources) == 1
    source = sources[0]
    assert source.source_type == "court_opinion"
    assert source.source_authority == "court"
    assert "outcome_source" in source.source_roles
    assert source.metadata["cluster_id"] == 202
    assert source.metadata["search_type"] == "o"


def test_courtlistener_connector_hard_caps_results():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    results = [docket_result(i) for i in range(10)]
    session = FakeSession([FakeResponse({"results": results})])
    connector = CourtListenerConnector(session=session, search_types=("r",))

    sources = connector.search(packet.input, max_results=3, max_queries=1)

    assert len(sources) == 3
    assert session.calls[0]["params"]["page_size"] == 3


def test_missing_api_key_still_allows_mocked_search_without_auth_header(monkeypatch):
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    monkeypatch.delenv("COURTLISTENER_API_KEY", raising=False)
    session = FakeSession([FakeResponse({"results": []})])
    connector = CourtListenerConnector(session=session, search_types=("r",))

    sources = connector.search(packet.input, max_results=5, max_queries=1)

    assert sources == []
    assert session.calls[0]["url"] == COURTLISTENER_SEARCH_ENDPOINT
    assert session.calls[0]["headers"] == {}
    assert session.calls[0]["params"] == {
        "q": "Min Jian Guan",
        "type": "r",
        "format": "json",
        "page_size": 5,
    }

    monkeypatch.setenv("COURTLISTENER_API_KEY", "test-token")
    token_session = FakeSession([FakeResponse({"results": []})])
    token_connector = CourtListenerConnector(session=token_session, search_types=("o",))

    token_connector.search(packet.input, max_results=2, max_queries=1)

    assert token_session.calls[0]["headers"] == {"Authorization": "Token test-token"}


def test_429_or_timeout_gracefully_returns_empty_with_diagnostics():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    connector = CourtListenerConnector(session=FakeSession([FakeResponse(status_code=429)]), search_types=("r",))

    sources = connector.search(packet.input, max_results=5, max_queries=1)

    assert sources == []
    assert connector.last_status_code == 429
    assert connector.last_error == "HTTP 429"

    timeout_connector = CourtListenerConnector(session=FakeSession([TimeoutError("timed out")]), search_types=("r",))
    assert timeout_connector.search(packet.input, max_results=5, max_queries=1) == []
    assert "timed out" in timeout_connector.last_error


def test_bodycam_exhibit_language_can_be_claim_source_but_never_verified_artifact():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    session = FakeSession([
        FakeResponse({
            "results": [
                docket_result(
                    303,
                    snippet="The court referenced a bodycam exhibit during sentencing.",
                )
            ]
        })
    ])
    connector = CourtListenerConnector(session=session, search_types=("r",))

    packet.sources.extend(connector.search(packet.input, max_results=5, max_queries=1))

    assert len(packet.sources) == 1
    assert "claim_source" in packet.sources[0].source_roles
    assert "artifact_source" not in packet.sources[0].source_roles
    assert packet.verified_artifacts == []
    assert packet.verdict == "HOLD"
    assert packet.case_identity.identity_confidence == "low"
    assert_valid_packet(packet)


@pytest.mark.skipif(os.environ.get("FLAMEON_RUN_LIVE_COURTLISTENER") != "1", reason="live CourtListener smoke is opt-in")
def test_live_courtlistener_smoke_metadata_only():
    token_present = bool(os.environ.get("COURTLISTENER_API_KEY"))
    if not token_present:
        pytest.skip("COURTLISTENER_API_KEY missing; skipping live CourtListener smoke")

    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    connector = CourtListenerConnector(search_types=("r",))
    started = time.perf_counter()
    sources = connector.search(packet.input, max_results=5, max_queries=1)
    runtime = time.perf_counter() - started
    packet.sources.extend(sources)

    print(f"LIVE_COURTLISTENER_ENDPOINT={connector.last_endpoint or ''}")
    print(f"LIVE_COURTLISTENER_QUERY={connector.last_query}")
    print(f"LIVE_COURTLISTENER_TOKEN_PRESENT={token_present}")
    print(f"LIVE_COURTLISTENER_STATUS_CODE={connector.last_status_code if connector.last_status_code is not None else ''}")
    print(f"LIVE_COURTLISTENER_RESULT_COUNT={len(sources)}")
    print(f"LIVE_COURTLISTENER_RUNTIME_SECONDS={runtime:.2f}")
    print(f"LIVE_COURTLISTENER_ERROR={connector.last_error or ''}")

    assert connector.last_query == "Min Jian Guan"
    assert len(sources) <= 5
    assert packet.verified_artifacts == []
    assert packet.verdict == "HOLD"
    assert packet.case_identity.identity_confidence == "low"
