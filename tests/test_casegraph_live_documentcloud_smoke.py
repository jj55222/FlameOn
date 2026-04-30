"""LIVE3 — Capped live DocumentCloud metadata smoke.

Reuses the LIVE1 ``run_capped_live_smoke`` harness with the
DocumentCloud connector. The mocked path runs in the default no-live
suite; the real-live test is opt-in via
``FLAMEON_RUN_LIVE_CASEGRAPH=1``.

Hard caps (enforced by ``validate_live_run`` upstream):
- max_queries = 1
- max_results <= 5
- no downloads, no scraping, no PDF fetching, no OCR, no LLM
- no resolver invocation (harness alone produces zero VerifiedArtifacts)

DocumentCloud's public search endpoint may or may not respond
successfully without authentication. The real-live test logs the
status/error diagnostics regardless and asserts only the safety
invariants (caps, zero verified artifacts, zero paid-provider calls).
A non-200 response is logged but does NOT fail the test — that
preserves the experiment-level "park" decision when DocumentCloud
needs credentials.
"""
import json
import os
import time
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    DocumentCloudConnector,
    LiveRunBlocked,
    LiveRunConfig,
    LiveSmokeResult,
    parse_wapo_uof_case_input,
    run_capped_live_smoke,
)
from pipeline2_discovery.casegraph.live_safety import DEFAULT_ENV_VAR


ROOT = Path(__file__).resolve().parents[1]
STRUCTURED_FIXTURE = ROOT / "tests" / "fixtures" / "structured_inputs" / "wapo_uof_complete.json"


def load_case_input():
    with STRUCTURED_FIXTURE.open("r", encoding="utf-8") as f:
        row = json.load(f)
    return parse_wapo_uof_case_input(row).case_input


# --- Fake session ----------------------------------------------------------


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, *, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        return self.response


def mock_documentcloud_response():
    return FakeResponse(
        {
            "results": [
                {
                    "id": 7700001,
                    "title": "Phoenix PD incident report on John Example",
                    "description": "Records produced by Phoenix Police Department.",
                    "canonical_url": "https://www.documentcloud.org/documents/7700001-phoenix-pd-john-example",
                    "pdf_url": "https://s3.documentcloud.org/documents/7700001/phoenix-pd-john-example.pdf",
                    "publisher": "Arizona Republic",
                    "published_at": "2022-06-30T15:00:00Z",
                    "page_count": 14,
                    "language": "eng",
                    "access": "public",
                }
            ]
        }
    )


# --- Mocked path ----------------------------------------------------------


def test_mocked_documentcloud_smoke_produces_sourcerecords_without_network():
    case_input = load_case_input()
    session = FakeSession(mock_documentcloud_response())
    connector = DocumentCloudConnector(session=session)

    cfg = LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5)
    result = run_capped_live_smoke(
        case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
    )

    assert isinstance(result, LiveSmokeResult)
    assert result.connector == "documentcloud"
    assert result.source_count >= 1
    assert all(s.source_authority == "documentcloud" for s in result.sources)
    # Harness alone: zero verified artifacts even when canonical/pdf
    # URLs are present in metadata. Verification is the F4b resolver.
    assert result.verified_artifact_count == 0
    assert result.budget.api_calls["documentcloud"] == 1
    assert result.budget.api_calls["brave"] == 0
    assert result.budget.api_calls["firecrawl"] == 0
    assert result.budget.estimated_cost_usd == 0.0


def test_mocked_documentcloud_smoke_clamps_max_results_to_two():
    case_input = load_case_input()
    big_payload = FakeResponse(
        {
            "results": [
                {
                    "id": 7800000 + i,
                    "title": f"Document {i}",
                    "canonical_url": f"https://www.documentcloud.org/documents/7800000{i}-doc-{i}",
                    "access": "public",
                }
                for i in range(10)
            ]
        }
    )
    session = FakeSession(big_payload)
    connector = DocumentCloudConnector(session=session)

    cfg = LiveRunConfig(connector="documentcloud", max_queries=1, max_results=2)
    result = run_capped_live_smoke(
        case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
    )
    assert result.source_count <= 2
    assert result.budget.result_count == result.source_count


def test_mocked_documentcloud_smoke_records_diagnostics_in_ledger_shape():
    case_input = load_case_input()
    session = FakeSession(mock_documentcloud_response())
    connector = DocumentCloudConnector(session=session)

    cfg = LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5)
    result = run_capped_live_smoke(
        case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
    )

    diagnostics = result.to_diagnostics()
    assert diagnostics["connector"] == "documentcloud"
    assert diagnostics["query"] == "John Example"
    assert "documentcloud.org" in diagnostics["endpoint"]
    assert diagnostics["status_code"] == 200
    assert diagnostics["verified_artifact_count"] == 0
    assert diagnostics["estimated_cost_usd"] == 0.0
    assert diagnostics["api_calls"]["documentcloud"] == 1


@pytest.mark.parametrize("status_code", [401, 403, 404, 500])
def test_mocked_documentcloud_smoke_handles_failure_status_codes_gracefully(status_code):
    """If DocumentCloud requires auth or is unavailable, the harness
    must surface the diagnostics without raising. This is the path
    that supports parking the live experiment cleanly."""
    case_input = load_case_input()
    session = FakeSession(FakeResponse(payload={}, status_code=status_code))
    connector = DocumentCloudConnector(session=session)

    cfg = LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5)
    result = run_capped_live_smoke(
        case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
    )
    assert result.source_count == 0
    assert result.last_status_code == status_code
    assert result.last_error == f"HTTP {status_code}"
    # Even on failure: still within caps, still no artifacts, still no cost.
    assert result.verified_artifact_count == 0
    assert result.budget.api_calls["documentcloud"] == 1
    assert result.budget.estimated_cost_usd == 0.0


def test_documentcloud_smoke_disabled_without_env_gate():
    case_input = load_case_input()
    cfg = LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5)

    class TripwireSession:
        def get(self, *args, **kwargs):
            raise AssertionError("DocumentCloud connector should NOT be hit without env gate")

    connector = DocumentCloudConnector(session=TripwireSession())
    with pytest.raises(LiveRunBlocked, match="live run not enabled"):
        run_capped_live_smoke(case_input, config=cfg, connector=connector, env={})


def test_documentcloud_smoke_rejects_oversize_max_results():
    case_input = load_case_input()
    cfg = LiveRunConfig(connector="documentcloud", max_queries=1, max_results=99)

    class TripwireSession:
        def get(self, *args, **kwargs):
            raise AssertionError("oversize cap should reject before any HTTP call")

    connector = DocumentCloudConnector(session=TripwireSession())
    with pytest.raises(LiveRunBlocked, match="max_results"):
        run_capped_live_smoke(
            case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
        )


# --- Real-live opt-in path ------------------------------------------------


@pytest.mark.skipif(
    os.environ.get(DEFAULT_ENV_VAR) != "1",
    reason="opt-in via FLAMEON_RUN_LIVE_CASEGRAPH=1",
)
def test_real_live_documentcloud_capped_smoke():
    """Real live smoke against DocumentCloud's public search endpoint.

    Strict caps. The harness creates SourceRecords only — no
    VerifiedArtifacts, no PRODUCE side-effects. Diagnostic output is
    printed for the experiment ledger row.

    Asserts ONLY the safety invariants: caps held, zero paid-provider
    calls, zero VerifiedArtifacts. The endpoint may return non-200
    if it requires credentials — that's logged but does not fail the
    test, supporting the "park" decision when DocumentCloud is
    auth-only.
    """
    case_input = load_case_input()
    cfg = LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5)

    started = time.perf_counter()
    result = run_capped_live_smoke(case_input, config=cfg)
    runtime = time.perf_counter() - started

    diag = result.to_diagnostics()
    print(f"LIVE3_CONNECTOR={diag['connector']}")
    print(f"LIVE3_ENDPOINT={diag['endpoint']}")
    print(f"LIVE3_QUERY={diag['query']}")
    print(f"LIVE3_STATUS_CODE={diag['status_code']}")
    print(f"LIVE3_RESULT_COUNT={diag['result_count']}")
    print(f"LIVE3_VERIFIED_ARTIFACT_COUNT={diag['verified_artifact_count']}")
    print(f"LIVE3_WALLCLOCK_SECONDS={runtime:.2f}")
    print(f"LIVE3_API_CALLS={diag['api_calls']}")
    print(f"LIVE3_ESTIMATED_COST_USD={diag['estimated_cost_usd']}")
    print(f"LIVE3_ERROR={diag['error']}")

    # Caps + invariants — these MUST hold regardless of HTTP outcome.
    assert result.budget.query_count == 1
    assert result.source_count <= 5
    assert result.verified_artifact_count == 0
    assert result.budget.api_calls["documentcloud"] == 1
    assert result.budget.api_calls["brave"] == 0
    assert result.budget.api_calls["firecrawl"] == 0
    assert result.budget.api_calls["llm"] == 0
    assert result.budget.estimated_cost_usd == 0.0
