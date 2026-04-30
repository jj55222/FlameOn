"""LIVE2 — Capped live MuckRock metadata smoke.

Reuses the LIVE1 ``run_capped_live_smoke`` harness with the MuckRock
connector. The mocked path runs in the default no-live suite; the
real-live test is opt-in via ``FLAMEON_RUN_LIVE_CASEGRAPH=1``.

Hard caps (enforced by ``validate_live_run`` upstream):
- max_queries = 1
- max_results <= 5
- no downloads, no scraping, no LLM, no transcript fetching
- no resolver invocation (harness alone produces zero VerifiedArtifacts)

The smoke uses the WaPo UoF complete fixture as its CaseInput so the
query is name-driven and deterministic.
"""
import json
import os
import time
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    LiveRunBlocked,
    LiveRunConfig,
    LiveSmokeResult,
    MuckRockConnector,
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


# --- Fake session for the mocked path -------------------------------------


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


def mock_muckrock_response():
    return FakeResponse(
        {
            "results": [
                {
                    "id": 5500001,
                    "title": "Public records request for John Example bodycam",
                    "description": (
                        "FOIA request seeking body-worn camera footage from the "
                        "Phoenix Police Department regarding John Example."
                    ),
                    "status": "done",
                    "absolute_url": "https://www.muckrock.com/foi/example/5500001/",
                    "agency": {"name": "Phoenix Police Department"},
                    "date_submitted": "2022-06-01",
                    "date_done": "2022-07-15",
                    "jurisdiction": "Arizona",
                },
                {
                    "id": 5500002,
                    "title": "Use of force report request",
                    "description": (
                        "FOIA request for use-of-force report and incident "
                        "narrative for John Example case in Phoenix."
                    ),
                    "status": "done",
                    "absolute_url": "https://www.muckrock.com/foi/example/5500002/",
                    "agency": {"name": "Phoenix Police Department"},
                    "date_submitted": "2022-06-15",
                    "date_done": "2022-08-01",
                    "jurisdiction": "Arizona",
                },
            ]
        }
    )


# --- Mocked path ----------------------------------------------------------


def test_mocked_muckrock_smoke_produces_sourcerecords_without_network():
    case_input = load_case_input()
    session = FakeSession(mock_muckrock_response())
    connector = MuckRockConnector(session=session)

    cfg = LiveRunConfig(connector="muckrock", max_queries=1, max_results=5)
    result = run_capped_live_smoke(
        case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
    )

    assert isinstance(result, LiveSmokeResult)
    assert result.connector == "muckrock"
    assert result.source_count >= 1
    assert all(s.source_authority == "foia" for s in result.sources)
    assert all(s.api_name == "muckrock" for s in result.sources)
    # The harness does NOT invoke any resolver — verified artifact count
    # stays zero even when sources include claim language.
    assert result.verified_artifact_count == 0
    assert result.budget.query_count == 1
    assert result.budget.api_calls["muckrock"] == 1
    assert result.budget.api_calls["brave"] == 0
    assert result.budget.api_calls["firecrawl"] == 0
    assert result.budget.estimated_cost_usd == 0.0


def test_mocked_muckrock_smoke_clamps_max_results_to_two():
    case_input = load_case_input()
    big_payload = FakeResponse(
        {
            "results": [
                {
                    "id": 5600000 + i,
                    "title": f"Request {i}",
                    "description": "Public records request.",
                    "status": "done",
                    "absolute_url": f"https://www.muckrock.com/foi/example/5600000{i}/",
                    "agency": {"name": "Phoenix Police Department"},
                    "date_submitted": "2022-06-01",
                    "date_done": "2022-07-15",
                    "jurisdiction": "Arizona",
                }
                for i in range(8)
            ]
        }
    )
    session = FakeSession(big_payload)
    connector = MuckRockConnector(session=session)

    cfg = LiveRunConfig(connector="muckrock", max_queries=1, max_results=2)
    result = run_capped_live_smoke(
        case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
    )
    assert result.source_count <= 2
    assert result.budget.result_count == result.source_count


def test_mocked_muckrock_smoke_records_diagnostics_in_ledger_shape():
    case_input = load_case_input()
    session = FakeSession(mock_muckrock_response())
    connector = MuckRockConnector(session=session)

    cfg = LiveRunConfig(connector="muckrock", max_queries=1, max_results=5)
    result = run_capped_live_smoke(
        case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
    )

    diagnostics = result.to_diagnostics()
    assert diagnostics["connector"] == "muckrock"
    assert diagnostics["query"] == "John Example"
    assert "muckrock.com" in diagnostics["endpoint"]
    assert diagnostics["status_code"] == 200
    assert diagnostics["verified_artifact_count"] == 0
    assert diagnostics["estimated_cost_usd"] == 0.0
    assert diagnostics["api_calls"]["muckrock"] == 1


def test_mocked_muckrock_smoke_handles_500_gracefully():
    case_input = load_case_input()
    session = FakeSession(FakeResponse(payload={}, status_code=500))
    connector = MuckRockConnector(session=session)

    cfg = LiveRunConfig(connector="muckrock", max_queries=1, max_results=5)
    result = run_capped_live_smoke(
        case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
    )
    assert result.source_count == 0
    assert result.last_status_code == 500
    assert result.last_error == "HTTP 500"


def test_muckrock_smoke_disabled_without_env_gate():
    case_input = load_case_input()
    cfg = LiveRunConfig(connector="muckrock", max_queries=1, max_results=5)

    class TripwireSession:
        def get(self, *args, **kwargs):
            raise AssertionError("MuckRock connector should NOT be hit without env gate")

    connector = MuckRockConnector(session=TripwireSession())
    with pytest.raises(LiveRunBlocked, match="live run not enabled"):
        run_capped_live_smoke(case_input, config=cfg, connector=connector, env={})


def test_muckrock_smoke_rejects_oversize_max_results_before_connector_call():
    case_input = load_case_input()
    cfg = LiveRunConfig(connector="muckrock", max_queries=1, max_results=99)

    class TripwireSession:
        def get(self, *args, **kwargs):
            raise AssertionError("oversize cap should reject before any HTTP call")

    connector = MuckRockConnector(session=TripwireSession())
    with pytest.raises(LiveRunBlocked, match="max_results"):
        run_capped_live_smoke(
            case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
        )


# --- Real-live opt-in path ------------------------------------------------


@pytest.mark.skipif(
    os.environ.get(DEFAULT_ENV_VAR) != "1",
    reason="opt-in via FLAMEON_RUN_LIVE_CASEGRAPH=1",
)
def test_real_live_muckrock_capped_smoke():
    """Real live smoke against MuckRock's public requests endpoint.

    Strict caps (max_queries=1, max_results<=5). The harness creates
    SourceRecords only — no VerifiedArtifacts, no PRODUCE side effects.
    Diagnostic output is printed for the experiment ledger row.
    """
    case_input = load_case_input()
    cfg = LiveRunConfig(connector="muckrock", max_queries=1, max_results=5)

    started = time.perf_counter()
    result = run_capped_live_smoke(case_input, config=cfg)
    runtime = time.perf_counter() - started

    diag = result.to_diagnostics()
    print(f"LIVE2_CONNECTOR={diag['connector']}")
    print(f"LIVE2_ENDPOINT={diag['endpoint']}")
    print(f"LIVE2_QUERY={diag['query']}")
    print(f"LIVE2_STATUS_CODE={diag['status_code']}")
    print(f"LIVE2_RESULT_COUNT={diag['result_count']}")
    print(f"LIVE2_VERIFIED_ARTIFACT_COUNT={diag['verified_artifact_count']}")
    print(f"LIVE2_WALLCLOCK_SECONDS={runtime:.2f}")
    print(f"LIVE2_API_CALLS={diag['api_calls']}")
    print(f"LIVE2_ESTIMATED_COST_USD={diag['estimated_cost_usd']}")
    print(f"LIVE2_ERROR={diag['error']}")

    assert result.budget.query_count == 1
    assert result.source_count <= 5
    assert result.verified_artifact_count == 0
    assert result.budget.api_calls["muckrock"] == 1
    assert result.budget.api_calls["brave"] == 0
    assert result.budget.api_calls["firecrawl"] == 0
    assert result.budget.api_calls["llm"] == 0
    assert result.budget.estimated_cost_usd == 0.0


@pytest.mark.skipif(
    os.environ.get(DEFAULT_ENV_VAR) != "1",
    reason="opt-in via FLAMEON_RUN_LIVE_CASEGRAPH=1",
)
def test_real_live_muckrock_smoke_does_not_create_verified_artifacts():
    """Even when MuckRock returns FOIA records that mention released
    artifacts, the smoke harness must not graduate them. Verification
    is the F2b resolver's job, run separately when requested."""
    case_input = load_case_input()
    cfg = LiveRunConfig(connector="muckrock", max_queries=1, max_results=5)
    result = run_capped_live_smoke(case_input, config=cfg)
    assert result.verified_artifact_count == 0
