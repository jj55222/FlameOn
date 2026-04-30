"""LIVE4 — Capped multi-connector live metadata smoke.

Sequentially runs ``run_capped_live_smoke`` against at most
``MAX_CONNECTORS_HARD_CAP`` connectors (currently 2). Each per-connector
config goes through ``validate_live_run`` BEFORE the connector is
touched. Aggregated diagnostics are returned via
``MultiConnectorSmokeResult``.

Hard caps:
- max_connectors = 2 (MAX_CONNECTORS_HARD_CAP)
- max_queries per connector = 1
- max_results per connector <= 5
- total live calls <= 2
- no downloads, no scraping, no transcript fetching, no LLM
- no Brave / Firecrawl
- no resolver invocation (verified_artifact_count stays 0)

The mocked path runs in the default no-live suite; the real-live test
is opt-in via ``FLAMEON_RUN_LIVE_CASEGRAPH=1``. The default real-live
pair is CourtListener + MuckRock (DocumentCloud is also acceptable).
"""
import json
import os
import time
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    CourtListenerConnector,
    DocumentCloudConnector,
    LiveRunBlocked,
    LiveRunConfig,
    MAX_CONNECTORS_HARD_CAP,
    MultiConnectorSmokeResult,
    MuckRockConnector,
    parse_wapo_uof_case_input,
    run_capped_multi_connector_smoke,
)
from pipeline2_discovery.casegraph.live_safety import DEFAULT_ENV_VAR


ROOT = Path(__file__).resolve().parents[1]
STRUCTURED_FIXTURE = ROOT / "tests" / "fixtures" / "structured_inputs" / "wapo_uof_complete.json"


def load_case_input():
    with STRUCTURED_FIXTURE.open("r", encoding="utf-8") as f:
        row = json.load(f)
    return parse_wapo_uof_case_input(row).case_input


# --- Fake session/connector plumbing for the mocked path -------------------


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, *, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        if self._responses:
            return self._responses.pop(0)
        return FakeResponse({"results": []})


def courtlistener_response():
    return FakeResponse(
        {
            "results": [
                {
                    "id": 5511001,
                    "caseName": "State v. John Example",
                    "absolute_url": "/opinion/5511001/state-v-john-example/",
                    "snippet": "John Example was sentenced after the Phoenix Police Department investigation.",
                    "docketNumber": "CR-2022-001234",
                    "court": "AZSP",
                    "dateFiled": "2022-09-15",
                }
            ]
        }
    )


def muckrock_response():
    return FakeResponse(
        {
            "results": [
                {
                    "id": 6611001,
                    "title": "Public records request for John Example bodycam",
                    "description": "FOIA request seeking body-worn camera footage from Phoenix PD.",
                    "status": "done",
                    "absolute_url": "https://www.muckrock.com/foi/example/6611001/",
                    "agency": {"name": "Phoenix Police Department"},
                    "date_submitted": "2022-06-01",
                    "date_done": "2022-07-15",
                    "jurisdiction": "Arizona",
                }
            ]
        }
    )


# --- Mocked path ----------------------------------------------------------


def test_mocked_multi_connector_smoke_runs_courtlistener_and_muckrock():
    case_input = load_case_input()
    cl_session = FakeSession([courtlistener_response()])
    mr_session = FakeSession([muckrock_response()])
    cl_connector = CourtListenerConnector(session=cl_session)
    mr_connector = MuckRockConnector(session=mr_session)

    configs = [
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5),
        LiveRunConfig(connector="muckrock", max_queries=1, max_results=5),
    ]
    result = run_capped_multi_connector_smoke(
        case_input,
        configs=configs,
        connectors=[cl_connector, mr_connector],
        env={DEFAULT_ENV_VAR: "1"},
    )

    assert isinstance(result, MultiConnectorSmokeResult)
    assert len(result.per_connector) == 2
    assert {r.connector for r in result.per_connector} == {"courtlistener", "muckrock"}
    assert result.total_live_calls == 2
    assert result.total_source_records >= 1
    assert result.total_verified_artifacts == 0
    assert result.api_calls["courtlistener"] == 1
    assert result.api_calls["muckrock"] == 1
    assert result.api_calls["brave"] == 0
    assert result.api_calls["firecrawl"] == 0
    assert result.api_calls["llm"] == 0
    assert result.total_estimated_cost_usd == 0.0


def test_mocked_multi_connector_diagnostics_carry_per_connector_detail():
    case_input = load_case_input()
    cl_session = FakeSession([courtlistener_response()])
    mr_session = FakeSession([muckrock_response()])
    configs = [
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5),
        LiveRunConfig(connector="muckrock", max_queries=1, max_results=5),
    ]
    result = run_capped_multi_connector_smoke(
        case_input,
        configs=configs,
        connectors=[
            CourtListenerConnector(session=cl_session),
            MuckRockConnector(session=mr_session),
        ],
        env={DEFAULT_ENV_VAR: "1"},
    )
    diag = result.to_diagnostics()
    assert diag["connectors"] == ["courtlistener", "muckrock"]
    assert diag["total_live_calls"] == 2
    assert diag["total_verified_artifacts"] == 0
    assert len(diag["per_connector"]) == 2
    assert diag["per_connector"][0]["connector"] == "courtlistener"
    assert diag["per_connector"][1]["connector"] == "muckrock"
    assert diag["api_calls"]["courtlistener"] == 1
    assert diag["api_calls"]["muckrock"] == 1


def test_multi_connector_smoke_rejects_more_than_max_connectors():
    case_input = load_case_input()
    configs = [
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5),
        LiveRunConfig(connector="muckrock", max_queries=1, max_results=5),
        LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5),
    ]
    with pytest.raises(LiveRunBlocked, match="capped at"):
        run_capped_multi_connector_smoke(
            case_input, configs=configs, env={DEFAULT_ENV_VAR: "1"}
        )


def test_multi_connector_smoke_rejects_duplicate_connector():
    case_input = load_case_input()
    configs = [
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5),
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5),
    ]
    with pytest.raises(LiveRunBlocked, match="duplicate connector"):
        run_capped_multi_connector_smoke(
            case_input, configs=configs, env={DEFAULT_ENV_VAR: "1"}
        )


def test_multi_connector_smoke_rejects_empty_config_list():
    case_input = load_case_input()
    with pytest.raises(LiveRunBlocked, match="at least one"):
        run_capped_multi_connector_smoke(
            case_input, configs=[], env={DEFAULT_ENV_VAR: "1"}
        )


def test_multi_connector_smoke_rejects_oversize_max_results_per_config():
    case_input = load_case_input()
    configs = [
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=99),
        LiveRunConfig(connector="muckrock", max_queries=1, max_results=5),
    ]
    with pytest.raises(LiveRunBlocked, match="max_results"):
        run_capped_multi_connector_smoke(
            case_input, configs=configs, env={DEFAULT_ENV_VAR: "1"}
        )


def test_multi_connector_smoke_rejects_brave_in_any_slot():
    case_input = load_case_input()
    configs = [
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5),
        LiveRunConfig(connector="brave", max_queries=1, max_results=5),
    ]
    with pytest.raises(LiveRunBlocked, match="brave"):
        run_capped_multi_connector_smoke(
            case_input, configs=configs, env={DEFAULT_ENV_VAR: "1"}
        )


def test_multi_connector_smoke_rejects_firecrawl_in_any_slot():
    case_input = load_case_input()
    configs = [
        LiveRunConfig(connector="firecrawl", max_queries=1, max_results=5),
        LiveRunConfig(connector="muckrock", max_queries=1, max_results=5),
    ]
    with pytest.raises(LiveRunBlocked, match="firecrawl"):
        run_capped_multi_connector_smoke(
            case_input, configs=configs, env={DEFAULT_ENV_VAR: "1"}
        )


def test_multi_connector_smoke_disabled_without_env_gate():
    case_input = load_case_input()
    configs = [
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5),
        LiveRunConfig(connector="muckrock", max_queries=1, max_results=5),
    ]

    class TripwireSession:
        def get(self, *args, **kwargs):
            raise AssertionError("connector should NOT be hit without env gate")

    with pytest.raises(LiveRunBlocked, match="live run not enabled"):
        run_capped_multi_connector_smoke(
            case_input,
            configs=configs,
            connectors=[
                CourtListenerConnector(session=TripwireSession()),
                MuckRockConnector(session=TripwireSession()),
            ],
            env={},
        )


def test_max_connectors_hard_cap_value_stable():
    """The hard cap is part of the contract — pinning it at 2 here so a
    later widening shows up as a deliberate change."""
    assert MAX_CONNECTORS_HARD_CAP == 2


def test_multi_connector_smoke_does_not_create_verified_artifacts_even_with_release_text():
    """When mocked sources contain release language, the harness must
    still leave verified_artifact_count at zero — verification belongs
    to the resolvers, not the smoke."""
    case_input = load_case_input()
    cl_payload = {
        "results": [
            {
                "id": 6500001,
                "caseName": "State v. John Example",
                "absolute_url": "/opinion/6500001/state-v-john-example/",
                "snippet": "Bodycam footage was released. Records were produced.",
                "docketNumber": "CR-9999",
                "court": "AZSP",
                "dateFiled": "2024-01-01",
            }
        ]
    }
    mr_payload = {
        "results": [
            {
                "id": 6500002,
                "title": "Released bodycam records",
                "description": "Records produced included body-worn camera footage.",
                "status": "done",
                "absolute_url": "https://www.muckrock.com/foi/example/6500002/",
                "agency": {"name": "Phoenix Police Department"},
                "date_submitted": "2024-02-01",
                "date_done": "2024-03-01",
                "jurisdiction": "Arizona",
            }
        ]
    }
    cl_connector = CourtListenerConnector(session=FakeSession([FakeResponse(cl_payload)]))
    mr_connector = MuckRockConnector(session=FakeSession([FakeResponse(mr_payload)]))

    configs = [
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5),
        LiveRunConfig(connector="muckrock", max_queries=1, max_results=5),
    ]
    result = run_capped_multi_connector_smoke(
        case_input,
        configs=configs,
        connectors=[cl_connector, mr_connector],
        env={DEFAULT_ENV_VAR: "1"},
    )
    assert result.total_verified_artifacts == 0
    for r in result.per_connector:
        assert r.verified_artifact_count == 0


# --- Real-live opt-in path ------------------------------------------------


@pytest.mark.skipif(
    os.environ.get(DEFAULT_ENV_VAR) != "1",
    reason="opt-in via FLAMEON_RUN_LIVE_CASEGRAPH=1",
)
def test_real_live_multi_connector_capped_smoke():
    """Real live multi-connector smoke against CourtListener + MuckRock.

    Strict caps. Diagnostic output is printed for the experiment
    ledger row.
    """
    case_input = load_case_input()
    configs = [
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5),
        LiveRunConfig(connector="muckrock", max_queries=1, max_results=5),
    ]

    started = time.perf_counter()
    result = run_capped_multi_connector_smoke(case_input, configs=configs)
    runtime = time.perf_counter() - started

    diag = result.to_diagnostics()
    print(f"LIVE4_CONNECTORS={diag['connectors']}")
    print(f"LIVE4_TOTAL_LIVE_CALLS={diag['total_live_calls']}")
    print(f"LIVE4_TOTAL_SOURCE_RECORDS={diag['total_source_records']}")
    print(f"LIVE4_TOTAL_VERIFIED_ARTIFACTS={diag['total_verified_artifacts']}")
    print(f"LIVE4_TOTAL_WALLCLOCK_SECONDS={runtime:.2f}")
    print(f"LIVE4_API_CALLS={diag['api_calls']}")
    print(f"LIVE4_TOTAL_ESTIMATED_COST_USD={diag['total_estimated_cost_usd']}")
    print(f"LIVE4_ERRORS={diag['errors']}")
    for entry in diag["per_connector"]:
        print(
            f"LIVE4_PER_CONNECTOR connector={entry['connector']} "
            f"endpoint={entry['endpoint']} "
            f"status={entry['status_code']} "
            f"results={entry['result_count']} "
            f"wallclock={entry['wallclock_seconds']}"
        )

    # Caps + invariants — these MUST hold regardless of HTTP outcome.
    assert result.total_live_calls <= MAX_CONNECTORS_HARD_CAP
    assert len(result.per_connector) == 2
    assert result.total_verified_artifacts == 0
    assert result.api_calls["brave"] == 0
    assert result.api_calls["firecrawl"] == 0
    assert result.api_calls["llm"] == 0
    assert result.total_estimated_cost_usd == 0.0
    for r in result.per_connector:
        assert r.budget.query_count == 1
        assert r.source_count <= 5
