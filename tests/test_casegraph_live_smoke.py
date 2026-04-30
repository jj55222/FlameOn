"""LIVE1 — Capped live query-planner smoke harness tests.

The default test path is fully mocked: a `FixtureSession`-backed
connector replaces the real HTTP client so the harness logic is
exercised without touching the network. The real-live test is
opt-in via ``FLAMEON_RUN_LIVE_CASEGRAPH=1`` and is skipped by default.

Asserts:
- the harness rejects unsafe configs before any connector call
- a mocked CourtListener smoke produces SourceRecords without network
- per-batch hard caps (max_queries=1, max_results<=5) are honored
- a mocked smoke creates ZERO VerifiedArtifacts
- diagnostics dict is ledger-shaped
- the harness never makes a live HTTP call when the env gate is unset
"""
import json
import os
import time
from pathlib import Path
from typing import List, Optional

import pytest

from pipeline2_discovery.casegraph import (
    CourtListenerConnector,
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


def courtlistener_response_fixture(query: str):
    return FakeResponse(
        {
            "results": [
                {
                    "id": 9100001,
                    "caseName": "State v. John Example",
                    "absolute_url": "/opinion/9100001/state-v-john-example/",
                    "snippet": (
                        "John Example was sentenced after the Phoenix Police Department "
                        "investigation. The defendant pleaded guilty."
                    ),
                    "docketNumber": "CR-2022-001234",
                    "court": "AZSP",
                    "dateFiled": "2022-09-15",
                }
                for _ in range(2)
            ]
        }
    )


# --- Safety-gate behavior --------------------------------------------------


def test_smoke_disabled_by_default_does_not_call_connector():
    """Without the env gate, the harness raises BEFORE any connector
    call. We confirm by injecting a connector that would fail loudly
    if invoked."""
    case_input = load_case_input()
    cfg = LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5)

    class TripwireConnector:
        name = "tripwire"

        def search(self, *args, **kwargs):
            raise AssertionError("connector.search should NOT have been called")

        def fetch(self, *args, **kwargs):
            raise AssertionError("connector.fetch should NOT have been called")

    with pytest.raises(LiveRunBlocked, match="live run not enabled"):
        run_capped_live_smoke(case_input, config=cfg, connector=TripwireConnector(), env={})


def test_smoke_rejects_oversize_max_results_before_connector_call():
    case_input = load_case_input()
    cfg = LiveRunConfig(connector="courtlistener", max_queries=1, max_results=99)

    class TripwireConnector:
        name = "tripwire"

        def search(self, *args, **kwargs):
            raise AssertionError("should not be invoked under oversize cap")

        def fetch(self, *args, **kwargs):
            raise AssertionError("should not be invoked under oversize cap")

    with pytest.raises(LiveRunBlocked, match="max_results"):
        run_capped_live_smoke(
            case_input, config=cfg, connector=TripwireConnector(), env={DEFAULT_ENV_VAR: "1"}
        )


def test_smoke_rejects_brave_without_explicit_allow():
    case_input = load_case_input()
    cfg = LiveRunConfig(connector="brave", max_queries=1, max_results=5)
    with pytest.raises(LiveRunBlocked, match="brave"):
        run_capped_live_smoke(case_input, config=cfg, env={DEFAULT_ENV_VAR: "1"})


def test_smoke_rejects_when_downloads_toggled_on():
    case_input = load_case_input()
    cfg = LiveRunConfig(
        connector="courtlistener", max_queries=1, max_results=5, allow_downloads=True
    )
    with pytest.raises(LiveRunBlocked, match="downloads"):
        run_capped_live_smoke(case_input, config=cfg, env={DEFAULT_ENV_VAR: "1"})


# --- Mocked connector path -------------------------------------------------


def test_mocked_courtlistener_smoke_produces_sourcerecords_without_network():
    case_input = load_case_input()
    session = FakeSession([courtlistener_response_fixture("John Example")])
    connector = CourtListenerConnector(session=session)

    cfg = LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5)
    result = run_capped_live_smoke(
        case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
    )

    assert isinstance(result, LiveSmokeResult)
    assert result.connector == "courtlistener"
    assert result.source_count >= 1
    assert all(s.source_authority == "court" for s in result.sources)
    # The harness alone must NOT graduate sources into VerifiedArtifacts.
    assert result.verified_artifact_count == 0
    # Budget records exactly one query.
    assert result.budget.query_count == 1
    assert result.budget.api_calls["courtlistener"] == 1
    assert result.budget.estimated_cost_usd == 0.0


def test_mocked_smoke_clamps_max_results_below_returned_payload():
    case_input = load_case_input()
    # Build a fixture session that returns 5 results — the connector
    # itself will clamp to max_results.
    big_results = {
        "results": [
            {
                "id": 9200000 + i,
                "caseName": f"Case {i}",
                "absolute_url": f"/opinion/9200000{i}/case-{i}/",
                "snippet": "Court records.",
                "docketNumber": f"CR-{i}",
                "court": "AZSP",
                "dateFiled": "2022-01-01",
            }
            for i in range(5)
        ]
    }
    session = FakeSession([FakeResponse(big_results), FakeResponse(big_results)])
    connector = CourtListenerConnector(session=session)

    cfg = LiveRunConfig(connector="courtlistener", max_queries=1, max_results=2)
    result = run_capped_live_smoke(
        case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
    )
    # The CourtListener connector iterates over both search types ("r"
    # and "o") and clamps the *combined* output at max_results.
    assert result.source_count <= 2
    assert result.budget.result_count == result.source_count


def test_mocked_smoke_records_diagnostics_in_ledger_shape():
    case_input = load_case_input()
    session = FakeSession([courtlistener_response_fixture("John Example")])
    connector = CourtListenerConnector(session=session)

    cfg = LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5)
    result = run_capped_live_smoke(
        case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
    )

    diagnostics = result.to_diagnostics()
    for key in (
        "connector",
        "query",
        "endpoint",
        "status_code",
        "error",
        "result_count",
        "verified_artifact_count",
        "wallclock_seconds",
        "api_calls",
        "estimated_cost_usd",
        "notes",
    ):
        assert key in diagnostics, f"missing diagnostics key {key!r}"
    assert diagnostics["connector"] == "courtlistener"
    assert diagnostics["query"] == "John Example"
    assert diagnostics["endpoint"] == "https://www.courtlistener.com/api/rest/v4/search/"
    assert diagnostics["verified_artifact_count"] == 0
    assert diagnostics["estimated_cost_usd"] == 0.0


def test_mocked_smoke_handles_404_gracefully_without_raising():
    case_input = load_case_input()
    session = FakeSession([FakeResponse(payload={}, status_code=404)])
    connector = CourtListenerConnector(session=session)

    cfg = LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5)
    result = run_capped_live_smoke(
        case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
    )
    assert result.source_count == 0
    assert result.last_status_code == 404
    assert result.last_error == "HTTP 404"


def test_mocked_muckrock_smoke_path():
    """Same harness, swapped connector — confirms the harness is
    connector-agnostic given an explicit instance."""
    case_input = load_case_input()
    payload = {
        "results": [
            {
                "id": 9300001,
                "title": "FOIA bodycam request for John Example",
                "description": "Public records request for body-worn camera footage.",
                "status": "done",
                "absolute_url": "https://www.muckrock.com/foi/example/9300001/",
                "agency": {"name": "Phoenix Police Department"},
                "date_submitted": "2022-06-01",
                "date_done": "2022-07-15",
                "jurisdiction": "Arizona",
            }
        ]
    }
    session = FakeSession([FakeResponse(payload)])
    connector = MuckRockConnector(session=session)

    cfg = LiveRunConfig(connector="muckrock", max_queries=1, max_results=5)
    result = run_capped_live_smoke(
        case_input, config=cfg, connector=connector, env={DEFAULT_ENV_VAR: "1"}
    )
    assert result.connector == "muckrock"
    assert result.source_count == 1
    assert result.sources[0].source_authority == "foia"
    assert result.verified_artifact_count == 0
    assert result.budget.api_calls["muckrock"] == 1


# --- Network invariant -----------------------------------------------------


def test_safety_check_blocks_before_real_session_get_when_env_unset(monkeypatch):
    """The harness must call validate_live_run BEFORE any connector
    HTTP call. If the env gate is unset, monkey-patched
    requests.Session.get must record zero calls."""
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    case_input = load_case_input()
    cfg = LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5)
    with pytest.raises(LiveRunBlocked):
        run_capped_live_smoke(case_input, config=cfg, env={})  # env gate unset

    assert calls == [], f"safety check did not block — {len(calls)} live HTTP call(s)"


# --- Opt-in real live test -------------------------------------------------


@pytest.mark.skipif(
    os.environ.get(DEFAULT_ENV_VAR) != "1",
    reason="opt-in via FLAMEON_RUN_LIVE_CASEGRAPH=1",
)
def test_real_live_courtlistener_capped_smoke():
    """Real live smoke against CourtListener.

    Strict caps (max_queries=1, max_results<=5). The harness creates
    SourceRecords only — no VerifiedArtifacts, no PRODUCE side-effects.

    Diagnostic output is printed for the experiment ledger row.
    """
    case_input = load_case_input()
    cfg = LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5)

    started = time.perf_counter()
    result = run_capped_live_smoke(case_input, config=cfg)
    runtime = time.perf_counter() - started

    diag = result.to_diagnostics()
    print(f"LIVE1_CONNECTOR={diag['connector']}")
    print(f"LIVE1_ENDPOINT={diag['endpoint']}")
    print(f"LIVE1_QUERY={diag['query']}")
    print(f"LIVE1_STATUS_CODE={diag['status_code']}")
    print(f"LIVE1_RESULT_COUNT={diag['result_count']}")
    print(f"LIVE1_VERIFIED_ARTIFACT_COUNT={diag['verified_artifact_count']}")
    print(f"LIVE1_WALLCLOCK_SECONDS={runtime:.2f}")
    print(f"LIVE1_API_CALLS={diag['api_calls']}")
    print(f"LIVE1_ESTIMATED_COST_USD={diag['estimated_cost_usd']}")
    print(f"LIVE1_ERROR={diag['error']}")

    assert result.budget.query_count == 1
    assert result.source_count <= 5
    assert result.verified_artifact_count == 0
    assert result.budget.api_calls["brave"] == 0
    assert result.budget.api_calls["firecrawl"] == 0
    assert result.budget.api_calls["llm"] == 0
    assert result.budget.estimated_cost_usd == 0.0


@pytest.mark.skipif(
    os.environ.get(DEFAULT_ENV_VAR) != "1",
    reason="opt-in via FLAMEON_RUN_LIVE_CASEGRAPH=1",
)
def test_real_live_smoke_does_not_create_verified_artifacts():
    """Even with real CourtListener results, the smoke harness alone
    does not turn court documents into VerifiedArtifacts. The
    F3b CourtListener resolver must run separately to do that."""
    case_input = load_case_input()
    cfg = LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5)
    result = run_capped_live_smoke(case_input, config=cfg)
    assert result.verified_artifact_count == 0
