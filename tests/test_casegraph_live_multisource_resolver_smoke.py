"""LIVE5 — Capped multi-source live smoke + resolver orchestration.

Combines LIVE4's ``run_capped_multi_connector_smoke`` with RESOLVE1's
``run_metadata_only_resolvers``: run two capped live connector calls,
then run the metadata-only resolver orchestrator against the
returned SourceRecords. The orchestrator inspects only metadata
URLs already present on the SourceRecords — never downloads, never
scrapes.

Hard caps (enforced by ``validate_live_run`` upstream and
``MAX_CONNECTORS_HARD_CAP``):
- max_connectors = 2
- max_queries_per_connector = 1
- max_results_per_connector <= 5
- total_live_calls <= 2
- no downloads, no scraping, no transcript fetching, no LLM
- no Brave / Firecrawl
- the resolver orchestrator follows protected/private/PACER URLs:
  it rejects them via the underlying resolvers' built-in checks

The mocked path runs in the default no-live suite; the real-live
test is opt-in via ``FLAMEON_RUN_LIVE_CASEGRAPH=1``.
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
    run_metadata_only_resolvers,
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


def courtlistener_response_with_opinion():
    return FakeResponse(
        {
            "results": [
                {
                    "id": 6611001,
                    "caseName": "State v. John Example",
                    "absolute_url": "/opinion/6611001/state-v-john-example/",
                    "snippet": (
                        "John Example was sentenced after the Phoenix Police "
                        "Department investigation. The defendant pleaded guilty."
                    ),
                    "docketNumber": "CR-2022-001234",
                    "court": "AZSP",
                    "dateFiled": "2022-09-15",
                }
            ]
        }
    )


def muckrock_response_with_release_text_no_url():
    """MuckRock returns a request that *talks* about released bodycam
    but carries no `released_files` URL — the orchestrator must NOT
    graduate this into a VerifiedArtifact."""
    return FakeResponse(
        {
            "results": [
                {
                    "id": 6611002,
                    "title": "Bodycam release request",
                    "description": (
                        "The Phoenix Police Department released bodycam footage "
                        "from the John Example incident."
                    ),
                    "status": "done",
                    "absolute_url": "https://www.muckrock.com/foi/example/6611002/",
                    "agency": {"name": "Phoenix Police Department"},
                    "date_submitted": "2022-06-01",
                    "date_done": "2022-07-15",
                    "jurisdiction": "Arizona",
                    # Note: NO `released_files` array — release language only.
                }
            ]
        }
    )


def documentcloud_response_with_public_pdf_url():
    """DocumentCloud's connector preserves canonical_url and pdf_url
    on SourceRecord metadata, so the resolver can graduate them. This
    is the LIVE5 spec's preferred CourtListener+DocumentCloud pair
    when the goal is to surface a document VerifiedArtifact via the
    orchestrator."""
    return FakeResponse(
        {
            "results": [
                {
                    "id": 6611003,
                    "title": "Phoenix incident report on John Example",
                    "description": "Records produced include the use of force report.",
                    "canonical_url": "https://www.documentcloud.org/documents/6611003-phoenix-incident-report",
                    "pdf_url": "https://s3.documentcloud.org/documents/6611003/phoenix-incident-report.pdf",
                    "publisher": "Arizona Republic",
                    "published_at": "2022-06-30T15:00:00Z",
                    "page_count": 14,
                    "language": "eng",
                    "access": "public",
                }
            ]
        }
    )


def _run_mocked_smoke_and_orchestrate(
    case_input,
    cl_response,
    mr_response,
    *,
    cl_max_results=5,
    mr_max_results=5,
):
    """Helper: build FakeSession-backed CourtListener + MuckRock
    connectors, run a capped multi-connector smoke, then run the
    resolver orchestrator against the assembled SourceRecords."""
    cl_session = FakeSession([cl_response])
    mr_session = FakeSession([mr_response])
    configs = [
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=cl_max_results),
        LiveRunConfig(connector="muckrock", max_queries=1, max_results=mr_max_results),
    ]
    smoke = run_capped_multi_connector_smoke(
        case_input,
        configs=configs,
        connectors=[
            CourtListenerConnector(session=cl_session),
            MuckRockConnector(session=mr_session),
        ],
        env={DEFAULT_ENV_VAR: "1"},
    )
    sources = []
    for r in smoke.per_connector:
        sources.extend(r.sources)
    orch = run_metadata_only_resolvers(sources)
    return smoke, sources, orch


# --- Mocked path ----------------------------------------------------------


def test_mocked_smoke_plus_orchestrator_with_opinion_yields_court_doc_artifact():
    case_input = load_case_input()
    smoke, sources, orch = _run_mocked_smoke_and_orchestrate(
        case_input,
        courtlistener_response_with_opinion(),
        muckrock_response_with_release_text_no_url(),
    )
    assert isinstance(smoke, MultiConnectorSmokeResult)
    assert smoke.total_live_calls == 2
    assert smoke.total_verified_artifacts == 0  # smoke alone never graduates
    assert smoke.api_calls["courtlistener"] == 1
    assert smoke.api_calls["muckrock"] == 1
    assert smoke.api_calls["brave"] == 0
    assert smoke.api_calls["firecrawl"] == 0
    assert smoke.api_calls["llm"] == 0

    # Orchestrator: courtlistener opinion URL graduates into a docket_docs
    # artifact. The MuckRock release-language-only source does NOT.
    assert orch.verified_artifact_count == 1
    artifact = orch.verified_artifacts[0]
    assert artifact.source_authority == "court"
    assert artifact.artifact_type == "docket_docs"
    assert artifact.format == "document"
    # Cross-cut invariant: media count stays 0.
    assert orch.media_artifact_count == 0


def test_mocked_smoke_plus_orchestrator_with_documentcloud_pdf_yields_document_artifact():
    """LIVE5's preferred pair: CourtListener + DocumentCloud. The
    DocumentCloud connector preserves canonical_url and pdf_url on
    SourceRecord metadata, so the resolver can graduate them into
    document VerifiedArtifacts post-smoke without any download."""
    case_input = load_case_input()
    cl_session = FakeSession([courtlistener_response_with_opinion()])
    dc_session = FakeSession([documentcloud_response_with_public_pdf_url()])
    configs = [
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5),
        LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5),
    ]
    smoke = run_capped_multi_connector_smoke(
        case_input,
        configs=configs,
        connectors=[
            CourtListenerConnector(session=cl_session),
            DocumentCloudConnector(session=dc_session),
        ],
        env={DEFAULT_ENV_VAR: "1"},
    )
    sources = []
    for r in smoke.per_connector:
        sources.extend(r.sources)
    orch = run_metadata_only_resolvers(sources)

    # Smoke alone produced no VerifiedArtifacts — the harness deliberately
    # doesn't run resolvers.
    assert smoke.total_verified_artifacts == 0
    assert smoke.api_calls["courtlistener"] == 1
    assert smoke.api_calls["documentcloud"] == 1
    assert smoke.api_calls["muckrock"] == 0
    assert smoke.api_calls["brave"] == 0
    assert smoke.api_calls["firecrawl"] == 0

    # Orchestrator graduates concrete public URLs from both connectors.
    artifact_authorities = sorted(a.source_authority for a in orch.verified_artifacts)
    assert "court" in artifact_authorities, "CourtListener opinion should yield a document artifact"
    assert "documentcloud" in artifact_authorities, "DocumentCloud public PDF should yield a document artifact"
    assert orch.media_artifact_count == 0, "all artifacts here are documents"
    assert orch.document_artifact_count == orch.verified_artifact_count


def test_mocked_smoke_release_language_without_url_creates_no_artifact():
    """The MuckRock smoke returns release language but no
    released_files URL. The orchestrator must NOT graduate that into a
    VerifiedArtifact."""
    case_input = load_case_input()
    smoke, sources, orch = _run_mocked_smoke_and_orchestrate(
        case_input,
        FakeResponse({"results": []}),  # CourtListener returns empty
        muckrock_response_with_release_text_no_url(),
    )
    foia_artifacts = [a for a in orch.verified_artifacts if a.source_authority == "foia"]
    assert foia_artifacts == [], (
        "MuckRock release language without released_files URL must not create artifact"
    )


def test_mocked_smoke_plus_orchestrator_makes_zero_extra_network_calls(monkeypatch):
    """Once the smoke is done, the orchestrator must not make any
    additional HTTP call. Resolvers are metadata-only."""
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    case_input = load_case_input()
    cl_session = FakeSession([courtlistener_response_with_opinion()])
    dc_session = FakeSession([documentcloud_response_with_public_pdf_url()])
    configs = [
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5),
        LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5),
    ]
    # The smoke calls connector.search → FakeSession.get (still no live HTTP).
    # The orchestrator runs purely off SourceRecord metadata.
    smoke = run_capped_multi_connector_smoke(
        case_input,
        configs=configs,
        connectors=[
            CourtListenerConnector(session=cl_session),
            DocumentCloudConnector(session=dc_session),
        ],
        env={DEFAULT_ENV_VAR: "1"},
    )
    sources = []
    for r in smoke.per_connector:
        sources.extend(r.sources)
    orch = run_metadata_only_resolvers(sources)

    # FakeSession recorded the calls; requests.Session.get itself was
    # never invoked because we replaced the connector's session.
    assert calls == [], (
        f"orchestrator made {len(calls)} live HTTP call(s); must be metadata-only"
    )


def test_safety_rejects_more_than_two_connectors_for_live_resolver_smoke():
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


def test_safety_rejects_oversize_max_results_for_live_resolver_smoke():
    case_input = load_case_input()
    configs = [
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=99),
        LiveRunConfig(connector="muckrock", max_queries=1, max_results=5),
    ]
    with pytest.raises(LiveRunBlocked, match="max_results"):
        run_capped_multi_connector_smoke(
            case_input, configs=configs, env={DEFAULT_ENV_VAR: "1"}
        )


def test_resolver_does_not_download_or_scrape(monkeypatch):
    """The orchestrator (and its underlying resolvers) must never
    invoke ``requests.Session.get`` themselves — they're purely
    metadata-only."""
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    case_input = load_case_input()
    cl_session = FakeSession([courtlistener_response_with_opinion()])
    dc_session = FakeSession([documentcloud_response_with_public_pdf_url()])
    smoke = run_capped_multi_connector_smoke(
        case_input,
        configs=[
            LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5),
            LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5),
        ],
        connectors=[
            CourtListenerConnector(session=cl_session),
            DocumentCloudConnector(session=dc_session),
        ],
        env={DEFAULT_ENV_VAR: "1"},
    )
    sources = []
    for r in smoke.per_connector:
        sources.extend(r.sources)

    # Reset the call list, then run only the orchestrator. The
    # orchestrator must NOT make any HTTP call — even one would be a
    # bug (resolvers are metadata-only).
    calls.clear()
    orch = run_metadata_only_resolvers(sources)
    assert calls == [], (
        f"resolver orchestrator made {len(calls)} HTTP call(s) — must be 0"
    )


# --- Real-live opt-in path ------------------------------------------------


@pytest.mark.skipif(
    os.environ.get(DEFAULT_ENV_VAR) != "1",
    reason="opt-in via FLAMEON_RUN_LIVE_CASEGRAPH=1",
)
def test_real_live_multisource_resolver_smoke():
    """Real live multi-source smoke + resolver orchestration.

    Strict caps. The smoke fires two live calls (CourtListener +
    DocumentCloud — the LIVE5 spec's preferred pair); the orchestrator
    then runs over the returned SourceRecords without any further
    network call.

    Assertions: caps held, total live calls <= 2, zero paid-provider
    calls, orchestrator runs without error. The number of
    VerifiedArtifacts depends on what the live API actually returns;
    we assert only the safety invariants and log the diagnostics.
    """
    case_input = load_case_input()
    configs = [
        LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5),
        LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5),
    ]

    started = time.perf_counter()
    smoke = run_capped_multi_connector_smoke(case_input, configs=configs)
    sources = []
    for r in smoke.per_connector:
        sources.extend(r.sources)
    orch = run_metadata_only_resolvers(sources)
    runtime = time.perf_counter() - started

    smoke_diag = smoke.to_diagnostics()
    orch_diag = orch.to_diagnostics()

    print(f"LIVE5_CONNECTORS={smoke_diag['connectors']}")
    print(f"LIVE5_TOTAL_LIVE_CALLS={smoke_diag['total_live_calls']}")
    print(f"LIVE5_TOTAL_SOURCE_RECORDS={smoke_diag['total_source_records']}")
    print(f"LIVE5_API_CALLS={smoke_diag['api_calls']}")
    print(f"LIVE5_TOTAL_ESTIMATED_COST_USD={smoke_diag['total_estimated_cost_usd']}")
    print(f"LIVE5_TOTAL_WALLCLOCK_SECONDS={runtime:.2f}")
    print(f"LIVE5_ERRORS={smoke_diag['errors']}")
    for entry in smoke_diag["per_connector"]:
        print(
            f"LIVE5_PER_CONNECTOR connector={entry['connector']} "
            f"endpoint={entry['endpoint']} "
            f"status={entry['status_code']} "
            f"results={entry['result_count']} "
            f"wallclock={entry['wallclock_seconds']}"
        )
    print(f"LIVE5_ORCH_RESOLVERS_RUN={orch_diag['resolvers_run']}")
    print(f"LIVE5_ORCH_VERIFIED_ARTIFACT_COUNT={orch_diag['verified_artifact_count']}")
    print(f"LIVE5_ORCH_MEDIA_ARTIFACT_COUNT={orch_diag['media_artifact_count']}")
    print(f"LIVE5_ORCH_DOCUMENT_ARTIFACT_COUNT={orch_diag['document_artifact_count']}")
    print(f"LIVE5_ORCH_VERIFIED_ARTIFACT_URLS={orch_diag['verified_artifact_urls']}")
    print(f"LIVE5_ORCH_RISK_FLAGS={orch_diag['risk_flags']}")

    # Safety invariants — these MUST hold regardless of live yield.
    assert smoke.total_live_calls <= MAX_CONNECTORS_HARD_CAP
    assert len(smoke.per_connector) == 2
    assert smoke.api_calls["brave"] == 0
    assert smoke.api_calls["firecrawl"] == 0
    assert smoke.api_calls["llm"] == 0
    assert smoke.total_estimated_cost_usd == 0.0
    for r in smoke.per_connector:
        assert r.budget.query_count == 1
        assert r.source_count <= 5
    # The orchestrator may produce zero or more artifacts depending on
    # what's in the live SourceRecords — we don't enforce a specific
    # count, only that no paid/blocked-provider call was made and no
    # download/scraping occurred (verified by the no-network test
    # above).
    assert orch.verified_artifact_count >= 0
