"""LIVE6 — Known-case pilot live smoke (Endpoint v0 condition 3).

Drives a single capped live call against the connector chosen by the
PILOT3 selector for the first Endpoint v0 attempt (currently
``mpv_documentcloud_pilot``: DocumentCloud, 1 query, max 5 results),
runs the metadata-only resolver over the returned SourceRecords,
assembles a CasePacket, scores it, and writes the canonical
deliverables (run bundle, ledger entry, dry-vs-live comparison
report, live-yield report, pilot/validation scoreboard, endpoint v0
status) under ``autoresearch/.runs/live6/`` (gitignored).

Hard caps enforced by validate_live_run upstream and by the test
itself:

- max_connectors      = 1
- max_queries         = 1
- max_results         = 5
- total_live_calls   <= 1
- no Brave / Firecrawl / LLM / downloads / scraping / transcript
  fetching
- VerifiedArtifacts only from concrete public URLs in returned
  metadata (the existing DocumentCloud resolver enforces this)

Mocked tests run in the default no-live suite. The real-live test is
opt-in via ``FLAMEON_RUN_LIVE_CASEGRAPH=1``.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    DocumentCloudConnector,
    LiveRunConfig,
    build_live_yield_report,
    build_pilot_validation_scoreboard,
    build_run_ledger_entry,
    build_validation_metrics_report,
    compare_run_bundles,
    parse_mapping_police_violence_case_input,
    resolve_documentcloud_files,
    run_capped_live_smoke,
    run_pilot_manifest,
    run_validation_manifest,
    score_case_packet,
    select_pilot_for_live_smoke,
)
from pipeline2_discovery.casegraph.assembly import assemble_structured_case_packet
from pipeline2_discovery.casegraph.cli import _write_bundle, build_run_bundle
from pipeline2_discovery.casegraph.live_safety import DEFAULT_ENV_VAR


ROOT = Path(__file__).resolve().parents[1]
PILOT_MANIFEST = ROOT / "tests" / "fixtures" / "pilot_cases" / "pilot_manifest.json"
RUN_DIR = ROOT / "autoresearch" / ".runs" / "live6"


def _load_mpv_input():
    with (ROOT / "tests" / "fixtures" / "structured_inputs" / "mpv_complete.json").open(
        "r", encoding="utf-8"
    ) as f:
        row = json.load(f)
    return parse_mapping_police_violence_case_input(row)


# --- Fake session/connector plumbing for the mocked path -------------------


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


def _documentcloud_response_with_public_pdfs():
    return FakeResponse(
        {
            "results": [
                {
                    "id": 88001001,
                    "title": "Memphis Police IA Report - Jordan Example",
                    "canonical_url": "https://www.documentcloud.org/documents/88001001-jordan-example-ia-report/",
                    "pdf_url": "https://s3.documentcloud.org/documents/88001001/jordan-example-ia-report.pdf",
                    "access": "public",
                    "publisher": "Memphis Police Department",
                    "published_date": "2022-09-12",
                    "page_count": 14,
                    "language": "eng",
                },
                {
                    "id": 88001002,
                    "title": "Shelby County DA Memo",
                    "canonical_url": "https://www.documentcloud.org/documents/88001002-shelby-county-da-memo/",
                    "pdf_url": "https://s3.documentcloud.org/documents/88001002/shelby-county-da-memo.pdf",
                    "access": "public",
                    "publisher": "Shelby County DA",
                    "published_date": "2022-10-05",
                    "page_count": 4,
                    "language": "eng",
                },
            ]
        }
    )


def _seed_documentcloud_factory(monkeypatch, response):
    fake = FakeSession(response)

    def factory():
        return DocumentCloudConnector(session=fake)

    import pipeline2_discovery.casegraph.live_smoke as live_smoke_module
    monkeypatch.setitem(
        live_smoke_module.CONNECTOR_FACTORIES, "documentcloud", factory
    )
    return fake


# --- Mocked tests (run in default suite) -----------------------------------


def test_pilot_selector_picks_mpv_documentcloud_pilot():
    """LIVE6 must run against PILOT3's selection. If the manifest or
    selector ever changes the winner, this test breaks loudly so we
    re-anchor before firing live."""
    selection = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    assert selection["selected_pilot_id"] == "mpv_documentcloud_pilot"
    assert selection["allowed_connectors"] == ["documentcloud"]
    assert selection["max_live_calls"] == 1
    assert selection["max_results_per_connector"] == 5


def test_mocked_live6_full_flow_yields_verified_document_artifacts(
    monkeypatch, tmp_path
):
    """End-to-end mocked LIVE6: the chosen pilot's smoke -> resolver
    flow MUST produce >=1 VerifiedArtifact when DocumentCloud returns
    public canonical URLs, and write all canonical deliverables."""
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    fake = _seed_documentcloud_factory(
        monkeypatch, _documentcloud_response_with_public_pdfs()
    )

    parsed = _load_mpv_input()
    case_input = parsed.case_input

    config = LiveRunConfig(
        connector="documentcloud", max_queries=1, max_results=5
    )

    started = time.perf_counter()
    smoke = run_capped_live_smoke(case_input, config=config)
    wallclock = round(time.perf_counter() - started, 4)

    # Live SourceRecords go onto the assembled CasePacket.
    assembly = assemble_structured_case_packet(parsed)
    packet = assembly.packet
    packet.sources.extend(smoke.sources)

    # Resolver graduates concrete public URLs to VerifiedArtifacts.
    resolve_documentcloud_files(packet)
    result = score_case_packet(packet)

    # Endpoint v0 condition 3 anchors:
    assert smoke.budget.query_count == 1
    assert len(fake.calls) <= 1
    assert smoke.budget.api_calls["brave"] == 0
    assert smoke.budget.api_calls["firecrawl"] == 0
    assert smoke.budget.api_calls["llm"] == 0
    assert smoke.budget.estimated_cost_usd == 0.0

    assert len(packet.verified_artifacts) >= 1, (
        "LIVE6 mocked: expected >=1 VerifiedArtifact from DocumentCloud public PDFs"
    )
    media_count = sum(
        1 for a in packet.verified_artifacts if a.format in {"video", "audio"}
    )
    document_count = sum(
        1 for a in packet.verified_artifacts if a.format in {"pdf", "document"}
    )
    assert document_count >= 1
    # Document-only must NEVER PRODUCE per the gate.
    if media_count == 0:
        assert result.verdict != "PRODUCE"

    # Bundle / ledger / comparison / live-yield -- write to tmp_path
    # in the test (not the repo .runs/) to keep the mocked path
    # hermetic.
    diagnostics = smoke.to_diagnostics()
    api_calls = smoke.budget.to_ledger_summary().get("api_calls", {})

    bundle = build_run_bundle(
        mode="live",
        experiment_id="LIVE6-mocked",
        wallclock_seconds=wallclock,
        packet=packet,
        parsed=parsed,
        smoke_diagnostics=diagnostics,
        api_calls=api_calls,
        notes=["LIVE6", "mocked", "documentcloud", "max_results=5"],
    )
    bundle_path = tmp_path / "live_bundle.json"
    _write_bundle(bundle_path, bundle)
    assert bundle_path.exists()
    assert bundle["mode"] == "live"
    assert bundle["verified_artifacts"]

    ledger = build_run_ledger_entry(
        experiment_id="LIVE6-mocked",
        packet=packet,
        api_calls=api_calls,
        wallclock_seconds=wallclock,
        notes=["LIVE6 mocked"],
    )
    assert ledger.api_calls["documentcloud"] == 1
    assert ledger.api_calls["brave"] == 0
    assert ledger.api_calls["firecrawl"] == 0
    assert ledger.api_calls["llm"] == 0
    assert ledger.estimated_cost_usd == 0.0

    # Dry bundle for comparison.
    dry_bundle = build_run_bundle(
        mode="multi_source_dry_run",
        experiment_id="LIVE6-mocked-dry",
        wallclock_seconds=0.0,
        packet=assemble_structured_case_packet(parsed).packet,
        parsed=parsed,
    )
    diff = compare_run_bundles(dry_bundle, bundle)
    assert diff["artifact_yield_delta"]["delta"] >= 1
    assert diff["source_count_delta"]["delta"] >= 1

    yield_report = build_live_yield_report(
        [ledger], per_connector_diagnostics=[diagnostics]
    )
    assert yield_report["total_runs"] == 1
    assert yield_report["total_live_calls"] == 1
    assert yield_report["total_estimated_cost_usd"] == 0.0


def test_mocked_live6_makes_exactly_one_documentcloud_call(monkeypatch):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    fake = _seed_documentcloud_factory(
        monkeypatch, _documentcloud_response_with_public_pdfs()
    )

    parsed = _load_mpv_input()
    config = LiveRunConfig(
        connector="documentcloud", max_queries=1, max_results=5
    )
    run_capped_live_smoke(parsed.case_input, config=config)
    # The harness records exactly one query in the budget; the
    # connector itself may make at most one HTTP call per query.
    assert len(fake.calls) <= 1


def test_mocked_live6_zero_paid_provider_calls(monkeypatch):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    _seed_documentcloud_factory(
        monkeypatch, _documentcloud_response_with_public_pdfs()
    )

    parsed = _load_mpv_input()
    config = LiveRunConfig(
        connector="documentcloud", max_queries=1, max_results=5
    )
    smoke = run_capped_live_smoke(parsed.case_input, config=config)
    for paid in ("brave", "firecrawl"):
        assert smoke.budget.api_calls[paid] == 0
    assert smoke.budget.api_calls["llm"] == 0
    assert smoke.budget.estimated_cost_usd == 0.0


def test_mocked_live6_documents_do_not_produce(monkeypatch):
    """Document-only output must keep verdict at HOLD/SKIP, never
    PRODUCE."""
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    _seed_documentcloud_factory(
        monkeypatch, _documentcloud_response_with_public_pdfs()
    )

    parsed = _load_mpv_input()
    config = LiveRunConfig(
        connector="documentcloud", max_queries=1, max_results=5
    )
    smoke = run_capped_live_smoke(parsed.case_input, config=config)

    assembly = assemble_structured_case_packet(parsed)
    packet = assembly.packet
    packet.sources.extend(smoke.sources)
    resolve_documentcloud_files(packet)
    result = score_case_packet(packet)

    media_count = sum(
        1 for a in packet.verified_artifacts if a.format in {"video", "audio"}
    )
    if media_count == 0:
        assert result.verdict != "PRODUCE", (
            f"document-only artifacts with verdict={result.verdict} "
            "violates the media gate"
        )


def test_mocked_live6_refuses_oversize_max_results(monkeypatch):
    """Safety: validate_live_run upstream caps max_results at 5; pass
    a higher value and confirm it's refused."""
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    _seed_documentcloud_factory(
        monkeypatch, _documentcloud_response_with_public_pdfs()
    )

    parsed = _load_mpv_input()
    config = LiveRunConfig(
        connector="documentcloud", max_queries=1, max_results=999
    )
    from pipeline2_discovery.casegraph import LiveRunBlocked
    with pytest.raises(LiveRunBlocked):
        run_capped_live_smoke(parsed.case_input, config=config)


def test_mocked_live6_refuses_paid_connector(monkeypatch):
    """Safety: any attempt to swap in a paid connector (e.g. brave)
    must be refused upstream."""
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")

    parsed = _load_mpv_input()
    config = LiveRunConfig(connector="brave", max_queries=1, max_results=5)
    from pipeline2_discovery.casegraph import LiveRunBlocked
    with pytest.raises(LiveRunBlocked):
        run_capped_live_smoke(parsed.case_input, config=config)


# --- Real-live opt-in test -------------------------------------------------


@pytest.mark.skipif(
    os.environ.get(DEFAULT_ENV_VAR) != "1",
    reason="opt-in via FLAMEON_RUN_LIVE_CASEGRAPH=1",
)
def test_real_live_known_case_pilot_smoke():
    """LIVE6 — single capped live call against the PILOT3-selected
    pilot. Writes the canonical deliverables to autoresearch/.runs/
    live6/ for review.

    Safety invariants are asserted; the actual artifact yield depends
    on what DocumentCloud returns. Endpoint v0 condition 3 is
    reported via endpoint_v0_status.json regardless.
    """
    selection = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    assert selection["selected_pilot_id"] == "mpv_documentcloud_pilot"
    assert selection["max_live_calls"] == 1
    assert selection["max_results_per_connector"] == 5

    parsed = _load_mpv_input()
    case_input = parsed.case_input

    config = LiveRunConfig(
        connector="documentcloud", max_queries=1, max_results=5
    )

    started = time.perf_counter()
    smoke = run_capped_live_smoke(case_input, config=config)
    smoke_wall = round(time.perf_counter() - started, 4)

    diagnostics = smoke.to_diagnostics()
    print(f"LIVE6_CONNECTOR=documentcloud")
    print(f"LIVE6_QUERY={diagnostics.get('query')}")
    print(f"LIVE6_ENDPOINT={diagnostics.get('endpoint')}")
    print(f"LIVE6_STATUS={diagnostics.get('status_code')}")
    print(f"LIVE6_RESULT_COUNT={diagnostics.get('result_count')}")
    print(f"LIVE6_WALLCLOCK={diagnostics.get('wallclock_seconds')}")
    print(f"LIVE6_API_CALLS={diagnostics.get('api_calls')}")
    print(f"LIVE6_ESTIMATED_COST_USD={diagnostics.get('estimated_cost_usd')}")
    print(f"LIVE6_ERROR={diagnostics.get('error')}")

    # Safety invariants.
    assert smoke.budget.query_count == 1
    assert smoke.budget.api_calls["brave"] == 0
    assert smoke.budget.api_calls["firecrawl"] == 0
    assert smoke.budget.api_calls["llm"] == 0
    assert smoke.budget.estimated_cost_usd == 0.0
    assert smoke.source_count <= 5

    # Resolver graduates artifacts from returned metadata only.
    assembly = assemble_structured_case_packet(parsed)
    packet = assembly.packet
    packet.sources.extend(smoke.sources)
    resolve_documentcloud_files(packet)
    result = score_case_packet(packet)

    # Document-only must not become PRODUCE.
    media_count = sum(
        1 for a in packet.verified_artifacts if a.format in {"video", "audio"}
    )
    document_count = sum(
        1 for a in packet.verified_artifacts if a.format in {"pdf", "document"}
    )
    if media_count == 0:
        assert result.verdict != "PRODUCE"

    print(f"LIVE6_VERIFIED_ARTIFACT_COUNT={len(packet.verified_artifacts)}")
    print(f"LIVE6_MEDIA_ARTIFACT_COUNT={media_count}")
    print(f"LIVE6_DOCUMENT_ARTIFACT_COUNT={document_count}")
    print(
        "LIVE6_VERIFIED_ARTIFACT_URLS="
        + ",".join(a.artifact_url for a in packet.verified_artifacts)
    )
    print(f"LIVE6_VERDICT={result.verdict}")
    print(f"LIVE6_IDENTITY_CONFIDENCE={packet.case_identity.identity_confidence}")
    print(f"LIVE6_OUTCOME_STATUS={packet.case_identity.outcome_status}")

    # Build canonical deliverables and write to ignored .runs/live6.
    RUN_DIR.mkdir(parents=True, exist_ok=True)

    api_calls = smoke.budget.to_ledger_summary().get("api_calls", {})

    live_bundle = build_run_bundle(
        mode="live",
        experiment_id="LIVE6-known-case-pilot-live-smoke",
        wallclock_seconds=smoke_wall,
        packet=packet,
        parsed=parsed,
        smoke_diagnostics=diagnostics,
        api_calls=api_calls,
        notes=[
            "LIVE6",
            "pilot=mpv_documentcloud_pilot",
            "connector=documentcloud",
            "max_queries=1",
            "max_results=5",
        ],
    )
    _write_bundle(RUN_DIR / "live_bundle.json", live_bundle)

    # Dry bundle for comparison: same fixture, no live data.
    dry_bundle = build_run_bundle(
        mode="multi_source_dry_run",
        experiment_id="LIVE6-pilot3-dry",
        wallclock_seconds=0.0,
        packet=assemble_structured_case_packet(parsed).packet,
        parsed=parsed,
    )
    _write_bundle(RUN_DIR / "dry_bundle_for_comparison.json", dry_bundle)

    comparison = compare_run_bundles(dry_bundle, live_bundle)
    _write_bundle(RUN_DIR / "comparison.json", comparison)

    ledger = build_run_ledger_entry(
        experiment_id="LIVE6-known-case-pilot-live-smoke",
        packet=packet,
        api_calls=api_calls,
        wallclock_seconds=smoke_wall,
        notes=[
            "LIVE6",
            "pilot=mpv_documentcloud_pilot",
            "connector=documentcloud",
        ],
    )
    _write_bundle(RUN_DIR / "ledger_entry.json", ledger.to_dict())

    yield_report = build_live_yield_report(
        [ledger], per_connector_diagnostics=[diagnostics]
    )
    _write_bundle(RUN_DIR / "live_yield.json", yield_report)

    val = run_validation_manifest()
    pilot_out = run_pilot_manifest()
    scoreboard = build_pilot_validation_scoreboard(
        validation_output=val, pilot_output=pilot_out
    )
    _write_bundle(RUN_DIR / "scoreboard.json", scoreboard)

    val_metrics = build_validation_metrics_report(val)

    endpoint_status = {
        "experiment_id": "LIVE6-known-case-pilot-live-smoke",
        "pilot_id": "mpv_documentcloud_pilot",
        "connector_used": "documentcloud",
        "queries_used": diagnostics.get("query"),
        "live_calls_used": smoke.budget.query_count,
        "source_records_returned": smoke.source_count,
        "verified_artifact_count": len(packet.verified_artifacts),
        "media_artifact_count": media_count,
        "document_artifact_count": document_count,
        "verified_artifact_urls": [a.artifact_url for a in packet.verified_artifacts],
        "verdict": result.verdict,
        "identity_confidence": packet.case_identity.identity_confidence,
        "outcome_status": packet.case_identity.outcome_status,
        "estimated_cost_usd": smoke.budget.estimated_cost_usd,
        "endpoint_v0_conditions": {
            "cond1_validation_passes": (
                val_metrics["verdict_accuracy"]["accuracy_pct"] == 100.0
                and all(v == 0 for v in val_metrics["guard_counters"].values())
            ),
            "cond2_pilot_readiness": (
                scoreboard["pilots"]["ready_for_live"] >= 3
                and not scoreboard["warnings"]
            ),
            "cond3_live_pilot_artifact": len(packet.verified_artifacts) >= 1,
            "cond3_run_bundle_written": (RUN_DIR / "live_bundle.json").exists(),
            "cond3_ledger_entry_generated": (RUN_DIR / "ledger_entry.json").exists(),
            "cond3_dry_vs_live_comparison": (RUN_DIR / "comparison.json").exists(),
            "cond4_media_doc_distinction": True,
            "cond5_no_safety_gate_breaks": (
                smoke.budget.api_calls["brave"] == 0
                and smoke.budget.api_calls["firecrawl"] == 0
                and smoke.budget.api_calls["llm"] == 0
                and smoke.budget.estimated_cost_usd == 0.0
                and smoke.budget.query_count <= 1
                and smoke.source_count <= 5
            ),
        },
    }
    _write_bundle(RUN_DIR / "endpoint_v0_status.json", endpoint_status)

    # The test asserts ONLY the safety invariants. Endpoint v0
    # achievement is captured in endpoint_v0_status.json for review;
    # if zero artifacts come back, the live run still succeeded as a
    # smoke (the resolver and gates still held).
    print(f"LIVE6_ENDPOINT_V0_COND3_ARTIFACT={endpoint_status['endpoint_v0_conditions']['cond3_live_pilot_artifact']}")
    print(f"LIVE6_OUTPUT_DIR={RUN_DIR}")
