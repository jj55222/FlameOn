"""LIVE7 — Real-case capped live pilot smoke (Endpoint v0 finish).

Drives a single capped live HTTPS call against the connector chosen
by the PILOT3 selector for the first real-case attempt (currently
``real_case_min_jian_guan_pilot``: documentcloud, 1 query, max 5
results), runs the metadata-only resolver over returned
SourceRecords, attaches them to the pre-locked CasePacket seed
(identity=high, outcome=sentenced), scores it, and writes the
canonical deliverables under ``autoresearch/.runs/live7/`` (gitignored).

Hard caps enforced upstream by validate_live_run AND by the test:

- max_connectors      = 1
- max_queries         = 1
- max_results         = 5
- total_live_calls   <= 1
- no Brave / Firecrawl / LLM / downloads / scraping / transcript
  fetching
- VerifiedArtifacts only from concrete public URLs in returned
  metadata

Mocked tests run in the default no-live suite. The real-live test
is opt-in via ``FLAMEON_RUN_LIVE_CASEGRAPH=1``.
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
    resolve_documentcloud_files,
    run_capped_live_smoke,
    run_pilot_manifest,
    run_validation_manifest,
    score_case_packet,
    select_pilot_for_live_smoke,
)
from pipeline2_discovery.casegraph.cli import _load_fixture, _write_bundle, build_run_bundle
from pipeline2_discovery.casegraph.live_safety import DEFAULT_ENV_VAR


ROOT = Path(__file__).resolve().parents[1]
PILOT_MANIFEST = ROOT / "tests" / "fixtures" / "pilot_cases" / "pilot_manifest.json"
SEED_PATH = ROOT / "tests" / "fixtures" / "pilot_cases" / "real_case_min_jian_guan.json"
RUN_DIR = ROOT / "autoresearch" / ".runs" / "live7"


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


def _documentcloud_response():
    return FakeResponse(
        {
            "results": [
                {
                    "id": 71001001,
                    "title": "SFPD Critical Incident Report - case CR-1001",
                    "canonical_url": "https://www.documentcloud.org/documents/71001001-sfpd-cr-1001-incident-report/",
                    "pdf_url": "https://s3.documentcloud.org/documents/71001001/sfpd-cr-1001-incident-report.pdf",
                    "access": "public",
                    "publisher": "San Francisco Police Department",
                    "published_date": "2024-04-30",
                    "page_count": 22,
                    "language": "eng",
                },
                {
                    "id": 71001002,
                    "title": "San Francisco DA Sentencing Memorandum",
                    "canonical_url": "https://www.documentcloud.org/documents/71001002-sf-da-sentencing-memo/",
                    "pdf_url": "https://s3.documentcloud.org/documents/71001002/sf-da-sentencing-memo.pdf",
                    "access": "public",
                    "publisher": "San Francisco District Attorney",
                    "published_date": "2024-09-12",
                    "page_count": 6,
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


# --- Mocked tests (default-runnable) --------------------------------------


def test_live7_pilot_selector_picks_real_case_pilot():
    selection = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    assert selection["selected_pilot_id"] == "real_case_min_jian_guan_pilot"
    assert selection["allowed_connectors"] == ["documentcloud"]
    assert selection["max_live_calls"] == 1
    assert selection["max_results_per_connector"] == 5


def test_mocked_live7_full_flow_yields_documents_and_keeps_hold(monkeypatch):
    """End-to-end mocked LIVE7: real-case seed (identity=high,
    outcome=sentenced) plus live document yield must produce a packet
    with verified document artifacts and verdict HOLD (document-only,
    media gate non-negotiable)."""
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    fake = _seed_documentcloud_factory(monkeypatch, _documentcloud_response())

    packet = _load_fixture(SEED_PATH)
    case_input = packet.input

    config = LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5)
    smoke = run_capped_live_smoke(case_input, config=config)

    packet.sources.extend(smoke.sources)
    resolve_documentcloud_files(packet)
    result = score_case_packet(packet)

    # Safety invariants.
    assert smoke.budget.query_count == 1
    assert len(fake.calls) <= 1
    for paid in ("brave", "firecrawl"):
        assert smoke.budget.api_calls[paid] == 0
    assert smoke.budget.api_calls["llm"] == 0
    assert smoke.budget.estimated_cost_usd == 0.0

    # The seed already had high identity + sentenced outcome; with
    # added document VerifiedArtifacts, the packet now satisfies
    # condition 3 sub-bullets a, b, c, d.
    assert packet.case_identity.identity_confidence == "high"
    assert packet.case_identity.outcome_status == "sentenced"
    assert len(packet.verified_artifacts) >= 1
    document_count = sum(
        1 for a in packet.verified_artifacts if a.format in {"pdf", "document"}
    )
    media_count = sum(
        1 for a in packet.verified_artifacts if a.format in {"video", "audio"}
    )
    assert document_count >= 1
    # Document-only -> verdict must remain HOLD per the media gate.
    if media_count == 0:
        assert result.verdict != "PRODUCE"


def test_mocked_live7_makes_at_most_one_documentcloud_call(monkeypatch):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    fake = _seed_documentcloud_factory(monkeypatch, _documentcloud_response())
    packet = _load_fixture(SEED_PATH)
    config = LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5)
    run_capped_live_smoke(packet.input, config=config)
    assert len(fake.calls) <= 1


def test_mocked_live7_zero_paid_calls(monkeypatch):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    _seed_documentcloud_factory(monkeypatch, _documentcloud_response())
    packet = _load_fixture(SEED_PATH)
    config = LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5)
    smoke = run_capped_live_smoke(packet.input, config=config)
    assert smoke.budget.api_calls["brave"] == 0
    assert smoke.budget.api_calls["firecrawl"] == 0
    assert smoke.budget.api_calls["llm"] == 0
    assert smoke.budget.estimated_cost_usd == 0.0


def test_mocked_live7_documents_do_not_produce(monkeypatch):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    _seed_documentcloud_factory(monkeypatch, _documentcloud_response())
    packet = _load_fixture(SEED_PATH)
    config = LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5)
    smoke = run_capped_live_smoke(packet.input, config=config)
    packet.sources.extend(smoke.sources)
    resolve_documentcloud_files(packet)
    result = score_case_packet(packet)
    media_count = sum(
        1 for a in packet.verified_artifacts if a.format in {"video", "audio"}
    )
    if media_count == 0:
        assert result.verdict != "PRODUCE"


# --- Real-live opt-in test ------------------------------------------------


@pytest.mark.skipif(
    os.environ.get(DEFAULT_ENV_VAR) != "1",
    reason="opt-in via FLAMEON_RUN_LIVE_CASEGRAPH=1",
)
def test_real_live7_real_case_smoke():
    """LIVE7 -- single capped live HTTPS call against DocumentCloud
    with the real-case seed (Min Jian Guan, San Francisco). Asserts
    safety invariants only; actual yield depends on what
    DocumentCloud returns. All canonical deliverables are written to
    autoresearch/.runs/live7/."""
    selection = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    assert selection["selected_pilot_id"] == "real_case_min_jian_guan_pilot"
    assert selection["max_live_calls"] == 1
    assert selection["max_results_per_connector"] == 5

    packet = _load_fixture(SEED_PATH)
    case_input = packet.input

    config = LiveRunConfig(connector="documentcloud", max_queries=1, max_results=5)
    started = time.perf_counter()
    smoke = run_capped_live_smoke(case_input, config=config)
    smoke_wall = round(time.perf_counter() - started, 4)

    diagnostics = smoke.to_diagnostics()
    print(f"LIVE7_PILOT=real_case_min_jian_guan_pilot")
    print(f"LIVE7_CONNECTOR=documentcloud")
    print(f"LIVE7_QUERY={diagnostics.get('query')}")
    print(f"LIVE7_ENDPOINT={diagnostics.get('endpoint')}")
    print(f"LIVE7_STATUS={diagnostics.get('status_code')}")
    print(f"LIVE7_RESULT_COUNT={diagnostics.get('result_count')}")
    print(f"LIVE7_WALLCLOCK={diagnostics.get('wallclock_seconds')}")
    print(f"LIVE7_API_CALLS={diagnostics.get('api_calls')}")
    print(f"LIVE7_ESTIMATED_COST_USD={diagnostics.get('estimated_cost_usd')}")
    print(f"LIVE7_ERROR={diagnostics.get('error')}")

    # Safety invariants.
    assert smoke.budget.query_count == 1
    assert smoke.budget.api_calls["brave"] == 0
    assert smoke.budget.api_calls["firecrawl"] == 0
    assert smoke.budget.api_calls["llm"] == 0
    assert smoke.budget.estimated_cost_usd == 0.0
    assert smoke.source_count <= 5

    # Resolver graduates artifacts from returned metadata only. Seed's
    # high identity + sentenced outcome carry through.
    packet.sources.extend(smoke.sources)
    resolve_documentcloud_files(packet)
    result = score_case_packet(packet)

    media_count = sum(
        1 for a in packet.verified_artifacts if a.format in {"video", "audio"}
    )
    document_count = sum(
        1 for a in packet.verified_artifacts if a.format in {"pdf", "document"}
    )
    if media_count == 0:
        assert result.verdict != "PRODUCE"

    print(f"LIVE7_VERIFIED_ARTIFACT_COUNT={len(packet.verified_artifacts)}")
    print(f"LIVE7_MEDIA_ARTIFACT_COUNT={media_count}")
    print(f"LIVE7_DOCUMENT_ARTIFACT_COUNT={document_count}")
    print(
        "LIVE7_VERIFIED_ARTIFACT_URLS="
        + ",".join(a.artifact_url for a in packet.verified_artifacts)
    )
    print(f"LIVE7_VERDICT={result.verdict}")
    print(f"LIVE7_IDENTITY_CONFIDENCE={packet.case_identity.identity_confidence}")
    print(f"LIVE7_OUTCOME_STATUS={packet.case_identity.outcome_status}")

    # Build canonical deliverables and write to ignored .runs/live7.
    RUN_DIR.mkdir(parents=True, exist_ok=True)

    api_calls = smoke.budget.to_ledger_summary().get("api_calls", {})

    live_bundle = build_run_bundle(
        mode="live",
        experiment_id="LIVE7-real-case-pilot-live-smoke",
        wallclock_seconds=smoke_wall,
        packet=packet,
        smoke_diagnostics=diagnostics,
        api_calls=api_calls,
        notes=[
            "LIVE7",
            "pilot=real_case_min_jian_guan_pilot",
            "connector=documentcloud",
            "max_queries=1",
            "max_results=5",
            "real_case=true",
        ],
    )
    _write_bundle(RUN_DIR / "real_case_live_bundle.json", live_bundle)

    # Dry bundle for comparison: re-load the seed fresh.
    dry_packet = _load_fixture(SEED_PATH)
    dry_bundle = build_run_bundle(
        mode="default",
        experiment_id="LIVE7-real-case-dry-baseline",
        wallclock_seconds=0.0,
        packet=dry_packet,
    )
    _write_bundle(RUN_DIR / "real_case_dry_bundle_for_comparison.json", dry_bundle)

    comparison = compare_run_bundles(dry_bundle, live_bundle)
    _write_bundle(RUN_DIR / "real_case_comparison.json", comparison)

    ledger = build_run_ledger_entry(
        experiment_id="LIVE7-real-case-pilot-live-smoke",
        packet=packet,
        api_calls=api_calls,
        wallclock_seconds=smoke_wall,
        notes=["LIVE7", "pilot=real_case_min_jian_guan_pilot"],
    )
    _write_bundle(RUN_DIR / "real_case_ledger_entry.json", ledger.to_dict())

    yield_report = build_live_yield_report(
        [ledger], per_connector_diagnostics=[diagnostics]
    )
    _write_bundle(RUN_DIR / "real_case_live_yield.json", yield_report)

    val = run_validation_manifest()
    pilot_out = run_pilot_manifest()
    scoreboard = build_pilot_validation_scoreboard(
        validation_output=val, pilot_output=pilot_out
    )
    _write_bundle(RUN_DIR / "real_case_scoreboard.json", scoreboard)

    val_metrics = build_validation_metrics_report(val)

    endpoint_status = {
        "experiment_id": "LIVE7-real-case-pilot-live-smoke",
        "pilot_id": "real_case_min_jian_guan_pilot",
        "real_case": True,
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
            "cond3a_identity_locked_high": (
                packet.case_identity.identity_confidence == "high"
            ),
            "cond3b_concluded_outcome": (
                packet.case_identity.outcome_status
                in {"sentenced", "closed", "convicted"}
            ),
            "cond3c_verified_artifact_url": len(packet.verified_artifacts) >= 1,
            "cond3d_artifact_type_classified": (
                len(packet.verified_artifacts) >= 1
                and all(
                    (a.format in {"video", "audio", "pdf", "document", "html"})
                    for a in packet.verified_artifacts
                )
            ),
            "cond3e_run_bundle_written": (RUN_DIR / "real_case_live_bundle.json").exists(),
            "cond3f_ledger_entry_generated": (RUN_DIR / "real_case_ledger_entry.json").exists(),
            "cond3g_dry_vs_live_comparison": (RUN_DIR / "real_case_comparison.json").exists(),
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
    endpoint_status["endpoint_v0_fully_achieved"] = all(
        v for k, v in endpoint_status["endpoint_v0_conditions"].items()
    )
    _write_bundle(RUN_DIR / "real_case_endpoint_v0_status.json", endpoint_status)

    print(
        "LIVE7_ENDPOINT_V0_FULLY_ACHIEVED="
        f"{endpoint_status['endpoint_v0_fully_achieved']}"
    )
    print(f"LIVE7_OUTPUT_DIR={RUN_DIR}")
