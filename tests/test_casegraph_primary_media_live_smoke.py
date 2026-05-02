"""LIVE9_REAL - capped official-primary media pilot smoke.

Default tests are fully mocked and no-live. The real smoke is opt-in
through FLAMEON_RUN_LIVE_CASEGRAPH=1 and stays under the selected
pilot budget.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    LiveRunConfig,
    YouTubeConnector,
    build_run_ledger_entry,
    classify_media_relevance,
    compare_run_bundles,
    resolve_youtube_media_sources,
    run_capped_live_smoke,
    score_case_packet,
)
from pipeline2_discovery.casegraph.cli import _load_fixture, _write_bundle, build_run_bundle
from pipeline2_discovery.casegraph.live_safety import DEFAULT_ENV_VAR
from pipeline2_discovery.casegraph.pilots import select_primary_media_pilot_for_live_smoke


ROOT = Path(__file__).resolve().parents[1]
PILOT_MANIFEST = ROOT / "tests" / "fixtures" / "pilot_cases" / "pilot_manifest.json"
SEED_PATH = ROOT / "tests" / "fixtures" / "pilot_cases" / "primary_media_case_alan_champagne_youtube.json"
RUN_DIR = ROOT / "autoresearch" / ".runs" / "live9"
PILOT_ID = "primary_media_alan_champagne_youtube_pilot"


class FakeYoutubeDL:
    def __init__(self, opts):
        self.opts = opts

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def extract_info(self, query, download=False):
        assert download is False
        assert query == "ytsearch5:Alan Matthew Champagne bodycam"
        return {
            "entries": [
                {
                    "id": "champagne001",
                    "title": "Alan Matthew Champagne Police Interrogation Full Video",
                    "channel": "Court Records Channel",
                    "description": "Police interrogation video in the Alan Champagne case.",
                    "duration": 600,
                    "upload_date": "20200101",
                }
            ]
        }


def test_live9_primary_media_selector_picks_real_youtube_pilot():
    selection = select_primary_media_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)

    assert selection["selected_pilot_id"] == PILOT_ID
    assert selection["allowed_connectors"] == ["youtube"]
    assert selection["max_live_calls"] == 1
    assert selection["max_results_per_connector"] == 5
    assert selection["expected_minimum"]["tier_a_media_required"] is True


def test_mocked_live9_youtube_media_flow_produces_tier_a_without_weak_warning(monkeypatch, tmp_path):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    packet = _load_fixture(SEED_PATH)
    connector = YouTubeConnector(ydl_cls=FakeYoutubeDL)
    config = LiveRunConfig(connector="youtube", max_queries=1, max_results=5)

    started = time.perf_counter()
    smoke = run_capped_live_smoke(packet.input, config=config, connector=connector)
    wallclock = round(time.perf_counter() - started, 4)
    packet.sources.extend(smoke.sources)
    resolution = resolve_youtube_media_sources(packet)
    result = score_case_packet(packet)
    relevance = [classify_media_relevance(artifact) for artifact in packet.verified_artifacts]

    assert smoke.budget.query_count == 1
    assert smoke.source_count == 1
    assert smoke.source_count <= 5
    assert smoke.budget.api_calls["brave"] == 0
    assert smoke.budget.api_calls["firecrawl"] == 0
    assert smoke.budget.api_calls["llm"] == 0
    assert smoke.budget.estimated_cost_usd == 0.0

    assert resolution.verified_artifact_count == 1
    assert len(packet.verified_artifacts) == 1
    assert relevance[0].media_relevance_tier == "A"
    assert packet.case_identity.identity_confidence == "high"
    assert packet.case_identity.outcome_status == "convicted"
    assert result.verdict == "PRODUCE"
    assert "produce_based_on_weak_or_uncertain_media" not in result.risk_flags
    assert "media_query_artifact_type_mismatch" not in result.risk_flags

    live_bundle = build_run_bundle(
        mode="live",
        experiment_id="LIVE9-mocked-primary-media",
        wallclock_seconds=wallclock,
        packet=packet,
        smoke_diagnostics=smoke.to_diagnostics(),
        api_calls=smoke.budget.to_ledger_summary().get("api_calls", {}),
        notes=["LIVE9", "mocked", "youtube", "max_results=5"],
    )
    dry_packet = _load_fixture(SEED_PATH)
    dry_bundle = build_run_bundle(
        mode="default",
        experiment_id="LIVE9-mocked-primary-media-dry",
        wallclock_seconds=0.0,
        packet=dry_packet,
    )
    comparison = compare_run_bundles(dry_bundle, live_bundle)
    ledger = build_run_ledger_entry(
        experiment_id="LIVE9-mocked-primary-media",
        packet=packet,
        api_calls=smoke.budget.to_ledger_summary().get("api_calls", {}),
        wallclock_seconds=wallclock,
        notes=["LIVE9 mocked"],
    )

    _write_bundle(tmp_path / "live_bundle.json", live_bundle)
    _write_bundle(tmp_path / "dry_bundle.json", dry_bundle)
    _write_bundle(tmp_path / "comparison.json", comparison)
    _write_bundle(tmp_path / "ledger.json", ledger.to_dict())

    assert comparison["artifact_yield_delta"]["delta"] == 1
    assert ledger.verdict == "PRODUCE"


@pytest.mark.skipif(
    os.environ.get(DEFAULT_ENV_VAR) != "1",
    reason="opt-in via FLAMEON_RUN_LIVE_CASEGRAPH=1",
)
def test_real_live9_primary_media_youtube_smoke():
    selection = select_primary_media_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    assert selection["selected_pilot_id"] == PILOT_ID
    assert selection["allowed_connectors"] == ["youtube"]
    assert selection["max_live_calls"] == 1
    assert selection["max_results_per_connector"] == 5

    packet = _load_fixture(SEED_PATH)
    config = LiveRunConfig(connector="youtube", max_queries=1, max_results=5)

    started = time.perf_counter()
    smoke = run_capped_live_smoke(packet.input, config=config)
    wallclock = round(time.perf_counter() - started, 4)
    packet.sources.extend(smoke.sources)
    resolution = resolve_youtube_media_sources(packet)
    result = score_case_packet(packet)
    relevance = [classify_media_relevance(artifact) for artifact in packet.verified_artifacts]

    diagnostics = smoke.to_diagnostics()
    media_count = sum(1 for artifact in packet.verified_artifacts if artifact.format in {"video", "audio"})
    tier_counts = {}
    for item in relevance:
        tier_counts[item.media_relevance_tier] = tier_counts.get(item.media_relevance_tier, 0) + 1

    print(f"LIVE9_PILOT={PILOT_ID}")
    print("LIVE9_CONNECTOR=youtube")
    print(f"LIVE9_QUERY={diagnostics.get('query')}")
    print(f"LIVE9_ENDPOINT={diagnostics.get('endpoint')}")
    print(f"LIVE9_STATUS={diagnostics.get('status_code')}")
    print(f"LIVE9_RESULT_COUNT={diagnostics.get('result_count')}")
    print(f"LIVE9_SOURCE_RECORDS={smoke.source_count}")
    print(f"LIVE9_VERIFIED_ARTIFACT_COUNT={len(packet.verified_artifacts)}")
    print(f"LIVE9_VERIFIED_ARTIFACT_URLS={','.join(a.artifact_url for a in packet.verified_artifacts)}")
    print(f"LIVE9_MEDIA_ARTIFACT_COUNT={media_count}")
    print(f"LIVE9_MEDIA_RELEVANCE_TIERS={tier_counts}")
    print(f"LIVE9_VERDICT={result.verdict}")
    print(f"LIVE9_RISK_FLAGS={result.risk_flags}")
    print(f"LIVE9_REASON_CODES={result.reason_codes}")
    print(f"LIVE9_COST_USD={smoke.budget.estimated_cost_usd}")
    print(f"LIVE9_ERROR={diagnostics.get('error')}")

    assert smoke.budget.query_count == 1
    assert smoke.source_count <= 5
    assert smoke.budget.api_calls["brave"] == 0
    assert smoke.budget.api_calls["firecrawl"] == 0
    assert smoke.budget.api_calls["llm"] == 0
    assert smoke.budget.estimated_cost_usd == 0.0

    RUN_DIR.mkdir(parents=True, exist_ok=True)
    api_calls = smoke.budget.to_ledger_summary().get("api_calls", {})
    live_bundle = build_run_bundle(
        mode="live",
        experiment_id="LIVE9-real-official-primary-media-live-smoke",
        wallclock_seconds=wallclock,
        packet=packet,
        smoke_diagnostics=diagnostics,
        api_calls=api_calls,
        notes=["LIVE9", f"pilot={PILOT_ID}", "connector=youtube", "max_results=5"],
    )
    _write_bundle(RUN_DIR / "primary_media_live_bundle.json", live_bundle)
    dry_packet = _load_fixture(SEED_PATH)
    dry_bundle = build_run_bundle(
        mode="default",
        experiment_id="LIVE9-real-official-primary-media-dry-baseline",
        wallclock_seconds=0.0,
        packet=dry_packet,
    )
    _write_bundle(RUN_DIR / "primary_media_dry_bundle_for_comparison.json", dry_bundle)
    comparison = compare_run_bundles(dry_bundle, live_bundle)
    _write_bundle(RUN_DIR / "primary_media_comparison.json", comparison)
    ledger = build_run_ledger_entry(
        experiment_id="LIVE9-real-official-primary-media-live-smoke",
        packet=packet,
        api_calls=api_calls,
        wallclock_seconds=wallclock,
        notes=["LIVE9", f"pilot={PILOT_ID}"],
    )
    _write_bundle(RUN_DIR / "primary_media_ledger_entry.json", ledger.to_dict())

    endpoint_status = {
        "experiment_id": "LIVE9-real-official-primary-media-live-smoke",
        "pilot_id": PILOT_ID,
        "connector_used": "youtube",
        "query": diagnostics.get("query"),
        "live_calls_used": smoke.budget.query_count,
        "source_records_returned": smoke.source_count,
        "verified_artifact_count": len(packet.verified_artifacts),
        "media_artifact_count": media_count,
        "verified_artifact_urls": [artifact.artifact_url for artifact in packet.verified_artifacts],
        "media_relevance_tiers": tier_counts,
        "verdict": result.verdict,
        "risk_flags": result.risk_flags,
        "reason_codes": result.reason_codes,
        "estimated_cost_usd": smoke.budget.estimated_cost_usd,
        "endpoint_v2_achieved": (
            result.verdict == "PRODUCE"
            and tier_counts.get("A", 0) >= 1
            and "produce_based_on_weak_or_uncertain_media" not in result.risk_flags
            and packet.case_identity.identity_confidence == "high"
            and packet.case_identity.outcome_status in {"sentenced", "closed", "convicted"}
        ),
        "blockers": [],
    }
    if not endpoint_status["endpoint_v2_achieved"]:
        if tier_counts.get("A", 0) < 1:
            endpoint_status["blockers"].append("no_verified_tier_a_media")
        if result.verdict != "PRODUCE":
            endpoint_status["blockers"].append("verdict_not_produce")
        if "produce_based_on_weak_or_uncertain_media" in result.risk_flags:
            endpoint_status["blockers"].append("weak_or_uncertain_media_warning_present")
    _write_bundle(RUN_DIR / "primary_media_endpoint_v2_status.json", endpoint_status)
    print(f"LIVE9_ENDPOINT_V2_ACHIEVED={endpoint_status['endpoint_v2_achieved']}")
    print(f"LIVE9_OUTPUT_DIR={RUN_DIR}")
