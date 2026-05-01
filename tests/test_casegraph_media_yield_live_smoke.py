"""LIVE8 — Real-case media-yield pilot live smoke (Endpoint v1 attempt).

Drives a single capped live YouTube search against the
media_case_christa_gail_pike_pilot manifest budget (max_queries=1,
max_results=5), runs ``resolve_youtube_files`` over the returned
SourceRecords, attaches them to the pre-locked CasePacket seed
(identity=high, outcome=convicted), scores it, and writes 7
canonical deliverables under ``autoresearch/.runs/live8/``
(gitignored).

Hard caps enforced by validate_live_run AND the test:

- max_connectors      = 1
- max_queries         = 1
- max_results         = 5
- total_live_calls   <= 1
- no Brave / Firecrawl / LLM / downloads / scraping / transcript
  fetching
- VerifiedArtifacts only from concrete public URLs in returned
  metadata (the central MEDIA1 policy + youtube_files resolver
  enforce this)

Mocked tests run in the default no-live suite (yt_dlp is monkey-
patched). The real-live test is opt-in via
``FLAMEON_RUN_LIVE_CASEGRAPH=1`` and depends on the host having the
``yt_dlp`` package installed.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    LiveRunConfig,
    YouTubeConnector,
    build_live_yield_report,
    build_pilot_validation_scoreboard,
    build_run_ledger_entry,
    build_validation_metrics_report,
    classify_media_url,
    compare_run_bundles,
    resolve_youtube_files,
    run_capped_live_smoke,
    run_pilot_manifest,
    run_validation_manifest,
    score_case_packet,
)
from pipeline2_discovery.casegraph.cli import _load_fixture, _write_bundle, build_run_bundle
from pipeline2_discovery.casegraph.live_safety import DEFAULT_ENV_VAR


ROOT = Path(__file__).resolve().parents[1]
PILOT_MANIFEST = ROOT / "tests" / "fixtures" / "pilot_cases" / "pilot_manifest.json"
SEED_PATH = ROOT / "tests" / "fixtures" / "pilot_cases" / "media_case_christa_gail_pike.json"
RUN_DIR = ROOT / "autoresearch" / ".runs" / "live8"


PILOT_ID = "media_case_christa_gail_pike_pilot"


# --- Fake yt-dlp plumbing for the mocked path -----------------------------


class FakeYDL:
    """Minimal yt_dlp.YoutubeDL stand-in for the mocked path. Returns
    a deterministic fake search payload that mimics the real API shape
    (entries list with id / title / webpage_url / channel)."""

    def __init__(self, ydl_opts=None):
        self.ydl_opts = ydl_opts or {}
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def extract_info(self, query, *, download=False):
        self.calls.append({"query": query, "download": download})
        return {
            "_type": "playlist",
            "entries": [
                {
                    "id": "fakeYTID111",
                    "title": "Christa Gail Pike sentencing hearing - court video",
                    "webpage_url": "https://www.youtube.com/watch?v=fakeYTID111",
                    "channel": "Court TV Archive",
                    "duration": 412,
                    "description": "Sentencing hearing court video for the State of Tennessee v. Christa Gail Pike.",
                },
                {
                    "id": "fakeYTID222",
                    "title": "Pike confession bodycam-style interview",
                    "webpage_url": "https://www.youtube.com/watch?v=fakeYTID222",
                    "channel": "True Crime Archive",
                    "duration": 1820,
                    "description": "Interrogation footage / interview-style video.",
                },
            ],
        }


class _FakeYDLFactory:
    """A class that the YouTubeConnector can use as ydl_cls (passes a
    callable that returns a FakeYDL when called like a constructor)."""

    def __init__(self):
        self.instances = []

    def __call__(self, opts=None):
        instance = FakeYDL(opts)
        self.instances.append(instance)
        return instance


def _seed_youtube_factory(monkeypatch):
    factory_holder = _FakeYDLFactory()

    def factory():
        return YouTubeConnector(ydl_cls=factory_holder)

    import pipeline2_discovery.casegraph.live_smoke as live_smoke_module
    monkeypatch.setitem(live_smoke_module.CONNECTOR_FACTORIES, "youtube", factory)
    return factory_holder


# --- Mocked tests (default-runnable) --------------------------------------


def test_live8_pilot_entry_present_with_youtube_connector():
    manifest = json.loads(PILOT_MANIFEST.read_text(encoding="utf-8"))
    pilot = next(p for p in manifest["pilots"] if p["id"] == PILOT_ID)
    assert pilot["allowed_connectors"] == ["youtube"]


def test_mocked_live8_full_flow_yields_media_artifacts(monkeypatch):
    """End-to-end mocked LIVE8: real-case seed (identity=high,
    outcome=convicted) + YouTube connector returning two video
    results -> resolve_youtube_files graduates them to media
    VerifiedArtifacts. Even with media artifacts present, verdict
    should pass the gate path."""
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    factory = _seed_youtube_factory(monkeypatch)

    packet = _load_fixture(SEED_PATH)
    case_input = packet.input

    config = LiveRunConfig(connector="youtube", max_queries=1, max_results=5)
    smoke = run_capped_live_smoke(case_input, config=config)

    packet.sources.extend(smoke.sources)
    resolve_youtube_files(packet)
    result = score_case_packet(packet)

    # Safety invariants.
    assert smoke.budget.query_count == 1
    for paid in ("brave", "firecrawl"):
        assert smoke.budget.api_calls[paid] == 0
    assert smoke.budget.api_calls["llm"] == 0
    assert smoke.budget.estimated_cost_usd == 0.0

    media_count = sum(
        1 for a in packet.verified_artifacts if a.format in {"video", "audio"}
    )
    document_count = sum(
        1 for a in packet.verified_artifacts if a.format in {"pdf", "document"}
    )

    # The mocked YouTube payload contained two video results that
    # classify as media via MEDIA1 policy.
    assert media_count >= 1
    assert document_count == 0
    # Identity and outcome carry through from the seed.
    assert packet.case_identity.identity_confidence == "high"
    assert packet.case_identity.outcome_status == "convicted"


def test_mocked_live8_zero_paid_calls(monkeypatch):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    _seed_youtube_factory(monkeypatch)

    packet = _load_fixture(SEED_PATH)
    config = LiveRunConfig(connector="youtube", max_queries=1, max_results=5)
    smoke = run_capped_live_smoke(packet.input, config=config)
    assert smoke.budget.api_calls["brave"] == 0
    assert smoke.budget.api_calls["firecrawl"] == 0
    assert smoke.budget.api_calls["llm"] == 0
    assert smoke.budget.estimated_cost_usd == 0.0


def test_mocked_live8_refuses_oversize_max_results(monkeypatch):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    _seed_youtube_factory(monkeypatch)

    packet = _load_fixture(SEED_PATH)
    config = LiveRunConfig(connector="youtube", max_queries=1, max_results=999)
    from pipeline2_discovery.casegraph import LiveRunBlocked
    with pytest.raises(LiveRunBlocked):
        run_capped_live_smoke(packet.input, config=config)


def test_mocked_live8_artifact_classification_uses_media1():
    """The graduated artifact_type should come from the central MEDIA1
    classifier (with the source's hint applied), keeping resolver
    behavior consistent across the codebase."""
    cls = classify_media_url(
        "https://www.youtube.com/watch?v=fakeYTID111",
        hint="court video sentencing",
    )
    assert cls.is_media is True
    assert cls.format == "video"
    # Hint maps "court video" / "sentencing" to court_video artifact_type.
    assert cls.artifact_type == "court_video"


# --- Real-live opt-in test ------------------------------------------------


@pytest.mark.skipif(
    os.environ.get(DEFAULT_ENV_VAR) != "1",
    reason="opt-in via FLAMEON_RUN_LIVE_CASEGRAPH=1",
)
def test_real_live8_media_yield_smoke():
    """LIVE8 -- single capped live YouTube search against the real-
    case media seed (Christa Gail Pike). Asserts safety invariants
    only; actual yield depends on what YouTube returns. All canonical
    deliverables are written under autoresearch/.runs/live8/."""
    pytest.importorskip("yt_dlp", reason="yt_dlp required for the real-live LIVE8 test")

    packet = _load_fixture(SEED_PATH)
    case_input = packet.input

    config = LiveRunConfig(connector="youtube", max_queries=1, max_results=5)
    started = time.perf_counter()
    smoke = run_capped_live_smoke(case_input, config=config)
    smoke_wall = round(time.perf_counter() - started, 4)

    diagnostics = smoke.to_diagnostics()
    print(f"LIVE8_PILOT={PILOT_ID}")
    print(f"LIVE8_CONNECTOR=youtube")
    print(f"LIVE8_QUERY={diagnostics.get('query')}")
    print(f"LIVE8_ENDPOINT={diagnostics.get('endpoint')}")
    print(f"LIVE8_STATUS={diagnostics.get('status_code')}")
    print(f"LIVE8_RESULT_COUNT={diagnostics.get('result_count')}")
    print(f"LIVE8_WALLCLOCK={diagnostics.get('wallclock_seconds')}")
    print(f"LIVE8_API_CALLS={diagnostics.get('api_calls')}")
    print(f"LIVE8_ESTIMATED_COST_USD={diagnostics.get('estimated_cost_usd')}")
    print(f"LIVE8_ERROR={diagnostics.get('error')}")

    # Safety invariants.
    assert smoke.budget.query_count == 1
    assert smoke.budget.api_calls["brave"] == 0
    assert smoke.budget.api_calls["firecrawl"] == 0
    assert smoke.budget.api_calls["llm"] == 0
    assert smoke.budget.estimated_cost_usd == 0.0
    assert smoke.source_count <= 5

    packet.sources.extend(smoke.sources)
    resolve_youtube_files(packet)
    result = score_case_packet(packet)

    media_count = sum(
        1 for a in packet.verified_artifacts if a.format in {"video", "audio"}
    )
    document_count = sum(
        1 for a in packet.verified_artifacts if a.format in {"pdf", "document"}
    )

    print(f"LIVE8_VERIFIED_ARTIFACT_COUNT={len(packet.verified_artifacts)}")
    print(f"LIVE8_MEDIA_ARTIFACT_COUNT={media_count}")
    print(f"LIVE8_DOCUMENT_ARTIFACT_COUNT={document_count}")
    print(
        "LIVE8_VERIFIED_ARTIFACT_URLS="
        + ",".join(a.artifact_url for a in packet.verified_artifacts)
    )
    print(f"LIVE8_VERDICT={result.verdict}")
    print(f"LIVE8_IDENTITY_CONFIDENCE={packet.case_identity.identity_confidence}")
    print(f"LIVE8_OUTCOME_STATUS={packet.case_identity.outcome_status}")

    RUN_DIR.mkdir(parents=True, exist_ok=True)
    api_calls = smoke.budget.to_ledger_summary().get("api_calls", {})

    live_bundle = build_run_bundle(
        mode="live",
        experiment_id="LIVE8-media-yield-pilot-live-smoke",
        wallclock_seconds=smoke_wall,
        packet=packet,
        smoke_diagnostics=diagnostics,
        api_calls=api_calls,
        notes=[
            "LIVE8",
            f"pilot={PILOT_ID}",
            "connector=youtube",
            "max_queries=1",
            "max_results=5",
            "real_case=true",
            "endpoint_v1_attempt",
        ],
    )
    _write_bundle(RUN_DIR / "media_case_live_bundle.json", live_bundle)

    dry_packet = _load_fixture(SEED_PATH)
    dry_bundle = build_run_bundle(
        mode="default",
        experiment_id="LIVE8-media-dry-baseline",
        wallclock_seconds=0.0,
        packet=dry_packet,
    )
    _write_bundle(RUN_DIR / "media_case_dry_bundle_for_comparison.json", dry_bundle)

    comparison = compare_run_bundles(dry_bundle, live_bundle)
    _write_bundle(RUN_DIR / "media_case_comparison.json", comparison)

    ledger = build_run_ledger_entry(
        experiment_id="LIVE8-media-yield-pilot-live-smoke",
        packet=packet,
        api_calls=api_calls,
        wallclock_seconds=smoke_wall,
        notes=["LIVE8", f"pilot={PILOT_ID}"],
    )
    _write_bundle(RUN_DIR / "media_case_ledger_entry.json", ledger.to_dict())

    yield_report = build_live_yield_report(
        [ledger], per_connector_diagnostics=[diagnostics]
    )
    _write_bundle(RUN_DIR / "media_case_live_yield.json", yield_report)

    val = run_validation_manifest()
    pilot_out = run_pilot_manifest()
    scoreboard = build_pilot_validation_scoreboard(
        validation_output=val, pilot_output=pilot_out
    )
    _write_bundle(RUN_DIR / "media_case_scoreboard.json", scoreboard)

    val_metrics = build_validation_metrics_report(val)

    endpoint_v1_status = {
        "experiment_id": "LIVE8-media-yield-pilot-live-smoke",
        "pilot_id": PILOT_ID,
        "real_case": True,
        "endpoint_v1_target": "verified public media artifact URL on a real case",
        "connector_used": "youtube",
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
        "endpoint_v1_conditions": {
            "cond_v1a_real_case_live_bundle_generated": (
                RUN_DIR / "media_case_live_bundle.json"
            ).exists(),
            "cond_v1b_ledger_generated": (RUN_DIR / "media_case_ledger_entry.json").exists(),
            "cond_v1c_dry_vs_live_comparison": (RUN_DIR / "media_case_comparison.json").exists(),
            "cond_v1d_at_least_one_verified_media_url": media_count >= 1,
            "cond_v1e_media_artifact_type_classified": (
                media_count >= 1
                and any(
                    a.artifact_type
                    in {"bodycam", "dashcam", "court_video", "interrogation",
                        "video_footage", "dispatch_911", "audio", "surveillance"}
                    for a in packet.verified_artifacts
                    if a.format in {"video", "audio"}
                )
            ),
            "cond_v1f_identity_evaluated": (
                packet.case_identity.identity_confidence in {"high", "medium", "low"}
            ),
            "cond_v1g_outcome_evaluated": bool(packet.case_identity.outcome_status),
            "cond_v1h_no_safety_gate_breaks": (
                smoke.budget.api_calls["brave"] == 0
                and smoke.budget.api_calls["firecrawl"] == 0
                and smoke.budget.api_calls["llm"] == 0
                and smoke.budget.estimated_cost_usd == 0.0
                and smoke.budget.query_count <= 1
                and smoke.source_count <= 5
            ),
        },
        "endpoint_v0_validation_unchanged": (
            val_metrics["verdict_accuracy"]["accuracy_pct"] == 100.0
            and all(v == 0 for v in val_metrics["guard_counters"].values())
        ),
    }
    endpoint_v1_status["endpoint_v1_fully_achieved"] = all(
        v for v in endpoint_v1_status["endpoint_v1_conditions"].values()
    )
    _write_bundle(RUN_DIR / "media_case_endpoint_v1_status.json", endpoint_v1_status)

    print(
        "LIVE8_ENDPOINT_V1_FULLY_ACHIEVED="
        f"{endpoint_v1_status['endpoint_v1_fully_achieved']}"
    )
    print(f"LIVE8_OUTPUT_DIR={RUN_DIR}")
