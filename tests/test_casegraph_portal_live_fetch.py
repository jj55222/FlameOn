"""PORTAL-LIVE-2 — Portal-live orchestrator tests.

Exercises the end-to-end flow: target fixture → safety preflight →
target-domain check → mocked fetch → save raw → extract → save
extracted, plus the replay-roundtrip equivalence with the existing
``--portal-replay`` path. Every test is offline; the mock fetcher is
the only fully-implemented client. Firecrawl / requests stay
unblocked, with API-key leak detection across every output surface.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph.portal_fetch_client import (
    FirecrawlFetchClient,
    MockFetchClient,
    PortalFetchResult,
    PortalLiveTarget,
)
from pipeline2_discovery.casegraph.portal_live_fetch import (
    PortalLiveResult,
    build_live_fetch_section,
    extract_to_agency_ois,
    load_portal_live_target,
    run_portal_live,
)


ROOT = Path(__file__).resolve().parents[1]
TARGET_FIXTURE = ROOT / "tests" / "fixtures" / "portal_live_targets" / "sheriff_bodycam_dummy.json"
GATED_ENV = {
    "FLAMEON_RUN_LIVE_CASEGRAPH": "1",
    "FLAMEON_RUN_LIVE_PORTAL_FETCH": "1",
}
FAKE_API_KEY = "test-key-not-real-do-not-leak-12345"


# ---- target fixture loader -------------------------------------------


def test_load_portal_live_target_parses_sheriff_dummy_fixture():
    target = load_portal_live_target(TARGET_FIXTURE)

    assert target.target_id == "sheriff_bodycam_dummy"
    assert target.url.startswith("https://example-public-sheriff.gov/")
    assert target.profile_id == "agency_ois_detail"
    assert target.fetcher == "mock"
    assert target.allowed_domains == ["example-public-sheriff.gov"]
    assert target.max_pages == 1
    assert target.max_links == 5
    assert target.expected_response_status == 200
    assert target.save_raw_payload is True
    assert target.save_extracted_payload is True
    assert target.replay_through_portal_replay is True
    assert isinstance(target.mock_response, dict)
    assert target.mock_response.get("page_type") == "incident_detail"


def test_load_portal_live_target_rejects_missing_required_keys(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"target_id": "x"}), encoding="utf-8")

    with pytest.raises(ValueError, match="missing required keys"):
        load_portal_live_target(bad)


def test_load_portal_live_target_rejects_non_object_root(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps([1, 2, 3]), encoding="utf-8")

    with pytest.raises(ValueError, match="root is not a JSON object"):
        load_portal_live_target(bad)


def test_load_portal_live_target_rejects_non_list_allowed_domains(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(
        json.dumps(
            {
                "target_id": "x",
                "url": "https://x.example",
                "profile_id": "agency_ois_detail",
                "allowed_domains": "not-a-list",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="allowed_domains must be a list"):
        load_portal_live_target(bad)


# ---- preflight & domain blocks ---------------------------------------


def test_run_portal_live_blocks_without_env_gates(tmp_path):
    target = load_portal_live_target(TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env={},  # no gates
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert result.blocked_reason is not None
    assert "missing_env_gates" in result.blocked_reason
    assert result.fetch_result is None
    assert result.extracted_payload is None
    assert result.raw_payload_path is None
    assert result.extracted_payload_path is None


def test_run_portal_live_blocks_when_only_one_env_gate_set(tmp_path):
    target = load_portal_live_target(TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env={"FLAMEON_RUN_LIVE_CASEGRAPH": "1"},  # missing portal gate
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert "FLAMEON_RUN_LIVE_PORTAL_FETCH" in (result.blocked_reason or "")


def test_run_portal_live_blocks_when_target_allowed_domains_empty(tmp_path):
    target = load_portal_live_target(TARGET_FIXTURE)
    target.allowed_domains = []  # simulate operator forgetting allowlist
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert result.blocked_reason == "target_allowed_domains_empty"
    assert result.target_domain_status == "target_allowed_domains_empty"


def test_run_portal_live_blocks_when_url_domain_not_in_target_allowlist(tmp_path):
    target = load_portal_live_target(TARGET_FIXTURE)
    target.allowed_domains = ["other-domain.example"]
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert result.blocked_reason == "url_domain_not_in_target_allowlist"


def test_run_portal_live_blocks_over_cap_target(tmp_path):
    target = load_portal_live_target(TARGET_FIXTURE)
    target.max_pages = 99  # exceeds profile cap
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    # The FIRE1 safety preflight surfaces this as max_pages_exceeds_profile_cap.
    assert "max_pages_exceeds_profile_cap" in (result.blocked_reason or "")


# ---- fetch errors ----------------------------------------------------


def test_run_portal_live_blocks_when_firecrawl_missing_key(tmp_path):
    target = load_portal_live_target(TARGET_FIXTURE)
    target.fetcher = "firecrawl"
    # GATED_ENV has the live gates but no FIRECRAWL_API_KEY.
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert result.blocked_reason == "missing_FIRECRAWL_API_KEY"


def test_run_portal_live_blocks_when_firecrawl_default_scrape_is_deferred(tmp_path):
    target = load_portal_live_target(TARGET_FIXTURE)
    target.fetcher = "firecrawl"
    env = {**GATED_ENV, "FIRECRAWL_API_KEY": FAKE_API_KEY}
    result = run_portal_live(
        target,
        env=env,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert result.blocked_reason == "firecrawl_live_call_deferred"


def test_run_portal_live_blocks_on_unexpected_status_code(tmp_path):
    """An injected fetch_client returns 500 → orchestrator blocks
    before saving anything."""

    class _BadStatusClient:
        def fetch(self, target):
            return PortalFetchResult(
                raw_payload={"page_type": "incident_detail"},
                status_code=500,
                fetcher="mock",
                wallclock_seconds=0.0,
                api_calls={"mock": 1},
                estimated_cost_usd=0.0,
                error=None,
            )

    target = load_portal_live_target(TARGET_FIXTURE)
    result = run_portal_live(
        target,
        fetch_client=_BadStatusClient(),
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert "unexpected_status_code:500" in (result.blocked_reason or "")
    assert result.raw_payload_path is None  # nothing saved on failure


def test_run_portal_live_blocks_when_extracted_payload_is_not_agency_ois(tmp_path):
    """A payload with no agency_ois shape keys is rejected by the
    extractor — the orchestrator surfaces ``extract_failed:...``."""

    class _BadShapeClient:
        def fetch(self, target):
            return PortalFetchResult(
                raw_payload={"random": "junk"},
                status_code=200,
                fetcher="mock",
                wallclock_seconds=0.0,
                api_calls={"mock": 1},
                estimated_cost_usd=0.0,
                error=None,
            )

    target = load_portal_live_target(TARGET_FIXTURE)
    result = run_portal_live(
        target,
        fetch_client=_BadShapeClient(),
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert (result.blocked_reason or "").startswith("extract_failed:")
    # Raw payload was still saved (it's the operator's diagnostic).
    assert result.raw_payload_path is not None
    # Extracted payload was NOT saved.
    assert result.extracted_payload_path is None


# ---- happy path ------------------------------------------------------


def test_run_portal_live_completes_with_mock_target_and_saves_payloads(tmp_path):
    target = load_portal_live_target(TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "completed", (
        f"expected completed; got blocked_reason={result.blocked_reason!r}"
    )
    assert result.fetch_result is not None
    assert result.fetch_result.error is None
    assert result.fetch_result.api_calls == {"mock": 1}
    assert result.fetch_result.estimated_cost_usd == 0.0

    assert result.raw_payload_path is not None
    assert result.raw_payload_path.exists()
    assert result.raw_payload_path.parent == tmp_path

    assert result.extracted_payload_path is not None
    assert result.extracted_payload_path.exists()
    assert result.extracted_payload_path.parent == tmp_path

    assert result.extracted_payload is not None
    assert result.extracted_payload["page_type"] == "incident_detail"
    assert result.extracted_payload["agency"] == "Example County Sheriff's Office"


def test_run_portal_live_writes_raw_and_extracted_with_target_id_in_filename(tmp_path):
    target = load_portal_live_target(TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
        timestamp_provider=lambda: "2026-05-03T12-34-56Z",
    )

    raw = result.raw_payload_path
    extracted = result.extracted_payload_path
    assert raw and extracted

    assert "2026-05-03T12-34-56Z" in raw.name
    assert "sheriff_bodycam_dummy" in raw.name
    assert raw.name.endswith(".raw.json")
    assert extracted.name.endswith(".extracted.json")


def test_run_portal_live_skips_save_when_target_opts_out(tmp_path):
    target = load_portal_live_target(TARGET_FIXTURE)
    target.save_raw_payload = False
    target.save_extracted_payload = False
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "completed"
    assert result.raw_payload_path is None
    assert result.extracted_payload_path is None
    assert result.extracted_payload is not None  # still in memory


# ---- replay roundtrip equivalence ------------------------------------


def test_extracted_payload_can_be_replayed_through_portal_replay(tmp_path):
    """The whole point of saving the extracted payload to disk: a
    follow-up ``--portal-replay --fixture <extracted>`` invocation
    must produce a valid result. Here we exercise it via the
    Python API to keep the test self-contained."""
    from pipeline2_discovery.casegraph.cli import build_portal_replay_payload

    target = load_portal_live_target(TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )
    assert result.status == "completed"
    assert result.extracted_payload_path is not None

    # Reload the saved file from disk and feed it through the
    # offline replay path. Must yield a complete payload.
    with result.extracted_payload_path.open("r", encoding="utf-8") as f:
        replay_input = json.load(f)
    replay = build_portal_replay_payload(
        replay_input,
        fixture_path=result.extracted_payload_path,
        emit_handoffs=True,
    )

    # The replay output mirrors --portal-replay shape.
    for key in (
        "input_summary",
        "packet_summary",
        "result",
        "report",
        "ledger_entry",
        "portal_replay",
        "handoffs",
    ):
        assert key in replay, f"missing key {key!r} from replay output"

    # Mock target's payload graduates a bodycam media link, so the
    # replay should produce a verified bodycam.
    types = set(replay["packet_summary"]["verified_artifact_types"])
    assert "bodycam" in types
    # Identity should reach high (agency, incident_date, case_number all populated).
    assert replay["packet_summary"]["identity_confidence"] == "high"


# ---- live_fetch JSON section -----------------------------------------


def test_build_live_fetch_section_completed_includes_all_keys(tmp_path):
    target = load_portal_live_target(TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )
    section = build_live_fetch_section(result)

    for key in (
        "target_id",
        "url",
        "profile_id",
        "fetcher",
        "raw_payload_path",
        "extracted_payload_path",
        "status",
        "blocked_reason",
        "status_code",
        "estimated_cost_usd",
        "api_calls",
        "wallclock_seconds",
        "safety_status",
        "target_domain_status",
        "replayed",
    ):
        assert key in section, f"missing live_fetch key {key!r}"

    assert section["status"] == "completed"
    assert section["target_id"] == "sheriff_bodycam_dummy"
    assert section["fetcher"] == "mock"
    assert section["api_calls"] == {"mock": 1}
    assert section["estimated_cost_usd"] == 0.0
    assert section["status_code"] == 200
    assert section["target_domain_status"] == "allowed"
    assert section["safety_status"] == "allowed"
    assert section["replayed"] is True
    assert section["blocked_reason"] is None


def test_build_live_fetch_section_blocked_includes_reason(tmp_path):
    target = load_portal_live_target(TARGET_FIXTURE)
    result = run_portal_live(target, env={}, repo_root=ROOT, payloads_dir=tmp_path)
    section = build_live_fetch_section(result)

    assert section["status"] == "blocked"
    assert section["blocked_reason"] is not None
    assert section["replayed"] is False


# ---- API key non-leakage scan ----------------------------------------


def test_api_key_never_appears_in_orchestrator_outputs(tmp_path):
    """End-to-end leak detection. Run the orchestrator with both env
    gates set AND a fake FIRECRAWL_API_KEY in scope; assert the key
    value is absent from every artifact (result dict, live_fetch
    section, raw payload file, extracted payload file)."""
    target = load_portal_live_target(TARGET_FIXTURE)
    target.fetcher = "firecrawl"
    env = {**GATED_ENV, "FIRECRAWL_API_KEY": FAKE_API_KEY}

    # Use a patched Firecrawl client that successfully returns a payload.
    class _PatchedFirecrawl(FirecrawlFetchClient):
        def _scrape(self, url, *, max_pages):
            return {
                "page_type": "incident_detail",
                "agency": "Example County Sheriff's Office",
                "url": url,
                "title": "Patched",
                "narrative": "patched narrative",
                "subjects": ["Pat Mock"],
                "incident_date": "2024-09-22",
                "case_number": "2024-EX-001",
                "outcome_text": "subject pleaded guilty 2024",
                "media_links": [],
                "document_links": [],
                "claims": [],
                "status_code": 200,
            }

    client = _PatchedFirecrawl(api_key=FAKE_API_KEY)
    result = run_portal_live(
        target,
        fetch_client=client,
        env=env,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )
    assert result.status == "completed"

    section = build_live_fetch_section(result)
    section_text = json.dumps(section)
    result_text = json.dumps(result.to_dict())

    raw_text = result.raw_payload_path.read_text(encoding="utf-8")
    extracted_text = result.extracted_payload_path.read_text(encoding="utf-8")

    for surface_name, surface_text in (
        ("live_fetch_section", section_text),
        ("result_dict", result_text),
        ("raw_payload_file", raw_text),
        ("extracted_payload_file", extracted_text),
    ):
        assert FAKE_API_KEY not in surface_text, (
            f"API key leaked into {surface_name}"
        )


# ---- extractor unit ---------------------------------------------------


def test_extract_to_agency_ois_passes_through_agency_shape():
    raw = {"page_type": "incident_detail", "agency": "X"}
    out = extract_to_agency_ois(raw)
    assert out == raw
    assert out is not raw  # returns a copy


def test_extract_to_agency_ois_accepts_portal_profile_id_only():
    raw = {"portal_profile_id": "agency_ois_detail"}
    out = extract_to_agency_ois(raw)
    assert out == raw


def test_extract_to_agency_ois_accepts_source_records_list():
    raw = {"source_records": [{"source_id": "x"}]}
    out = extract_to_agency_ois(raw)
    assert out == raw


def test_extract_to_agency_ois_rejects_payloads_without_known_keys():
    with pytest.raises(ValueError, match="agency_ois shape required"):
        extract_to_agency_ois({"random": "junk"})


# ---- zero-network across orchestrator --------------------------------


def test_orchestrator_makes_zero_network_calls(monkeypatch, tmp_path):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    target = load_portal_live_target(TARGET_FIXTURE)
    # Two paths — one blocked, one completed — neither must touch HTTP.
    run_portal_live(target, env={}, repo_root=ROOT, payloads_dir=tmp_path)
    run_portal_live(target, env=GATED_ENV, repo_root=ROOT, payloads_dir=tmp_path)

    assert calls == [], f"orchestrator triggered {len(calls)} live HTTP call(s)"
