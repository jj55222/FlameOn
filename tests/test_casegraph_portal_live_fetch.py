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


# ---- requests fetcher path (HTML wrapper extraction) -----------------
#
# These tests cover the full requests path end-to-end with the network
# monkey-patched. The fixture sheriff_bodycam_requests_dummy.json has
# fetcher="requests" and no mock_response — the orchestrator must
# call requests.Session.get (mocked), receive the HTML wrapper, and
# extract the embedded agency_ois JSON marker block.


REQUESTS_TARGET_FIXTURE = (
    ROOT / "tests" / "fixtures" / "portal_live_targets" / "sheriff_bodycam_requests_dummy.json"
)


def _agency_ois_marker_html(agency_ois_payload: dict) -> str:
    """Build a minimal HTML body with the canonical
    ``flameon-agency-ois`` JSON marker block embedded inside it."""
    return (
        "<html><head><title>Mock</title></head><body>"
        "<h1>Mock Critical Incident</h1>"
        "<script type=\"application/json\" id=\"flameon-agency-ois\">"
        + json.dumps(agency_ois_payload)
        + "</script>"
        "</body></html>"
    )


def _agency_ois_payload_for_requests_fixture() -> dict:
    return {
        "page_type": "incident_detail",
        "agency": "Example County Sheriff's Office",
        "agency_url_root": "https://example-public-sheriff.gov",
        "url": "https://example-public-sheriff.gov/critical-incidents/2024-EX-002",
        "title": "Critical Incident Briefing 2024-EX-002 (mock-html)",
        "narrative": (
            "On 2024-09-22 Example County Sheriff's Office deputies responded "
            "to a residence in unincorporated Example County. The subject, "
            "Pat Mock, was charged with multiple offenses and pleaded guilty "
            "in 2024 to case number 2024-EX-002. Body-worn camera (BWC) "
            "footage from the responding deputies is included in the briefing "
            "video below."
        ),
        "subjects": ["Pat Mock"],
        "incident_date": "2024-09-22",
        "case_number": "2024-EX-002",
        "outcome_text": "subject pleaded guilty 2024",
        "media_links": [
            {
                "url": "https://example-public-sheriff.gov/critical-incidents/media/2024-EX-002-briefing.mp4",
                "label": "Critical Incident Briefing video (mock-html)",
                "type": "bodycam_briefing",
            }
        ],
        "document_links": [],
        "claims": [],
    }


def _patch_requests_get(monkeypatch, factory):
    """Monkey-patch ``requests.Session.get`` and record calls."""
    import requests

    calls = []

    def fake_get(self, url, *args, **kwargs):
        calls.append({"url": url, "args": args, "kwargs": kwargs})
        return factory(url, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    return calls


class _FakeResponse:
    def __init__(self, *, status_code=200, headers=None, text="", url=""):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text
        self.url = url


def test_run_portal_live_with_requests_fetcher_completes_via_html_marker(monkeypatch, tmp_path):
    """Full requests path: fetcher returns HTML, extractor parses the
    agency_ois marker block, payloads are saved, status=completed."""
    payload = _agency_ois_payload_for_requests_fixture()
    html = _agency_ois_marker_html(payload)
    calls = _patch_requests_get(
        monkeypatch,
        lambda url, **kwargs: _FakeResponse(
            status_code=200,
            headers={"Content-Type": "text/html; charset=utf-8"},
            text=html,
            url=url,
        ),
    )

    target = load_portal_live_target(REQUESTS_TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "completed", (
        f"expected completed; blocked_reason={result.blocked_reason!r}"
    )
    assert len(calls) == 1, "requests fetcher must do exactly one Session.get call"
    assert result.fetch_result.fetcher == "requests"
    assert result.fetch_result.api_calls == {"requests": 1}

    # Raw payload is the HTML wrapper.
    raw = result.fetch_result.raw_payload
    assert raw["content_type"] == "text/html"
    assert raw["status_code"] == 200
    assert "flameon-agency-ois" in raw["text"]

    # Extracted payload is the agency_ois shape from inside the marker.
    assert result.extracted_payload["page_type"] == "incident_detail"
    assert result.extracted_payload["agency"] == "Example County Sheriff's Office"
    assert result.extracted_payload["case_number"] == "2024-EX-002"

    # Both files were saved.
    assert result.raw_payload_path.exists()
    assert result.extracted_payload_path.exists()


def test_run_portal_live_with_requests_extracted_payload_replays_through_portal_replay(
    monkeypatch, tmp_path
):
    """The roundtrip invariant: a saved extracted payload from the
    requests path must be replayable through ``--portal-replay``."""
    from pipeline2_discovery.casegraph.cli import build_portal_replay_payload

    payload = _agency_ois_payload_for_requests_fixture()
    html = _agency_ois_marker_html(payload)
    _patch_requests_get(
        monkeypatch,
        lambda url, **kwargs: _FakeResponse(
            status_code=200,
            headers={"Content-Type": "text/html"},
            text=html,
            url=url,
        ),
    )

    target = load_portal_live_target(REQUESTS_TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )
    assert result.status == "completed"

    with result.extracted_payload_path.open("r", encoding="utf-8") as f:
        replay_input = json.load(f)
    replay = build_portal_replay_payload(
        replay_input,
        fixture_path=result.extracted_payload_path,
        emit_handoffs=True,
    )

    types = set(replay["packet_summary"]["verified_artifact_types"])
    assert "bodycam" in types
    assert replay["packet_summary"]["identity_confidence"] == "high"
    assert replay["result"]["verdict"] == "PRODUCE"
    assert replay["handoffs"]["p2_to_p5"]["verdict"] == "PRODUCE"


def test_run_portal_live_with_requests_blocks_on_html_without_marker(monkeypatch, tmp_path):
    """HTML body without the canonical marker block → extract fails →
    orchestrator surfaces ``blocked_reason="extract_failed:..."``."""
    _patch_requests_get(
        monkeypatch,
        lambda url, **kwargs: _FakeResponse(
            status_code=200,
            headers={"Content-Type": "text/html"},
            text="<html><body>no marker here</body></html>",
            url=url,
        ),
    )

    target = load_portal_live_target(REQUESTS_TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert (result.blocked_reason or "").startswith("extract_failed:")
    assert "html_marker_block_missing" in result.blocked_reason
    # Raw payload is still saved (operator diagnostic), extracted is not.
    assert result.raw_payload_path is not None
    assert result.extracted_payload_path is None


def test_run_portal_live_with_requests_blocks_on_malformed_marker_json(monkeypatch, tmp_path):
    bad_html = (
        "<html><body>"
        "<script type=\"application/json\" id=\"flameon-agency-ois\">"
        "{this-is-not-json"
        "</script>"
        "</body></html>"
    )
    _patch_requests_get(
        monkeypatch,
        lambda url, **kwargs: _FakeResponse(
            status_code=200,
            headers={"Content-Type": "text/html"},
            text=bad_html,
            url=url,
        ),
    )

    target = load_portal_live_target(REQUESTS_TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert "html_marker_block_payload_invalid" in (result.blocked_reason or "")


def test_run_portal_live_with_requests_blocks_on_unexpected_status(monkeypatch, tmp_path):
    """Status code != target.expected_response_status → orchestrator
    surfaces ``unexpected_status_code`` and skips save+extract."""
    _patch_requests_get(
        monkeypatch,
        lambda url, **kwargs: _FakeResponse(
            status_code=403,
            headers={"Content-Type": "text/html"},
            text="<html>denied</html>",
            url=url,
        ),
    )

    target = load_portal_live_target(REQUESTS_TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert "unexpected_status_code:403" in (result.blocked_reason or "")
    assert result.raw_payload_path is None
    assert result.extracted_payload_path is None


def test_run_portal_live_with_requests_blocks_without_env_gates(monkeypatch, tmp_path):
    """No env gates → safety preflight blocks BEFORE the fetcher is
    constructed → ``requests.Session.get`` is never called."""
    calls = _patch_requests_get(
        monkeypatch,
        lambda url, **kwargs: _FakeResponse(
            status_code=200,
            headers={"Content-Type": "text/html"},
            text="",
            url=url,
        ),
    )

    target = load_portal_live_target(REQUESTS_TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env={},  # no gates
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert "missing_env_gates" in (result.blocked_reason or "")
    assert calls == [], "requests.Session.get must not be called when blocked"


def test_run_portal_live_with_requests_blocks_when_safety_blocks(monkeypatch, tmp_path):
    """Even with env gates set, safety preflight failures (e.g. cap
    exceeded) must short-circuit before the requests session is hit."""
    calls = _patch_requests_get(
        monkeypatch,
        lambda url, **kwargs: _FakeResponse(
            status_code=200,
            headers={"Content-Type": "text/html"},
            text="",
            url=url,
        ),
    )

    target = load_portal_live_target(REQUESTS_TARGET_FIXTURE)
    target.max_pages = 999  # blow past the profile cap
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert "max_pages_exceeds_profile_cap" in (result.blocked_reason or "")
    assert calls == [], "safety block must prevent the session from being hit"


def test_run_portal_live_with_requests_surfaces_timeout(monkeypatch, tmp_path):
    import requests

    _patch_requests_get(
        monkeypatch,
        lambda url, **kwargs: (_ for _ in ()).throw(
            requests.exceptions.Timeout("simulated")
        ),
    )

    target = load_portal_live_target(REQUESTS_TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert (result.blocked_reason or "").startswith("timeout:")


# ---- extractor unit (HTML path) --------------------------------------


def test_extract_to_agency_ois_parses_html_marker_block():
    payload = {"page_type": "incident_detail", "agency": "X", "url": "https://example/"}
    html = _agency_ois_marker_html(payload)
    raw = {
        "content_type": "text/html",
        "url": "https://example/",
        "status_code": 200,
        "text": html,
    }
    out = extract_to_agency_ois(raw)
    assert out == payload


def test_extract_to_agency_ois_html_path_rejects_when_marker_missing():
    raw = {
        "content_type": "text/html",
        "url": "https://example/",
        "status_code": 200,
        "text": "<html><body>nothing</body></html>",
    }
    with pytest.raises(ValueError, match="html_marker_block_missing"):
        extract_to_agency_ois(raw)


def test_extract_to_agency_ois_html_path_rejects_empty_marker_block():
    raw = {
        "content_type": "text/html",
        "url": "https://example/",
        "status_code": 200,
        "text": (
            "<script type=\"application/json\" id=\"flameon-agency-ois\">"
            "</script>"
        ),
    }
    with pytest.raises(ValueError, match="html_marker_block_empty"):
        extract_to_agency_ois(raw)


def test_extract_to_agency_ois_html_path_rejects_invalid_json():
    raw = {
        "content_type": "text/html",
        "url": "https://example/",
        "status_code": 200,
        "text": (
            "<script type=\"application/json\" id=\"flameon-agency-ois\">"
            "{nope"
            "</script>"
        ),
    }
    with pytest.raises(ValueError, match="html_marker_block_payload_invalid"):
        extract_to_agency_ois(raw)


def test_extract_to_agency_ois_html_path_rejects_payload_without_required_keys():
    raw = {
        "content_type": "text/html",
        "url": "https://example/",
        "status_code": 200,
        "text": (
            "<script type=\"application/json\" id=\"flameon-agency-ois\">"
            "{\"random\": \"junk\"}"
            "</script>"
        ),
    }
    with pytest.raises(ValueError, match="html_marker_block_payload_invalid"):
        extract_to_agency_ois(raw)


# ---- fetch-only mode (require_extraction=false) ----------------------
#
# When the target opts out of extraction, the orchestrator stops
# after saving the raw payload: no extract attempt, no extracted
# file, no replay. Status is "completed" so the operator can inspect
# the raw HTML and design a real-page extractor in a follow-up PR.


FETCH_ONLY_TARGET_FIXTURE = (
    ROOT / "tests" / "fixtures" / "portal_live_targets" / "sheriff_bodycam_fetch_only_dummy.json"
)


def test_load_portal_live_target_defaults_require_extraction_to_true():
    """Backwards-compat: existing fixtures (which don't carry the new
    field) must still load with require_extraction=True so PR #19
    behavior is unchanged for every prior target."""
    target = load_portal_live_target(TARGET_FIXTURE)
    assert target.require_extraction is True


def test_load_portal_live_target_honors_explicit_false():
    target = load_portal_live_target(FETCH_ONLY_TARGET_FIXTURE)
    assert target.require_extraction is False
    assert target.replay_through_portal_replay is False
    assert target.save_extracted_payload is False
    assert target.save_raw_payload is True


def test_load_portal_live_target_rejects_non_boolean_require_extraction(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(
        json.dumps(
            {
                "target_id": "x",
                "url": "https://x.example",
                "profile_id": "agency_ois_detail",
                "allowed_domains": ["x.example"],
                "require_extraction": "yes",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="require_extraction must be a boolean"):
        load_portal_live_target(bad)


def test_run_portal_live_fetch_only_mock_target_completes_without_extraction(tmp_path):
    target = load_portal_live_target(FETCH_ONLY_TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "completed"
    assert result.blocked_reason is None
    assert result.fetch_result is not None
    assert result.fetch_result.error is None
    # Raw saved.
    assert result.raw_payload_path is not None
    assert result.raw_payload_path.exists()
    # Extracted skipped.
    assert result.extracted_payload is None
    assert result.extracted_payload_path is None


def test_run_portal_live_fetch_only_requests_html_without_marker_completes(monkeypatch, tmp_path):
    """The whole point of fetch-only mode: HTML without the
    flameon-agency-ois marker must NOT block. The raw HTML lands on
    disk and the orchestrator reports success."""
    import requests

    captured_urls: list[str] = []

    def fake_get(self, url, *args, **kwargs):
        captured_urls.append(url)

        class _R:
            status_code = 200
            headers = {"Content-Type": "text/html"}
            text = "<html><body>arbitrary real-page HTML, no marker block here</body></html>"

            def __init__(self, u):
                self.url = u

        return _R(url)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    fetch_only_target_path = tmp_path / "requests_fetch_only_target.json"
    fetch_only_target_path.write_text(
        json.dumps(
            {
                "target_id": "requests_fetch_only_synth",
                "url": "https://example-public-sheriff.gov/critical-incidents/2024-EX-FETCH-ONLY",
                "profile_id": "agency_ois_detail",
                "fetcher": "requests",
                "max_pages": 1,
                "max_links": 5,
                "allowed_domains": ["example-public-sheriff.gov"],
                "expected_response_status": 200,
                "save_raw_payload": True,
                "save_extracted_payload": False,
                "replay_through_portal_replay": False,
                "require_extraction": False,
            }
        ),
        encoding="utf-8",
    )
    target = load_portal_live_target(fetch_only_target_path)
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "completed", (
        f"expected completed; blocked_reason={result.blocked_reason!r}"
    )
    assert len(captured_urls) == 1, "exactly one Session.get call expected"
    assert result.fetch_result is not None
    assert result.fetch_result.error is None
    assert result.raw_payload_path is not None
    assert result.raw_payload_path.exists()
    assert result.extracted_payload is None
    assert result.extracted_payload_path is None
    saved = json.loads(result.raw_payload_path.read_text(encoding="utf-8"))
    assert saved["status_code"] == 200
    assert "no marker block here" in saved["text"]


def test_run_portal_live_extraction_required_html_without_marker_still_blocks(monkeypatch, tmp_path):
    """Regression guard: with require_extraction=true (the default),
    HTML without the marker must still block exactly as PR #19 set
    up. The new flag must not silently weaken the existing path."""
    import requests

    def fake_get(self, url, *args, **kwargs):
        class _R:
            status_code = 200
            headers = {"Content-Type": "text/html"}
            text = "<html><body>no marker</body></html>"

            def __init__(self, u):
                self.url = u

        return _R(url)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    target_path = tmp_path / "extraction_required_target.json"
    target_path.write_text(
        json.dumps(
            {
                "target_id": "extraction_required_synth",
                "url": "https://example-public-sheriff.gov/critical-incidents/2024-EX-REQ",
                "profile_id": "agency_ois_detail",
                "fetcher": "requests",
                "max_pages": 1,
                "max_links": 5,
                "allowed_domains": ["example-public-sheriff.gov"],
                "expected_response_status": 200,
                "save_raw_payload": True,
                "save_extracted_payload": True,
                "replay_through_portal_replay": True,
                # require_extraction omitted → defaults to True
            }
        ),
        encoding="utf-8",
    )
    target = load_portal_live_target(target_path)
    assert target.require_extraction is True
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert result.status == "blocked"
    assert (result.blocked_reason or "").startswith("extract_failed:")
    assert "html_marker_block_missing" in (result.blocked_reason or "")


def test_build_live_fetch_section_fetch_only_reports_replayed_false(tmp_path):
    target = load_portal_live_target(FETCH_ONLY_TARGET_FIXTURE)
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )
    section = build_live_fetch_section(result)

    assert section["status"] == "completed"
    assert section["require_extraction"] is False
    assert section["replayed"] is False
    assert section["raw_payload_path"] is not None
    assert section["extracted_payload_path"] is None


def test_build_live_fetch_section_extraction_mode_replayed_true_when_extracted_present(tmp_path):
    """Backwards-compat: with require_extraction=true and a successful
    extract+replay, replayed=True (locks the existing PR #19 contract)."""
    target = load_portal_live_target(TARGET_FIXTURE)  # mock fetcher, full path
    result = run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )
    section = build_live_fetch_section(result)

    assert section["status"] == "completed"
    assert section["require_extraction"] is True
    assert section["replayed"] is True


def test_run_portal_live_fetch_only_makes_zero_real_network_calls(monkeypatch, tmp_path):
    """The fetch-only mock target uses the mock fetcher; assert the
    requests session is never touched."""
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return None

    monkeypatch.setattr(requests.Session, "get", fake_get)

    target = load_portal_live_target(FETCH_ONLY_TARGET_FIXTURE)
    run_portal_live(
        target,
        env=GATED_ENV,
        repo_root=ROOT,
        payloads_dir=tmp_path,
    )

    assert calls == []
