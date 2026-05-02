"""FIRE2 - gated known-URL live smoke skeleton tests.

The skeleton is default-off. These tests exercise gate and safety
diagnostics only; no Firecrawl/request fetch is performed.
"""
import json
from pathlib import Path

from pipeline2_discovery.casegraph.firecrawl_safety import (
    KnownUrlLiveSmokeTarget,
    evaluate_known_url_live_smoke_skeleton,
    known_url_live_smoke_to_jsonable,
)


ROOT = Path(__file__).resolve().parents[1]
GATED_ENV = {
    "FLAMEON_RUN_LIVE_CASEGRAPH": "1",
    "FLAMEON_RUN_LIVE_PORTAL_FETCH": "1",
}


def _target(**overrides):
    base = {
        "target_id": "phoenix_ois_known_url",
        "url": "https://www.phoenix.gov/police/critical-incidents/2024-OIS-050",
        "profile_id": "agency_ois_detail",
        "fetcher": "firecrawl",
        "max_pages": 1,
        "max_links": 5,
    }
    base.update(overrides)
    return KnownUrlLiveSmokeTarget(**base)


def test_default_live_smoke_skeleton_skips_without_env_gates():
    decision = evaluate_known_url_live_smoke_skeleton(_target(), env={}, repo_root=ROOT)

    assert decision.execution_status == "skipped"
    assert decision.live_call_allowed is False
    assert decision.safety_preflight_status == "not_run"
    assert "FLAMEON_RUN_LIVE_CASEGRAPH" in decision.required_env_vars
    assert "FLAMEON_RUN_LIVE_PORTAL_FETCH" in decision.required_env_vars


def test_gated_skeleton_reports_ready_without_fetching():
    decision = evaluate_known_url_live_smoke_skeleton(_target(), env=GATED_ENV, repo_root=ROOT)

    assert decision.execution_status == "ready_for_future_live_fetch"
    assert decision.live_call_allowed is True
    assert decision.safety_preflight_status == "allowed"
    assert decision.safety_decision["fetch_allowed"] is True


def test_gated_skeleton_blocks_over_cap_target():
    decision = evaluate_known_url_live_smoke_skeleton(
        _target(max_pages=99),
        env=GATED_ENV,
        repo_root=ROOT,
    )

    assert decision.execution_status == "blocked"
    assert decision.live_call_allowed is False
    assert decision.safety_preflight_status == "blocked"
    assert decision.skip_reason == "max_pages_exceeds_profile_cap"


def test_gated_skeleton_blocks_private_login_target():
    decision = evaluate_known_url_live_smoke_skeleton(
        _target(url="https://www.phoenix.gov/login?redirect=/police/critical-incidents/2024-OIS-050"),
        env=GATED_ENV,
        repo_root=ROOT,
    )

    assert decision.execution_status == "blocked"
    assert decision.skip_reason == "private_or_login_not_allowed"
    assert decision.live_call_allowed is False


def test_live_smoke_decision_is_json_serializable():
    decision = evaluate_known_url_live_smoke_skeleton(_target(), env={}, repo_root=ROOT)

    encoded = json.dumps(known_url_live_smoke_to_jsonable(decision), sort_keys=True)
    decoded = json.loads(encoded)

    assert decoded["target_id"] == "phoenix_ois_known_url"
    assert decoded["execution_status"] == "skipped"


def test_live_smoke_skeleton_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    evaluate_known_url_live_smoke_skeleton(_target(), env=GATED_ENV, repo_root=ROOT)
    assert calls == []
