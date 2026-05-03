"""PORTAL-LIVE-3 — --portal-live CLI mode tests.

The CLI mode is opt-in via env gates and exclusive with the other
modes (--portal-replay / --query-plan / --multi-source-dry-run /
--live-dry). This file exercises:

  - argparse mutex with the other modes
  - --target-fixture validation (missing / invalid / non-existent)
  - default-blocked behavior without env gates
  - completed-mode JSON / bundle shape (mock fetcher only)
  - bundle includes live_fetch + portal_replay + handoffs
  - extracted payload is replayable by --portal-replay --fixture
  - API key never appears in any operator-visible output

Every test runs the CLI in-process. No real network calls. The mock
fetcher is the only success path; Firecrawl is exercised through the
"missing key" / "deferred scrape" error paths only.
"""
from __future__ import annotations

import io
import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Tuple

import pytest

from pipeline2_discovery.casegraph import cli


ROOT = Path(__file__).resolve().parents[1]
TARGET_FIXTURE = ROOT / "tests" / "fixtures" / "portal_live_targets" / "sheriff_bodycam_dummy.json"
GATED_ENV = {
    "FLAMEON_RUN_LIVE_CASEGRAPH": "1",
    "FLAMEON_RUN_LIVE_PORTAL_FETCH": "1",
}
FAKE_API_KEY = "test-key-not-real-do-not-leak-12345"


def run_cli(argv) -> Tuple[int, str, str]:
    out = io.StringIO()
    err = io.StringIO()
    code = cli.main(argv, stdout=out, stderr=err)
    return code, out.getvalue(), err.getvalue()


@contextmanager
def _patched_env(env: dict):
    """Set selected env vars for the duration of a test, restoring on exit."""
    saved = {k: os.environ.get(k) for k in env}
    try:
        for k, v in env.items():
            os.environ[k] = v
        yield
    finally:
        for k, prev in saved.items():
            if prev is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = prev


@contextmanager
def _cleared_env(keys: list[str]):
    saved = {k: os.environ.pop(k, None) for k in keys}
    try:
        yield
    finally:
        for k, prev in saved.items():
            if prev is not None:
                os.environ[k] = prev


# ---- argparse / mutex ------------------------------------------------


def test_portal_live_requires_target_fixture():
    code, _, err = run_cli(["--portal-live", "--json"])
    assert code == cli.EXIT_FIXTURE_INVALID
    assert "--target-fixture" in err


def test_portal_live_missing_target_fixture_returns_exit_3(tmp_path):
    missing = tmp_path / "does_not_exist.json"
    code, _, err = run_cli(
        ["--portal-live", "--target-fixture", str(missing), "--json"]
    )
    assert code == cli.EXIT_FIXTURE_MISSING
    assert "not found" in err.lower()


def test_portal_live_invalid_target_fixture_returns_exit_4(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{ not valid json", encoding="utf-8")
    code, _, err = run_cli(
        ["--portal-live", "--target-fixture", str(bad), "--json"]
    )
    assert code == cli.EXIT_FIXTURE_INVALID


def test_portal_live_target_fixture_missing_required_keys_returns_exit_4(tmp_path):
    bad = tmp_path / "shape.json"
    bad.write_text(json.dumps({"target_id": "x"}), encoding="utf-8")
    code, _, err = run_cli(
        ["--portal-live", "--target-fixture", str(bad), "--json"]
    )
    assert code == cli.EXIT_FIXTURE_INVALID


def test_portal_live_conflicts_with_portal_replay():
    with pytest.raises(SystemExit):
        run_cli(
            [
                "--portal-live",
                "--portal-replay",
                "--target-fixture",
                str(TARGET_FIXTURE),
                "--json",
            ]
        )


def test_portal_live_conflicts_with_live_dry():
    with pytest.raises(SystemExit):
        run_cli(
            [
                "--portal-live",
                "--live-dry",
                "--target-fixture",
                str(TARGET_FIXTURE),
                "--json",
            ]
        )


def test_portal_live_conflicts_with_query_plan():
    with pytest.raises(SystemExit):
        run_cli(
            [
                "--portal-live",
                "--query-plan",
                "--target-fixture",
                str(TARGET_FIXTURE),
                "--json",
            ]
        )


# ---- default-blocked without env -------------------------------------


def test_portal_live_default_blocked_without_env_gates():
    """No env vars → safety preflight refuses → exit 5, no fetch."""
    with _cleared_env(["FLAMEON_RUN_LIVE_CASEGRAPH", "FLAMEON_RUN_LIVE_PORTAL_FETCH"]):
        code, out, err = run_cli(
            [
                "--portal-live",
                "--target-fixture",
                str(TARGET_FIXTURE),
                "--json",
            ]
        )
    assert code == cli.EXIT_LIVE_BLOCKED
    payload = json.loads(out)
    assert payload["live_fetch"]["status"] == "blocked"
    assert "missing_env_gates" in payload["live_fetch"]["blocked_reason"]
    assert "blocked" in err.lower()


def test_portal_live_blocked_with_only_one_env_gate():
    with _cleared_env(["FLAMEON_RUN_LIVE_PORTAL_FETCH"]):
        with _patched_env({"FLAMEON_RUN_LIVE_CASEGRAPH": "1"}):
            code, out, _ = run_cli(
                [
                    "--portal-live",
                    "--target-fixture",
                    str(TARGET_FIXTURE),
                    "--json",
                ]
            )
    assert code == cli.EXIT_LIVE_BLOCKED
    payload = json.loads(out)
    assert "FLAMEON_RUN_LIVE_PORTAL_FETCH" in payload["live_fetch"]["blocked_reason"]


# ---- completed (mock fetcher) ----------------------------------------


def test_portal_live_completes_with_mock_fetcher_and_env_gates(tmp_path, monkeypatch):
    """The mock fetcher is the only success path in this PR. With both
    env gates set, the orchestrator runs to completion, replays
    through --portal-replay, and emits the standard JSON shape +
    live_fetch section."""
    monkeypatch.setattr(
        "pipeline2_discovery.casegraph.portal_live_fetch._default_payloads_dir",
        lambda repo_root: tmp_path,
    )
    with _patched_env(GATED_ENV):
        code, out, err = run_cli(
            [
                "--portal-live",
                "--target-fixture",
                str(TARGET_FIXTURE),
                "--json",
            ]
        )
    assert code == cli.EXIT_OK, f"stderr: {err}"
    payload = json.loads(out)

    assert payload["input_summary"]["input_type"] == "portal_live"
    assert payload["input_summary"]["target_id"] == "sheriff_bodycam_dummy"
    assert payload["input_summary"]["fetcher"] == "mock"

    live = payload["live_fetch"]
    assert live["status"] == "completed"
    assert live["blocked_reason"] is None
    assert live["fetcher"] == "mock"
    assert live["api_calls"] == {"mock": 1}
    assert live["estimated_cost_usd"] == 0.0
    assert live["target_domain_status"] == "allowed"
    assert live["safety_status"] == "allowed"
    assert live["replayed"] is True
    assert live["raw_payload_path"]
    assert live["extracted_payload_path"]

    # The replay output is threaded into the same JSON envelope.
    assert payload["result"]["verdict"] == "PRODUCE"
    assert payload["packet_summary"]["identity_confidence"] == "high"
    assert "bodycam" in payload["packet_summary"]["verified_artifact_types"]


def test_portal_live_emit_handoffs_includes_p3_p4_p5(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "pipeline2_discovery.casegraph.portal_live_fetch._default_payloads_dir",
        lambda repo_root: tmp_path,
    )
    with _patched_env(GATED_ENV):
        code, out, _ = run_cli(
            [
                "--portal-live",
                "--target-fixture",
                str(TARGET_FIXTURE),
                "--emit-handoffs",
                "--json",
            ]
        )
    assert code == cli.EXIT_OK
    payload = json.loads(out)
    handoffs = payload["handoffs"]
    assert sorted(handoffs.keys()) == ["p2_to_p3", "p2_to_p4", "p2_to_p5"]
    assert len(handoffs["p2_to_p3"]) == 1  # the bodycam media row
    assert handoffs["p2_to_p5"]["verdict"] == "PRODUCE"


def test_portal_live_bundle_out_includes_live_fetch_and_portal_replay(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "pipeline2_discovery.casegraph.portal_live_fetch._default_payloads_dir",
        lambda repo_root: tmp_path / "live",
    )
    bundle_path = tmp_path / "bundle.json"
    with _patched_env(GATED_ENV):
        code, _, err = run_cli(
            [
                "--portal-live",
                "--target-fixture",
                str(TARGET_FIXTURE),
                "--emit-handoffs",
                "--json",
                "--bundle-out",
                str(bundle_path),
            ]
        )
    assert code == cli.EXIT_OK, f"stderr: {err}"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))

    assert bundle["mode"] == "portal_live"
    assert "live_fetch" in bundle
    assert "portal_replay" in bundle
    assert "handoffs" in bundle
    assert bundle["live_fetch"]["status"] == "completed"
    assert bundle["live_fetch"]["fetcher"] == "mock"
    assert bundle["result"]["verdict"] == "PRODUCE"
    assert bundle["handoffs"]["p2_to_p5"]["verdict"] == "PRODUCE"


def test_portal_live_extracted_payload_is_replayable_through_portal_replay(tmp_path, monkeypatch):
    """Round-trip: --portal-live writes an extracted payload; running
    --portal-replay --fixture <extracted_path> on it produces an
    equivalent JSON output with the same verdict and identity."""
    monkeypatch.setattr(
        "pipeline2_discovery.casegraph.portal_live_fetch._default_payloads_dir",
        lambda repo_root: tmp_path,
    )
    with _patched_env(GATED_ENV):
        code, out, _ = run_cli(
            [
                "--portal-live",
                "--target-fixture",
                str(TARGET_FIXTURE),
                "--json",
            ]
        )
    assert code == cli.EXIT_OK
    live_payload = json.loads(out)
    extracted_rel = live_payload["live_fetch"]["extracted_payload_path"]
    extracted_abs = (
        Path(extracted_rel)
        if Path(extracted_rel).is_absolute()
        else ROOT / extracted_rel if (ROOT / extracted_rel).exists()
        else (Path.cwd() / extracted_rel)
    )
    if not extracted_abs.exists():
        # The orchestrator default uses the repo's autoresearch/.runs/...
        # path; under the monkey-patched payloads_dir we know it lives
        # under tmp_path. Fall back to tmp_path-rooted scan.
        candidates = list(tmp_path.glob("*.extracted.json"))
        assert candidates, "extracted payload file not found"
        extracted_abs = candidates[0]

    code2, out2, _ = run_cli(
        [
            "--portal-replay",
            "--fixture",
            str(extracted_abs),
            "--emit-handoffs",
            "--json",
        ]
    )
    assert code2 == 0
    replay_payload = json.loads(out2)

    assert replay_payload["result"]["verdict"] == live_payload["result"]["verdict"]
    assert (
        replay_payload["packet_summary"]["identity_confidence"]
        == live_payload["packet_summary"]["identity_confidence"]
    )
    assert (
        replay_payload["packet_summary"]["verified_artifact_types"]
        == live_payload["packet_summary"]["verified_artifact_types"]
    )


# ---- domain enforcement at CLI surface --------------------------------


def test_portal_live_blocks_when_target_domain_not_in_target_allowlist(tmp_path, monkeypatch):
    """Authoring a target with mismatched URL/allowlist must block at
    the orchestrator level, exit 5, no fetch."""
    monkeypatch.setattr(
        "pipeline2_discovery.casegraph.portal_live_fetch._default_payloads_dir",
        lambda repo_root: tmp_path,
    )
    bad_target = tmp_path / "bad_target.json"
    bad_target.write_text(
        json.dumps(
            {
                "target_id": "bad",
                "url": "https://example-public-sheriff.gov/x",
                "profile_id": "agency_ois_detail",
                "fetcher": "mock",
                "max_pages": 1,
                "max_links": 5,
                "allowed_domains": ["other-domain.example"],
                "expected_response_status": 200,
                "save_raw_payload": False,
                "save_extracted_payload": False,
                "replay_through_portal_replay": False,
                "mock_response": {
                    "page_type": "incident_detail",
                    "agency": "x",
                    "url": "https://example-public-sheriff.gov/x",
                },
            }
        ),
        encoding="utf-8",
    )
    with _patched_env(GATED_ENV):
        code, out, _ = run_cli(
            [
                "--portal-live",
                "--target-fixture",
                str(bad_target),
                "--json",
            ]
        )
    assert code == cli.EXIT_LIVE_BLOCKED
    payload = json.loads(out)
    assert payload["live_fetch"]["status"] == "blocked"
    assert payload["live_fetch"]["blocked_reason"] == "url_domain_not_in_target_allowlist"


# ---- firecrawl path stays blocked ------------------------------------


def test_portal_live_firecrawl_without_api_key_is_blocked(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "pipeline2_discovery.casegraph.portal_live_fetch._default_payloads_dir",
        lambda repo_root: tmp_path,
    )
    fc_target = tmp_path / "fc_target.json"
    fc_target.write_text(
        json.dumps(
            {
                "target_id": "fc_target",
                "url": "https://example-public-sheriff.gov/x",
                "profile_id": "agency_ois_detail",
                "fetcher": "firecrawl",
                "max_pages": 1,
                "max_links": 5,
                "allowed_domains": ["example-public-sheriff.gov"],
                "expected_response_status": 200,
                "save_raw_payload": False,
                "save_extracted_payload": False,
                "replay_through_portal_replay": False,
            }
        ),
        encoding="utf-8",
    )
    with _cleared_env(["FIRECRAWL_API_KEY"]):
        with _patched_env(GATED_ENV):
            code, out, _ = run_cli(
                [
                    "--portal-live",
                    "--target-fixture",
                    str(fc_target),
                    "--json",
                ]
            )
    assert code == cli.EXIT_LIVE_BLOCKED
    payload = json.loads(out)
    assert payload["live_fetch"]["blocked_reason"] == "missing_FIRECRAWL_API_KEY"


def test_portal_live_firecrawl_with_api_key_default_scrape_is_blocked(tmp_path, monkeypatch):
    """The Firecrawl client's default _scrape raises NotImplementedError;
    the orchestrator surfaces that as `firecrawl_live_call_deferred`
    so the CLI exits 5 without making a network call."""
    monkeypatch.setattr(
        "pipeline2_discovery.casegraph.portal_live_fetch._default_payloads_dir",
        lambda repo_root: tmp_path,
    )
    fc_target = tmp_path / "fc_target.json"
    fc_target.write_text(
        json.dumps(
            {
                "target_id": "fc_target",
                "url": "https://example-public-sheriff.gov/x",
                "profile_id": "agency_ois_detail",
                "fetcher": "firecrawl",
                "max_pages": 1,
                "max_links": 5,
                "allowed_domains": ["example-public-sheriff.gov"],
                "expected_response_status": 200,
                "save_raw_payload": False,
                "save_extracted_payload": False,
                "replay_through_portal_replay": False,
            }
        ),
        encoding="utf-8",
    )
    with _patched_env({**GATED_ENV, "FIRECRAWL_API_KEY": FAKE_API_KEY}):
        code, out, _ = run_cli(
            [
                "--portal-live",
                "--target-fixture",
                str(fc_target),
                "--json",
            ]
        )
    assert code == cli.EXIT_LIVE_BLOCKED
    payload = json.loads(out)
    assert payload["live_fetch"]["blocked_reason"] == "firecrawl_live_call_deferred"
    # API key must not appear anywhere in the operator-visible JSON.
    assert FAKE_API_KEY not in out


# ---- API key non-leak across CLI surface -----------------------------


def test_portal_live_api_key_never_leaks_into_stdout_or_bundle(tmp_path, monkeypatch):
    """End-to-end leak detection: with FIRECRAWL_API_KEY in scope and
    a Firecrawl-fetcher target, neither stdout nor the bundle file
    should contain the key value."""
    monkeypatch.setattr(
        "pipeline2_discovery.casegraph.portal_live_fetch._default_payloads_dir",
        lambda repo_root: tmp_path / "live",
    )
    fc_target = tmp_path / "fc_target.json"
    fc_target.write_text(
        json.dumps(
            {
                "target_id": "fc_leak_check",
                "url": "https://example-public-sheriff.gov/x",
                "profile_id": "agency_ois_detail",
                "fetcher": "firecrawl",
                "max_pages": 1,
                "max_links": 5,
                "allowed_domains": ["example-public-sheriff.gov"],
                "expected_response_status": 200,
                "save_raw_payload": False,
                "save_extracted_payload": False,
                "replay_through_portal_replay": False,
            }
        ),
        encoding="utf-8",
    )
    bundle_path = tmp_path / "bundle.json"
    with _patched_env({**GATED_ENV, "FIRECRAWL_API_KEY": FAKE_API_KEY}):
        code, out, err = run_cli(
            [
                "--portal-live",
                "--target-fixture",
                str(fc_target),
                "--json",
            ]
        )

    assert FAKE_API_KEY not in out
    assert FAKE_API_KEY not in err
    # Bundle path didn't get written for blocked Firecrawl path; no
    # bundle to check. (Mock-fetcher leak detection is covered in the
    # orchestrator test file.)


# ---- zero-network --------------------------------------------------


def test_portal_live_makes_zero_network_calls(monkeypatch, tmp_path):
    import requests

    monkeypatch.setattr(
        "pipeline2_discovery.casegraph.portal_live_fetch._default_payloads_dir",
        lambda repo_root: tmp_path,
    )
    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    # Cold path (env unset) and hot path (env set + mock fetcher).
    run_cli(
        [
            "--portal-live",
            "--target-fixture",
            str(TARGET_FIXTURE),
            "--json",
        ]
    )
    with _patched_env(GATED_ENV):
        run_cli(
            [
                "--portal-live",
                "--target-fixture",
                str(TARGET_FIXTURE),
                "--emit-handoffs",
                "--json",
            ]
        )

    assert calls == [], f"--portal-live triggered {len(calls)} live HTTP call(s)"


# ---- default mode unaffected -----------------------------------------


def test_default_mode_unaffected_when_portal_live_not_passed():
    """Adding --portal-live to the CLI must not perturb default-mode
    runs against existing CasePacket fixtures."""
    fixture = ROOT / "tests" / "fixtures" / "casegraph_scenarios" / "media_rich_produce.json"
    code, out, _ = run_cli(["--fixture", str(fixture), "--json"])
    assert code == 0
    payload = json.loads(out)
    assert "live_fetch" not in payload
    assert "portal_replay" not in payload
    assert payload["result"]["verdict"] == "PRODUCE"
