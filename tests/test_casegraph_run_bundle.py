"""PIPE5 — CLI run bundle tests.

Asserts that the CaseGraph CLI's ``--bundle-out`` flag:

- writes a single canonical JSON bundle that includes every required
  top-level section (input_summary, query_plan, connector_summary,
  identity, outcome, artifact_claims, verified_artifacts, result,
  actionability_report, ledger_entry, live_yield_report, next_actions,
  risk_flags, plus mode/experiment_id/wallclock_seconds)
- works in default mode (CasePacket fixture), multi-source-dry-run
  mode (structured-row fixture), and query-plan mode
- by default refuses non-gitignored repo paths with exit code 6
- accepts paths inside ``autoresearch/.runs`` / ``autoresearch/.tmp``
  / ``autoresearch/.artifacts`` / ``autoresearch/.cache`` /
  ``autoresearch/.logs`` (all gitignored)
- accepts paths outside the repo entirely (e.g. ``tmp_path``)
- accepts unsafe paths only when ``--allow-unsafe-bundle-path`` is set
- never makes any network call
- never graduates claim-only sources to VerifiedArtifacts
- never marks document-only fixtures as PRODUCE
- never marks the structured-row fixture (alone) as PRODUCE
- writes nothing to disk when fixture loading fails
"""
from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import cli


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "casegraph_scenarios"
STRUCTURED_FIXTURE_DIR = ROOT / "tests" / "fixtures" / "structured_inputs"
WAPO_FIXTURE = str(STRUCTURED_FIXTURE_DIR / "wapo_uof_complete.json")


REQUIRED_BUNDLE_KEYS = (
    "experiment_id",
    "mode",
    "wallclock_seconds",
    "input_summary",
    "query_plan",
    "connector_summary",
    "multi_source_summary",
    "smoke_diagnostics",
    "identity",
    "outcome",
    "artifact_claims",
    "verified_artifacts",
    "result",
    "actionability_report",
    "live_yield_report",
    "ledger_entry",
    "next_actions",
    "risk_flags",
)


def run_cli(argv):
    out = io.StringIO()
    err = io.StringIO()
    code = cli.main(argv, stdout=out, stderr=err)
    return code, out.getvalue(), err.getvalue()


def _read_bundle(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_default_mode_writes_bundle_to_safe_tmp_path(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, _, err = run_cli(
        ["--fixture", fixture, "--json", "--bundle-out", str(bundle_path)]
    )
    assert code == 0, f"non-zero exit: {err}"
    assert bundle_path.exists(), "bundle file was not written"
    bundle = _read_bundle(bundle_path)
    assert bundle["mode"] == "default"
    assert bundle["experiment_id"] == "PIPE1-cli-dry-run"


def test_bundle_includes_every_required_top_level_key(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, _, err = run_cli(
        ["--fixture", fixture, "--json", "--bundle-out", str(bundle_path)]
    )
    assert code == 0, err
    bundle = _read_bundle(bundle_path)
    for key in REQUIRED_BUNDLE_KEYS:
        assert key in bundle, f"missing required bundle key: {key!r}"


def test_default_mode_bundle_has_identity_outcome_and_verified_artifacts(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, _, _ = run_cli(
        ["--fixture", fixture, "--json", "--bundle-out", str(bundle_path)]
    )
    assert code == 0
    bundle = _read_bundle(bundle_path)
    identity = bundle["identity"]
    assert identity is not None
    assert identity["defendant_names"] == ["Min Jian Guan"]
    assert identity["identity_confidence"] == "high"
    outcome = bundle["outcome"]
    assert outcome is not None
    assert outcome["outcome_status"] == "sentenced"
    artifacts = bundle["verified_artifacts"]
    assert isinstance(artifacts, list)
    assert {a["artifact_type"] for a in artifacts} >= {"bodycam", "dispatch_911", "docket_docs"}
    assert bundle["result"]["verdict"] == "PRODUCE"


def test_document_only_hold_bundle_has_documents_no_media_and_hold_verdict(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    fixture = str(FIXTURE_DIR / "document_only_hold.json")
    code, _, _ = run_cli(
        ["--fixture", fixture, "--json", "--bundle-out", str(bundle_path)]
    )
    assert code == 0
    bundle = _read_bundle(bundle_path)
    artifacts = bundle["verified_artifacts"]
    assert len(artifacts) >= 1, "document-only fixture should still have document artifacts"
    types = {a["artifact_type"] for a in artifacts}
    assert "bodycam" not in types and "interrogation" not in types
    assert bundle["result"]["verdict"] == "HOLD"


def test_claim_only_hold_bundle_has_artifact_claims_but_no_verified_artifacts(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    fixture = str(FIXTURE_DIR / "claim_only_hold.json")
    code, _, _ = run_cli(
        ["--fixture", fixture, "--json", "--bundle-out", str(bundle_path)]
    )
    assert code == 0
    bundle = _read_bundle(bundle_path)
    assert bundle["verified_artifacts"] == []
    # Either claims or sources alone — but never a graduated VerifiedArtifact.
    assert bundle["result"]["verdict"] == "HOLD"


def test_multi_source_dry_run_bundle_has_query_plan_and_multi_source_sections(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    code, _, err = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "courtlistener,muckrock,documentcloud",
            "--json",
            "--bundle-out",
            str(bundle_path),
        ]
    )
    assert code == 0, err
    bundle = _read_bundle(bundle_path)
    assert bundle["mode"] == "multi_source_dry_run"
    plan = bundle["query_plan"]
    assert plan is not None
    assert plan["connector_count"] >= 1
    multi = bundle["multi_source_summary"]
    assert multi is not None
    assert multi["connectors"] == ["courtlistener", "muckrock", "documentcloud"]
    assert multi["total_source_records"] == 0
    assert multi["total_verified_artifacts"] == 0
    assert multi["total_estimated_cost_usd"] == 0.0


def test_multi_source_dry_run_bundle_does_not_produce_from_fixture_alone(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    code, _, _ = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "courtlistener,muckrock,documentcloud",
            "--json",
            "--bundle-out",
            str(bundle_path),
        ]
    )
    assert code == 0
    bundle = _read_bundle(bundle_path)
    # The structured row alone never produces a verified artifact and
    # never graduates to PRODUCE — both must hold in the bundle.
    assert bundle["verified_artifacts"] == []
    assert bundle["result"]["verdict"] != "PRODUCE"


def test_query_plan_mode_bundle_has_plan_but_no_packet_sections(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    code, _, err = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--query-plan",
            "--json",
            "--bundle-out",
            str(bundle_path),
        ]
    )
    assert code == 0, err
    bundle = _read_bundle(bundle_path)
    assert bundle["mode"] == "query_plan"
    assert bundle["query_plan"] is not None
    # Without a packet, identity/outcome are explicitly null.
    assert bundle["identity"] is None
    assert bundle["outcome"] is None
    assert bundle["result"] is None
    assert bundle["actionability_report"] is None
    assert bundle["verified_artifacts"] == []
    assert bundle["artifact_claims"] == []


def test_bundle_out_makes_zero_network_calls(monkeypatch, tmp_path):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    bundle_path = tmp_path / "bundle.json"
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, _, err = run_cli(
        ["--fixture", fixture, "--json", "--bundle-out", str(bundle_path)]
    )
    assert code == 0, err

    bundle_path2 = tmp_path / "bundle2.json"
    code2, _, err2 = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "courtlistener,muckrock,documentcloud",
            "--json",
            "--bundle-out",
            str(bundle_path2),
        ]
    )
    assert code2 == 0, err2
    assert calls == [], (
        f"--bundle-out triggered {len(calls)} live HTTP call(s); must be no-live"
    )


def test_bundle_out_refuses_unsafe_repo_path(tmp_path):
    """A path inside the repo but NOT under a gitignored artifact dir
    must be refused with exit code 6, and no file written."""
    unsafe = ROOT / "tests" / "_pipe5_unsafe_bundle.json"
    # Make sure it doesn't exist beforehand.
    if unsafe.exists():
        unsafe.unlink()
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    try:
        code, out, err = run_cli(
            ["--fixture", fixture, "--json", "--bundle-out", str(unsafe)]
        )
        assert code == 6, f"expected exit 6 for unsafe path, got {code}; err={err}"
        assert out == ""
        assert "bundle" in err.lower()
        assert not unsafe.exists(), "unsafe bundle was written despite refusal"
    finally:
        if unsafe.exists():
            unsafe.unlink()


def test_bundle_out_allows_unsafe_path_with_explicit_override(tmp_path):
    """--allow-unsafe-bundle-path should let a non-gitignored repo path
    through; we still write to tmp_path under autoresearch/.tmp to keep
    the test from polluting the repo even when the override is on."""
    # Use a path inside the repo but force a known-gitignored prefix
    # so the test never leaves cruft behind regardless of safety.
    safe_dir = ROOT / "autoresearch" / ".tmp" / "pipe5_test"
    safe_dir.mkdir(parents=True, exist_ok=True)
    bundle = safe_dir / "bundle.json"
    if bundle.exists():
        bundle.unlink()
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    try:
        code, _, err = run_cli(
            [
                "--fixture",
                fixture,
                "--json",
                "--bundle-out",
                str(bundle),
                "--allow-unsafe-bundle-path",
            ]
        )
        assert code == 0, err
        assert bundle.exists()
    finally:
        if bundle.exists():
            bundle.unlink()
        if safe_dir.exists():
            try:
                safe_dir.rmdir()
            except OSError:
                pass


def test_bundle_out_accepts_autoresearch_runs_path():
    """``autoresearch/.runs/*`` is in .gitignore — it should be accepted
    as a safe target without --allow-unsafe-bundle-path."""
    safe_dir = ROOT / "autoresearch" / ".runs" / "pipe5_test"
    safe_dir.mkdir(parents=True, exist_ok=True)
    bundle = safe_dir / "bundle.json"
    if bundle.exists():
        bundle.unlink()
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    try:
        code, _, err = run_cli(
            ["--fixture", fixture, "--json", "--bundle-out", str(bundle)]
        )
        assert code == 0, err
        assert bundle.exists()
    finally:
        if bundle.exists():
            bundle.unlink()
        if safe_dir.exists():
            try:
                safe_dir.rmdir()
            except OSError:
                pass


def test_invalid_fixture_does_not_write_bundle(tmp_path):
    """If the fixture fails to load, the CLI must exit non-zero and
    must not leave a bundle file behind on disk."""
    bad = tmp_path / "bad.json"
    bad.write_text("not json", encoding="utf-8")
    bundle_path = tmp_path / "bundle.json"
    code, out, err = run_cli(
        ["--fixture", str(bad), "--json", "--bundle-out", str(bundle_path)]
    )
    assert code == 4
    assert err
    assert not bundle_path.exists()


def test_missing_fixture_does_not_write_bundle(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    missing = tmp_path / "does_not_exist.json"
    code, _, err = run_cli(
        ["--fixture", str(missing), "--json", "--bundle-out", str(bundle_path)]
    )
    assert code == 3
    assert "fixture not found" in err
    assert not bundle_path.exists()


def test_cli_without_bundle_out_unchanged(tmp_path):
    """Sanity: omitting --bundle-out must leave the existing CLI
    behaviour completely unchanged (no bundle file, normal exit)."""
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, out, err = run_cli(["--fixture", fixture, "--json"])
    assert code == 0, err
    payload = json.loads(out)
    assert payload["packet_summary"]["case_id"] == "scenario_media_rich_produce"
    # Confirm we did not accidentally create files in the working dir.
    assert not (Path.cwd() / "bundle.json").exists()


def test_bundle_ledger_entry_has_zero_cost_no_live(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, _, _ = run_cli(
        ["--fixture", fixture, "--json", "--bundle-out", str(bundle_path)]
    )
    assert code == 0
    bundle = _read_bundle(bundle_path)
    led = bundle["ledger_entry"]
    assert led["estimated_cost_usd"] == 0.0
    for provider, count in led["api_calls"].items():
        assert count == 0, f"unexpected api_calls.{provider} = {count}"


def test_bundle_artifact_claims_serialize_with_required_fields(tmp_path):
    """ArtifactClaim entries written into the bundle must include the
    canonical fields (claim_id, artifact_type, claim_label,
    claim_source_url, supporting_snippet) so consumers can audit the
    claim ⇄ artifact distinction."""
    bundle_path = tmp_path / "bundle.json"
    fixture = str(FIXTURE_DIR / "claim_only_hold.json")
    code, _, _ = run_cli(
        ["--fixture", fixture, "--json", "--bundle-out", str(bundle_path)]
    )
    assert code == 0
    bundle = _read_bundle(bundle_path)
    claims = bundle["artifact_claims"]
    if claims:
        for claim in claims:
            for key in (
                "claim_id",
                "artifact_type",
                "claim_label",
                "claim_source_url",
                "supporting_snippet",
            ):
                assert key in claim, f"artifact claim missing {key!r}"


def test_bundle_validate_unsafe_path_helper_directly():
    """Direct unit test of the path-safety helper, independent of the
    CLI flow."""
    repo = ROOT
    safe = repo / "autoresearch" / ".runs" / "x.json"
    unsafe = repo / "tests" / "fixtures" / "x.json"
    outside = Path("/some/absolute/path/that/is/outside.json")
    assert cli._is_safe_bundle_path(safe) is True
    assert cli._is_safe_bundle_path(unsafe) is False
    # ``outside`` resolves to whatever the OS thinks; on Windows it
    # becomes ``C:\some\absolute\path\...`` which is outside the repo
    # and therefore safe.
    assert cli._is_safe_bundle_path(outside) is True


# ---- --emit-handoffs in the run bundle ---------------------------------


def test_default_mode_bundle_omits_handoffs_without_flag(tmp_path):
    """Without --emit-handoffs the bundle must NOT carry the handoffs
    key — this guards backwards-compatible bundle output and protects
    every existing REQUIRED_BUNDLE_KEYS expectation."""
    bundle_path = tmp_path / "bundle.json"
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, _, err = run_cli(
        ["--fixture", fixture, "--json", "--bundle-out", str(bundle_path)]
    )
    assert code == 0, f"non-zero exit: {err}"
    bundle = _read_bundle(bundle_path)
    assert "handoffs" not in bundle


def test_default_mode_bundle_includes_handoffs_when_flag_passed(tmp_path):
    """With --emit-handoffs and --bundle-out, the bundle must include
    the handoffs key alongside every existing REQUIRED_BUNDLE_KEYS
    section."""
    bundle_path = tmp_path / "bundle.json"
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, _, err = run_cli(
        [
            "--fixture",
            fixture,
            "--json",
            "--emit-handoffs",
            "--bundle-out",
            str(bundle_path),
        ]
    )
    assert code == 0, f"non-zero exit: {err}"
    bundle = _read_bundle(bundle_path)
    # Existing canonical keys still all present.
    for key in REQUIRED_BUNDLE_KEYS:
        assert key in bundle, f"missing canonical bundle key {key!r}"
    handoffs = bundle.get("handoffs")
    assert handoffs is not None, "bundle should include handoffs under --emit-handoffs"
    assert sorted(handoffs.keys()) == ["p2_to_p3", "p2_to_p4", "p2_to_p5"]
    # Sanity: P3 rows for media_rich_produce.json should be non-empty.
    assert handoffs["p2_to_p3"], "P3 rows should be non-empty for media-rich fixture"


def test_build_run_bundle_handoffs_param_is_optional_and_additive():
    """Direct unit test of build_run_bundle: omitting handoffs leaves
    the bundle unchanged; passing a handoffs dict adds exactly that
    one top-level key."""
    fixture = Path(FIXTURE_DIR / "media_rich_produce.json")
    packet = cli._load_fixture(fixture)
    no_handoffs = cli.build_run_bundle(
        mode="default",
        experiment_id="test-no-handoffs",
        wallclock_seconds=0.0,
        packet=packet,
    )
    handoffs = cli.build_handoffs(packet)
    with_handoffs = cli.build_run_bundle(
        mode="default",
        experiment_id="test-with-handoffs",
        wallclock_seconds=0.0,
        packet=packet,
        handoffs=handoffs,
    )
    assert "handoffs" not in no_handoffs
    assert with_handoffs["handoffs"] == handoffs
    # Every other key matches modulo experiment_id.
    extra_keys = set(with_handoffs.keys()) - set(no_handoffs.keys())
    assert extra_keys == {"handoffs"}


# ---- portal_replay= param (PR #11) -------------------------------------


def test_build_run_bundle_portal_replay_param_is_optional_and_additive():
    """Direct unit test of build_run_bundle's portal_replay param.

    Mirrors the handoffs= test above: omitting portal_replay leaves the
    bundle structurally unchanged; supplying a portal_replay dict adds
    exactly that one top-level key. Backwards-compat guarantees:
    REQUIRED_BUNDLE_KEYS still all present in both bundles."""
    fixture = Path(FIXTURE_DIR / "media_rich_produce.json")
    packet = cli._load_fixture(fixture)
    no_portal = cli.build_run_bundle(
        mode="default",
        experiment_id="test-no-portal",
        wallclock_seconds=0.0,
        packet=packet,
    )
    fake_portal_replay = {
        "portal_profile_id": "agency_ois_detail",
        "fixture_path": "tests/fixtures/agency_ois/incident_detail_with_bodycam_video.json",
        "source_records_count": 2,
        "artifact_claims_count": 0,
        "candidate_urls_count": 1,
        "rejected_urls_count": 0,
        "executor_status": "completed",
        "executor_risk_flags": [],
        "executor_next_actions": [],
    }
    with_portal = cli.build_run_bundle(
        mode="portal_replay",
        experiment_id="test-with-portal",
        wallclock_seconds=0.0,
        packet=packet,
        portal_replay=fake_portal_replay,
    )
    assert "portal_replay" not in no_portal
    assert with_portal["portal_replay"] == fake_portal_replay
    # Canonical bundle keys still present in both.
    for key in REQUIRED_BUNDLE_KEYS:
        assert key in no_portal
        assert key in with_portal
    # Adding portal_replay must not displace any other key.
    extra_keys = set(with_portal.keys()) - set(no_portal.keys())
    assert extra_keys == {"portal_replay"}


def test_default_mode_bundle_omits_portal_replay_without_flag(tmp_path):
    """Backwards-compat guard: default-mode CLI runs (no
    --portal-replay) must NOT carry a portal_replay key in the
    bundle. Locks the opt-in invariant from the operator surface."""
    bundle_path = tmp_path / "bundle.json"
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, _, err = run_cli(
        ["--fixture", fixture, "--json", "--bundle-out", str(bundle_path)]
    )
    assert code == 0, f"non-zero exit: {err}"
    bundle = _read_bundle(bundle_path)
    assert "portal_replay" not in bundle
