"""PORTAL6 — Offline portal-replay CLI mode tests.

Covers the new ``--portal-replay`` CLI mode. Tests run the CLI
in-process via ``cli.main(argv, stdout=..., stderr=...)`` (matches
the existing ``test_casegraph_cli.py`` pattern; subprocess black-box
coverage continues to live in ``test_casegraph_cli_golden_smoke.py``).

What this file locks in (per PR scope):

- The CLI portal-replay JSON shape (top-level keys + ``portal_replay``
  section keys).
- Doctrinal outcomes per fixture: bodycam media graduates, claim-only
  payload yields ArtifactClaims but no verified_artifacts, document-
  only payloads HOLD, protected URLs are rejected with the
  ``protected_or_nonpublic`` risk flag.
- ``--emit-handoffs`` semantics carry over from PR #6.
- Live URL guard: http:// and https:// fixture values fail with
  EXIT_LIVE_BLOCKED before any I/O.
- Standard CLI error mapping for missing / invalid JSON fixtures.
- Default mode (no ``--portal-replay``) remains structurally
  unchanged.
- Zero HTTP calls across the full chain.
"""
from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Tuple

import pytest

from pipeline2_discovery.casegraph import cli


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "schemas"
AGENCY_OIS_DIR = ROOT / "tests" / "fixtures" / "agency_ois"
SCENARIO_DIR = ROOT / "tests" / "fixtures" / "casegraph_scenarios"

BODYCAM_FIXTURE = str(AGENCY_OIS_DIR / "incident_detail_with_bodycam_video.json")
CLAIM_ONLY_FIXTURE = str(AGENCY_OIS_DIR / "incident_detail_with_bodycam_claim_no_url.json")
PROTECTED_FIXTURE = str(AGENCY_OIS_DIR / "incident_detail_with_protected_link.json")
PDF_FIXTURE = str(AGENCY_OIS_DIR / "incident_detail_with_pdf.json")

REQUIRED_PORTAL_REPLAY_KEYS = (
    "portal_profile_id",
    "fixture_path",
    "source_records_count",
    "artifact_claims_count",
    "candidate_urls_count",
    "rejected_urls_count",
    "executor_status",
    "executor_risk_flags",
    "executor_next_actions",
)

REQUIRED_TOP_LEVEL_KEYS = (
    "input_summary",
    "packet_summary",
    "result",
    "report",
    "ledger_entry",
    "portal_replay",
)


def run_cli(argv) -> Tuple[int, str, str]:
    out = io.StringIO()
    err = io.StringIO()
    code = cli.main(argv, stdout=out, stderr=err)
    return code, out.getvalue(), err.getvalue()


def _assert_valid(schema_name: str, instance) -> None:
    try:
        from jsonschema import Draft7Validator  # type: ignore
    except ImportError:  # pragma: no cover
        pytest.skip("jsonschema not installed")
    schema = json.loads((SCHEMA_DIR / schema_name).read_text(encoding="utf-8"))
    errors = sorted(
        Draft7Validator(schema).iter_errors(instance), key=lambda e: list(e.path)
    )
    assert not errors, "; ".join(f"{list(e.path)}: {e.message}" for e in errors)


# ---- Bodycam / video fixture: happy path -------------------------------


def test_portal_replay_bodycam_fixture_exits_zero_and_emits_json():
    code, out, err = run_cli(
        ["--portal-replay", "--fixture", BODYCAM_FIXTURE, "--json"]
    )
    assert code == 0, f"non-zero exit: {err}"
    payload = json.loads(out)
    for key in REQUIRED_TOP_LEVEL_KEYS:
        assert key in payload, f"missing top-level key {key!r}"
    assert payload["input_summary"]["input_type"] == "portal_replay"


def test_portal_replay_section_has_canonical_fields():
    code, out, _ = run_cli(
        ["--portal-replay", "--fixture", BODYCAM_FIXTURE, "--json"]
    )
    assert code == 0
    pr = json.loads(out)["portal_replay"]
    for key in REQUIRED_PORTAL_REPLAY_KEYS:
        assert key in pr, f"missing portal_replay key {key!r}"
    assert pr["portal_profile_id"] == "agency_ois_detail"
    assert pr["executor_status"] == "completed"
    # The bodycam fixture has 1 media link; AgencyOISConnector emits
    # the page record + the media link record.
    assert pr["source_records_count"] >= 1
    assert pr["candidate_urls_count"] >= 1
    assert pr["rejected_urls_count"] == 0
    assert pr["fixture_path"].endswith(
        "tests/fixtures/agency_ois/incident_detail_with_bodycam_video.json"
    )


def test_portal_replay_bodycam_media_graduates_into_verified_artifacts():
    code, out, _ = run_cli(
        ["--portal-replay", "--fixture", BODYCAM_FIXTURE, "--json"]
    )
    assert code == 0
    payload = json.loads(out)
    types = set(payload["packet_summary"]["verified_artifact_types"])
    assert "bodycam" in types, (
        f"bodycam media should graduate; got types={sorted(types)}"
    )
    assert payload["packet_summary"]["verified_artifact_count"] >= 1


def test_portal_replay_emit_handoffs_validates_p3_p4_p5():
    code, out, _ = run_cli(
        [
            "--portal-replay",
            "--fixture",
            BODYCAM_FIXTURE,
            "--emit-handoffs",
            "--json",
        ]
    )
    assert code == 0
    handoffs = json.loads(out)["handoffs"]
    assert sorted(handoffs.keys()) == ["p2_to_p3", "p2_to_p4", "p2_to_p5"]
    assert handoffs["p2_to_p3"], "P3 rows expected for graduated bodycam media"
    for row in handoffs["p2_to_p3"]:
        _assert_valid("p2_to_p3.schema.json", row)
    _assert_valid("p2_to_p4.schema.json", handoffs["p2_to_p4"])
    _assert_valid("p2_to_p5.schema.json", handoffs["p2_to_p5"])


def test_portal_replay_omits_handoffs_without_flag():
    """--emit-handoffs is opt-in. Without it, no handoffs key appears."""
    code, out, _ = run_cli(
        ["--portal-replay", "--fixture", BODYCAM_FIXTURE, "--json"]
    )
    assert code == 0
    payload = json.loads(out)
    assert "handoffs" not in payload


# ---- Claim-only fixture: no verified artifacts -------------------------


def test_portal_replay_claim_only_yields_artifact_claims_not_artifacts():
    code, out, _ = run_cli(
        ["--portal-replay", "--fixture", CLAIM_ONLY_FIXTURE, "--json"]
    )
    assert code == 0
    payload = json.loads(out)
    assert payload["portal_replay"]["candidate_urls_count"] == 0
    assert payload["portal_replay"]["artifact_claims_count"] >= 1
    pkt = payload["packet_summary"]
    assert pkt["verified_artifact_count"] == 0
    assert pkt["artifact_claim_count"] >= 1


def test_portal_replay_claim_only_emits_zero_p3_rows():
    code, out, _ = run_cli(
        [
            "--portal-replay",
            "--fixture",
            CLAIM_ONLY_FIXTURE,
            "--emit-handoffs",
            "--json",
        ]
    )
    assert code == 0
    handoffs = json.loads(out)["handoffs"]
    assert handoffs["p2_to_p3"] == []
    _assert_valid("p2_to_p4.schema.json", handoffs["p2_to_p4"])
    _assert_valid("p2_to_p5.schema.json", handoffs["p2_to_p5"])


# ---- Protected / login fixture -----------------------------------------


def test_portal_replay_protected_link_rejected_and_risk_flagged():
    code, out, _ = run_cli(
        ["--portal-replay", "--fixture", PROTECTED_FIXTURE, "--json"]
    )
    assert code == 0
    payload = json.loads(out)
    pr = payload["portal_replay"]
    assert pr["rejected_urls_count"] >= 1
    assert "protected_or_nonpublic" in pr["executor_risk_flags"]
    # The protected media URL must NOT graduate.
    types = set(payload["packet_summary"]["verified_artifact_types"])
    assert "bodycam" not in types, (
        "protected media URL must not graduate as bodycam"
    )
    # Risk flag also surfaces on the scored result / packet.
    risks = set(payload["result"]["risk_flags"])
    assert "protected_or_nonpublic" in risks


# ---- Document-only fixture ---------------------------------------------


def test_portal_replay_document_only_does_not_produce():
    code, out, _ = run_cli(
        ["--portal-replay", "--fixture", PDF_FIXTURE, "--json"]
    )
    assert code == 0
    payload = json.loads(out)
    assert payload["result"]["verdict"] != "PRODUCE"
    types = set(payload["packet_summary"]["verified_artifact_types"])
    # PDF graduates as docket_docs document; no media artifact.
    assert "docket_docs" in types or "document" in types
    assert "bodycam" not in types


# ---- Live URL guard ----------------------------------------------------


@pytest.mark.parametrize(
    "live_url",
    [
        "http://example.com/some-fixture.json",
        "https://www.phoenix.gov/police/critical-incidents/2024-OIS-014",
    ],
)
def test_portal_replay_refuses_live_urls_with_exit_5(live_url):
    code, _, err = run_cli(
        ["--portal-replay", "--fixture", live_url, "--json"]
    )
    assert code == cli.EXIT_LIVE_BLOCKED
    assert "live URL" in err or "live url" in err.lower()


# ---- Standard error-code mappings --------------------------------------


def test_portal_replay_missing_fixture_returns_exit_3(tmp_path):
    missing = tmp_path / "does_not_exist.json"
    code, _, err = run_cli(
        ["--portal-replay", "--fixture", str(missing), "--json"]
    )
    assert code == cli.EXIT_FIXTURE_MISSING
    assert "not found" in err.lower()


def test_portal_replay_invalid_json_returns_exit_4(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{ not valid json", encoding="utf-8")
    code, _, err = run_cli(
        ["--portal-replay", "--fixture", str(bad), "--json"]
    )
    assert code == cli.EXIT_FIXTURE_INVALID


def test_portal_replay_payload_missing_required_shape_returns_exit_4(tmp_path):
    """A JSON object that has none of page_type / portal_profile_id /
    source_records is rejected as an invalid portal payload."""
    bad = tmp_path / "shape.json"
    bad.write_text(json.dumps({"hello": "world"}), encoding="utf-8")
    code, _, err = run_cli(
        ["--portal-replay", "--fixture", str(bad), "--json"]
    )
    assert code == cli.EXIT_FIXTURE_INVALID


# ---- Mutex / argparse ---------------------------------------------------


def test_portal_replay_conflicts_with_query_plan():
    with pytest.raises(SystemExit):
        run_cli(
            [
                "--portal-replay",
                "--query-plan",
                "--fixture",
                BODYCAM_FIXTURE,
                "--json",
            ]
        )


def test_portal_replay_conflicts_with_multi_source_dry_run():
    with pytest.raises(SystemExit):
        run_cli(
            [
                "--portal-replay",
                "--multi-source-dry-run",
                "--fixture",
                BODYCAM_FIXTURE,
                "--json",
            ]
        )


def test_portal_replay_conflicts_with_live_dry():
    with pytest.raises(SystemExit):
        run_cli(
            [
                "--portal-replay",
                "--live-dry",
                "--fixture",
                BODYCAM_FIXTURE,
                "--json",
            ]
        )


# ---- Network isolation -------------------------------------------------


def test_portal_replay_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    for fixture in (
        BODYCAM_FIXTURE,
        CLAIM_ONLY_FIXTURE,
        PROTECTED_FIXTURE,
        PDF_FIXTURE,
    ):
        code, _, _ = run_cli(
            [
                "--portal-replay",
                "--fixture",
                fixture,
                "--emit-handoffs",
                "--json",
            ]
        )
        assert code == 0, f"non-zero exit on {fixture}"

    assert calls == [], (
        f"portal-replay CLI made {len(calls)} live HTTP call(s)"
    )


# ---- Backwards-compat: default mode unchanged --------------------------


def test_default_mode_unchanged_when_portal_replay_not_passed():
    """Default-mode CLI on a CasePacket fixture must remain structurally
    identical to the PR #6 / PR #7 baseline shape."""
    fixture = str(SCENARIO_DIR / "media_rich_produce.json")
    code, out, _ = run_cli(["--fixture", fixture, "--json"])
    assert code == 0
    payload = json.loads(out)
    # No portal_replay section in default mode.
    assert "portal_replay" not in payload
    # Default-mode canonical keys still present.
    for key in (
        "input_summary",
        "packet_summary",
        "result",
        "report",
        "ledger_entry",
    ):
        assert key in payload
    assert payload["packet_summary"]["case_id"] == "scenario_media_rich_produce"
    assert payload["result"]["verdict"] == "PRODUCE"


# ---- Manifest-entry mode -----------------------------------------------
#
# `--portal-replay --portal-manifest-entry <case_id>` resolves the saved
# fixture from tests/fixtures/portal_replay/portal_replay_manifest.json.
# Direct fixture mode (PR #9) stays valid; the manifest-entry path is
# additive.


def test_portal_replay_manifest_entry_resolves_known_case_id():
    code, out, err = run_cli(
        ["--portal-replay", "--portal-manifest-entry", "31", "--json"]
    )
    assert code == 0, f"non-zero exit: {err}"
    payload = json.loads(out)
    pr = payload["portal_replay"]
    # Fixture path must match the manifest's mocked_payload_fixture.
    assert pr["fixture_path"].endswith(
        "tests/fixtures/agency_ois/incident_detail_with_youtube_embed.json"
    )
    # manifest_entry only appears in manifest mode.
    assert "manifest_entry" in pr
    assert pr["manifest_entry"]["case_id"] == 31
    assert pr["manifest_entry"]["manifest_path"].endswith(
        "tests/fixtures/portal_replay/portal_replay_manifest.json"
    )


def test_portal_replay_manifest_entry_runs_full_chain():
    """Case 31 exercises the YouTube-embed agency_ois fixture, which
    PR #8 proved graduates as a bodycam VerifiedArtifact."""
    code, out, _ = run_cli(
        ["--portal-replay", "--portal-manifest-entry", "31", "--json"]
    )
    assert code == 0
    payload = json.loads(out)
    types = set(payload["packet_summary"]["verified_artifact_types"])
    assert "bodycam" in types, (
        f"manifest case_id=31 should graduate bodycam media; got {sorted(types)}"
    )


def test_portal_replay_manifest_entry_emit_handoffs_validates():
    code, out, _ = run_cli(
        [
            "--portal-replay",
            "--portal-manifest-entry",
            "31",
            "--emit-handoffs",
            "--json",
        ]
    )
    assert code == 0
    handoffs = json.loads(out)["handoffs"]
    assert sorted(handoffs.keys()) == ["p2_to_p3", "p2_to_p4", "p2_to_p5"]
    assert handoffs["p2_to_p3"], "case_id=31 should yield at least one P3 row"
    for row in handoffs["p2_to_p3"]:
        _assert_valid("p2_to_p3.schema.json", row)
    _assert_valid("p2_to_p4.schema.json", handoffs["p2_to_p4"])
    _assert_valid("p2_to_p5.schema.json", handoffs["p2_to_p5"])


def test_portal_replay_direct_fixture_does_not_emit_manifest_entry_key():
    """Backwards-compat guard: PR #9's direct-fixture mode must NOT
    include portal_replay.manifest_entry in its JSON output."""
    code, out, _ = run_cli(
        ["--portal-replay", "--fixture", BODYCAM_FIXTURE, "--json"]
    )
    assert code == 0
    pr = json.loads(out)["portal_replay"]
    assert "manifest_entry" not in pr


def test_portal_replay_unknown_manifest_case_id_returns_exit_3():
    code, _, err = run_cli(
        ["--portal-replay", "--portal-manifest-entry", "9999", "--json"]
    )
    assert code == cli.EXIT_FIXTURE_MISSING
    assert "9999" in err
    assert "case_id" in err.lower()


def test_portal_replay_requires_fixture_or_manifest_entry():
    """--portal-replay alone, with no --fixture and no
    --portal-manifest-entry, must fail clearly with EXIT_FIXTURE_INVALID."""
    code, _, err = run_cli(["--portal-replay", "--json"])
    assert code == cli.EXIT_FIXTURE_INVALID
    assert "exactly one" in err.lower() or "requires" in err.lower()


def test_portal_replay_rejects_both_fixture_and_manifest_entry():
    code, _, err = run_cli(
        [
            "--portal-replay",
            "--fixture",
            BODYCAM_FIXTURE,
            "--portal-manifest-entry",
            "31",
            "--json",
        ]
    )
    assert code == cli.EXIT_FIXTURE_INVALID
    assert "mutually exclusive" in err.lower()


def test_portal_replay_manifest_mode_makes_zero_network_calls(monkeypatch):
    """Run all 5 manifest case_ids back-to-back under a
    requests.Session.get monkeypatch and assert no HTTP calls."""
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    for case_id in (31, 32, 33, 34, 37):
        code, _, err = run_cli(
            [
                "--portal-replay",
                "--portal-manifest-entry",
                str(case_id),
                "--emit-handoffs",
                "--json",
            ]
        )
        assert code == 0, f"case_id={case_id} failed: {err}"

    assert calls == [], (
        f"manifest-entry portal-replay made {len(calls)} live HTTP call(s)"
    )


# ---- Per-mode --fixture validation (no longer argparse-required) -------
#
# The argparse `--fixture` argument was relaxed from required=True to
# required=False so --portal-manifest-entry can stand alone in
# portal-replay mode. Every other mode still demands --fixture; the
# error message is now an inline EXIT_FIXTURE_INVALID instead of
# argparse's auto-generated SystemExit.


def test_default_mode_without_fixture_returns_exit_4_with_clear_error():
    code, _, err = run_cli(["--json"])
    assert code == cli.EXIT_FIXTURE_INVALID
    assert "--fixture" in err


def test_query_plan_without_fixture_returns_exit_4():
    code, _, err = run_cli(["--query-plan", "--json"])
    assert code == cli.EXIT_FIXTURE_INVALID
    assert "--fixture" in err


def test_multi_source_without_fixture_returns_exit_4():
    code, _, err = run_cli(
        ["--multi-source-dry-run", "--connectors", "courtlistener", "--json"]
    )
    assert code == cli.EXIT_FIXTURE_INVALID
    assert "--fixture" in err


def test_live_dry_without_fixture_returns_exit_4():
    code, _, err = run_cli(["--live-dry", "--json"])
    assert code == cli.EXIT_FIXTURE_INVALID
    assert "--fixture" in err


# ---- --bundle-out includes portal_replay (PR #11) ----------------------
#
# Direct fixture mode + --bundle-out: bundle gains the portal_replay
# section that was previously written only to JSON output. Manifest-
# entry mode additionally surfaces portal_replay.manifest_entry in
# the bundle. --emit-handoffs is independently composable.


REQUIRED_BUNDLE_CANONICAL_KEYS = (
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


def _read_bundle_file(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_portal_replay_direct_fixture_bundle_includes_portal_replay_section(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    code, _, err = run_cli(
        [
            "--portal-replay",
            "--fixture",
            BODYCAM_FIXTURE,
            "--json",
            "--bundle-out",
            str(bundle_path),
        ]
    )
    assert code == 0, f"non-zero exit: {err}"
    bundle = _read_bundle_file(bundle_path)
    # Canonical bundle keys still all present.
    for key in REQUIRED_BUNDLE_CANONICAL_KEYS:
        assert key in bundle, f"missing canonical bundle key {key!r}"
    # portal_replay section now lives in the bundle too.
    assert "portal_replay" in bundle
    pr = bundle["portal_replay"]
    for key in REQUIRED_PORTAL_REPLAY_KEYS:
        assert key in pr, f"missing portal_replay key {key!r}"
    # Direct fixture mode does NOT include manifest_entry — that's
    # the manifest-entry mode marker.
    assert "manifest_entry" not in pr
    assert pr["fixture_path"].endswith(
        "tests/fixtures/agency_ois/incident_detail_with_bodycam_video.json"
    )
    assert bundle["mode"] == "portal_replay"


def test_portal_replay_manifest_entry_bundle_includes_manifest_entry(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    code, _, err = run_cli(
        [
            "--portal-replay",
            "--portal-manifest-entry",
            "31",
            "--json",
            "--bundle-out",
            str(bundle_path),
        ]
    )
    assert code == 0, f"non-zero exit: {err}"
    bundle = _read_bundle_file(bundle_path)
    pr = bundle["portal_replay"]
    assert "manifest_entry" in pr, (
        "manifest-entry mode must surface portal_replay.manifest_entry "
        "in the bundle"
    )
    assert pr["manifest_entry"]["case_id"] == 31
    assert pr["manifest_entry"]["manifest_path"].endswith(
        "tests/fixtures/portal_replay/portal_replay_manifest.json"
    )
    # Resolved fixture path matches the manifest's mocked_payload_fixture.
    assert pr["fixture_path"].endswith(
        "tests/fixtures/agency_ois/incident_detail_with_youtube_embed.json"
    )


def test_portal_replay_bundle_with_emit_handoffs_includes_both_sections(tmp_path):
    bundle_path = tmp_path / "bundle.json"
    code, _, err = run_cli(
        [
            "--portal-replay",
            "--portal-manifest-entry",
            "31",
            "--emit-handoffs",
            "--json",
            "--bundle-out",
            str(bundle_path),
        ]
    )
    assert code == 0, f"non-zero exit: {err}"
    bundle = _read_bundle_file(bundle_path)
    # Both opt-in sections present alongside every canonical key.
    for key in REQUIRED_BUNDLE_CANONICAL_KEYS:
        assert key in bundle, f"missing canonical bundle key {key!r}"
    assert "portal_replay" in bundle
    assert "handoffs" in bundle
    handoffs = bundle["handoffs"]
    assert sorted(handoffs.keys()) == ["p2_to_p3", "p2_to_p4", "p2_to_p5"]
    # P3 should have at least one row for case_id=31 (YouTube embed).
    assert handoffs["p2_to_p3"], "case_id=31 should yield at least one P3 row"
