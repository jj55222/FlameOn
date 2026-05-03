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
SHERIFF_BODYCAM_FIXTURE = str(AGENCY_OIS_DIR / "incident_detail_sheriff_bodycam_video.json")

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


# ---- enrich_portal_replay_identity helper unit tests -------------------
#
# The helper lifts agency / incident_date / case_number from a saved
# agency_ois portal payload onto a manual-router-built CasePacket so
# resolve_identity can anchor on those structured fields. Pure: only
# fills blank values, never overwrites, ignores None / empty strings,
# coerces non-string inputs.


def _empty_portal_packet():
    """Fresh manual-router packet with no defendant; mirrors the empty
    state route_manual_defendant_jurisdiction yields before enrichment."""
    from pipeline2_discovery.casegraph.routers import (
        route_manual_defendant_jurisdiction,
    )

    return route_manual_defendant_jurisdiction("Test Subject", "Phoenix, AZ")


def test_enrich_identity_fills_blank_agency_incident_date_case_number():
    from pipeline2_discovery.casegraph.cli import enrich_portal_replay_identity

    packet = _empty_portal_packet()
    payload = {
        "agency": "Maricopa County Sheriff's Office",
        "incident_date": "2024-09-22",
        "case_number": "2024-MCSO-009",
    }
    enrich_portal_replay_identity(packet, payload)
    assert packet.case_identity.agency == "Maricopa County Sheriff's Office"
    assert packet.case_identity.incident_date == "2024-09-22"
    assert packet.case_identity.case_numbers == ["2024-MCSO-009"]


def test_enrich_identity_does_not_overwrite_existing_fields():
    from pipeline2_discovery.casegraph.cli import enrich_portal_replay_identity

    packet = _empty_portal_packet()
    packet.case_identity.agency = "Pre-existing Agency"
    packet.case_identity.incident_date = "2020-01-01"
    packet.case_identity.case_numbers = ["EXISTING-001"]
    payload = {
        "agency": "Other Agency",
        "incident_date": "2024-09-22",
        "case_number": "2024-MCSO-009",
    }
    enrich_portal_replay_identity(packet, payload)
    assert packet.case_identity.agency == "Pre-existing Agency"
    assert packet.case_identity.incident_date == "2020-01-01"
    assert packet.case_identity.case_numbers == ["EXISTING-001"]


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"agency": None, "incident_date": None, "case_number": None},
        {"agency": "", "incident_date": "", "case_number": ""},
        {"agency": "   ", "incident_date": "  ", "case_number": "\t"},
    ],
)
def test_enrich_identity_ignores_missing_none_and_empty_values(payload):
    from pipeline2_discovery.casegraph.cli import enrich_portal_replay_identity

    packet = _empty_portal_packet()
    enrich_portal_replay_identity(packet, payload)
    assert packet.case_identity.agency is None
    assert packet.case_identity.incident_date is None
    assert packet.case_identity.case_numbers == []


def test_enrich_identity_coerces_non_string_case_number_to_string():
    from pipeline2_discovery.casegraph.cli import enrich_portal_replay_identity

    packet = _empty_portal_packet()
    payload = {"case_number": 12345}
    enrich_portal_replay_identity(packet, payload)
    assert packet.case_identity.case_numbers == ["12345"]


def test_enrich_identity_strips_whitespace_around_string_values():
    from pipeline2_discovery.casegraph.cli import enrich_portal_replay_identity

    packet = _empty_portal_packet()
    payload = {
        "agency": "  Phoenix Police Department  ",
        "incident_date": "  2024-12-01\n",
        "case_number": "\t2024-OIS-050\t",
    }
    enrich_portal_replay_identity(packet, payload)
    assert packet.case_identity.agency == "Phoenix Police Department"
    assert packet.case_identity.incident_date == "2024-12-01"
    assert packet.case_identity.case_numbers == ["2024-OIS-050"]


def test_enrich_identity_does_not_mutate_source_records_or_artifacts():
    from pipeline2_discovery.casegraph.cli import enrich_portal_replay_identity

    packet = _empty_portal_packet()
    sources_before = list(packet.sources)
    artifacts_before = list(packet.verified_artifacts)
    payload = {
        "agency": "Phoenix Police Department",
        "incident_date": "2024-12-01",
        "case_number": "2024-OIS-050",
    }
    enrich_portal_replay_identity(packet, payload)
    assert packet.sources == sources_before
    assert packet.verified_artifacts == artifacts_before


# ---- Case 38: realistic sheriff-office bodycam fixture -----------------
#
# Case 38 was added in PR #14 with a hand-authored realistic agency_ois
# payload (Maricopa County Sheriff's Office, public bodycam .mp4). The
# media graduates and the page surfaces a concluded outcome ("subject
# pleaded guilty 2024"). Pre-enrichment, identity stayed at MEDIUM
# because the manual router never lifted agency / incident_date /
# case_number off the page payload onto case_identity. This block
# locks in the post-enrichment behavior: HIGH identity and a PRODUCE
# verdict driven by an honest score >= 70, never a forced flip.


def _payload_keys_for_case_38():
    """Return (verdict, identity_confidence, production_score, p3_count,
    types) extracted from the manifest-entry CLI run with handoffs."""
    code, out, err = run_cli(
        [
            "--portal-replay",
            "--portal-manifest-entry",
            "38",
            "--emit-handoffs",
            "--json",
        ]
    )
    assert code == 0, f"non-zero exit: {err}"
    payload = json.loads(out)
    return payload


def test_case_38_manifest_entry_yields_high_identity_confidence():
    payload = _payload_keys_for_case_38()
    assert payload["packet_summary"]["identity_confidence"] == "high", (
        "case 38 should reach HIGH identity confidence after agency_ois "
        "payload metadata is enriched onto case_identity. "
        f"Got: {payload['packet_summary']['identity_confidence']!r}, "
        f"verdict={payload['result']['verdict']!r}, "
        f"production={payload['result']['production_actionability_score']}"
    )


def test_case_38_manifest_entry_keeps_bodycam_graduation():
    """Regression guard for PR #14 behavior: enrichment must not
    perturb media graduation."""
    payload = _payload_keys_for_case_38()
    types = set(payload["packet_summary"]["verified_artifact_types"])
    assert "bodycam" in types, (
        f"case 38 must still graduate bodycam after enrichment; got {sorted(types)}"
    )
    assert payload["packet_summary"]["verified_artifact_count"] == 1


def test_case_38_manifest_entry_p3_row_count_is_one():
    payload = _payload_keys_for_case_38()
    handoffs = payload["handoffs"]
    assert len(handoffs["p2_to_p3"]) == 1, (
        f"case 38 should yield exactly one P3 row (the bodycam media); "
        f"got {len(handoffs['p2_to_p3'])}"
    )


def test_case_38_manifest_entry_produces_when_score_above_threshold():
    """If honest enrichment lifts production_actionability_score over
    70.0 (the PRODUCE threshold in scoring._verdict), the verdict must
    be PRODUCE. If not, the test reports the actual score so we don't
    silently force the flip."""
    payload = _payload_keys_for_case_38()
    score = payload["result"]["production_actionability_score"]
    verdict = payload["result"]["verdict"]
    if score >= 70.0:
        assert verdict == "PRODUCE", (
            f"case 38 production score {score} >= 70 but verdict is "
            f"{verdict!r}; PRODUCE gate is identity == high (got "
            f"{payload['packet_summary']['identity_confidence']!r}) AND "
            f"media present AND no severe risks. "
            f"risks={payload['result']['risk_flags']}"
        )
    else:
        # The recommendation is to honestly report rather than force.
        # If we fall here the inspection prediction was wrong; surface it.
        pytest.fail(
            f"case 38 production_actionability_score={score} (< 70). "
            f"Identity enrichment did not raise the score over the "
            f"PRODUCE threshold. verdict={verdict!r}"
        )


def test_case_38_direct_fixture_mode_yields_high_identity_and_produce():
    """Direct --fixture mode must reach the same identity/verdict as
    manifest-entry mode; both routes go through _build_portal_packet
    so they share the enrichment helper."""
    code, out, err = run_cli(
        ["--portal-replay", "--fixture", SHERIFF_BODYCAM_FIXTURE, "--json"]
    )
    assert code == 0, f"non-zero exit: {err}"
    payload = json.loads(out)
    assert payload["packet_summary"]["identity_confidence"] == "high"
    types = set(payload["packet_summary"]["verified_artifact_types"])
    assert "bodycam" in types
    score = payload["result"]["production_actionability_score"]
    verdict = payload["result"]["verdict"]
    if score >= 70.0:
        assert verdict == "PRODUCE", (
            f"production score {score} but verdict {verdict!r}"
        )


# ---- Stored vs fresh verdict surfaces (Option F) ----------------------
#
# packet.verdict is the manual-router default ("HOLD") for portal-replay
# packets and is never updated, since score_case_packet is documented
# as pure. The CLI now exposes both the stored value and the fresh
# scorer verdict in JSON output, and threads score_result.verdict into
# the P5 handoff so downstream automation no longer sees PRODUCE under
# result.verdict next to HOLD under handoffs.p2_to_p5.verdict.


def test_case_38_packet_summary_exposes_both_stored_and_fresh_verdict():
    payload = _payload_keys_for_case_38()
    pkt = payload["packet_summary"]
    # Stored router default — always HOLD for portal-replay packets.
    assert pkt["packet_verdict"] == "HOLD", (
        f"portal-replay packet should keep router default HOLD; got {pkt['packet_verdict']!r}"
    )
    # Fresh scorer verdict — should match payload["result"]["verdict"].
    assert pkt["score_verdict"] == payload["result"]["verdict"]
    assert pkt["score_verdict"] == "PRODUCE", (
        f"case 38 score_verdict should be PRODUCE; got {pkt['score_verdict']!r}"
    )


def test_case_38_p5_handoff_verdict_matches_result_verdict():
    """P5 handoff verdict must reflect the fresh scorer outcome, not
    the packet's stored router default. This is the most painful
    contradiction PR resolves."""
    payload = _payload_keys_for_case_38()
    p5_verdict = payload["handoffs"]["p2_to_p5"]["verdict"]
    result_verdict = payload["result"]["verdict"]
    assert p5_verdict == result_verdict, (
        f"P5 handoff verdict ({p5_verdict!r}) must match result.verdict "
        f"({result_verdict!r}) when score_result is threaded"
    )
    assert p5_verdict == "PRODUCE"


def test_case_38_direct_fixture_packet_summary_and_p5_verdict_coherent():
    """Direct --fixture mode must reach the same coherence as
    manifest-entry mode. Both go through build_portal_replay_payload
    which now threads score_result into _packet_summary and handoffs."""
    code, out, err = run_cli(
        [
            "--portal-replay",
            "--fixture",
            SHERIFF_BODYCAM_FIXTURE,
            "--emit-handoffs",
            "--json",
        ]
    )
    assert code == 0, f"non-zero exit: {err}"
    payload = json.loads(out)
    pkt = payload["packet_summary"]
    assert pkt["packet_verdict"] == "HOLD"
    assert pkt["score_verdict"] == "PRODUCE"
    assert payload["result"]["verdict"] == "PRODUCE"
    assert payload["handoffs"]["p2_to_p5"]["verdict"] == "PRODUCE"


def test_portal_replay_score_verdict_present_even_without_emit_handoffs():
    """score_verdict is part of the JSON output's packet_summary, not
    the handoffs payload. It must appear regardless of --emit-handoffs."""
    code, out, _ = run_cli(
        ["--portal-replay", "--fixture", SHERIFF_BODYCAM_FIXTURE, "--json"]
    )
    assert code == 0
    payload = json.loads(out)
    assert "score_verdict" in payload["packet_summary"]
    assert payload["packet_summary"]["score_verdict"] == "PRODUCE"


# ---- Regression guard: existing non-PRODUCE manifest cases ------------
#
# Cases 32 (claim only, no URL), 33 (PDF only), 34 (protected media +
# public PDF), and 37 (generic weak YouTube) must continue to NOT
# PRODUCE after enrichment, since none of them surface graduating
# media — identity confidence may rise but the PRODUCE boolean gate
# also requires media artifacts.


@pytest.mark.parametrize("case_id", [32, 33, 34, 37])
def test_manifest_case_remains_non_produce_after_enrichment(case_id):
    code, out, err = run_cli(
        [
            "--portal-replay",
            "--portal-manifest-entry",
            str(case_id),
            "--json",
        ]
    )
    assert code == 0, f"case_id={case_id} non-zero exit: {err}"
    payload = json.loads(out)
    verdict = payload["result"]["verdict"]
    assert verdict != "PRODUCE", (
        f"case_id={case_id} flipped to PRODUCE after identity enrichment; "
        f"identity={payload['packet_summary']['identity_confidence']!r} "
        f"types={payload['packet_summary']['verified_artifact_types']} "
        f"score={payload['result']['production_actionability_score']}"
    )


# ---- Stale router-default risk-flag filter ---------------------------
#
# Case 38 has high identity AND a graduated bodycam, so both
# `identity_not_locked` and `no_verified_artifacts` are stale. The
# filter must strip them from result, P4, P5, and bundle surfaces
# while leaving genuine advisories (e.g. `conflicting_outcome_signals`)
# intact. Cases without graduated media keep the flags by design.


_STALE_ROUTER_FLAGS = {"identity_not_locked", "no_verified_artifacts"}


def test_case_38_result_risk_flags_excludes_stale_router_defaults():
    payload = _payload_keys_for_case_38()
    risks = set(payload["result"]["risk_flags"])
    assert not (risks & _STALE_ROUTER_FLAGS), (
        f"case 38 result.risk_flags should not carry stale router defaults; "
        f"got {sorted(risks)}"
    )


def test_case_38_p4_source_quality_notes_excludes_stale_router_defaults():
    payload = _payload_keys_for_case_38()
    notes = set(payload["handoffs"]["p2_to_p4"]["source_quality_notes"])
    assert not (notes & _STALE_ROUTER_FLAGS), (
        f"case 38 P4 source_quality_notes should not carry stale router defaults; "
        f"got {sorted(notes)}"
    )


def test_case_38_p5_risk_flags_excludes_stale_router_defaults():
    payload = _payload_keys_for_case_38()
    risks = set(payload["handoffs"]["p2_to_p5"]["risk_flags"])
    assert not (risks & _STALE_ROUTER_FLAGS), (
        f"case 38 P5 risk_flags should not carry stale router defaults; "
        f"got {sorted(risks)}"
    )


def test_case_38_advisory_flags_other_than_router_defaults_survive():
    """The filter must be conservative: it strips only the two named
    router defaults. Other advisory flags (e.g. conflicting_outcome_signals,
    outcome_not_concluded_advisory if present) must survive untouched."""
    payload = _payload_keys_for_case_38()
    risks = set(payload["result"]["risk_flags"])
    # The smoke output for case 38 in PR #16 carried "conflicting_outcome_signals".
    # Lock it as a survivability witness.
    assert "conflicting_outcome_signals" in risks, (
        f"expected conflicting_outcome_signals to survive the filter; got {sorted(risks)}"
    )


@pytest.mark.parametrize("case_id", [32, 33, 34, 37])
def test_non_resolved_cases_keep_stale_router_flags(case_id):
    """Cases with no graduating media (and identity ≤ medium for some)
    legitimately still need both router defaults — the filter must NOT
    strip them. Locks the conservative scoping."""
    code, out, err = run_cli(
        [
            "--portal-replay",
            "--portal-manifest-entry",
            str(case_id),
            "--emit-handoffs",
            "--json",
        ]
    )
    assert code == 0, f"case_id={case_id} non-zero exit: {err}"
    payload = json.loads(out)
    pkt = payload["packet_summary"]
    identity_high = pkt["identity_confidence"] == "high"
    has_artifacts = pkt["verified_artifact_count"] > 0
    p5_risks = set(payload["handoffs"]["p2_to_p5"]["risk_flags"])
    if not identity_high:
        assert "identity_not_locked" in p5_risks, (
            f"case_id={case_id} identity!=high but identity_not_locked was stripped; "
            f"p5_risks={sorted(p5_risks)}"
        )
    if not has_artifacts:
        assert "no_verified_artifacts" in p5_risks, (
            f"case_id={case_id} no artifacts but no_verified_artifacts was stripped; "
            f"p5_risks={sorted(p5_risks)}"
        )
