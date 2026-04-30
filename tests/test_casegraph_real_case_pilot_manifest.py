"""REAL1 — Real-case pilot seed fixture tests.

Asserts that ``tests/fixtures/pilot_cases/real_case_min_jian_guan.json``:

- loads as a valid CasePacket fixture via the CLI loader
- represents a real publicly-documented case from the project's
  calibration corpus (Min Jian Guan, San Francisco; calibration_data
  case_id=1, tier=ENOUGH)
- pre-locks identity to high (full_name + jurisdiction + agency +
  case_number anchors)
- carries a concluded outcome (sentenced)
- has zero pre-loaded sources / claims / verified artifacts so live
  data has somewhere to land
- scores HOLD as a dry verdict (high identity + concluded outcome +
  zero artifacts -> no_verified_media risk_flag)
- contains no private / protected URLs
- contains no obvious secret patterns

Asserts that the pilot manifest carries a corresponding pilot entry
``real_case_min_jian_guan_pilot``:

- references the real-case seed fixture
- declares allowed_connectors=['documentcloud'] (proven artifact
  path)
- max_live_calls=1, max_results_per_connector=5
- expected_verdict_without_live='HOLD'
- expected_minimum.media_required_for_produce=true
- allow_downloads / allow_scraping / allow_llm all false
- is_real_case=true and references the calibration corpus
- agrees with the live runner on dry verdict (HOLD)
- agrees with the pilot scoreboard (no warnings)

Asserts the real-case pilot is selected by ``select_pilot_for_live_smoke``
over placeholder pilots (real-case seed scores +2 for HOLD vs SKIP).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    build_pilot_validation_scoreboard,
    run_pilot_manifest,
    run_validation_manifest,
    score_case_packet,
    select_pilot_for_live_smoke,
)
from pipeline2_discovery.casegraph.cli import _load_fixture


ROOT = Path(__file__).resolve().parents[1]
SEED_PATH = ROOT / "tests" / "fixtures" / "pilot_cases" / "real_case_min_jian_guan.json"
PILOT_MANIFEST = ROOT / "tests" / "fixtures" / "pilot_cases" / "pilot_manifest.json"


SECRET_PATTERNS = (
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"AIza[0-9A-Za-z_-]{35}"),
    re.compile(r"sk-[A-Za-z0-9]{40,}"),
    re.compile(r"-----BEGIN [A-Z ]+PRIVATE KEY-----"),
    re.compile(r"BSAcl[A-Za-z0-9_-]{20,}"),
)
FORBIDDEN_HOSTS = (
    "pacer.gov",
    "ecf.uscourts.gov",
    "login.gov",
    "auth0.com",
    "okta.com",
)


# ---- Seed fixture --------------------------------------------------------


def test_seed_fixture_loads_as_casepacket():
    packet = _load_fixture(SEED_PATH)
    assert packet.case_id == "real_case_min_jian_guan"


def test_seed_fixture_identity_is_high_with_full_anchors():
    packet = _load_fixture(SEED_PATH)
    ident = packet.case_identity
    assert ident.identity_confidence == "high"
    assert "Min Jian Guan" in ident.defendant_names
    assert ident.jurisdiction.city == "San Francisco"
    assert ident.jurisdiction.state == "CA"
    assert ident.agency == "San Francisco Police Department"
    assert ident.case_numbers == ["CR-1001"]
    for anchor in ("full_name", "jurisdiction", "agency", "case_number"):
        assert anchor in ident.identity_anchors


def test_seed_fixture_outcome_is_concluded():
    packet = _load_fixture(SEED_PATH)
    assert packet.case_identity.outcome_status == "sentenced"


def test_seed_fixture_has_no_pre_loaded_sources_or_artifacts():
    """Live data must have somewhere to land. The fixture
    intentionally carries no sources, claims, or artifacts."""
    packet = _load_fixture(SEED_PATH)
    assert packet.sources == []
    assert packet.artifact_claims == []
    assert packet.verified_artifacts == []


def test_seed_fixture_dry_verdict_is_hold_with_no_verified_media():
    """High identity + concluded outcome + zero artifacts -> HOLD
    with no_verified_media risk_flag."""
    packet = _load_fixture(SEED_PATH)
    result = score_case_packet(packet)
    assert result.verdict == "HOLD"
    assert "no_verified_media" in result.risk_flags
    assert "high_identity" in result.reason_codes
    assert "sentenced_or_convicted" in result.reason_codes


def test_seed_fixture_has_no_secrets():
    raw = SEED_PATH.read_text(encoding="utf-8")
    for pattern in SECRET_PATTERNS:
        assert not pattern.search(raw), f"secret pattern matched: {pattern.pattern}"


def test_seed_fixture_has_no_pacer_or_login_walled_hosts():
    raw_lower = SEED_PATH.read_text(encoding="utf-8").lower()
    for host in FORBIDDEN_HOSTS:
        assert host not in raw_lower, f"forbidden host present: {host}"


# ---- Pilot manifest entry ------------------------------------------------


def test_pilot_manifest_has_real_case_pilot_entry():
    manifest = json.loads(PILOT_MANIFEST.read_text(encoding="utf-8"))
    pilot_ids = [p["id"] for p in manifest["pilots"]]
    assert "real_case_min_jian_guan_pilot" in pilot_ids


def test_real_case_pilot_references_real_case_seed():
    manifest = json.loads(PILOT_MANIFEST.read_text(encoding="utf-8"))
    pilot = next(
        p for p in manifest["pilots"] if p["id"] == "real_case_min_jian_guan_pilot"
    )
    assert pilot["seed_fixture_path"] == "tests/fixtures/pilot_cases/real_case_min_jian_guan.json"
    assert pilot["allowed_connectors"] == ["documentcloud"]
    assert pilot["max_live_calls"] == 1
    assert pilot["max_results_per_connector"] == 5
    assert pilot["expected_verdict_without_live"] == "HOLD"


def test_real_case_pilot_keeps_safety_invariants():
    manifest = json.loads(PILOT_MANIFEST.read_text(encoding="utf-8"))
    pilot = next(
        p for p in manifest["pilots"] if p["id"] == "real_case_min_jian_guan_pilot"
    )
    assert pilot["allow_downloads"] is False
    assert pilot["allow_scraping"] is False
    assert pilot["allow_llm"] is False
    assert pilot["expected_minimum"]["media_required_for_produce"] is True
    assert pilot.get("is_real_case") is True


def test_real_case_pilot_runner_marks_as_ready_with_dry_hold_verdict():
    out = run_pilot_manifest(PILOT_MANIFEST)
    pilot_results = {r["id"]: r for r in out["results"]}
    real_case = pilot_results["real_case_min_jian_guan_pilot"]
    assert real_case["readiness_status"] == "ready_for_live_smoke"
    assert real_case["actual_dry_verdict"] == "HOLD"
    assert real_case["expected_verdict_without_live"] == "HOLD"
    assert real_case["verdict_match"] is True
    # Pre-set high identity + concluded outcome means these gates are
    # satisfied by the seed; the only missing gate is media artifact.
    assert real_case["satisfied_gates"]["identity_high"] is True
    assert real_case["satisfied_gates"]["concluded_outcome"] is True
    assert "media_artifact_present" in real_case["missing_gates"]


# ---- Selector integration -------------------------------------------------


def test_real_case_pilot_outranks_placeholder_pilots():
    """The real-case pilot should outscore the placeholder
    mpv_documentcloud_pilot. Both use documentcloud + 1 call, but the
    real-case has expected verdict HOLD (+2) vs placeholder SKIP
    (+0)."""
    out = select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    assert out["selected_pilot_id"] == "real_case_min_jian_guan_pilot"
    assert out["selection_score"] >= 132


def test_pilot_scoreboard_remains_clean_with_real_case_pilot():
    val = run_validation_manifest()
    pilot = run_pilot_manifest(PILOT_MANIFEST)
    sb = build_pilot_validation_scoreboard(validation_output=val, pilot_output=pilot)
    assert sb["pilots"]["ready_for_live"] == sb["pilots"]["total"]
    assert sb["warnings"] == []
    # documentcloud demand bumped by 1 because real-case pilot also
    # uses it.
    assert sb["connector_demand"]["documentcloud"] >= 3


# ---- Network -------------------------------------------------------------


def test_real_case_pilot_load_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    _load_fixture(SEED_PATH)
    run_pilot_manifest(PILOT_MANIFEST)
    select_pilot_for_live_smoke(manifest_path=PILOT_MANIFEST)
    assert calls == [], f"REAL1 fixture/manifest loading made {len(calls)} live HTTP call(s)"
