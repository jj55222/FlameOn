"""MEDIA3 — Media-yield pilot manifest tests.

Asserts that the media-yield pilot entry
``media_case_christa_gail_pike_pilot`` and its seed fixture
``tests/fixtures/pilot_cases/media_case_christa_gail_pike.json``:

- load as a valid CasePacket fixture
- pre-lock identity to high (full_name + jurisdiction + agency
  anchors)
- declare ``outcome_status='convicted'`` (a concluded outcome)
- declare zero pre-loaded sources / claims / artifacts so live data
  has somewhere to land
- score HOLD dry with no_verified_media + high_identity +
  sentenced_or_convicted reason codes
- pass pilot manifest validation (no paid connectors, no downloads /
  scraping / LLM, media_required_for_produce=true,
  max_live_calls=1, max_results_per_connector=5)
- declare YouTube as the primary connector (consistent with the
  candidate's known YouTube-heavy verified_sources in calibration)
- mark as ``ready_for_live_smoke`` via the pilot runner with the
  dry verdict matching expected_verdict_without_live='HOLD'
- keep the pilot scoreboard clean (no warnings, all pilots ready)
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    build_pilot_validation_scoreboard,
    run_pilot_manifest,
    run_validation_manifest,
    score_case_packet,
)
from pipeline2_discovery.casegraph.cli import _load_fixture


ROOT = Path(__file__).resolve().parents[1]
SEED_PATH = ROOT / "tests" / "fixtures" / "pilot_cases" / "media_case_christa_gail_pike.json"
PILOT_MANIFEST = ROOT / "tests" / "fixtures" / "pilot_cases" / "pilot_manifest.json"


def test_media_seed_loads_as_casepacket():
    packet = _load_fixture(SEED_PATH)
    assert packet.case_id == "media_case_christa_gail_pike"


def test_media_seed_identity_is_high_with_anchors():
    packet = _load_fixture(SEED_PATH)
    ident = packet.case_identity
    assert ident.identity_confidence == "high"
    assert "Christa Gail Pike" in ident.defendant_names
    assert ident.jurisdiction.city == "Knoxville"
    assert ident.jurisdiction.state == "Tennessee"
    for anchor in ("full_name", "jurisdiction", "agency"):
        assert anchor in ident.identity_anchors


def test_media_seed_outcome_is_concluded_convicted():
    packet = _load_fixture(SEED_PATH)
    assert packet.case_identity.outcome_status == "convicted"


def test_media_seed_has_no_pre_loaded_artifacts():
    packet = _load_fixture(SEED_PATH)
    assert packet.sources == []
    assert packet.artifact_claims == []
    assert packet.verified_artifacts == []


def test_media_seed_dry_verdict_is_hold_with_no_verified_media():
    packet = _load_fixture(SEED_PATH)
    result = score_case_packet(packet)
    assert result.verdict == "HOLD"
    assert "no_verified_media" in result.risk_flags
    assert "high_identity" in result.reason_codes
    assert "sentenced_or_convicted" in result.reason_codes


def test_media_pilot_entry_exists_in_manifest():
    manifest = json.loads(PILOT_MANIFEST.read_text(encoding="utf-8"))
    pilot = next(
        (p for p in manifest["pilots"] if p["id"] == "media_case_christa_gail_pike_pilot"),
        None,
    )
    assert pilot is not None
    assert pilot["seed_fixture_path"] == "tests/fixtures/pilot_cases/media_case_christa_gail_pike.json"


def test_media_pilot_uses_youtube_as_primary_connector():
    manifest = json.loads(PILOT_MANIFEST.read_text(encoding="utf-8"))
    pilot = next(
        p for p in manifest["pilots"] if p["id"] == "media_case_christa_gail_pike_pilot"
    )
    assert pilot["allowed_connectors"] == ["youtube"]
    assert pilot["max_live_calls"] == 1
    assert pilot["max_results_per_connector"] == 5
    assert pilot["expected_verdict_without_live"] == "HOLD"


def test_media_pilot_safety_invariants():
    manifest = json.loads(PILOT_MANIFEST.read_text(encoding="utf-8"))
    pilot = next(
        p for p in manifest["pilots"] if p["id"] == "media_case_christa_gail_pike_pilot"
    )
    assert pilot["allow_downloads"] is False
    assert pilot["allow_scraping"] is False
    assert pilot["allow_llm"] is False
    assert pilot["expected_minimum"]["media_required_for_produce"] is True
    assert pilot.get("is_real_case") is True
    artifacts_desired = pilot["expected_minimum"]["artifact_types_desired"]
    # Media pilot should target media artifact types.
    media_targets = {"bodycam", "court_video", "interrogation", "dispatch_911", "video_footage"}
    assert media_targets & set(artifacts_desired)


def test_media_pilot_runner_marks_as_ready_with_hold():
    out = run_pilot_manifest(PILOT_MANIFEST)
    pilot = {r["id"]: r for r in out["results"]}["media_case_christa_gail_pike_pilot"]
    assert pilot["readiness_status"] == "ready_for_live_smoke"
    assert pilot["actual_dry_verdict"] == "HOLD"
    assert pilot["expected_verdict_without_live"] == "HOLD"
    assert pilot["verdict_match"] is True
    assert pilot["satisfied_gates"]["identity_high"] is True
    assert pilot["satisfied_gates"]["concluded_outcome"] is True
    assert "media_artifact_present" in pilot["missing_gates"]


def test_media_pilot_keeps_scoreboard_clean():
    val = run_validation_manifest()
    pilot = run_pilot_manifest(PILOT_MANIFEST)
    sb = build_pilot_validation_scoreboard(validation_output=val, pilot_output=pilot)
    assert sb["pilots"]["ready_for_live"] == sb["pilots"]["total"]
    assert sb["warnings"] == []
    # YouTube demand bumped by adding the media pilot.
    assert sb["connector_demand"].get("youtube", 0) >= 1


def test_media_pilot_seed_has_no_obvious_secrets():
    raw = SEED_PATH.read_text(encoding="utf-8")
    import re
    for pattern in (
        r"AKIA[0-9A-Z]{16}",
        r"AIza[0-9A-Za-z_-]{35}",
        r"sk-[A-Za-z0-9]{40,}",
        r"-----BEGIN [A-Z ]+PRIVATE KEY-----",
    ):
        assert not re.search(pattern, raw), f"secret pattern matched: {pattern}"


def test_media_pilot_makes_zero_network_calls(monkeypatch):
    import requests
    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    _load_fixture(SEED_PATH)
    run_pilot_manifest(PILOT_MANIFEST)
    assert calls == []
