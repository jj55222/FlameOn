"""PILOT1 — known-case pilot manifest scaffold tests.

Asserts that ``tests/fixtures/pilot_cases/pilot_manifest.json``:

- loads as well-formed JSON with the canonical top-level shape
  (manifest_version, description, global_constraints, pilots)
- declares global_constraints that block paid connectors, downloads,
  scraping, and LLM use, and the listed allowed_free_connectors agree
  with ``casegraph.ALLOWED_FREE_CONNECTORS``
- carries every required per-pilot key
- caps every pilot's ``max_live_calls`` inside the existing live_safety
  hard-cap envelope (max_queries_hard_cap * max_connectors_hard_cap)
- caps every pilot's ``max_results_per_connector`` at MAX_RESULTS_HARD_CAP
- never names a paid connector in any pilot's allowed_connectors
- limits allowed_connectors length to MAX_CONNECTORS_HARD_CAP
- has allow_downloads / allow_scraping / allow_llm == false on every
  pilot
- has expected_minimum.media_required_for_produce == true on every
  pilot (the production gate is non-negotiable)
- only references existing fixtures via seed_fixture_path
- uses POSIX-relative paths (no backslashes, no absolute paths)
- contains no obvious secrets / credentials / private URL patterns
- contains no PACER / login-walled hosts
- has unique pilot ids
- has expected_verdict_without_live in {PRODUCE, HOLD, SKIP}
- agrees with the validation manifest where seed fixtures overlap
- round-trips through json.dumps deterministically
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    ALLOWED_FREE_CONNECTORS,
    MAX_CONNECTORS_HARD_CAP,
    MAX_QUERIES_HARD_CAP,
    MAX_RESULTS_HARD_CAP,
    PAID_CONNECTORS,
)


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "tests" / "fixtures" / "pilot_cases" / "pilot_manifest.json"


REQUIRED_PILOT_KEYS = (
    "id",
    "input_type",
    "seed_fixture_path",
    "expected_minimum",
    "allowed_connectors",
    "max_live_calls",
    "max_results_per_connector",
    "allow_resolvers",
    "allow_downloads",
    "allow_scraping",
    "allow_llm",
    "expected_verdict_without_live",
    "notes",
)


REQUIRED_EXPECTED_MIN_KEYS = (
    "identity_lock_required",
    "outcome_required",
    "media_required_for_produce",
    "artifact_types_desired",
)


VALID_INPUT_TYPES = {"structured", "youtube", "manual"}
VALID_VERDICTS = {"PRODUCE", "HOLD", "SKIP"}


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


@pytest.fixture(scope="module")
def manifest():
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


# ---- Top-level shape ------------------------------------------------------


def test_pilot_manifest_loads_with_canonical_top_level(manifest):
    assert manifest["manifest_version"] == 1
    assert isinstance(manifest["description"], str) and manifest["description"]
    assert isinstance(manifest["global_constraints"], dict)
    assert isinstance(manifest["pilots"], list)
    assert len(manifest["pilots"]) >= 3


def test_global_constraints_block_paid_downloads_scraping_llm(manifest):
    g = manifest["global_constraints"]
    assert g["allow_paid_connectors"] is False
    assert g["allow_downloads"] is False
    assert g["allow_scraping"] is False
    assert g["allow_llm"] is False
    assert int(g["max_live_calls_total_default"]) >= 1
    assert int(g["max_results_per_connector_default"]) <= MAX_RESULTS_HARD_CAP


def test_global_allowed_free_connectors_agree_with_live_safety(manifest):
    declared = set(manifest["global_constraints"]["allowed_free_connectors"])
    assert declared == set(ALLOWED_FREE_CONNECTORS), (
        f"manifest declares {sorted(declared)} but live_safety says "
        f"{sorted(ALLOWED_FREE_CONNECTORS)}"
    )
    declared_paid = set(manifest["global_constraints"]["paid_connectors_blocked"])
    assert declared_paid == set(PAID_CONNECTORS)


# ---- Per-pilot shape + constraints ----------------------------------------


def test_every_pilot_has_canonical_keys(manifest):
    for pilot in manifest["pilots"]:
        for key in REQUIRED_PILOT_KEYS:
            assert key in pilot, f"pilot {pilot.get('id')!r} missing key {key!r}"
        for key in REQUIRED_EXPECTED_MIN_KEYS:
            assert key in pilot["expected_minimum"], (
                f"pilot {pilot['id']!r} expected_minimum missing key {key!r}"
            )


def test_every_pilot_id_is_unique(manifest):
    ids = [pilot["id"] for pilot in manifest["pilots"]]
    assert len(ids) == len(set(ids))


def test_every_pilot_input_type_is_valid(manifest):
    for pilot in manifest["pilots"]:
        assert pilot["input_type"] in VALID_INPUT_TYPES, (
            f"{pilot['id']}: invalid input_type {pilot['input_type']!r}"
        )


def test_every_pilot_expected_verdict_without_live_is_valid(manifest):
    for pilot in manifest["pilots"]:
        assert pilot["expected_verdict_without_live"] in VALID_VERDICTS, (
            f"{pilot['id']}: invalid expected_verdict_without_live "
            f"{pilot['expected_verdict_without_live']!r}"
        )


def test_every_pilot_caps_max_live_calls_within_safety_envelope(manifest):
    """A pilot's max_live_calls cannot exceed the product of the
    existing live_safety hard caps - i.e. enough budget for at most
    MAX_QUERIES_HARD_CAP queries against MAX_CONNECTORS_HARD_CAP
    connectors."""
    envelope = MAX_QUERIES_HARD_CAP * MAX_CONNECTORS_HARD_CAP
    for pilot in manifest["pilots"]:
        assert isinstance(pilot["max_live_calls"], int)
        assert pilot["max_live_calls"] >= 0
        assert pilot["max_live_calls"] <= envelope, (
            f"{pilot['id']}: max_live_calls={pilot['max_live_calls']} > "
            f"envelope={envelope}"
        )


def test_every_pilot_caps_max_results_per_connector(manifest):
    for pilot in manifest["pilots"]:
        assert isinstance(pilot["max_results_per_connector"], int)
        assert 1 <= pilot["max_results_per_connector"] <= MAX_RESULTS_HARD_CAP, (
            f"{pilot['id']}: max_results_per_connector="
            f"{pilot['max_results_per_connector']} not in [1, {MAX_RESULTS_HARD_CAP}]"
        )


def test_no_pilot_lists_a_paid_connector_in_allowed_connectors(manifest):
    paid = set(PAID_CONNECTORS)
    for pilot in manifest["pilots"]:
        listed = set(pilot["allowed_connectors"])
        intersection = listed & paid
        assert not intersection, (
            f"{pilot['id']}: paid connectors leaked into allowed_connectors: "
            f"{sorted(intersection)}"
        )


def test_every_pilots_allowed_connectors_in_free_list(manifest):
    free = set(ALLOWED_FREE_CONNECTORS)
    for pilot in manifest["pilots"]:
        for conn in pilot["allowed_connectors"]:
            assert conn in free, (
                f"{pilot['id']}: allowed_connector {conn!r} not in free list "
                f"{sorted(free)}"
            )


def test_every_pilots_allowed_connectors_within_max_connectors_cap(manifest):
    for pilot in manifest["pilots"]:
        assert len(pilot["allowed_connectors"]) <= MAX_CONNECTORS_HARD_CAP, (
            f"{pilot['id']}: allowed_connectors count > MAX_CONNECTORS_HARD_CAP"
        )


def test_every_pilot_disallows_downloads_scraping_llm(manifest):
    for pilot in manifest["pilots"]:
        assert pilot["allow_downloads"] is False, f"{pilot['id']} allows downloads"
        assert pilot["allow_scraping"] is False, f"{pilot['id']} allows scraping"
        assert pilot["allow_llm"] is False, f"{pilot['id']} allows LLM"


def test_every_pilot_requires_media_for_produce(manifest):
    for pilot in manifest["pilots"]:
        assert pilot["expected_minimum"]["media_required_for_produce"] is True, (
            f"{pilot['id']}: media_required_for_produce must be true (production "
            "gate)"
        )


# ---- Seed fixture references ----------------------------------------------


def test_every_seed_fixture_path_exists_and_is_posix_relative(manifest):
    for pilot in manifest["pilots"]:
        path_str = pilot.get("seed_fixture_path")
        if path_str is None:
            # seed_fields-only pilots are allowed; if seed_fixture_path is
            # absent, ensure seed_fields is present and a dict.
            assert isinstance(pilot.get("seed_fields"), dict), (
                f"{pilot['id']}: missing both seed_fixture_path and seed_fields"
            )
            continue
        assert "\\" not in path_str, (
            f"{pilot['id']}: backslash in seed_fixture_path {path_str!r}"
        )
        assert not path_str.startswith("/"), (
            f"{pilot['id']}: absolute seed_fixture_path {path_str!r}"
        )
        assert ".." not in Path(path_str).parts, (
            f"{pilot['id']}: parent traversal in {path_str!r}"
        )
        assert path_str.startswith("tests/fixtures/"), (
            f"{pilot['id']}: seed_fixture_path must live under tests/fixtures/"
        )
        absolute = ROOT / path_str
        assert absolute.exists(), (
            f"{pilot['id']}: seed_fixture_path missing on disk: {absolute}"
        )


# ---- Security / content guards --------------------------------------------


def test_pilot_manifest_has_no_obvious_secrets():
    raw = MANIFEST_PATH.read_text(encoding="utf-8")
    matches = []
    for pattern in SECRET_PATTERNS:
        for hit in pattern.findall(raw):
            matches.append(f"{pattern.pattern} matched {hit!r}")
    assert not matches, "pilot manifest may contain secrets:\n" + "\n".join(matches)


def test_pilot_manifest_has_no_pacer_or_login_walled_hosts():
    raw_lower = MANIFEST_PATH.read_text(encoding="utf-8").lower()
    found = [host for host in FORBIDDEN_HOSTS if host in raw_lower]
    assert not found, f"pilot manifest references forbidden hosts: {found}"


def test_pilot_manifest_round_trips_through_json():
    raw = MANIFEST_PATH.read_text(encoding="utf-8")
    once = json.loads(raw)
    twice = json.loads(json.dumps(once))
    assert once == twice


# ---- Cross-manifest consistency -------------------------------------------


def test_pilot_seed_fixtures_with_validation_overlap_agree_on_verdict(manifest):
    """When a pilot uses the same seed fixture as a validation manifest
    entry, the pilot's expected_verdict_without_live must agree with
    the validation entry's expected_verdict (so the pilot's deterministic
    starting point is consistent with the validation contract)."""
    validation = json.loads(
        (ROOT / "tests" / "fixtures" / "validation_manifest.json").read_text(encoding="utf-8")
    )
    by_fixture = {entry["fixture_path"]: entry["expected_verdict"] for entry in validation["entries"]}
    drift = []
    for pilot in manifest["pilots"]:
        seed = pilot.get("seed_fixture_path")
        if seed and seed in by_fixture:
            if by_fixture[seed] != pilot["expected_verdict_without_live"]:
                drift.append(
                    f"{pilot['id']}: validation says {by_fixture[seed]}, pilot says "
                    f"{pilot['expected_verdict_without_live']}"
                )
    assert not drift, "; ".join(drift)
