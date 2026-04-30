"""DATA1 — validation manifest tests.

Asserts that ``tests/fixtures/validation_manifest.json``:

- loads as valid JSON with the canonical top-level shape
- references only fixtures that actually exist on disk
- uses repo-relative POSIX paths (no backslashes, no absolute paths,
  no parent-traversal segments)
- carries ``live_allowed: false`` on every entry, defaulting to false
  at the manifest level
- agrees with each fixture's internal ``expected.verdict`` label
  (when the fixture carries one) — so the manifest cannot drift from
  the source-of-truth fixture metadata
- agrees with the verdict produced by :func:`score_case_packet` when
  the CLI runs against the named fixture in default (CasePacket) mode
- contains no obvious secret / credential strings
- contains entries covering every required category from the DATA1
  spec (media-rich PRODUCE, multi-artifact premium PRODUCE,
  document-only HOLD, claim-only HOLD, weak-identity HOLD,
  protected/nonpublic HOLD, structured-row-alone SKIP, weak transcript
  HOLD)
"""
from __future__ import annotations

import io
import json
import re
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import cli


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "tests" / "fixtures" / "validation_manifest.json"


REQUIRED_CATEGORIES = (
    "media_rich_PRODUCE",
    "multi_artifact_premium_PRODUCE",
    "document_only_HOLD",
    "claim_only_HOLD",
    "weak_identity_HOLD",
    "protected_nonpublic_HOLD",
    "structured_row_alone_SKIP",
    "weak_input_claim_only_HOLD",
)


REQUIRED_ENTRY_KEYS = (
    "id",
    "category",
    "fixture_path",
    "input_type",
    "expected_verdict",
    "must_include_reason_codes",
    "must_not_include_reason_codes",
    "must_include_risk_flags",
    "must_not_include_risk_flags",
    "notes",
    "live_allowed",
)


SECRET_PATTERNS = (
    re.compile(r"AKIA[0-9A-Z]{16}"),                # AWS access key
    re.compile(r"AIza[0-9A-Za-z_-]{35}"),           # Google API key
    re.compile(r"sk-[A-Za-z0-9]{40,}"),             # OpenAI-style secret
    re.compile(r"-----BEGIN [A-Z ]+PRIVATE KEY-----"),
    re.compile(r"BSAcl[A-Za-z0-9_-]{20,}"),         # Brave-style key prefix
)


def run_cli(argv):
    out = io.StringIO()
    err = io.StringIO()
    code = cli.main(argv, stdout=out, stderr=err)
    return code, out.getvalue(), err.getvalue()


@pytest.fixture(scope="module")
def manifest():
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def test_manifest_loads_with_canonical_top_level_keys(manifest):
    assert manifest["manifest_version"] == 1
    assert isinstance(manifest["entries"], list)
    assert len(manifest["entries"]) >= 8
    assert manifest["live_allowed_default"] is False


def test_every_entry_has_canonical_keys(manifest):
    for entry in manifest["entries"]:
        for key in REQUIRED_ENTRY_KEYS:
            assert key in entry, f"entry {entry.get('id')!r} missing {key!r}"


def test_entry_ids_are_unique(manifest):
    ids = [entry["id"] for entry in manifest["entries"]]
    assert len(ids) == len(set(ids)), f"duplicate manifest ids: {ids}"


def test_every_referenced_fixture_exists(manifest):
    missing = []
    for entry in manifest["entries"]:
        path = ROOT / entry["fixture_path"]
        if not path.exists():
            missing.append(entry["fixture_path"])
    assert not missing, f"manifest references missing fixtures: {missing}"


def test_fixture_paths_are_relative_posix(manifest):
    """Manifest paths must be POSIX-relative so the manifest is portable
    across Windows / macOS / Linux. No backslashes, no absolute paths,
    no parent-traversal segments."""
    for entry in manifest["entries"]:
        path = entry["fixture_path"]
        assert "\\" not in path, f"manifest entry uses backslash: {path}"
        assert not path.startswith("/"), f"manifest entry is absolute: {path}"
        assert ".." not in Path(path).parts, f"manifest entry traverses up: {path}"
        assert path.startswith("tests/fixtures/"), (
            f"manifest entry must live under tests/fixtures/: {path}"
        )


def test_every_entry_has_live_allowed_false(manifest):
    """The manifest never authorizes live network calls. live_allowed
    must always be the boolean False on every entry."""
    for entry in manifest["entries"]:
        assert entry["live_allowed"] is False, (
            f"entry {entry['id']!r} has live_allowed != False"
        )


def test_expected_verdict_is_one_of_three_values(manifest):
    allowed = {"PRODUCE", "HOLD", "SKIP"}
    for entry in manifest["entries"]:
        assert entry["expected_verdict"] in allowed, (
            f"entry {entry['id']!r} has invalid expected_verdict "
            f"{entry['expected_verdict']!r}"
        )


def test_all_required_categories_are_covered(manifest):
    categories = {entry["category"] for entry in manifest["entries"]}
    missing = set(REQUIRED_CATEGORIES) - categories
    assert not missing, f"manifest is missing required categories: {sorted(missing)}"


def test_manifest_verdict_matches_fixture_expected_label(manifest):
    """When a fixture carries an internal `expected.verdict` block, it
    MUST agree with the manifest's expected_verdict for that fixture
    — that's the contract that prevents the manifest from drifting
    away from the fixture's own self-described shape."""
    drift = []
    for entry in manifest["entries"]:
        path = ROOT / entry["fixture_path"]
        raw = json.loads(path.read_text(encoding="utf-8"))
        fixture_expected = (raw.get("expected") or {}).get("verdict")
        if fixture_expected is None:
            continue  # fixture has no internal expected — manifest is the truth
        if fixture_expected != entry["expected_verdict"]:
            drift.append(
                f"{entry['id']}: fixture says {fixture_expected!r}, "
                f"manifest says {entry['expected_verdict']!r}"
            )
    assert not drift, "manifest drifted from fixture expected.verdict labels: " + "; ".join(drift)


def test_cli_verdict_matches_manifest_expected(manifest):
    """Round-trip: run the CLI on each fixture and confirm the scored
    verdict matches the manifest's expected_verdict. This is the
    deterministic floor — if this breaks, scoring drifted from the
    manifest contract."""
    failures = []
    for entry in manifest["entries"]:
        fixture_path = str(ROOT / entry["fixture_path"])
        code, out, err = run_cli(["--fixture", fixture_path, "--json"])
        if code != 0:
            failures.append(f"{entry['id']}: CLI exit {code}: {err}")
            continue
        payload = json.loads(out)
        verdict = payload["result"]["verdict"]
        if verdict != entry["expected_verdict"]:
            failures.append(
                f"{entry['id']}: CLI got {verdict!r}, manifest expected "
                f"{entry['expected_verdict']!r}"
            )
    assert not failures, "manifest verdict mismatches:\n" + "\n".join(failures)


def test_cli_reason_codes_match_manifest_must_includes(manifest):
    """The fixtures listed in the manifest must produce every reason
    code in must_include_reason_codes and zero codes from
    must_not_include_reason_codes."""
    failures = []
    for entry in manifest["entries"]:
        fixture_path = str(ROOT / entry["fixture_path"])
        code, out, err = run_cli(["--fixture", fixture_path, "--json"])
        assert code == 0, f"{entry['id']}: CLI exit {code}: {err}"
        payload = json.loads(out)
        codes = set(payload["result"]["reason_codes"])
        for required in entry["must_include_reason_codes"]:
            if required not in codes:
                failures.append(
                    f"{entry['id']}: missing required reason_code {required!r}"
                )
        for forbidden in entry["must_not_include_reason_codes"]:
            if forbidden in codes:
                failures.append(
                    f"{entry['id']}: contains forbidden reason_code {forbidden!r}"
                )
    assert not failures, "reason code mismatches:\n" + "\n".join(failures)


def test_cli_risk_flags_match_manifest_must_includes(manifest):
    failures = []
    for entry in manifest["entries"]:
        fixture_path = str(ROOT / entry["fixture_path"])
        code, out, err = run_cli(["--fixture", fixture_path, "--json"])
        assert code == 0, f"{entry['id']}: CLI exit {code}: {err}"
        payload = json.loads(out)
        flags = set(payload["result"]["risk_flags"])
        for required in entry["must_include_risk_flags"]:
            if required not in flags:
                failures.append(
                    f"{entry['id']}: missing required risk_flag {required!r}"
                )
        for forbidden in entry["must_not_include_risk_flags"]:
            if forbidden in flags:
                failures.append(
                    f"{entry['id']}: contains forbidden risk_flag {forbidden!r}"
                )
    assert not failures, "risk flag mismatches:\n" + "\n".join(failures)


def test_manifest_has_no_obvious_secrets():
    """Scan the raw manifest text for common secret patterns. The
    manifest should never contain API keys, private keys, or
    credentials of any kind."""
    raw = MANIFEST_PATH.read_text(encoding="utf-8")
    matches = []
    for pattern in SECRET_PATTERNS:
        for hit in pattern.findall(raw):
            matches.append(f"{pattern.pattern} matched {hit!r}")
    assert not matches, "manifest may contain secrets:\n" + "\n".join(matches)


def test_manifest_has_no_pacer_or_login_walled_urls():
    """The manifest itself never names a PACER or login-walled URL.
    (Fixtures may contain these to model the protected_nonpublic case
    — the manifest, as the index, doesn't.)"""
    raw = MANIFEST_PATH.read_text(encoding="utf-8")
    forbidden = ("pacer.gov", "ecf.uscourts.gov", "login.gov", "auth0.com")
    found = [needle for needle in forbidden if needle in raw.lower()]
    assert not found, f"manifest contains forbidden host references: {found}"


def test_manifest_round_trips_through_json():
    """Sanity: manifest must be deterministic JSON — load + dump +
    re-load should produce the same object."""
    raw = MANIFEST_PATH.read_text(encoding="utf-8")
    once = json.loads(raw)
    twice = json.loads(json.dumps(once))
    assert once == twice


def test_manifest_run_makes_zero_network_calls(monkeypatch, manifest):
    """Running the CLI on every manifest entry must make zero network
    calls."""
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    for entry in manifest["entries"]:
        fixture_path = str(ROOT / entry["fixture_path"])
        code, _, err = run_cli(["--fixture", fixture_path, "--json"])
        assert code == 0, f"{entry['id']}: CLI exit {code}: {err}"
    assert calls == [], (
        f"manifest CLI sweep made {len(calls)} live HTTP call(s); must be no-live"
    )
