"""DATA2 — validation manifest runner tests.

Asserts that ``run_validation_manifest`` :

- loads the committed ``tests/fixtures/validation_manifest.json``
- runs every entry through the no-live scoring pipeline
- emits one validation result per entry with the expected canonical
  shape (id, fixture_path, expected_verdict, actual_verdict, passed,
  fail_reasons, reason_code_matches, risk_flag_matches,
  research_completeness_score, production_actionability_score,
  actionability_score, verified_artifact_count, media_artifact_count,
  document_artifact_count, identity_confidence, outcome_status,
  bundle_path)
- causes EVERY current entry to pass against the deterministic
  scoring contract
- keeps ALL false-PRODUCE guard counters at zero (no document-only
  PRODUCE, no claim-only PRODUCE, no weak-identity PRODUCE, no
  protected/pacer PRODUCE, no false PRODUCE vs the manifest's expected
  verdict)
- handles a missing fixture path gracefully (no raise; pass=False;
  fail_reasons populated; still JSON-serializable)
- handles a manifest entry whose verdict / reason codes / risk flags
  drift away from the contract (synthetic case) — fail=True with the
  specific failure reason surfaced
- writes per-entry bundles when ``bundle_dir`` is supplied; bundle
  files are well-formed JSON; never written when omitted
- output is JSON-serializable round-trip
- never makes a network call
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import run_validation_manifest, validate_entry


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "tests" / "fixtures" / "validation_manifest.json"


REQUIRED_RESULT_KEYS = (
    "id",
    "fixture_path",
    "expected_verdict",
    "actual_verdict",
    "passed",
    "fail_reasons",
    "reason_code_matches",
    "risk_flag_matches",
    "research_completeness_score",
    "production_actionability_score",
    "actionability_score",
    "verified_artifact_count",
    "media_artifact_count",
    "document_artifact_count",
    "identity_confidence",
    "outcome_status",
    "bundle_path",
)


def _load_manifest():
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def test_runner_loads_committed_manifest():
    output = run_validation_manifest(MANIFEST_PATH)
    assert output["manifest_version"] == 1
    assert output["total_entries"] == len(_load_manifest()["entries"])
    assert output["manifest_path"] == str(MANIFEST_PATH)


def test_runner_returns_canonical_top_level_keys():
    output = run_validation_manifest(MANIFEST_PATH)
    for key in ("manifest_path", "manifest_version", "total_entries", "results", "summary"):
        assert key in output


def test_every_result_has_canonical_shape():
    output = run_validation_manifest(MANIFEST_PATH)
    assert len(output["results"]) > 0
    for result in output["results"]:
        for key in REQUIRED_RESULT_KEYS:
            assert key in result, f"result {result.get('id')!r} missing key {key!r}"


def test_every_current_manifest_entry_passes():
    output = run_validation_manifest(MANIFEST_PATH)
    failures = [r for r in output["results"] if not r["passed"]]
    assert not failures, (
        "manifest entries that failed validation:\n"
        + "\n".join(f"{f['id']}: {f['fail_reasons']}" for f in failures)
    )
    assert output["summary"]["passed"] == output["total_entries"]
    assert output["summary"]["failed"] == 0


def test_false_produce_guard_counters_remain_zero():
    output = run_validation_manifest(MANIFEST_PATH)
    summary = output["summary"]
    assert summary["false_produce_count"] == 0
    assert summary["document_only_produce_count"] == 0
    assert summary["claim_only_produce_count"] == 0
    assert summary["weak_identity_produce_count"] == 0
    assert summary["protected_or_pacer_produce_count"] == 0


def test_runner_handles_missing_fixture_gracefully(tmp_path):
    bad_manifest = {
        "manifest_version": 1,
        "entries": [
            {
                "id": "missing_fixture_case",
                "fixture_path": "tests/fixtures/does_not_exist.json",
                "input_type": "manual",
                "expected_verdict": "PRODUCE",
                "must_include_reason_codes": [],
                "must_not_include_reason_codes": [],
                "must_include_risk_flags": [],
                "must_not_include_risk_flags": [],
                "notes": "synthetic missing fixture",
                "live_allowed": False,
            }
        ],
    }
    output = run_validation_manifest(manifest_dict=bad_manifest)
    assert output["total_entries"] == 1
    assert output["summary"]["failed"] == 1
    result = output["results"][0]
    assert result["passed"] is False
    assert result["actual_verdict"] is None
    assert any("fixture not found" in reason for reason in result["fail_reasons"])


def test_runner_detects_verdict_drift_in_synthetic_entry(tmp_path):
    """A synthetic manifest entry that mislabels a fixture's expected
    verdict must surface as a failure with the verdict mismatch
    explicitly named in fail_reasons."""
    synthetic = {
        "manifest_version": 1,
        "entries": [
            {
                "id": "synthetic_drift",
                # media_rich fixture really produces — claim it's HOLD.
                "fixture_path": "tests/fixtures/casegraph_scenarios/media_rich_produce.json",
                "input_type": "manual",
                "expected_verdict": "HOLD",
                "must_include_reason_codes": [],
                "must_not_include_reason_codes": [],
                "must_include_risk_flags": [],
                "must_not_include_risk_flags": [],
                "notes": "intentional drift to exercise the runner's failure path",
                "live_allowed": False,
            }
        ],
    }
    output = run_validation_manifest(manifest_dict=synthetic)
    result = output["results"][0]
    assert result["passed"] is False
    assert any(
        "verdict mismatch" in reason for reason in result["fail_reasons"]
    ), result["fail_reasons"]
    assert result["actual_verdict"] == "PRODUCE"
    assert result["expected_verdict"] == "HOLD"


def test_runner_writes_bundles_when_bundle_dir_supplied(tmp_path):
    bundle_dir = tmp_path / "bundles"
    output = run_validation_manifest(MANIFEST_PATH, bundle_dir=bundle_dir)
    for result in output["results"]:
        assert result["bundle_path"] is not None
        bundle_file = Path(result["bundle_path"])
        assert bundle_file.exists()
        bundle = json.loads(bundle_file.read_text(encoding="utf-8"))
        assert bundle["mode"] == "validation"
        assert bundle["experiment_id"] == f"DATA2-{result['id']}"


def test_runner_does_not_write_bundles_when_dir_not_supplied():
    output = run_validation_manifest(MANIFEST_PATH)
    for result in output["results"]:
        assert result["bundle_path"] is None


def test_runner_output_is_json_serializable():
    output = run_validation_manifest(MANIFEST_PATH)
    encoded = json.dumps(output)
    decoded = json.loads(encoded)
    assert decoded == output


def test_runner_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    run_validation_manifest(MANIFEST_PATH)
    assert calls == [], (
        f"validation runner triggered {len(calls)} live HTTP call(s); must be no-live"
    )


def test_validate_entry_directly_returns_per_entry_shape():
    """Direct call to ``validate_entry`` (not via the full manifest
    runner) returns the same canonical per-entry shape, so callers can
    score one entry at a time without instantiating a full manifest."""
    manifest = _load_manifest()
    entry = manifest["entries"][0]
    result = validate_entry(entry)
    for key in REQUIRED_RESULT_KEYS:
        assert key in result


def test_runner_aggregate_verdict_counts_match_results():
    output = run_validation_manifest(MANIFEST_PATH)
    summary = output["summary"]
    counted = {"PRODUCE": 0, "HOLD": 0, "SKIP": 0}
    for r in output["results"]:
        if r["actual_verdict"] in counted:
            counted[r["actual_verdict"]] += 1
    assert summary["verdict_counts"] == counted
    expected_counted = {"PRODUCE": 0, "HOLD": 0, "SKIP": 0}
    for r in output["results"]:
        if r["expected_verdict"] in expected_counted:
            expected_counted[r["expected_verdict"]] += 1
    assert summary["expected_verdict_counts"] == expected_counted


def test_runner_total_passed_and_failed_sum_to_total():
    output = run_validation_manifest(MANIFEST_PATH)
    summary = output["summary"]
    assert summary["passed"] + summary["failed"] == summary["total"]
    assert summary["total"] == output["total_entries"]
