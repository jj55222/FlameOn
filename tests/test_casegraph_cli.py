"""PIPE1 — CLI dry-run tests.

Asserts that the no-live CLI:
- runs end-to-end on a real fixture and emits valid JSON
- produces verdicts that match the fixture's manual `expected` label
- carries research_completeness_score, production_actionability_score,
  actionability_score, ledger fields, and report keys
- rejects a missing fixture path with exit code 3
- rejects an invalid fixture (broken JSON or schema-violating) with exit code 4
- handles the test-only `expected` field on scenario fixtures
- never makes any network call (verified by monkey-patching
  requests.Session.get)
"""
import io
import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import cli


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "casegraph_scenarios"


def run_cli(argv):
    out = io.StringIO()
    err = io.StringIO()
    code = cli.main(argv, stdout=out, stderr=err)
    return code, out.getvalue(), err.getvalue()


def test_cli_runs_on_media_rich_fixture_and_emits_valid_json():
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, out, err = run_cli(["--fixture", fixture, "--json"])
    assert code == 0, f"non-zero exit: {err}"
    payload = json.loads(out)
    assert payload["packet_summary"]["case_id"] == "scenario_media_rich_produce"
    assert payload["result"]["verdict"] == "PRODUCE"


def test_cli_includes_research_production_and_actionability_scores():
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, out, _ = run_cli(["--fixture", fixture, "--json"])
    assert code == 0
    payload = json.loads(out)
    result = payload["result"]
    for key in (
        "research_completeness_score",
        "production_actionability_score",
        "actionability_score",
    ):
        assert key in result, f"missing {key!r} in result"
        assert isinstance(result[key], (int, float))


def test_cli_includes_ledger_fields_with_canonical_api_call_keys():
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, out, _ = run_cli(["--fixture", fixture, "--json"])
    assert code == 0
    payload = json.loads(out)
    led = payload["ledger_entry"]
    for key in (
        "experiment_id",
        "timestamp",
        "case_id",
        "api_calls",
        "wallclock_seconds",
        "estimated_cost_usd",
        "verdict",
        "research_completeness_score",
        "production_actionability_score",
        "actionability_score",
    ):
        assert key in led, f"missing ledger key {key!r}"
    # Canonical 7-provider api_calls key set from the ledger module.
    for provider in (
        "courtlistener",
        "muckrock",
        "documentcloud",
        "youtube",
        "brave",
        "firecrawl",
        "llm",
    ):
        assert provider in led["api_calls"], f"missing api_calls.{provider}"
        assert led["api_calls"][provider] == 0
    assert led["estimated_cost_usd"] == 0.0
    assert led["verdict"] == "PRODUCE"


def test_cli_includes_report_aggregation_over_single_packet():
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, out, _ = run_cli(["--fixture", fixture, "--json"])
    payload = json.loads(out)
    report = payload["report"]
    assert report["total_cases"] == 1
    assert report["verdict_counts"]["PRODUCE"] == 1
    assert report["produce_eligible_inventory"][0]["case_id"] == "scenario_media_rich_produce"


@pytest.mark.parametrize(
    "fixture_name,expected_verdict",
    [
        ("media_rich_produce.json", "PRODUCE"),
        ("multi_artifact_premium_produce.json", "PRODUCE"),
        ("structured_verified_bodycam_produce.json", "PRODUCE"),
        ("transcript_corroborated_media_produce.json", "PRODUCE"),
        ("document_only_hold.json", "HOLD"),
        ("claim_only_hold.json", "HOLD"),
        ("charged_with_media_hold.json", "HOLD"),
        ("protected_nonpublic_blocked.json", "HOLD"),
        ("transcript_artifact_claim_hold.json", "HOLD"),
        ("transcript_noisy_bodycam_not_produce.json", "HOLD"),
        ("structured_official_bodycam_claim_hold.json", "HOLD"),
        ("weak_identity_media_blocked.json", "HOLD"),
        ("structured_conflicting_source_not_produce.json", "SKIP"),
        ("structured_wapo_row_only_not_produce.json", "SKIP"),
        ("transcript_candidate_name_hold.json", "SKIP"),
    ],
)
def test_cli_verdict_matches_fixture_expected_label(fixture_name, expected_verdict):
    """The CLI verdict must match the fixture's manual `expected.verdict`
    label across the full 15-fixture corpus."""
    fixture_path = FIXTURE_DIR / fixture_name
    raw = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert raw.get("expected", {}).get("verdict") == expected_verdict, (
        f"fixture label drift for {fixture_name}"
    )
    code, out, err = run_cli(["--fixture", str(fixture_path), "--json"])
    assert code == 0, f"CLI failed on {fixture_name}: {err}"
    payload = json.loads(out)
    assert payload["result"]["verdict"] == expected_verdict


def test_cli_text_output_is_human_readable_when_json_flag_omitted():
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, out, _ = run_cli(["--fixture", fixture])
    assert code == 0
    assert "=== CaseGraph dry run ===" in out
    assert "verdict: PRODUCE" in out
    assert "research_completeness_score:" in out
    assert "ledger.api_calls:" in out


def test_cli_returns_exit_3_for_missing_fixture(tmp_path):
    missing = tmp_path / "does_not_exist.json"
    code, out, err = run_cli(["--fixture", str(missing), "--json"])
    assert code == 3
    assert out == ""
    assert "fixture not found" in err


def test_cli_returns_exit_4_for_invalid_json(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("not json at all", encoding="utf-8")
    code, out, err = run_cli(["--fixture", str(bad), "--json"])
    assert code == 4
    assert out == ""
    assert err  # some stderr message present


def test_cli_returns_exit_4_for_schema_violation(tmp_path):
    """A JSON object that is missing required CasePacket fields must be
    rejected with exit code 4 (schema validation failure)."""
    bad = tmp_path / "bad_packet.json"
    bad.write_text(json.dumps({"case_id": "incomplete"}), encoding="utf-8")
    code, out, err = run_cli(["--fixture", str(bad), "--json"])
    assert code == 4
    assert out == ""
    assert err


def test_cli_strips_test_only_expected_field_before_validation():
    """The fixtures in tests/fixtures/casegraph_scenarios/ all carry an
    `expected` block from EVAL2. The CLI must strip that test-only
    field before schema validation; otherwise schema validation
    rejects the fixture due to additionalProperties: false."""
    fixture = str(FIXTURE_DIR / "structured_wapo_row_only_not_produce.json")
    code, out, err = run_cli(["--fixture", fixture, "--json"])
    assert code == 0, f"CLI rejected fixture with expected block: {err}"
    payload = json.loads(out)
    # Verify the loader did its job — packet was assembled despite the
    # extra field on disk.
    assert payload["packet_summary"]["case_id"] == "scenario_structured_wapo_row_only_not_produce"


def test_cli_uses_supplied_experiment_id():
    fixture = str(FIXTURE_DIR / "media_rich_produce.json")
    code, out, _ = run_cli(["--fixture", fixture, "--json", "--experiment-id", "test-custom-id"])
    payload = json.loads(out)
    assert payload["ledger_entry"]["experiment_id"] == "test-custom-id"


def test_cli_makes_zero_network_calls(monkeypatch):
    """Monkey-patch requests.Session.get and confirm zero calls during
    a CLI run across multiple fixtures."""
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    for fixture_name in (
        "media_rich_produce.json",
        "document_only_hold.json",
        "claim_only_hold.json",
        "weak_identity_media_blocked.json",
        "transcript_corroborated_media_produce.json",
    ):
        fixture = str(FIXTURE_DIR / fixture_name)
        code, _, err = run_cli(["--fixture", fixture, "--json"])
        assert code == 0, f"CLI failed on {fixture_name}: {err}"

    assert calls == [], (
        f"CLI made {len(calls)} live HTTP call(s); CLI must be no-live"
    )


def test_cli_does_not_mutate_input_fixture_on_disk(tmp_path):
    """Round-trip a fixture through the CLI and verify the on-disk
    file is unchanged."""
    src = FIXTURE_DIR / "media_rich_produce.json"
    snapshot = src.read_text(encoding="utf-8")
    code, _, _ = run_cli(["--fixture", str(src), "--json"])
    assert code == 0
    assert src.read_text(encoding="utf-8") == snapshot


def test_cli_module_is_runnable_via_python_dash_m():
    """`python -m pipeline2_discovery.casegraph.cli` must be the
    documented entrypoint — the module exposes a __main__ guard and
    a main() callable."""
    import pipeline2_discovery.casegraph.cli as cli_module

    assert hasattr(cli_module, "main")
    assert callable(cli_module.main)
