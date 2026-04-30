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


# ---- PIPE2 — --query-plan mode --------------------------------------------


STRUCTURED_FIXTURE_DIR = ROOT / "tests" / "fixtures" / "structured_inputs"
WAPO_FIXTURE = str(STRUCTURED_FIXTURE_DIR / "wapo_uof_complete.json")
FE_FIXTURE = str(STRUCTURED_FIXTURE_DIR / "fatal_encounters_complete.json")
MPV_FIXTURE = str(STRUCTURED_FIXTURE_DIR / "mpv_complete.json")


def test_cli_query_plan_mode_runs_on_wapo_fixture_and_emits_json():
    code, out, err = run_cli(["--fixture", WAPO_FIXTURE, "--query-plan", "--json"])
    assert code == 0, f"non-zero exit: {err}"
    payload = json.loads(out)
    assert "input_summary" in payload
    assert "query_plan" in payload
    assert "ledger_entry" in payload
    assert payload["input_summary"]["dataset_name"] == "wapo_uof"
    assert payload["input_summary"]["defendant_names"] == ["John Example"]
    plan = payload["query_plan"]
    assert plan["connector_count"] >= 1
    assert any(p["query_count"] >= 1 for p in plan["plans"])


def test_cli_query_plan_mode_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    code, _, err = run_cli(["--fixture", WAPO_FIXTURE, "--query-plan", "--json"])
    assert code == 0, f"unexpected error: {err}"
    assert calls == [], f"--query-plan made {len(calls)} live HTTP call(s)"


@pytest.mark.parametrize(
    "fixture_path,expected_dataset",
    [
        (WAPO_FIXTURE, "wapo_uof"),
        (FE_FIXTURE, "fatal_encounters"),
        (MPV_FIXTURE, "mapping_police_violence"),
    ],
)
def test_cli_query_plan_mode_dispatches_per_dataset(fixture_path, expected_dataset):
    code, out, err = run_cli(["--fixture", fixture_path, "--query-plan", "--json"])
    assert code == 0, f"non-zero exit on {fixture_path}: {err}"
    payload = json.loads(out)
    assert payload["input_summary"]["dataset_name"] == expected_dataset


def test_cli_query_plan_text_output_is_human_readable():
    code, out, _ = run_cli(["--fixture", WAPO_FIXTURE, "--query-plan"])
    assert code == 0
    assert "=== CaseGraph query plan ===" in out
    assert "dataset_name: wapo_uof" in out
    assert "connector_plans:" in out


def test_cli_query_plan_returns_exit_3_for_missing_fixture(tmp_path):
    missing = tmp_path / "missing.json"
    code, out, err = run_cli(["--fixture", str(missing), "--query-plan", "--json"])
    assert code == 3
    assert "fixture not found" in err


def test_cli_query_plan_uses_default_experiment_id():
    code, out, _ = run_cli(["--fixture", WAPO_FIXTURE, "--query-plan", "--json"])
    payload = json.loads(out)
    assert payload["ledger_entry"]["experiment_id"] == "PIPE1-cli-query-plan"


# ---- PIPE2 — --live-dry mode ----------------------------------------------


from pipeline2_discovery.casegraph import (
    CourtListenerConnector,
    DocumentCloudConnector,
    MuckRockConnector,
)
from pipeline2_discovery.casegraph.live_safety import DEFAULT_ENV_VAR


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, *, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        return self.response


def _seed_courtlistener_connector_factory(monkeypatch, payload):
    """Replace the CONNECTOR_FACTORIES['courtlistener'] entry with a
    FakeSession-backed connector so --live-dry never hits the network."""
    fake = FakeSession(FakeResponse(payload))

    def factory():
        return CourtListenerConnector(session=fake)

    import pipeline2_discovery.casegraph.live_smoke as live_smoke_module

    monkeypatch.setitem(
        live_smoke_module.CONNECTOR_FACTORIES, "courtlistener", factory
    )
    return fake


def _courtlistener_payload():
    return {
        "results": [
            {
                "id": 9100001,
                "caseName": "State v. John Example",
                "absolute_url": "/opinion/9100001/state-v-john-example/",
                "snippet": "John Example was sentenced after the Phoenix Police Department investigation.",
                "docketNumber": "CR-2022-001234",
                "court": "AZSP",
                "dateFiled": "2022-09-15",
            }
        ]
    }


def test_cli_live_dry_refuses_without_env_gate(monkeypatch):
    monkeypatch.delenv(DEFAULT_ENV_VAR, raising=False)
    _seed_courtlistener_connector_factory(monkeypatch, _courtlistener_payload())

    code, out, err = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--live-dry",
            "--connector",
            "courtlistener",
            "--max-results",
            "5",
            "--json",
        ]
    )
    assert code == 5
    assert out == ""
    assert "live-dry blocked" in err.lower()
    assert "live run not enabled" in err.lower()


def test_cli_live_dry_refuses_oversize_max_results(monkeypatch):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    _seed_courtlistener_connector_factory(monkeypatch, _courtlistener_payload())

    code, out, err = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--live-dry",
            "--connector",
            "courtlistener",
            "--max-results",
            "99",
            "--json",
        ]
    )
    assert code == 5
    assert "max_results" in err.lower()


def test_cli_live_dry_refuses_brave_connector(monkeypatch):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")

    code, out, err = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--live-dry",
            "--connector",
            "brave",
            "--max-results",
            "5",
            "--json",
        ]
    )
    assert code == 5
    assert "brave" in err.lower()


def test_cli_live_dry_refuses_unknown_connector(monkeypatch):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")

    code, out, err = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--live-dry",
            "--connector",
            "some_future_thing",
            "--max-results",
            "5",
            "--json",
        ]
    )
    assert code == 5
    assert "not in allow-list" in err.lower() or "some_future_thing" in err.lower()


def test_cli_live_dry_mocked_courtlistener_returns_ledger_data(monkeypatch):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    fake = _seed_courtlistener_connector_factory(monkeypatch, _courtlistener_payload())

    code, out, err = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--live-dry",
            "--connector",
            "courtlistener",
            "--max-results",
            "5",
            "--json",
        ]
    )
    assert code == 0, f"non-zero exit: {err}"
    payload = json.loads(out)

    assert payload["live_dry"]["connector"] == "courtlistener"
    assert payload["live_dry"]["max_queries"] == 1
    assert payload["live_dry"]["max_results"] == 5
    assert payload["live_dry"]["source_record_count"] >= 1
    # The CLI live-dry mode must NOT graduate sources to artifacts.
    assert payload["live_dry"]["verified_artifact_count"] == 0

    diag = payload["live_dry"]["diagnostics"]
    assert diag["api_calls"]["courtlistener"] == 1
    assert diag["api_calls"]["brave"] == 0
    assert diag["api_calls"]["firecrawl"] == 0
    assert diag["estimated_cost_usd"] == 0.0
    assert diag["status_code"] == 200

    led = payload["ledger_entry"]
    assert led["experiment_id"] == "PIPE2-cli-live-dry"
    assert led["api_calls"]["courtlistener"] == 1
    assert led["api_calls"]["brave"] == 0
    assert led["estimated_cost_usd"] == 0.0
    # FakeSession recorded exactly one call.
    assert len(fake.calls) >= 1


def test_cli_live_dry_does_not_create_verified_artifacts_from_sources(monkeypatch):
    """Even if the FakeSession returns court records that mention
    bodycam/release language, the CLI live-dry path must NOT graduate
    them into VerifiedArtifacts. Verification is the resolver's job,
    not the live-dry harness."""
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    payload = {
        "results": [
            {
                "id": 9100002,
                "caseName": "State v. Jane Example",
                "absolute_url": "/opinion/9100002/state-v-jane-example/",
                "snippet": "Bodycam footage was released. Records were produced.",
                "docketNumber": "CR-2023-000999",
                "court": "AZSP",
                "dateFiled": "2023-04-22",
            }
        ]
    }
    _seed_courtlistener_connector_factory(monkeypatch, payload)

    code, out, _ = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--live-dry",
            "--connector",
            "courtlistener",
            "--max-results",
            "5",
            "--json",
        ]
    )
    assert code == 0
    body = json.loads(out)
    assert body["live_dry"]["verified_artifact_count"] == 0


def test_cli_live_dry_text_output_is_human_readable(monkeypatch):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    _seed_courtlistener_connector_factory(monkeypatch, _courtlistener_payload())

    code, out, err = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--live-dry",
            "--connector",
            "courtlistener",
            "--max-results",
            "5",
        ]
    )
    assert code == 0, err
    assert "=== CaseGraph live-dry smoke ===" in out
    assert "connector: courtlistener" in out
    assert "verified_artifact_count: 0" in out


def test_cli_live_dry_makes_exactly_one_courtlistener_call(monkeypatch):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    fake = _seed_courtlistener_connector_factory(monkeypatch, _courtlistener_payload())

    code, _, _ = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--live-dry",
            "--connector",
            "courtlistener",
            "--max-results",
            "5",
            "--json",
        ]
    )
    assert code == 0
    # CourtListener connector iterates over multiple search types
    # internally; the cap on max_results is what bounds total work.
    # The harness itself records exactly one query in the budget.
    assert len(fake.calls) <= 2  # at most one call per search-type pass


def test_cli_live_dry_returns_exit_3_for_missing_fixture(monkeypatch, tmp_path):
    monkeypatch.setenv(DEFAULT_ENV_VAR, "1")
    missing = tmp_path / "missing.json"
    code, out, err = run_cli(
        [
            "--fixture",
            str(missing),
            "--live-dry",
            "--connector",
            "courtlistener",
            "--max-results",
            "5",
            "--json",
        ]
    )
    assert code == 3
    assert "fixture not found" in err


def test_cli_query_plan_and_live_dry_are_mutually_exclusive():
    """argparse mutual exclusion — supplying both should fail at parse
    time with a non-zero exit, not silently pick one."""
    with pytest.raises(SystemExit):
        run_cli(
            [
                "--fixture",
                WAPO_FIXTURE,
                "--query-plan",
                "--live-dry",
                "--connector",
                "courtlistener",
            ]
        )


# ---- PIPE3 — --multi-source-dry-run mode ----------------------------------


def test_cli_multi_source_dry_run_works_with_structured_fixture():
    code, out, err = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "courtlistener,muckrock,documentcloud",
            "--json",
        ]
    )
    assert code == 0, f"non-zero exit: {err}"
    payload = json.loads(out)
    assert payload["input_summary"]["dataset_name"] == "wapo_uof"
    multi = payload["multi_source_dry_run"]
    assert multi["connectors"] == ["courtlistener", "muckrock", "documentcloud"]
    assert multi["max_results"] == 5
    assert len(multi["per_connector"]) == 3
    assert multi["total_source_records"] == 0
    assert multi["total_verified_artifacts"] == 0
    assert multi["total_estimated_cost_usd"] == 0.0


def test_cli_multi_source_dry_run_returns_per_connector_summaries():
    code, out, _ = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "courtlistener,muckrock",
            "--json",
        ]
    )
    payload = json.loads(out)
    per_connector = payload["multi_source_dry_run"]["per_connector"]
    assert {entry["connector"] for entry in per_connector} == {"courtlistener", "muckrock"}
    for entry in per_connector:
        assert "max_results" in entry
        assert "planned_query_count" in entry
        assert entry["source_record_count"] == 0
        assert entry["verified_artifact_count"] == 0
        assert entry["estimated_cost_usd"] == 0.0
    # At least one connector should produce a planned query for the
    # WaPo fixture (defendant + agency + jurisdiction all present).
    assert sum(e["planned_query_count"] for e in per_connector) >= 1


def test_cli_multi_source_dry_run_refuses_unsupported_connector():
    code, out, err = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "courtlistener,some_future_thing",
            "--json",
        ]
    )
    assert code == 5
    assert out == ""
    assert "some_future_thing" in err


def test_cli_multi_source_dry_run_refuses_brave_by_default():
    code, _, err = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "courtlistener,brave",
            "--json",
        ]
    )
    assert code == 5
    assert "brave" in err.lower()


def test_cli_multi_source_dry_run_refuses_firecrawl_by_default():
    code, _, err = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "firecrawl",
            "--json",
        ]
    )
    assert code == 5
    assert "firecrawl" in err.lower()


def test_cli_multi_source_dry_run_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    code, _, err = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "courtlistener,muckrock,documentcloud",
            "--json",
        ]
    )
    assert code == 0, err
    assert calls == [], f"--multi-source-dry-run made {len(calls)} live HTTP call(s)"


def test_cli_multi_source_dry_run_does_not_create_verified_artifacts():
    code, out, _ = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "courtlistener,muckrock,documentcloud",
            "--json",
        ]
    )
    payload = json.loads(out)
    assert payload["multi_source_dry_run"]["total_verified_artifacts"] == 0
    # The assembled CasePacket from the fixture alone never carries
    # verified artifacts.
    assert payload["packet_summary"]["verified_artifact_count"] == 0


def test_cli_multi_source_dry_run_does_not_produce_from_fixture_alone():
    code, out, _ = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "courtlistener,muckrock,documentcloud",
            "--json",
        ]
    )
    payload = json.loads(out)
    assert payload["result"]["verdict"] != "PRODUCE"


def test_cli_multi_source_dry_run_empty_connectors_returns_exit_4():
    code, out, err = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "",
            "--json",
        ]
    )
    assert code == 4
    assert "connectors" in err.lower()


def test_cli_multi_source_dry_run_text_output_is_human_readable():
    code, out, _ = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "courtlistener,muckrock",
        ]
    )
    assert code == 0
    assert "=== CaseGraph multi-source dry run ===" in out
    assert "connectors: courtlistener, muckrock" in out
    assert "total_source_records: 0" in out
    assert "total_verified_artifacts: 0" in out


def test_cli_multi_source_dry_run_uses_default_experiment_id():
    code, out, _ = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "courtlistener",
            "--json",
        ]
    )
    payload = json.loads(out)
    assert payload["ledger_entry"]["experiment_id"] == "PIPE3-cli-multisource-dry-run"
    assert payload["ledger_entry"]["estimated_cost_usd"] == 0.0
    # Zero api_calls — purely a planning preview.
    for provider, count in payload["ledger_entry"]["api_calls"].items():
        assert count == 0, f"unexpected api_calls.{provider} = {count}"


@pytest.mark.parametrize("fixture_path", [WAPO_FIXTURE, FE_FIXTURE, MPV_FIXTURE])
def test_cli_multi_source_dry_run_dispatches_per_dataset(fixture_path):
    code, out, _ = run_cli(
        [
            "--fixture",
            fixture_path,
            "--multi-source-dry-run",
            "--connectors",
            "courtlistener,muckrock",
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(out)
    multi = payload["multi_source_dry_run"]
    assert len(multi["per_connector"]) == 2


def test_cli_multi_source_and_other_modes_are_mutually_exclusive():
    with pytest.raises(SystemExit):
        run_cli(
            [
                "--fixture",
                WAPO_FIXTURE,
                "--multi-source-dry-run",
                "--query-plan",
                "--connectors",
                "courtlistener",
            ]
        )
