"""EVAL5 — pure-no-live comparison report tests.

Asserts that ``compare_run_bundles`` (the pure dry-vs-enriched bundle
diff) emits a deterministic, JSON-serializable structure that:

- compares two minimal bundles end-to-end
- detects source count increases (per-api and total)
- detects document and media artifact yield changes
- correctly classifies media vs document artifacts
- reports verdict change vs unchanged
- reports cost / runtime deltas
- reports added / removed query strings and connector names
- emits a ``why_not_produce`` summary when the enriched bundle is HOLD
- omits ``why_not_produce`` when the enriched bundle is PRODUCE
- handles bundles with missing / null sections without raising
- returns JSON-serializable output (round-trips through json.dumps)
- compares two real CLI bundles (default vs multi-source-dry-run)
- never makes a network call
"""
from __future__ import annotations

import io
import json
from pathlib import Path

from pipeline2_discovery.casegraph import cli, compare_run_bundles


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "casegraph_scenarios"
STRUCTURED_FIXTURE_DIR = ROOT / "tests" / "fixtures" / "structured_inputs"
WAPO_FIXTURE = str(STRUCTURED_FIXTURE_DIR / "wapo_uof_complete.json")


def run_cli(argv):
    out = io.StringIO()
    err = io.StringIO()
    code = cli.main(argv, stdout=out, stderr=err)
    return code, out.getvalue(), err.getvalue()


def _empty_bundle(experiment_id: str = "exp", mode: str = "default"):
    return {
        "experiment_id": experiment_id,
        "mode": mode,
        "wallclock_seconds": 0.0,
        "input_summary": {},
        "query_plan": None,
        "connector_summary": None,
        "multi_source_summary": None,
        "smoke_diagnostics": None,
        "identity": None,
        "outcome": None,
        "artifact_claims": [],
        "verified_artifacts": [],
        "result": None,
        "actionability_report": None,
        "live_yield_report": None,
        "ledger_entry": {
            "experiment_id": experiment_id,
            "wallclock_seconds": 0.0,
            "estimated_cost_usd": 0.0,
            "api_calls": {
                "courtlistener": 0,
                "muckrock": 0,
                "documentcloud": 0,
                "youtube": 0,
                "brave": 0,
                "firecrawl": 0,
                "llm": 0,
            },
        },
        "next_actions": [],
        "risk_flags": [],
    }


# ---- Synthetic bundle tests ------------------------------------------------


def test_compare_returns_canonical_top_level_keys():
    diff = compare_run_bundles(_empty_bundle("dry"), _empty_bundle("enriched"))
    for key in (
        "experiment_ids",
        "modes",
        "query_plan_delta",
        "source_count_delta",
        "provider_yield_delta",
        "artifact_yield_delta",
        "cost_delta",
        "runtime_delta",
        "verdict_change",
        "reason_code_delta",
        "risk_flag_delta",
        "why_not_produce",
    ):
        assert key in diff, f"missing top-level key {key!r}"


def test_compare_detects_source_count_increase():
    dry = _empty_bundle("dry")
    enriched = _empty_bundle("enriched")
    enriched["connector_summary"] = {
        "total_source_records": 5,
        "by_api": {"courtlistener": 2, "documentcloud": 3},
        "by_role": {},
        "source_ids": ["a", "b", "c", "d", "e"],
    }
    diff = compare_run_bundles(dry, enriched)
    src = diff["source_count_delta"]
    assert src["dry"] == 0
    assert src["enriched"] == 5
    assert src["delta"] == 5
    assert src["by_api"]["courtlistener"]["delta"] == 2
    assert src["by_api"]["documentcloud"]["delta"] == 3


def test_compare_detects_document_artifact_increase_with_no_media():
    dry = _empty_bundle("dry")
    enriched = _empty_bundle("enriched")
    enriched["verified_artifacts"] = [
        {"artifact_type": "docket_docs", "format": "pdf"},
        {"artifact_type": "docket_docs", "format": "pdf"},
    ]
    diff = compare_run_bundles(dry, enriched)
    art = diff["artifact_yield_delta"]
    assert art["dry_total"] == 0
    assert art["enriched_total"] == 2
    assert art["delta"] == 2
    assert art["document_delta"]["delta"] == 2
    assert art["media_delta"]["delta"] == 0
    assert art["by_type"]["docket_docs"]["enriched"] == 2


def test_compare_detects_media_artifact_increase():
    dry = _empty_bundle("dry")
    enriched = _empty_bundle("enriched")
    enriched["verified_artifacts"] = [
        {"artifact_type": "bodycam", "format": "video"},
        {"artifact_type": "dispatch_911", "format": "audio"},
    ]
    diff = compare_run_bundles(dry, enriched)
    art = diff["artifact_yield_delta"]
    assert art["media_delta"]["delta"] == 2
    assert art["document_delta"]["delta"] == 0


def test_compare_reports_verdict_unchanged_hold():
    dry = _empty_bundle("dry")
    dry["result"] = {"verdict": "HOLD", "reason_codes": [], "risk_flags": []}
    enriched = _empty_bundle("enriched")
    enriched["result"] = {"verdict": "HOLD", "reason_codes": [], "risk_flags": []}
    diff = compare_run_bundles(dry, enriched)
    assert diff["verdict_change"]["dry"] == "HOLD"
    assert diff["verdict_change"]["enriched"] == "HOLD"
    assert diff["verdict_change"]["changed"] is False


def test_compare_reports_verdict_changed_hold_to_produce():
    dry = _empty_bundle("dry")
    dry["result"] = {"verdict": "HOLD", "reason_codes": [], "risk_flags": []}
    enriched = _empty_bundle("enriched")
    enriched["result"] = {"verdict": "PRODUCE", "reason_codes": [], "risk_flags": []}
    diff = compare_run_bundles(dry, enriched)
    assert diff["verdict_change"]["changed"] is True


def test_compare_reports_cost_and_runtime_deltas():
    dry = _empty_bundle("dry")
    dry["ledger_entry"]["estimated_cost_usd"] = 0.0
    dry["ledger_entry"]["wallclock_seconds"] = 0.5
    enriched = _empty_bundle("enriched")
    enriched["ledger_entry"]["estimated_cost_usd"] = 0.005
    enriched["ledger_entry"]["wallclock_seconds"] = 14.89
    diff = compare_run_bundles(dry, enriched)
    assert diff["cost_delta"]["dry"] == 0.0
    assert diff["cost_delta"]["enriched"] == 0.005
    assert diff["cost_delta"]["delta"] == 0.005
    assert diff["runtime_delta"]["delta"] == round(14.89 - 0.5, 4)


def test_compare_diffs_added_and_removed_queries_and_connectors():
    dry = _empty_bundle("dry")
    dry["query_plan"] = {
        "connector_count": 1,
        "plans": [
            {
                "connector": "courtlistener",
                "queries": [{"query": "John Example"}, {"query": "Phoenix Police"}],
            }
        ],
    }
    enriched = _empty_bundle("enriched")
    enriched["query_plan"] = {
        "connector_count": 2,
        "plans": [
            {
                "connector": "courtlistener",
                "queries": [
                    {"query": "John Example"},
                    {"query": "John Example bodycam"},
                ],
            },
            {
                "connector": "muckrock",
                "queries": [{"query": "Phoenix Police FOIA"}],
            },
        ],
    }
    diff = compare_run_bundles(dry, enriched)
    qpd = diff["query_plan_delta"]
    assert qpd["dry_connector_count"] == 1
    assert qpd["enriched_connector_count"] == 2
    assert qpd["delta_connector_count"] == 1
    assert "muckrock" in qpd["connectors"]["added"]
    assert "courtlistener" in qpd["connectors"]["shared"]
    assert "John Example bodycam" in qpd["queries"]["added"]
    assert "Phoenix Police FOIA" in qpd["queries"]["added"]
    assert "Phoenix Police" in qpd["queries"]["removed"]


def test_compare_reason_code_and_risk_flag_diffs_are_sorted_sets():
    dry = _empty_bundle("dry")
    dry["result"] = {
        "verdict": "HOLD",
        "reason_codes": ["document_only_hold"],
        "risk_flags": ["weak_identity"],
    }
    enriched = _empty_bundle("enriched")
    enriched["result"] = {
        "verdict": "HOLD",
        "reason_codes": ["document_only_hold", "claim_only_hold"],
        "risk_flags": ["no_verified_media"],
    }
    diff = compare_run_bundles(dry, enriched)
    assert diff["reason_code_delta"]["added"] == ["claim_only_hold"]
    assert diff["reason_code_delta"]["shared"] == ["document_only_hold"]
    assert diff["risk_flag_delta"]["added"] == ["no_verified_media"]
    assert diff["risk_flag_delta"]["removed"] == ["weak_identity"]


def test_compare_emits_why_not_produce_when_enriched_is_hold():
    dry = _empty_bundle("dry")
    enriched = _empty_bundle("enriched")
    enriched["result"] = {
        "verdict": "HOLD",
        "reason_codes": ["document_only_hold"],
        "risk_flags": ["no_verified_media"],
    }
    enriched["identity"] = {"identity_confidence": "low"}
    enriched["outcome"] = {"outcome_status": "charged"}
    enriched["verified_artifacts"] = [
        {"artifact_type": "docket_docs", "format": "pdf"}
    ]
    diff = compare_run_bundles(dry, enriched)
    why = diff["why_not_produce"]
    assert why is not None
    assert why["verdict"] == "HOLD"
    assert "no_verified_media" in why["blocking_risk_flags"]
    assert "document_only_hold" in why["blocking_reason_codes"]
    assert why["missing_media_artifacts"] is True
    assert why["missing_high_identity"] is True
    assert why["missing_concluded_outcome"] is True


def test_compare_omits_why_not_produce_when_enriched_is_produce():
    dry = _empty_bundle("dry")
    enriched = _empty_bundle("enriched")
    enriched["result"] = {
        "verdict": "PRODUCE",
        "reason_codes": ["media_artifact_present"],
        "risk_flags": [],
    }
    diff = compare_run_bundles(dry, enriched)
    assert diff["why_not_produce"] is None


def test_compare_handles_missing_sections_gracefully():
    """A bundle with everything stripped except experiment_id should
    still round-trip through the comparator without raising."""
    dry = {"experiment_id": "dry"}
    enriched = {"experiment_id": "enriched"}
    diff = compare_run_bundles(dry, enriched)
    assert diff["source_count_delta"]["dry"] == 0
    assert diff["source_count_delta"]["enriched"] == 0
    assert diff["artifact_yield_delta"]["dry_total"] == 0
    assert diff["artifact_yield_delta"]["enriched_total"] == 0
    assert diff["verdict_change"]["dry"] is None
    assert diff["verdict_change"]["enriched"] is None
    assert diff["verdict_change"]["changed"] is False


def test_compare_output_is_json_serializable():
    dry = _empty_bundle("dry")
    enriched = _empty_bundle("enriched")
    enriched["result"] = {
        "verdict": "HOLD",
        "reason_codes": ["document_only_hold"],
        "risk_flags": ["no_verified_media"],
    }
    enriched["verified_artifacts"] = [
        {"artifact_type": "docket_docs", "format": "pdf"},
    ]
    diff = compare_run_bundles(dry, enriched)
    encoded = json.dumps(diff)
    decoded = json.loads(encoded)
    assert decoded == diff


def test_compare_provider_yield_delta_tracks_api_calls():
    dry = _empty_bundle("dry")
    enriched = _empty_bundle("enriched")
    enriched["ledger_entry"]["api_calls"]["courtlistener"] = 1
    enriched["ledger_entry"]["api_calls"]["documentcloud"] = 1
    diff = compare_run_bundles(dry, enriched)
    pyd = diff["provider_yield_delta"]
    assert pyd["total_calls"]["enriched"] == 2
    assert pyd["total_calls"]["dry"] == 0
    assert pyd["total_calls"]["delta"] == 2
    assert pyd["by_provider"]["courtlistener"]["delta"] == 1
    assert pyd["by_provider"]["documentcloud"]["delta"] == 1


# ---- End-to-end with real CLI bundles --------------------------------------


def test_compare_against_real_cli_bundles(tmp_path):
    """Build two real bundles via the CLI (default-mode-on-PRODUCE-fixture
    vs multi-source-dry-run-on-WaPo-row), and confirm the comparator
    surfaces the expected deltas without any network calls."""
    dry_path = tmp_path / "dry.json"
    enriched_path = tmp_path / "enriched.json"

    code_a, _, err_a = run_cli(
        [
            "--fixture",
            WAPO_FIXTURE,
            "--multi-source-dry-run",
            "--connectors",
            "courtlistener,muckrock,documentcloud",
            "--json",
            "--bundle-out",
            str(dry_path),
        ]
    )
    assert code_a == 0, err_a

    code_b, _, err_b = run_cli(
        [
            "--fixture",
            str(FIXTURE_DIR / "media_rich_produce.json"),
            "--json",
            "--bundle-out",
            str(enriched_path),
        ]
    )
    assert code_b == 0, err_b

    dry = json.loads(dry_path.read_text(encoding="utf-8"))
    enriched = json.loads(enriched_path.read_text(encoding="utf-8"))
    diff = compare_run_bundles(dry, enriched)

    # Verdict flipped from non-PRODUCE to PRODUCE.
    assert diff["verdict_change"]["enriched"] == "PRODUCE"
    assert diff["verdict_change"]["changed"] is True

    # The PRODUCE fixture carries verified artifacts; the WaPo-row dry
    # has none. Delta should be positive.
    assert diff["artifact_yield_delta"]["enriched_total"] >= 1
    assert diff["artifact_yield_delta"]["delta"] >= 1
    # Media artifacts present on the enriched (PRODUCE) side.
    assert diff["artifact_yield_delta"]["media_delta"]["enriched"] >= 1

    # Both bundles are no-live, so cost delta must be zero.
    assert diff["cost_delta"]["delta"] == 0.0


def test_compare_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    dry = _empty_bundle("dry")
    enriched = _empty_bundle("enriched")
    enriched["verified_artifacts"] = [
        {"artifact_type": "docket_docs", "format": "pdf"},
    ]
    enriched["result"] = {
        "verdict": "HOLD",
        "reason_codes": [],
        "risk_flags": [],
    }
    compare_run_bundles(dry, enriched)
    assert calls == [], f"compare_run_bundles made {len(calls)} live HTTP call(s)"
