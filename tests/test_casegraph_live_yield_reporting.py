"""EVAL4 — Live-yield summary report tests.

Asserts that ``build_live_yield_report`` produces a stable, structured
summary across RunLedgerEntry rows + per-connector live diagnostics
without making any network call.
"""
import json

import pytest

from pipeline2_discovery.casegraph import (
    DEFAULT_API_CALLS,
    RunLedgerEntry,
    build_live_yield_report,
    build_run_ledger_entry,
)


# ---- Empty input ---------------------------------------------------------


def test_empty_input_returns_zero_filled_report():
    report = build_live_yield_report([])
    assert report["total_runs"] == 0
    assert report["total_live_calls"] == 0
    assert report["total_estimated_cost_usd"] == 0.0
    assert report["total_wallclock_seconds"] == 0.0
    assert report["total_source_records"] == 0
    assert report["total_verified_artifacts"] == 0
    assert report["total_media_artifacts"] == 0
    assert report["total_document_artifacts"] == 0
    assert report["verdict_counts"] == {"PRODUCE": 0, "HOLD": 0, "SKIP": 0, "unknown": 0}
    # Per-provider rollup pre-populated with the canonical 7 keys.
    assert set(report["by_provider"].keys()) == set(DEFAULT_API_CALLS.keys())
    for slot in report["by_provider"].values():
        assert slot["calls"] == 0
    assert report["warnings"] == []


# ---- Aggregation across CourtListener / MuckRock / DocumentCloud ---------


def test_summarizes_three_connector_ledger_entries():
    cl_entry = build_run_ledger_entry(
        experiment_id="LIVE1",
        api_calls={"courtlistener": 1},
        wallclock_seconds=4.59,
    )
    mr_entry = build_run_ledger_entry(
        experiment_id="LIVE2",
        api_calls={"muckrock": 1},
        wallclock_seconds=1.06,
    )
    dc_entry = build_run_ledger_entry(
        experiment_id="LIVE3",
        api_calls={"documentcloud": 1},
        wallclock_seconds=2.78,
    )
    report = build_live_yield_report([cl_entry, mr_entry, dc_entry])

    assert report["total_runs"] == 3
    assert report["total_live_calls"] == 3
    assert report["total_estimated_cost_usd"] == 0.0
    assert report["total_wallclock_seconds"] == round(4.59 + 1.06 + 2.78, 4)
    assert report["by_provider"]["courtlistener"]["calls"] == 1
    assert report["by_provider"]["muckrock"]["calls"] == 1
    assert report["by_provider"]["documentcloud"]["calls"] == 1
    assert report["by_provider"]["brave"]["calls"] == 0
    assert report["by_provider"]["firecrawl"]["calls"] == 0


def test_total_live_calls_sums_per_provider_calls():
    e1 = build_run_ledger_entry(
        experiment_id="run1",
        api_calls={"courtlistener": 1, "muckrock": 1},
    )
    e2 = build_run_ledger_entry(
        experiment_id="run2",
        api_calls={"courtlistener": 1, "documentcloud": 1},
    )
    report = build_live_yield_report([e1, e2])
    assert report["total_live_calls"] == 4
    assert report["by_provider"]["courtlistener"]["calls"] == 2
    assert report["by_provider"]["muckrock"]["calls"] == 1
    assert report["by_provider"]["documentcloud"]["calls"] == 1


# ---- Cost handling -------------------------------------------------------


def test_total_cost_is_zero_for_free_only_runs():
    entries = [
        build_run_ledger_entry(experiment_id="x", api_calls={"courtlistener": 5}),
        build_run_ledger_entry(experiment_id="y", api_calls={"muckrock": 3}),
    ]
    report = build_live_yield_report(entries)
    assert report["total_estimated_cost_usd"] == 0.0


def test_unexpected_cost_emits_warning():
    """Cost > 0 from a smoke run should surface as a warning so a
    reviewer can investigate."""
    paid_entry = RunLedgerEntry(
        experiment_id="hypothetical-paid",
        api_calls={"brave": 3},
        estimated_cost_usd=0.015,
    )
    report = build_live_yield_report([paid_entry])
    assert report["total_estimated_cost_usd"] == 0.015
    assert any(w.startswith("unexpected_cost:") for w in report["warnings"])


# ---- Per-connector diagnostics + zero-yield warnings ---------------------


def test_per_connector_diagnostics_attribute_source_records_to_provider():
    entries = [
        build_run_ledger_entry(experiment_id="LIVE4-cl", api_calls={"courtlistener": 1}),
        build_run_ledger_entry(experiment_id="LIVE4-mr", api_calls={"muckrock": 1}),
    ]
    diags = [
        {
            "connector": "courtlistener",
            "endpoint": "https://www.courtlistener.com/api/rest/v4/search/",
            "result_count": 5,
            "verified_artifact_count": 0,
            "wallclock_seconds": 1.99,
            "estimated_cost_usd": 0.0,
            "status_code": 200,
            "error": None,
            "api_calls": {"courtlistener": 1},
        },
        {
            "connector": "muckrock",
            "endpoint": "https://www.muckrock.com/api_v2/requests/",
            "result_count": 0,
            "verified_artifact_count": 0,
            "wallclock_seconds": 0.79,
            "estimated_cost_usd": 0.0,
            "status_code": 200,
            "error": None,
            "api_calls": {"muckrock": 1},
        },
    ]
    report = build_live_yield_report(entries, per_connector_diagnostics=diags)
    cl = report["by_provider"]["courtlistener"]
    mr = report["by_provider"]["muckrock"]
    assert cl["result_count"] == 5
    assert cl["source_records"] == 5
    assert mr["result_count"] == 0
    assert mr["source_records"] == 0
    assert "https://www.courtlistener.com/api/rest/v4/search/" in cl["endpoints"]
    assert "https://www.muckrock.com/api_v2/requests/" in mr["endpoints"]
    # Total source records reflects the diagnostic counts.
    assert report["total_source_records"] == 5
    # Zero-yield provider warning fires for muckrock.
    assert any(w == "zero_yield_provider:muckrock" for w in report["warnings"])


def test_zero_yield_provider_warning_does_not_fire_when_no_calls_made():
    """A provider with zero calls is not 'zero-yield' — it just wasn't
    used. The warning only fires when calls > 0 but result_count == 0."""
    e = build_run_ledger_entry(
        experiment_id="cl-only",
        api_calls={"courtlistener": 1},
    )
    diag = {
        "connector": "courtlistener",
        "result_count": 5,
        "verified_artifact_count": 0,
        "wallclock_seconds": 2.0,
        "estimated_cost_usd": 0.0,
    }
    report = build_live_yield_report([e], per_connector_diagnostics=[diag])
    # muckrock has no calls; should NOT trigger zero_yield_provider warning.
    assert not any("zero_yield_provider:muckrock" == w for w in report["warnings"])


# ---- Unexpected verified artifact warning --------------------------------


def test_unexpected_verified_artifact_from_smoke_emits_warning():
    """If a smoke entry reports a non-zero verified_artifact_count,
    something has gone wrong — smokes alone shouldn't graduate sources."""
    entry = build_run_ledger_entry(
        experiment_id="hypothetical-leak",
        api_calls={"courtlistener": 1},
    )
    # Patch the entry so it carries a verified artifact count.
    entry.verified_artifact_count = 2
    report = build_live_yield_report([entry])
    assert report["total_verified_artifacts"] == 2
    assert any(
        w.startswith("unexpected_verified_artifacts_from_smoke:") for w in report["warnings"]
    )


def test_diagnostics_with_verified_artifact_also_trigger_warning():
    diag = {
        "connector": "courtlistener",
        "result_count": 5,
        "verified_artifact_count": 1,  # leaking
        "wallclock_seconds": 2.0,
        "estimated_cost_usd": 0.0,
    }
    report = build_live_yield_report(
        [build_run_ledger_entry(experiment_id="x", api_calls={"courtlistener": 1})],
        per_connector_diagnostics=[diag],
    )
    assert report["total_verified_artifacts"] == 1
    assert any(
        w.startswith("unexpected_verified_artifacts_from_smoke:") for w in report["warnings"]
    )


# ---- expected_connectors warnings ----------------------------------------


def test_missing_expected_connector_emits_warning():
    e = build_run_ledger_entry(
        experiment_id="cl-only",
        api_calls={"courtlistener": 1},
    )
    report = build_live_yield_report(
        [e],
        expected_connectors=["courtlistener", "muckrock"],
    )
    assert "missing_provider:muckrock" in report["warnings"]
    assert "missing_provider:courtlistener" not in report["warnings"]


def test_no_missing_warning_when_all_expected_are_present():
    entries = [
        build_run_ledger_entry(experiment_id="cl", api_calls={"courtlistener": 1}),
        build_run_ledger_entry(experiment_id="mr", api_calls={"muckrock": 1}),
    ]
    report = build_live_yield_report(
        entries, expected_connectors=["courtlistener", "muckrock"]
    )
    assert not any(w.startswith("missing_provider:") for w in report["warnings"])


# ---- Verdict aggregation -------------------------------------------------


def test_verdict_counts_aggregate_across_runs():
    e1 = RunLedgerEntry(experiment_id="r1", verdict="PRODUCE")
    e2 = RunLedgerEntry(experiment_id="r2", verdict="HOLD")
    e3 = RunLedgerEntry(experiment_id="r3", verdict="HOLD")
    e4 = RunLedgerEntry(experiment_id="r4", verdict="SKIP")
    e5 = RunLedgerEntry(experiment_id="r5")  # no verdict
    report = build_live_yield_report([e1, e2, e3, e4, e5])
    assert report["verdict_counts"]["PRODUCE"] == 1
    assert report["verdict_counts"]["HOLD"] == 2
    assert report["verdict_counts"]["SKIP"] == 1
    assert report["verdict_counts"]["unknown"] == 1


# ---- JSON serializability ------------------------------------------------


def test_report_is_json_serializable():
    entries = [
        build_run_ledger_entry(
            experiment_id="LIVE1",
            api_calls={"courtlistener": 1},
            wallclock_seconds=4.59,
        ),
        build_run_ledger_entry(
            experiment_id="LIVE2",
            api_calls={"muckrock": 1},
            wallclock_seconds=1.06,
        ),
    ]
    report = build_live_yield_report(entries)
    serialized = json.dumps(report, sort_keys=False)
    parsed = json.loads(serialized)
    assert parsed["total_live_calls"] == 2
    assert parsed["by_provider"]["courtlistener"]["calls"] == 1


def test_report_handles_only_diagnostics_no_ledger_entries():
    """When only diagnostics are supplied (e.g. a fresh smoke run with
    no JSONL persistence yet), the report still aggregates."""
    diag = {
        "connector": "documentcloud",
        "endpoint": "https://api.www.documentcloud.org/api/documents/search/",
        "result_count": 5,
        "verified_artifact_count": 0,
        "wallclock_seconds": 2.78,
        "estimated_cost_usd": 0.0,
        "status_code": 200,
        "error": None,
        "api_calls": {"documentcloud": 1},
    }
    report = build_live_yield_report([], per_connector_diagnostics=[diag])
    assert report["total_runs"] == 0
    assert report["total_source_records"] == 5
    assert report["by_provider"]["documentcloud"]["result_count"] == 5
    assert report["by_provider"]["documentcloud"]["calls"] == 0  # no ledger entry
    # No zero-yield warning because we never recorded a call.
    assert not any("zero_yield_provider:" in w for w in report["warnings"])


def test_warnings_are_sorted_deterministically():
    entries = [
        build_run_ledger_entry(experiment_id="a", api_calls={"muckrock": 1}),
        build_run_ledger_entry(experiment_id="b", api_calls={"documentcloud": 1}),
    ]
    diags = [
        {"connector": "muckrock", "result_count": 0, "verified_artifact_count": 0},
        {"connector": "documentcloud", "result_count": 0, "verified_artifact_count": 0},
    ]
    report = build_live_yield_report(entries, per_connector_diagnostics=diags)
    warnings = report["warnings"]
    assert warnings == sorted(warnings)
    assert "zero_yield_provider:documentcloud" in warnings
    assert "zero_yield_provider:muckrock" in warnings


def test_endpoints_dedupe_per_provider():
    diags = [
        {"connector": "courtlistener", "endpoint": "https://x", "result_count": 1, "verified_artifact_count": 0},
        {"connector": "courtlistener", "endpoint": "https://x", "result_count": 1, "verified_artifact_count": 0},
        {"connector": "courtlistener", "endpoint": "https://y", "result_count": 1, "verified_artifact_count": 0},
    ]
    report = build_live_yield_report([], per_connector_diagnostics=diags)
    endpoints = report["by_provider"]["courtlistener"]["endpoints"]
    assert endpoints == ["https://x", "https://y"]


def test_per_provider_zero_for_unused_canonical_providers():
    e = build_run_ledger_entry(experiment_id="x", api_calls={"courtlistener": 1})
    report = build_live_yield_report([e])
    for provider in ("muckrock", "documentcloud", "youtube", "brave", "firecrawl", "llm"):
        assert report["by_provider"][provider]["calls"] == 0
        assert report["by_provider"][provider]["result_count"] == 0
