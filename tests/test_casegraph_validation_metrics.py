"""EVAL6 — validation metrics report tests.

Asserts that ``build_validation_metrics_report``:

- computes deterministic metrics from a fully-passing validation output
- exposes the canonical top-level shape (verdict_accuracy,
  verdict_confusion, false_verdicts, guard_counters, artifact_yield,
  scenario_counts, score_distribution, failure_examples,
  top_next_actions, top_risk_flags, top_reason_codes)
- detects a false PRODUCE in synthetic input (actual=PRODUCE,
  expected!=PRODUCE)
- detects a false HOLD and false SKIP
- correctly tracks the missing-media-gate scenario (a synthetic
  result that produced without media must increment
  document_only_produce_count)
- handles empty input (returns zero-default report with all keys
  present)
- reports failure examples capped at ``top_n`` with the canonical
  per-failure shape
- aggregates top_risk_flags / top_reason_codes / top_next_actions
  in (count desc, name asc) order
- emits JSON-serializable output
- passes-through guard_counters from the validation summary when
  present
- never makes a network call
- end-to-end against the real validation runner output
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    build_validation_metrics_report,
    run_validation_manifest,
)


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "tests" / "fixtures" / "validation_manifest.json"


REQUIRED_TOP_KEYS = (
    "total_entries",
    "verdict_accuracy",
    "verdict_confusion",
    "false_verdicts",
    "guard_counters",
    "artifact_yield",
    "scenario_counts",
    "score_distribution",
    "failure_examples",
    "top_next_actions",
    "top_risk_flags",
    "top_reason_codes",
)


def _make_result(
    *,
    id_,
    passed=True,
    actual="HOLD",
    expected="HOLD",
    media=0,
    document=0,
    identity_confidence="high",
    outcome_status="charged",
    risk_flags=None,
    reason_codes=None,
    next_actions=None,
    research=0.0,
    production=0.0,
    actionability=0.0,
    fail_reasons=None,
    input_type="manual",
):
    return {
        "id": id_,
        "fixture_path": "tests/fixtures/synthetic.json",
        "expected_verdict": expected,
        "actual_verdict": actual,
        "passed": passed,
        "fail_reasons": list(fail_reasons or []),
        "reason_codes": list(reason_codes or []),
        "risk_flags": list(risk_flags or []),
        "next_actions": list(next_actions or []),
        "reason_code_matches": {
            "must_include_present": [],
            "must_include_missing": [],
            "must_not_include_present": [],
        },
        "risk_flag_matches": {
            "must_include_present": [],
            "must_include_missing": [],
            "must_not_include_present": [],
        },
        "research_completeness_score": research,
        "production_actionability_score": production,
        "actionability_score": actionability,
        "verified_artifact_count": media + document,
        "media_artifact_count": media,
        "document_artifact_count": document,
        "input_type": input_type,
        "identity_confidence": identity_confidence,
        "outcome_status": outcome_status,
        "bundle_path": None,
    }


def _wrap(results, **summary_overrides):
    summary = {
        "total": len(results),
        "passed": sum(1 for r in results if r["passed"]),
        "failed": sum(1 for r in results if not r["passed"]),
        "verdict_counts": {"PRODUCE": 0, "HOLD": 0, "SKIP": 0},
        "expected_verdict_counts": {"PRODUCE": 0, "HOLD": 0, "SKIP": 0},
        "false_produce_count": 0,
        "document_only_produce_count": 0,
        "claim_only_produce_count": 0,
        "weak_identity_produce_count": 0,
        "protected_or_pacer_produce_count": 0,
    }
    summary.update(summary_overrides)
    return {
        "manifest_path": "synthetic",
        "manifest_version": 1,
        "total_entries": len(results),
        "results": results,
        "summary": summary,
    }


# ---- Empty / shape ---------------------------------------------------------


def test_metrics_empty_input_returns_zero_default():
    report = build_validation_metrics_report({"results": []})
    assert report["total_entries"] == 0
    assert report["verdict_accuracy"]["accuracy_pct"] == 0.0
    assert report["false_verdicts"]["false_produce_count"] == 0
    assert report["guard_counters"]["document_only_produce_count"] == 0
    for key in REQUIRED_TOP_KEYS:
        assert key in report


def test_metrics_invalid_input_returns_zero_default():
    report = build_validation_metrics_report(None)  # type: ignore[arg-type]
    assert report["total_entries"] == 0
    assert report["failure_examples"] == []


def test_metrics_top_level_shape_is_canonical():
    output = run_validation_manifest(MANIFEST_PATH)
    report = build_validation_metrics_report(output)
    for key in REQUIRED_TOP_KEYS:
        assert key in report


# ---- All-pass real manifest -----------------------------------------------


def test_metrics_against_real_manifest_all_pass():
    output = run_validation_manifest(MANIFEST_PATH)
    report = build_validation_metrics_report(output)
    assert report["total_entries"] == output["total_entries"]
    assert report["verdict_accuracy"]["correct"] == output["total_entries"]
    assert report["verdict_accuracy"]["incorrect"] == 0
    assert report["verdict_accuracy"]["accuracy_pct"] == 100.0
    assert report["false_verdicts"]["false_produce_count"] == 0
    assert report["false_verdicts"]["false_hold_count"] == 0
    assert report["false_verdicts"]["false_skip_count"] == 0
    for key in (
        "document_only_produce_count",
        "claim_only_produce_count",
        "weak_identity_produce_count",
        "protected_or_pacer_produce_count",
    ):
        assert report["guard_counters"][key] == 0
    assert report["failure_examples"] == []


def test_metrics_artifact_yield_aggregates_correctly_on_real_manifest():
    output = run_validation_manifest(MANIFEST_PATH)
    report = build_validation_metrics_report(output)
    yld = report["artifact_yield"]
    expected_total = sum(r["verified_artifact_count"] for r in output["results"])
    expected_media = sum(r["media_artifact_count"] for r in output["results"])
    expected_doc = sum(r["document_artifact_count"] for r in output["results"])
    assert yld["total_verified_artifacts"] == expected_total
    assert yld["total_media_artifacts"] == expected_media
    assert yld["total_document_artifacts"] == expected_doc
    if expected_total > 0:
        assert 0.0 <= yld["media_artifact_rate"] <= 1.0


# ---- Synthetic detection scenarios ----------------------------------------


def test_metrics_detects_false_produce():
    """Synthetic: actual=PRODUCE but expected=HOLD must show up as a
    false_produce_count of 1 (verdict_accuracy.incorrect = 1)."""
    results = [
        _make_result(
            id_="bad_produce",
            passed=False,
            actual="PRODUCE",
            expected="HOLD",
            media=1,
            document=0,
            fail_reasons=["verdict mismatch: actual=PRODUCE expected=HOLD"],
        ),
    ]
    report = build_validation_metrics_report(_wrap(results))
    assert report["false_verdicts"]["false_produce_count"] == 1
    assert report["verdict_accuracy"]["incorrect"] == 1
    assert report["verdict_accuracy"]["correct"] == 0
    assert report["verdict_accuracy"]["accuracy_pct"] == 0.0


def test_metrics_detects_false_hold_and_false_skip():
    results = [
        _make_result(id_="false_hold", passed=False, actual="HOLD", expected="PRODUCE"),
        _make_result(id_="false_skip", passed=False, actual="SKIP", expected="HOLD"),
    ]
    report = build_validation_metrics_report(_wrap(results))
    assert report["false_verdicts"]["false_hold_count"] == 1
    assert report["false_verdicts"]["false_skip_count"] == 1
    assert report["false_verdicts"]["false_produce_count"] == 0


def test_metrics_detects_missing_media_gate_for_synthetic_produce_without_media():
    """Synthetic: an actual=PRODUCE result with media_artifact_count=0
    must increment document_only_produce_count in guard_counters
    (i.e., the gate would have been violated if it weren't deterministic
    in the real pipeline)."""
    results = [
        _make_result(
            id_="produce_without_media",
            actual="PRODUCE",
            expected="PRODUCE",
            media=0,
            document=2,
            identity_confidence="high",
        ),
    ]
    report = build_validation_metrics_report(_wrap(results))
    assert report["guard_counters"]["document_only_produce_count"] == 1


def test_metrics_detects_claim_only_produce_synthetic():
    results = [
        _make_result(
            id_="claim_only_produce",
            actual="PRODUCE",
            expected="PRODUCE",
            media=0,
            document=0,
            identity_confidence="high",
        ),
    ]
    report = build_validation_metrics_report(_wrap(results))
    assert report["guard_counters"]["claim_only_produce_count"] == 1


def test_metrics_detects_weak_identity_produce_synthetic():
    results = [
        _make_result(
            id_="weak_id_produce",
            actual="PRODUCE",
            expected="PRODUCE",
            media=2,
            document=0,
            identity_confidence="low",
        ),
    ]
    report = build_validation_metrics_report(_wrap(results))
    assert report["guard_counters"]["weak_identity_produce_count"] == 1


def test_metrics_detects_protected_or_pacer_produce_synthetic():
    results = [
        _make_result(
            id_="protected_produce",
            actual="PRODUCE",
            expected="PRODUCE",
            media=2,
            document=0,
            identity_confidence="high",
            risk_flags=["protected_or_nonpublic"],
        ),
    ]
    report = build_validation_metrics_report(_wrap(results))
    assert report["guard_counters"]["protected_or_pacer_produce_count"] == 1


def test_metrics_failure_examples_capped_at_top_n():
    results = [
        _make_result(
            id_=f"fail_{i}",
            passed=False,
            actual="HOLD",
            expected="PRODUCE",
            fail_reasons=["verdict mismatch"],
        )
        for i in range(7)
    ]
    report = build_validation_metrics_report(_wrap(results), top_n=3) if False else build_validation_metrics_report(_wrap(results))
    # default top_n=5
    assert len(report["failure_examples"]) == 5
    for example in report["failure_examples"]:
        for key in ("id", "expected_verdict", "actual_verdict", "fail_reasons"):
            assert key in example


def test_metrics_failure_examples_respect_custom_top_n():
    results = [
        _make_result(
            id_=f"fail_{i}",
            passed=False,
            actual="HOLD",
            expected="PRODUCE",
            fail_reasons=["verdict mismatch"],
        )
        for i in range(7)
    ]
    report = build_validation_metrics_report(_wrap(results), top_n=2)
    assert len(report["failure_examples"]) == 2


# ---- Top-N aggregations ----------------------------------------------------


def test_metrics_top_risk_flags_sorted_by_count_desc_then_name_asc():
    results = [
        _make_result(id_="a", risk_flags=["weak_identity", "no_verified_media"]),
        _make_result(id_="b", risk_flags=["weak_identity", "protected_or_nonpublic"]),
        _make_result(id_="c", risk_flags=["weak_identity"]),
    ]
    report = build_validation_metrics_report(_wrap(results))
    flags = report["top_risk_flags"]
    # Three distinct flags: weak_identity x3, no_verified_media x1, protected_or_nonpublic x1.
    # Tie between the two ones is broken by name asc.
    assert flags[0]["flag"] == "weak_identity"
    assert flags[0]["count"] == 3
    assert flags[1]["flag"] == "no_verified_media"
    assert flags[2]["flag"] == "protected_or_nonpublic"


def test_metrics_top_reason_codes_aggregated_and_sorted():
    results = [
        _make_result(id_="a", reason_codes=["document_only_hold"]),
        _make_result(id_="b", reason_codes=["document_only_hold", "claim_only_hold"]),
    ]
    report = build_validation_metrics_report(_wrap(results))
    codes = report["top_reason_codes"]
    assert codes[0]["code"] == "document_only_hold"
    assert codes[0]["count"] == 2


def test_metrics_top_next_actions_aggregated_and_sorted():
    results = [
        _make_result(id_="a", next_actions=["Check_for_corroborating_news_coverage"]),
        _make_result(id_="b", next_actions=["Check_for_corroborating_news_coverage", "Request_FOIA"]),
    ]
    report = build_validation_metrics_report(_wrap(results))
    actions = report["top_next_actions"]
    assert actions[0]["action"] == "Check_for_corroborating_news_coverage"
    assert actions[0]["count"] == 2


# ---- Confusion matrix ------------------------------------------------------


def test_metrics_confusion_matrix_cells_count_correctly():
    results = [
        _make_result(id_="a", actual="PRODUCE", expected="PRODUCE", media=1),
        _make_result(id_="b", actual="HOLD", expected="PRODUCE", passed=False),
        _make_result(id_="c", actual="HOLD", expected="HOLD"),
        _make_result(id_="d", actual="SKIP", expected="HOLD", passed=False),
    ]
    report = build_validation_metrics_report(_wrap(results))
    confusion = report["verdict_confusion"]
    assert confusion["PRODUCE"]["PRODUCE"] == 1
    assert confusion["PRODUCE"]["HOLD"] == 1
    assert confusion["HOLD"]["HOLD"] == 1
    assert confusion["HOLD"]["SKIP"] == 1
    # SKIP row should be empty.
    assert sum(confusion["SKIP"].values()) == 0


# ---- Pass-through summary --------------------------------------------------


def test_metrics_passes_through_summary_guard_counters_when_present():
    """When the validation summary already carries guard_counters,
    the metrics report should respect them (passthrough)."""
    results = [
        _make_result(id_="a", actual="HOLD", expected="HOLD"),
    ]
    wrapped = _wrap(results, document_only_produce_count=42)
    report = build_validation_metrics_report(wrapped)
    assert report["guard_counters"]["document_only_produce_count"] == 42


# ---- JSON serialization + network --------------------------------------------


def test_metrics_output_is_json_serializable():
    output = run_validation_manifest(MANIFEST_PATH)
    report = build_validation_metrics_report(output)
    encoded = json.dumps(report)
    decoded = json.loads(encoded)
    assert decoded == report


def test_metrics_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    output = run_validation_manifest(MANIFEST_PATH)
    build_validation_metrics_report(output)
    assert calls == [], (
        f"validation metrics builder triggered {len(calls)} live HTTP call(s)"
    )
