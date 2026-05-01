"""OUTCOME2 - no-live outcome corroboration seed planner tests."""
import json
from pathlib import Path

from pipeline2_discovery.casegraph.outcome_seed_plan import (
    build_outcome_seed_plan_report,
    outcome_seed_plan_to_jsonable,
)


ROOT = Path(__file__).resolve().parents[1]


def test_outcome_seed_plan_covers_all_unknown_outcomes():
    report = build_outcome_seed_plan_report(repo_root=ROOT)

    assert report.total_cases == 38
    assert report.unresolved_outcome_count == 27
    assert len(report.plans) == 27
    assert report.outcome_plan_ready_count == 27
    assert report.manual_seed_needed_count == 0


def test_outcome_seed_plan_does_not_hallucinate_outcomes():
    report = build_outcome_seed_plan_report(repo_root=ROOT)

    for plan in report.plans:
        plan_data = plan.to_dict()
        assert plan.current_outcome_seed_status == "unknown"
        assert plan.missing_outcome_reason
        assert plan_data["current_outcome_seed_status"] == "unknown"
        assert "resolved_outcome_status" not in plan_data


def test_outcome_seed_plan_recommends_court_sources_when_available():
    report = build_outcome_seed_plan_report(repo_root=ROOT)

    christa = next(plan for plan in report.plans if plan.title.startswith("Christa Gail Pike"))
    keonte = next(plan for plan in report.plans if plan.title == "Keonte Gathron")

    assert "courtlistener" in christa.recommended_outcome_sources
    assert "court_docket_search" in christa.recommended_outcome_sources
    assert christa.supported_live_path_available is True
    assert christa.priority == "high"
    assert any("Christa Gail Pike" in query for query in christa.suggested_deterministic_query_seeds)

    assert "county/state court portal" in keonte.recommended_outcome_sources
    assert keonte.blocker == "needs_seed_url_discovery"
    assert keonte.priority == "low"


def test_outcome_seed_plan_reports_source_and_priority_counts():
    report = build_outcome_seed_plan_report(repo_root=ROOT)

    assert report.source_counts["county/state court portal"] == 27
    assert report.source_counts["courtlistener"] >= 8
    assert report.priority_counts["high"] >= 10


def test_outcome_seed_plan_is_json_serializable():
    report = build_outcome_seed_plan_report(repo_root=ROOT)

    encoded = json.dumps(outcome_seed_plan_to_jsonable(report), sort_keys=True)
    decoded = json.loads(encoded)

    assert decoded["unresolved_outcome_count"] == 27
    assert decoded["outcome_plan_ready_count"] == 27
    assert decoded["plans"][0]["suggested_deterministic_query_seeds"]


def test_outcome_seed_plan_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    build_outcome_seed_plan_report(repo_root=ROOT)
    assert calls == []
