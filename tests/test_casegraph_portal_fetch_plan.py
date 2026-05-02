"""PORTAL2 - no-live seeded portal fetch plan tests."""
import json
from pathlib import Path

from pipeline2_discovery.casegraph.calibration_replay import run_calibration_replay
from pipeline2_discovery.casegraph.portal_fetch_plan import (
    build_portal_fetch_plan_report,
    portal_fetch_plan_to_jsonable,
)


ROOT = Path(__file__).resolve().parents[1]


def test_fetch_plan_covers_likely_firecrawl_needed_cases():
    report = build_portal_fetch_plan_report(repo_root=ROOT)

    assert report.total_cases == 38
    assert report.candidate_cases == 34
    assert report.ready_for_portal_fetch_count == 30
    assert report.needs_seed_url_discovery_count == 4
    assert report.firecrawl_fetcher_count == 4
    assert report.requests_fetcher_count == 26


def test_fetch_plans_respect_portal_profile_caps_and_policies():
    report = build_portal_fetch_plan_report(repo_root=ROOT)

    for plan in report.plans:
        if plan.blocked_reason:
            continue
        assert plan.max_pages <= 3
        assert plan.max_links <= 50
        assert plan.resolver_policy["metadata_only"] is True
        assert plan.resolver_policy["require_public_url"] is True
        assert plan.resolver_policy["allow_downloads"] is False
        assert plan.resolver_policy["allow_scraping"] is False
        assert plan.resolver_policy["allow_login"] is False
        assert plan.allowed_domain


def test_fetch_plan_separates_seeded_ready_cases_from_seed_discovery():
    report = build_portal_fetch_plan_report(repo_root=ROOT)

    ready = [plan for plan in report.plans if plan.seed_url_exists and not plan.blocked_reason]
    blocked = [plan for plan in report.plans if plan.blocked_reason == "needs_seed_url_discovery"]

    assert len(ready) == 30
    assert len(blocked) == 4
    assert all(plan.fetcher in {"requests", "firecrawl"} for plan in ready)
    assert all(plan.fetcher is None for plan in blocked)


def test_fetch_plan_assigns_seeded_official_portal_profile():
    report = build_portal_fetch_plan_report(repo_root=ROOT)

    marvin = next(plan for plan in report.plans if plan.title == "Marvin G. Johnson")

    assert marvin.portal_profile_id == "agency_ois_detail"
    assert marvin.seed_url_exists is True
    assert marvin.fetcher == "firecrawl"
    assert marvin.allowed_domain == "www.chicago.gov"
    assert "official_critical_incident_video" in marvin.expected_artifact_types


def test_fetch_plan_updates_replay_scoreboard_metrics():
    replay = run_calibration_replay(repo_root=ROOT)

    assert replay.metrics["likely_firecrawl_needed_count"] == 34
    assert replay.metrics["ready_for_portal_fetch_count"] == 30
    assert replay.metrics["needs_seed_url_discovery_count"] == 4


def test_fetch_plan_report_is_json_serializable():
    report = build_portal_fetch_plan_report(repo_root=ROOT)

    encoded = json.dumps(portal_fetch_plan_to_jsonable(report), sort_keys=True)
    decoded = json.loads(encoded)

    assert decoded["candidate_cases"] == 34
    assert decoded["plans"][0]["portal_profile_id"]


def test_fetch_plan_makes_no_network_or_firecrawl_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    build_portal_fetch_plan_report(repo_root=ROOT)
    assert calls == []
