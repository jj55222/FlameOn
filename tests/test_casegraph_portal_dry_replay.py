"""PORTAL4 - integrated no-live portal dry replay tests."""
import json
from pathlib import Path

from pipeline2_discovery.casegraph.portal_dry_replay import (
    build_portal_dry_replay_report,
    portal_dry_replay_to_jsonable,
)
from pipeline2_discovery.casegraph.portal_fetch_plan import PortalFetchPlan, build_portal_fetch_plan_report


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "agency_ois"


def _load_fixture(name):
    with (FIXTURE_DIR / name).open("r", encoding="utf-8") as f:
        data = json.load(f)
    data["portal_profile_id"] = "agency_ois_detail"
    return data


def _marvin_plan():
    plans = build_portal_fetch_plan_report(repo_root=ROOT).plans
    return next(plan for plan in plans if plan.title == "Marvin G. Johnson")


def _custom_plan(**overrides):
    base = {
        "case_id": 999,
        "title": "Unsafe Example",
        "portal_profile_id": "agency_ois_detail",
        "seed_url": "https://www.phoenix.gov/police/critical-incidents/2024-OIS-050",
        "seed_url_exists": True,
        "fetcher": "firecrawl",
        "max_pages": 2,
        "max_links": 40,
        "allowed_domain": "www.phoenix.gov",
        "expected_artifact_types": ["bodycam"],
        "resolver_policy": {
            "metadata_only": True,
            "require_public_url": True,
            "allow_downloads": False,
            "allow_scraping": False,
            "allow_login": False,
        },
        "needs_seed_url_discovery": False,
        "blocked_reason": None,
        "safety_flags": [],
    }
    base.update(overrides)
    return PortalFetchPlan(**base)


def test_dry_replay_executes_representative_seeded_agency_case():
    plan = _marvin_plan()
    report = build_portal_dry_replay_report(
        plans=[plan],
        mocked_payloads_by_case_id={
            plan.case_id: _load_fixture("incident_detail_with_youtube_embed.json")
        },
        repo_root=ROOT,
    )
    result = report.case_results[0]

    assert report.total_plans == 1
    assert report.executed_count == 1
    assert result.fetch_plan_status == "ready"
    assert result.safety_status == "allowed"
    assert result.executor_status == "completed"
    assert result.source_records_count > 0
    assert result.artifact_claims_count > 0
    assert result.candidate_urls_count == 1
    assert result.resolver_actions_count == 1


def test_dry_replay_reports_missing_mock_payload_blocker():
    plan = _marvin_plan()
    report = build_portal_dry_replay_report(plans=[plan], repo_root=ROOT)
    result = report.case_results[0]

    assert report.executed_count == 0
    assert report.missing_payload_count == 1
    assert result.safety_status == "allowed"
    assert result.executor_status == "skipped"
    assert "mock_payload_missing" in result.blockers


def test_dry_replay_blocks_unsafe_fetch_plan_before_executor():
    plan = _custom_plan(max_pages=99)
    report = build_portal_dry_replay_report(
        plans=[plan],
        mocked_payloads_by_case_id={999: _load_fixture("incident_detail_with_youtube_embed.json")},
        repo_root=ROOT,
    )
    result = report.case_results[0]

    assert result.safety_status == "blocked"
    assert result.executor_status == "skipped"
    assert result.source_records_count == 0
    assert "max_pages_exceeds_profile_cap" in result.blockers


def test_dry_replay_propagates_protected_link_diagnostics():
    plan = _custom_plan(case_id=1000)
    report = build_portal_dry_replay_report(
        plans=[plan],
        mocked_payloads_by_case_id={
            1000: _load_fixture("incident_detail_with_protected_link.json")
        },
        repo_root=ROOT,
    )
    result = report.case_results[0]

    assert result.executor_status == "completed"
    assert result.candidate_urls_count == 1
    assert result.rejected_urls_count == 1
    assert "protected_or_nonpublic" in result.blockers


def test_dry_replay_respects_blocked_fetch_plan_status():
    plan = _custom_plan(seed_url=None, seed_url_exists=False, fetcher=None, blocked_reason="needs_seed_url_discovery")
    report = build_portal_dry_replay_report(plans=[plan], repo_root=ROOT)
    result = report.case_results[0]

    assert result.fetch_plan_status == "blocked"
    assert result.safety_status == "not_run"
    assert result.executor_status == "skipped"
    assert "needs_seed_url_discovery" in result.blockers


def test_dry_replay_report_is_json_serializable():
    plan = _marvin_plan()
    report = build_portal_dry_replay_report(
        plans=[plan],
        mocked_payloads_by_case_id={
            plan.case_id: _load_fixture("incident_detail_with_youtube_embed.json")
        },
        repo_root=ROOT,
    )

    encoded = json.dumps(portal_dry_replay_to_jsonable(report), sort_keys=True)
    decoded = json.loads(encoded)

    assert decoded["executed_count"] == 1
    assert decoded["case_results"][0]["candidate_urls_count"] == 1


def test_dry_replay_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    plan = _marvin_plan()
    build_portal_dry_replay_report(
        plans=[plan],
        mocked_payloads_by_case_id={
            plan.case_id: _load_fixture("incident_detail_with_youtube_embed.json")
        },
        repo_root=ROOT,
    )
    assert calls == []
