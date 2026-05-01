"""PORTAL3 - mocked seeded portal executor tests."""
import json
from pathlib import Path

from pipeline2_discovery.casegraph.portal_executor import (
    execute_mock_portal_plan,
    portal_execution_to_jsonable,
)
from pipeline2_discovery.casegraph.portal_fetch_plan import PortalFetchPlan


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "agency_ois"


def _load_fixture(name):
    with (FIXTURE_DIR / name).open("r", encoding="utf-8") as f:
        data = json.load(f)
    data["portal_profile_id"] = "agency_ois_detail"
    return data


def _plan(**overrides):
    base = {
        "case_id": 1,
        "title": "Agency OIS Example",
        "portal_profile_id": "agency_ois_detail",
        "seed_url": "https://www.phoenix.gov/police/critical-incidents/2024-OIS-050",
        "seed_url_exists": True,
        "fetcher": "firecrawl",
        "max_pages": 2,
        "max_links": 40,
        "allowed_domain": "www.phoenix.gov",
        "expected_artifact_types": ["bodycam", "official_critical_incident_video"],
        "resolver_policy": {
            "metadata_only": True,
            "require_public_url": True,
            "allow_downloads": False,
            "allow_scraping": False,
            "allow_login": False,
        },
        "needs_seed_url_discovery": False,
        "blocked_reason": None,
        "safety_flags": ["public_only"],
    }
    base.update(overrides)
    return PortalFetchPlan(**base)


def test_mock_executor_extracts_agency_ois_media_candidates():
    result = execute_mock_portal_plan(
        _plan(),
        _load_fixture("incident_detail_with_youtube_embed.json"),
    )

    assert result.execution_status == "completed"
    assert result.mocked_fetch_status == "ok"
    assert result.candidate_artifact_urls == ["https://www.youtube.com/watch?v=officialBWC050"]
    assert result.rejected_urls == []
    assert any(action.startswith("candidate_ready_for_resolver:") for action in result.resolver_actions)
    assert result.artifact_claims


def test_mock_executor_claim_only_page_creates_claim_but_no_artifact_candidate():
    result = execute_mock_portal_plan(
        _plan(seed_url="https://www.phoenix.gov/police/critical-incidents/2024-OIS-031"),
        _load_fixture("incident_detail_with_bodycam_claim_no_url.json"),
    )

    assert result.artifact_claims
    assert result.candidate_artifact_urls == []
    assert all("candidate_ready_for_resolver" not in action for action in result.resolver_actions)


def test_mock_executor_document_only_page_produces_document_candidate_not_verdict():
    result = execute_mock_portal_plan(
        _plan(seed_url="https://www.phoenix.gov/police/critical-incidents/2024-OIS-022"),
        _load_fixture("incident_detail_with_pdf.json"),
    )

    assert result.candidate_artifact_urls == [
        "https://www.phoenix.gov/police/docs/2024-OIS-022-IA-report.pdf"
    ]
    assert result.execution_status == "completed"
    assert "PRODUCE" not in json.dumps(portal_execution_to_jsonable(result))


def test_mock_executor_rejects_protected_private_links():
    result = execute_mock_portal_plan(
        _plan(seed_url="https://www.phoenix.gov/police/critical-incidents/2024-OIS-040"),
        _load_fixture("incident_detail_with_protected_link.json"),
    )

    assert result.rejected_urls == [
        "https://portal.phoenix.gov/login?redirect=/oa/2024-OIS-040.mp4"
    ]
    assert "protected_or_nonpublic" in result.risk_flags
    assert all("login" not in url for url in result.candidate_artifact_urls)


def test_mock_executor_respects_plan_link_cap():
    result = execute_mock_portal_plan(
        _plan(max_links=1),
        _load_fixture("incident_detail_with_protected_link.json"),
    )

    link_records = [
        record for record in result.extracted_source_records
        if record["metadata"].get("fixture_kind") != "agency_page"
    ]
    assert len(link_records) == 1


def test_mock_executor_blocks_plan_without_seed_url():
    result = execute_mock_portal_plan(
        _plan(seed_url=None, seed_url_exists=False, fetcher=None, blocked_reason="needs_seed_url_discovery"),
        _load_fixture("incident_detail_with_youtube_embed.json"),
    )

    assert result.execution_status == "blocked"
    assert result.mocked_fetch_status == "not_fetched"
    assert result.extracted_source_records == []
    assert "needs_seed_url_discovery" in result.risk_flags


def test_mock_executor_result_is_json_serializable():
    result = execute_mock_portal_plan(
        _plan(),
        _load_fixture("incident_detail_with_youtube_embed.json"),
    )

    encoded = json.dumps(portal_execution_to_jsonable(result), sort_keys=True)
    decoded = json.loads(encoded)

    assert decoded["profile_id"] == "agency_ois_detail"
    assert decoded["candidate_artifact_urls"]


def test_mock_executor_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    execute_mock_portal_plan(
        _plan(),
        _load_fixture("incident_detail_with_youtube_embed.json"),
    )
    assert calls == []
