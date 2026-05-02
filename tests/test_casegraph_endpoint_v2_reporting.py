"""EVAL9 - Endpoint v2 status reporting."""
import json

from pipeline2_discovery.casegraph import build_endpoint_v2_status_report


def test_endpoint_v2_report_marks_achieved_with_tier_a_produce():
    report = build_endpoint_v2_status_report(
        endpoint_v2_status={
            "endpoint_v2_achieved": True,
            "pilot_id": "real_tier_a",
            "connector_used": "youtube",
            "query": "Real Case bodycam",
            "live_calls_used": 1,
            "source_records_returned": 2,
            "verified_artifact_count": 1,
            "media_artifact_count": 1,
            "media_relevance_tiers": {"A": 1},
            "verdict": "PRODUCE",
            "risk_flags": [],
            "reason_codes": ["high_identity", "sentenced_or_convicted"],
            "estimated_cost_usd": 0.0,
        }
    )

    endpoint = report["endpoint_v2"]
    assert endpoint["achieved"] is True
    assert endpoint["tier_a_media_count"] == 1
    assert endpoint["weak_media_advisory_count"] == 0
    assert endpoint["blockers"] == []
    assert endpoint["connector_yields"]["youtube"]["verified_artifacts"] == 1


def test_endpoint_v2_report_surfaces_parked_live9_blockers():
    report = build_endpoint_v2_status_report(
        endpoint_v2_status={
            "endpoint_v2_achieved": False,
            "pilot_id": "primary_media_alan_champagne_youtube_pilot",
            "connector_used": "youtube",
            "query": None,
            "live_calls_used": 1,
            "source_records_returned": 0,
            "verified_artifact_count": 0,
            "media_artifact_count": 0,
            "media_relevance_tiers": {},
            "verdict": "HOLD",
            "risk_flags": ["no_verified_media"],
            "reason_codes": ["high_identity", "sentenced_or_convicted", "no_verified_media"],
            "estimated_cost_usd": 0.0,
            "blockers": ["no_verified_tier_a_media", "verdict_not_produce"],
        }
    )

    endpoint = report["endpoint_v2"]
    assert endpoint["achieved"] is False
    assert endpoint["tier_a_media_count"] == 0
    assert endpoint["live_call_count"] == 1
    assert "no_verified_artifacts" in endpoint["blockers"]
    assert "no_verified_tier_a_media" in endpoint["blockers"]
    assert "verdict_not_produce" in endpoint["blockers"]
    assert any("yt-dlp" in action for action in endpoint["next_actions"])


def test_endpoint_v2_report_counts_weak_media_advisories():
    report = build_endpoint_v2_status_report(
        endpoint_v2_status={
            "endpoint_v2_achieved": False,
            "connector_used": "youtube",
            "source_records_returned": 1,
            "verified_artifact_count": 1,
            "media_artifact_count": 1,
            "media_relevance_tiers": {"C": 1},
            "verdict": "PRODUCE",
            "risk_flags": ["produce_based_on_weak_or_uncertain_media"],
            "reason_codes": ["produce_based_on_weak_or_uncertain_media"],
            "estimated_cost_usd": 0.0,
        }
    )

    endpoint = report["endpoint_v2"]
    assert endpoint["tier_c_or_unknown_media_count"] == 1
    assert endpoint["weak_media_advisory_count"] == 2
    assert "no_verified_tier_a_media" in endpoint["blockers"]


def test_endpoint_v2_report_compares_previous_endpoint_statuses():
    report = build_endpoint_v2_status_report(
        endpoint_v0_status={
            "endpoint_v0_fully_achieved": True,
            "verified_artifact_count": 5,
            "document_artifact_count": 5,
        },
        endpoint_v1_status={
            "endpoint_v1_achieved": True,
            "media_artifact_count": 5,
            "verdict": "PRODUCE",
        },
        endpoint_v11_media_quality={
            "warnings": ["produce_based_only_on_weak_or_uncertain_media:case"],
            "tier_counts": {"A": 0, "B": 0, "C": 5, "unknown": 0},
        },
        endpoint_v2_status={"endpoint_v2_achieved": False},
    )

    assert report["endpoint_v0"]["achieved"] is True
    assert report["endpoint_v0"]["document_artifact_count"] == 5
    assert report["endpoint_v1"]["achieved"] is True
    assert report["endpoint_v1"]["media_artifact_count"] == 5
    assert report["endpoint_v1_1"]["weak_media_warning_count"] == 1
    assert report["endpoint_v1_1"]["tier_counts"]["C"] == 5


def test_endpoint_v2_report_is_json_serializable():
    report = build_endpoint_v2_status_report(
        endpoint_v2_status={
            "endpoint_v2_achieved": False,
            "connector_used": "youtube",
            "media_relevance_tiers": {"unknown": 1},
        }
    )
    decoded = json.loads(json.dumps(report))
    assert decoded == report


def test_endpoint_v2_report_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)
    build_endpoint_v2_status_report(endpoint_v2_status={"endpoint_v2_achieved": False})
    assert calls == []
