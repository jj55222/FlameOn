"""CAL1 - calibration set profiler tests."""
import json
from pathlib import Path

from pipeline2_discovery.casegraph.calibration_profile import (
    CalibrationProfileReport,
    profile_calibration_set,
)


ROOT = Path(__file__).resolve().parents[1]


def test_profiles_all_legacy_calibration_cases_once():
    report = profile_calibration_set(repo_root=ROOT)

    assert isinstance(report, CalibrationProfileReport)
    assert report.total_cases == 38
    assert len(report.profile_rows) == 38
    assert [row.case_id for row in report.profile_rows] == list(range(38))
    assert len(report.source_paths) == 2


def test_profile_identifies_media_and_tier_a_cases():
    report = profile_calibration_set(repo_root=ROOT)
    summary = report.summary

    assert summary["media_positive_cases"] == 27
    assert summary["tier_a_primary_media_cases"] == 26
    assert summary["document_artifact_cases"] == 27
    assert summary["negative_or_no_artifact_cases"] == 18

    alan = next(row for row in report.profile_rows if row.title == "Alan Matthew Champagne")
    assert "tier_a_primary_media_case" in alan.benchmark_roles
    assert {"bodycam", "interrogation", "dispatch_911"} <= set(alan.expected_media_types)
    assert "youtube" in alan.likely_connector_path
    assert alan.supported_live_path_available is True


def test_profile_distinguishes_generic_media_from_primary_source_needs():
    report = profile_calibration_set(repo_root=ROOT)
    christa = next(row for row in report.profile_rows if row.title.startswith("Christa Gail Pike"))

    assert "youtube" in christa.source_types_already_known
    assert "tier_a_media_needs_primary_source_verification" in christa.risk_flags
    assert christa.needed_portal_profile in {
        "youtube_agency_channel",
        "courtlistener_search",
        "source_discovery_required",
    }


def test_profile_marks_document_only_and_negative_cases():
    report = profile_calibration_set(repo_root=ROOT)

    document_only = [
        row for row in report.profile_rows
        if "document_only_expected" in row.risk_flags
    ]
    no_artifact = [
        row for row in report.profile_rows
        if "no_known_artifact_signal" in row.risk_flags
    ]

    assert len(document_only) >= 1
    assert len(no_artifact) >= 1
    assert all("document_artifact_case" in row.benchmark_roles for row in document_only)
    assert all(row.benchmark_role == "negative_or_no_artifact_case" for row in no_artifact)


def test_profile_includes_supported_connector_and_portal_fields():
    report = profile_calibration_set(repo_root=ROOT)
    rows = report.profile_rows

    assert any(row.supported_live_path_available for row in rows)
    assert any("youtube" in row.likely_connector_path for row in rows)
    assert any("courtlistener" in row.likely_connector_path for row in rows)
    assert all(isinstance(row.portal_profiles_needed, list) for row in rows)
    assert all(row.needed_portal_profile for row in rows)


def test_profile_report_is_json_serializable():
    report = profile_calibration_set(repo_root=ROOT)

    encoded = json.dumps(report.to_dict())
    decoded = json.loads(encoded)

    assert decoded["total_cases"] == 38
    assert len(decoded["profile_rows"]) == 38


def test_profiler_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    profile_calibration_set(repo_root=ROOT)
    assert calls == []
