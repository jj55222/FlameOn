"""PORTAL5 - calibration portal replay manifest tests."""
import json
from pathlib import Path

from pipeline2_discovery.casegraph.portal_dry_replay import (
    load_portal_replay_manifest,
    portal_dry_replay_to_jsonable,
    run_portal_replay_manifest,
)


ROOT = Path(__file__).resolve().parents[1]


def test_portal_replay_manifest_loads_and_references_existing_fixtures():
    manifest = load_portal_replay_manifest(repo_root=ROOT)

    assert manifest.version == 1
    assert len(manifest.entries) >= 5
    for entry in manifest.entries:
        assert (ROOT / entry.mocked_payload_fixture).exists()
        assert entry.portal_profile_id
        assert entry.expected_source_records >= 1


def test_portal_replay_manifest_covers_representative_shapes():
    manifest = load_portal_replay_manifest(repo_root=ROOT)
    notes = " ".join(" ".join(entry.notes) for entry in manifest.entries).lower()

    assert "youtube embed" in notes
    assert "claim-only" in notes
    assert "document-only" in notes
    assert "protected" in notes
    assert "generic weak youtube" in notes


def test_portal_replay_manifest_runs_and_matches_expectations():
    manifest = load_portal_replay_manifest(repo_root=ROOT)
    report = run_portal_replay_manifest(manifest, repo_root=ROOT)
    by_case = {result.case_id: result for result in report.case_results}

    assert report.total_plans == len(manifest.entries)
    assert report.executed_count == len(manifest.entries)
    for entry in manifest.entries:
        result = by_case[entry.case_id]
        assert result.source_records_count == entry.expected_source_records
        assert result.artifact_claims_count == entry.expected_artifact_claims
        assert result.candidate_urls_count == entry.expected_candidate_urls
        assert result.rejected_urls_count == entry.expected_rejected_urls
        assert result.resolver_actions_count == entry.expected_resolver_actions
        for blocker in entry.expected_blockers:
            assert blocker in result.blockers


def test_portal_replay_manifest_result_is_json_serializable():
    report = run_portal_replay_manifest(repo_root=ROOT)

    encoded = json.dumps(portal_dry_replay_to_jsonable(report), sort_keys=True)
    decoded = json.loads(encoded)

    assert decoded["executed_count"] >= 5
    assert decoded["case_results"][0]["source_records_count"] >= 1


def test_portal_replay_manifest_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    run_portal_replay_manifest(repo_root=ROOT)
    assert calls == []
