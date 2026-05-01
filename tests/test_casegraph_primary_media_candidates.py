"""PRIMARY1 - official-primary media candidate miner tests."""
from __future__ import annotations

import json
from pathlib import Path

from pipeline2_discovery.casegraph import (
    mine_primary_media_candidates,
)


ROOT = Path(__file__).resolve().parents[1]


def test_miner_finds_ranked_official_agency_ois_bodycam_candidate():
    report = mine_primary_media_candidates(
        [ROOT / "tests" / "fixtures" / "agency_ois"],
        repo_root=ROOT,
    )

    assert report.total_candidates >= 1
    top = report.candidates[0]
    assert top.case_name == "John Example"
    assert top.agency == "Phoenix Police Department"
    assert top.expected_artifact_type == "bodycam"
    assert "bodycam" in top.media_signal_terms
    assert "agency_ois" in top.likely_connector_path
    assert any(url.endswith(".mp4") for url in top.known_urls)
    assert top.confidence_media_is_tier_a >= 0.75
    assert "generic_youtube_or_video_host_only" not in top.risk_flags


def test_miner_does_not_invent_media_claims_for_document_only_fixture():
    report = mine_primary_media_candidates(
        [ROOT / "tests" / "fixtures" / "casegraph_scenarios" / "document_only_hold.json"],
        repo_root=ROOT,
    )

    assert report.total_candidates == 0
    assert report.candidates == []


def test_miner_distinguishes_claim_only_from_verified_artifact_url():
    report = mine_primary_media_candidates(
        [
            ROOT / "tests" / "fixtures" / "casegraph_scenarios" / "structured_official_bodycam_claim_hold.json",
            ROOT / "tests" / "fixtures" / "casegraph_scenarios" / "structured_verified_bodycam_produce.json",
        ],
        repo_root=ROOT,
    )

    by_id = {candidate.candidate_id: candidate for candidate in report.candidates}
    claim = next(c for c in by_id.values() if "claim_hold" in c.candidate_id)
    verified = next(c for c in by_id.values() if "produce" in c.candidate_id)

    assert "claim_signal_not_verified_artifact" in claim.risk_flags
    assert claim.confidence_media_is_tier_a < verified.confidence_media_is_tier_a
    assert any(url.endswith(".mp4") for url in verified.known_urls)


def test_miner_finds_real_calibration_rows_but_flags_generic_video_hosts():
    report = mine_primary_media_candidates(
        [ROOT / "autoresearch" / "calibration_data.json"],
        repo_root=ROOT,
    )

    assert report.total_candidates >= 1
    assert any(candidate.case_name == "Min Jian Guan" for candidate in report.candidates)
    youtube_candidates = [
        candidate
        for candidate in report.candidates
        if any("youtube.com" in url or "youtu.be" in url for url in candidate.known_urls)
    ]
    assert youtube_candidates
    assert any(
        "generic_youtube_or_video_host_only" in candidate.risk_flags
        for candidate in youtube_candidates
    )


def test_miner_output_is_json_serializable():
    report = mine_primary_media_candidates(
        [ROOT / "tests" / "fixtures" / "agency_ois"],
        repo_root=ROOT,
        top_n=3,
    )

    encoded = json.dumps(report.to_dict(), sort_keys=True)
    decoded = json.loads(encoded)
    assert decoded["total_candidates"] == len(decoded["candidates"])


def test_miner_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    mine_primary_media_candidates(
        [ROOT / "tests" / "fixtures" / "agency_ois"],
        repo_root=ROOT,
    )

    assert calls == []
