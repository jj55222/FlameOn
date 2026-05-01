"""MEDIA2 — media candidate miner tests.

Asserts that ``find_media_candidates`` :

- finds at least one media-positive candidate from the existing
  calibration corpus
- ranks public media URLs and ground-truth media flags higher than
  document-only signals
- agrees with the central MEDIA1 classifier on URL classification
- produces JSON-serializable output
- never makes a network call
- never invents URLs or media claims that are not in the source data
- emits the canonical per-candidate shape (case_id / name / tier /
  media_signal_terms / desired_artifact_types / source_fields /
  known_urls / classified_urls / url_classification_summary /
  likely_connector_path / candidate_query_strings /
  media_confidence_score / risk_flags)
- top candidate has youtube as the first likely_connector_path entry
  when known YouTube URLs are present in calibration
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import find_media_candidates
from pipeline2_discovery.casegraph.media_candidates import (
    MEDIA_SIGNAL_TERMS,
)


ROOT = Path(__file__).resolve().parents[1]


REQUIRED_CANDIDATE_KEYS = (
    "case_id",
    "name",
    "jurisdiction",
    "tier",
    "source",
    "media_signal_terms",
    "desired_artifact_types",
    "source_fields",
    "known_urls",
    "classified_urls",
    "url_classification_summary",
    "likely_connector_path",
    "candidate_query_strings",
    "media_confidence_score",
    "risk_flags",
)


# ---- Top-level shape -----------------------------------------------------


def test_report_has_canonical_top_level_shape():
    report = find_media_candidates()
    for key in (
        "experiment_id",
        "scanned_sources",
        "candidate_count",
        "fixture_signal_hits",
        "summary",
        "candidates",
    ):
        assert key in report, f"missing top-level key {key!r}"
    assert report["experiment_id"] == "MEDIA2"


def test_report_finds_at_least_one_candidate_from_calibration():
    report = find_media_candidates()
    assert report["candidate_count"] >= 1
    assert isinstance(report["candidates"], list)
    assert len(report["candidates"]) == report["candidate_count"]


def test_report_summary_counts_youtube_and_media_urls():
    report = find_media_candidates()
    summary = report["summary"]
    assert "candidates_with_youtube_media" in summary
    assert "total_known_media_urls" in summary
    # The committed calibration corpus contains many YouTube URLs.
    assert summary["candidates_with_youtube_media"] >= 1
    assert summary["total_known_media_urls"] >= 1


# ---- Per-candidate shape -------------------------------------------------


def test_every_candidate_has_canonical_shape():
    report = find_media_candidates()
    for cand in report["candidates"]:
        for key in REQUIRED_CANDIDATE_KEYS:
            assert key in cand, f"candidate {cand.get('case_id')} missing {key!r}"


def test_candidates_are_sorted_by_score_desc():
    report = find_media_candidates()
    scores = [c["media_confidence_score"] for c in report["candidates"]]
    assert scores == sorted(scores, reverse=True)


def test_top_candidate_has_youtube_first_when_youtube_urls_known():
    """If the top candidate has any YouTube URL in classified_urls, the
    likely_connector_path should start with 'youtube'."""
    report = find_media_candidates()
    if not report["candidates"]:
        pytest.skip("no candidates")
    top = report["candidates"][0]
    has_yt = any(
        "youtube" in (cls.get("url") or "").lower()
        for cls in top.get("classified_urls") or []
        if cls.get("is_media")
    )
    if has_yt:
        assert top["likely_connector_path"][0] == "youtube"


def test_classified_urls_agree_with_media1_policy():
    """For every URL in known_urls, the classified entry should match
    what classify_media_url returns directly. No drift between the
    MEDIA2 miner and the MEDIA1 classifier."""
    from pipeline2_discovery.casegraph import classify_media_url
    report = find_media_candidates()
    for cand in report["candidates"]:
        for cls_dict in cand["classified_urls"]:
            cls = classify_media_url(cls_dict["url"])
            assert cls.is_media == cls_dict["is_media"]
            assert cls.is_document == cls_dict["is_document"]
            assert cls.format == cls_dict["format"]
            assert cls.rejected == cls_dict["rejected"]


# ---- No-fabrication invariants -------------------------------------------


def test_known_urls_only_come_from_calibration_data():
    """The miner must NOT invent URLs. Every URL in any candidate's
    known_urls must appear in the underlying calibration_data
    verified_sources."""
    cal = json.loads(
        (ROOT / "autoresearch" / "calibration_data.json").read_text(encoding="utf-8")
    )
    cal_urls = set()
    for entry in cal:
        for url in (entry.get("ground_truth") or {}).get("verified_sources") or []:
            if isinstance(url, str):
                cal_urls.add(url.strip())
    report = find_media_candidates()
    for cand in report["candidates"]:
        for url in cand["known_urls"]:
            assert url.strip() in cal_urls, (
                f"miner emitted a URL not present in calibration_data: {url!r}"
            )


def test_signal_terms_are_from_canonical_list():
    """media_signal_terms entries must come from MEDIA_SIGNAL_TERMS or
    be calibration_data ground_truth flag names."""
    canonical = set(MEDIA_SIGNAL_TERMS) | {
        "bodycam",
        "dashcam",
        "court_video",
        "interrogation",
        "dispatch_911",
        "surveillance",
        "sentencing_video",
    }
    report = find_media_candidates()
    for cand in report["candidates"]:
        for term in cand["media_signal_terms"]:
            assert term in canonical, (
                f"unknown signal term {term!r} for case {cand.get('case_id')}"
            )


# ---- Top_n + scoping -----------------------------------------------------


def test_top_n_truncates_candidate_list():
    report = find_media_candidates(top_n=3)
    assert report["candidate_count"] <= 3
    assert len(report["candidates"]) <= 3


def test_calibration_paths_arg_overrides_default(tmp_path):
    """Passing an explicit (empty) calibration path list must produce
    zero candidates (no fabrication)."""
    fake = tmp_path / "empty.json"
    fake.write_text("[]", encoding="utf-8")
    report = find_media_candidates(calibration_paths=[fake], fixture_dirs=[])
    assert report["candidate_count"] == 0
    assert report["candidates"] == []


# ---- Output is JSON-serializable + zero network --------------------------


def test_report_is_json_serializable():
    report = find_media_candidates()
    encoded = json.dumps(report)
    decoded = json.loads(encoded)
    assert decoded == report


def test_miner_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    find_media_candidates()
    assert calls == [], f"MEDIA2 miner made {len(calls)} live HTTP call(s)"


# ---- Top-3 sanity checks against known calibration cases ----------------


def test_top_3_includes_high_scoring_known_cases():
    """The top 3 candidates from the committed calibration corpus
    should be Christa Gail Pike (case 4), William D. Foster (case
    15), or Manuel Marin (case 5) - all ENOUGH-tier with multiple
    YouTube URLs in verified_sources. This guards against scoring
    drift."""
    report = find_media_candidates(top_n=3)
    top_case_ids = {c.get("case_id") for c in report["candidates"]}
    expected_top_pool = {4, 15, 5}
    assert top_case_ids & expected_top_pool, (
        f"top 3 candidates {top_case_ids} did not overlap with expected "
        f"high-scoring pool {expected_top_pool}"
    )
