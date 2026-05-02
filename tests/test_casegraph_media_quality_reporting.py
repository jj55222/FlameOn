"""EVAL8 - media-yield quality reporting tests."""
from __future__ import annotations

import json

from pipeline2_discovery.casegraph import (
    CaseIdentity,
    CaseInput,
    CasePacket,
    Jurisdiction,
    VerifiedArtifact,
    build_media_quality_report,
)


def packet(case_id: str, artifacts: list[VerifiedArtifact], *, verdict_hint: str = "PRODUCE") -> CasePacket:
    return CasePacket(
        case_id=case_id,
        input=CaseInput(input_type="fixture", known_fields={"defendant_names": ["Jane Example"]}),
        case_identity=CaseIdentity(
            defendant_names=["Jane Example"],
            agency="Example Police Department",
            jurisdiction=Jurisdiction(city="Phoenix", state="AZ"),
            incident_date="2024-01-02",
            outcome_status="sentenced",
            identity_confidence="high",
            identity_anchors=["full_name", "jurisdiction", "agency", "case_number"],
        ),
        verified_artifacts=artifacts,
        verdict=verdict_hint,
    )


def artifact(
    artifact_id: str,
    artifact_type: str,
    *,
    title: str,
    url: str | None = None,
    fmt: str = "video",
    source_authority: str = "official",
    downloadable: bool = True,
    metadata: dict | None = None,
) -> VerifiedArtifact:
    merged_metadata = {"title": title}
    if metadata:
        merged_metadata.update(metadata)
    return VerifiedArtifact(
        artifact_id=artifact_id,
        artifact_type=artifact_type,
        artifact_url=url or f"https://www.youtube.com/watch?v={artifact_id}",
        source_url=url or f"https://www.youtube.com/watch?v={artifact_id}",
        source_authority=source_authority,
        downloadable=downloadable,
        format=fmt,
        matched_case_fields=["defendant_full_name", "agency"],
        confidence=0.88,
        verification_method="fixture",
        metadata=merged_metadata,
    )


def test_tier_a_media_report_counts_primary_source_media():
    report = build_media_quality_report([
        packet("tier_a", [artifact("bodycam001", "bodycam", title="Police bodycam BWC footage")])
    ])

    assert report["total_media_artifacts"] == 1
    assert report["tier_counts"]["A"] == 1
    assert report["primary_source_media_count"] == 1
    assert report["top_media_artifacts"][0]["media_relevance_tier"] == "A"


def test_tier_b_report_counts_secondary_media():
    report = build_media_quality_report([
        packet("tier_b", [artifact("court001", "court_video", title="Sentencing court video")])
    ])

    assert report["tier_counts"]["B"] == 1
    assert report["secondary_source_media_count"] == 1


def test_tier_c_only_produce_emits_warning():
    report = build_media_quality_report([
        packet(
            "tier_c_produce",
            [
                artifact(
                    "generic001",
                    "other_video",
                    title="Christa Pike court hearing update",
                    source_authority="official",
                    metadata={"query_used": "Christa Gail Pike bodycam"},
                )
            ],
        )
    ])

    assert report["tier_counts"]["C"] == 1
    assert report["produce_media_basis_counts"]["C"] == 1
    assert any(w.startswith("produce_based_only_on_weak_or_uncertain_media:tier_c_produce") for w in report["warnings"])


def test_unknown_or_weak_media_emits_manual_review_warning():
    report = build_media_quality_report([
        packet("weak_media", [artifact("weak001", "other_video", title="Full story explained")])
    ])

    assert report["needs_manual_review_count"] == 1
    assert any("manual_review_media_relevance" in w for w in report["warnings"])


def test_bodycam_query_mismatch_warning_is_reported():
    report = build_media_quality_report([
        packet(
            "bodycam_mismatch",
            [
                artifact(
                    "mismatch001",
                    "other_video",
                    title="Court hearing update",
                    metadata={"query_used": "Jane Example bodycam"},
                )
            ],
        )
    ])

    assert any("media_query_artifact_type_mismatch:bodycam_mismatch:mismatch001" == w for w in report["warnings"])


def test_document_only_case_has_zero_media_quality_counts():
    report = build_media_quality_report([
        packet(
            "doc_only",
            [
                artifact(
                    "doc001",
                    "docket_docs",
                    title="Complaint PDF",
                    url="https://example.gov/complaint.pdf",
                    fmt="pdf",
                    source_authority="court",
                )
            ],
            verdict_hint="HOLD",
        )
    ])

    assert report["total_media_artifacts"] == 0
    assert report["tier_counts"] == {"A": 0, "B": 0, "C": 0, "unknown": 0}
    assert report["top_media_artifacts"] == []


def test_mixed_media_and_document_artifacts_report_media_only_for_quality():
    report = build_media_quality_report([
        packet(
            "mixed",
            [
                artifact("bodycam002", "bodycam", title="Bodycam video"),
                artifact(
                    "doc002",
                    "docket_docs",
                    title="Docket PDF",
                    url="https://example.gov/docket.pdf",
                    fmt="pdf",
                    source_authority="court",
                ),
            ],
        )
    ])

    assert report["total_media_artifacts"] == 1
    assert report["tier_counts"]["A"] == 1
    assert report["top_media_artifacts"][0]["artifact_id"] == "bodycam002"


def test_media_quality_report_is_json_serializable():
    report = build_media_quality_report([
        packet("json_case", [artifact("json001", "court_video", title="Trial video")])
    ])

    encoded = json.dumps(report, sort_keys=True)
    assert json.loads(encoded)["total_media_artifacts"] == 1


def test_media_quality_report_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    build_media_quality_report([
        packet("no_network", [artifact("net001", "bodycam", title="Police bodycam")])
    ])

    assert calls == []
