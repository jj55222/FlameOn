"""MEDIA4 - media artifact relevance classifier tests."""
from __future__ import annotations

import json

from pipeline2_discovery.casegraph import (
    SourceRecord,
    VerifiedArtifact,
    classify_media_relevance,
)


def artifact(
    artifact_id: str,
    *,
    title: str,
    artifact_type: str = "video_footage",
    url: str | None = None,
    source_authority: str = "news",
    fmt: str = "video",
    metadata: dict | None = None,
    risk_flags: list[str] | None = None,
) -> VerifiedArtifact:
    merged_metadata = {"title": title}
    if metadata:
        merged_metadata.update(metadata)
    return VerifiedArtifact(
        artifact_id=artifact_id,
        artifact_type=artifact_type,
        artifact_url=url or f"https://www.youtube.com/watch?v={artifact_id}",
        source_authority=source_authority,
        downloadable=False,
        format=fmt,
        source_url=url or f"https://www.youtube.com/watch?v={artifact_id}",
        matched_case_fields=["defendant_full_name"],
        confidence=0.84,
        verification_method="public_video_host:youtube",
        risk_flags=risk_flags or [],
        metadata=merged_metadata,
    )


def source(**kwargs) -> SourceRecord:
    return SourceRecord(
        source_id="source_001",
        url=kwargs.get("url", "https://example.gov/critical-incident"),
        title=kwargs.get("title", "Official critical incident video"),
        snippet=kwargs.get("snippet", ""),
        source_type=kwargs.get("source_type", "official_video"),
        source_authority=kwargs.get("source_authority", "official"),
        source_roles=kwargs.get("source_roles", ["artifact_source"]),
        api_name=kwargs.get("api_name", "fixture"),
        discovered_via=kwargs.get("discovered_via", "fixture"),
        retrieved_at="2026-04-30T00:00:00Z",
        metadata=kwargs.get("metadata", {}),
    )


def test_bodycam_title_classifies_tier_a():
    result = classify_media_relevance(artifact("bodycam001", title="Police release BWC bodycam footage"))

    assert result.media_relevance_tier == "A"
    assert result.media_relevance_score >= 0.85
    assert "bodycam" in result.matched_terms
    assert "primary_production_media_terms" in result.reason_codes
    assert result.needs_manual_review is False


def test_dashcam_title_classifies_tier_a():
    result = classify_media_relevance(artifact("dash001", title="Dashcam video from patrol vehicle"))

    assert result.media_relevance_tier == "A"
    assert "dashcam" in result.matched_terms


def test_interrogation_confession_title_classifies_tier_a():
    result = classify_media_relevance(artifact("int001", title="Full interrogation confession video"))

    assert result.media_relevance_tier == "A"
    assert {"interrogation", "confession"} <= set(result.matched_terms)


def test_911_audio_title_classifies_tier_a():
    result = classify_media_relevance(
        artifact(
            "audio001",
            title="911 dispatch audio released",
            artifact_type="dispatch_911",
            fmt="audio",
            url="https://example.gov/audio/dispatch.mp3",
            source_authority="official",
        )
    )

    assert result.media_relevance_tier == "A"
    assert "dispatch_911" in result.matched_terms


def test_official_critical_incident_source_classifies_tier_a():
    art = artifact(
        "ois001",
        title="Critical incident briefing raw footage",
        url="https://www.phoenix.gov/police/critical-incident/ois001.mp4",
        source_authority="official",
        metadata={"channel": "Phoenix Police Department"},
    )

    result = classify_media_relevance(art, source=source())

    assert result.media_relevance_tier == "A"
    assert result.official_source_likelihood >= 0.7
    assert "official_source_likely" in result.reason_codes


def test_sentencing_trial_court_video_classifies_tier_b():
    result = classify_media_relevance(artifact("court001", title="Sentencing court video", artifact_type="court_video"))

    assert result.media_relevance_tier == "B"
    assert "strong_secondary_media_terms" in result.reason_codes
    assert "sentencing_video" in result.matched_terms


def test_local_news_raw_courtroom_footage_classifies_tier_b():
    result = classify_media_relevance(
        artifact(
            "courtnews001",
            title="Local news raw courtroom footage from trial",
            metadata={"channel": "Knoxville Local News"},
        )
    )

    assert result.media_relevance_tier == "B"
    assert "courtroom_footage" in result.matched_terms


def test_documentary_commentary_explainer_classifies_tier_c():
    result = classify_media_relevance(
        artifact("doc001", title="True crime documentary commentary explainer")
    )

    assert result.media_relevance_tier == "C"
    assert result.needs_manual_review is True
    assert "weak_or_uncertain_media" in result.risk_flags
    assert "documentary" in result.matched_terms


def test_bodycam_query_without_confirming_metadata_flags_mismatch():
    result = classify_media_relevance(
        artifact(
            "generic001",
            title="Christa Pike court hearing update",
            metadata={"query_used": "Christa Gail Pike bodycam"},
        )
    )

    assert result.media_relevance_tier in {"C", "unknown"}
    assert result.needs_manual_review is True
    assert "media_query_artifact_type_mismatch" in result.risk_flags
    assert "media_query_artifact_type_mismatch" in result.mismatch_warnings


def test_name_collision_like_result_adds_risk_flag():
    result = classify_media_relevance(
        artifact(
            "collision001",
            title="Different person with same name commentary",
            metadata={"description": "Possible name collision, unrelated case."},
        )
    )

    assert "possible_name_collision" in result.risk_flags
    assert "possible_name_collision" in result.mismatch_warnings


def test_protected_private_url_not_tier_a_or_b():
    result = classify_media_relevance(
        artifact(
            "private001",
            title="Bodycam footage",
            url="https://portal.example.gov/login?redirect=/private/bodycam.mp4",
            source_authority="official",
        )
    )

    assert result.media_relevance_tier not in {"A", "B"}
    assert result.needs_manual_review is True
    assert "protected_or_nonpublic" in result.risk_flags
    assert "protected_or_nonpublic_url_not_production_ready" in result.mismatch_warnings


def test_generic_youtube_watch_url_with_weak_title_needs_manual_review():
    result = classify_media_relevance(artifact("weak001", title="Full story explained"))

    assert result.media_relevance_tier == "C"
    assert result.needs_manual_review is True
    assert "generic_youtube_media" in result.risk_flags


def test_result_is_json_serializable():
    result = classify_media_relevance(artifact("json001", title="Police interview video"))

    encoded = json.dumps(result.to_dict(), sort_keys=True)
    assert json.loads(encoded)["media_relevance_tier"] == "A"


def test_classifier_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    classify_media_relevance(artifact("net001", title="Police bodycam video"))

    assert calls == []
