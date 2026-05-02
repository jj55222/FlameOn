"""LIVE9 helper - metadata-only YouTube media resolver tests."""
import json

from pipeline2_discovery.casegraph import (
    CaseIdentity,
    CaseInput,
    CasePacket,
    Jurisdiction,
    SourceRecord,
    classify_media_relevance,
    resolve_youtube_media_sources,
    run_metadata_only_resolvers,
    score_case_packet,
)


def _packet():
    return CasePacket(
        case_id="youtube_resolver_alan_champagne",
        input=CaseInput(
            input_type="manual",
            raw_input={"defendant_names": "Alan Matthew Champagne"},
            known_fields={"defendant_names": ["Alan Matthew Champagne"]},
        ),
        case_identity=CaseIdentity(
            defendant_names=["Alan Matthew Champagne"],
            jurisdiction=Jurisdiction(county="Maricopa", state="AZ"),
            charges=["murder"],
            outcome_status="convicted",
            identity_confidence="high",
            identity_anchors=["full_name", "jurisdiction", "court_opinion_url"],
        ),
    )


def _youtube_source(
    *,
    title="Alan Champagne Police Interrogation Full Video",
    snippet="Police interrogation video in the Alan Champagne case.",
    url="https://www.youtube.com/watch?v=abc123",
    roles=None,
    channel="Court Records Channel",
):
    return SourceRecord(
        source_id="youtube_abc123",
        url=url,
        title=title,
        snippet=snippet,
        raw_text=snippet,
        source_type="video",
        source_authority="third_party",
        source_roles=list(roles or ["possible_artifact_source", "claim_source"]),
        api_name="youtube_yt_dlp",
        discovered_via="Alan Matthew Champagne bodycam",
        retrieved_at="2026-05-01T00:00:00Z",
        case_input_id="youtube_resolver_alan_champagne",
        metadata={"video_id": "abc123", "channel": channel, "duration": 600},
        cost_estimate=0.0,
        confidence_signals={"youtube_watch_url": True},
        matched_case_fields=[],
    )


def test_youtube_interrogation_source_becomes_tier_a_media_artifact():
    packet = _packet()
    packet.sources.append(_youtube_source())

    resolution = resolve_youtube_media_sources(packet)
    result = score_case_packet(packet)
    relevance = classify_media_relevance(packet.verified_artifacts[0])

    assert resolution.verified_artifacts
    artifact = packet.verified_artifacts[0]
    assert artifact.artifact_type == "interrogation"
    assert artifact.format == "video"
    assert artifact.artifact_url == "https://www.youtube.com/watch?v=abc123"
    assert artifact.verification_method == "youtube_metadata_public_watch_url"
    assert "defendant_full_name" in artifact.matched_case_fields
    assert relevance.media_relevance_tier == "A"
    assert result.verdict == "PRODUCE"
    assert "produce_based_on_weak_or_uncertain_media" not in result.risk_flags


def test_generic_youtube_source_graduates_as_other_video_with_review_flags():
    packet = _packet()
    packet.sources.append(
        _youtube_source(
            title="Alan Champagne case explained",
            snippet="A general overview of the Alan Champagne case.",
            channel="True Crime Explainers",
        )
    )

    resolve_youtube_media_sources(packet)
    artifact = packet.verified_artifacts[0]
    relevance = classify_media_relevance(artifact)

    assert artifact.artifact_type == "other_video"
    assert "generic_youtube_media" in artifact.risk_flags
    assert relevance.media_relevance_tier == "C"
    assert relevance.needs_manual_review is True


def test_youtube_claim_only_source_never_verifies_artifact():
    packet = _packet()
    packet.sources.append(_youtube_source(roles=["claim_source"]))

    resolution = resolve_youtube_media_sources(packet)

    assert resolution.verified_artifacts == []
    assert packet.verified_artifacts == []


def test_youtube_protected_or_token_url_is_rejected():
    packet = _packet()
    packet.sources.append(
        _youtube_source(url="https://www.youtube.com/watch?v=abc123&token=secret")
    )

    resolution = resolve_youtube_media_sources(packet)

    assert resolution.verified_artifacts == []
    assert "protected_or_nonpublic" in resolution.risk_flags
    assert "protected_or_nonpublic" in packet.risk_flags


def test_youtube_resolver_participates_in_orchestrator_allow_list():
    packet = _packet()
    packet.sources.append(_youtube_source())

    orchestration = run_metadata_only_resolvers(packet, allow_list=["youtube"])

    assert orchestration.resolvers_run == ["youtube"]
    assert orchestration.verified_artifact_count == 1
    assert orchestration.media_artifact_count == 1


def test_youtube_media_resolution_is_json_serializable():
    packet = _packet()
    packet.sources.append(_youtube_source())

    resolution = resolve_youtube_media_sources(packet)
    payload = {
        "artifacts": [artifact.__dict__ for artifact in resolution.verified_artifacts],
        "risk_flags": resolution.risk_flags,
        "next_actions": resolution.next_actions,
    }

    json.dumps(payload)


def test_youtube_media_resolver_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    packet = _packet()
    packet.sources.append(_youtube_source())
    resolve_youtube_media_sources(packet)

    assert calls == []
