"""AGENCY1 - official agency/OIS fixture contract matrix."""
from pathlib import Path

from pipeline2_discovery.casegraph import (
    AgencyOISConnector,
    classify_media_relevance,
    extract_artifact_claims,
    resolve_agency_ois_files,
)
from pipeline2_discovery.casegraph.models import (
    CaseIdentity,
    CaseInput,
    CasePacket,
    Jurisdiction,
    Scores,
    SourceRecord,
    VerifiedArtifact,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "agency_ois"


CONTRACTS = {
    "incident_detail_with_youtube_embed.json": {
        "portal_profile_id": "agency_ois_detail",
        "candidate_urls": ["https://www.youtube.com/watch?v=officialBWC050"],
        "rejected_urls": [],
        "claim_count": 2,
        "verified_after_resolver": 1,
        "tiers": ["A"],
    },
    "incident_detail_with_bodycam_video.json": {
        "portal_profile_id": "agency_ois_detail",
        "candidate_urls": ["https://www.phoenix.gov/police/media/2024-OIS-014-briefing.mp4"],
        "rejected_urls": [],
        "claim_count": 1,
        "verified_after_resolver": 1,
        "tiers": ["A"],
    },
    "incident_detail_with_vimeo_link.json": {
        "portal_profile_id": "agency_ois_detail",
        "candidate_urls": ["https://vimeo.com/123456789"],
        "rejected_urls": [],
        "claim_count": 2,
        "verified_after_resolver": 1,
        "tiers": ["A"],
    },
    "incident_detail_with_911_audio.json": {
        "portal_profile_id": "agency_ois_detail",
        "candidate_urls": ["https://www.phoenix.gov/police/media/2024-OIS-052-911-audio.mp3"],
        "rejected_urls": [],
        "claim_count": 2,
        "verified_after_resolver": 1,
        "tiers": ["A"],
    },
    "incident_detail_with_pdf.json": {
        "portal_profile_id": "agency_ois_detail",
        "candidate_urls": ["https://www.phoenix.gov/police/docs/2024-OIS-022-IA-report.pdf"],
        "rejected_urls": [],
        "claim_count": 0,
        "verified_after_resolver": 1,
        "tiers": [],
    },
    "incident_detail_with_bodycam_claim_no_url.json": {
        "portal_profile_id": "agency_ois_detail",
        "candidate_urls": [],
        "rejected_urls": [],
        "claim_count": 2,
        "verified_after_resolver": 0,
        "tiers": [],
    },
    "incident_detail_with_protected_link.json": {
        "portal_profile_id": "agency_ois_detail",
        "candidate_urls": [
            "https://portal.phoenix.gov/login?redirect=/oa/2024-OIS-040.mp4",
            "https://www.phoenix.gov/police/docs/2024-OIS-040-public-summary.pdf",
        ],
        "rejected_urls": ["https://portal.phoenix.gov/login?redirect=/oa/2024-OIS-040.mp4"],
        "claim_count": 1,
        "verified_after_resolver": 1,
        "tiers": [],
    },
}


def _case_input():
    return CaseInput(
        input_type="manual",
        raw_input={"defendant_names": "Agency OIS Example"},
        known_fields={"defendant_names": ["Agency OIS Example"]},
    )


def _packet(records):
    return CasePacket(
        case_id="agency_ois_contract_matrix",
        input=_case_input(),
        case_identity=CaseIdentity(
            defendant_names=["Agency OIS Example"],
            agency="Phoenix Police Department",
            jurisdiction=Jurisdiction(city="Phoenix", state="AZ"),
            outcome_status="closed",
            identity_confidence="high",
            identity_anchors=["full_name", "agency", "jurisdiction"],
        ),
        sources=list(records),
        artifact_claims=[],
        verified_artifacts=[],
        scores=Scores(),
        verdict="HOLD",
        next_actions=[],
        risk_flags=[],
    )


def _records_for(filename):
    conn = AgencyOISConnector([FIXTURE_DIR / filename])
    return list(conn.fetch(_case_input()))


def _candidate_urls(records):
    return [
        record.url for record in records
        if "possible_artifact_source" in record.source_roles
    ]


def _rejected_urls(records):
    return [
        record.url for record in records
        if "protected_or_nonpublic" in (record.metadata or {}).get("risk_flags", [])
    ]


def test_agency_ois_fixture_contract_matrix():
    for filename, expected in CONTRACTS.items():
        records = _records_for(filename)
        packet = _packet(records)
        claims = extract_artifact_claims(packet).artifact_claims

        assert records
        assert expected["portal_profile_id"] == "agency_ois_detail"
        assert _candidate_urls(records) == expected["candidate_urls"]
        assert _rejected_urls(records) == expected["rejected_urls"]
        assert len(claims) == expected["claim_count"]
        assert packet.verified_artifacts == []

        resolution = resolve_agency_ois_files(packet)
        assert len(resolution.verified_artifacts) == expected["verified_after_resolver"]

        source_map = {record.source_id: record for record in records}
        media_artifacts = [
            artifact for artifact in resolution.verified_artifacts
            if artifact.format in {"video", "audio"}
        ]
        tiers = [
            classify_media_relevance(
                artifact,
                source=source_map.get((artifact.metadata or {}).get("source_id")),
            ).media_relevance_tier
            for artifact in media_artifacts
        ]
        assert tiers == expected["tiers"]


def test_claim_only_fixture_never_creates_verified_artifact():
    records = _records_for("incident_detail_with_bodycam_claim_no_url.json")
    packet = _packet(records)

    claims = extract_artifact_claims(packet).artifact_claims
    resolution = resolve_agency_ois_files(packet)

    assert len(claims) == 2
    assert any(claim.claim_label == "artifact_released" for claim in claims)
    assert resolution.verified_artifacts == []
    assert packet.verified_artifacts == []


def test_protected_private_link_is_rejected_and_not_verified():
    records = _records_for("incident_detail_with_protected_link.json")
    packet = _packet(records)
    resolution = resolve_agency_ois_files(packet)

    assert _rejected_urls(records) == [
        "https://portal.phoenix.gov/login?redirect=/oa/2024-OIS-040.mp4"
    ]
    assert "protected_or_nonpublic" in resolution.risk_flags
    assert all("login" not in artifact.artifact_url for artifact in resolution.verified_artifacts)


def test_generic_youtube_link_is_not_tier_a_without_primary_metadata():
    artifact = VerifiedArtifact(
        artifact_id="generic_youtube",
        artifact_type="video_footage",
        artifact_url="https://www.youtube.com/watch?v=generic123",
        source_url="https://www.example.gov/news/community-update",
        source_authority="media",
        downloadable=False,
        format="video",
        matched_case_fields=[],
        confidence=0.7,
        claim_source_url="https://www.youtube.com/watch?v=generic123",
        verification_method="agency_ois_video_host",
        risk_flags=[],
        metadata={},
    )
    source = SourceRecord(
        source_id="youtube::media::generic",
        url=artifact.artifact_url,
        title="Community update video",
        snippet="A public information video.",
        source_type="video_host",
        source_roles=["possible_artifact_source"],
        source_authority="media",
        api_name="youtube",
        metadata={"media_link_type": "video"},
    )

    relevance = classify_media_relevance(artifact, source=source)

    assert relevance.media_relevance_tier != "A"
    assert "weak_or_uncertain_media" in relevance.risk_flags
    assert relevance.needs_manual_review is True


def test_agency_ois_contract_matrix_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    for filename in CONTRACTS:
        packet = _packet(_records_for(filename))
        extract_artifact_claims(packet)
        resolve_agency_ois_files(packet)

    assert calls == []
