import json
from pathlib import Path

from jsonschema import Draft7Validator

from pipeline2_discovery.casegraph import (
    SourceRecord,
    resolve_muckrock_released_files,
    route_manual_defendant_jurisdiction,
)


ROOT = Path(__file__).resolve().parents[1]
CASE_PACKET_SCHEMA = ROOT / "schemas" / "p2_case_packet.schema.json"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def assert_valid_packet(packet):
    validator = Draft7Validator(load_json(CASE_PACKET_SCHEMA))
    errors = sorted(validator.iter_errors(packet.to_dict()), key=lambda e: list(e.path))
    assert errors == [], "\n".join(error.message for error in errors)


def muckrock_packet(*, title="Fixture MuckRock request", text="", metadata=None, matched=None):
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    packet.sources.append(SourceRecord(
        source_id="muckrock_fixture",
        url="https://www.muckrock.com/foi/california-52/request-for-bodycam-101/",
        title=title,
        snippet=text,
        raw_text=text,
        source_type="foia_request",
        source_roles=["claim_source"],
        source_authority="foia",
        api_name="muckrock",
        discovered_via="bodycam",
        retrieved_at="2026-04-30T00:00:00Z",
        case_input_id="fixture",
        metadata=metadata or {},
        cost_estimate=0.0,
        confidence_signals={},
        matched_case_fields=matched or [],
    ))
    return packet


def test_request_only_source_creates_no_verified_artifact():
    packet = muckrock_packet(
        title="Request for bodycam footage",
        text="Request seeking bodycam footage with no public file links.",
    )

    result = resolve_muckrock_released_files(packet)

    assert result.verified_artifacts == []
    assert packet.verified_artifacts == []
    assert_valid_packet(packet)


def test_public_pdf_link_creates_document_artifact():
    packet = muckrock_packet(
        title="Complaint and affidavit released",
        metadata={"files": [{"url": "https://www.muckrock.com/foi/example/files/complaint.pdf", "filename": "complaint.pdf"}]},
    )

    result = resolve_muckrock_released_files(packet)

    assert len(result.verified_artifacts) == 1
    artifact = packet.verified_artifacts[0]
    assert artifact.artifact_url.endswith("complaint.pdf")
    assert artifact.format == "pdf"
    assert artifact.artifact_type in {"docket_docs", "document"}
    assert artifact.source_authority == "foia"
    assert artifact.verification_method == "file_extension"
    assert_valid_packet(packet)


def test_public_mp4_link_creates_bodycam_video_artifact():
    packet = muckrock_packet(
        title="Bodycam files released",
        text="Body-worn camera video released.",
        metadata={"files": [{"url": "https://www.muckrock.com/foi/example/files/bodycam_redacted.mp4"}]},
    )

    resolve_muckrock_released_files(packet)

    assert len(packet.verified_artifacts) == 1
    artifact = packet.verified_artifacts[0]
    assert artifact.format == "video"
    assert artifact.artifact_type == "bodycam"
    assert artifact.downloadable is True
    assert artifact.source_url == packet.sources[0].url
    assert_valid_packet(packet)


def test_released_text_without_link_stays_claim_only():
    packet = muckrock_packet(
        title="Records produced for request",
        text="Responsive records were produced, but the metadata has no public file URL.",
    )

    result = resolve_muckrock_released_files(packet)

    assert result.verified_artifacts == []
    assert packet.verified_artifacts == []
    assert result.next_actions
    assert packet.next_actions
    assert_valid_packet(packet)


def test_protected_or_private_link_is_rejected():
    packet = muckrock_packet(
        title="Bodycam file unavailable",
        metadata={"files": [{"url": "https://www.muckrock.com/login/files/bodycam.mp4"}]},
    )

    result = resolve_muckrock_released_files(packet)

    assert result.verified_artifacts == []
    assert packet.verified_artifacts == []
    assert "protected_or_nonpublic" in result.risk_flags
    assert "protected_or_nonpublic" in packet.risk_flags
    assert_valid_packet(packet)


def test_identity_match_without_file_url_does_not_create_artifact():
    packet = muckrock_packet(
        title="Min Jian Guan bodycam request",
        text="Min Jian Guan San Francisco responsive records were produced.",
        matched=["defendant_full_name", "city"],
    )

    resolve_muckrock_released_files(packet)

    assert packet.verified_artifacts == []
    assert packet.verdict == "HOLD"
    assert packet.case_identity.identity_confidence == "low"
    assert_valid_packet(packet)


def test_multiple_public_files_create_typed_artifacts():
    packet = muckrock_packet(
        title="Released records package",
        metadata={
            "files": [
                {"url": "https://www.muckrock.com/foi/example/files/complaint.pdf", "filename": "complaint.pdf"},
                {"url": "https://www.muckrock.com/foi/example/files/bodycam_redacted.mp4", "filename": "bodycam_redacted.mp4"},
                {"url": "https://www.muckrock.com/foi/example/files/911_call.mp3", "filename": "911_call.mp3"},
            ]
        },
        matched=["defendant_full_name"],
    )

    result = resolve_muckrock_released_files(packet)

    assert len(result.verified_artifacts) == 3
    by_url = {artifact.artifact_url: artifact for artifact in packet.verified_artifacts}
    assert by_url["https://www.muckrock.com/foi/example/files/complaint.pdf"].format == "pdf"
    assert by_url["https://www.muckrock.com/foi/example/files/bodycam_redacted.mp4"].artifact_type == "bodycam"
    assert by_url["https://www.muckrock.com/foi/example/files/911_call.mp3"].artifact_type == "dispatch_911"
    assert all(artifact.downloadable for artifact in packet.verified_artifacts)
    assert packet.verdict == "HOLD"
    assert_valid_packet(packet)
