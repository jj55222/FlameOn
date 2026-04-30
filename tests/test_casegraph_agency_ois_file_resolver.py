"""SOURCE2 — Agency OIS media/document resolver tests.

Asserts that ``resolve_agency_ois_files``:

- graduates a public ``.mp4`` agency-OIS SourceRecord into a media
  VerifiedArtifact with format=video and an artifact_type derived
  from the agency's link-type hint (bodycam_briefing -> bodycam)
- graduates a public ``.pdf`` agency-OIS SourceRecord into a
  document VerifiedArtifact with format=pdf and
  artifact_type=docket_docs
- recognizes YouTube watch / youtu.be URLs as media candidates with
  format=video and artifact_type=video_footage when no link hint is
  present
- recognizes ``.mp3``/.wav/.m4a as audio media (artifact_type
  defaulting to dispatch_911)
- skips claim-only sources (``claim_source`` without
  ``possible_artifact_source``) - claim text without a URL never
  graduates (the non-negotiable claim_source != artifact_source rule)
- refuses login/auth/private/portal URLs and surfaces
  ``protected_or_nonpublic`` in the resolution's risk_flags
- refuses URLs without a recognized media or document extension /
  video host - they remain candidates with no graduation
- ignores SourceRecords from non-agency-OIS connectors entirely
- works against a bare list of SourceRecords (no packet)
- attaches new VerifiedArtifacts to the supplied CasePacket and
  appends risk_flags / next_actions onto the packet
- is idempotent: re-running on the same packet does not duplicate
  artifacts
- never graduates a SourceRecord that doesn't carry the
  ``possible_artifact_source`` role
- end-to-end via the existing
  ``run_metadata_only_resolvers`` orchestrator does not regress its
  own behaviour (the agency-OIS resolver is additive)
- never makes a network call
"""
from __future__ import annotations

from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    AgencyOISConnector,
    resolve_agency_ois_files,
)
from pipeline2_discovery.casegraph.models import (
    CaseIdentity,
    CaseInput,
    CasePacket,
    Jurisdiction,
    Scores,
    SourceRecord,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "agency_ois"


def _make_input():
    return CaseInput(
        input_type="manual",
        raw_input={"defendant_names": "John Example"},
        known_fields={"defendant_names": ["John Example"]},
    )


def _make_packet(sources):
    return CasePacket(
        case_id="agency_ois_test",
        input=_make_input(),
        case_identity=CaseIdentity(
            defendant_names=["John Example"],
            agency="Phoenix Police Department",
            jurisdiction=Jurisdiction(city="Phoenix", state="AZ"),
            outcome_status="charged",
            identity_confidence="high",
            identity_anchors=["full_name", "agency", "jurisdiction"],
        ),
        sources=list(sources),
        artifact_claims=[],
        verified_artifacts=[],
        scores=Scores(),
        verdict="HOLD",
        next_actions=[],
        risk_flags=[],
    )


def _connector_records(*fixture_filenames):
    paths = [FIXTURE_DIR / name for name in fixture_filenames]
    conn = AgencyOISConnector(paths)
    return list(conn.fetch(_make_input()))


# ---- Public media ------------------------------------------------------


def test_public_mp4_graduates_to_media_artifact_with_link_type_hint():
    records = _connector_records("incident_detail_with_bodycam_video.json")
    result = resolve_agency_ois_files(records)
    media = [a for a in result.verified_artifacts if a.format == "video"]
    assert len(media) == 1
    art = media[0]
    assert art.artifact_url.endswith(".mp4")
    assert art.format == "video"
    assert art.artifact_type == "bodycam"
    assert art.source_authority == "official"
    assert art.metadata["media_or_document"] == "media"


def test_public_pdf_graduates_to_document_artifact():
    records = _connector_records("incident_detail_with_pdf.json")
    result = resolve_agency_ois_files(records)
    docs = [a for a in result.verified_artifacts if a.format == "pdf"]
    assert len(docs) == 1
    art = docs[0]
    assert art.artifact_url.endswith(".pdf")
    assert art.artifact_type == "docket_docs"
    assert art.format == "pdf"
    assert art.metadata["media_or_document"] == "document"


def test_youtube_watch_url_graduates_to_video_footage_artifact():
    """YouTube watch URLs should be recognized as media candidates
    when supplied via an agency_ois SourceRecord with role
    possible_artifact_source. artifact_type defaults to video_footage
    when no link_type hint is present."""
    src = SourceRecord(
        source_id="agency_ois::media::yt::1",
        url="https://www.youtube.com/watch?v=abc12345",
        title="Agency-released YouTube video",
        snippet="",
        source_type="agency_media:bodycam_briefing",
        source_roles=["possible_artifact_source"],
        source_authority="official",
        api_name="agency_ois",
        metadata={"agency": "Phoenix PD", "media_link_type": ""},
    )
    result = resolve_agency_ois_files([src])
    assert len(result.verified_artifacts) == 1
    art = result.verified_artifacts[0]
    assert art.format == "video"
    assert art.artifact_type == "video_footage"


def test_mp3_url_graduates_to_audio_dispatch_artifact():
    src = SourceRecord(
        source_id="agency_ois::media::audio::1",
        url="https://www.phoenix.gov/police/media/dispatch-2024-OIS-014.mp3",
        title="911 dispatch audio",
        snippet="",
        source_type="agency_media:dispatch_911",
        source_roles=["possible_artifact_source"],
        source_authority="official",
        api_name="agency_ois",
        metadata={"agency": "Phoenix PD", "media_link_type": "dispatch_911"},
    )
    result = resolve_agency_ois_files([src])
    assert len(result.verified_artifacts) == 1
    art = result.verified_artifacts[0]
    assert art.format == "audio"
    assert art.artifact_type == "dispatch_911"


# ---- Claim-only / page records never graduate -------------------------


def test_claim_only_source_does_not_graduate_to_verified_artifact():
    """The non-negotiable rule: claim text without a URL never
    becomes a VerifiedArtifact. The connector emits these with role
    'claim_source' only - the resolver must skip them."""
    records = _connector_records(
        "incident_detail_with_bodycam_claim_no_url.json"
    )
    result = resolve_agency_ois_files(records)
    assert result.verified_artifacts == []


def test_page_only_record_does_not_graduate():
    """The agency-listing page itself (no per-incident detail) emits
    a SourceRecord with no roles. It must never become an artifact."""
    records = _connector_records("agency_listing.json")
    result = resolve_agency_ois_files(records)
    assert result.verified_artifacts == []


def test_page_with_subjects_but_no_links_does_not_graduate():
    """The detail-page record (identity_source / outcome_source roles
    but NOT possible_artifact_source) should never graduate - only
    explicit link records can."""
    records = _connector_records(
        "incident_detail_with_bodycam_claim_no_url.json"
    )
    page = next(
        r for r in records if r.metadata.get("fixture_kind") == "agency_page"
    )
    assert "possible_artifact_source" not in page.source_roles
    result = resolve_agency_ois_files([page])
    assert result.verified_artifacts == []


# ---- Protected URLs -----------------------------------------------------


def test_protected_login_url_does_not_graduate_and_flags_risk():
    records = _connector_records(
        "incident_detail_with_protected_link.json"
    )
    result = resolve_agency_ois_files(records)
    # The protected media link must NOT graduate. The public PDF
    # alongside it MUST graduate (resolver picks the one it can verify).
    assert len(result.verified_artifacts) == 1
    art = result.verified_artifacts[0]
    assert art.format == "pdf"
    assert "protected_or_nonpublic" in result.risk_flags


def test_resolver_makes_no_artifact_for_protected_link_when_isolated():
    src = SourceRecord(
        source_id="agency_ois::media::protected::1",
        url="https://portal.example.gov/login?redirect=/oa/2024-001.mp4",
        title="Protected video",
        snippet="login required",
        source_type="agency_media:bodycam_briefing",
        source_roles=["possible_artifact_source"],
        source_authority="official",
        api_name="agency_ois",
        metadata={
            "agency": "Example PD",
            "media_link_type": "bodycam_briefing",
            "risk_flags": ["protected_or_nonpublic"],
        },
    )
    result = resolve_agency_ois_files([src])
    assert result.verified_artifacts == []
    assert "protected_or_nonpublic" in result.risk_flags


# ---- Cross-connector isolation -----------------------------------------


def test_resolver_ignores_non_agency_ois_sources():
    other = SourceRecord(
        source_id="muckrock::1",
        url="https://www.muckrock.com/media/agency/foo.mp4",
        title="Non-agency-OIS source",
        snippet="",
        source_type="foia_request",
        source_roles=["possible_artifact_source"],
        source_authority="muckrock",
        api_name="muckrock",
        metadata={},
    )
    result = resolve_agency_ois_files([other])
    assert result.verified_artifacts == []
    assert other.source_id not in result.inspected_source_ids


# ---- Packet integration -------------------------------------------------


def test_resolver_attaches_artifacts_to_packet():
    records = _connector_records(
        "incident_detail_with_bodycam_video.json"
    )
    packet = _make_packet(records)
    assert packet.verified_artifacts == []
    result = resolve_agency_ois_files(packet)
    assert len(result.verified_artifacts) == 1
    assert len(packet.verified_artifacts) == 1
    assert packet.verified_artifacts[0].artifact_url.endswith(".mp4")


def test_resolver_is_idempotent_on_same_packet():
    records = _connector_records(
        "incident_detail_with_bodycam_video.json"
    )
    packet = _make_packet(records)
    resolve_agency_ois_files(packet)
    first = list(packet.verified_artifacts)
    resolve_agency_ois_files(packet)
    second = list(packet.verified_artifacts)
    # Same URL should not graduate twice.
    assert {a.artifact_url for a in first} == {a.artifact_url for a in second}
    assert len(first) == len(second)


# ---- Bare-source-list mode -----------------------------------------------


def test_resolver_works_against_bare_source_list():
    records = _connector_records(
        "incident_detail_with_bodycam_video.json",
        "incident_detail_with_pdf.json",
    )
    result = resolve_agency_ois_files(records)
    artifact_urls = {a.artifact_url for a in result.verified_artifacts}
    assert any(u.endswith(".mp4") for u in artifact_urls)
    assert any(u.endswith(".pdf") for u in artifact_urls)


# ---- Orchestrator co-existence ------------------------------------------


def test_orchestrator_no_regression_against_agency_sources():
    """The existing run_metadata_only_resolvers orchestrator only runs
    muckrock/documentcloud/courtlistener resolvers; agency-OIS sources
    should pass through it without producing artifacts (since none of
    those resolvers match) and the orchestrator must not raise."""
    from pipeline2_discovery.casegraph import run_metadata_only_resolvers

    records = _connector_records(
        "incident_detail_with_bodycam_video.json"
    )
    out = run_metadata_only_resolvers(records)
    # Existing orchestrator doesn't know about agency_ois yet, so no
    # artifacts; the test just guards against a regression / crash.
    assert out.verified_artifacts == []


# ---- No network ---------------------------------------------------------


def test_resolver_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    records = _connector_records(
        "incident_detail_with_bodycam_video.json",
        "incident_detail_with_pdf.json",
        "incident_detail_with_bodycam_claim_no_url.json",
        "incident_detail_with_protected_link.json",
        "agency_listing.json",
    )
    resolve_agency_ois_files(records)
    assert calls == [], f"agency-OIS resolver made {len(calls)} live HTTP call(s)"


# ---- Score gate sanity --------------------------------------------------


def test_media_artifact_with_high_identity_and_concluded_outcome_is_PRODUCE():
    """End-to-end gate sanity: when identity is high, outcome is
    concluded, and a media VerifiedArtifact has been graduated, the
    scorer should return PRODUCE. This guards the gate path through
    the new resolver."""
    from pipeline2_discovery.casegraph import score_case_packet

    records = _connector_records(
        "incident_detail_with_bodycam_video.json"
    )
    packet = _make_packet(records)
    packet.case_identity.outcome_status = "sentenced"
    resolve_agency_ois_files(packet)
    result = score_case_packet(packet)
    assert any(a.format == "video" for a in packet.verified_artifacts)
    assert result.verdict == "PRODUCE"


def test_document_only_agency_ois_packet_remains_HOLD():
    """Document-only verification must NEVER produce, even with high
    identity + concluded outcome. The media gate is non-negotiable."""
    from pipeline2_discovery.casegraph import score_case_packet

    records = _connector_records("incident_detail_with_pdf.json")
    packet = _make_packet(records)
    packet.case_identity.outcome_status = "closed"
    resolve_agency_ois_files(packet)
    result = score_case_packet(packet)
    # At least one document artifact, no media.
    assert any(a.format == "pdf" for a in packet.verified_artifacts)
    assert all(a.format != "video" for a in packet.verified_artifacts)
    assert result.verdict != "PRODUCE"
