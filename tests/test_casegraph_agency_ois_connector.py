"""SOURCE1 — Agency OIS connector tests.

Asserts that ``AgencyOISConnector``:

- emits SourceRecords from fixture-shaped agency OIS pages without
  any network call
- always sets source_authority='official' on every emitted record
- assigns page-level roles deterministically
  (identity_source / outcome_source / claim_source per fixture
  content; never possible_artifact_source on the page itself)
- emits one SourceRecord per public media link, with role
  ``possible_artifact_source`` and metadata pointing back at the host
  page
- emits one SourceRecord per public document link, with role
  ``possible_artifact_source``
- emits one SourceRecord per claim with role ``claim_source`` ONLY -
  NEVER ``possible_artifact_source`` (the non-negotiable
  ``claim_source != artifact_source`` rule)
- flags protected/login/private/auth URLs with
  ``protected_or_nonpublic`` in metadata.risk_flags so the resolver
  can refuse them deterministically without ever contacting the
  network
- creates NO VerifiedArtifact at any point (artifact graduation is
  the resolver's job, SOURCE2)
- handles a listing-only fixture with no per-incident details (no
  subjects, no outcome, no claims) - emits the page record with no
  roles
- supports both pre-loaded dicts AND file paths via the constructor
- supports loading every JSON file in a directory via
  ``from_directory``
- is exposed at the package surface via the ``AgencyOISConnector``
  re-export
- never makes a network call across the full fixture set
"""
from __future__ import annotations

from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import AgencyOISConnector
from pipeline2_discovery.casegraph.models import CaseInput


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "agency_ois"


def _make_input(**overrides):
    base = {
        "input_type": "manual",
        "raw_input": {"defendant_names": "John Example"},
        "known_fields": {"defendant_names": ["John Example"]},
    }
    base.update(overrides)
    return CaseInput(**base)


# ---- Construction / loading ----------------------------------------------


def test_connector_loads_from_directory():
    conn = AgencyOISConnector.from_directory(FIXTURE_DIR)
    records = list(conn.fetch(_make_input()))
    assert len(records) >= 5  # 5 fixtures, each emits >=1 record


def test_connector_accepts_explicit_dict_fixtures():
    fixture = {
        "page_type": "incident_detail",
        "agency": "Example Police Department",
        "url": "https://example.gov/police/incidents/X-1",
        "title": "Incident X-1",
        "narrative": "Subject was charged.",
        "subjects": ["Foo Bar"],
        "incident_date": "2024-01-01",
        "case_number": "X-1",
        "outcome_text": "charged",
        "media_links": [],
        "document_links": [],
        "claims": [],
    }
    conn = AgencyOISConnector([fixture])
    records = list(conn.fetch(_make_input()))
    assert len(records) == 1


def test_connector_from_directory_raises_on_missing_dir(tmp_path):
    missing = tmp_path / "does_not_exist"
    with pytest.raises(FileNotFoundError):
        AgencyOISConnector.from_directory(missing)


# ---- Source authority + role discipline ----------------------------------


def test_every_record_has_source_authority_official():
    conn = AgencyOISConnector.from_directory(FIXTURE_DIR)
    records = list(conn.fetch(_make_input()))
    assert records, "expected at least one record"
    for r in records:
        assert r.source_authority == "official", (
            f"{r.source_id} has authority {r.source_authority!r}"
        )


def test_every_record_has_api_name_agency_ois():
    conn = AgencyOISConnector.from_directory(FIXTURE_DIR)
    for r in conn.fetch(_make_input()):
        assert r.api_name == "agency_ois"


def test_page_record_with_subjects_gets_identity_source_role():
    conn = AgencyOISConnector(
        [FIXTURE_DIR / "incident_detail_with_bodycam_video.json"]
    )
    records = list(conn.fetch(_make_input()))
    page = next(r for r in records if r.metadata.get("fixture_kind") == "agency_page")
    assert "identity_source" in page.source_roles
    assert "outcome_source" in page.source_roles


def test_listing_only_fixture_has_no_subjects_or_outcome_roles():
    """An agency listing index without per-incident details should emit
    a page record with NO roles (no identity_source / outcome_source /
    claim_source) since it carries no per-case content."""
    conn = AgencyOISConnector([FIXTURE_DIR / "agency_listing.json"])
    records = list(conn.fetch(_make_input()))
    assert len(records) == 1
    assert records[0].source_roles == []
    assert records[0].source_authority == "official"


def test_page_record_never_gets_possible_artifact_source():
    """The page itself is never marked as a possible_artifact_source.
    Artifact-source role lives on per-link records."""
    conn = AgencyOISConnector.from_directory(FIXTURE_DIR)
    for r in conn.fetch(_make_input()):
        if r.metadata.get("fixture_kind") == "agency_page":
            assert "possible_artifact_source" not in r.source_roles, (
                f"page {r.source_id} got artifact role: {r.source_roles}"
            )


# ---- Per-link emission ---------------------------------------------------


def test_public_media_link_becomes_possible_artifact_source_record():
    conn = AgencyOISConnector(
        [FIXTURE_DIR / "incident_detail_with_bodycam_video.json"]
    )
    records = list(conn.fetch(_make_input()))
    media = [r for r in records if r.metadata.get("fixture_kind") == "agency_media_link"]
    assert len(media) == 1
    rec = media[0]
    assert rec.source_roles == ["possible_artifact_source"]
    assert rec.source_authority == "official"
    assert rec.url.endswith(".mp4")
    assert rec.metadata["host_page_url"].endswith("2024-OIS-014")


def test_public_document_link_becomes_possible_artifact_source_record():
    conn = AgencyOISConnector(
        [FIXTURE_DIR / "incident_detail_with_pdf.json"]
    )
    records = list(conn.fetch(_make_input()))
    docs = [r for r in records if r.metadata.get("fixture_kind") == "agency_document_link"]
    assert len(docs) == 1
    rec = docs[0]
    assert rec.source_roles == ["possible_artifact_source"]
    assert rec.url.endswith(".pdf")


def test_protected_url_carries_risk_flag_in_metadata():
    """A login-walled or auth-token URL stays in the SourceRecord but
    carries 'protected_or_nonpublic' in metadata.risk_flags so the
    resolver can refuse it deterministically without contacting the
    network."""
    conn = AgencyOISConnector(
        [FIXTURE_DIR / "incident_detail_with_protected_link.json"]
    )
    records = list(conn.fetch(_make_input()))
    media = [r for r in records if r.metadata.get("fixture_kind") == "agency_media_link"]
    assert len(media) == 1
    assert "protected_or_nonpublic" in media[0].metadata["risk_flags"]
    # Document link in the same fixture is public; it should NOT be
    # flagged.
    docs = [r for r in records if r.metadata.get("fixture_kind") == "agency_document_link"]
    assert len(docs) == 1
    assert "protected_or_nonpublic" not in docs[0].metadata["risk_flags"]


# ---- Claim-only emission (the gate-critical test) ------------------------


def test_claim_with_no_url_becomes_claim_source_only_NOT_artifact_source():
    """Critical CaseGraph rule: claim text without a URL must produce
    a SourceRecord with role 'claim_source' ONLY. It must NEVER carry
    'possible_artifact_source' (or 'artifact_source')."""
    conn = AgencyOISConnector(
        [FIXTURE_DIR / "incident_detail_with_bodycam_claim_no_url.json"]
    )
    records = list(conn.fetch(_make_input()))
    claim_records = [r for r in records if r.metadata.get("fixture_kind") == "agency_claim"]
    assert len(claim_records) == 1
    claim = claim_records[0]
    assert claim.source_roles == ["claim_source"], (
        f"claim record got roles {claim.source_roles!r} - violates "
        "claim_source != artifact_source"
    )
    assert "possible_artifact_source" not in claim.source_roles
    assert "artifact_source" not in claim.source_roles


def test_claim_only_fixture_has_no_artifact_source_records_at_all():
    """A fixture with claims but no media_links / document_links must
    produce no possible_artifact_source records - the page itself
    cannot stand in for the missing URL."""
    conn = AgencyOISConnector(
        [FIXTURE_DIR / "incident_detail_with_bodycam_claim_no_url.json"]
    )
    records = list(conn.fetch(_make_input()))
    artifact_likes = [
        r for r in records if "possible_artifact_source" in r.source_roles
    ]
    assert artifact_likes == [], (
        "claim-only fixture leaked artifact-source records: "
        + ", ".join(r.source_id for r in artifact_likes)
    )


# ---- Connector creates no VerifiedArtifact -------------------------------


def test_connector_does_not_emit_verified_artifacts():
    """The connector layer never creates VerifiedArtifact - its only
    job is SourceRecords. Resolvers (SOURCE2) handle artifact
    graduation."""
    conn = AgencyOISConnector.from_directory(FIXTURE_DIR)
    for r in conn.fetch(_make_input()):
        assert isinstance(r.source_id, str)
        # No verified_artifact attribute / role at the connector layer.
        assert "artifact_source" not in r.source_roles
        # SourceRecord does not have a "verified_artifact" attribute.
        assert not hasattr(r, "verified_artifact")


# ---- Network invariance --------------------------------------------------


def test_connector_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    conn = AgencyOISConnector.from_directory(FIXTURE_DIR)
    for _ in conn.fetch(_make_input()):
        pass
    assert calls == [], f"agency OIS connector made {len(calls)} live HTTP call(s)"


# ---- Package surface -----------------------------------------------------


def test_connector_is_re_exported_from_package():
    from pipeline2_discovery.casegraph import AgencyOISConnector as Reimport
    assert Reimport is AgencyOISConnector


# ---- Determinism / idempotence -------------------------------------------


def test_fetching_twice_yields_identical_records():
    conn = AgencyOISConnector.from_directory(FIXTURE_DIR)
    first = [(r.source_id, r.url, r.source_roles) for r in conn.fetch(_make_input())]
    second = [(r.source_id, r.url, r.source_roles) for r in conn.fetch(_make_input())]
    assert first == second


def test_connector_yields_count_matches_fixtures():
    """Every fixture should produce: 1 page record + N media + N doc + N claim.
    Totals must add up across the fixture corpus."""
    conn = AgencyOISConnector.from_directory(FIXTURE_DIR)
    records = list(conn.fetch(_make_input()))
    by_kind = {}
    for r in records:
        kind = r.metadata.get("fixture_kind")
        by_kind[kind] = by_kind.get(kind, 0) + 1
    # 8 page records (one per fixture)
    assert by_kind.get("agency_page", 0) == 8
    # 5 media links across the fixture set (mp4, protected, YouTube, Vimeo, 911 audio)
    assert by_kind.get("agency_media_link", 0) == 5
    # 2 document links (pdf-only + protected-link's pdf)
    assert by_kind.get("agency_document_link", 0) == 2
    # 1 claim (bodycam_claim_no_url)
    assert by_kind.get("agency_claim", 0) == 1
