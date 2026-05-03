"""PORTAL_LINT - fixture authoring lint tests.

Two sections, both pure (no network, no production code touched):

1. Manifest lint — validates ``portal_replay_manifest.json`` beyond
   what dataclass strictness already enforces:
   - ``case_id`` values are unique across entries
   - ``portal_profile_id`` values exist in the loaded
     ``portal_profiles.json`` (cross-references the live manifest, so
     adding a new legitimate profile auto-passes)
   - ``mocked_payload_fixture`` paths are repo-relative and exist on
     disk
   - ``expected_*`` counts are non-negative integers
   - ``expected_blockers`` strings are recognized blocker codes
   - ``notes`` are non-empty and human-readable

2. Agency-OIS fixture lint — validates each
   ``tests/fixtures/agency_ois/*.json`` shape so authors get a clear
   error before the connector / resolver run produces silent
   misbehavior:
   - root is a JSON object
   - ``page_type`` is one of the known page types
   - ``url`` is an http(s) string
   - ``agency`` is a non-empty string
   - ``subjects`` is a list of non-empty strings when present
   - ``media_links`` / ``document_links`` are well-shaped link dicts
     (``url``, ``label``, ``type``)
   - link URLs are http(s)
   - link ``type`` values are non-empty strings; emit a warning when
     not in the recognized hint set, but do not fail (resolver falls
     back to URL/format inference)
   - ``claims`` are well-shaped claim dicts (``text`` non-empty,
     ``label`` string)

Sister doc: ``tests/fixtures/portal_replay/README.md`` walks new
contributors through adding a fixture safely.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse

import pytest

from pipeline2_discovery.casegraph.portal_dry_replay import (
    load_portal_replay_manifest,
)
from pipeline2_discovery.casegraph.portal_profiles import load_portal_profiles


ROOT = Path(__file__).resolve().parents[1]
AGENCY_OIS_DIR = ROOT / "tests" / "fixtures" / "agency_ois"


# Known agency-OIS page types accepted by the connector.
KNOWN_AGENCY_PAGE_TYPES = {"agency_listing", "incident_detail"}


# Recognized link ``type`` hints — these map directly to
# canonical ``artifact_type`` values via
# ``LINK_TYPE_TO_ARTIFACT_TYPE`` in
# ``pipeline2_discovery/casegraph/resolvers/agency_ois_files.py``.
# Hints outside this set are accepted by the connector and the
# resolver falls back to URL-extension / format inference, but
# authors should prefer recognized hints when one applies.
KNOWN_LINK_TYPE_HINTS = {
    "bodycam_briefing",
    "bodycam",
    "bwc",
    "dashcam",
    "surveillance",
    "interrogation",
    "police_interview",
    "court_video",
    "sentencing_video",
    "trial_video",
    "dispatch_911",
    "911_audio",
    "incident_report",
    "incident_summary",
    "ia_report",
    "use_of_force_report",
    "police_report",
    "agency_document",
}


# Blocker codes that are legitimate values for manifest
# ``expected_blockers``. Today the executor emits
# ``protected_or_nonpublic`` for protected URL flagging; the
# replay framework also emits ``mock_payload_missing`` for entries
# whose payload fixture path is absent at run time. If a future
# blocker is intentionally introduced, extend this set.
KNOWN_BLOCKER_CODES = {
    "protected_or_nonpublic",
    "mock_payload_missing",
}


# ---- Manifest lint -----------------------------------------------------


def test_manifest_case_ids_are_unique():
    manifest = load_portal_replay_manifest(repo_root=ROOT)
    case_ids = [entry.case_id for entry in manifest.entries]
    duplicates = sorted({cid for cid in case_ids if case_ids.count(cid) > 1})
    assert not duplicates, (
        f"duplicate case_id(s) in portal_replay_manifest.json: {duplicates}. "
        f"Pick a unique integer for each manifest entry."
    )


def test_manifest_portal_profile_ids_exist_in_canonical_set():
    """Cross-reference the LOADED portal_profiles.json so future
    legitimate profile additions pass automatically."""
    manifest = load_portal_replay_manifest(repo_root=ROOT)
    profiles = load_portal_profiles(repo_root=ROOT)
    known = set(profiles.profile_ids)
    for entry in manifest.entries:
        assert entry.portal_profile_id in known, (
            f"manifest case_id={entry.case_id}: portal_profile_id "
            f"{entry.portal_profile_id!r} is not a known profile in "
            f"tests/fixtures/portal_profiles/portal_profiles.json. "
            f"Known profile_ids: {sorted(known)}"
        )


def test_manifest_mocked_payload_fixtures_are_repo_relative_and_exist():
    manifest = load_portal_replay_manifest(repo_root=ROOT)
    for entry in manifest.entries:
        path_str = entry.mocked_payload_fixture
        assert isinstance(path_str, str) and path_str, (
            f"manifest case_id={entry.case_id}: mocked_payload_fixture "
            f"must be a non-empty string"
        )
        # Reject absolute POSIX or Windows-drive paths — the manifest
        # is repo-relative.
        assert not path_str.startswith("/"), (
            f"manifest case_id={entry.case_id}: mocked_payload_fixture "
            f"must be repo-relative; got absolute POSIX path {path_str!r}"
        )
        assert ":" not in Path(path_str).parts[0] if Path(path_str).parts else True, (
            f"manifest case_id={entry.case_id}: mocked_payload_fixture "
            f"must be repo-relative; got Windows-drive path {path_str!r}"
        )
        full_path = ROOT / path_str
        assert full_path.exists(), (
            f"manifest case_id={entry.case_id}: fixture file does not exist "
            f"at {path_str!r} (expanded to {full_path})"
        )


def test_manifest_expected_counts_are_non_negative_integers():
    manifest = load_portal_replay_manifest(repo_root=ROOT)
    count_fields = (
        "expected_source_records",
        "expected_artifact_claims",
        "expected_candidate_urls",
        "expected_rejected_urls",
        "expected_resolver_actions",
    )
    for entry in manifest.entries:
        for field_name in count_fields:
            value = getattr(entry, field_name)
            assert isinstance(value, int) and value >= 0, (
                f"manifest case_id={entry.case_id}: {field_name} must be a "
                f"non-negative int; got {value!r}"
            )


def test_manifest_expected_blockers_are_recognized_strings():
    manifest = load_portal_replay_manifest(repo_root=ROOT)
    for entry in manifest.entries:
        assert isinstance(entry.expected_blockers, list), (
            f"manifest case_id={entry.case_id}: expected_blockers must be a list"
        )
        for blocker in entry.expected_blockers:
            assert isinstance(blocker, str), (
                f"manifest case_id={entry.case_id}: expected_blockers entries "
                f"must be strings; got {blocker!r}"
            )
            assert blocker in KNOWN_BLOCKER_CODES, (
                f"manifest case_id={entry.case_id}: unrecognized blocker "
                f"{blocker!r}. Known blockers: {sorted(KNOWN_BLOCKER_CODES)}. "
                f"If a new blocker is intentional, extend KNOWN_BLOCKER_CODES "
                f"in tests/test_casegraph_portal_fixture_lint.py."
            )


def test_manifest_notes_are_non_empty_and_meaningful():
    manifest = load_portal_replay_manifest(repo_root=ROOT)
    for entry in manifest.entries:
        assert entry.notes, (
            f"manifest case_id={entry.case_id}: notes must be non-empty so "
            f"the entry's purpose is self-documenting. Add at least one "
            f"sentence describing what this case demonstrates."
        )
        for note in entry.notes:
            assert isinstance(note, str) and note.strip(), (
                f"manifest case_id={entry.case_id}: notes entries must be "
                f"non-empty strings; got {note!r}"
            )


# ---- Agency-OIS fixture shape lint ------------------------------------


AGENCY_OIS_FIXTURES = sorted(p for p in AGENCY_OIS_DIR.glob("*.json"))


def _fixture_id(path: Path) -> str:
    return path.name


def _load_fixture(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _is_http_url(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def test_agency_ois_fixture_directory_is_non_empty():
    """Sentinel: catches accidental deletion or relocation of all
    agency-OIS fixtures."""
    assert AGENCY_OIS_FIXTURES, (
        f"no agency-OIS fixtures found in {AGENCY_OIS_DIR}. "
        f"Did you move or delete the fixture directory?"
    )


@pytest.mark.parametrize("fixture_path", AGENCY_OIS_FIXTURES, ids=_fixture_id)
def test_agency_ois_fixture_is_json_object(fixture_path):
    data = _load_fixture(fixture_path)
    assert isinstance(data, dict), (
        f"{fixture_path.name}: fixture root must be a JSON object; "
        f"got {type(data).__name__}"
    )


@pytest.mark.parametrize("fixture_path", AGENCY_OIS_FIXTURES, ids=_fixture_id)
def test_agency_ois_fixture_has_known_page_type(fixture_path):
    data = _load_fixture(fixture_path)
    page_type = data.get("page_type")
    assert page_type in KNOWN_AGENCY_PAGE_TYPES, (
        f"{fixture_path.name}: page_type must be one of "
        f"{sorted(KNOWN_AGENCY_PAGE_TYPES)}; got {page_type!r}"
    )


@pytest.mark.parametrize("fixture_path", AGENCY_OIS_FIXTURES, ids=_fixture_id)
def test_agency_ois_fixture_url_is_http(fixture_path):
    data = _load_fixture(fixture_path)
    url = data.get("url")
    assert _is_http_url(url), (
        f"{fixture_path.name}: url must be a non-empty http(s) string; "
        f"got {url!r}"
    )


@pytest.mark.parametrize("fixture_path", AGENCY_OIS_FIXTURES, ids=_fixture_id)
def test_agency_ois_fixture_agency_is_non_empty(fixture_path):
    data = _load_fixture(fixture_path)
    agency = data.get("agency")
    assert isinstance(agency, str) and agency.strip(), (
        f"{fixture_path.name}: agency must be a non-empty string; "
        f"got {agency!r}"
    )


@pytest.mark.parametrize("fixture_path", AGENCY_OIS_FIXTURES, ids=_fixture_id)
def test_agency_ois_fixture_subjects_shape(fixture_path):
    data = _load_fixture(fixture_path)
    subjects = data.get("subjects", [])
    assert isinstance(subjects, list), (
        f"{fixture_path.name}: subjects must be a list (or absent); "
        f"got {type(subjects).__name__}"
    )
    for idx, subject in enumerate(subjects):
        assert isinstance(subject, str) and subject.strip(), (
            f"{fixture_path.name}: subjects[{idx}] must be a non-empty "
            f"string; got {subject!r}"
        )


@pytest.mark.parametrize("fixture_path", AGENCY_OIS_FIXTURES, ids=_fixture_id)
def test_agency_ois_fixture_media_links_well_shaped(fixture_path):
    data = _load_fixture(fixture_path)
    links = data.get("media_links", [])
    _assert_link_list_shape(links, fixture_path.name, "media_links")


@pytest.mark.parametrize("fixture_path", AGENCY_OIS_FIXTURES, ids=_fixture_id)
def test_agency_ois_fixture_document_links_well_shaped(fixture_path):
    data = _load_fixture(fixture_path)
    links = data.get("document_links", [])
    _assert_link_list_shape(links, fixture_path.name, "document_links")


def _assert_link_list_shape(links: Any, fixture_name: str, field_name: str) -> None:
    assert isinstance(links, list), (
        f"{fixture_name}: {field_name} must be a list (or absent); "
        f"got {type(links).__name__}"
    )
    for idx, link in enumerate(links):
        assert isinstance(link, dict), (
            f"{fixture_name}: {field_name}[{idx}] must be an object; "
            f"got {type(link).__name__}"
        )
        url = link.get("url")
        assert _is_http_url(url), (
            f"{fixture_name}: {field_name}[{idx}].url must be a non-empty "
            f"http(s) string; got {url!r}"
        )
        label = link.get("label")
        assert isinstance(label, str), (
            f"{fixture_name}: {field_name}[{idx}].label must be a string "
            f"(may be empty); got {label!r}"
        )
        type_hint = link.get("type")
        assert isinstance(type_hint, str) and type_hint, (
            f"{fixture_name}: {field_name}[{idx}].type must be a non-empty "
            f"string; got {type_hint!r}. See README §3 for recognized hints."
        )


@pytest.mark.parametrize("fixture_path", AGENCY_OIS_FIXTURES, ids=_fixture_id)
def test_agency_ois_fixture_link_type_hints_in_recognized_set(fixture_path):
    """All link ``type`` hints across the existing fixtures should
    use a recognized hint from ``LINK_TYPE_TO_ARTIFACT_TYPE``. The
    resolver accepts unknown hints (URL/format fallback), but using
    a recognized hint produces a more specific ``artifact_type``.

    If a new fixture intentionally introduces a new hint, extend
    ``KNOWN_LINK_TYPE_HINTS`` in this lint test alongside the
    matching entry in
    ``pipeline2_discovery/casegraph/resolvers/agency_ois_files.LINK_TYPE_TO_ARTIFACT_TYPE``.
    """
    data = _load_fixture(fixture_path)
    unknown_hints: List[str] = []
    for field_name in ("media_links", "document_links"):
        for link in data.get(field_name, []) or []:
            type_hint = link.get("type") if isinstance(link, dict) else None
            if isinstance(type_hint, str) and type_hint and type_hint not in KNOWN_LINK_TYPE_HINTS:
                unknown_hints.append(f"{field_name}: {type_hint!r}")
    assert not unknown_hints, (
        f"{fixture_path.name}: link type hint(s) not in the recognized set: "
        f"{unknown_hints}. Recognized hints: {sorted(KNOWN_LINK_TYPE_HINTS)}. "
        f"If introducing a new hint intentionally, extend KNOWN_LINK_TYPE_HINTS "
        f"in tests/test_casegraph_portal_fixture_lint.py and add a matching "
        f"entry to LINK_TYPE_TO_ARTIFACT_TYPE in agency_ois_files.py."
    )


@pytest.mark.parametrize("fixture_path", AGENCY_OIS_FIXTURES, ids=_fixture_id)
def test_agency_ois_fixture_claims_well_shaped(fixture_path):
    data = _load_fixture(fixture_path)
    claims = data.get("claims", [])
    assert isinstance(claims, list), (
        f"{fixture_path.name}: claims must be a list (or absent); "
        f"got {type(claims).__name__}"
    )
    for idx, claim in enumerate(claims):
        assert isinstance(claim, dict), (
            f"{fixture_path.name}: claims[{idx}] must be an object; "
            f"got {type(claim).__name__}"
        )
        text = claim.get("text")
        assert isinstance(text, str) and text.strip(), (
            f"{fixture_path.name}: claims[{idx}].text must be a non-empty "
            f"string; got {text!r}"
        )
        label = claim.get("label", "")
        assert isinstance(label, str), (
            f"{fixture_path.name}: claims[{idx}].label must be a string when "
            f"present; got {label!r}"
        )
