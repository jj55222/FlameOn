"""PORTAL1 - portal profile manifest tests."""
import json
from pathlib import Path

from pipeline2_discovery.casegraph.portal_profiles import (
    MAX_DEFAULT_LINKS,
    MAX_DEFAULT_PAGES,
    REQUIRED_PROFILE_IDS,
    load_portal_profiles,
    manifest_to_jsonable,
    profiles_for_expected_artifact,
)


ROOT = Path(__file__).resolve().parents[1]


def test_manifest_loads_all_required_profiles_without_warnings():
    manifest = load_portal_profiles(repo_root=ROOT)

    assert set(manifest.profile_ids) == REQUIRED_PROFILE_IDS
    assert manifest.warnings == []


def test_every_profile_has_caps_and_blocks_private_access():
    manifest = load_portal_profiles(repo_root=ROOT)

    for profile in manifest.profiles:
        assert profile.max_pages <= MAX_DEFAULT_PAGES
        assert profile.max_links <= MAX_DEFAULT_LINKS
        assert profile.resolver_policy["allow_downloads"] is False
        assert profile.resolver_policy["allow_scraping"] is False
        assert profile.resolver_policy["allow_login"] is False
        assert profile.resolver_policy["metadata_only"] is True
        assert profile.resolver_policy["require_public_url"] is True
        assert {"login", "private", "protected", "password"} <= set(profile.blocked_url_patterns)
        assert {"login", "private", "protected", "password"} <= set(profile.protected_private_login_patterns)


def test_profile_ids_are_stable_and_json_serializable():
    manifest = load_portal_profiles(repo_root=ROOT)

    assert manifest.profile_ids == [
        "agency_ois_listing",
        "agency_ois_detail",
        "da_critical_incident",
        "city_critical_incident",
        "sheriff_critical_incident",
        "court_docket_search",
        "court_case_detail",
        "foia_request_page",
        "document_release_page",
        "youtube_agency_channel",
        "vimeo_agency_channel",
        "documentcloud_search",
        "muckrock_request",
        "courtlistener_search",
    ]
    encoded = json.dumps(manifest_to_jsonable(manifest), sort_keys=True)
    decoded = json.loads(encoded)

    assert decoded["version"] == 1
    assert len(decoded["profiles"]) == len(REQUIRED_PROFILE_IDS)


def test_profiles_map_to_expected_source_and_resolver_behavior():
    manifest = load_portal_profiles(repo_root=ROOT)

    agency = manifest.get("agency_ois_detail")
    youtube = manifest.get("youtube_agency_channel")
    courtlistener = manifest.get("courtlistener_search")
    muckrock = manifest.get("muckrock_request")

    assert agency is not None
    assert agency.source_authority == "official_agency"
    assert "official_critical_incident_video" in agency.expected_artifact_types
    assert "extract_public_media_urls" in agency.next_actions

    assert youtube is not None
    assert youtube.allowed_fetchers == ["youtube_metadata_api"]
    assert "manual_review_if_generic" in youtube.safety_flags

    assert courtlistener is not None
    assert courtlistener.source_authority == "court"
    assert "avoid_document_downloads" in courtlistener.next_actions

    assert muckrock is not None
    assert muckrock.source_authority == "foia"
    assert "resolve_public_muckrock_file_links" in muckrock.next_actions


def test_expected_artifact_lookup_finds_primary_media_profiles():
    manifest = load_portal_profiles(repo_root=ROOT)

    bodycam_profiles = profiles_for_expected_artifact(manifest, "bodycam")
    profile_ids = {profile.profile_id for profile in bodycam_profiles}

    assert "agency_ois_detail" in profile_ids
    assert "youtube_agency_channel" in profile_ids
    assert "muckrock_request" in profile_ids


def test_portal_profile_loader_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    load_portal_profiles(repo_root=ROOT)
    assert calls == []
