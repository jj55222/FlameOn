"""PORTAL5 — Offline portal-replay → CasePacket → handoffs integration harness.

Proves that saved portal/agency-OIS payload fixtures can be threaded
through the full offline pipeline and produce schema-validated
downstream handoffs without any live network call, without Firecrawl,
and without browser automation.

Chain under test (all offline):

    saved portal payload fixture
      -> portal executor (mocked, no fetch)
      -> agency_ois SourceRecords (or manifest-supplied SourceRecords)
      -> CasePacket (built via route_manual_defendant_jurisdiction +
         attach the portal-extracted SourceRecords)
      -> resolve_identity / resolve_outcome / extract_artifact_claims
      -> run_metadata_only_resolvers (which now includes agency_ois
         per PR #4)
      -> score_case_packet
      -> export_p2_to_p3 / export_p2_to_p4 / export_p2_to_p5
      -> schema validation against schemas/p2_to_p*.schema.json

This is a TEST-ONLY harness: it uses the existing public package API
to glue executor → assembly → handoffs. No production code is added
or modified. No new fixtures are introduced. The bridge logic lives
inside this test file and naturally falls out as the basis for a
later operator-facing CLI portal-replay mode.

Doctrine asserted:
- concrete public bodycam/media URLs graduate into verified_artifacts
- claim-only payloads do NOT graduate into verified_artifacts (only
  artifact_claims surface)
- protected/private/login/token/auth URLs are rejected with the
  protected_or_nonpublic risk flag
- document-only payloads do NOT become PRODUCE
- P3 rows exist only for verified artifacts
- handoffs validate against the canonical p2_to_p* schemas
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping
from urllib.parse import urlparse

import pytest

from pipeline2_discovery.casegraph import (
    AgencyOISConnector,
    CaseInput,
    CasePacket,
    SourceRecord,
    export_p2_to_p3,
    export_p2_to_p4,
    export_p2_to_p5,
    extract_artifact_claims,
    resolve_identity,
    resolve_outcome,
    route_manual_defendant_jurisdiction,
    run_metadata_only_resolvers,
    score_case_packet,
)
from pipeline2_discovery.casegraph.cli import enrich_portal_replay_identity
from pipeline2_discovery.casegraph.portal_executor import execute_mock_portal_plan
from pipeline2_discovery.casegraph.portal_fetch_plan import PortalFetchPlan


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "schemas"
PORTAL_REPLAY_DIR = ROOT / "tests" / "fixtures" / "portal_replay"
MANIFEST_PATH = PORTAL_REPLAY_DIR / "portal_replay_manifest.json"


# ---- Helpers (test-only bridge) -----------------------------------------


def _load_manifest_entries() -> List[Dict[str, Any]]:
    with MANIFEST_PATH.open("r", encoding="utf-8") as f:
        return list(json.load(f)["entries"])


def _entry_by_case_id(entries: List[Dict[str, Any]], case_id: int) -> Dict[str, Any]:
    for entry in entries:
        if entry["case_id"] == case_id:
            return entry
    raise KeyError(f"manifest has no entry with case_id={case_id}")


def _load_payload(entry: Mapping[str, Any]) -> Dict[str, Any]:
    fixture_path = ROOT / entry["mocked_payload_fixture"]
    with fixture_path.open("r", encoding="utf-8") as f:
        return dict(json.load(f))


def _build_plan(entry: Mapping[str, Any], payload: Mapping[str, Any]) -> PortalFetchPlan:
    """Build a minimal fetch plan that mirrors the manifest entry. The
    plan is never actually used to fetch — it only seeds the executor's
    diagnostic shape."""
    profile_id = str(entry["portal_profile_id"])
    seed_url = str(payload.get("url") or "")
    title = ""
    subjects = payload.get("subjects") or []
    if isinstance(subjects, list) and subjects:
        title = str(subjects[0])
    elif payload.get("title"):
        title = str(payload["title"])
    allowed_domain = urlparse(seed_url).netloc.lower() or None
    return PortalFetchPlan(
        case_id=int(entry["case_id"]),
        title=title,
        portal_profile_id=profile_id,
        seed_url=seed_url or None,
        seed_url_exists=bool(seed_url),
        fetcher="firecrawl",
        max_pages=1,
        max_links=10,
        allowed_domain=allowed_domain,
    )


def _source_record_from_dict(raw: Mapping[str, Any]) -> SourceRecord:
    return SourceRecord(
        source_id=str(raw["source_id"]),
        url=str(raw.get("url") or ""),
        title=str(raw.get("title") or ""),
        snippet=str(raw.get("snippet") or ""),
        raw_text=str(raw.get("raw_text") or raw.get("snippet") or ""),
        source_type=str(raw.get("source_type") or "unknown"),
        source_authority=str(raw.get("source_authority") or "unknown"),
        source_roles=list(raw.get("source_roles") or []),
        api_name=raw.get("api_name"),
        discovered_via=str(raw.get("discovered_via") or ""),
        case_input_id=raw.get("case_input_id"),
        metadata=dict(raw.get("metadata") or {}),
        cost_estimate=float(raw.get("cost_estimate") or 0.0),
        confidence_signals=dict(raw.get("confidence_signals") or {}),
        matched_case_fields=list(raw.get("matched_case_fields") or []),
    )


def _portal_source_records(payload: Mapping[str, Any]) -> List[SourceRecord]:
    """Public-API replay of portal_executor._records_from_payload.

    Mirrors the executor's dispatch: manifest-supplied source_records
    arrays are loaded directly; agency-shaped pages flow through
    AgencyOISConnector. This lets the harness work with both the
    agency_ois fixtures and the generic manifest-supplied fixtures
    without importing the executor's private helpers.
    """
    records_field = payload.get("source_records")
    if isinstance(records_field, list):
        return [_source_record_from_dict(item) for item in records_field]
    profile_id = str(payload.get("portal_profile_id") or "")
    if profile_id.startswith("agency_ois") or payload.get("page_type"):
        subjects = payload.get("subjects") or []
        if isinstance(subjects, str):
            subjects = [subjects]
        case_input = CaseInput(
            input_type="manual",
            raw_input={"defendant_names": ", ".join(str(s) for s in subjects)},
            known_fields={"defendant_names": list(subjects)},
        )
        return list(AgencyOISConnector([dict(payload)]).fetch(case_input))
    return []


def _derive_jurisdiction_string(payload: Mapping[str, Any]) -> str:
    """Best-effort city derivation from agency name. Agency-OIS
    fixtures use 'Phoenix Police Department' / 'Maricopa County
    Sheriff's Office' style strings; we strip common suffixes and
    pair with AZ as a placeholder. The exact jurisdiction does not
    affect this harness's doctrinal assertions — identity scoring
    only needs *some* jurisdiction anchor."""
    agency = str(payload.get("agency") or "")
    if not agency:
        return "Unknown"
    city = agency
    for tag in (
        " Police Department",
        " Sheriff's Office",
        " Sheriff Department",
        " Sheriff's Department",
        " Sheriff",
        " Department of Public Safety",
        " PD",
    ):
        if tag in city:
            city = city.replace(tag, "").strip()
            break
    return f"{city}, AZ" if city else "Unknown"


def _build_packet_from_payload(payload: Mapping[str, Any]) -> CasePacket:
    """Build a CasePacket from a portal payload via the public manual
    router, attach the portal-extracted SourceRecords, and lift
    agency_ois identity facts via the shared CLI helper so this harness
    stays in lockstep with --portal-replay mode."""
    subjects = payload.get("subjects") or []
    defendant = subjects[0] if (isinstance(subjects, list) and subjects) else "Generic Subject"
    jurisdiction = _derive_jurisdiction_string(payload)
    packet = route_manual_defendant_jurisdiction(str(defendant), jurisdiction)
    packet.sources = list(_portal_source_records(payload))
    enrich_portal_replay_identity(packet, payload)
    return packet


def _run_offline_pipeline(packet: CasePacket):
    """Run the full offline assembly pipeline against a packet that
    already has portal-extracted SourceRecords attached. Returns
    ``ActionabilityResult`` from ``score_case_packet``."""
    resolve_identity(packet)
    resolve_outcome(packet)
    extract_artifact_claims(packet)
    run_metadata_only_resolvers(packet)
    return score_case_packet(packet)


def _assert_valid(schema_name: str, instance: Any) -> None:
    """Validate against an existing schema. Skip cleanly when
    jsonschema is unavailable (matches PR #6 / PR #7 pattern)."""
    try:
        from jsonschema import Draft7Validator  # type: ignore
    except ImportError:  # pragma: no cover
        pytest.skip("jsonschema not installed")
    schema = json.loads((SCHEMA_DIR / schema_name).read_text(encoding="utf-8"))
    errors = sorted(
        Draft7Validator(schema).iter_errors(instance), key=lambda e: list(e.path)
    )
    assert not errors, "; ".join(
        f"{list(e.path)}: {e.message}" for e in errors
    )


def _validate_handoffs(packet: CasePacket) -> Dict[str, Any]:
    """Run the three exporters and validate each against its schema.
    Returns the three exports for further per-test assertions."""
    p3 = export_p2_to_p3(packet)
    p4 = export_p2_to_p4(packet)
    p5 = export_p2_to_p5(packet)
    for row in p3:
        _assert_valid("p2_to_p3.schema.json", row)
    _assert_valid("p2_to_p4.schema.json", p4)
    _assert_valid("p2_to_p5.schema.json", p5)
    return {"p2_to_p3": p3, "p2_to_p4": p4, "p2_to_p5": p5}


# ---- Module-scoped fixtures: executor diagnostics + assembly result ----


@pytest.fixture(scope="module")
def manifest_entries() -> List[Dict[str, Any]]:
    return _load_manifest_entries()


def _run_full(entry: Mapping[str, Any]) -> Dict[str, Any]:
    """Execute the full chain for a manifest entry and return the
    artifacts that the test functions assert against. Diagnostics
    from the executor are kept alongside the assembled packet so
    individual tests can cross-check both surfaces."""
    payload = _load_payload(entry)
    plan = _build_plan(entry, payload)
    exec_result = execute_mock_portal_plan(plan, payload)
    packet = _build_packet_from_payload(payload)
    score_result = _run_offline_pipeline(packet)
    handoffs = _validate_handoffs(packet)
    return {
        "entry": dict(entry),
        "payload": payload,
        "plan": plan,
        "exec_result": exec_result,
        "packet": packet,
        "score_result": score_result,
        "handoffs": handoffs,
    }


@pytest.fixture(scope="module")
def case_31_run(manifest_entries):
    return _run_full(_entry_by_case_id(manifest_entries, 31))


@pytest.fixture(scope="module")
def case_32_run(manifest_entries):
    return _run_full(_entry_by_case_id(manifest_entries, 32))


@pytest.fixture(scope="module")
def case_33_run(manifest_entries):
    return _run_full(_entry_by_case_id(manifest_entries, 33))


@pytest.fixture(scope="module")
def case_34_run(manifest_entries):
    return _run_full(_entry_by_case_id(manifest_entries, 34))


@pytest.fixture(scope="module")
def case_37_run(manifest_entries):
    return _run_full(_entry_by_case_id(manifest_entries, 37))


# ---- Case 31: agency_ois_detail with YouTube embed ---------------------
# Doctrine: a public YouTube media URL surfaced by a portal page must
# graduate into a verified_artifact via the agency_ois resolver chain.


def test_case_31_executor_diagnostics_match_manifest(case_31_run):
    entry = case_31_run["entry"]
    exec_result = case_31_run["exec_result"]
    assert exec_result.execution_status == "completed"
    assert len(exec_result.extracted_source_records) == entry["expected_source_records"]
    assert len(exec_result.candidate_artifact_urls) == entry["expected_candidate_urls"]
    assert len(exec_result.rejected_urls) == entry["expected_rejected_urls"]


def test_case_31_youtube_embed_graduates_via_assembly(case_31_run):
    packet = case_31_run["packet"]
    youtube_artifacts = [
        a for a in packet.verified_artifacts if "youtube.com" in a.artifact_url
    ]
    assert youtube_artifacts, (
        "YouTube embed from agency_ois portal payload should graduate as a "
        "verified_artifact via the metadata-only resolver chain"
    )
    # The agency_ois resolver classifies link_type=bodycam_briefing as bodycam.
    assert youtube_artifacts[0].artifact_type == "bodycam"
    assert youtube_artifacts[0].source_authority == "official"


def test_case_31_handoffs_validate_and_p3_includes_media(case_31_run):
    handoffs = case_31_run["handoffs"]
    assert handoffs["p2_to_p3"], "P3 rows expected for graduated YouTube media"
    media_rows = [
        row for row in handoffs["p2_to_p3"] if row.get("format") == "video"
    ]
    assert media_rows, "P3 should include at least one video row from the portal payload"


# ---- Case 32: agency_ois_detail with bodycam claim, no URL -------------
# Doctrine (claim_source != possible_artifact_source): claim text alone
# never graduates into a verified_artifact.


def test_case_32_executor_diagnostics_match_manifest(case_32_run):
    entry = case_32_run["entry"]
    exec_result = case_32_run["exec_result"]
    assert exec_result.execution_status == "completed"
    assert len(exec_result.extracted_source_records) == entry["expected_source_records"]
    assert len(exec_result.candidate_artifact_urls) == 0
    assert exec_result.artifact_claims, "claim text must surface as ArtifactClaims"


def test_case_32_claim_only_payload_does_not_graduate(case_32_run):
    packet = case_32_run["packet"]
    assert packet.verified_artifacts == [], (
        "claim-only portal payload must not produce verified_artifacts"
    )
    assert packet.artifact_claims, (
        "claim text must still surface as ArtifactClaim entries"
    )


def test_case_32_handoffs_validate_with_zero_p3_rows(case_32_run):
    handoffs = case_32_run["handoffs"]
    assert handoffs["p2_to_p3"] == [], (
        "no verified_artifacts → no P3 rows for claim-only payloads"
    )
    # P4 / P5 still validate as schema-conformant objects.
    score_result = case_32_run["score_result"]
    assert score_result.verdict != "PRODUCE"


# ---- Case 33: agency_ois_detail with public PDF (document-only) --------
# Doctrine: a document-only payload graduates a docket_docs artifact
# but cannot reach PRODUCE without media.


def test_case_33_executor_diagnostics_match_manifest(case_33_run):
    entry = case_33_run["entry"]
    exec_result = case_33_run["exec_result"]
    assert exec_result.execution_status == "completed"
    assert len(exec_result.candidate_artifact_urls) == entry["expected_candidate_urls"]


def test_case_33_document_graduates_but_no_media(case_33_run):
    packet = case_33_run["packet"]
    documents = [a for a in packet.verified_artifacts if a.format == "pdf"]
    media = [a for a in packet.verified_artifacts if a.format in {"video", "audio"}]
    assert documents, "public PDF should graduate as a verified document artifact"
    assert media == [], "no media artifact graduates from a document-only payload"


def test_case_33_document_only_does_not_produce(case_33_run):
    score_result = case_33_run["score_result"]
    assert score_result.verdict != "PRODUCE", (
        "document-only payload must not reach PRODUCE without media"
    )


def test_case_33_handoffs_validate_with_document_p3_row(case_33_run):
    handoffs = case_33_run["handoffs"]
    assert handoffs["p2_to_p3"], "document graduation should yield a P3 row"


# ---- Case 34: agency_ois_detail with protected/login link --------------
# Doctrine: protected/private/login URLs must be rejected; the public
# document alongside them still graduates.


def test_case_34_executor_rejects_protected_url(case_34_run):
    entry = case_34_run["entry"]
    exec_result = case_34_run["exec_result"]
    assert exec_result.execution_status == "completed"
    assert len(exec_result.rejected_urls) == entry["expected_rejected_urls"]
    assert "protected_or_nonpublic" in exec_result.risk_flags


def test_case_34_protected_media_does_not_graduate(case_34_run):
    packet = case_34_run["packet"]
    # No verified artifact should carry the protected URL.
    protected_artifact_urls = [
        a.artifact_url for a in packet.verified_artifacts
        if "login" in a.artifact_url.lower() or "/private/" in a.artifact_url.lower()
    ]
    assert protected_artifact_urls == [], (
        f"protected URLs must not graduate; found: {protected_artifact_urls}"
    )


def test_case_34_public_document_alongside_protected_media_graduates(case_34_run):
    packet = case_34_run["packet"]
    documents = [a for a in packet.verified_artifacts if a.format == "pdf"]
    assert documents, (
        "public PDF on the same page should still graduate even when its "
        "media sibling is protected"
    )


def test_case_34_protected_risk_flag_surfaces(case_34_run):
    packet = case_34_run["packet"]
    score_result = case_34_run["score_result"]
    risks = set(packet.risk_flags) | set(score_result.risk_flags)
    assert "protected_or_nonpublic" in risks, (
        f"protected_or_nonpublic risk flag must surface; risks={sorted(risks)}"
    )


def test_case_34_handoffs_validate(case_34_run):
    # _validate_handoffs ran inside the fixture; arriving here means schemas passed.
    assert case_34_run["handoffs"]["p2_to_p3"] is not None


# ---- Case 37: youtube_agency_channel generic weak media ----------------
# Doctrine: a generic YouTube source with no real case context should
# graduate (it carries possible_artifact_source role) but must NOT
# reach PRODUCE without a locked identity.


def test_case_37_executor_diagnostics_match_manifest(case_37_run):
    entry = case_37_run["entry"]
    exec_result = case_37_run["exec_result"]
    assert exec_result.execution_status == "completed"
    assert len(exec_result.extracted_source_records) == entry["expected_source_records"]
    assert len(exec_result.candidate_artifact_urls) == entry["expected_candidate_urls"]


def test_case_37_generic_youtube_does_not_produce_without_identity(case_37_run):
    score_result = case_37_run["score_result"]
    # No real subject/jurisdiction in the payload → identity is low →
    # severe risks fire (weak_identity / identity_unconfirmed) → cannot
    # PRODUCE regardless of whether the media URL graduated.
    assert score_result.verdict != "PRODUCE"


def test_case_37_handoffs_validate(case_37_run):
    handoffs = case_37_run["handoffs"]
    # P3 may or may not be empty depending on whether the generic
    # YouTube source graduates; either way, all schemas must validate.
    assert isinstance(handoffs["p2_to_p3"], list)


# ---- Cross-cutting safety: the harness never makes a network call ------


def test_portal_replay_harness_makes_zero_network_calls(monkeypatch, manifest_entries):
    """Belt-and-suspenders: re-run a representative subset of the
    chain under a request-mocking guard to prove no module on the path
    quietly reaches the network."""
    import requests

    calls: List[Any] = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    for case_id in (31, 32, 33, 34, 37):
        entry = _entry_by_case_id(manifest_entries, case_id)
        payload = _load_payload(entry)
        plan = _build_plan(entry, payload)
        execute_mock_portal_plan(plan, payload)
        packet = _build_packet_from_payload(payload)
        _run_offline_pipeline(packet)
        _validate_handoffs(packet)

    assert calls == [], f"portal replay harness made {len(calls)} live HTTP call(s)"
