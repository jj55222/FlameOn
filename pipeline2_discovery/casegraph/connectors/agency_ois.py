"""SOURCE1 — Agency OIS / critical incident connector (mocked first).

Pure no-network connector for official agency OIS / critical-incident
pages. Reads pre-loaded fixture dicts (or fixture file paths) shaped
like agency listing pages and emits :class:`SourceRecord` objects with
``source_authority="official"``. No live HTTP. No scraping. No
download.

Fixture shape (one JSON object per agency page)::

    {
        "page_type": "agency_listing" | "incident_detail",
        "agency": "Phoenix Police Department",
        "url": "https://www.phoenix.gov/police/...",
        "title": "...",
        "narrative": "...",
        "subjects": ["John Example"],
        "incident_date": "2024-05-12",
        "case_number": "2024-OIS-014",
        "outcome_text": "subject sentenced 2024" | null,
        "media_links": [{"url": "...", "label": "...", "type": "..."}],
        "document_links": [{"url": "...", "label": "...", "type": "..."}],
        "claims": [{"text": "Body-worn camera footage will be released ...", "label": "release_pending"}]
    }

For each page the connector emits:

- one **page-level** SourceRecord. Roles assigned from page content:
  ``identity_source`` if subjects present, ``outcome_source`` if
  outcome_text present, ``claim_source`` if claims present. The page
  record itself is NEVER assigned ``possible_artifact_source``.
- one SourceRecord per ``media_links`` item. Role
  ``possible_artifact_source``; URL pattern that looks
  protected/private/login-walled adds ``protected_or_nonpublic`` to
  ``risk_flags`` so the resolver can refuse it without contacting the
  network.
- one SourceRecord per ``document_links`` item, same shape as media.
- one SourceRecord per ``claims`` item — role ``claim_source`` ONLY.
  Claim text without a URL never gets ``possible_artifact_source``;
  this preserves the non-negotiable rule that ``claim_source !=
  artifact_source``.

The connector creates NO VerifiedArtifact at any layer — that is the
agency-OIS resolver's job (SOURCE2)."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Union

from ..models import CaseInput, SourceRecord
from .base import SourceConnector


PROTECTED_URL_PATTERNS = (
    re.compile(r"\blogin\b", re.IGNORECASE),
    re.compile(r"\bauth\b", re.IGNORECASE),
    re.compile(r"\btoken=", re.IGNORECASE),
    re.compile(r"/private/", re.IGNORECASE),
    re.compile(r"/restricted/", re.IGNORECASE),
    re.compile(r"\bpacer\b", re.IGNORECASE),
)


FixtureLike = Union[Path, Mapping[str, Any]]


def _looks_protected(url: str) -> bool:
    if not url:
        return True
    return any(p.search(url) for p in PROTECTED_URL_PATTERNS)


def _normalize_fixture(fixture: FixtureLike) -> Mapping[str, Any]:
    if isinstance(fixture, Mapping):
        return fixture
    path = Path(fixture)
    with path.open("r", encoding="utf-8") as f:
        loaded = json.load(f)
    if not isinstance(loaded, dict):
        raise ValueError(f"agency OIS fixture root is not a JSON object: {path}")
    return loaded


class AgencyOISConnector(SourceConnector):
    """Pure no-network agency OIS connector.

    Construct with an iterable of fixture dicts or fixture file paths;
    ``fetch(case_input)`` then yields the SourceRecords those fixtures
    describe. The connector ignores ``case_input`` for filtering — the
    caller is expected to pre-curate the fixture list (just like the
    existing ``MockSourceConnector``)."""

    name = "agency_ois"

    def __init__(self, fixtures: Iterable[FixtureLike]):
        self._fixtures: List[Mapping[str, Any]] = [
            _normalize_fixture(f) for f in list(fixtures)
        ]

    @classmethod
    def from_directory(cls, directory: Path) -> "AgencyOISConnector":
        directory = Path(directory)
        if not directory.exists():
            raise FileNotFoundError(f"agency_ois fixture directory not found: {directory}")
        paths = sorted(directory.glob("*.json"))
        return cls(paths)

    def fetch(self, case_input: CaseInput) -> Iterable[SourceRecord]:
        case_input_id = (case_input.raw_input or {}).get("defendant_names")
        for fixture in self._fixtures:
            yield from self._records_from_fixture(fixture, case_input_id=case_input_id)

    # ---- per-fixture emission ---------------------------------------------

    def _records_from_fixture(
        self,
        fixture: Mapping[str, Any],
        *,
        case_input_id: Optional[Any],
    ) -> Iterable[SourceRecord]:
        page_url = str(fixture.get("url") or "")
        page_title = str(fixture.get("title") or "")
        narrative = str(fixture.get("narrative") or "")
        agency = str(fixture.get("agency") or "")
        case_number = fixture.get("case_number")
        case_number_str = str(case_number) if case_number else ""
        page_type = str(fixture.get("page_type") or "agency_page")
        subjects = list(fixture.get("subjects") or [])
        outcome_text = fixture.get("outcome_text")
        media_links = list(fixture.get("media_links") or [])
        document_links = list(fixture.get("document_links") or [])
        claims = list(fixture.get("claims") or [])

        page_roles: List[str] = []
        if subjects:
            page_roles.append("identity_source")
        if outcome_text:
            page_roles.append("outcome_source")
        if claims:
            page_roles.append("claim_source")
        # page-level source record (always emitted, even if listing page
        # has no subjects — the listing itself is still an official page)
        if not page_roles:
            # Listings with no per-incident details still surface as a
            # background/identity-anchor source; we mark with no role
            # so identity / outcome / claim resolvers ignore it.
            page_roles = []

        slug = re.sub(r"[^A-Za-z0-9_-]+", "_", page_url) or "page"
        page_source_id = f"agency_ois::page::{slug}"
        yield SourceRecord(
            source_id=page_source_id,
            url=page_url,
            title=page_title,
            snippet=(narrative[:280] if narrative else page_title),
            raw_text=narrative,
            source_type=page_type,
            source_roles=page_roles,
            source_authority="official",
            api_name="agency_ois",
            discovered_via="agency_ois_fixture",
            case_input_id=case_input_id,
            metadata={
                "agency": agency,
                "case_number": case_number_str,
                "incident_date": fixture.get("incident_date"),
                "subjects": subjects,
                "outcome_text": outcome_text,
                "fixture_kind": "agency_page",
            },
            cost_estimate=0.0,
            confidence_signals={
                "official_agency_page": True,
                "subjects_named": bool(subjects),
                "outcome_text_present": bool(outcome_text),
                "claim_language_present": bool(claims),
            },
            matched_case_fields=[
                f for f in ("defendant_full_name" if subjects else None,
                            "case_number" if case_number else None,
                            "agency") if f
            ],
        )

        for idx, link in enumerate(media_links):
            url = str(link.get("url") or "")
            label = str(link.get("label") or "")
            link_type = str(link.get("type") or "agency_media")
            if not url:
                continue
            risk_flags: List[str] = []
            if _looks_protected(url):
                risk_flags.append("protected_or_nonpublic")
            yield SourceRecord(
                source_id=f"agency_ois::media::{slug}::{idx}",
                url=url,
                title=label or page_title,
                snippet=label or "Agency-published media link",
                raw_text=label,
                source_type=f"agency_media:{link_type}",
                source_roles=["possible_artifact_source"],
                source_authority="official",
                api_name="agency_ois",
                discovered_via="agency_ois_fixture",
                case_input_id=case_input_id,
                metadata={
                    "agency": agency,
                    "case_number": case_number_str,
                    "host_page_url": page_url,
                    "media_link_type": link_type,
                    "risk_flags": list(risk_flags),
                    "fixture_kind": "agency_media_link",
                },
                cost_estimate=0.0,
                confidence_signals={
                    "official_host": True,
                    "concrete_url_present": True,
                    "looks_protected": "protected_or_nonpublic" in risk_flags,
                },
                matched_case_fields=["agency"],
            )

        for idx, link in enumerate(document_links):
            url = str(link.get("url") or "")
            label = str(link.get("label") or "")
            link_type = str(link.get("type") or "agency_document")
            if not url:
                continue
            risk_flags = []
            if _looks_protected(url):
                risk_flags.append("protected_or_nonpublic")
            yield SourceRecord(
                source_id=f"agency_ois::doc::{slug}::{idx}",
                url=url,
                title=label or page_title,
                snippet=label or "Agency-published document link",
                raw_text=label,
                source_type=f"agency_document:{link_type}",
                source_roles=["possible_artifact_source"],
                source_authority="official",
                api_name="agency_ois",
                discovered_via="agency_ois_fixture",
                case_input_id=case_input_id,
                metadata={
                    "agency": agency,
                    "case_number": case_number_str,
                    "host_page_url": page_url,
                    "document_link_type": link_type,
                    "risk_flags": list(risk_flags),
                    "fixture_kind": "agency_document_link",
                },
                cost_estimate=0.0,
                confidence_signals={
                    "official_host": True,
                    "concrete_url_present": True,
                    "looks_protected": "protected_or_nonpublic" in risk_flags,
                },
                matched_case_fields=["agency"],
            )

        for idx, claim in enumerate(claims):
            text = str(claim.get("text") or "")
            label = str(claim.get("label") or "release_language")
            if not text:
                continue
            yield SourceRecord(
                source_id=f"agency_ois::claim::{slug}::{idx}",
                url=page_url,
                title=label,
                snippet=text,
                raw_text=text,
                source_type=f"agency_claim:{label}",
                # claim_source ONLY -- never possible_artifact_source.
                source_roles=["claim_source"],
                source_authority="official",
                api_name="agency_ois",
                discovered_via="agency_ois_fixture",
                case_input_id=case_input_id,
                metadata={
                    "agency": agency,
                    "case_number": case_number_str,
                    "host_page_url": page_url,
                    "claim_label": label,
                    "fixture_kind": "agency_claim",
                },
                cost_estimate=0.0,
                confidence_signals={
                    "official_host": True,
                    "claim_language_present": True,
                    "concrete_url_present": False,
                },
                matched_case_fields=["agency"],
            )
