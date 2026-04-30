"""H5-lite — YouTube transcript conflict handling.

The YouTube parser combines title + description + transcript text for
candidate-field extraction, but when those segments DISAGREE on a
field (different dates, different agencies, different cities,
different defendant names), the parser must:

- preserve all candidates (no silent overwrite)
- emit a `conflicting_<field>_across_segments` risk flag
- expose a per-segment view via `YouTubeInputParseResult.segment_conflicts`
- never PRODUCE downstream until the conflict is corroborated

Goal: an unverified video lead that contradicts itself never becomes
the basis for a confident case packet.
"""
import json
from pathlib import Path

from pipeline2_discovery.casegraph import (
    SourceConnector,
    SourceRecord,
    assemble_weak_input_case_packet,
    parse_youtube_case_input,
    plan_queries_from_youtube_result,
)


YOUTUBE_FIXTURES = Path(__file__).resolve().parent / "fixtures" / "youtube_inputs"


def load_youtube_fixture(name):
    with (YOUTUBE_FIXTURES / name).open("r", encoding="utf-8") as f:
        return json.load(f)


def test_conflicting_dates_emit_segment_conflict_and_preserve_all_candidates():
    parsed = parse_youtube_case_input(load_youtube_fixture("conflicting_dates.json"))

    # All candidates preserved — no silent overwrite.
    candidates = parsed.candidate_fields["incident_date_candidates"]
    assert "2024-04-21" in candidates
    assert "2024-04-22" in candidates
    # incident_date is None when there are multiple candidates (existing behavior).
    assert parsed.candidate_fields["incident_date"] is None

    # Risk flags surface the conflict at the per-segment level.
    assert "conflicting_incident_dates" in parsed.risk_flags
    assert "conflicting_incident_date_across_segments" in parsed.risk_flags

    # segment_conflicts breaks down which segment introduced which value.
    assert "incident_date" in parsed.segment_conflicts
    seg_view = parsed.segment_conflicts["incident_date"]
    assert "2024-04-21" in seg_view.get("title", [])
    assert "2024-04-22" in seg_view.get("transcript", [])


def test_conflicting_agencies_emit_segment_conflict_and_preserve_all_agencies():
    parsed = parse_youtube_case_input(load_youtube_fixture("conflicting_agencies.json"))

    # Risk flag emitted.
    assert "conflicting_agency_across_segments" in parsed.risk_flags

    # Per-segment view shows the disagreement.
    assert "agency" in parsed.segment_conflicts
    seg_view = parsed.segment_conflicts["agency"]
    title_agencies = set(seg_view.get("title", []))
    transcript_agencies = set(seg_view.get("transcript", []))
    assert "Phoenix Police Department" in title_agencies
    assert "Mesa Police Department" in transcript_agencies
    # No silent overwrite: both agencies appear somewhere in the segment view.
    assert "Phoenix Police Department" in title_agencies | transcript_agencies
    assert "Mesa Police Department" in title_agencies | transcript_agencies


def test_conflicting_locations_emit_segment_conflict():
    parsed = parse_youtube_case_input(load_youtube_fixture("conflicting_locations.json"))

    assert "conflicting_location_city_across_segments" in parsed.risk_flags

    assert "location_city" in parsed.segment_conflicts
    seg_view = parsed.segment_conflicts["location_city"]
    cities_seen = set()
    for cities in seg_view.values():
        cities_seen.update(cities)
    assert "Phoenix" in cities_seen
    assert "Tucson" in cities_seen


def test_conflicting_defendant_names_emit_segment_conflict():
    parsed = parse_youtube_case_input(load_youtube_fixture("conflicting_defendant_names.json"))

    assert "conflicting_defendant_names_across_segments" in parsed.risk_flags

    assert "defendant_names" in parsed.segment_conflicts
    seg_view = parsed.segment_conflicts["defendant_names"]
    names_seen = set()
    for names in seg_view.values():
        names_seen.update(names)
    assert "Daniel Reyes" in names_seen
    assert "Marcus Hill" in names_seen


def test_unanimous_segments_emit_no_conflict_flag():
    """Sanity check: when title/description/transcript all agree (or only
    one carries a value), the conflict logic does NOT fire."""
    parsed = parse_youtube_case_input(load_youtube_fixture("transcript_suspect_agency_date.json"))
    for field in ("incident_date", "agency", "location_city", "defendant_names"):
        assert field not in parsed.segment_conflicts, (
            f"transcript_suspect_agency_date.json should not flag {field} conflict"
        )
        assert f"conflicting_{field}_across_segments" not in parsed.risk_flags


class FixtureMockConnector(SourceConnector):
    """Test-only connector seeded with pre-built SourceRecords."""

    name = "fixture_mock"

    def __init__(self, sources):
        self._sources = list(sources)

    def fetch(self, case_input):
        return iter(self._sources)


def test_conflicting_input_does_not_produce_even_with_corroborating_source():
    """Even with a high-quality identity source, if the YouTube input
    itself carries a conflict, the parser keeps the conflict flag and
    no PRODUCE happens because verified media is still missing."""
    parsed = parse_youtube_case_input(load_youtube_fixture("conflicting_agencies.json"))
    plan = plan_queries_from_youtube_result(parsed)

    # Mock a strong identity source — note: it doesn't reference either of
    # the conflicting agencies, just confirms identity in a generic way.
    court_source = SourceRecord(
        source_id="mock_court_h5",
        url="https://www.courtlistener.com/docket/h5-conflict",
        title="Court records confirm identity",
        snippet="Court records confirm identity for the suspect referenced in this case.",
        raw_text="Court records confirm identity for the suspect referenced in this case.",
        source_type="court_docket",
        source_authority="court",
        source_roles=["identity_source"],
        api_name="courtlistener",
        discovered_via="mock_query",
        retrieved_at="2026-04-30T00:00:00Z",
        case_input_id="h5_conflict_fixture",
        metadata={},
        cost_estimate=0.0,
        confidence_signals={},
        matched_case_fields=["defendant_full_name"],
    )

    connector = FixtureMockConnector(sources=[court_source])
    sources = connector.collect(parsed.case_input)
    result = assemble_weak_input_case_packet(parsed, query_plan=plan, sources=sources)

    # Conflict flag survives all the way to the assembled packet via
    # parsed.risk_flags being included in packet.risk_flags.
    assert "conflicting_agency_across_segments" in result.packet.risk_flags
    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"


def test_conflicting_input_alone_e2e_does_not_produce():
    """Run each conflict fixture alone through the full pipeline — no
    sources at all — and assert verdict is never PRODUCE."""
    for fixture in (
        "conflicting_dates.json",
        "conflicting_agencies.json",
        "conflicting_locations.json",
        "conflicting_defendant_names.json",
    ):
        parsed = parse_youtube_case_input(load_youtube_fixture(fixture))
        plan = plan_queries_from_youtube_result(parsed)
        result = assemble_weak_input_case_packet(parsed, query_plan=plan, sources=[])
        assert result.actionability.verdict != "PRODUCE", f"{fixture}: must not PRODUCE"
        assert result.packet.verified_artifacts == [], f"{fixture}: no VerifiedArtifact"


def test_segment_conflicts_field_is_a_dict_even_when_no_conflict():
    """The dataclass field default is an empty dict, not None — callers
    can iterate without a None check."""
    parsed = parse_youtube_case_input(load_youtube_fixture("transcript_suspect_agency_date.json"))
    assert isinstance(parsed.segment_conflicts, dict)
    # When there's no conflict, the dict is empty.
    assert parsed.segment_conflicts == {}
