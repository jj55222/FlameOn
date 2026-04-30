"""H6-lite — YouTube transcript artifact portfolio extraction.

The YouTube parser detects artifact-type mentions across title,
description, and transcript. H6-lite expands the synonym vocabulary
for each category and adds a per-category audit trail
(`artifact_portfolio`) showing exactly which substrings triggered each
signal.

Hard rule: detected signals are claims/leads only — NEVER
VerifiedArtifacts. The parser does not download, does not fetch
transcripts live, does not call any LLM.

Categories covered (all were already there; H6 widens the synonym set):
- bodycam (incl. BWC footage, BWC video, officer-worn camera, body-worn camera)
- dashcam (incl. in-car video, in-car camera, patrol car video, cruiser cam)
- dispatch_911 (incl. dispatch call, emergency dispatch, radio traffic)
- interrogation (incl. interview footage, confession video, custodial interview, questioning)
- surveillance_video (incl. CCTV footage, security camera footage, store surveillance)
- court_video (incl. trial footage, courtroom footage, hearing video,
  sentencing video, sentencing hearing video, arraignment video)
- critical_incident_video (incl. OIS video, officer-involved shooting video)
"""
import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    assemble_weak_input_case_packet,
    parse_youtube_case_input,
    plan_queries_from_youtube_result,
)


YOUTUBE_FIXTURES = Path(__file__).resolve().parent / "fixtures" / "youtube_inputs"


def load_youtube_fixture(name):
    with (YOUTUBE_FIXTURES / name).open("r", encoding="utf-8") as f:
        return json.load(f)


def parse_payload(payload):
    return parse_youtube_case_input(payload)


def parse_text(text):
    """Convenience helper for inline test payloads."""
    return parse_youtube_case_input({"title": "", "description": "", "transcript": text})


# (snippet to embed, expected category, expected substring inside
# artifact_portfolio[category]).
EXPANDED_VOCABULARY = [
    pytest.param("Officer activated his BWC footage during the stop.", "bodycam", "bwc footage", id="bodycam_bwc_footage"),
    pytest.param("The agency uploaded the BWC video days later.", "bodycam", "bwc video", id="bodycam_bwc_video"),
    pytest.param("Investigators referenced the officer-worn camera output.", "bodycam", "officer-worn camera", id="bodycam_officer_worn_camera"),
    pytest.param("The patrol car video shows the pursuit.", "dashcam", "patrol car video", id="dashcam_patrol_car"),
    pytest.param("In-car video has been preserved.", "dashcam", "in-car video", id="dashcam_in_car_video"),
    pytest.param("The cruiser cam captured the stop.", "dashcam", "cruiser cam", id="dashcam_cruiser_cam"),
    pytest.param("The dispatch call lasted nine minutes.", "dispatch_911", "dispatch call", id="dispatch_dispatch_call"),
    pytest.param("Emergency dispatch logs are attached.", "dispatch_911", "emergency dispatch", id="dispatch_emergency_dispatch"),
    pytest.param("Radio traffic captured the stop.", "dispatch_911", "radio traffic", id="dispatch_radio_traffic"),
    pytest.param("The detectives released interview footage.", "interrogation", "interview footage", id="interrogation_interview_footage"),
    pytest.param("A confession video was admitted at trial.", "interrogation", "confession video", id="interrogation_confession_video"),
    pytest.param("Custodial interview transcripts are sealed.", "interrogation", "custodial interview", id="interrogation_custodial_interview"),
    pytest.param("Store surveillance shows the encounter.", "surveillance_video", "store surveillance", id="surveillance_store"),
    pytest.param("CCTV footage was reviewed by investigators.", "surveillance_video", "cctv footage", id="surveillance_cctv_footage"),
    pytest.param("Security camera footage from the lobby is included.", "surveillance_video", "security camera footage", id="surveillance_security_camera_footage"),
    pytest.param("Trial footage was published to the docket.", "court_video", "trial footage", id="court_trial_footage"),
    pytest.param("The courtroom footage from yesterday is online.", "court_video", "courtroom footage", id="court_courtroom_footage"),
    pytest.param("Sentencing hearing video was livestreamed.", "court_video", "sentencing hearing video", id="court_sentencing_hearing_video"),
    pytest.param("The arraignment video runs five minutes.", "court_video", "arraignment video", id="court_arraignment"),
    pytest.param("OIS video was filed by the agency.", "critical_incident_video", "ois video", id="critical_ois_video"),
    pytest.param("Officer-involved shooting video has been released.", "critical_incident_video", "officer-involved shooting video", id="critical_ois_long"),
]


@pytest.mark.parametrize("snippet, category, expected_substring", EXPANDED_VOCABULARY)
def test_each_expanded_vocabulary_term_produces_a_signal_and_portfolio_entry(
    snippet, category, expected_substring
):
    parsed = parse_text(snippet)
    assert category in parsed.artifact_signals, (
        f"snippet {snippet!r} should produce {category} signal"
    )
    assert category in parsed.artifact_portfolio
    matched = parsed.artifact_portfolio[category]
    assert any(expected_substring in m for m in matched), (
        f"portfolio[{category}] should contain a snippet matching "
        f"{expected_substring!r}; got {matched}"
    )


def test_multi_artifact_transcript_emits_diverse_portfolio():
    parsed = parse_payload(load_youtube_fixture("multi_artifact_transcript.json"))

    expected_categories = {
        "bodycam",
        "dashcam",
        "dispatch_911",
        "interrogation",
        "surveillance_video",
        "court_video",
        "critical_incident_video",
    }
    detected = set(parsed.artifact_signals)
    missing = expected_categories - detected
    assert not missing, f"multi-artifact fixture missed categories: {missing}"

    # Portfolio mirrors signals (same key set).
    assert set(parsed.artifact_portfolio.keys()) == detected

    # Each category has at least one matched snippet.
    for category in detected:
        assert parsed.artifact_portfolio[category], f"{category} has empty portfolio entry"


def test_multi_artifact_signals_emit_diversity_risk_flag():
    parsed = parse_payload(load_youtube_fixture("multi_artifact_transcript.json"))
    assert "multiple_artifact_signals" in parsed.risk_flags


def test_single_artifact_signal_does_not_emit_diversity_flag():
    parsed = parse_text("Officer activated his bodycam during the stop.")
    assert parsed.artifact_signals == ["bodycam"]
    assert "multiple_artifact_signals" not in parsed.risk_flags


def test_no_artifact_signal_means_empty_portfolio_and_no_diversity_flag():
    parsed = parse_text("The narrator does not mention any specific evidence type.")
    assert parsed.artifact_signals == []
    assert parsed.artifact_portfolio == {}
    assert "multiple_artifact_signals" not in parsed.risk_flags


def test_portfolio_signals_never_create_verified_artifacts():
    """Even with a maximally rich artifact transcript, the parser must
    not fabricate VerifiedArtifacts. The full E2E run with no mock
    sources should still HOLD/SKIP."""
    parsed = parse_payload(load_youtube_fixture("multi_artifact_transcript.json"))
    plan = plan_queries_from_youtube_result(parsed)
    result = assemble_weak_input_case_packet(parsed, query_plan=plan, sources=[])

    assert result.packet.verified_artifacts == []
    assert result.actionability.verdict != "PRODUCE"
    # And artifact_signals on the parsed result are claims, not artifacts.
    assert parsed.artifact_signals
    assert not hasattr(parsed, "verified_artifacts")


def test_portfolio_dedupes_repeated_matches_within_a_category():
    """If the same exact substring appears multiple times in the text,
    portfolio[category] should record it once."""
    parsed = parse_text("bodycam bodycam bodycam — bodycam.")
    assert parsed.artifact_portfolio["bodycam"] == ["bodycam"]


def test_portfolio_records_multiple_distinct_synonyms_in_same_category():
    """If different synonyms within the same category appear, portfolio
    should record each distinct match."""
    parsed = parse_text("Officer activated bodycam and the BWC footage was preserved as body-worn camera output.")
    portfolio = parsed.artifact_portfolio.get("bodycam", [])
    # Should have multiple distinct entries (bodycam, bwc footage, body-worn camera).
    assert len(portfolio) >= 2, f"expected multiple distinct bodycam matches; got {portfolio}"


def test_artifact_signals_field_remains_simple_list_of_categories():
    """Backwards compat: artifact_signals stays a list of category names
    (no objects, no dicts) so existing consumers (query planner, scoring)
    continue to work."""
    parsed = parse_payload(load_youtube_fixture("multi_artifact_transcript.json"))
    assert isinstance(parsed.artifact_signals, list)
    for sig in parsed.artifact_signals:
        assert isinstance(sig, str)


def test_artifact_portfolio_keys_subset_of_known_categories():
    """Portfolio keys must come from the closed set of detection
    categories — no surprise fields leak through."""
    known = {
        "bodycam",
        "dashcam",
        "dispatch_911",
        "interrogation",
        "surveillance_video",
        "court_video",
        "critical_incident_video",
    }
    parsed = parse_payload(load_youtube_fixture("multi_artifact_transcript.json"))
    assert set(parsed.artifact_portfolio.keys()) <= known
