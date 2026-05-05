"""Zero-network tests for ``pipeline2_discovery.dataset_sources.models``.

Pins the DatasetCandidate dataclass shape, the grade-from-score
mapping, and the next-action hint codes."""
from __future__ import annotations

import pytest

from pipeline2_discovery.dataset_sources import (
    DATASET_GRADES,
    DatasetCandidate,
    NextActionHint,
    assign_grade,
)
from pipeline2_discovery.dataset_sources.scoring import (
    evidence_strength_from_grade,
)


# ---- DatasetCandidate dataclass -------------------------------------


def test_candidate_minimal_construction_has_safe_defaults():
    c = DatasetCandidate(
        candidate_id="x:1",
        source_lane="x",
        source_dataset="x",
        source_row_id="1",
    )
    # Every list field defaults to an empty list.
    assert c.news_urls == []
    assert c.official_urls == []
    assert c.muckrock_urls == []
    assert c.youtube_queries == []
    assert c.muckrock_queries == []
    assert c.official_source_queries == []
    assert c.outcome_queries == []
    assert c.artifact_types_detected == []
    assert c.next_actions_hint == []
    assert c.notes == []
    assert c.bodycam_likelihood == "unknown"
    assert c.footage_likelihood == "unknown"
    assert c.outcome_validation_needed is True
    assert c.evidence_strength == "weak"
    assert c.packet_priority_score == 0
    assert c.grade == "D"


def test_candidate_to_dict_round_trips_all_fields():
    c = DatasetCandidate(
        candidate_id="x:1",
        source_lane="x",
        source_dataset="x",
        source_row_id="1",
        case_title="A case",
        agency_name="Phoenix Police Department",
        news_urls=["https://example.com/a", "https://example.com/b"],
        packet_priority_score=10,
        grade="A",
    )
    d = c.to_dict()
    assert d["case_title"] == "A case"
    assert d["news_urls"] == ["https://example.com/a", "https://example.com/b"]
    assert d["packet_priority_score"] == 10
    assert d["grade"] == "A"


# ---- assign_grade ---------------------------------------------------


@pytest.mark.parametrize(
    "score,expected",
    [
        (12, "A"),
        (9, "A"),
        (8, "B"),
        (6, "B"),
        (5, "C"),
        (3, "C"),
        (2, "D"),
        (0, "D"),
        (-3, "D"),
    ],
)
def test_assign_grade_thresholds(score, expected):
    assert assign_grade(score) == expected


def test_dataset_grades_constant_is_complete():
    assert set(DATASET_GRADES) == {"A", "B", "C", "D"}


# ---- evidence_strength_from_grade -----------------------------------


@pytest.mark.parametrize(
    "grade,expected",
    [
        ("A", "strong"),
        ("B", "moderate"),
        ("C", "weak"),
        ("D", "weak"),
        ("garbage", "weak"),
    ],
)
def test_evidence_strength_mapping(grade, expected):
    assert evidence_strength_from_grade(grade) == expected


# ---- NextActionHint -------------------------------------------------


def test_next_action_hint_codes_are_complete():
    assert NextActionHint.PORTAL_LIVE_VALIDATE in NextActionHint.ALL
    assert NextActionHint.MUCKROCK_PARSE_RELEASED_FILES in NextActionHint.ALL
    assert NextActionHint.YOUTUBE_METADATA_TRANSCRIPT in NextActionHint.ALL
    assert NextActionHint.ARTIFACT_SEARCH in NextActionHint.ALL
    assert NextActionHint.OUTCOME_VALIDATE in NextActionHint.ALL
    assert len(NextActionHint.ALL) == 5
