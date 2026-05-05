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
    is_federal_agency,
    rank_key,
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


# ---- is_federal_agency ----------------------------------------------


@pytest.mark.parametrize(
    "agency,expected",
    [
        ("U.S. Border Patrol", True),
        ("Border Patrol", True),
        ("Customs and Border Protection", True),
        ("CBP", True),
        ("FBI", True),
        ("Drug Enforcement Administration", True),
        ("DEA", True),
        ("ATF", True),
        ("Department of Homeland Security", True),
        ("U.S. Marshals Service", True),
        ("Secret Service", True),
        # case-insensitive
        ("u.s. border patrol", True),
        # negatives
        ("Phoenix Police Department", False),
        ("Maricopa County Sheriff's Office", False),
        ("California Highway Patrol", False),
        ("Ohio State Highway Patrol", False),
        # corner cases
        ("", False),
        (None, False),
    ],
)
def test_is_federal_agency_matches(agency, expected):
    assert is_federal_agency(agency) is expected


# ---- rank_key tie-breaks --------------------------------------------


def _c(**kwargs) -> DatasetCandidate:
    """Build a minimal DatasetCandidate for rank-key tests."""
    base = dict(
        candidate_id=kwargs.pop("candidate_id", "x:1"),
        source_lane="x",
        source_dataset="x",
        source_row_id="1",
    )
    base.update(kwargs)
    return DatasetCandidate(**base)


def test_rank_key_primary_is_score_descending():
    high = _c(candidate_id="x:1", packet_priority_score=20)
    low = _c(candidate_id="x:2", packet_priority_score=5)
    ordered = sorted([low, high], key=rank_key)
    assert [c.candidate_id for c in ordered] == ["x:1", "x:2"]


def test_rank_key_breaks_score_tie_with_real_name_first():
    placeholder = _c(
        candidate_id="x:1",
        packet_priority_score=20,
        subject_name=None,
    )
    real = _c(
        candidate_id="x:2",
        packet_priority_score=20,
        subject_name="Jane Doe",
    )
    ordered = sorted([placeholder, real], key=rank_key)
    # real name beats no-name, even though placeholder has lower id.
    assert [c.candidate_id for c in ordered] == ["x:2", "x:1"]


def test_rank_key_breaks_tie_with_news_count_descending():
    one = _c(candidate_id="x:1", packet_priority_score=20,
             subject_name="A", news_urls=["https://a"])
    three = _c(candidate_id="x:2", packet_priority_score=20,
               subject_name="B", news_urls=["https://a", "https://b", "https://c"])
    ordered = sorted([one, three], key=rank_key)
    assert [c.candidate_id for c in ordered] == ["x:2", "x:1"]


def test_rank_key_breaks_tie_with_fatality_count_descending():
    less = _c(candidate_id="x:1", packet_priority_score=20,
              subject_name="A", fatality_count=2)
    more = _c(candidate_id="x:2", packet_priority_score=20,
              subject_name="A", fatality_count=5)
    ordered = sorted([less, more], key=rank_key)
    assert [c.candidate_id for c in ordered] == ["x:2", "x:1"]


def test_rank_key_breaks_tie_with_recent_date_first():
    older = _c(candidate_id="x:1", packet_priority_score=20,
               subject_name="A", incident_date="2020-01-01")
    newer = _c(candidate_id="x:2", packet_priority_score=20,
               subject_name="A", incident_date="2024-12-31")
    none = _c(candidate_id="x:3", packet_priority_score=20,
              subject_name="A", incident_date=None)
    ordered = sorted([older, newer, none], key=rank_key)
    assert [c.candidate_id for c in ordered] == ["x:2", "x:1", "x:3"]


def test_rank_key_breaks_tie_with_non_federal_first():
    federal = _c(candidate_id="x:1", packet_priority_score=20,
                 subject_name="A", agency_name="U.S. Border Patrol")
    municipal = _c(candidate_id="x:2", packet_priority_score=20,
                   subject_name="A", agency_name="Phoenix Police Department")
    ordered = sorted([federal, municipal], key=rank_key)
    assert [c.candidate_id for c in ordered] == ["x:2", "x:1"]


def test_rank_key_breaks_tie_with_target_state_first():
    non_target = _c(candidate_id="x:1", packet_priority_score=20,
                    subject_name="A", jurisdiction_state="WY")
    target = _c(candidate_id="x:2", packet_priority_score=20,
                subject_name="A", jurisdiction_state="AZ")
    ordered = sorted([non_target, target],
                     key=lambda c: rank_key(c, target_states=("AZ",)))
    assert [c.candidate_id for c in ordered] == ["x:2", "x:1"]


def test_rank_key_falls_back_to_candidate_id_when_all_else_equal():
    a = _c(candidate_id="x:0001", packet_priority_score=20, subject_name="A")
    b = _c(candidate_id="x:0002", packet_priority_score=20, subject_name="A")
    ordered = sorted([b, a], key=rank_key)
    assert [c.candidate_id for c in ordered] == ["x:0001", "x:0002"]
