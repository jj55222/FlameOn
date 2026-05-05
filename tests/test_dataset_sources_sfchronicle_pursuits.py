"""Zero-network tests for the SF Chronicle pursuits parser.

Includes the exact 5-row spec fixture: high-priority bystander/passenger
fatality, traffic-stop named row, agency-missing-but-news-present,
low-info, non-target-state."""
from __future__ import annotations

import pytest

from pipeline2_discovery.dataset_sources import sfchronicle_pursuits as sfc
from pipeline2_discovery.dataset_sources.models import NextActionHint


# Hand-built CSV that hits every grading branch.
SAMPLE_CSV = """\
date,year,number_killed,name,initial_reason,person_role,main_agency,news_urls,city,county,state,in_fars_pursuit
2025-04-12,2025,2,John Doe,traffic stop,bystander,Phoenix Police Department,https://example.com/news/john-doe; https://example.com/secondary,Phoenix,Maricopa,AZ,1
2024-11-05,2024,1,Jane Smith,traffic stop,driver,Mesa Police Department,https://example.com/news/jane,Mesa,Maricopa,AZ,0
2025-03-01,2025,1,Pat Roe,minor/no crime,passenger,,https://example.com/news/pat,Tampa,Hillsborough,FL,1
2024-12-20,2024,1,,warrant,driver,Some Sheriff,,Anywhere,Anywhere,WY,
2025-07-04,2025,1,Alex Doe,suspected nonviolent,bystander,Boise Police,https://example.com/news/alex,Boise,Ada,ID,1
"""


@pytest.fixture
def candidates():
    return sfc.parse_csv_text(SAMPLE_CSV)


# ---- spec-row 1: bystander + multi-fatality + AZ + agency + news ----


def test_row_1_high_priority_bystander_fatality_grade_A(candidates):
    c = candidates[0]
    assert c.subject_name == "John Doe"
    assert c.person_role == "bystander"
    assert c.fatality_count == 2
    assert c.agency_name == "Phoenix Police Department"
    assert c.jurisdiction_state == "AZ"
    assert c.incident_date == "2025-04-12"
    # news_urls split correctly from "; "-separated input
    assert c.news_urls == [
        "https://example.com/news/john-doe",
        "https://example.com/secondary",
    ]
    # Score breakdown:
    # +3 name +3 news +3 bystander +3 >1 killed +2 agency
    # +2 traffic stop +2 AZ in target +1 city/county +1 in_fars
    # = 20
    assert c.packet_priority_score == 20
    assert c.grade == "A"
    assert c.evidence_strength == "strong"
    assert "in_fars_pursuit=1 (FARS reconciled)" in c.notes
    assert "news_article" in c.artifact_types_detected


# ---- spec-row 2: traffic-stop named driver, AZ ----------------------


def test_row_2_traffic_stop_named_driver(candidates):
    c = candidates[1]
    assert c.subject_name == "Jane Smith"
    assert c.person_role == "driver"
    assert c.agency_name == "Mesa Police Department"
    # Score:
    # +3 name +3 news +0 (driver not in {bystander,passenger,officer})
    # +0 (1 killed) +2 agency +2 traffic stop +2 AZ
    # +1 city/county +0 (in_fars=0)
    # = 13
    assert c.packet_priority_score == 13
    assert c.grade == "A"


# ---- spec-row 3: agency missing but news present + passenger + FL ----


def test_row_3_agency_missing_but_news_and_passenger(candidates):
    c = candidates[2]
    assert c.subject_name == "Pat Roe"
    assert c.agency_name is None
    assert c.person_role == "passenger"
    assert c.jurisdiction_state == "FL"
    # Score:
    # +3 name +3 news +3 passenger +0 (1 killed)
    # +0 (no agency) +2 minor/no crime +2 FL +1 city/county +1 in_fars
    # -3 missing agency AND missing news? news IS present, so NO penalty
    # = 15
    assert c.packet_priority_score == 15
    assert c.grade == "A"


# ---- spec-row 4: low-info row (no name, no news, no agency) ---------


def test_row_4_low_info_row_penalised(candidates):
    c = candidates[3]
    assert c.subject_name is None
    assert c.news_urls == []
    # Score:
    # +0 name +0 news +0 (driver) +0 (1 killed) +2 agency
    # +0 (warrant not in low-pretext) +0 (WY not target) +1 city/county
    # +0 (in_fars="" → None)
    # -3 missing agency? agency present, so NO. -2 missing name AND missing news → YES
    # net: 2 + 1 - 2 = 1
    assert c.packet_priority_score == 1
    assert c.grade == "D"
    assert c.evidence_strength == "weak"


# ---- spec-row 5: non-target state (ID) ------------------------------


def test_row_5_non_target_state_no_state_bonus(candidates):
    c = candidates[4]
    assert c.subject_name == "Alex Doe"
    assert c.jurisdiction_state == "ID"
    # ID not in default target_states → no +2 state bonus
    # +3 name +3 news +3 bystander +0 +2 agency +2 nonviolent +0
    # +1 city/county +1 in_fars
    # = 15
    assert c.packet_priority_score == 15


def test_target_states_override_changes_score():
    """Explicit target_states must change the bonus."""
    rows = sfc.parse_csv_text(SAMPLE_CSV, target_states=("ID",))
    # row 5 (Alex Doe, ID) now scores +2 more than the default run.
    assert rows[4].packet_priority_score == 17
    # row 1 (Phoenix, AZ) loses its +2.
    assert rows[0].packet_priority_score == 18


# ---- next-action hints -----------------------------------------------


def test_high_priority_row_emits_artifact_search_and_outcome_validate(candidates):
    c = candidates[0]
    # No official_urls / muckrock_urls on a Chronicle row → no
    # PORTAL_LIVE_VALIDATE / MUCKROCK_PARSE_RELEASED_FILES hint.
    # YouTube queries are emitted → YOUTUBE_METADATA_TRANSCRIPT.
    # Subject + agency present, no artifact URL → ARTIFACT_SEARCH.
    # outcome_validation_needed default True → OUTCOME_VALIDATE.
    assert NextActionHint.YOUTUBE_METADATA_TRANSCRIPT in c.next_actions_hint
    assert NextActionHint.ARTIFACT_SEARCH in c.next_actions_hint
    assert NextActionHint.OUTCOME_VALIDATE in c.next_actions_hint
    assert NextActionHint.PORTAL_LIVE_VALIDATE not in c.next_actions_hint
    assert NextActionHint.MUCKROCK_PARSE_RELEASED_FILES not in c.next_actions_hint


# ---- search task seeds ----------------------------------------------


def test_search_task_seeds_are_populated_for_named_agency_row(candidates):
    c = candidates[0]
    assert any("John Doe" in q and "bodycam" in q for q in c.youtube_queries)
    assert any(
        "Phoenix Police Department" in q for q in c.muckrock_queries
    )
    assert any(
        "press release" in q for q in c.official_source_queries
    )
    assert any("charged" in q for q in c.outcome_queries)


def test_search_task_seeds_empty_when_name_and_agency_missing(candidates):
    c = candidates[3]  # name=None, agency=Some Sheriff
    # Agency present but no name → some queries still emit.
    # Name absent → outcome queries about "<name> charged" don't fire.
    assert all("None" not in q for q in c.outcome_queries)


# ---- date normalisation ---------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Original formats — must keep working.
        ("2025-04-12", "2025-04-12"),
        ("4/12/2025", "2025-04-12"),
        ("12/5/2024", "2024-12-05"),
        ("not a date", None),
        ("", None),
        # 2-digit US years: 00-49 -> 20xx, 50-99 -> 19xx.
        ("12/26/20", "2020-12-26"),
        ("4/12/25", "2025-04-12"),
        ("1/1/00", "2000-01-01"),
        ("12/31/49", "2049-12-31"),
        ("1/1/50", "1950-01-01"),
        ("12/31/99", "1999-12-31"),
        # Textual months — full + abbreviated, comma + period + spaces.
        ("December 26, 2020", "2020-12-26"),
        ("Dec 26, 2020", "2020-12-26"),
        ("Dec. 26, 2020", "2020-12-26"),
        ("Dec 26 2020", "2020-12-26"),
        ("September 1, 2025", "2025-09-01"),
        ("Sept. 1, 2025", "2025-09-01"),
        ("January 1, 1999", "1999-01-01"),
        # Ordinals tolerated.
        ("Dec 26th, 2020", "2020-12-26"),
        ("December 1st, 2025", "2025-12-01"),
        # Bad textual months reject cleanly.
        ("Smarch 26, 2020", None),
        # Garbage rejects.
        ("Dec 99, 2020", None),
        ("13/45/2020", None),
    ],
)
def test_date_normalisation(raw, expected):
    assert sfc._normalize_date(raw) == expected


# ---- placeholder-name handling --------------------------------------


@pytest.mark.parametrize(
    "raw_name",
    ["name withheld", "name unknown", "withheld", "unknown",
     "n/a", "n.a.", "not released", "not given", "unidentified",
     "Name Withheld", "  WITHHELD  "],
)
def test_placeholder_name_loses_name_bonus(raw_name):
    """Placeholder names must NOT earn the +3 name bonus and the
    extracted candidate's subject_name must be None."""
    csv_text = (
        "date,year,number_killed,name,initial_reason,person_role,"
        "main_agency,news_urls,city,county,state,in_fars_pursuit\n"
        f"2025-01-01,2025,2,{raw_name},traffic stop,bystander,"
        "Phoenix Police Department,https://x.com/news,Phoenix,Maricopa,AZ,1\n"
    )
    candidates = sfc.parse_csv_text(csv_text)
    c = candidates[0]
    # subject_name is suppressed.
    assert c.subject_name is None
    # The +3 name bonus is suppressed; everything else still scores.
    # Real-name version of this row scores 20; without the +3 bonus it scores 17.
    assert c.packet_priority_score == 17
    # Note carries the original raw value for traceability.
    assert any("placeholder_name_suppressed=" in n for n in c.notes)


def test_placeholder_name_does_not_generate_name_bearing_queries():
    """When subject_name is suppressed, the search-task seeds that
    join name + agency must NOT fire."""
    csv_text = (
        "date,year,number_killed,name,initial_reason,person_role,"
        "main_agency,news_urls,city,county,state,in_fars_pursuit\n"
        "2025-01-01,2025,2,name withheld,traffic stop,bystander,"
        "Phoenix Police Department,https://x.com/news,Phoenix,Maricopa,AZ,1\n"
    )
    c = sfc.parse_csv_text(csv_text)[0]
    # outcome queries are name-anchored — should be empty.
    assert c.outcome_queries == []
    # YouTube queries that need name (first two) shouldn't fire.
    # The name-less city/agency queries can still fire.
    assert all("name withheld" not in q for q in c.youtube_queries)
    assert all("name withheld" not in q for q in c.muckrock_queries)
    assert all("name withheld" not in q for q in c.official_source_queries)


def test_real_name_still_earns_bonus_unchanged():
    """Regression: non-placeholder names work exactly as before."""
    csv_text = (
        "date,year,number_killed,name,initial_reason,person_role,"
        "main_agency,news_urls,city,county,state,in_fars_pursuit\n"
        "2025-04-12,2025,2,John Doe,traffic stop,bystander,"
        "Phoenix Police Department,https://x.com/news,Phoenix,Maricopa,AZ,1\n"
    )
    c = sfc.parse_csv_text(csv_text)[0]
    assert c.subject_name == "John Doe"
    assert c.packet_priority_score == 20


# ---- news_urls split -------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("", []),
        ("https://x.com/a", ["https://x.com/a"]),
        (
            "https://x.com/a; https://y.com/b",
            ["https://x.com/a", "https://y.com/b"],
        ),
        (
            "https://x.com/a\nhttps://y.com/b",
            ["https://x.com/a", "https://y.com/b"],
        ),
        # dedup preserves order
        (
            "https://x.com/a; https://x.com/a; https://y.com/b",
            ["https://x.com/a", "https://y.com/b"],
        ),
        # bare protocol-less strings rejected
        ("not a url; https://x.com/a", ["https://x.com/a"]),
    ],
)
def test_news_urls_split(raw, expected):
    assert sfc._split_news_urls(raw) == expected


# ---- zero-network ---------------------------------------------------


def test_parser_makes_zero_network_calls(monkeypatch):
    import requests

    def fail_get(self, *args, **kwargs):
        raise AssertionError("sfchronicle parser must never touch the network")

    monkeypatch.setattr(requests.Session, "get", fail_get)
    sfc.parse_csv_text(SAMPLE_CSV)
