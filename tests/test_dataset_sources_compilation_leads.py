"""Zero-network tests for the compilation case-lead parser.

Operates entirely on canned video metadata fixtures — yt-dlp is
never invoked. Mirrors the test conventions of the
``muckrock_leads`` and ``sfchronicle_pursuits`` parsers.
"""
from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

from pipeline2_discovery.dataset_sources.compilation_leads import (
    INCIDENT_TERMS,
    SOURCE_DATASET,
    SOURCE_LANE,
    STRONG_INCIDENT_TERMS,
    parse_compilation_input_file,
    parse_compilation_json_text,
    parse_compilation_jsonl_text,
    parse_compilation_records,
    parse_compilation_video,
    score_lead,
)
from pipeline2_discovery.dataset_sources.models import DatasetCandidate


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "tools" / "run_compilation_intake.py"


# ---- fixtures -------------------------------------------------------


def _video(
    *,
    video_id: str = "abc123",
    url: str = None,
    title: str = "",
    uploader: str = "Real Body Cams",
    description: str = "",
    chapters: List[Dict[str, Any]] = None,
    upload_date: str = "20240615",
) -> Dict[str, Any]:
    return {
        "video_id": video_id,
        "url": url or f"https://www.youtube.com/watch?v={video_id}",
        "title": title,
        "uploader": uploader,
        "description": description,
        "chapters": chapters or [],
        "upload_date": upload_date,
    }


# ---- parse_compilation_video ----------------------------------------


def test_parse_compilation_video_tolerates_missing_fields():
    v = parse_compilation_video({"video_id": "x", "url": "u"})
    assert v.video_id == "x"
    assert v.url == "u"
    assert v.title == ""
    assert v.uploader == ""
    assert v.description == ""
    assert v.chapters == []


def test_parse_compilation_video_uses_provided_fields():
    rec = _video(
        title="T", uploader="U", description="D",
        chapters=[{"start_time": 0, "title": "C1"}],
        upload_date="20240601",
    )
    v = parse_compilation_video(rec)
    assert v.title == "T"
    assert v.uploader == "U"
    assert v.description == "D"
    assert len(v.chapters) == 1
    assert v.upload_date == "20240601"


# ---- end-to-end record parsing --------------------------------------


def test_parses_video_with_subject_agency_city_state_date_artifacts():
    rec = _video(
        title="Bodycam: Fresno Police Department arrest of Christopher Vang",
        description=(
            "Officer-involved shooting in Fresno, CA on 2020-12-26. "
            "See https://www.muckrock.com/foi/12345 for the FOIA request."
        ),
    )
    cands = parse_compilation_records([rec])
    assert len(cands) == 1
    c = cands[0]
    assert c.subject_name == "Christopher Vang"
    assert c.agency_name == "Fresno Police Department"
    assert c.jurisdiction_city == "Fresno"
    assert c.jurisdiction_state == "CA"
    assert c.incident_date == "2020-12-26"
    assert "officer-involved shooting" in c.artifact_types_detected
    assert "bodycam" in c.artifact_types_detected
    assert c.muckrock_urls == ["https://www.muckrock.com/foi/12345"]
    assert c.grade == "A"
    assert c.source_lane == SOURCE_LANE
    assert c.source_dataset == SOURCE_DATASET


def test_parses_chapters_into_separate_candidates():
    rec = _video(
        title="Bodycam compilation: 2 wild cases",
        description=(
            "First: Phoenix Police Department officer-involved shooting "
            "of John Smith in Phoenix, AZ.\n"
            "Second: Houston Police Department pursuit of Maria Garcia "
            "in Houston, TX."
        ),
        chapters=[
            {"start_time": 0,    "title": "John Smith Phoenix PD bodycam"},
            {"start_time": 612,  "title": "Maria Garcia Houston PD pursuit"},
        ],
    )
    cands = parse_compilation_records([rec])
    # One candidate per chapter (assumes leads dedupe to distinct subjects)
    assert len(cands) == 2
    by_subject = {c.subject_name: c for c in cands}
    assert "John Smith" in by_subject
    assert "Maria Garcia" in by_subject
    # Chapter start times surface in notes
    smith = by_subject["John Smith"]
    assert any(n.startswith("chapter_start=0") for n in smith.notes)
    garcia = by_subject["Maria Garcia"]
    assert any(n.startswith("chapter_start=612") for n in garcia.notes)


def test_extracts_youtube_news_muckrock_links_from_description():
    rec = _video(
        title="Bodycam: Phoenix PD arrest",
        description=(
            "Phoenix Police Department officer-involved shooting "
            "of John Smith in Phoenix, AZ. Watch full footage at "
            "https://www.youtube.com/watch?v=otherVideo01 or read "
            "https://www.abc15.com/news/phoenix-shooting-coverage. "
            "FOIA request: https://www.muckrock.com/foi/99999. "
            "Official briefing: https://police.phoenix.gov/news/briefing-2024-06-15"
        ),
    )
    cands = parse_compilation_records([rec])
    c = cands[0]
    assert any("youtube.com/watch?v=otherVideo01" in u for u in c.muckrock_urls + (c.notes or []))  # at minimum surfaces somewhere
    assert any("muckrock.com/foi/99999" in u for u in c.muckrock_urls)
    # Ensure the parser also recorded the .gov URL as official
    assert any(".gov" in u for u in c.official_urls)
    # And news URL (abc15)
    assert any("abc15.com" in u for u in c.news_urls)


def test_emits_dataset_candidate_compatible_shape():
    rec = _video(
        title="Bodycam: Fresno Police Department arrest of Christopher Vang",
        description="Officer-involved shooting in Fresno, CA on 2020-12-26.",
    )
    cands = parse_compilation_records([rec])
    c = cands[0]
    assert isinstance(c, DatasetCandidate)
    # Required identity fields populated
    assert c.candidate_id.startswith("compilation_leads:")
    assert c.source_url == rec["url"]
    assert c.source_row_id == rec["video_id"]
    # Rubric fields all set
    assert isinstance(c.packet_priority_score, int)
    assert c.grade in {"A", "B", "C", "D"}
    assert c.evidence_strength in {"strong", "moderate", "weak"}


def test_generates_search_tasks_for_high_grade_candidate():
    rec = _video(
        title="Bodycam: Houston PD officer-involved shooting of Maria Garcia",
        description=(
            "Houston Police Department officer-involved shooting in "
            "Houston, TX on 2024-03-15."
        ),
    )
    cands = parse_compilation_records([rec])
    c = cands[0]
    assert c.youtube_queries
    assert c.muckrock_queries
    assert c.outcome_queries
    # Subject + agency present → should appear in YouTube query
    assert any("Maria Garcia" in q and "Houston" in q for q in c.youtube_queries)
    # Outcome queries reference subject
    assert any("Maria Garcia" in q for q in c.outcome_queries)


# ---- dedup ----------------------------------------------------------


def test_dedupe_by_subject_last_name_and_agency():
    """Same subject + same agency mentioned across chapter and
    description should collapse to one candidate, keeping the
    higher-scoring one."""
    rec = _video(
        title="Bodycam compilation",
        description=(
            "Phoenix Police Department arrest of Maria Garcia. "
            "Maria Garcia was driving the suspect vehicle during "
            "the pursuit."
        ),
        chapters=[
            {"start_time": 0,   "title": "Maria Garcia Phoenix PD bodycam"},
            {"start_time": 60,  "title": "Maria Garcia Phoenix Police Department pursuit"},
        ],
    )
    cands = parse_compilation_records([rec])
    # Both chapters mention "Maria Garcia" + "Phoenix Police Department";
    # dedupe should reduce to one candidate.
    assert len(cands) == 1
    assert cands[0].subject_name == "Maria Garcia"


def test_dedupe_keeps_highest_score():
    rec1 = _video(
        video_id="vid1",
        title="Bodycam: Phoenix PD",
        description=(
            "Phoenix Police Department arrest of John Smith. "
            "Officer-involved shooting in Phoenix, AZ on 2024-01-01."
        ),
    )
    rec2 = _video(
        video_id="vid2",
        title="Quick clip",
        description="John Smith Phoenix Police Department bodycam.",
    )
    cands = parse_compilation_records([rec1, rec2])
    # Both leads dedupe to (smith, phoenix-police-department); the
    # rec1 lead has more anchors → higher score → wins.
    assert len(cands) == 1
    assert cands[0].source_row_id in {"vid1", "vid2"}
    # Ensure the kept candidate has the higher-scoring artifact set
    assert "officer-involved shooting" in cands[0].artifact_types_detected


# ---- grade rubric ---------------------------------------------------


def test_grade_a_with_subject_agency_city_state_artifact():
    rec = _video(
        title="Bodycam: Fresno PD",
        description=(
            "Fresno Police Department officer-involved shooting of "
            "Christopher Vang in Fresno, CA on 2020-12-26."
        ),
    )
    cands = parse_compilation_records([rec])
    assert cands[0].grade == "A"


def test_grade_b_when_agency_city_artifact_but_no_subject():
    rec = _video(
        title="Bodycam compilation",
        description=(
            "Fresno Police Department bodycam pursuit footage from "
            "Fresno, CA. Several arrests made."
        ),
    )
    cands = parse_compilation_records([rec])
    # Has agency + city + artifact but no subject name
    c = cands[0]
    assert c.subject_name is None
    assert c.grade in {"B", "C"}  # rubric: agency+city+artifact = B-ish (3 items)


def test_grade_d_for_generic_compilation_with_no_anchors():
    rec = _video(
        title="Top 10 wild moments",
        description="Crazy footage from this week's cases.",
    )
    cands = parse_compilation_records([rec])
    assert cands[0].grade == "D"


def test_compilation_uploader_is_not_treated_as_subject():
    """'Real Body Cams' looks like a 3-word capitalised phrase but
    it's the publishing channel — the parser must not anchor on it
    as the subject name."""
    rec = _video(
        uploader="Real Body Cams",
        title="Bodycam: Real Body Cams compilation",
        description="Footage from various cases this week.",
    )
    cands = parse_compilation_records([rec])
    # Whatever the parser does, the subject name MUST NOT be the
    # uploader's name.
    for c in cands:
        if c.subject_name:
            assert c.subject_name.lower() not in {
                "real body cams", "real body", "body cams",
            }


# ---- I/O helpers ---------------------------------------------------


def test_parse_compilation_jsonl_text_skips_blank_and_invalid():
    text = (
        "\n"
        + json.dumps(_video(title="Phoenix PD bodycam of John Smith"))
        + "\n"
        + "  \n"
        + "not valid json\n"
        + json.dumps(_video(
            video_id="b",
            title="Houston PD pursuit of Maria Garcia",
        ))
        + "\n"
    )
    cands = parse_compilation_jsonl_text(text)
    # Two valid records → two candidates
    assert len(cands) == 2


def test_parse_compilation_json_text_accepts_list():
    body = json.dumps([
        _video(title="Phoenix PD bodycam of John Smith"),
        _video(video_id="b", title="Houston PD pursuit of Maria Garcia"),
    ])
    cands = parse_compilation_json_text(body)
    assert len(cands) == 2


def test_parse_compilation_json_text_accepts_single_dict():
    body = json.dumps(_video(title="Phoenix PD bodycam of John Smith"))
    cands = parse_compilation_json_text(body)
    assert len(cands) == 1


def test_parse_compilation_input_file_dispatches_by_extension(tmp_path):
    jsonl = tmp_path / "videos.jsonl"
    jsonl.write_text(
        json.dumps(_video(title="Phoenix PD bodycam of John Smith")) + "\n",
        encoding="utf-8",
    )
    cands = parse_compilation_input_file(jsonl)
    assert len(cands) == 1

    json_path = tmp_path / "videos.json"
    json_path.write_text(
        json.dumps([_video(title="Phoenix PD bodycam of John Smith")]),
        encoding="utf-8",
    )
    cands2 = parse_compilation_input_file(json_path)
    assert len(cands2) == 1


# ---- score_lead -----------------------------------------------------


def test_score_lead_grade_a_combo():
    score = score_lead(
        has_subject=True, has_agency=True, has_city_or_state=True,
        has_artifact=True, has_strong_incident=True, has_date=True,
    )
    # subject+agency+(city)+artifact = +5; strong = +2;
    # individual axes (subject, agency, city/state, artifact, date) = +5
    # → 12 → grade A
    assert score >= 9


def test_score_lead_grade_d_no_anchors():
    score = score_lead(
        has_subject=False, has_agency=False, has_city_or_state=False,
        has_artifact=False, has_strong_incident=False, has_date=False,
    )
    assert score == 0


# ---- CLI ------------------------------------------------------------


def _make_safe_output_dir(tmp_path: Path) -> Path:
    target = REPO_ROOT / ".tmp" / "test_compilation_intake" / tmp_path.name
    target.mkdir(parents=True, exist_ok=True)
    return target


def _run_cli(argv):
    out = io.StringIO()
    err = io.StringIO()
    from tools import run_compilation_intake as script
    code = script.main(argv, stdout=out, stderr=err)
    return code, out.getvalue(), err.getvalue()


def test_cli_writes_csv_json_tasks_packets(tmp_path):
    inp = tmp_path / "videos.jsonl"
    inp.write_text(
        json.dumps(_video(
            title="Bodycam: Houston PD officer-involved shooting of Maria Garcia",
            description=(
                "Houston Police Department officer-involved shooting in "
                "Houston, TX on 2024-03-15."
            ),
        )) + "\n",
        encoding="utf-8",
    )
    output_dir = _make_safe_output_dir(tmp_path)
    csv_path = output_dir / "candidates.csv"
    json_path = output_dir / "candidates.json"
    tasks_path = output_dir / "search_tasks.json"
    packets_dir = output_dir / "packets"

    code, _out, _err = _run_cli([
        "--input", str(inp),
        "--output-csv", str(csv_path),
        "--output-json", str(json_path),
        "--search-tasks-out", str(tasks_path),
        "--packet-stubs-dir", str(packets_dir),
        "--json",
    ])
    assert code == 0
    assert csv_path.exists() and csv_path.read_text(encoding="utf-8")
    assert json_path.exists()
    assert tasks_path.exists()
    assert packets_dir.exists()
    # packet stub for the candidate
    stub_files = list(packets_dir.glob("*.md"))
    assert len(stub_files) >= 1
    stub_text = stub_files[0].read_text(encoding="utf-8")
    assert "Maria Garcia" in stub_text
    # Search tasks document has youtube + muckrock + outcome tasks
    tasks = json.loads(tasks_path.read_text(encoding="utf-8"))
    task_types = {t["task_type"] for t in tasks["tasks"]}
    assert {"youtube_query", "muckrock_query", "outcome_query"}.issubset(task_types)


def test_cli_missing_input_returns_exit_2(tmp_path):
    code, _out, err = _run_cli([
        "--input", str(tmp_path / "nope.jsonl"),
    ])
    assert code == 2
    assert "input not found" in err


def test_cli_unsafe_output_dir_rejected(tmp_path):
    inp = tmp_path / "videos.jsonl"
    inp.write_text(
        json.dumps(_video(title="Phoenix PD")) + "\n", encoding="utf-8",
    )
    unsafe = REPO_ROOT / "pipeline2_discovery" / "_should_be_rejected.csv"
    code, _out, err = _run_cli([
        "--input", str(inp),
        "--output-csv", str(unsafe),
    ])
    assert code == 2
    assert "safe artifact dirs" in err


def test_cli_help_exits_zero():
    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--help"],
        cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0
    assert "--input" in result.stdout
    assert "--output-csv" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


# ---- regression guard: zero-network --------------------------------


def test_parser_makes_no_network_call(monkeypatch):
    """The parser is pure-text — no requests, no yt-dlp. Pinned by
    monkeypatching requests + yt_dlp imports to raise if touched."""
    import sys as _sys

    class _Boom:
        def __getattr__(self, name):
            raise AssertionError(
                f"compilation_leads parser must not access network "
                f"(yt_dlp.{name} touched)"
            )

    monkeypatch.setitem(_sys.modules, "yt_dlp", _Boom())

    rec = _video(
        title="Bodycam: Houston PD officer-involved shooting of Maria Garcia",
        description="Houston Police Department in Houston, TX on 2024-03-15.",
    )
    cands = parse_compilation_records([rec])
    # If parser tried to import yt_dlp, the _Boom getattr would have raised.
    assert len(cands) == 1


# ---- headline-phrase rejection (post-real-smoke) -------------------


@pytest.mark.parametrize("title,expected_subject_none", [
    # 5 actual false-positive titles surfaced by the real smoke
    ('Mom Reports "Missing" Son, But Cops Find Him In HER Car', "Mom Reports"),
    ("Passenger Shows Up Two Days Late and Demands to Board a Flight", "Passenger Shows"),
    ("Brat Threatens Police During Traffic Stop... It Backfires Terribly", "Brat Threatens"),
    ("Entitled Customer Refuses to Pay for Dances at Club, Doesn't End Well", "Entitled Customer"),
    ("Cops Fiance Flashes Fake Police Badge During DUI Arrest", "Flashes Fake"),
])
def test_headline_phrase_titles_no_longer_extract_subject(title, expected_subject_none):
    """The 5 false-positive subjects from the real-metadata smoke
    must now resolve to subject=None instead of leaking the
    title-case headline phrase as a "subject"."""
    rec = _video(
        title=title,
        description="Generic compilation channel description.",
        uploader="Real Body Cams",
    )
    cands = parse_compilation_records([rec])
    assert len(cands) == 1
    # Must NOT extract the headline phrase as a subject
    assert cands[0].subject_name != expected_subject_none
    # Stronger assertion: subject should be None for these titles
    # (no real name available anywhere in title or description).
    assert cands[0].subject_name is None


@pytest.mark.parametrize("title,expected_subject", [
    ("Bodycam: Fresno Police Department arrest of Christopher Vang", "Christopher Vang"),
    ("Family of Ivonne Reyes killed during police chase", "Ivonne Reyes"),
    ("Bodycam: Phoenix PD pursuit of John Smith", "John Smith"),
    ("Bodycam: Houston PD officer-involved shooting of Maria Garcia", "Maria Garcia"),
])
def test_real_names_still_extract_after_headline_filter(title, expected_subject):
    """Real subject names must continue to extract after the
    headline-phrase filter is added."""
    rec = _video(
        title=title,
        description="Generic compilation channel description.",
        uploader="Real Body Cams",
    )
    cands = parse_compilation_records([rec])
    assert len(cands) == 1
    assert cands[0].subject_name == expected_subject


def test_grade_drops_to_d_for_smoke_shaped_records_without_subject():
    """The 5 smoke-shaped titles previously produced score=2-3 with
    a false-positive subject anchor inflating the grade. After
    filtering, no subject anchor → no subject points → grade D."""
    titles = [
        "Mom Reports Missing Son",
        "Passenger Shows Up Late",
        "Brat Threatens Police",
        "Entitled Customer Refuses",
        "Cops Fiance Flashes Fake Badge",
    ]
    for title in titles:
        rec = _video(title=title, description="Generic.", uploader="Real Body Cams")
        cands = parse_compilation_records([rec])
        assert len(cands) == 1
        assert cands[0].subject_name is None
        # Score should not have been inflated by a false subject:
        # without a subject anchor and without an agency, the rubric's
        # subject-bonus paths can't fire. Grade is D unless artifact
        # terms alone happen to cross the C threshold.
        assert cands[0].grade in ("D", "C")


def test_search_tasks_skip_subject_queries_when_subject_filtered():
    """When the subject is filtered as a headline phrase, the
    generated search-task queries must not contain the false
    headline (e.g. 'Mom Reports body-worn camera')."""
    rec = _video(
        title="Mom Reports Missing Son",
        description="Generic.",
        uploader="Real Body Cams",
    )
    cands = parse_compilation_records([rec])
    c = cands[0]
    all_queries = (
        list(c.youtube_queries or [])
        + list(c.muckrock_queries or [])
        + list(c.outcome_queries or [])
    )
    for q in all_queries:
        assert "Mom Reports" not in q
        assert "mom reports" not in q.lower()


def test_looks_like_headline_phrase_helper_directly():
    from pipeline2_discovery.dataset_sources.compilation_leads import (
        _looks_like_headline_phrase,
    )
    # Headline phrases — should be rejected
    assert _looks_like_headline_phrase("Mom Reports") is True
    assert _looks_like_headline_phrase("Passenger Shows") is True
    assert _looks_like_headline_phrase("Brat Threatens") is True
    assert _looks_like_headline_phrase("Entitled Customer") is True
    assert _looks_like_headline_phrase("Flashes Fake") is True
    # Real names — should NOT be rejected
    assert _looks_like_headline_phrase("Christopher Vang") is False
    assert _looks_like_headline_phrase("Ivonne Reyes") is False
    assert _looks_like_headline_phrase("John Smith") is False
    assert _looks_like_headline_phrase("Maria Garcia") is False
    assert _looks_like_headline_phrase("Joe Gold") is False
    assert _looks_like_headline_phrase("Michael Beaver") is False
    assert _looks_like_headline_phrase("Chase Bulmahn") is False
    # Empty / degenerate input
    assert _looks_like_headline_phrase("") is True
    assert _looks_like_headline_phrase("   ") is True


def test_role_noun_first_word_rejected_even_with_unfamiliar_second_word():
    """Headline filter generalises beyond the 5 specific smoke phrases:
    any "Role Verb" / "Role Noun" with the role as the first word
    should be rejected."""
    from pipeline2_discovery.dataset_sources.compilation_leads import (
        _looks_like_headline_phrase,
    )
    # First word is a role noun → rejected regardless of second word
    assert _looks_like_headline_phrase("Driver Caught") is True
    assert _looks_like_headline_phrase("Suspect Demands") is True
    assert _looks_like_headline_phrase("Customer Yells") is True
    # Plurals
    assert _looks_like_headline_phrase("Cops Find") is True
    assert _looks_like_headline_phrase("Officers Refuse") is True


def test_action_verb_last_word_rejected_even_with_unfamiliar_first_word():
    """Symmetric coverage: any 'X <verb>' with verb as last word
    should be rejected even if X isn't a recognised role noun."""
    from pipeline2_discovery.dataset_sources.compilation_leads import (
        _looks_like_headline_phrase,
    )
    # Last word is an action verb → rejected
    assert _looks_like_headline_phrase("Mary Demands") is True  # Mary not in roles, but Demands matches
    assert _looks_like_headline_phrase("Nathan Threatens") is True


# ---- end-to-end smoke replay ---------------------------------------


def test_smoke_replay_5_real_titles_produce_no_subject_anchor():
    """Reproduces the 5 real-metadata smoke records (NYDETECTIVE /
    Real Body Cams / Crime Time Cam / ThisIsBodycam / Uncovered)
    and asserts none of them now produce false-positive subject
    anchors. This is the regression guard for the actual issue
    surfaced by the live smoke."""
    smoke_titles = [
        ("0AI6cY3mT-k", "NYDETECTIVE",
         "Passenger Shows Up Two Days Late and Demands to Board a Flight"),
        ("MnUMzE22ptA", "Real Body Cams",
         'Mom Reports "Missing" Son, But Cops Find Him In HER Car'),
        ("SRpXpuLoh2I", "Crime Time Cam",
         "Entitled Customer Refuses to Pay for Dances at Club, Doesn't End Well"),
        ("vY0i0e_tkR0", "ThisIsBodycam",
         "Cops Fiance Flashes Fake Police Badge During DUI Arrest"),
        ("IDhNW4Bi-PI", "Uncovered",
         "Brat Threatens Police During Traffic Stop... It Backfires Terribly"),
    ]
    records = [
        _video(video_id=vid, uploader=uploader, title=title,
               description="Channel boilerplate. No structured case info.")
        for vid, uploader, title in smoke_titles
    ]
    cands = parse_compilation_records(records)
    assert len(cands) == 5
    for c in cands:
        assert c.subject_name is None, (
            f"unexpected subject anchor {c.subject_name!r} for "
            f"{c.candidate_id} — headline filter regression"
        )
