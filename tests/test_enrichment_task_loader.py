"""Zero-network tests for the search_tasks.json loader."""
from __future__ import annotations

import json

import pytest

from pipeline2_discovery.enrichment import (
    load_search_tasks_text,
    parse_task_dict,
)


def _doc(*, tasks):
    return json.dumps({
        "generated_at": "2026-05-05T00:00:00Z",
        "source_lane": "sfchronicle_pursuits",
        "candidate_count": 1,
        "task_count": len(tasks),
        "tasks": tasks,
    })


# ---- happy path ----------------------------------------------------


def test_loader_accepts_well_formed_document():
    text = _doc(tasks=[
        {
            "candidate_id": "x:1",
            "grade": "A",
            "task_type": "youtube_query",
            "query": "q",
            "context": {"agency": "Phoenix PD"},
        },
        {
            "candidate_id": "x:2",
            "grade": "B",
            "task_type": "muckrock_query",
            "query": "q2",
            "context": {},
        },
    ])
    tasks = load_search_tasks_text(text)
    assert len(tasks) == 2
    assert tasks[0].candidate_id == "x:1"
    assert tasks[0].task_type == "youtube_query"
    assert tasks[1].task_type == "muckrock_query"


def test_loader_returns_empty_list_on_empty_text():
    assert load_search_tasks_text("") == []
    assert load_search_tasks_text("   \n  ") == []


def test_loader_accepts_empty_tasks_array():
    assert load_search_tasks_text(_doc(tasks=[])) == []


# ---- shape rejection ------------------------------------------------


def test_loader_rejects_non_object_root():
    with pytest.raises(ValueError, match="must be an object"):
        load_search_tasks_text(json.dumps([{"x": 1}]))


def test_loader_rejects_missing_tasks_key():
    with pytest.raises(ValueError, match="'tasks' key must be a JSON array"):
        load_search_tasks_text(json.dumps({"generated_at": "x"}))


def test_loader_rejects_non_list_tasks():
    with pytest.raises(ValueError, match="'tasks' key must be a JSON array"):
        load_search_tasks_text(json.dumps({"tasks": "not-a-list"}))


# ---- per-row validation --------------------------------------------


def test_parse_task_dict_rejects_missing_required_keys():
    with pytest.raises(ValueError, match="missing required keys"):
        parse_task_dict({"candidate_id": "x:1"}, row_index=0)


def test_parse_task_dict_rejects_empty_required_values():
    with pytest.raises(ValueError, match="missing required keys"):
        parse_task_dict({
            "candidate_id": "x:1",
            "task_type": "youtube_query",
            "query": "   ",  # whitespace-only
        }, row_index=3)


def test_parse_task_dict_rejects_unknown_task_type():
    with pytest.raises(ValueError, match="task_type 'bogus' not in"):
        parse_task_dict({
            "candidate_id": "x:1",
            "task_type": "bogus",
            "query": "q",
        }, row_index=0)


def test_parse_task_dict_rejects_non_dict_row():
    with pytest.raises(ValueError, match=r"must be an object"):
        parse_task_dict("not-a-dict", row_index=0)


def test_loader_surfaces_row_index_in_error_message():
    text = _doc(tasks=[
        {"candidate_id": "x:1", "grade": "A", "task_type": "youtube_query", "query": "q"},
        {"candidate_id": "x:2"},  # malformed
    ])
    with pytest.raises(ValueError, match=r"task row \[1\]"):
        load_search_tasks_text(text)
