"""Zero-network tests for ``pipeline2_discovery.enrichment.models``."""
from __future__ import annotations

import pytest

from pipeline2_discovery.enrichment import (
    EnrichmentResult,
    EnrichmentTask,
    TaskStatus,
    TaskType,
)


# ---- TaskType --------------------------------------------------------


def test_task_type_codes_are_complete():
    expected = {
        "youtube_query",
        "muckrock_query",
        "official_source_query",
        "outcome_query",
    }
    assert set(TaskType.ALL) == expected
    assert TaskType.YOUTUBE_QUERY in TaskType.ALL
    assert TaskType.MUCKROCK_QUERY in TaskType.ALL
    assert TaskType.OFFICIAL_SOURCE_QUERY in TaskType.ALL
    assert TaskType.OUTCOME_QUERY in TaskType.ALL


# ---- TaskStatus ------------------------------------------------------


def test_task_status_codes_are_complete():
    assert set(TaskStatus.ALL) == {"dry_run", "completed", "failed", "skipped"}


# ---- EnrichmentTask --------------------------------------------------


def test_enrichment_task_minimal_fields():
    t = EnrichmentTask(
        candidate_id="x:1",
        grade="A",
        task_type="youtube_query",
        query="q",
    )
    assert t.context == {}


def test_enrichment_task_round_trips_through_dict():
    t = EnrichmentTask(
        candidate_id="x:1",
        grade="A",
        task_type="youtube_query",
        query="John Doe Phoenix Police bodycam",
        context={"agency": "Phoenix PD", "subject_name": "John Doe"},
    )
    again = EnrichmentTask.from_dict(t.to_dict())
    assert again == t


def test_enrichment_task_from_dict_tolerates_extra_keys():
    t = EnrichmentTask.from_dict({
        "candidate_id": "x:1",
        "grade": "A",
        "task_type": "youtube_query",
        "query": "q",
        "context": {"agency": "x"},
        "extra_metadata": "ignored",
    })
    assert t.candidate_id == "x:1"
    assert t.context == {"agency": "x"}


# ---- EnrichmentResult ------------------------------------------------


def test_enrichment_result_minimal_fields():
    r = EnrichmentResult(
        candidate_id="x:1",
        task_type="youtube_query",
        query="q",
        status="dry_run",
        provider="mock",
    )
    assert r.result_urls == []
    assert r.result_titles == []
    assert r.confidence == "unknown"
    assert r.next_actions_hint == []
    assert r.error is None
    assert r.notes == []


def test_enrichment_result_to_dict_serialises_lists():
    r = EnrichmentResult(
        candidate_id="x:1",
        task_type="youtube_query",
        query="q",
        status="completed",
        provider="mock",
        result_urls=["https://a", "https://b"],
        result_titles=["A", "B"],
        confidence="high",
        next_actions_hint=["PORTAL_LIVE_VALIDATE"],
        notes=["a note"],
    )
    d = r.to_dict()
    assert d["status"] == "completed"
    assert d["result_urls"] == ["https://a", "https://b"]
    assert d["next_actions_hint"] == ["PORTAL_LIVE_VALIDATE"]
