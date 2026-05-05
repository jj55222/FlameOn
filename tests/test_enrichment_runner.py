"""Zero-network tests for the enrichment runner (filter + dispatch + aggregate)."""
from __future__ import annotations

import pytest

from pipeline2_discovery.enrichment import (
    EnrichmentResult,
    EnrichmentTask,
    MockProvider,
    TaskStatus,
    filter_tasks,
    run_enrichment_batch,
)


def _t(*, candidate_id="x:1", grade="A",
       task_type="youtube_query", query="q"):
    return EnrichmentTask(
        candidate_id=candidate_id,
        grade=grade,
        task_type=task_type,
        query=query,
        context={"agency": "Phoenix PD"},
    )


# ---- filter_tasks ---------------------------------------------------


def test_filter_with_no_filters_returns_all():
    tasks = [_t(candidate_id="x:1"), _t(candidate_id="x:2")]
    assert filter_tasks(tasks) == tasks


def test_filter_by_task_type():
    tasks = [
        _t(task_type="youtube_query"),
        _t(task_type="muckrock_query"),
        _t(task_type="outcome_query"),
    ]
    out = filter_tasks(tasks, task_types=["youtube_query"])
    assert len(out) == 1
    assert out[0].task_type == "youtube_query"


def test_filter_by_multiple_task_types():
    tasks = [
        _t(task_type="youtube_query"),
        _t(task_type="muckrock_query"),
        _t(task_type="outcome_query"),
    ]
    out = filter_tasks(tasks, task_types=["youtube_query", "outcome_query"])
    assert {t.task_type for t in out} == {"youtube_query", "outcome_query"}


def test_filter_by_candidate_id():
    tasks = [
        _t(candidate_id="x:1"),
        _t(candidate_id="x:2"),
        _t(candidate_id="x:3"),
    ]
    out = filter_tasks(tasks, candidate_ids=["x:2"])
    assert len(out) == 1
    assert out[0].candidate_id == "x:2"


def test_filter_by_grade_case_insensitive():
    tasks = [_t(grade="A"), _t(grade="B"), _t(grade="C")]
    # lowercase 'a' should still match grade A.
    out = filter_tasks(tasks, grades=["a", "B"])
    assert {t.grade for t in out} == {"A", "B"}


def test_filter_combines_dimensions_with_AND():
    tasks = [
        _t(candidate_id="x:1", task_type="youtube_query", grade="A"),
        _t(candidate_id="x:1", task_type="muckrock_query", grade="A"),
        _t(candidate_id="x:2", task_type="youtube_query", grade="A"),
        _t(candidate_id="x:1", task_type="youtube_query", grade="B"),
    ]
    out = filter_tasks(
        tasks,
        candidate_ids=["x:1"],
        task_types=["youtube_query"],
        grades=["A"],
    )
    assert len(out) == 1
    assert out[0].candidate_id == "x:1"
    assert out[0].task_type == "youtube_query"
    assert out[0].grade == "A"


# ---- run_enrichment_batch ------------------------------------------


def test_run_batch_dry_run_does_not_invoke_provider():
    """In dry-run mode, the provider's execute() must NOT be called.
    A provider that raises on execute() should still let the dry-run
    finish cleanly."""
    class ExplodingProvider:
        name = "exploding"
        def execute(self, task):
            raise AssertionError("dry-run must not invoke execute()")

    tasks = [_t(query=f"q{i}") for i in range(3)]
    summary = run_enrichment_batch(
        tasks,
        provider=ExplodingProvider(),
        dry_run=True,
        max_tasks=10,
    )
    assert summary["dry_run"] is True
    assert summary["selected_count"] == 3
    assert summary["attempted_count"] == 3
    assert summary["completed_count"] == 0
    assert summary["failed_count"] == 0
    assert all(r["status"] == TaskStatus.DRY_RUN for r in summary["results"])
    assert all(r["provider"] == "exploding" for r in summary["results"])


def test_run_batch_with_mock_provider_completes_all():
    tasks = [_t(query=f"q{i}") for i in range(3)]
    summary = run_enrichment_batch(
        tasks,
        provider=MockProvider(),
        dry_run=False,
        max_tasks=10,
    )
    assert summary["dry_run"] is False
    assert summary["completed_count"] == 3
    assert summary["failed_count"] == 0
    assert all(r["status"] == TaskStatus.COMPLETED for r in summary["results"])
    assert all(r["result_urls"] for r in summary["results"])


def test_run_batch_caps_at_max_tasks():
    tasks = [_t(candidate_id=f"x:{i}", query=f"q{i}") for i in range(20)]
    summary = run_enrichment_batch(
        tasks,
        provider=MockProvider(),
        dry_run=False,
        max_tasks=5,
    )
    assert summary["selected_count"] == 5
    assert summary["attempted_count"] == 5


def test_run_batch_sorts_deterministically_before_capping():
    """Sort key is (candidate_id, task_type, query). With max_tasks=2,
    the first two by sort order win — not by input order."""
    tasks = [
        _t(candidate_id="x:zzz", task_type="youtube_query", query="z"),
        _t(candidate_id="x:aaa", task_type="muckrock_query", query="a"),
        _t(candidate_id="x:aaa", task_type="youtube_query", query="a"),
    ]
    summary = run_enrichment_batch(
        tasks,
        provider=MockProvider(),
        dry_run=True,
        max_tasks=2,
    )
    # Sort: (x:aaa, muckrock_query, a) > (x:aaa, youtube_query, a) ...
    # Actually candidate_id ascending, then task_type ascending, then query.
    # 'muckrock_query' < 'youtube_query' alphabetically, so:
    selected_ids = [r["candidate_id"] for r in summary["results"]]
    selected_types = [r["task_type"] for r in summary["results"]]
    assert selected_ids == ["x:aaa", "x:aaa"]
    assert selected_types == ["muckrock_query", "youtube_query"]


def test_run_batch_invalid_max_tasks_raises():
    with pytest.raises(ValueError, match="max_tasks must be >= 1"):
        run_enrichment_batch(
            [_t()], provider=MockProvider(), dry_run=False, max_tasks=0,
        )


def test_run_batch_provider_exception_becomes_failed_row():
    """A regular exception thrown by execute() should be caught and
    mapped to status=failed; the run should not abort."""
    class BadProvider:
        name = "bad"
        def execute(self, task):
            raise RuntimeError("synthetic failure")

    tasks = [_t(candidate_id="x:1"), _t(candidate_id="x:2")]
    summary = run_enrichment_batch(
        tasks, provider=BadProvider(), dry_run=False, max_tasks=10,
    )
    assert summary["failed_count"] == 2
    assert summary["completed_count"] == 0
    for r in summary["results"]:
        assert r["status"] == TaskStatus.FAILED
        assert "RuntimeError" in r["error"]
        assert "synthetic failure" in r["error"]


def test_run_batch_not_implemented_provider_propagates():
    """A NotImplementedError from execute() should propagate — it
    means a deferred provider was misconfigured at the runner
    boundary, and we want the run to abort loudly, not silently
    mark every task failed."""
    class DeferredProvider:
        name = "deferred"
        def execute(self, task):
            raise NotImplementedError("deferred to PR 2")

    with pytest.raises(NotImplementedError, match="deferred to PR 2"):
        run_enrichment_batch(
            [_t()], provider=DeferredProvider(), dry_run=False, max_tasks=10,
        )


def test_run_batch_summary_carries_run_metadata():
    tasks = [_t()]
    summary = run_enrichment_batch(
        tasks, provider=MockProvider(), dry_run=False, max_tasks=10,
    )
    for key in (
        "run_id", "started_at", "finished_at", "elapsed_seconds",
        "dry_run", "provider", "selected_count", "attempted_count",
        "completed_count", "failed_count", "skipped_count",
        "max_tasks", "results",
    ):
        assert key in summary
    assert summary["provider"] == "mock"
