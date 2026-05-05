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
from pipeline2_discovery.enrichment.runner import select_tasks


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
        "dry_run", "provider", "selected_count", "selected_candidate_count",
        "attempted_count", "completed_count", "failed_count",
        "skipped_count", "max_tasks", "max_candidates",
        "tasks_per_candidate", "results",
    ):
        assert key in summary
    assert summary["provider"] == "mock"
    # When neither candidate-aware flag is set, both echo as None.
    assert summary["max_candidates"] is None
    assert summary["tasks_per_candidate"] is None


# ---- candidate-aware selection (post-PR #38 YouTube top-25 finding) -


def _make_pool(n_candidates: int, tasks_per_cand: int):
    """Build a pool of n_candidates × tasks_per_cand tasks. Candidate
    ids are zero-padded so lexicographic order matches numeric order."""
    out = []
    width = max(2, len(str(n_candidates)))
    for c in range(n_candidates):
        cid = f"x:{c:0{width}d}"
        for k in range(tasks_per_cand):
            out.append(_t(
                candidate_id=cid, query=f"q{k}",
                task_type="youtube_query" if k % 2 == 0 else "muckrock_query",
            ))
    return out


def test_select_tasks_default_matches_old_behavior():
    """No candidate-aware flags → flat sort + max_tasks cap (the v0
    behavior). Reproduces the YouTube top-25 coverage failure: 25
    tasks across 4-task-per-candidate pool covers ~6–7 candidates."""
    pool = _make_pool(n_candidates=10, tasks_per_cand=4)
    selected = select_tasks(pool, max_tasks=25)
    assert len(selected) == 25
    # First 6 candidates fully consumed (24 tasks) plus 1 task from the 7th
    cids = [t.candidate_id for t in selected]
    assert len(set(cids)) == 7
    # Lexicographic order preserved
    assert cids == sorted(cids)


def test_select_tasks_with_max_candidates_only_caps_candidate_count():
    pool = _make_pool(n_candidates=10, tasks_per_cand=4)
    selected = select_tasks(pool, max_tasks=100, max_candidates=3)
    assert len({t.candidate_id for t in selected}) == 3
    # All 4 tasks per kept candidate
    assert len(selected) == 12


def test_select_tasks_with_tasks_per_candidate_only_caps_tasks_per_candidate():
    pool = _make_pool(n_candidates=10, tasks_per_cand=4)
    selected = select_tasks(pool, max_tasks=100, tasks_per_candidate=2)
    # All 10 candidates × 2 tasks = 20
    assert len(selected) == 20
    assert len({t.candidate_id for t in selected}) == 10
    # Per-candidate cap holds
    from collections import Counter
    counts = Counter(t.candidate_id for t in selected)
    assert all(c <= 2 for c in counts.values())


def test_select_tasks_combines_max_candidates_and_tasks_per_candidate():
    """The recommended operational shape: 25 candidates × 1 task = 25
    distinct-candidate fan-out instead of 6-candidate stacking."""
    pool = _make_pool(n_candidates=50, tasks_per_cand=4)
    selected = select_tasks(
        pool, max_tasks=25, max_candidates=25, tasks_per_candidate=1,
    )
    assert len(selected) == 25
    assert len({t.candidate_id for t in selected}) == 25


def test_select_tasks_max_tasks_acts_as_final_global_cap():
    """When max_tasks is tighter than max_candidates × tasks_per_candidate
    would allow, max_tasks wins."""
    pool = _make_pool(n_candidates=50, tasks_per_cand=4)
    selected = select_tasks(
        pool, max_tasks=10, max_candidates=25, tasks_per_candidate=2,
    )
    # Would have produced 50 (25 cands × 2 tasks), capped to 10
    assert len(selected) == 10


def test_select_tasks_invalid_max_tasks_raises():
    with pytest.raises(ValueError, match="max_tasks"):
        select_tasks([_t()], max_tasks=0)


def test_select_tasks_invalid_max_candidates_raises():
    with pytest.raises(ValueError, match="max_candidates"):
        select_tasks([_t()], max_tasks=10, max_candidates=0)


def test_select_tasks_invalid_tasks_per_candidate_raises():
    with pytest.raises(ValueError, match="tasks_per_candidate"):
        select_tasks([_t()], max_tasks=10, tasks_per_candidate=0)


def test_select_tasks_filtering_happens_before_grouping():
    """The runner expects filtered tasks as input — make sure
    select_tasks doesn't try to second-guess that. Tasks with mixed
    task_types group by candidate cleanly."""
    pool = _make_pool(n_candidates=3, tasks_per_cand=4)
    # Pre-filter to youtube_query only
    yt_only = [t for t in pool if t.task_type == "youtube_query"]
    selected = select_tasks(
        yt_only, max_tasks=10, max_candidates=3, tasks_per_candidate=1,
    )
    assert len(selected) == 3
    assert all(t.task_type == "youtube_query" for t in selected)


def test_select_tasks_per_candidate_order_is_deterministic():
    pool = [
        _t(candidate_id="x:0", task_type="youtube_query", query="q_b"),
        _t(candidate_id="x:0", task_type="youtube_query", query="q_a"),
        _t(candidate_id="x:0", task_type="muckrock_query", query="q_c"),
    ]
    selected = select_tasks(
        pool, max_tasks=10, max_candidates=1, tasks_per_candidate=2,
    )
    # Sorted by (candidate_id, task_type, query): muckrock < youtube;
    # within youtube, q_a < q_b. Cap to 2.
    assert [(t.task_type, t.query) for t in selected] == [
        ("muckrock_query", "q_c"),
        ("youtube_query", "q_a"),
    ]


# ---- run_enrichment_batch wiring ------------------------------------


def test_run_batch_passes_through_candidate_aware_flags():
    pool = _make_pool(n_candidates=20, tasks_per_cand=4)
    summary = run_enrichment_batch(
        pool, provider=MockProvider(), dry_run=False,
        max_tasks=25, max_candidates=10, tasks_per_candidate=2,
    )
    # 10 cands × 2 tasks = 20 (under the max_tasks=25 cap)
    assert summary["selected_count"] == 20
    assert summary["selected_candidate_count"] == 10
    assert summary["max_candidates"] == 10
    assert summary["tasks_per_candidate"] == 2


def test_run_batch_dry_run_with_candidate_aware_does_not_invoke_provider():
    pool = _make_pool(n_candidates=10, tasks_per_cand=4)

    class _BoomProvider:
        name = "mock"
        def execute(self, task):
            raise AssertionError("provider must not be invoked in dry-run")

    summary = run_enrichment_batch(
        pool, provider=_BoomProvider(), dry_run=True,
        max_tasks=25, max_candidates=5, tasks_per_candidate=1,
    )
    assert summary["selected_count"] == 5
    assert summary["selected_candidate_count"] == 5
    assert all(r["status"] == "dry_run" for r in summary["results"])
