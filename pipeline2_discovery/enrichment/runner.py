"""Enrichment runner: filter → sort → cap → execute → aggregate.

Pure orchestrator. Takes a list of ``EnrichmentTask`` rows
(typically loaded from ``search_tasks.json``), applies operator
filters, deterministically sorts the survivors, caps by
``max_tasks``, and either skips execution (dry-run) or dispatches
each task to the provider's ``execute(task)`` method.

Returns a summary dict with aggregate counts and the per-task
``EnrichmentResult`` rows.

This module knows nothing about the network. Live providers are
plugged in via the providers.py factory; the runner only sees
``provider.name`` and ``provider.execute(task)``.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence

from .models import EnrichmentResult, EnrichmentTask, TaskStatus, TaskType


def filter_tasks(
    tasks: Iterable[EnrichmentTask],
    *,
    task_types: Sequence[str] = (),
    candidate_ids: Sequence[str] = (),
    grades: Sequence[str] = (),
) -> List[EnrichmentTask]:
    """Apply per-dimension allowlist filters. Empty list ⇒ no filter
    on that dimension. Returns matching rows in their original
    iteration order; sorting is the runner's job."""
    task_type_set = set(task_types) if task_types else None
    candidate_id_set = set(candidate_ids) if candidate_ids else None
    grade_set = set(g.upper() for g in grades) if grades else None

    out: List[EnrichmentTask] = []
    for t in tasks:
        if task_type_set is not None and t.task_type not in task_type_set:
            continue
        if candidate_id_set is not None and t.candidate_id not in candidate_id_set:
            continue
        if grade_set is not None and (t.grade or "").upper() not in grade_set:
            continue
        out.append(t)
    return out


def _sort_key(task: EnrichmentTask):
    """Deterministic per-task sort key: candidate_id then task_type
    then query. Same task → same position across runs."""
    return (task.candidate_id, task.task_type, task.query)


def select_tasks(
    tasks: Iterable[EnrichmentTask],
    *,
    max_tasks: int,
    max_candidates: Optional[int] = None,
    tasks_per_candidate: Optional[int] = None,
) -> List[EnrichmentTask]:
    """Order and cap tasks for execution, optionally with
    candidate-aware sampling.

    Filtering by task_type / grade / candidate_id is upstream
    (:func:`filter_tasks`); this helper does only ordering + capping.

    Backward-compatible behavior (when neither ``max_candidates`` nor
    ``tasks_per_candidate`` is set):

      Sort all tasks by ``(candidate_id, task_type, query)`` and take
      the first ``max_tasks``. This is what the v0 / v1 runner did
      and is the right default for single-task-per-candidate inputs.

    Candidate-aware behavior (when either flag is set):

      1. Group tasks by ``candidate_id``.
      2. Sort the candidate IDs ascending — deterministic.
      3. For each candidate, sort its tasks by ``_sort_key`` and
         optionally cap to ``tasks_per_candidate``.
      4. Cap the candidate list to ``max_candidates``.
      5. Flatten back to a single list (still in candidate-then-task
         order) and apply ``max_tasks`` as a final global cap.

    The candidate-aware path was added after the YouTube top-25
    smoke (post-PR #38) revealed that a flat ``max_tasks=25`` over
    a candidate pool with ~4 tasks per candidate touches only ~6–7
    distinct candidates — a coverage failure mode. With
    ``--max-candidates 25 --tasks-per-candidate 1`` the same
    25-task budget fans out across 25 candidates instead.
    """
    if max_tasks < 1:
        raise ValueError(f"max_tasks must be >= 1; got {max_tasks}")

    task_list = list(tasks)

    if max_candidates is None and tasks_per_candidate is None:
        # v0 behaviour preserved exactly — flat sort + cap.
        return sorted(task_list, key=_sort_key)[:max_tasks]

    if max_candidates is not None and max_candidates < 1:
        raise ValueError(
            f"max_candidates must be >= 1 if provided; got {max_candidates}"
        )
    if tasks_per_candidate is not None and tasks_per_candidate < 1:
        raise ValueError(
            f"tasks_per_candidate must be >= 1 if provided; got {tasks_per_candidate}"
        )

    by_cand: Dict[str, List[EnrichmentTask]] = defaultdict(list)
    for t in task_list:
        by_cand[t.candidate_id].append(t)

    cand_ids = sorted(by_cand.keys())
    if max_candidates is not None:
        cand_ids = cand_ids[: int(max_candidates)]

    selected: List[EnrichmentTask] = []
    for cid in cand_ids:
        per = sorted(by_cand[cid], key=_sort_key)
        if tasks_per_candidate is not None:
            per = per[: int(tasks_per_candidate)]
        selected.extend(per)

    return selected[:max_tasks]


def run_enrichment_batch(
    tasks: Sequence[EnrichmentTask],
    *,
    provider,
    dry_run: bool,
    max_tasks: int,
    max_candidates: Optional[int] = None,
    tasks_per_candidate: Optional[int] = None,
) -> dict:
    """Top-level batch runner. Returns the summary dict.

    ``provider`` is any object with a ``name`` attribute and an
    ``execute(task) -> EnrichmentResult`` method. The runner
    catches exceptions thrown by ``execute`` and converts them
    to ``status="failed"`` rows so a single bad task doesn't kill
    the run.

    ``max_candidates`` and ``tasks_per_candidate`` enable candidate-
    aware sampling — see :func:`select_tasks`. Both default ``None``
    to preserve the original ``max_tasks``-only behaviour.
    """
    started = datetime.now(timezone.utc)
    selected = select_tasks(
        tasks,
        max_tasks=max_tasks,
        max_candidates=max_candidates,
        tasks_per_candidate=tasks_per_candidate,
    )

    results: List[EnrichmentResult] = []
    completed = 0
    failed = 0
    skipped = 0

    if dry_run:
        for task in selected:
            results.append(EnrichmentResult(
                candidate_id=task.candidate_id,
                task_type=task.task_type,
                query=task.query,
                status=TaskStatus.DRY_RUN,
                provider=provider.name if provider else "(none)",
                result_urls=[],
                result_titles=[],
                confidence="unknown",
                next_actions_hint=[],
                notes=["dry-run — provider was not invoked"],
            ))
    else:
        for task in selected:
            try:
                row = provider.execute(task)
            except NotImplementedError:
                # Re-raise — a deferred provider hitting execute
                # should crash the run loudly, not be papered over
                # as a failed task. This catches misuse of get_provider
                # at the runner boundary.
                raise
            except Exception as exc:  # noqa: BLE001
                row = EnrichmentResult(
                    candidate_id=task.candidate_id,
                    task_type=task.task_type,
                    query=task.query,
                    status=TaskStatus.FAILED,
                    provider=provider.name,
                    error=f"{type(exc).__name__}: {exc}",
                )
            results.append(row)
            if row.status == TaskStatus.COMPLETED:
                completed += 1
            elif row.status == TaskStatus.FAILED:
                failed += 1
            else:
                skipped += 1

    finished = datetime.now(timezone.utc)
    elapsed = (finished - started).total_seconds()

    selected_candidate_count = len({t.candidate_id for t in selected})

    return {
        "run_id": started.strftime("%Y%m%dT%H%M%SZ"),
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "elapsed_seconds": elapsed,
        "dry_run": dry_run,
        "provider": provider.name if provider else "(none)",
        "selected_count": len(selected),
        "selected_candidate_count": selected_candidate_count,
        "attempted_count": len(results),
        "completed_count": completed,
        "failed_count": failed,
        "skipped_count": skipped,
        "max_tasks": max_tasks,
        "max_candidates": max_candidates,
        "tasks_per_candidate": tasks_per_candidate,
        "results": [asdict(r) for r in results],
    }


__all__ = ["filter_tasks", "run_enrichment_batch", "select_tasks"]
