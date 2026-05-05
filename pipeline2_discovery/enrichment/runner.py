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

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Sequence

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


def run_enrichment_batch(
    tasks: Sequence[EnrichmentTask],
    *,
    provider,
    dry_run: bool,
    max_tasks: int,
) -> dict:
    """Top-level batch runner. Returns the summary dict.

    ``provider`` is any object with a ``name`` attribute and an
    ``execute(task) -> EnrichmentResult`` method. The runner
    catches exceptions thrown by ``execute`` and converts them
    to ``status="failed"`` rows so a single bad task doesn't kill
    the run.
    """
    if max_tasks < 1:
        raise ValueError(f"max_tasks must be >= 1; got {max_tasks}")

    started = datetime.now(timezone.utc)
    selected = sorted(tasks, key=_sort_key)[:max_tasks]

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

    return {
        "run_id": started.strftime("%Y%m%dT%H%M%SZ"),
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "elapsed_seconds": elapsed,
        "dry_run": dry_run,
        "provider": provider.name if provider else "(none)",
        "selected_count": len(selected),
        "attempted_count": len(results),
        "completed_count": completed,
        "failed_count": failed,
        "skipped_count": skipped,
        "max_tasks": max_tasks,
        "results": [asdict(r) for r in results],
    }


__all__ = ["filter_tasks", "run_enrichment_batch"]
