"""Enrichment dataclasses + stable enum-like constants.

Kept dataclass-light so the same shapes round-trip cleanly through
JSON (``EnrichmentTask.from_dict`` / ``EnrichmentResult.to_dict``)
without a serialization framework.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple


class TaskType:
    """Stable task-type codes mirrored from
    ``pipeline2_discovery.dataset_sources.run_dataset_intake``'s
    output. Each task in ``search_tasks.json`` carries one of these."""

    YOUTUBE_QUERY = "youtube_query"
    MUCKROCK_QUERY = "muckrock_query"
    OFFICIAL_SOURCE_QUERY = "official_source_query"
    OUTCOME_QUERY = "outcome_query"

    ALL: Tuple[str, ...] = (
        "youtube_query",
        "muckrock_query",
        "official_source_query",
        "outcome_query",
    )


class TaskStatus:
    """Stable per-task status codes the runner emits. Every
    ``EnrichmentResult.status`` is one of these strings."""

    DRY_RUN = "dry_run"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

    ALL: Tuple[str, ...] = ("dry_run", "completed", "failed", "skipped")


@dataclass
class EnrichmentTask:
    """One row from ``search_tasks.json``.

    ``context`` carries the originating candidate's identity-anchor
    fields (agency, subject_name, city, state) so a provider can
    refine the query (e.g. add ``site:phoenix.gov``) without going
    back to the dataset-intake layer.
    """
    candidate_id: str
    grade: str
    task_type: str
    query: str
    context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "EnrichmentTask":
        return cls(
            candidate_id=str(data.get("candidate_id") or ""),
            grade=str(data.get("grade") or ""),
            task_type=str(data.get("task_type") or ""),
            query=str(data.get("query") or ""),
            context=dict(data.get("context") or {}),
        )


@dataclass
class EnrichmentResult:
    """One row in the runner's output ``results`` array.

    Carries the originating task's identity (``candidate_id`` /
    ``task_type`` / ``query``) verbatim so downstream tooling can
    join back to the source task. Provider-specific output goes
    into ``result_urls`` / ``result_titles`` / ``confidence``;
    routing hints (e.g. "this URL should now feed into
    portal-live") go into ``next_actions_hint``.
    """
    candidate_id: str
    task_type: str
    query: str
    status: str
    provider: str
    result_urls: List[str] = field(default_factory=list)
    result_titles: List[str] = field(default_factory=list)
    confidence: str = "unknown"  # high | medium | low | unknown
    next_actions_hint: List[str] = field(default_factory=list)
    error: Optional[str] = None
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


__all__ = [
    "EnrichmentResult",
    "EnrichmentTask",
    "TaskStatus",
    "TaskType",
]
