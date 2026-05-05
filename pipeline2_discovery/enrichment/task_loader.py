"""Load + validate ``search_tasks.json`` into ``EnrichmentTask`` rows.

The task-document shape is what
``tools/run_dataset_intake.py`` writes:

  {
    "generated_at":   "<UTC ISO-8601>",
    "source_lane":    "sfchronicle_pursuits",
    "candidate_count": 100,
    "task_count":     900,
    "tasks": [
      {
        "candidate_id": "...",
        "grade":        "A",
        "task_type":    "youtube_query",
        "query":        "...",
        "context":      {"agency": "...", "subject_name": "...", ...}
      },
      ...
    ]
  }

This loader:
  - rejects non-object roots
  - rejects missing-or-non-list ``tasks`` key
  - rejects each row missing required keys (candidate_id, task_type, query)
  - drops rows where any required field is empty after strip
  - tolerates extra row keys (forward-compat)

Returns ``list[EnrichmentTask]``. Empty input is allowed.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, List

from .models import EnrichmentTask, TaskType


_REQUIRED_TASK_KEYS = ("candidate_id", "task_type", "query")


def parse_task_dict(raw: Any, *, row_index: int = 0) -> EnrichmentTask:
    """Validate one row dict and return an EnrichmentTask.

    Raises ``ValueError`` with a row-positioned message if the row
    is malformed."""
    if not isinstance(raw, dict):
        raise ValueError(
            f"task row [{row_index}] must be an object, got {type(raw).__name__}"
        )
    missing = [k for k in _REQUIRED_TASK_KEYS if not (raw.get(k) or "").strip()]
    if missing:
        raise ValueError(
            f"task row [{row_index}] missing required keys / empty values: "
            f"{missing}"
        )
    task_type = (raw.get("task_type") or "").strip()
    if task_type not in TaskType.ALL:
        raise ValueError(
            f"task row [{row_index}] task_type {task_type!r} not in "
            f"{TaskType.ALL}"
        )
    return EnrichmentTask.from_dict(raw)


def load_search_tasks_text(text: str) -> List[EnrichmentTask]:
    """Parse a JSON document string into a list of EnrichmentTask
    rows. Validates the document shape + each row."""
    if not text or not text.strip():
        return []
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError(
            "search_tasks.json root must be an object with a 'tasks' key"
        )
    tasks_raw = data.get("tasks")
    if not isinstance(tasks_raw, list):
        raise ValueError(
            "search_tasks.json 'tasks' key must be a JSON array"
        )
    out: List[EnrichmentTask] = []
    for i, row in enumerate(tasks_raw):
        out.append(parse_task_dict(row, row_index=i))
    return out


def load_search_tasks_file(path: Path) -> List[EnrichmentTask]:
    """Read + parse a search_tasks.json file."""
    text = Path(path).read_text(encoding="utf-8")
    return load_search_tasks_text(text)


__all__ = [
    "load_search_tasks_file",
    "load_search_tasks_text",
    "parse_task_dict",
]
