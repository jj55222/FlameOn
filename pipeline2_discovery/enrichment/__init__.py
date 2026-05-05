"""Pipeline 1 enrichment: bridge between dataset-intake search tasks
and downstream validators (portal-live, MuckRock parser, YouTube
metadata + transcript, identity / outcome validation).

Reads ``search_tasks.json`` produced by the dataset-intake layer
(``pipeline2_discovery.dataset_sources``) and runs each task through
a provider that returns candidate URLs / titles / metadata. The
results then feed back into the existing pipelines:

  - official URLs   ->  portal-live curated scaffold + generator
  - MuckRock URLs   ->  muckrock_curated parser
  - YouTube URLs    ->  future media preprocessing (PR roadmap)
  - outcome URLs    ->  identity / outcome validation

This PR ships the **skeleton + Mock provider only**. The harness
loads tasks, filters / sorts / caps them, dispatches to a provider,
and aggregates per-task results into a summary JSON. Live providers
(YouTube Data API, MuckRock API, Brave / Exa / Tavily web search)
are deferred to follow-up PRs and are explicitly gated via
``NotImplementedError`` here so a misconfigured CLI invocation
fails loudly instead of silently calling out.
"""
from __future__ import annotations

from .models import (
    EnrichmentResult,
    EnrichmentTask,
    TaskStatus,
    TaskType,
)
from .providers import (
    DEFERRED_PROVIDERS,
    KNOWN_PROVIDERS,
    MockProvider,
    get_provider,
)
from .runner import filter_tasks, run_enrichment_batch
from .task_loader import (
    load_search_tasks_file,
    load_search_tasks_text,
    parse_task_dict,
)


__all__ = [
    "DEFERRED_PROVIDERS",
    "EnrichmentResult",
    "EnrichmentTask",
    "KNOWN_PROVIDERS",
    "MockProvider",
    "TaskStatus",
    "TaskType",
    "filter_tasks",
    "get_provider",
    "load_search_tasks_file",
    "load_search_tasks_text",
    "parse_task_dict",
    "run_enrichment_batch",
]
