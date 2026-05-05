"""Enrichment provider interface + Mock implementation + factory.

Live provider implementations live in dedicated modules so their
helpers don't bloat this module:

- :mod:`youtube_provider` — yt-dlp metadata + relevance gate
- :mod:`muckrock_provider` — MuckRock API v2 GET-only client +
  anchor / supporting-term scoring

Both classes are re-exported from here for backward compatibility
with code that imports them from ``pipeline2_discovery.enrichment``.

Remaining live providers (Brave / Exa / Tavily web search) are
deferred to follow-up PRs and raise ``NotImplementedError`` here so
a misconfigured CLI invocation fails loudly at the harness level
instead of silently calling out.

Provider contract (informal Protocol — Python's ``typing.Protocol``
isn't strictly required, but the runner only uses ``provider.name``
and ``provider.execute(task)``):

    class EnrichmentProvider:
        name: str
        def execute(self, task: EnrichmentTask) -> EnrichmentResult: ...
"""
from __future__ import annotations

import hashlib
from typing import Tuple

from .models import EnrichmentResult, EnrichmentTask, TaskStatus
from .muckrock_provider import MuckRockProvider
from .youtube_provider import YtDlpYouTubeSearchClient


# Names accepted by ``get_provider``.
KNOWN_PROVIDERS: Tuple[str, ...] = ("mock", "youtube", "muckrock")
DEFERRED_PROVIDERS: Tuple[str, ...] = (
    "brave",
    "exa",
    "tavily",
)


# ---- Mock provider --------------------------------------------------


class MockProvider:
    """Deterministic fake provider for tests and CLI smoke runs.

    Returns one synthetic result URL per task, derived from a stable
    hash of ``(task_type, query)`` so the same input always produces
    the same output. The fake URLs use ``mock.example.com`` so any
    downstream code that accidentally treats them as real will fail
    obviously (DNS) rather than silently.
    """

    name = "mock"

    def execute(self, task: EnrichmentTask) -> EnrichmentResult:
        digest = hashlib.sha256(
            f"{task.task_type}::{task.query}".encode("utf-8")
        ).hexdigest()[:12]
        url = f"https://mock.example.com/{task.task_type}/{digest}"
        title = f"[mock] {task.task_type} hit for {task.query!r}"
        return EnrichmentResult(
            candidate_id=task.candidate_id,
            task_type=task.task_type,
            query=task.query,
            status=TaskStatus.COMPLETED,
            provider=self.name,
            result_urls=[url],
            result_titles=[title],
            confidence="low",  # always low — they're fakes
            next_actions_hint=[],
            notes=["mock provider — synthetic result, do not consume"],
        )


# ---- factory --------------------------------------------------------


def get_provider(name: str):
    """Return a provider instance by name.

    - ``"mock"`` returns a ``MockProvider``.
    - ``"youtube"`` returns a ``YtDlpYouTubeSearchClient``.
    - ``"muckrock"`` returns a ``MuckRockProvider``.
    - Any name in ``DEFERRED_PROVIDERS`` raises NotImplementedError
      pointing at the follow-up PR roadmap.
    - Any other name raises ValueError.
    """
    if name == "mock":
        return MockProvider()
    if name == "youtube":
        return YtDlpYouTubeSearchClient()
    if name == "muckrock":
        return MuckRockProvider()
    if name in DEFERRED_PROVIDERS:
        raise NotImplementedError(
            f"provider {name!r} is gated to a follow-up PR; this PR ships "
            f"the harness, the mock provider, the yt-dlp youtube provider, "
            f"and the MuckRock API provider. Roadmap: PR 4 = official-source "
            f"web search (Brave / Exa / Tavily)."
        )
    raise ValueError(
        f"unknown provider {name!r}; known: {KNOWN_PROVIDERS}, "
        f"deferred: {DEFERRED_PROVIDERS}"
    )


__all__ = [
    "DEFERRED_PROVIDERS",
    "KNOWN_PROVIDERS",
    "MockProvider",
    "MuckRockProvider",
    "YtDlpYouTubeSearchClient",
    "get_provider",
]
