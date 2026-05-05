"""Enrichment provider interface + Mock and YouTube implementations.

This PR ships the Mock provider and the yt-dlp YouTube search provider.
Remaining live providers (MuckRock API, Brave / Exa / Tavily web search)
are deferred to follow-up PRs and raise ``NotImplementedError`` here
so a misconfigured CLI invocation fails loudly at the harness level
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
from typing import Any, List, Optional, Tuple

from .models import EnrichmentResult, EnrichmentTask, TaskStatus


# Names accepted by ``get_provider``.
KNOWN_PROVIDERS: Tuple[str, ...] = ("mock", "youtube")
DEFERRED_PROVIDERS: Tuple[str, ...] = (
    "muckrock",
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


# ---- YouTube (yt-dlp) provider --------------------------------------


class YtDlpYouTubeSearchClient:
    """yt-dlp backed YouTube search provider.

    Searches YouTube via yt-dlp's ``ytsearchN:`` pseudo-URL.  No YouTube
    Data API key, no quota consumption.  Metadata-only: runs in
    ``extract_flat`` mode — no media, audio, subtitle, or caption
    downloads, no writes to disk.

    ``ydl_cls`` is the YoutubeDL class to instantiate; default is
    ``yt_dlp.YoutubeDL``.  Inject a fake for tests.
    """

    name = "youtube"

    def __init__(
        self,
        *,
        max_results: int = 5,
        socket_timeout: int = 10,
        ydl_cls: Optional[Any] = None,
    ) -> None:
        self._max_results = max(1, min(max_results, 20))
        self._socket_timeout = socket_timeout
        self._ydl_cls = ydl_cls

    def _load_yt_dlp(self) -> Any:
        try:
            import yt_dlp  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "yt-dlp is required for the youtube provider; pip install yt-dlp"
            ) from exc
        return yt_dlp.YoutubeDL

    def execute(self, task: EnrichmentTask) -> EnrichmentResult:
        ydl_cls = self._ydl_cls or self._load_yt_dlp()
        opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
            "noplaylist": True,
            "socket_timeout": self._socket_timeout,
        }
        search_url = f"ytsearch{self._max_results}:{task.query}"
        try:
            with ydl_cls(opts) as ydl:
                info = ydl.extract_info(search_url, download=False)
        except Exception as exc:
            return EnrichmentResult(
                candidate_id=task.candidate_id,
                task_type=task.task_type,
                query=task.query,
                status=TaskStatus.FAILED,
                provider=self.name,
                error=f"{type(exc).__name__}: {exc}",
            )

        urls: List[str] = []
        titles: List[str] = []
        for entry in (info or {}).get("entries", [])[: self._max_results]:
            vid_id = str(entry.get("id") or "")
            if not vid_id:
                continue
            raw_url = str(entry.get("url") or "")
            url = raw_url if raw_url.startswith("http") else f"https://www.youtube.com/watch?v={vid_id}"
            urls.append(url)
            titles.append(str(entry.get("title") or vid_id))

        next_actions = ["youtube_metadata"] if urls else []
        confidence = "medium" if urls else "low"

        return EnrichmentResult(
            candidate_id=task.candidate_id,
            task_type=task.task_type,
            query=task.query,
            status=TaskStatus.COMPLETED,
            provider=self.name,
            result_urls=urls,
            result_titles=titles,
            confidence=confidence,
            next_actions_hint=next_actions,
        )


# ---- factory --------------------------------------------------------


def get_provider(name: str):
    """Return a provider instance by name.

    - ``"mock"`` returns a ``MockProvider``.
    - ``"youtube"`` returns a ``YtDlpYouTubeSearchClient``.
    - Any name in ``DEFERRED_PROVIDERS`` raises NotImplementedError
      pointing at the follow-up PR roadmap.
    - Any other name raises ValueError.
    """
    if name == "mock":
        return MockProvider()
    if name == "youtube":
        return YtDlpYouTubeSearchClient()
    if name in DEFERRED_PROVIDERS:
        raise NotImplementedError(
            f"provider {name!r} is gated to a follow-up PR; this PR ships "
            f"the harness, the mock provider, and the yt-dlp youtube "
            f"provider. Roadmap: PR 3 = MuckRock API, PR 4 = official-source "
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
    "YtDlpYouTubeSearchClient",
    "get_provider",
]
