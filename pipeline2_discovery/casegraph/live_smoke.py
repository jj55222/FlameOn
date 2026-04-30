"""LIVE1 — Capped live query-planner smoke harness.

A single, hard-capped live connector run gated by
:func:`pipeline2_discovery.casegraph.live_safety.validate_live_run`.
Default connector is CourtListener; MuckRock is acceptable when
CourtListener config is unavailable.

Hard rules (enforced by `validate_live_run` BEFORE any network call):
- env var ``FLAMEON_RUN_LIVE_CASEGRAPH=1`` required
- max_queries clamped to ``LiveRunConfig.max_queries`` (default 1)
- max_results clamped to ``LiveRunConfig.max_results`` (default 5)
- only the four free connectors allowed (courtlistener / muckrock /
  documentcloud / youtube); Brave / Firecrawl require explicit opt-in
- LLM / downloads / scraping / transcript fetching always rejected

What this module does:
1. validate the safety policy
2. construct a fresh `LiveRunBudget`
3. invoke ``connector.search(case_input, max_queries=cfg.max_queries,
   max_results=cfg.max_results)``
4. record query/result counts and wallclock into the budget
5. return a `LiveSmokeResult` carrying the SourceRecords +
   diagnostics — NO VerifiedArtifacts are produced here. Callers that
   want artifact verification must run the appropriate resolver
   separately on the returned packet/sources.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .connectors import (
    CourtListenerConnector,
    DocumentCloudConnector,
    MuckRockConnector,
    SourceConnector,
    YouTubeConnector,
)
from .live_safety import (
    LiveRunBlocked,
    LiveRunBudget,
    LiveRunConfig,
    safe_live_budget_for,
    validate_live_run,
)
from .models import CaseInput, SourceRecord


CONNECTOR_FACTORIES: Dict[str, Callable[[], SourceConnector]] = {
    "courtlistener": CourtListenerConnector,
    "muckrock": MuckRockConnector,
    "documentcloud": DocumentCloudConnector,
    "youtube": YouTubeConnector,
}


@dataclass
class LiveSmokeResult:
    """Outcome of a single capped live smoke run."""

    sources: List[SourceRecord]
    budget: LiveRunBudget
    connector: str
    last_query: Optional[str] = None
    last_endpoint: Optional[str] = None
    last_status_code: Optional[int] = None
    last_error: Optional[str] = None
    notes: List[str] = field(default_factory=list)

    @property
    def source_count(self) -> int:
        return len(self.sources)

    @property
    def verified_artifact_count(self) -> int:
        # The smoke harness deliberately does NOT call any resolver.
        return 0

    def to_diagnostics(self) -> Dict[str, Any]:
        """Stable, ledger-friendly diagnostics dict for a live smoke."""
        return {
            "connector": self.connector,
            "query": self.last_query,
            "endpoint": self.last_endpoint,
            "status_code": self.last_status_code,
            "error": self.last_error,
            "result_count": self.source_count,
            "verified_artifact_count": self.verified_artifact_count,
            "wallclock_seconds": round(self.budget.wallclock_seconds, 4),
            "api_calls": dict(self.budget.api_calls),
            "estimated_cost_usd": round(self.budget.estimated_cost_usd, 4),
            "notes": list(self.notes),
        }


def _resolve_connector(
    config: LiveRunConfig,
    *,
    connector: Optional[SourceConnector],
) -> SourceConnector:
    if connector is not None:
        return connector
    factory = CONNECTOR_FACTORIES.get(config.connector)
    if factory is None:
        raise LiveRunBlocked(
            f"no factory for connector {config.connector!r}; supply one explicitly via "
            f"run_capped_live_smoke(..., connector=<instance>)"
        )
    return factory()


def run_capped_live_smoke(
    case_input: CaseInput,
    *,
    config: LiveRunConfig,
    connector: Optional[SourceConnector] = None,
    env: Optional[Dict[str, str]] = None,
) -> LiveSmokeResult:
    """Run exactly one capped live connector search under the safety policy.

    The safety check runs FIRST. If it raises, no connector call is
    made and no budget is recorded. On success, exactly one
    ``connector.search(...)`` call is dispatched with the configured
    caps, results are returned as ``SourceRecord`` instances, and the
    `LiveRunBudget` records the query/result counts plus wallclock.

    The harness never builds VerifiedArtifacts and never sets a
    verdict — those decisions belong to the assembly + scoring path
    downstream. Callers are expected to run identity / outcome /
    claim / scoring after this returns if they want a full packet.
    """

    validate_live_run(config, env=env)

    sc = _resolve_connector(config, connector=connector)
    budget = safe_live_budget_for(config)

    started = time.perf_counter()
    sources: List[SourceRecord] = []
    last_error: Optional[str] = None
    try:
        sources = list(
            sc.search(
                case_input,
                max_queries=config.max_queries,
                max_results=config.max_results,
            )
        )
    except Exception as exc:  # pragma: no cover - defensive: surface live errors
        last_error = str(exc)
    finally:
        budget.record_wallclock(round(time.perf_counter() - started, 4))

    budget.record_query(1)
    budget.record_results(len(sources))

    last_query = getattr(sc, "last_query", None)
    last_endpoint = getattr(sc, "last_endpoint", None)
    last_status_code = getattr(sc, "last_status_code", None)
    if last_error is None:
        last_error = getattr(sc, "last_error", None)

    return LiveSmokeResult(
        sources=sources,
        budget=budget,
        connector=config.connector,
        last_query=last_query,
        last_endpoint=last_endpoint,
        last_status_code=last_status_code,
        last_error=last_error,
        notes=list(config.notes),
    )


__all__ = [
    "CONNECTOR_FACTORIES",
    "LiveSmokeResult",
    "run_capped_live_smoke",
]
