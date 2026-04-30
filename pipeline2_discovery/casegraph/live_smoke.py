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
from typing import Any, Callable, Dict, List, Optional, Set

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

# LIVE4 — explicit cap on the number of connectors a multi-connector
# smoke may invoke. Tighter than ALLOWED_FREE_CONNECTORS since the
# initial pass deliberately scopes to two providers.
MAX_CONNECTORS_HARD_CAP: int = 2


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


@dataclass
class MultiConnectorSmokeResult:
    """Aggregate of multiple :class:`LiveSmokeResult` runs.

    LIVE4 caps the number of connectors at
    :data:`MAX_CONNECTORS_HARD_CAP` (currently 2). Each per-connector
    `LiveSmokeResult` is preserved on ``per_connector`` and the
    aggregated counters are pre-computed for ledger emission.
    """

    per_connector: List[LiveSmokeResult]
    total_live_calls: int
    total_source_records: int
    total_verified_artifacts: int
    total_wallclock_seconds: float
    total_estimated_cost_usd: float
    api_calls: Dict[str, int]
    errors: List[str] = field(default_factory=list)

    def to_diagnostics(self) -> Dict[str, Any]:
        """Stable, ledger-friendly diagnostics dict for a multi-connector smoke."""
        return {
            "connectors": [r.connector for r in self.per_connector],
            "total_live_calls": self.total_live_calls,
            "total_source_records": self.total_source_records,
            "total_verified_artifacts": self.total_verified_artifacts,
            "total_wallclock_seconds": round(self.total_wallclock_seconds, 4),
            "total_estimated_cost_usd": round(self.total_estimated_cost_usd, 4),
            "api_calls": dict(self.api_calls),
            "per_connector": [r.to_diagnostics() for r in self.per_connector],
            "errors": list(self.errors),
        }


def run_capped_multi_connector_smoke(
    case_input,
    *,
    configs: List[LiveRunConfig],
    connectors: Optional[List[SourceConnector]] = None,
    env: Optional[Dict[str, str]] = None,
) -> MultiConnectorSmokeResult:
    """Run capped live smokes across at most :data:`MAX_CONNECTORS_HARD_CAP`
    connectors, sequentially.

    Each config goes through :func:`validate_live_run` BEFORE any
    connector instance is touched. The `connectors` argument, when
    supplied, must align positionally with `configs` (same length) so
    tests can inject FakeSession-backed connectors per slot.

    Returns a :class:`MultiConnectorSmokeResult` with per-connector
    diagnostics and aggregated counters. Verified artifacts stay 0 —
    the harness deliberately doesn't run a resolver. Brave / Firecrawl
    / LLM all stay at 0 (validate_live_run rejects those configs
    upstream).
    """
    if not isinstance(configs, list):
        configs = list(configs)
    if not configs:
        raise LiveRunBlocked("at least one LiveRunConfig is required")
    if len(configs) > MAX_CONNECTORS_HARD_CAP:
        raise LiveRunBlocked(
            f"multi-connector smoke capped at {MAX_CONNECTORS_HARD_CAP} connectors; "
            f"got {len(configs)}"
        )

    seen_connectors: Set[str] = set()
    for cfg in configs:
        if cfg.connector in seen_connectors:
            raise LiveRunBlocked(
                f"duplicate connector {cfg.connector!r} in multi-connector smoke; "
                "each connector can only run once per smoke"
            )
        seen_connectors.add(cfg.connector)

    if connectors is not None and len(connectors) != len(configs):
        raise LiveRunBlocked(
            "if `connectors` is supplied it must align positionally with `configs`"
        )

    api_calls: Dict[str, int] = {}
    per_connector: List[LiveSmokeResult] = []
    total_live_calls = 0
    total_sources = 0
    total_artifacts = 0
    total_wall = 0.0
    total_cost = 0.0
    errors: List[str] = []

    for idx, cfg in enumerate(configs):
        injected = connectors[idx] if connectors is not None else None
        smoke_result = run_capped_live_smoke(
            case_input, config=cfg, connector=injected, env=env
        )
        per_connector.append(smoke_result)
        total_live_calls += smoke_result.budget.query_count
        total_sources += smoke_result.source_count
        total_artifacts += smoke_result.verified_artifact_count
        total_wall += smoke_result.budget.wallclock_seconds
        total_cost += smoke_result.budget.estimated_cost_usd
        for provider, count in smoke_result.budget.api_calls.items():
            api_calls[provider] = api_calls.get(provider, 0) + int(count)
        if smoke_result.last_error:
            errors.append(f"{smoke_result.connector}: {smoke_result.last_error}")

    return MultiConnectorSmokeResult(
        per_connector=per_connector,
        total_live_calls=total_live_calls,
        total_source_records=total_sources,
        total_verified_artifacts=total_artifacts,
        total_wallclock_seconds=round(total_wall, 4),
        total_estimated_cost_usd=round(total_cost, 4),
        api_calls=api_calls,
        errors=errors,
    )


__all__ = [
    "CONNECTOR_FACTORIES",
    "LiveSmokeResult",
    "MAX_CONNECTORS_HARD_CAP",
    "MultiConnectorSmokeResult",
    "run_capped_live_smoke",
    "run_capped_multi_connector_smoke",
]
