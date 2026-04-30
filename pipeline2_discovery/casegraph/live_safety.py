"""LIVE0 — Live-run safety preflight harness.

Centralized guardrails so every future live experiment shares the same
caps, env gates, and allow-lists. The module itself never makes a live
call: `validate_live_run` only reads a config + the process environment.

Defaults are intentionally restrictive:

- live runs are DISABLED unless the configured env var (default
  ``FLAMEON_RUN_LIVE_CASEGRAPH``) equals ``"1"``
- the connector must be one of the free, metadata-only connectors
  (``courtlistener``, ``muckrock``, ``documentcloud``, ``youtube``)
  unless explicitly allow-listed
- Brave and Firecrawl are blocked unless ``allow_brave`` /
  ``allow_firecrawl`` are toggled on
- LLM, downloads, scraping, and transcript fetching are blocked
- ``max_queries`` and ``max_results`` are clamped to small hard caps

`LiveRunBudget` tracks per-run counters and converts to a
``ledger``-compatible api_calls dict so live results can be fed
straight into ``build_run_ledger_entry``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from .ledger import COST_PER_CALL_USD, DEFAULT_API_CALLS, estimate_cost


ALLOWED_FREE_CONNECTORS: Set[str] = {
    "courtlistener",
    "muckrock",
    "documentcloud",
    "youtube",
}
PAID_CONNECTORS: Set[str] = {"brave", "firecrawl"}
ALL_KNOWN_CONNECTORS: Set[str] = ALLOWED_FREE_CONNECTORS | PAID_CONNECTORS

# Hard caps for capped live smokes. Tighter than the per-connector
# `search()` clamps to keep this batch's surface deliberately small.
MAX_QUERIES_HARD_CAP: int = 3
MAX_RESULTS_HARD_CAP: int = 5
DEFAULT_ENV_VAR: str = "FLAMEON_RUN_LIVE_CASEGRAPH"


class LiveRunBlocked(RuntimeError):
    """Raised by validate_live_run when a config violates the safety policy."""


@dataclass
class LiveRunConfig:
    """Description of a single live smoke run.

    Defaults are safe — calling `validate_live_run` on a freshly
    constructed `LiveRunConfig(connector="courtlistener")` succeeds when
    the env gate is set, and refuses on any toggle that would unlock a
    blocked capability.
    """

    connector: str = "courtlistener"
    max_queries: int = 1
    max_results: int = 5
    enabled_env_var: str = DEFAULT_ENV_VAR
    allow_brave: bool = False
    allow_firecrawl: bool = False
    allow_llm: bool = False
    allow_downloads: bool = False
    allow_scraping: bool = False
    allow_transcript_fetch: bool = False
    additional_allowed_connectors: Set[str] = field(default_factory=set)
    notes: List[str] = field(default_factory=list)


def is_live_enabled(
    config: LiveRunConfig,
    *,
    env: Optional[Dict[str, str]] = None,
) -> bool:
    """Return True iff the live env gate is set to ``"1"``.

    The check is opt-in: any other value (including unset, empty,
    ``"0"``, ``"true"``, ``"yes"``) returns False.
    """
    environment = os.environ if env is None else env
    return environment.get(config.enabled_env_var) == "1"


def validate_live_run(
    config: LiveRunConfig,
    *,
    env: Optional[Dict[str, str]] = None,
) -> None:
    """Raise :class:`LiveRunBlocked` if the config violates safety policy.

    Order of checks (first failure wins so error messages are stable):

    1. Disallowed-feature toggles (downloads / scraping / LLM /
       transcript fetching) — these always reject before considering
       the env gate so misconfiguration surfaces in dev too.
    2. Hard caps on ``max_queries`` / ``max_results``.
    3. Connector allow-list (paid connectors require explicit opt-in).
    4. Env gate.

    Returns ``None`` on success.
    """

    if config.allow_downloads:
        raise LiveRunBlocked("downloads are not permitted in live smoke runs")
    if config.allow_scraping:
        raise LiveRunBlocked("scraping is not permitted in live smoke runs")
    if config.allow_llm:
        raise LiveRunBlocked("LLM use is not permitted in live smoke runs")
    if config.allow_transcript_fetch:
        raise LiveRunBlocked("transcript fetching is not permitted in live smoke runs")

    if not isinstance(config.max_queries, int) or config.max_queries < 1:
        raise LiveRunBlocked(
            f"max_queries must be a positive integer, got {config.max_queries!r}"
        )
    if config.max_queries > MAX_QUERIES_HARD_CAP:
        raise LiveRunBlocked(
            f"max_queries={config.max_queries} exceeds hard cap {MAX_QUERIES_HARD_CAP}"
        )
    if not isinstance(config.max_results, int) or config.max_results < 1:
        raise LiveRunBlocked(
            f"max_results must be a positive integer, got {config.max_results!r}"
        )
    if config.max_results > MAX_RESULTS_HARD_CAP:
        raise LiveRunBlocked(
            f"max_results={config.max_results} exceeds hard cap {MAX_RESULTS_HARD_CAP}"
        )

    allowed = set(ALLOWED_FREE_CONNECTORS) | set(config.additional_allowed_connectors or set())
    if config.allow_brave:
        allowed.add("brave")
    if config.allow_firecrawl:
        allowed.add("firecrawl")
    if config.connector not in allowed:
        if config.connector in PAID_CONNECTORS:
            raise LiveRunBlocked(
                f"connector {config.connector!r} is paid; set allow_{config.connector}=True to opt in"
            )
        raise LiveRunBlocked(
            f"connector {config.connector!r} not in allow-list {sorted(allowed)}"
        )

    if not is_live_enabled(config, env=env):
        raise LiveRunBlocked(
            f"live run not enabled — set {config.enabled_env_var}=1 to opt in"
        )


@dataclass
class LiveRunBudget:
    """Per-run accumulator for a single live smoke.

    Convertible to a ledger-compatible summary via
    :meth:`to_ledger_summary` so callers can hand the result straight
    to :func:`pipeline2_discovery.casegraph.ledger.build_run_ledger_entry`.
    """

    connector: str = ""
    query_count: int = 0
    result_count: int = 0
    wallclock_seconds: float = 0.0
    api_calls: Dict[str, int] = field(default_factory=lambda: dict(DEFAULT_API_CALLS))
    estimated_cost_usd: float = 0.0
    notes: List[str] = field(default_factory=list)

    def record_query(self, count: int = 1) -> None:
        self.query_count += int(count)
        if self.connector:
            self.api_calls[self.connector] = self.api_calls.get(self.connector, 0) + int(count)
            self.refresh_cost()

    def record_results(self, count: int) -> None:
        self.result_count += int(count)

    def record_wallclock(self, seconds: float) -> None:
        self.wallclock_seconds = round(self.wallclock_seconds + float(seconds), 4)

    def refresh_cost(self, per_call_overrides: Optional[Dict[str, float]] = None) -> None:
        self.estimated_cost_usd = estimate_cost(self.api_calls, per_call_overrides)

    def to_ledger_summary(self) -> Dict[str, Any]:
        """Returns a dict shaped for the ledger entry. Safe to feed
        into ``build_run_ledger_entry(..., api_calls=summary["api_calls"], ...)``.
        """
        return {
            "connector": self.connector,
            "query_count": self.query_count,
            "result_count": self.result_count,
            "wallclock_seconds": round(self.wallclock_seconds, 4),
            "api_calls": dict(self.api_calls),
            "estimated_cost_usd": round(self.estimated_cost_usd, 4),
            "notes": list(self.notes),
        }


def safe_live_budget_for(config: LiveRunConfig) -> LiveRunBudget:
    """Construct a fresh :class:`LiveRunBudget` initialized to the
    config's connector. Pure — no side effects, no network."""
    return LiveRunBudget(connector=config.connector)


__all__ = [
    "ALLOWED_FREE_CONNECTORS",
    "ALL_KNOWN_CONNECTORS",
    "DEFAULT_ENV_VAR",
    "LiveRunBlocked",
    "LiveRunBudget",
    "LiveRunConfig",
    "MAX_QUERIES_HARD_CAP",
    "MAX_RESULTS_HARD_CAP",
    "PAID_CONNECTORS",
    "is_live_enabled",
    "safe_live_budget_for",
    "validate_live_run",
]
