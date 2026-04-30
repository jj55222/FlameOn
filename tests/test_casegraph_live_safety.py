"""LIVE0 — Live-safety preflight tests.

Asserts the safety policy:
- live runs are disabled by default
- only enabled when the configured env var equals ``"1"``
- max_queries / max_results clamps are enforced
- Brave / Firecrawl rejected unless explicitly allowed
- LLM / downloads / scraping / transcript fetching always rejected
- LiveRunBudget converts to a ledger-compatible api_calls dict
- the safety module makes zero network calls
"""
import pytest

from pipeline2_discovery.casegraph import (
    DEFAULT_API_CALLS,
    LiveRunBlocked,
    LiveRunBudget,
    LiveRunConfig,
    is_live_enabled,
    safe_live_budget_for,
    validate_live_run,
)
from pipeline2_discovery.casegraph.live_safety import (
    ALLOWED_FREE_CONNECTORS,
    DEFAULT_ENV_VAR,
    MAX_QUERIES_HARD_CAP,
    MAX_RESULTS_HARD_CAP,
    PAID_CONNECTORS,
)


# ---- enablement ------------------------------------------------------------


def test_live_disabled_by_default():
    cfg = LiveRunConfig(connector="courtlistener")
    assert is_live_enabled(cfg, env={}) is False


def test_live_disabled_when_env_var_is_zero_or_other_truthy_string():
    cfg = LiveRunConfig(connector="courtlistener")
    for value in ("", "0", "false", "no", "true", "yes", "ok"):
        assert is_live_enabled(cfg, env={DEFAULT_ENV_VAR: value}) is False, (
            f"value {value!r} should not enable live"
        )


def test_live_enabled_only_when_env_var_equals_string_one():
    cfg = LiveRunConfig(connector="courtlistener")
    assert is_live_enabled(cfg, env={DEFAULT_ENV_VAR: "1"}) is True


def test_live_uses_custom_env_var_when_overridden():
    cfg = LiveRunConfig(connector="courtlistener", enabled_env_var="MY_CUSTOM_GATE")
    assert is_live_enabled(cfg, env={"MY_CUSTOM_GATE": "1"}) is True
    assert is_live_enabled(cfg, env={DEFAULT_ENV_VAR: "1"}) is False


def test_validate_live_run_raises_when_env_gate_not_set():
    cfg = LiveRunConfig(connector="courtlistener")
    with pytest.raises(LiveRunBlocked, match="live run not enabled"):
        validate_live_run(cfg, env={})


def test_validate_live_run_passes_when_env_gate_set_and_safe():
    cfg = LiveRunConfig(connector="courtlistener", max_queries=1, max_results=5)
    validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})  # no raise


# ---- caps ------------------------------------------------------------------


def test_max_queries_above_hard_cap_rejects():
    cfg = LiveRunConfig(
        connector="courtlistener",
        max_queries=MAX_QUERIES_HARD_CAP + 1,
        max_results=5,
    )
    with pytest.raises(LiveRunBlocked, match="max_queries"):
        validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})


def test_max_queries_below_one_rejects():
    cfg = LiveRunConfig(connector="courtlistener", max_queries=0, max_results=5)
    with pytest.raises(LiveRunBlocked, match="max_queries"):
        validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})


def test_max_results_above_hard_cap_rejects():
    cfg = LiveRunConfig(
        connector="courtlistener",
        max_queries=1,
        max_results=MAX_RESULTS_HARD_CAP + 1,
    )
    with pytest.raises(LiveRunBlocked, match="max_results"):
        validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})


def test_max_results_below_one_rejects():
    cfg = LiveRunConfig(connector="courtlistener", max_queries=1, max_results=0)
    with pytest.raises(LiveRunBlocked, match="max_results"):
        validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})


def test_caps_at_exact_hard_limits_pass():
    cfg = LiveRunConfig(
        connector="courtlistener",
        max_queries=MAX_QUERIES_HARD_CAP,
        max_results=MAX_RESULTS_HARD_CAP,
    )
    validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})  # no raise


# ---- connector allow-list --------------------------------------------------


@pytest.mark.parametrize("connector", sorted(ALLOWED_FREE_CONNECTORS))
def test_each_free_connector_passes(connector):
    cfg = LiveRunConfig(connector=connector, max_queries=1, max_results=5)
    validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})  # no raise


def test_brave_rejected_by_default():
    cfg = LiveRunConfig(connector="brave", max_queries=1, max_results=5)
    with pytest.raises(LiveRunBlocked, match="brave.*paid|allow_brave"):
        validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})


def test_firecrawl_rejected_by_default():
    cfg = LiveRunConfig(connector="firecrawl", max_queries=1, max_results=5)
    with pytest.raises(LiveRunBlocked, match="firecrawl.*paid|allow_firecrawl"):
        validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})


def test_brave_passes_when_explicitly_allowed():
    cfg = LiveRunConfig(connector="brave", max_queries=1, max_results=5, allow_brave=True)
    validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})  # no raise


def test_firecrawl_passes_when_explicitly_allowed():
    cfg = LiveRunConfig(
        connector="firecrawl", max_queries=1, max_results=5, allow_firecrawl=True
    )
    validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})  # no raise


def test_unknown_connector_rejected_unless_explicitly_added():
    cfg = LiveRunConfig(connector="some_future_thing", max_queries=1, max_results=5)
    with pytest.raises(LiveRunBlocked, match="not in allow-list"):
        validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})

    cfg_allowed = LiveRunConfig(
        connector="some_future_thing",
        max_queries=1,
        max_results=5,
        additional_allowed_connectors={"some_future_thing"},
    )
    validate_live_run(cfg_allowed, env={DEFAULT_ENV_VAR: "1"})  # no raise


# ---- disallowed activities -------------------------------------------------


def test_downloads_always_rejected():
    cfg = LiveRunConfig(connector="courtlistener", max_queries=1, allow_downloads=True)
    with pytest.raises(LiveRunBlocked, match="downloads"):
        validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})


def test_scraping_always_rejected():
    cfg = LiveRunConfig(connector="courtlistener", max_queries=1, allow_scraping=True)
    with pytest.raises(LiveRunBlocked, match="scraping"):
        validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})


def test_llm_always_rejected():
    cfg = LiveRunConfig(connector="courtlistener", max_queries=1, allow_llm=True)
    with pytest.raises(LiveRunBlocked, match="LLM"):
        validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})


def test_transcript_fetching_always_rejected():
    cfg = LiveRunConfig(
        connector="courtlistener", max_queries=1, allow_transcript_fetch=True
    )
    with pytest.raises(LiveRunBlocked, match="transcript"):
        validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})


def test_disallowed_features_reject_before_env_gate():
    """A misconfigured request to enable downloads should fail loudly
    even when the env gate is not set, so dev mistakes surface in
    every environment."""
    cfg = LiveRunConfig(connector="courtlistener", allow_downloads=True)
    with pytest.raises(LiveRunBlocked, match="downloads"):
        validate_live_run(cfg, env={})  # env gate unset


# ---- LiveRunBudget --------------------------------------------------------


def test_budget_starts_with_canonical_zero_api_calls():
    budget = LiveRunBudget(connector="courtlistener")
    for provider in DEFAULT_API_CALLS:
        assert budget.api_calls[provider] == 0
    assert budget.query_count == 0
    assert budget.result_count == 0
    assert budget.wallclock_seconds == 0.0
    assert budget.estimated_cost_usd == 0.0


def test_budget_record_query_increments_api_calls_for_connector():
    budget = LiveRunBudget(connector="courtlistener")
    budget.record_query(1)
    assert budget.query_count == 1
    assert budget.api_calls["courtlistener"] == 1
    assert budget.api_calls["brave"] == 0


def test_budget_record_query_for_brave_updates_cost_estimate():
    budget = LiveRunBudget(connector="brave")
    budget.record_query(3)
    assert budget.api_calls["brave"] == 3
    assert budget.estimated_cost_usd == 0.015  # 3 * $0.005


def test_budget_record_results_and_wallclock():
    budget = LiveRunBudget(connector="muckrock")
    budget.record_results(4)
    budget.record_wallclock(1.5)
    budget.record_wallclock(0.25)
    assert budget.result_count == 4
    assert budget.wallclock_seconds == 1.75


def test_budget_to_ledger_summary_carries_required_fields():
    budget = LiveRunBudget(connector="documentcloud")
    budget.record_query(1)
    budget.record_results(2)
    budget.record_wallclock(0.4)
    summary = budget.to_ledger_summary()
    assert summary["connector"] == "documentcloud"
    assert summary["query_count"] == 1
    assert summary["result_count"] == 2
    assert summary["wallclock_seconds"] == 0.4
    assert summary["api_calls"]["documentcloud"] == 1
    assert summary["estimated_cost_usd"] == 0.0
    assert isinstance(summary["notes"], list)


def test_budget_to_ledger_summary_api_calls_match_default_keys():
    budget = LiveRunBudget(connector="youtube")
    summary = budget.to_ledger_summary()
    assert set(summary["api_calls"].keys()) == set(DEFAULT_API_CALLS.keys())


def test_safe_live_budget_for_initializes_connector():
    cfg = LiveRunConfig(connector="muckrock")
    budget = safe_live_budget_for(cfg)
    assert budget.connector == "muckrock"
    assert budget.api_calls["muckrock"] == 0


# ---- network invariant ----------------------------------------------------


def test_live_safety_module_makes_zero_network_calls(monkeypatch):
    """Constructing configs, validating them, and tracking budgets must
    never make a live HTTP request."""
    import requests

    calls = []
    original_get = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original_get(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    for connector in sorted(ALLOWED_FREE_CONNECTORS):
        cfg = LiveRunConfig(connector=connector, max_queries=1, max_results=5)
        is_live_enabled(cfg, env={DEFAULT_ENV_VAR: "1"})
        validate_live_run(cfg, env={DEFAULT_ENV_VAR: "1"})
        budget = safe_live_budget_for(cfg)
        budget.record_query(1)
        budget.record_results(3)
        budget.to_ledger_summary()

    assert calls == [], f"live_safety module made {len(calls)} live HTTP call(s)"


def test_paid_connectors_set_is_disjoint_from_allowed_free_connectors():
    """The two allow-lists must not overlap — a connector cannot be
    simultaneously free-and-paid."""
    assert ALLOWED_FREE_CONNECTORS.isdisjoint(PAID_CONNECTORS)
