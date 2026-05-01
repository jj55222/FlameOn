"""Unit tests for ``apply_resolution_gate`` -- the production-readiness
verdict ceiling layered above the four-subscore math.

These cover the gate function in isolation. End-to-end orchestration
(env-var read, resolution_status priority chain, Pass-2 downgrade clamp,
verdict JSON metadata) is covered separately under Batch 2 once the
orchestration layer is wired up.
"""
from __future__ import annotations

import pytest

from scoring_math import (
    RESOLUTION_VERDICT_CEILING,
    VALID_RESOLUTION_STATUSES,
    apply_resolution_gate,
)


# ---- gate disabled (default) ---------------------------------------------


@pytest.mark.parametrize("verdict", ["PRODUCE", "HOLD", "SKIP"])
@pytest.mark.parametrize("status", list(VALID_RESOLUTION_STATUSES) + [None, "garbage"])
def test_gate_disabled_passes_all_verdicts_unchanged(verdict, status):
    """gate_enabled=False is the default. Every (verdict, status) pair
    must round-trip unchanged with gate_applied=False -- the gate is a
    no-op until the orchestration layer flips it on."""
    out, applied = apply_resolution_gate(verdict, status, gate_enabled=False)
    assert out == verdict
    assert applied is False


def test_gate_disabled_is_default_argument():
    """Caller can omit gate_enabled and get the off behaviour. Belt-and-
    suspenders against accidentally enabling the gate by forgetting the
    third arg in score_case."""
    out, applied = apply_resolution_gate("PRODUCE", "ongoing_or_unclear")
    assert out == "PRODUCE"
    assert applied is False


# ---- gate enabled, confirmed_final_outcome -------------------------------


def test_gate_enabled_confirmed_final_outcome_allows_produce():
    """Confirmed final outcome ceiling = PRODUCE. PRODUCE passes through."""
    out, applied = apply_resolution_gate(
        "PRODUCE", "confirmed_final_outcome", gate_enabled=True
    )
    assert out == "PRODUCE"
    assert applied is False


def test_gate_enabled_confirmed_final_outcome_allows_hold():
    out, applied = apply_resolution_gate(
        "HOLD", "confirmed_final_outcome", gate_enabled=True
    )
    assert out == "HOLD"
    assert applied is False


def test_gate_enabled_confirmed_final_outcome_allows_skip():
    out, applied = apply_resolution_gate(
        "SKIP", "confirmed_final_outcome", gate_enabled=True
    )
    assert out == "SKIP"
    assert applied is False


# ---- gate enabled, charges_filed_pending (caps at HOLD) ------------------


def test_gate_enabled_charges_filed_pending_caps_produce_to_hold():
    """The headline gate behaviour: a strong narrative case that is
    charged-but-not-yet-finally-adjudicated must not reach PRODUCE."""
    out, applied = apply_resolution_gate(
        "PRODUCE", "charges_filed_pending", gate_enabled=True
    )
    assert out == "HOLD"
    assert applied is True


def test_gate_enabled_charges_filed_pending_keeps_hold():
    out, applied = apply_resolution_gate(
        "HOLD", "charges_filed_pending", gate_enabled=True
    )
    assert out == "HOLD"
    assert applied is False


def test_gate_enabled_charges_filed_pending_keeps_skip():
    out, applied = apply_resolution_gate(
        "SKIP", "charges_filed_pending", gate_enabled=True
    )
    assert out == "SKIP"
    assert applied is False


# ---- gate enabled, ongoing_or_unclear (caps at SKIP) ---------------------


def test_gate_enabled_ongoing_or_unclear_caps_produce_to_skip():
    out, applied = apply_resolution_gate(
        "PRODUCE", "ongoing_or_unclear", gate_enabled=True
    )
    assert out == "SKIP"
    assert applied is True


def test_gate_enabled_ongoing_or_unclear_caps_hold_to_skip():
    out, applied = apply_resolution_gate(
        "HOLD", "ongoing_or_unclear", gate_enabled=True
    )
    assert out == "SKIP"
    assert applied is True


def test_gate_enabled_ongoing_or_unclear_keeps_skip():
    out, applied = apply_resolution_gate(
        "SKIP", "ongoing_or_unclear", gate_enabled=True
    )
    assert out == "SKIP"
    assert applied is False


# ---- gate enabled, missing (caps at SKIP, fail-closed) -------------------


def test_gate_enabled_missing_caps_produce_to_skip():
    """Missing resolution data -> SKIP. The fail-closed default that
    prevents unmeasurable cases from reaching PRODUCE."""
    out, applied = apply_resolution_gate(
        "PRODUCE", "missing", gate_enabled=True
    )
    assert out == "SKIP"
    assert applied is True


def test_gate_enabled_missing_caps_hold_to_skip():
    out, applied = apply_resolution_gate(
        "HOLD", "missing", gate_enabled=True
    )
    assert out == "SKIP"
    assert applied is True


def test_gate_enabled_missing_keeps_skip():
    out, applied = apply_resolution_gate(
        "SKIP", "missing", gate_enabled=True
    )
    assert out == "SKIP"
    assert applied is False


# ---- defensive: unknown / None status treated as missing -----------------


def test_gate_enabled_invalid_resolution_status_treated_as_missing():
    """Defensive: a typo / unknown enum value must not silently allow
    PRODUCE. Falls back to the missing ceiling (SKIP). Fail closed."""
    out, applied = apply_resolution_gate(
        "PRODUCE", "totally_made_up_status", gate_enabled=True
    )
    assert out == "SKIP"
    assert applied is True


def test_gate_enabled_none_resolution_status_treated_as_missing():
    """None -> treated as missing. Same fail-closed semantics."""
    out, applied = apply_resolution_gate("PRODUCE", None, gate_enabled=True)
    assert out == "SKIP"
    assert applied is True


# ---- gate_applied flag accuracy ------------------------------------------


def test_gate_applied_true_only_when_verdict_changes():
    """gate_applied accurately reports whether the ceiling actually fired.
    Audit-relevant: pre_gate_verdict + gate_applied is how we'll
    distinguish 'gate fired and changed verdict' from 'gate ran but
    verdict was already at-or-below ceiling'."""
    # Cap fires:
    _, applied_fires = apply_resolution_gate("PRODUCE", "missing", gate_enabled=True)
    assert applied_fires is True
    # Cap is no-op (already at ceiling):
    _, applied_noop_at = apply_resolution_gate("HOLD", "charges_filed_pending", gate_enabled=True)
    assert applied_noop_at is False
    # Cap is no-op (already below ceiling):
    _, applied_noop_below = apply_resolution_gate("SKIP", "charges_filed_pending", gate_enabled=True)
    assert applied_noop_below is False
    # Gate disabled: always False even when verdict-vs-ceiling would have fired:
    _, applied_disabled = apply_resolution_gate("PRODUCE", "missing", gate_enabled=False)
    assert applied_disabled is False


# ---- constant sanity (mirrored in test_scoring_math.py for redundancy) ---


def test_resolution_ceiling_keys_match_valid_statuses_set():
    """Cross-check: every status in VALID_RESOLUTION_STATUSES has a
    ceiling, and no orphan ceiling keys exist. A drift here is a fast-
    fail signal that the enum and the ceiling map have diverged."""
    assert set(RESOLUTION_VERDICT_CEILING) == set(VALID_RESOLUTION_STATUSES)


def test_resolution_ceiling_values_are_valid_verdicts():
    """Every ceiling must be a real verdict the rest of the pipeline
    knows how to emit. A typo (e.g. 'PRODOCE') would silently render
    apply_resolution_gate non-functional."""
    valid = {"PRODUCE", "HOLD", "SKIP"}
    for status, ceiling in RESOLUTION_VERDICT_CEILING.items():
        assert ceiling in valid, (
            f"Ceiling {ceiling!r} for status {status!r} is not a valid verdict"
        )
