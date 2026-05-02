"""Tests for the new observability + JSON-repair flow added to
pipeline4_score in Tier 1 #1.

Covers:
- ``_log_response_size`` warns when response is at/above the
  configured cap fraction (P4_NEAR_CAP_WARN_FRAC).
- ``_log_response_size`` does not warn for responses well under cap.
- ``_parse_with_repair`` returns parsed JSON when the input is
  already valid.
- ``_parse_with_repair`` falls back to ``repair_truncated_json`` and
  succeeds when input is truncated mid-array.
- ``_parse_with_repair`` raises ``LLMError`` when input is
  unrepairable.
- Default token caps come from env vars when set, fall back to
  module defaults otherwise.
"""
from __future__ import annotations

import importlib
import json

import pytest

from pipeline4_score import (
    DEFAULT_PASS1_MAX_TOKENS,
    DEFAULT_PASS2_MAX_TOKENS,
    NEAR_CAP_WARN_FRAC,
    _log_response_size,
    _parse_with_repair,
)
from llm_backends import LLMError


def test_default_pass1_max_tokens_is_at_least_16k():
    """Pass 1 cap must be >= 16000 — anything lower will clip
    high-density transcripts (see prompt caps: 60 moments + 100
    timeline + 20 emotional_arc ≈ 9k+ tokens, with margin)."""
    assert DEFAULT_PASS1_MAX_TOKENS >= 16000


def test_default_pass2_max_tokens_default_3000():
    assert DEFAULT_PASS2_MAX_TOKENS >= 3000


def test_near_cap_warn_frac_default_below_one():
    assert 0 < NEAR_CAP_WARN_FRAC < 1


def test_log_response_size_warns_when_near_cap(capsys):
    # Response of length 4000 chars ≈ 1000 tokens, against a 1000-token
    # cap → 100% of cap → must warn.
    raw = "x" * 4000
    _log_response_size("Pass 1", raw, max_tokens=1000, elapsed=1.5)
    captured = capsys.readouterr()
    assert "[WARN]" in captured.out
    assert "near limit" in captured.out


def test_log_response_size_quiet_when_far_under_cap(capsys):
    # Response of length 400 chars ≈ 100 tokens, against a 16000-token
    # cap → ~0.6% → must NOT warn.
    raw = "x" * 400
    _log_response_size("Pass 1", raw, max_tokens=16000, elapsed=1.5)
    captured = capsys.readouterr()
    assert "[WARN]" not in captured.out
    assert "Returned in" in captured.out


def test_parse_with_repair_returns_valid_json_unchanged(capsys):
    raw = '{"a": 1, "b": [1, 2, 3]}'
    parsed = _parse_with_repair(raw, "Pass 1")
    assert parsed == {"a": 1, "b": [1, 2, 3]}
    captured = capsys.readouterr()
    # No repair-warning when no repair needed
    assert "JSON was truncated" not in captured.out


def test_parse_with_repair_falls_back_to_repair_for_truncated(capsys):
    truncated = '{"timeline": [{"a": 1}], "moments": [{"b": 1}, {"b": 2}, {"b'
    parsed = _parse_with_repair(truncated, "Pass 1")
    assert parsed["moments"] == [{"b": 1}, {"b": 2}]
    captured = capsys.readouterr()
    # Repair fired — must surface a WARN to the operator
    assert "[WARN]" in captured.out
    assert "JSON was truncated" in captured.out


def test_parse_with_repair_raises_on_unrepairable():
    with pytest.raises(LLMError) as exc_info:
        _parse_with_repair("hello world {", "Pass 1")
    assert "JSON parse failed" in str(exc_info.value)


def test_parse_with_repair_includes_first_500_chars_on_failure():
    """Diagnostic detail must be preserved: when parse fails, the
    error message should include the first 500 chars of the raw
    response."""
    raw = "this is not json at all, just prose"
    with pytest.raises(LLMError) as exc_info:
        _parse_with_repair(raw, "Pass 2")
    assert "First 500 chars" in str(exc_info.value)
    assert "Pass 2" in str(exc_info.value)


def test_p4_pass1_max_tokens_env_var_override(monkeypatch):
    """Setting P4_PASS1_MAX_TOKENS should override the default after
    module reload."""
    monkeypatch.setenv("P4_PASS1_MAX_TOKENS", "32000")
    import pipeline4_score
    importlib.reload(pipeline4_score)
    assert pipeline4_score.DEFAULT_PASS1_MAX_TOKENS == 32000
    # Clean up — reload back to default for other tests
    monkeypatch.delenv("P4_PASS1_MAX_TOKENS")
    importlib.reload(pipeline4_score)
