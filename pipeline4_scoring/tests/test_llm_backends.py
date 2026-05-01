"""Unit tests for llm_backends.py — JSON cleanup + truncation repair.

Covers:
- ``clean_llm_output`` — strips markdown fences, ``<think>`` blocks,
  prose around JSON, with a brace-counting walker that handles
  strings + escapes.
- ``repair_truncated_json`` — best-effort repair for output-token-cap
  truncation. Drops partial trailing entries, closes open braces /
  brackets so the result parses.
"""
from __future__ import annotations

import json

import pytest

from llm_backends import clean_llm_output, repair_truncated_json


# ---- clean_llm_output ---------------------------------------------------


def test_clean_llm_output_strips_markdown_json_fence():
    raw = '```json\n{"a": 1}\n```'
    cleaned = clean_llm_output(raw)
    assert json.loads(cleaned) == {"a": 1}


def test_clean_llm_output_strips_plain_fence():
    raw = '```\n{"a": 1}\n```'
    cleaned = clean_llm_output(raw)
    assert json.loads(cleaned) == {"a": 1}


def test_clean_llm_output_strips_think_block():
    raw = '<think>I should output JSON</think>\n{"verdict": "PRODUCE"}'
    cleaned = clean_llm_output(raw)
    assert json.loads(cleaned) == {"verdict": "PRODUCE"}


def test_clean_llm_output_strips_prose_before_json():
    raw = 'Sure, here is the JSON:\n\n{"a": 1, "b": 2}'
    cleaned = clean_llm_output(raw)
    assert json.loads(cleaned) == {"a": 1, "b": 2}


def test_clean_llm_output_strips_prose_after_json():
    raw = '{"a": 1}\n\nLet me know if you need anything else.'
    cleaned = clean_llm_output(raw)
    assert json.loads(cleaned) == {"a": 1}


def test_clean_llm_output_handles_strings_with_braces():
    """The brace-counting walker must respect string boundaries —
    a `}` inside a string must not close the outer object."""
    raw = '{"text": "this } looks like a close but isn\'t", "n": 2}'
    cleaned = clean_llm_output(raw)
    parsed = json.loads(cleaned)
    assert parsed["n"] == 2
    assert "}" in parsed["text"]


def test_clean_llm_output_handles_escaped_quotes():
    raw = '{"text": "he said \\"hello\\"", "n": 1}'
    cleaned = clean_llm_output(raw)
    parsed = json.loads(cleaned)
    assert parsed["n"] == 1


def test_clean_llm_output_returns_empty_for_empty_input():
    assert clean_llm_output("") == ""
    assert clean_llm_output(None) == ""


def test_clean_llm_output_handles_empty_after_strip():
    assert clean_llm_output("   \n  ") == ""


# ---- repair_truncated_json ----------------------------------------------


def test_repair_truncated_json_returns_none_for_empty():
    assert repair_truncated_json("") is None
    assert repair_truncated_json(None) is None
    assert repair_truncated_json("   ") is None


def test_repair_truncated_json_returns_none_for_non_json():
    assert repair_truncated_json("hello world") is None
    assert repair_truncated_json("not even close") is None


def test_repair_truncated_json_returns_none_for_already_balanced():
    """If the JSON is balanced but parse failed for another reason
    (bad token, whatever), repair returns None — caller falls through
    to LLMError."""
    balanced = '{"a": 1, "b": [1, 2, 3]}'
    # Already balanced — returns None (no repair needed/possible)
    assert repair_truncated_json(balanced) is None


def test_repair_truncated_json_drops_partial_array_entry():
    """Most common Pass 1 truncation: cap hit mid-entry of an array."""
    truncated = '{"timeline": [{"a": 1}, {"a": 2}], "moments": [{"b": 1}, {"b": 2}, {"b'
    repaired = repair_truncated_json(truncated)
    assert repaired is not None
    parsed = json.loads(repaired)
    assert "moments" in parsed
    assert parsed["moments"] == [{"b": 1}, {"b": 2}]
    assert "timeline" in parsed


def test_repair_truncated_json_drops_partial_string_in_value():
    """Truncation mid-string: the comma walker won't find a safe cut
    inside the string, so it falls back to closing whatever's open."""
    truncated = '{"a": 1, "description": "this is a long descrip'
    repaired = repair_truncated_json(truncated)
    # Either a parseable repair (if the algorithm cuts before the open
    # string) or None — both are acceptable. The contract is "don't
    # crash and don't return invalid JSON".
    if repaired is not None:
        # Must be parseable.
        json.loads(repaired)


def test_repair_truncated_json_handles_nested_arrays():
    truncated = '{"x": [[1, 2], [3, 4], [5, 6'
    repaired = repair_truncated_json(truncated)
    assert repaired is not None
    parsed = json.loads(repaired)
    # The trailing partial inner array gets dropped + outer closes
    assert isinstance(parsed["x"], list)


def test_repair_truncated_json_handles_pass1_shape():
    """Realistic Pass 1 truncation — common production shape."""
    truncated = '''{"timeline": [{"source_idx": 0, "timestamp_sec": 1.0}], "moments": [{"source_idx": 0, "timestamp_sec": 12.0, "type": "contradiction", "description": "x", "provisional_importance": "critical"}, {"source_idx": 0, "timestamp_sec": 45.0, "type": "reveal", "description": "y", "provisional_importance": "high"}, {"source_idx": 0, "timestamp_sec": 90.0, "type": "emotional'''
    repaired = repair_truncated_json(truncated)
    assert repaired is not None
    parsed = json.loads(repaired)
    # Should preserve the two complete moments, drop the partial third.
    assert len(parsed["moments"]) == 2
    assert parsed["moments"][0]["type"] == "contradiction"
    assert parsed["moments"][1]["type"] == "reveal"


def test_repair_truncated_json_returns_parseable_output():
    """Strong invariant: when repair returns non-None, the result must
    parse successfully via json.loads."""
    truncations = [
        '{"a": [1, 2',
        '{"k": "v", "list": [{"x": 1}, {"x": 2}, {"',
        '[{"a": 1}, {"a": 2}, {"a',
        '{"nested": {"deep": [1, 2, 3',
    ]
    for t in truncations:
        repaired = repair_truncated_json(t)
        if repaired is not None:
            json.loads(repaired)  # must not raise


def test_repair_truncated_json_does_not_modify_balanced_after_close():
    """If a top-level object is closed before junk, the repair returns
    None or returns the balanced prefix."""
    balanced = '{"a": 1}garbage'
    repaired = repair_truncated_json(balanced)
    # Either None (already balanced; no repair needed) or just the
    # balanced prefix '{"a": 1}'. Both acceptable.
    if repaired is not None:
        parsed = json.loads(repaired)
        assert parsed == {"a": 1}


def test_repair_truncated_json_handles_string_with_braces():
    """Strings containing `{` or `[` must not throw off the walker."""
    truncated = '{"text": "look at this { weird } string", "moments": [{"x":1}, {"y'
    repaired = repair_truncated_json(truncated)
    assert repaired is not None
    parsed = json.loads(repaired)
    assert parsed["text"] == "look at this { weird } string"
    assert parsed["moments"] == [{"x": 1}]


def test_repair_truncated_json_handles_array_root():
    truncated = '[{"a": 1}, {"a": 2}, {"a'
    repaired = repair_truncated_json(truncated)
    assert repaired is not None
    parsed = json.loads(repaired)
    assert parsed == [{"a": 1}, {"a": 2}]
