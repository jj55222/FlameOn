"""Unit tests for Pass 1 + Pass 2 validators in pipeline4_score.py.

Covers:
- ``validate_pass1`` — drops invalid moment types, sanity-checks
  timestamps against source duration, coerces importance, caps
  moments at 60.
- ``validate_pass2`` — drops final_moments without matching Pass 1
  timestamps (anti-hallucination guard), coerces verdict, coerces
  arc_recommendation, clamps confidence to [0,1].
"""
from __future__ import annotations

import pytest

from pipeline4_score import validate_pass1, validate_pass2


# ---- validate_pass1 ----------------------------------------------------


def _merged(durations=None):
    """Minimal merged-transcript stub for validate_pass1."""
    if durations is None:
        durations = [600.0]  # one source, 10 min
    return {
        "case_id": "test_case",
        "sources": [
            {"source_idx": i, "duration_sec": d} for i, d in enumerate(durations)
        ],
        "segments": [],
        "total_duration_sec": sum(durations),
    }


def test_validate_pass1_fills_missing_keys():
    """Should add empty defaults for missing keys without crashing."""
    pass1 = {}
    out = validate_pass1(pass1, _merged())
    for key in (
        "timeline",
        "moments",
        "contradictions",
        "speaker_dynamics",
        "emotional_arc",
        "factual_anchors",
    ):
        assert key in out
    assert out["detected_structure_hint"] is None


def test_validate_pass1_drops_invalid_moment_type():
    pass1 = {
        "moments": [
            {"source_idx": 0, "timestamp_sec": 100.0, "type": "contradiction",
             "provisional_importance": "high"},
            {"source_idx": 0, "timestamp_sec": 200.0, "type": "garbage_type",
             "provisional_importance": "high"},
        ]
    }
    out = validate_pass1(pass1, _merged())
    assert len(out["moments"]) == 1
    assert out["moments"][0]["type"] == "contradiction"


def test_validate_pass1_drops_unknown_source_idx():
    pass1 = {
        "moments": [
            {"source_idx": 0, "timestamp_sec": 50.0, "type": "reveal",
             "provisional_importance": "high"},
            {"source_idx": 99, "timestamp_sec": 50.0, "type": "reveal",
             "provisional_importance": "high"},
        ]
    }
    out = validate_pass1(pass1, _merged())
    assert len(out["moments"]) == 1
    assert out["moments"][0]["source_idx"] == 0


def test_validate_pass1_drops_negative_timestamp():
    pass1 = {
        "moments": [
            {"source_idx": 0, "timestamp_sec": -5.0, "type": "reveal",
             "provisional_importance": "high"},
            {"source_idx": 0, "timestamp_sec": 100.0, "type": "reveal",
             "provisional_importance": "high"},
        ]
    }
    out = validate_pass1(pass1, _merged())
    assert len(out["moments"]) == 1
    assert out["moments"][0]["timestamp_sec"] == 100.0


def test_validate_pass1_drops_timestamp_past_source_duration():
    """A 10-min source's max_duration is 600s; 700s is past +5s tolerance."""
    pass1 = {
        "moments": [
            {"source_idx": 0, "timestamp_sec": 700.0, "type": "reveal",
             "provisional_importance": "high"},
            {"source_idx": 0, "timestamp_sec": 599.0, "type": "reveal",
             "provisional_importance": "high"},
        ]
    }
    out = validate_pass1(pass1, _merged([600.0]))
    assert len(out["moments"]) == 1
    assert out["moments"][0]["timestamp_sec"] == 599.0


def test_validate_pass1_allows_5sec_tolerance_past_duration():
    """A timestamp 4s past the source's reported duration should still
    pass — accommodates rounding in the transcript."""
    pass1 = {
        "moments": [
            {"source_idx": 0, "timestamp_sec": 604.0, "type": "reveal",
             "provisional_importance": "high"},
        ]
    }
    out = validate_pass1(pass1, _merged([600.0]))
    assert len(out["moments"]) == 1


def test_validate_pass1_coerces_invalid_importance_to_medium():
    pass1 = {
        "moments": [
            {"source_idx": 0, "timestamp_sec": 100.0, "type": "reveal",
             "provisional_importance": "EXTREME"},
        ]
    }
    out = validate_pass1(pass1, _merged())
    assert out["moments"][0]["provisional_importance"] == "medium"


def test_validate_pass1_caps_moments_at_60():
    """Even if Gemini returns 100 moments, validate_pass1 truncates."""
    pass1 = {
        "moments": [
            {"source_idx": 0, "timestamp_sec": float(i % 500), "type": "tension_shift",
             "provisional_importance": "medium"}
            for i in range(100)
        ]
    }
    out = validate_pass1(pass1, _merged())
    assert len(out["moments"]) == 60


def test_validate_pass1_handles_missing_timestamp():
    pass1 = {
        "moments": [
            {"source_idx": 0, "type": "reveal", "provisional_importance": "high"},
        ]
    }
    out = validate_pass1(pass1, _merged())
    assert len(out["moments"]) == 0


# ---- validate_pass2 ----------------------------------------------------


def _pass1_with_moment(source_idx=0, timestamp=100.0):
    return {
        "moments": [
            {
                "source_idx": source_idx,
                "timestamp_sec": timestamp,
                "type": "contradiction",
                "provisional_importance": "high",
            }
        ],
    }


def test_validate_pass2_coerces_invalid_verdict_to_hold():
    pass2 = {"verdict": "MAYBE", "final_moments": []}
    out = validate_pass2(pass2, _pass1_with_moment())
    assert out["verdict"] == "HOLD"


def test_validate_pass2_keeps_valid_verdict():
    for v in ("PRODUCE", "HOLD", "SKIP"):
        out = validate_pass2({"verdict": v, "final_moments": []}, _pass1_with_moment())
        assert out["verdict"] == v


def test_validate_pass2_uses_pass1_hint_for_invalid_arc():
    pass1 = _pass1_with_moment()
    pass1["detected_structure_hint"] = "cold_open"
    pass2 = {"verdict": "HOLD", "narrative_arc_recommendation": "garbage_arc",
             "final_moments": []}
    out = validate_pass2(pass2, pass1)
    assert out["narrative_arc_recommendation"] == "cold_open"


def test_validate_pass2_falls_back_to_chronological_when_no_hint():
    pass1 = _pass1_with_moment()  # no detected_structure_hint
    pass2 = {"verdict": "HOLD", "narrative_arc_recommendation": "x",
             "final_moments": []}
    out = validate_pass2(pass2, pass1)
    assert out["narrative_arc_recommendation"] == "chronological"


def test_validate_pass2_clamps_confidence():
    out = validate_pass2(
        {"verdict": "HOLD", "confidence": 1.5, "final_moments": []},
        _pass1_with_moment(),
    )
    assert out["confidence"] == 1.0
    out = validate_pass2(
        {"verdict": "HOLD", "confidence": -0.5, "final_moments": []},
        _pass1_with_moment(),
    )
    assert out["confidence"] == 0.0


def test_validate_pass2_clamps_non_numeric_confidence():
    out = validate_pass2(
        {"verdict": "HOLD", "confidence": "high", "final_moments": []},
        _pass1_with_moment(),
    )
    assert out["confidence"] == 0.5


def test_validate_pass2_drops_hallucinated_final_moment():
    """The anti-hallucination guard: a final_moment whose timestamp
    doesn't match any Pass 1 entry within 2s must be dropped."""
    pass1 = _pass1_with_moment(source_idx=0, timestamp=100.0)
    pass2 = {
        "verdict": "PRODUCE",
        "final_moments": [
            # This one matches Pass 1's (0, 100.0) entry exactly
            {"moment_type": "contradiction", "source_idx": 0, "timestamp_sec": 100.0,
             "importance": "high", "description": "real"},
            # This one does NOT match anything in Pass 1
            {"moment_type": "reveal", "source_idx": 0, "timestamp_sec": 999.0,
             "importance": "high", "description": "hallucinated"},
        ],
    }
    out = validate_pass2(pass2, pass1)
    assert len(out["final_moments"]) == 1
    assert out["final_moments"][0]["description"] == "real"


def test_validate_pass2_allows_2sec_tolerance_match():
    """A final_moment at timestamp 101.5s should match a Pass 1 entry
    at 100.0s (within the ±2s tolerance)."""
    pass1 = _pass1_with_moment(source_idx=0, timestamp=100.0)
    pass2 = {
        "verdict": "HOLD",
        "final_moments": [
            {"moment_type": "contradiction", "source_idx": 0, "timestamp_sec": 101.5,
             "importance": "high"},
        ],
    }
    out = validate_pass2(pass2, pass1)
    assert len(out["final_moments"]) == 1


def test_validate_pass2_drops_moment_with_invalid_type():
    pass1 = _pass1_with_moment()
    pass2 = {
        "verdict": "HOLD",
        "final_moments": [
            {"moment_type": "garbage", "source_idx": 0, "timestamp_sec": 100.0,
             "importance": "high"},
        ],
    }
    out = validate_pass2(pass2, pass1)
    assert out["final_moments"] == []


def test_validate_pass2_coerces_importance_to_medium():
    pass1 = _pass1_with_moment()
    pass2 = {
        "verdict": "HOLD",
        "final_moments": [
            {"moment_type": "contradiction", "source_idx": 0, "timestamp_sec": 100.0,
             "importance": "EXTREME"},
        ],
    }
    out = validate_pass2(pass2, pass1)
    assert out["final_moments"][0]["importance"] == "medium"


def test_validate_pass2_sets_default_content_pitch_and_reasoning():
    pass1 = _pass1_with_moment()
    out = validate_pass2({"verdict": "HOLD", "final_moments": []}, pass1)
    assert "content_pitch" in out
    assert "reasoning_summary" in out


def test_validate_pass2_handles_missing_verdict():
    pass1 = _pass1_with_moment()
    out = validate_pass2({"final_moments": []}, pass1)
    assert out["verdict"] == "HOLD"
