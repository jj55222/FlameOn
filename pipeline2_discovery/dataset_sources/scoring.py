"""Shared scoring helpers across dataset-source lanes.

Per-lane scoring rubrics live in their own modules
(sfchronicle_pursuits.py, muckrock_leads.py); this module owns only
the score → grade mapping and the evidence-strength classifier so
they're consistent across lanes.
"""
from __future__ import annotations


def assign_grade(score: int) -> str:
    """Map a candidate's packet_priority_score to a single-letter
    grade. Thresholds match the spec:

      A : >= 9
      B : 6–8
      C : 3–5
      D : < 3
    """
    if score >= 9:
        return "A"
    if score >= 6:
        return "B"
    if score >= 3:
        return "C"
    return "D"


def evidence_strength_from_grade(grade: str) -> str:
    """``A`` → ``strong``, ``B`` → ``moderate``, anything else
    → ``weak``. Mirrors how a downstream packet author would read
    the grade."""
    if grade == "A":
        return "strong"
    if grade == "B":
        return "moderate"
    return "weak"


__all__ = ["assign_grade", "evidence_strength_from_grade"]
