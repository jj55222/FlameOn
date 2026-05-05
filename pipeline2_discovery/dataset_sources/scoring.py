"""Shared scoring + ranking helpers across dataset-source lanes.

Per-lane scoring rubrics live in their own modules
(sfchronicle_pursuits.py, muckrock_leads.py); this module owns the
score → grade mapping, the evidence-strength classifier, the
federal-agency predicate, and the operational ``rank_key`` used by
the CLI to break score ties.
"""
from __future__ import annotations

from typing import Iterable, Optional, Sequence, Tuple


# ---- grade + strength -----------------------------------------------


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


# ---- federal-agency predicate ---------------------------------------


# Substring matches against the lower-cased agency_name. Federal
# agencies have very different transparency surfaces from municipal
# / county / state agencies and aren't currently in scope for the
# portal-live extractor path; operators can suppress them via the
# CLI's --exclude-federal-agencies flag.
_FEDERAL_AGENCY_PATTERNS: Tuple[str, ...] = (
    "u.s. border patrol",
    "us border patrol",
    "border patrol",
    "customs and border protection",
    " cbp ",
    "cbp ",
    " cbp",
    "fbi",
    "drug enforcement administration",
    " dea ",
    "dea ",
    " dea",
    "atf",
    "homeland security",
    "u.s. marshals",
    "us marshals",
    "us marshal service",
    "u.s. marshal service",
    "secret service",
)


def is_federal_agency(agency_name: Optional[str]) -> bool:
    """True if ``agency_name`` matches one of the documented federal
    agency name patterns. Case-insensitive substring match against
    the lower-cased name. Returns False for None/empty."""
    if not agency_name:
        return False
    lowered = " " + agency_name.strip().lower() + " "
    for pat in _FEDERAL_AGENCY_PATTERNS:
        if pat.startswith(" ") or pat.endswith(" "):
            if pat in lowered:
                return True
        else:
            if pat in lowered:
                return True
    return False


# ---- operational ranking --------------------------------------------


def rank_key(
    candidate,
    *,
    target_states: Sequence[str] = (),
) -> Tuple:
    """Sort key for the CLI's top-N selection.

    The raw score is the primary key, but with the SF Chronicle
    dataset many enriched rows saturate at the rubric ceiling (~20),
    so the top-25 by score alone is effectively CSV row order. This
    helper threads in operational tie-breaks so the saturated-A
    pool ranks meaningfully:

      1. score (descending)
      2. real subject name present (placeholder names lose the tie)
      3. news_urls count (descending)
      4. fatality_count (descending; missing → 0)
      5. incident_date parsed and recent (None last; among parsed,
         lex-descending on the ISO string)
      6. non-federal agency before federal (uses is_federal_agency)
      7. target state before non-target state
      8. candidate_id ascending (stable fallback)

    All seven dimensions return values where SMALLER is better, so
    the returned tuple can be passed directly to ``sorted(...,
    key=rank_key)``.
    """
    target_states_upper = frozenset(s.upper() for s in (target_states or ()))

    score = -getattr(candidate, "packet_priority_score", 0)

    name = getattr(candidate, "subject_name", None)
    real_name = 0 if (name and name.strip()) else 1

    news_urls = getattr(candidate, "news_urls", None) or []
    news_count = -len(news_urls)

    fatality = getattr(candidate, "fatality_count", None)
    fatality_key = -(fatality if isinstance(fatality, int) else 0)

    incident_date = getattr(candidate, "incident_date", None)
    if incident_date:
        # ISO yyyy-mm-dd lex-sorts as date-sort; negate by storing
        # (0, -ord-tuple). Simpler: use (0, "~~~~~~~~~~" if missing)
        # — but we already gated on `if incident_date`, so use
        # (0, -lex_value). Trick: convert to a sortable string and
        # use a sentinel for "more recent first".
        date_key = (0, "".join(chr(255 - ord(c)) for c in incident_date))
    else:
        date_key = (1, "")

    agency = getattr(candidate, "agency_name", None)
    federal_key = 1 if is_federal_agency(agency) else 0

    state = (getattr(candidate, "jurisdiction_state", None) or "").upper()
    state_key = 0 if state in target_states_upper else 1

    cid = getattr(candidate, "candidate_id", "") or ""

    return (
        score,
        real_name,
        news_count,
        fatality_key,
        date_key,
        federal_key,
        state_key,
        cid,
    )


__all__ = [
    "assign_grade",
    "evidence_strength_from_grade",
    "is_federal_agency",
    "rank_key",
]
