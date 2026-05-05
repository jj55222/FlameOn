"""SF Chronicle fatal police pursuits dataset parser.

Source: SF Chronicle public CSV of fatal pursuit incidents (column
schema mirrors the dataset's published data dictionary). This module
is **pure**: it reads a local CSV (or pre-loaded list of dicts) and
emits normalized DatasetCandidate rows. It never fetches the web,
never calls portal-live, never downloads news URLs.

Expected CSV columns (subset; extras tolerated):
  date              YYYY-MM-DD or M/D/YYYY
  year              YYYY
  number_killed     integer
  name              decedent's name
  initial_reason    e.g. "traffic stop", "minor/no crime", "warrant"
  person_role       e.g. "driver", "passenger", "bystander", "officer"
  main_agency       e.g. "Phoenix Police Department"
  news_urls         single URL or "; "-separated
  city              city name
  county            county name
  state             2-letter postal code
  in_fars_pursuit   "1" / "0" / "" (FARS reconciliation flag)
"""
from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from .models import DatasetCandidate, NextActionHint
from .scoring import assign_grade, evidence_strength_from_grade


SOURCE_LANE = "sfchronicle_pursuits"
SOURCE_DATASET = "sfchronicle_fatal_police_pursuits"

DEFAULT_TARGET_STATES: Tuple[str, ...] = ("AZ", "FL", "TX", "OH", "IL", "CA")

_HIGH_VALUE_ROLES = ("bystander", "passenger", "officer")
_LOW_PRETEXT_REASONS = (
    "traffic stop",
    "minor/no crime",
    "suspected nonviolent",
)

# Names the dataset uses to mark the decedent as deliberately
# unidentified (e.g. minors, anonymisation by request). For scoring
# and search-task purposes these are equivalent to a missing name —
# they cannot anchor a name-bearing query, and giving them the +3
# name bonus saturates the rubric on rows that aren't actually
# operationally useful.
_PLACEHOLDER_NAMES = frozenset({
    "name withheld",
    "name unknown",
    "withheld",
    "unknown",
    "n/a",
    "n.a.",
    "not released",
    "not given",
    "unidentified",
})


def _is_placeholder_name(name: Optional[str]) -> bool:
    """True if ``name`` is one of the dataset's documented stand-in
    strings for an unidentified decedent."""
    if not name:
        return False
    return name.strip().lower() in _PLACEHOLDER_NAMES

# Match either ISO yyyy-mm-dd, US m/d/yyyy, US m/d/yy, or
# textual-month forms like ``December 26, 2020`` / ``Dec. 26 2020``.
_ISO_DATE_RE = re.compile(r"^\s*(\d{4})-(\d{1,2})-(\d{1,2})\s*$")
_US_DATE_4_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*$")
_US_DATE_2_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})/(\d{2})\s*$")
_TEXTUAL_DATE_RE = re.compile(
    r"^\s*([A-Za-z]+)\.?\s+(\d{1,2})(?:st|nd|rd|th)?[,\s]\s*(\d{4})\s*$"
)

_MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


# ---- helpers --------------------------------------------------------


def _normalize_date(raw: str) -> Optional[str]:
    """Return ISO yyyy-mm-dd, or None if unparseable.

    Accepts:
      ISO       2020-12-26
      US 4-yr  12/26/2020
      US 2-yr  12/26/20  (00-49 -> 2000-2049, 50-99 -> 1950-1999)
      Textual   December 26, 2020 / Dec. 26, 2020 / Dec 26 2020
    """
    if not raw or not isinstance(raw, str):
        return None
    text = raw.strip()
    m = _ISO_DATE_RE.match(text)
    if m:
        y, mo, d = m.group(1), int(m.group(2)), int(m.group(3))
        if _valid_md(mo, d):
            return f"{y}-{mo:02d}-{d:02d}"
        return None
    m = _US_DATE_4_RE.match(text)
    if m:
        mo, d, y = int(m.group(1)), int(m.group(2)), m.group(3)
        if _valid_md(mo, d):
            return f"{y}-{mo:02d}-{d:02d}"
        return None
    m = _US_DATE_2_RE.match(text)
    if m:
        mo, d, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if _valid_md(mo, d):
            year = 2000 + yy if yy <= 49 else 1900 + yy
            return f"{year}-{mo:02d}-{d:02d}"
        return None
    m = _TEXTUAL_DATE_RE.match(text)
    if m:
        month_name = m.group(1).lower()
        mo = _MONTH_NAMES.get(month_name)
        if mo is None:
            return None
        d = int(m.group(2))
        y = m.group(3)
        if _valid_md(mo, d):
            return f"{y}-{mo:02d}-{d:02d}"
    return None


def _valid_md(month: int, day: int) -> bool:
    """Sanity-check month + day. Doesn't care about leap years; the
    upstream dataset is already validated."""
    return 1 <= month <= 12 and 1 <= day <= 31


def _split_news_urls(raw: str) -> List[str]:
    """News URLs are commonly stored as ``"; "``-separated, but
    sometimes as a single URL or whitespace-separated. Split
    defensively, drop empties, dedupe in stable order."""
    if not raw or not isinstance(raw, str):
        return []
    candidates: List[str] = []
    for chunk in re.split(r"[;\n]", raw):
        for piece in chunk.split():
            piece = piece.strip(" ,;\"'")
            if piece.startswith(("http://", "https://")):
                if piece not in candidates:
                    candidates.append(piece)
    return candidates


def _safe_int(raw: object) -> Optional[int]:
    if raw is None or raw == "":
        return None
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return None


def _normalize_state(raw: str) -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip().upper()
    if len(raw) == 2 and raw.isalpha():
        return raw
    return None


def _compose_case_title(
    *, name: Optional[str], date: Optional[str], city: Optional[str]
) -> Optional[str]:
    parts = [p for p in (name, date, city) if p]
    if not parts:
        return None
    return " — ".join(parts)


def _stable_candidate_id(source_row_id: str) -> str:
    return f"sfchronicle_pursuits:{source_row_id}"


# ---- search-task seed builders --------------------------------------


def _youtube_queries(
    *, name: Optional[str], agency: Optional[str], city: Optional[str]
) -> List[str]:
    out: List[str] = []
    if name and agency:
        out.append(f'{name} {agency} bodycam')
        out.append(f'{name} {agency} officer involved')
    if name and city:
        out.append(f'{name} {city} police pursuit')
    if agency and city:
        out.append(f'{agency} {city} critical incident briefing')
    # dedupe preserving order
    return list(dict.fromkeys(q for q in out if q))


def _muckrock_queries(
    *, name: Optional[str], agency: Optional[str]
) -> List[str]:
    out: List[str] = []
    if name and agency:
        out.append(f'{name} {agency} body-worn camera')
        out.append(f'{name} {agency} incident report')
    if agency:
        out.append(f'{agency} body-worn camera policy')
    return list(dict.fromkeys(q for q in out if q))


def _official_source_queries(
    *,
    name: Optional[str],
    agency: Optional[str],
    city: Optional[str],
) -> List[str]:
    out: List[str] = []
    if agency and city:
        out.append(f'{agency} {city} press release officer involved')
    if agency and name:
        out.append(f'{agency} {name} statement')
    if agency:
        out.append(f'{agency} critical incident briefing')
    return list(dict.fromkeys(q for q in out if q))


def _outcome_queries(
    *, name: Optional[str], agency: Optional[str]
) -> List[str]:
    out: List[str] = []
    if name:
        out.append(f'{name} charged')
        out.append(f'{name} indicted')
        out.append(f'{name} settlement lawsuit')
    if name and agency:
        out.append(f'{name} {agency} investigation closed')
    return list(dict.fromkeys(q for q in out if q))


# ---- scoring --------------------------------------------------------


def score_pursuit_row(
    *,
    name: Optional[str],
    news_urls: List[str],
    person_role: Optional[str],
    number_killed: Optional[int],
    main_agency: Optional[str],
    initial_reason: Optional[str],
    state: Optional[str],
    city: Optional[str],
    county: Optional[str],
    in_fars_pursuit: Optional[int],
    target_states: Sequence[str] = DEFAULT_TARGET_STATES,
) -> int:
    """Pursuit-row rubric per the spec.

    +3 name present
    +3 news_urls present
    +3 person_role in {bystander, passenger, officer}
    +3 number_killed > 1
    +2 main_agency present
    +2 initial_reason in {traffic stop, minor/no crime, suspected nonviolent}
    +2 state in target_states
    +1 city OR county present
    +1 in_fars_pursuit == 1
    -3 missing agency AND missing news_urls
    -2 missing name AND missing news_urls
    """
    score = 0
    if name:
        score += 3
    if news_urls:
        score += 3
    if person_role and person_role.lower() in _HIGH_VALUE_ROLES:
        score += 3
    if number_killed is not None and number_killed > 1:
        score += 3
    if main_agency:
        score += 2
    if initial_reason and initial_reason.lower() in _LOW_PRETEXT_REASONS:
        score += 2
    if state and state.upper() in {s.upper() for s in target_states}:
        score += 2
    if city or county:
        score += 1
    if in_fars_pursuit == 1:
        score += 1
    if not main_agency and not news_urls:
        score -= 3
    if not name and not news_urls:
        score -= 2
    return score


# ---- next-action routing --------------------------------------------


def _next_action_hints(candidate: DatasetCandidate) -> List[str]:
    hints: List[str] = []
    if candidate.official_urls:
        hints.append(NextActionHint.PORTAL_LIVE_VALIDATE)
    if candidate.muckrock_urls:
        hints.append(NextActionHint.MUCKROCK_PARSE_RELEASED_FILES)
    if candidate.youtube_queries:
        hints.append(NextActionHint.YOUTUBE_METADATA_TRANSCRIPT)
    has_artifact_url = bool(
        candidate.official_urls or candidate.muckrock_urls
    )
    if (
        not has_artifact_url
        and candidate.subject_name
        and candidate.agency_name
    ):
        hints.append(NextActionHint.ARTIFACT_SEARCH)
    if candidate.outcome_validation_needed:
        hints.append(NextActionHint.OUTCOME_VALIDATE)
    return hints


# ---- top-level entry points -----------------------------------------


def parse_row(
    row: dict,
    *,
    row_index: int,
    target_states: Sequence[str] = DEFAULT_TARGET_STATES,
) -> DatasetCandidate:
    """Build one DatasetCandidate from one CSV row dict.

    ``row_index`` is the 0-based row position in the source CSV; used
    as the stable source_row_id when no other unique key is available.
    """
    raw_name = (row.get("name") or "").strip() or None
    main_agency = (row.get("main_agency") or "").strip() or None
    initial_reason = (row.get("initial_reason") or "").strip() or None
    person_role = (row.get("person_role") or "").strip() or None
    city = (row.get("city") or "").strip() or None
    county = (row.get("county") or "").strip() or None
    state = _normalize_state(row.get("state") or "")
    incident_date = _normalize_date(row.get("date") or "")
    fatality_count = _safe_int(row.get("number_killed"))
    in_fars = _safe_int(row.get("in_fars_pursuit"))
    news_urls = _split_news_urls(row.get("news_urls") or "")

    # Placeholder names ("name withheld", "unknown", etc.) are
    # operationally equivalent to no name — they can't anchor a
    # name-bearing search query and shouldn't earn the +3 bonus.
    placeholder_suppressed = _is_placeholder_name(raw_name)
    name = None if placeholder_suppressed else raw_name

    score = score_pursuit_row(
        name=name,
        news_urls=news_urls,
        person_role=person_role,
        number_killed=fatality_count,
        main_agency=main_agency,
        initial_reason=initial_reason,
        state=state,
        city=city,
        county=county,
        in_fars_pursuit=in_fars,
        target_states=target_states,
    )
    grade = assign_grade(score)
    strength = evidence_strength_from_grade(grade)

    artifact_types: List[str] = []
    if news_urls:
        artifact_types.append("news_article")

    notes: List[str] = []
    if in_fars == 1:
        notes.append("in_fars_pursuit=1 (FARS reconciled)")
    if placeholder_suppressed:
        notes.append(f"placeholder_name_suppressed={raw_name!r}")

    candidate = DatasetCandidate(
        candidate_id=_stable_candidate_id(str(row_index)),
        source_lane=SOURCE_LANE,
        source_dataset=SOURCE_DATASET,
        source_row_id=str(row_index),
        source_url=None,
        case_title=_compose_case_title(
            name=name, date=incident_date, city=city
        ),
        agency_name=main_agency,
        jurisdiction_city=city,
        jurisdiction_county=county,
        jurisdiction_state=state,
        incident_date=incident_date,
        subject_name=name,
        person_role=person_role.lower() if person_role else None,
        initial_reason=initial_reason.lower() if initial_reason else None,
        fatality_count=fatality_count,
        injury_count=None,
        news_urls=news_urls,
        official_urls=[],
        muckrock_urls=[],
        youtube_queries=_youtube_queries(
            name=name, agency=main_agency, city=city
        ),
        muckrock_queries=_muckrock_queries(name=name, agency=main_agency),
        official_source_queries=_official_source_queries(
            name=name, agency=main_agency, city=city
        ),
        outcome_queries=_outcome_queries(name=name, agency=main_agency),
        artifact_types_detected=artifact_types,
        bodycam_likelihood="unknown",
        footage_likelihood="medium" if news_urls else "low",
        outcome_validation_needed=True,
        evidence_strength=strength,
        packet_priority_score=score,
        grade=grade,
        notes=notes,
    )
    candidate.next_actions_hint = _next_action_hints(candidate)
    return candidate


def parse_csv_text(
    text: str,
    *,
    target_states: Sequence[str] = DEFAULT_TARGET_STATES,
) -> List[DatasetCandidate]:
    """Parse a CSV string into DatasetCandidate rows."""
    reader = csv.DictReader(io.StringIO(text))
    out: List[DatasetCandidate] = []
    for i, row in enumerate(reader):
        out.append(parse_row(row, row_index=i, target_states=target_states))
    return out


def parse_csv_file(
    path: Path,
    *,
    target_states: Sequence[str] = DEFAULT_TARGET_STATES,
) -> List[DatasetCandidate]:
    text = Path(path).read_text(encoding="utf-8")
    return parse_csv_text(text, target_states=target_states)


__all__ = [
    "DEFAULT_TARGET_STATES",
    "SOURCE_LANE",
    "SOURCE_DATASET",
    "parse_csv_file",
    "parse_csv_text",
    "parse_row",
    "score_pursuit_row",
]
