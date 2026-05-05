"""MuckRock curated-leads parser.

Pure module. Consumes either:

  - a local newline-separated text file of MuckRock request URLs
    (operator-curated; each URL has been manually reviewed), OR
  - a local JSON file of pre-saved MuckRock request records
    (e.g. captured from a prior MuckRock API call into .tmp/)

Emits normalized DatasetCandidate rows. Never calls the MuckRock API.
Live API support is gated to a separate, future flag and is NOT
implemented in this PR; this parser only consumes saved/curated data.

Why two input shapes:

  - The URL-only shape gives operators a frictionless way to feed in
    URLs they collected from a manual MuckRock site search. Scoring
    is conservative (we can only see the URL slug + jurisdiction).
  - The JSON shape carries title / agency / files / status, so the
    parser can score richly. This is the shape a live API ingest
    would also produce.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from .models import DatasetCandidate, NextActionHint
from .scoring import assign_grade, evidence_strength_from_grade


SOURCE_LANE = "muckrock_curated"
SOURCE_DATASET = "muckrock_curated_urls"

_MUCKROCK_URL_RE = re.compile(
    r"^https?://(?:www\.)?muckrock\.com/foi/"
    r"(?P<jurisdiction>[a-z0-9\-]+)/"
    r"(?P<id>\d+)-(?P<slug>[a-z0-9\-]+)/?$",
    re.IGNORECASE,
)

_BWC_TERMS = (
    r"\bBWC\b",
    r"\bbody[-\s]worn\s+camera\b",
    r"\bbody\s+camera\b",
)
_INCIDENT_TERMS = (
    r"\bofficer[-\s]involved\s+shoot",
    r"\bofficer[-\s]involved\s+incident\b",
    r"\bdeputy[-\s]involved\b",
    r"\bpolice\s+shoot",
    r"\bcritical\s+incident\s+brief",
    r"\bin[-\s]custody\s+death\b",
)
_POLICY_ONLY_TERMS = (
    r"\bgeneral\s+orders?\b",
    r"\bpolicies?\s+and\s+procedures?\b",
    r"\bpolicy\s+manual\b",
    r"\b(all|any)\s+(records?|documents?)\b",
    r"\bunredacted\s+policy\b",
    r"\btraining\s+materials?\b",
)
_DATE_OR_CASE_RE = re.compile(
    r"\b(?:\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{2,4}|"
    r"case\s+(?:no|number|#)\s*[:#]?\s*\S+)",
    re.IGNORECASE,
)
_PROPER_NAME_RE = re.compile(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}\b")


# ---- URL parsing ----------------------------------------------------


def parse_muckrock_url(url: str) -> Optional[dict]:
    """Extract jurisdiction, id, slug from a MuckRock /foi/ URL.

    Returns ``None`` for non-MuckRock URLs."""
    if not url or not isinstance(url, str):
        return None
    m = _MUCKROCK_URL_RE.match(url.strip())
    if not m:
        return None
    return {
        "jurisdiction": m.group("jurisdiction").lower(),
        "id": m.group("id"),
        "slug": m.group("slug").lower(),
        "url": url.strip(),
    }


def _slug_to_title(slug: str) -> str:
    """``body-worn-camera-footage-2024-09-22`` →
    ``Body Worn Camera Footage 2024 09 22`` (best-effort)."""
    return " ".join(part.capitalize() for part in slug.split("-") if part)


# ---- text scanning helpers ------------------------------------------


def _any_match(text: str, patterns: Iterable[str]) -> bool:
    if not text:
        return False
    for pat in patterns:
        if re.search(pat, text, re.IGNORECASE):
            return True
    return False


def _has_incident_anchor(text: str) -> bool:
    """Either a date / case-number marker, or a multi-token Proper
    Name. The combination of "released files + incident anchor" is
    what distinguishes a useful per-incident MuckRock release from a
    policy-only request."""
    if not text:
        return False
    if _DATE_OR_CASE_RE.search(text):
        return True
    if _PROPER_NAME_RE.search(text):
        return True
    return False


# ---- scoring --------------------------------------------------------


def score_muckrock_record(
    *,
    title: Optional[str],
    body: Optional[str],
    agency: Optional[str],
    has_released_files: bool,
) -> int:
    """MuckRock rubric per the spec.

    +5 released files detected
    +4 BWC / Body-worn camera / body camera terms
    +3 incident-specific title/body (date, case#, or proper name)
    +3 officer-involved / deputy-involved / police shoot* / CIB / in-custody
    +2 agency present
    +2 date OR case number OR proper name detected somewhere
    -4 policy-only / generic-request terms
    -3 no released files AND no incident anchor

    The +3-incident and +2-date checks can both fire from the same
    text, by design — a request with a clear date AND a named subject
    deserves both bumps.
    """
    text = " ".join(filter(None, [title, body])).strip()

    score = 0
    if has_released_files:
        score += 5
    if _any_match(text, _BWC_TERMS):
        score += 4
    incident_anchor = _has_incident_anchor(text)
    if incident_anchor:
        score += 3
    if _any_match(text, _INCIDENT_TERMS):
        score += 3
    if agency:
        score += 2
    if _DATE_OR_CASE_RE.search(text or "") or _PROPER_NAME_RE.search(text or ""):
        score += 2
    if _any_match(text, _POLICY_ONLY_TERMS):
        score -= 4
    if not has_released_files and not incident_anchor:
        score -= 3
    return score


# ---- next-action routing --------------------------------------------


def _next_action_hints(candidate: DatasetCandidate) -> List[str]:
    hints: List[str] = []
    if candidate.muckrock_urls:
        hints.append(NextActionHint.MUCKROCK_PARSE_RELEASED_FILES)
    if candidate.official_urls:
        hints.append(NextActionHint.PORTAL_LIVE_VALIDATE)
    if candidate.youtube_queries:
        hints.append(NextActionHint.YOUTUBE_METADATA_TRANSCRIPT)
    if (
        not candidate.muckrock_urls
        and not candidate.official_urls
        and candidate.subject_name
        and candidate.agency_name
    ):
        hints.append(NextActionHint.ARTIFACT_SEARCH)
    if candidate.outcome_validation_needed:
        hints.append(NextActionHint.OUTCOME_VALIDATE)
    return hints


# ---- search-task seed builders --------------------------------------


def _muckrock_queries(
    *, agency: Optional[str], title: Optional[str]
) -> List[str]:
    out: List[str] = []
    if agency:
        out.append(f'{agency} body-worn camera released')
    if title and agency:
        out.append(f'{title} {agency}')
    return list(dict.fromkeys(q for q in out if q))


def _outcome_queries(
    *, title: Optional[str], agency: Optional[str]
) -> List[str]:
    out: List[str] = []
    if title:
        m = _PROPER_NAME_RE.search(title)
        if m:
            name = m.group(0)
            out.append(f'{name} charged')
            out.append(f'{name} settlement lawsuit')
    if agency:
        out.append(f'{agency} investigation outcome')
    return list(dict.fromkeys(q for q in out if q))


# ---- candidate construction ----------------------------------------


def _build_candidate_from_url_only(
    parsed: dict, *, original_url: str
) -> DatasetCandidate:
    """URL-only path. Scoring is conservative because we don't have
    title/body/files — we only have the slug and jurisdiction."""
    title = _slug_to_title(parsed["slug"])
    score = score_muckrock_record(
        title=title,
        body=None,
        agency=None,
        has_released_files=False,
    )
    grade = assign_grade(score)
    candidate = DatasetCandidate(
        candidate_id=f"muckrock_curated:{parsed['id']}",
        source_lane=SOURCE_LANE,
        source_dataset=SOURCE_DATASET,
        source_row_id=parsed["id"],
        source_url=original_url,
        case_title=title,
        muckrock_urls=[original_url],
        outcome_validation_needed=True,
        evidence_strength=evidence_strength_from_grade(grade),
        packet_priority_score=score,
        grade=grade,
        notes=[
            f"jurisdiction={parsed['jurisdiction']}",
            "URL-only ingest — title inferred from slug; agency unknown",
        ],
    )
    candidate.next_actions_hint = _next_action_hints(candidate)
    return candidate


def _build_candidate_from_record(record: dict) -> DatasetCandidate:
    """JSON-record path. Reads title / agency / status / files /
    communications from the MuckRock-shaped dict."""
    rid = str(record.get("id") or record.get("request_id") or "").strip()
    title = (record.get("title") or "").strip() or None
    body_chunks: List[str] = []
    if record.get("body"):
        body_chunks.append(str(record["body"]))
    for c in record.get("communications") or []:
        if isinstance(c, dict) and c.get("text"):
            body_chunks.append(str(c["text"]))
    body = " ".join(body_chunks).strip() or None
    agency = None
    agency_obj = record.get("agency")
    if isinstance(agency_obj, dict):
        agency = (agency_obj.get("name") or "").strip() or None
    elif isinstance(agency_obj, str):
        agency = agency_obj.strip() or None
    files = record.get("files") or []
    has_released_files = bool(files) or (
        str(record.get("status") or "").strip().lower() in {"done", "completed", "complete"}
    )
    abs_url = record.get("absolute_url") or record.get("url")
    full_url = abs_url
    if abs_url and abs_url.startswith("/"):
        full_url = f"https://www.muckrock.com{abs_url}"

    score = score_muckrock_record(
        title=title,
        body=body,
        agency=agency,
        has_released_files=has_released_files,
    )
    grade = assign_grade(score)
    strength = evidence_strength_from_grade(grade)

    artifact_types: List[str] = []
    if has_released_files:
        artifact_types.append("muckrock_release")
    if _any_match(" ".join(filter(None, [title, body])), _BWC_TERMS):
        artifact_types.append("bodycam")

    notes: List[str] = []
    status = (record.get("status") or "").strip()
    if status:
        notes.append(f"muckrock_status={status}")
    if files:
        notes.append(f"released_file_count={len(files)}")

    name_match = _PROPER_NAME_RE.search(title or "")
    subject_name = name_match.group(0) if name_match else None

    candidate = DatasetCandidate(
        candidate_id=f"muckrock_curated:{rid}" if rid else f"muckrock_curated:{abs(hash(full_url or title or '')) % 10**10}",
        source_lane=SOURCE_LANE,
        source_dataset=SOURCE_DATASET,
        source_row_id=rid or "",
        source_url=full_url,
        case_title=title,
        agency_name=agency,
        subject_name=subject_name,
        muckrock_urls=[full_url] if full_url else [],
        muckrock_queries=_muckrock_queries(agency=agency, title=title),
        outcome_queries=_outcome_queries(title=title, agency=agency),
        artifact_types_detected=artifact_types,
        bodycam_likelihood="high" if "bodycam" in artifact_types else "unknown",
        footage_likelihood="high" if has_released_files and "bodycam" in artifact_types else (
            "medium" if has_released_files else "low"
        ),
        outcome_validation_needed=True,
        evidence_strength=strength,
        packet_priority_score=score,
        grade=grade,
        notes=notes,
    )
    candidate.next_actions_hint = _next_action_hints(candidate)
    return candidate


# ---- top-level entry points -----------------------------------------


def parse_url_list_text(text: str) -> List[DatasetCandidate]:
    """Parse newline-separated URLs (with ``#`` comments)."""
    out: List[DatasetCandidate] = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parsed = parse_muckrock_url(line)
        if parsed is None:
            continue
        out.append(_build_candidate_from_url_only(parsed, original_url=line))
    return out


def parse_url_list_file(path: Path) -> List[DatasetCandidate]:
    text = Path(path).read_text(encoding="utf-8")
    return parse_url_list_text(text)


def parse_records_json_text(text: str) -> List[DatasetCandidate]:
    """Parse a JSON document containing either a single MuckRock
    record dict or a list of records."""
    data = json.loads(text or "null")
    if data is None:
        return []
    if isinstance(data, dict):
        return [_build_candidate_from_record(data)]
    if isinstance(data, list):
        return [
            _build_candidate_from_record(rec)
            for rec in data
            if isinstance(rec, dict)
        ]
    raise ValueError("MuckRock JSON input must be a dict or list of dicts")


def parse_records_json_file(path: Path) -> List[DatasetCandidate]:
    text = Path(path).read_text(encoding="utf-8")
    return parse_records_json_text(text)


__all__ = [
    "SOURCE_LANE",
    "SOURCE_DATASET",
    "parse_muckrock_url",
    "parse_records_json_file",
    "parse_records_json_text",
    "parse_url_list_file",
    "parse_url_list_text",
    "score_muckrock_record",
]
