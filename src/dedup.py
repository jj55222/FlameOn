"""Deduplication logic for the Sunshine-Gated Closed-Case Pipeline.

Two levels:
1. Video-level (hard dedup) — by video_id
2. Case-level (soft dedup) — likely-duplicate detection by suspect + location + date
3. Incident-level (content dedup) — same incident covered by multiple videos
"""

import re
from datetime import datetime, timedelta
from typing import Optional

from .logger import get_logger
from .models import CaseCandidate

log = get_logger()


def check_video_duplicate(video_id: str, existing_video_ids: set[str]) -> bool:
    """Check if this video_id has already been processed.

    Returns True if duplicate.
    """
    return video_id in existing_video_ids


def _normalize_name(name: str) -> str:
    """Lowercase, strip whitespace and punctuation for comparison."""
    if not name:
        return ""
    return " ".join(name.lower().split())


def _parse_date_loose(date_str: str) -> Optional[datetime]:
    """Try to parse a date string loosely."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%B %d, %Y", "%B %d %Y"):
        try:
            return datetime.strptime(date_str.strip().rstrip(","), fmt)
        except ValueError:
            continue
    return None


def find_likely_duplicate(
    candidate: CaseCandidate,
    existing_cases: list[dict],
    name_match_required: bool = True,
    date_window_days: int = 30,
) -> Optional[str]:
    """Check if a candidate is a likely duplicate of an existing case.

    Returns the existing case_id if a likely duplicate is found, else None.

    Matching criteria (ALL must hold when present):
    - suspect_name matches (normalized, case-insensitive)
    - state matches
    - city matches (if both present)
    - incident_date within date_window_days (if both present)
    """
    if not candidate.suspect_name and name_match_required:
        return None  # can't match without a name

    c_name = _normalize_name(candidate.suspect_name)
    c_state = candidate.state.upper() if candidate.state else ""
    c_city = candidate.city.lower().strip() if candidate.city else ""
    c_date = _parse_date_loose(candidate.incident_date)

    for existing in existing_cases:
        e_name = _normalize_name(existing.get("suspect_name", ""))
        e_state = (existing.get("state", "") or "").upper()
        e_city = (existing.get("city", "") or "").lower().strip()
        e_date = _parse_date_loose(existing.get("incident_date", ""))

        # Name must match if we have one
        if c_name and e_name and c_name != e_name:
            continue

        # If we require a name match but either side is missing, skip
        if name_match_required and (not c_name or not e_name):
            continue

        # State must match
        if c_state and e_state and c_state != e_state:
            continue

        # City must match if both present
        if c_city and e_city and c_city != e_city:
            continue

        # Date must be within window if both present
        if c_date and e_date:
            if abs((c_date - e_date).days) > date_window_days:
                continue

        # All checks passed — likely duplicate
        return existing.get("case_id", "unknown")

    return None


def _extract_location_from_description(desc: str) -> str:
    """Try to extract a street address or landmark from description for incident matching."""
    # Look for street addresses like "1951 Central Florida Parkway" or "6809 Colony Oaks Lane"
    match = re.search(r"\b\d{2,5}\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,4}\s+(?:Road|Rd|Street|St|Avenue|Ave|Drive|Dr|Boulevard|Blvd|Lane|Ln|Parkway|Pkwy|Court|Ct|Circle|Way|Trail|Place|Pl)\b", desc)
    if match:
        return match.group(0).lower()
    return ""


def find_incident_duplicate(
    candidate: CaseCandidate,
    existing_cases: list[dict],
    date_window_days: int = 7,
) -> Optional[str]:
    """Detect same-incident duplicates even when suspect names are unknown.

    Uses: same agency + same city + overlapping incident date (within 7 days) +
    same primary keyword (shooting, stabbing, etc.).
    This catches the pattern where an agency posts both a press briefing and a
    BWC release for the same incident.
    """
    c_state = (candidate.state or "").upper()
    c_city = (candidate.city or "").lower().strip()
    c_agency = (candidate.agency_name or "").lower().strip()
    c_date = _parse_date_loose(candidate.incident_date)
    c_keywords = set((candidate.case_keywords or "").lower().replace(",", " ").split())
    c_location = _extract_location_from_description(candidate.video_description or "")

    # Primary crime keyword (the most specific one)
    primary_crimes = {"murder", "homicide", "shooting", "stabbing", "carjacking",
                      "kidnapping", "robbery", "arson", "sexual assault", "rape"}
    c_primary = c_keywords & primary_crimes

    for existing in existing_cases:
        e_state = (existing.get("state", "") or "").upper()
        e_city = (existing.get("city", "") or "").lower().strip()
        e_agency = (existing.get("agency_name", "") or "").lower().strip()
        e_date = _parse_date_loose(existing.get("incident_date", ""))
        e_keywords = set((existing.get("case_keywords", "") or "").lower().replace(",", " ").split())
        e_primary = e_keywords & primary_crimes

        # Must be same state
        if c_state != e_state:
            continue
        # Must be same city or same agency
        if c_city != e_city and c_agency != e_agency:
            continue
        # Must have overlapping primary crime type
        if c_primary and e_primary and not (c_primary & e_primary):
            continue
        # Date must be close
        if c_date and e_date and abs((c_date - e_date).days) > date_window_days:
            continue
        # If no date on either side, require stronger match
        if not c_date or not e_date:
            # Need matching location string from description
            e_desc = existing.get("video_description", "") or ""
            e_location = _extract_location_from_description(e_desc)
            if not c_location or not e_location or c_location != e_location:
                continue

        return existing.get("case_id", "unknown")

    return None


def deduplicate_candidates(
    candidates: list[CaseCandidate],
    existing_video_ids: set[str],
    existing_cases: list[dict],
) -> tuple[list[CaseCandidate], list[CaseCandidate]]:
    """Filter candidates through deduplication.

    Returns (new_candidates, duplicates).
    """
    new = []
    dupes = []

    for c in candidates:
        # Hard dedup: video_id
        if check_video_duplicate(c.video_id, existing_video_ids):
            log.debug("Video duplicate skipped: %s", c.video_id)
            dupes.append(c)
            continue

        # Soft dedup: likely case duplicate (by name + location + date)
        existing_case_id = find_likely_duplicate(c, existing_cases)
        if existing_case_id:
            log.info(
                "Likely case duplicate: %s matches existing %s (flagging, still processing)",
                c.case_id,
                existing_case_id,
            )
            c.validation_note = f"Likely duplicate of {existing_case_id}"
            # Still process but flag — per spec, soft dupes are processed with a warning

        # Incident-level dedup: same incident, different video (press briefing + BWC release)
        if not existing_case_id:
            incident_match = find_incident_duplicate(c, existing_cases)
            if incident_match:
                log.info(
                    "Likely incident duplicate: %s matches existing %s",
                    c.case_id,
                    incident_match,
                )
                c.validation_note = f"Likely incident duplicate of {incident_match}"

        new.append(c)
        existing_video_ids.add(c.video_id)

    log.info(
        "Dedup: %d candidates → %d new, %d video-duplicates skipped",
        len(candidates),
        len(new),
        len(dupes),
    )
    return new, dupes
