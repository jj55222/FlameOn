"""Normalized DatasetCandidate dataclass + supporting enums.

Single shared schema across every dataset-source lane. Per-lane
parsers (sfchronicle_pursuits, muckrock_leads, ...) all emit lists
of DatasetCandidate so downstream consumers (search-task generator,
packet-stub writer, eventual portal-live router) only ever speak
one shape.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import List, Optional, Tuple


SourceLane = str   # e.g. "sfchronicle_pursuits", "muckrock_curated"


# Stable next-action hint codes the routing layer reads. Kept as a
# small frozen tuple so consumers can match by string equality.
DATASET_GRADES: Tuple[str, ...] = ("A", "B", "C", "D")


class NextActionHint:
    """Stable hint codes used in DatasetCandidate.next_actions_hint.

    The dataset-intake layer emits hints; downstream pipelines act on
    them. Hints are advisory — the dataset layer never directly
    invokes portal-live / MuckRock API / YouTube API.
    """

    PORTAL_LIVE_VALIDATE = "PORTAL_LIVE_VALIDATE"
    MUCKROCK_PARSE_RELEASED_FILES = "MUCKROCK_PARSE_RELEASED_FILES"
    YOUTUBE_METADATA_TRANSCRIPT = "YOUTUBE_METADATA_TRANSCRIPT"
    ARTIFACT_SEARCH = "ARTIFACT_SEARCH"
    OUTCOME_VALIDATE = "OUTCOME_VALIDATE"

    ALL: Tuple[str, ...] = (
        "PORTAL_LIVE_VALIDATE",
        "MUCKROCK_PARSE_RELEASED_FILES",
        "YOUTUBE_METADATA_TRANSCRIPT",
        "ARTIFACT_SEARCH",
        "OUTCOME_VALIDATE",
    )


@dataclass
class DatasetCandidate:
    """One normalized candidate row from a Pipeline 1 dataset source.

    Identity:
      - candidate_id is operator-stable (same input → same id; safe
        to use as a filename prefix).
      - source_row_id is the per-dataset row identifier (e.g. CSV row
        index, MuckRock request id).
    """

    # ---- identity -----------------------------------------------------
    candidate_id: str
    source_lane: SourceLane
    source_dataset: str
    source_row_id: str
    source_url: Optional[str] = None

    # ---- core facts (all optional; populate from source row) ---------
    case_title: Optional[str] = None
    agency_name: Optional[str] = None
    jurisdiction_city: Optional[str] = None
    jurisdiction_county: Optional[str] = None
    jurisdiction_state: Optional[str] = None  # 2-letter postal
    incident_date: Optional[str] = None       # ISO yyyy-mm-dd if parsable
    subject_name: Optional[str] = None
    person_role: Optional[str] = None         # lowercased
    initial_reason: Optional[str] = None      # lowercased
    fatality_count: Optional[int] = None
    injury_count: Optional[int] = None

    # ---- artifact pointers + search seeds ----------------------------
    news_urls: List[str] = field(default_factory=list)
    official_urls: List[str] = field(default_factory=list)
    muckrock_urls: List[str] = field(default_factory=list)

    youtube_queries: List[str] = field(default_factory=list)
    muckrock_queries: List[str] = field(default_factory=list)
    official_source_queries: List[str] = field(default_factory=list)
    outcome_queries: List[str] = field(default_factory=list)

    artifact_types_detected: List[str] = field(default_factory=list)

    # ---- heuristic verdict surface -----------------------------------
    bodycam_likelihood: str = "unknown"   # high | medium | low | unknown
    footage_likelihood: str = "unknown"   # same
    outcome_validation_needed: bool = True
    evidence_strength: str = "weak"       # strong | moderate | weak
    packet_priority_score: int = 0
    grade: str = "D"

    # ---- routing ------------------------------------------------------
    next_actions_hint: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)
