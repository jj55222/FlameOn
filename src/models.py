"""Data models for the Sunshine-Gated Closed-Case Pipeline."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class ValidationStatus(str, Enum):
    NEW_CANDIDATE = "new_candidate"
    VALIDATED_CLOSED = "validated_closed"
    REJECTED_OPEN_OR_UNCONFIRMED = "rejected_open_or_unconfirmed"
    MANUAL_REVIEW = "manual_review"
    LINKS_DISCOVERED = "links_discovered"
    DOWNLOADS_COMPLETED = "downloads_completed"


class SourceRank(str, Enum):
    COURT_GOV = "court_gov"
    COUNTY_CLERK = "county_clerk"
    LOCAL_NEWS = "local_news"
    LE_RELEASE = "le_release"
    OTHER = "other"


class CorroborationStatus(str, Enum):
    CONFIRMED = "confirmed"
    NOT_FOUND = "not_found"
    NOT_ATTEMPTED = "not_attempted"


@dataclass
class ChannelConfig:
    handle: str
    agency_name: str
    state: str
    city: str
    channel_id: Optional[str] = None
    uploads_playlist_id: Optional[str] = None


@dataclass
class CaseCandidate:
    case_id: str
    video_id: str
    channel_id: str
    channel_name: str
    agency_name: str
    state: str
    city: str
    video_title: str
    video_description: str
    video_url: str
    published_at: str
    suspect_name: str = ""
    incident_date: str = ""
    case_keywords: str = ""
    source_type: str = "youtube_upload"
    validation_status: str = ValidationStatus.NEW_CANDIDATE.value
    validation_query: str = ""
    validation_note: str = ""
    validation_source_url: str = ""
    source_rank_used: str = ""
    manual_review_reason: str = ""
    official_corroboration_status: str = CorroborationStatus.NOT_ATTEMPTED.value
    links_discovered: int = 0
    downloads_completed: int = 0
    local_case_folder: str = ""
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self):
        now = datetime.utcnow().isoformat()
        if not self.created_at:
            self.created_at = now
        if not self.updated_at:
            self.updated_at = now

    def touch(self):
        self.updated_at = datetime.utcnow().isoformat()


@dataclass
class ValidationResult:
    status: str  # ValidationStatus value
    query_used: str
    source_url: str = ""
    source_rank: str = ""
    note: str = ""
    manual_review_reason: str = ""
    raw_snippets: str = ""


@dataclass
class DiscoveredLink:
    url: str
    source_class: str  # SourceRank value
    link_type: str  # e.g. "court_docket", "sentencing_order", "news_article", "bwc_video"
    notes: str = ""
    download_recommended: bool = False
    official_corroboration: bool = False
    download_attempted: bool = False
    download_success: bool = False
    local_path: str = ""


@dataclass
class LinkInventory:
    case_id: str
    links: list = field(default_factory=list)  # list of DiscoveredLink dicts
    created_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.utcnow().isoformat()


# Google Sheet column order — must match the schema exactly
SHEET_COLUMNS = [
    "case_id",
    "video_id",
    "channel_id",
    "channel_name",
    "agency_name",
    "state",
    "city",
    "video_title",
    "video_url",
    "video_description",
    "published_at",
    "suspect_name",
    "incident_date",
    "case_keywords",
    "validation_status",
    "validation_query",
    "validation_note",
    "validation_source_url",
    "source_rank_used",
    "manual_review_reason",
    "official_corroboration_status",
    "links_discovered",
    "downloads_completed",
    "local_case_folder",
    "created_at",
    "updated_at",
]
