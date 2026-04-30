from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from ..models import CaseInput, SourceRecord
from .base import SourceConnector


class ConnectorUnavailable(RuntimeError):
    """Raised when an optional connector dependency is unavailable."""


ARTIFACT_TERMS = [
    "bodycam",
    "body cam",
    "body camera",
    "body-worn camera",
    "bwc",
    "interrogation",
    "police interview",
    "detective interview",
    "911",
    "dispatch",
    "court video",
    "trial video",
    "sentencing video",
    "critical incident video",
]

NEWS_CHANNEL_TERMS = [
    "news",
    "nbc",
    "abc",
    "cbs",
    "fox",
    "wsvn",
    "wplg",
    "ktla",
    "kron",
    "law&crime",
    "court tv",
]

OFFICIAL_CHANNEL_TERMS = [
    "police",
    "sheriff",
    "district attorney",
    "state attorney",
    "department of public safety",
    "public safety",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _text(*parts: Optional[str]) -> str:
    return " ".join(part for part in parts if part).lower()


def _has_artifact_terms(text: str) -> bool:
    return any(term in text for term in ARTIFACT_TERMS)


def _has_identity_anchor(case_input: CaseInput, text: str) -> bool:
    known = case_input.known_fields or {}
    names = known.get("defendant_names") or []
    jurisdiction = known.get("jurisdiction") or {}
    has_name = any(name and name.lower() in text for name in names)
    has_location = any(
        value and str(value).lower() in text
        for value in [
            jurisdiction.get("city"),
            jurisdiction.get("county"),
            jurisdiction.get("state"),
            known.get("agency"),
            known.get("incident_date"),
        ]
    )
    return bool(has_name and has_location)


def _authority_for_channel(channel: str) -> str:
    channel_text = channel.lower()
    if any(term in channel_text for term in OFFICIAL_CHANNEL_TERMS):
        return "official"
    if any(term in channel_text for term in NEWS_CHANNEL_TERMS):
        return "news"
    return "third_party"


def _video_url(video_id: str, raw_url: str = "") -> str:
    if raw_url.startswith("http"):
        return raw_url
    return f"https://www.youtube.com/watch?v={video_id}"


def _video_id(item: Dict[str, Any]) -> str:
    return str(item.get("id") or item.get("display_id") or "")


class YouTubeConnector(SourceConnector):
    """Capped metadata-only YouTube search wrapper around yt-dlp."""

    name = "youtube_yt_dlp"

    def __init__(self, ydl_cls: Any = None, *, socket_timeout: int = 8) -> None:
        self._ydl_cls = ydl_cls
        self.socket_timeout = socket_timeout
        self.last_query: Optional[str] = None
        self.last_error: Optional[str] = None

    def fetch(self, case_input: CaseInput) -> Iterable[SourceRecord]:
        return self.search(case_input)

    def build_queries(self, case_input: CaseInput) -> List[str]:
        known = case_input.known_fields or {}
        names = known.get("defendant_names") or []
        if names:
            return [f"{names[0]} bodycam"]
        agency = known.get("agency")
        incident_date = str(known.get("incident_date") or "")
        year_match = re.search(r"\b(19|20)\d{2}\b", incident_date)
        if agency and year_match:
            return [f"{agency} critical incident video {year_match.group(0)}"]
        candidates = case_input.candidate_queries or []
        return candidates[:1]

    def search(self, case_input: CaseInput, *, max_results: int = 3, max_queries: int = 1) -> List[SourceRecord]:
        max_results = max(0, min(max_results, 10))
        max_queries = max(0, min(max_queries, 3))
        if max_results == 0 or max_queries == 0:
            return []

        ydl_cls = self._ydl_cls or self._load_yt_dlp()
        records: List[SourceRecord] = []
        for query in self.build_queries(case_input)[:max_queries]:
            self.last_query = query
            opts = {
                "quiet": True,
                "no_warnings": True,
                "extract_flat": True,
                "skip_download": True,
                "noplaylist": True,
                "socket_timeout": self.socket_timeout,
            }
            try:
                with ydl_cls(opts) as ydl:
                    info = ydl.extract_info(f"ytsearch{max_results}:{query}", download=False)
            except Exception as exc:  # pragma: no cover - live path/network error
                self.last_error = str(exc)
                continue
            for item in (info or {}).get("entries", [])[:max_results]:
                video_id = _video_id(item)
                if not video_id:
                    continue
                records.append(self._source_from_item(case_input, query, item, video_id))
        return records[:max_results]

    def _load_yt_dlp(self):
        try:
            import yt_dlp  # type: ignore
        except ImportError as exc:
            raise ConnectorUnavailable("yt-dlp is not installed") from exc
        return yt_dlp.YoutubeDL

    def _source_from_item(self, case_input: CaseInput, query: str, item: Dict[str, Any], video_id: str) -> SourceRecord:
        title = str(item.get("title") or "")
        description = str(item.get("description") or item.get("channel") or item.get("uploader") or "")
        channel = str(item.get("channel") or item.get("uploader") or "")
        combined = _text(title, description, channel)
        roles = ["possible_artifact_source"]
        if _has_artifact_terms(combined):
            roles.append("claim_source")
        if _has_identity_anchor(case_input, combined):
            roles.append("identity_source")
        return SourceRecord(
            source_id=f"youtube_{video_id}",
            url=_video_url(video_id, str(item.get("url") or "")),
            title=title,
            snippet=description,
            raw_text=description,
            source_type="video",
            source_roles=list(dict.fromkeys(roles)),
            source_authority=_authority_for_channel(channel),
            api_name="youtube_yt_dlp",
            discovered_via=query,
            retrieved_at=_utc_now(),
            case_input_id=case_input.raw_input.get("defendant_names") or case_input.input_type,
            metadata={
                "video_id": video_id,
                "channel": channel,
                "duration": item.get("duration"),
                "upload_date": item.get("upload_date"),
            },
            cost_estimate=0.0,
            confidence_signals={
                "artifact_terms_present": _has_artifact_terms(combined),
                "identity_anchor_present": _has_identity_anchor(case_input, combined),
                "youtube_watch_url": True,
            },
            matched_case_fields=["defendant_full_name"] if _has_identity_anchor(case_input, combined) else [],
        )
