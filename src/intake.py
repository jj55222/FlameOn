"""Stage 1 — YouTube Intake & Field Extraction.

Monitors uploads playlists for tracked agency channels.
Extracts case signals from video title and description.
"""

import re
import time
from datetime import datetime
from typing import Optional

import requests
from googleapiclient.discovery import build

from .logger import get_logger
from .models import CaseCandidate, ChannelConfig

log = get_logger()

# --- Regex patterns for field extraction ---

# Suspect name patterns (common in LE video titles)
NAME_PATTERNS = [
    # "John Doe Arrested" / "John Doe Sentenced" / "John A. Doe ..."
    r"(?:Body\s*Cam|BWC|Bodycam)?[:\s\-|]*([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]{2,})(?:\s*,?\s*(?:\d+|Jr\.|Sr\.|III|II))?(?:\s*[-,]?\s*(?:Arrested|Charged|Sentenced|Convicted|Shot|Killed|Suspect|Defendant))",
    # "State v. John Doe" / "State vs. Doe"
    r"(?:State|People|Commonwealth)\s+(?:v\.?|vs\.?)\s+([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]{2,})",
    # "Arrest of John Doe" / "Sentencing of John Doe"
    r"(?:Arrest|Sentencing|Conviction|Murder|Shooting|Interrogation)\s+of\s+([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]{2,})",
]

# Date patterns
DATE_PATTERNS = [
    # MM/DD/YYYY or MM-DD-YYYY
    r"(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})",
    # Month DD, YYYY
    r"((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})",
    # YYYY-MM-DD
    r"(\d{4}-\d{2}-\d{2})",
]

# Case keywords that suggest crime/legal context
KEYWORD_PATTERNS = [
    r"\b(murder|homicide|manslaughter)\b",
    r"\b(shooting|shot|gunfire)\b",
    r"\b(robbery|armed robbery|burglary)\b",
    r"\b(assault|battery|aggravated assault)\b",
    r"\b(DUI|DWI|drunk driving)\b",
    r"\b(domestic violence|domestic)\b",
    r"\b(kidnapping|abduction)\b",
    r"\b(arson)\b",
    r"\b(drug|narcotics|trafficking)\b",
    r"\b(sexual assault|rape)\b",
    r"\b(stabbing|stabbed)\b",
    r"\b(carjacking)\b",
    r"\b(officer[- ]involved|OIS)\b",
    r"\b(body\s*cam|bodycam|BWC)\b",
    r"\b(interrogation|interview)\b",
    r"\b(pursuit|chase)\b",
    r"\b(sentenced|sentencing|convicted|conviction|plea)\b",
    r"\b(arrested|arrest)\b",
    r"\b(indicted|indictment)\b",
]


def _extract_name_regex(text: str) -> str:
    """Try to extract a suspect name using regex patterns."""
    for pattern in NAME_PATTERNS:
        match = re.search(pattern, text)
        if match:
            name = match.group(1).strip()
            # Basic sanity: name should be 2+ words, not a common false positive
            parts = name.split()
            if len(parts) >= 2 and len(name) > 5:
                return name
    return ""


def _extract_date_regex(text: str) -> str:
    """Try to extract an incident date from text."""
    for pattern in DATE_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


def _extract_keywords(text: str) -> str:
    """Extract crime/legal keywords from text."""
    found = set()
    for pattern in KEYWORD_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for m in matches:
            found.add(m.lower().strip())
    return ", ".join(sorted(found)) if found else ""


def _extract_name_llm(
    title: str,
    description: str,
    openrouter_api_key: str,
    openrouter_model: str,
    openrouter_base_url: str,
) -> str:
    """Use OpenRouter LLM to extract suspect name when regex fails."""
    if not openrouter_api_key:
        return ""

    prompt = (
        "Extract the primary suspect or defendant name from this law enforcement video. "
        "Return ONLY the full name (first and last), or 'NONE' if no clear suspect name is present. "
        "Do not guess or hallucinate.\n\n"
        f"Title: {title}\n"
        f"Description: {description[:500]}\n\n"
        "Suspect name:"
    )

    try:
        resp = requests.post(
            f"{openrouter_base_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {openrouter_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": openrouter_model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 50,
                "temperature": 0,
            },
            timeout=15,
        )
        resp.raise_for_status()
        result = resp.json()["choices"][0]["message"]["content"].strip()
        if result.upper() == "NONE" or len(result) < 3:
            return ""
        # Sanity check: should look like a name
        parts = result.split()
        if 2 <= len(parts) <= 5 and all(p[0].isupper() for p in parts if p[0].isalpha()):
            return result
        return ""
    except Exception as e:
        log.warning("LLM name extraction failed: %s", e)
        return ""


def _generate_case_id(state: str, suspect_name: str, video_id: str, published_at: str) -> str:
    """Generate a deterministic case_id."""
    year = ""
    if published_at:
        try:
            year = published_at[:4]
        except (IndexError, TypeError):
            pass

    if suspect_name:
        parts = suspect_name.strip().split()
        last = re.sub(r"[^\w]", "", parts[-1].lower()) if parts else "unknown"
        return f"{state.lower()}_{last}_{year or 'noyear'}_{video_id}"

    return f"{state.lower()}_unknown_{year or 'noyear'}_{video_id}"


def resolve_channel_id(youtube, handle: str) -> Optional[str]:
    """Resolve a YouTube @handle to a channel ID."""
    try:
        # Try searching for the channel by handle
        clean_handle = handle.lstrip("@")
        resp = youtube.search().list(
            part="snippet",
            q=f"@{clean_handle}",
            type="channel",
            maxResults=1,
        ).execute()

        items = resp.get("items", [])
        if items:
            return items[0]["snippet"]["channelId"]

        # Fallback: try channels.list with forHandle (newer API)
        resp = youtube.channels().list(
            part="contentDetails",
            forHandle=clean_handle,
        ).execute()
        items = resp.get("items", [])
        if items:
            return items[0]["id"]

    except Exception as e:
        log.warning("Failed to resolve channel ID for %s: %s", handle, e)

    return None


def get_uploads_playlist_id(youtube, channel_id: str) -> Optional[str]:
    """Get the uploads playlist ID for a channel."""
    try:
        resp = youtube.channels().list(
            part="contentDetails",
            id=channel_id,
        ).execute()
        items = resp.get("items", [])
        if items:
            return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
    except Exception as e:
        log.warning("Failed to get uploads playlist for %s: %s", channel_id, e)
    return None


def fetch_recent_uploads(
    youtube,
    playlist_id: str,
    max_results: int = 50,
    rate_limit: float = 1.0,
) -> list[dict]:
    """Fetch recent videos from a playlist (uploads playlist)."""
    videos = []
    page_token = None

    while len(videos) < max_results:
        try:
            resp = youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=min(50, max_results - len(videos)),
                pageToken=page_token,
            ).execute()

            for item in resp.get("items", []):
                snippet = item["snippet"]
                videos.append({
                    "video_id": snippet["resourceId"]["videoId"],
                    "title": snippet.get("title", ""),
                    "description": snippet.get("description", ""),
                    "published_at": snippet.get("publishedAt", ""),
                    "channel_id": snippet.get("channelId", ""),
                    "channel_title": snippet.get("channelTitle", ""),
                })

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

            time.sleep(rate_limit)

        except Exception as e:
            log.error("Error fetching playlist %s: %s", playlist_id, e)
            break

    return videos


def process_video(
    video: dict,
    channel_config: ChannelConfig,
    openrouter_api_key: str = "",
    openrouter_model: str = "google/gemini-flash-1.5",
    openrouter_base_url: str = "https://openrouter.ai/api/v1",
) -> CaseCandidate:
    """Extract case signals from a single video and build a CaseCandidate."""
    title = video.get("title", "")
    description = video.get("description", "")
    combined_text = f"{title} {description}"
    video_id = video["video_id"]

    # Extract fields with regex first
    suspect_name = _extract_name_regex(combined_text)
    incident_date = _extract_date_regex(combined_text)
    keywords = _extract_keywords(combined_text)

    # If regex missed the name, try LLM
    if not suspect_name and openrouter_api_key:
        log.debug("Regex missed name for %s, trying LLM", video_id)
        suspect_name = _extract_name_llm(
            title, description, openrouter_api_key, openrouter_model, openrouter_base_url
        )
        if suspect_name:
            log.debug("LLM extracted name: %s", suspect_name)

    case_id = _generate_case_id(
        channel_config.state, suspect_name, video_id, video.get("published_at", "")
    )

    return CaseCandidate(
        case_id=case_id,
        video_id=video_id,
        channel_id=video.get("channel_id", channel_config.channel_id or ""),
        channel_name=video.get("channel_title", channel_config.agency_name),
        agency_name=channel_config.agency_name,
        state=channel_config.state,
        city=channel_config.city,
        video_title=title,
        video_description=description[:500],
        video_url=f"https://www.youtube.com/watch?v={video_id}",
        published_at=video.get("published_at", ""),
        suspect_name=suspect_name,
        incident_date=incident_date,
        case_keywords=keywords,
    )


def run_intake(
    youtube_api_key: str,
    channels: list[ChannelConfig],
    max_videos_per_channel: int = 50,
    rate_limit: float = 1.0,
    openrouter_api_key: str = "",
    openrouter_model: str = "google/gemini-flash-1.5",
    openrouter_base_url: str = "https://openrouter.ai/api/v1",
) -> list[CaseCandidate]:
    """Run YouTube intake for all configured channels.

    Returns a list of CaseCandidates for new uploads.
    """
    youtube = build("youtube", "v3", developerKey=youtube_api_key)
    all_candidates = []

    for ch in channels:
        log.info("Processing channel: %s (%s)", ch.handle, ch.agency_name)

        # Resolve channel ID if needed
        if not ch.channel_id:
            ch.channel_id = resolve_channel_id(youtube, ch.handle)
            if not ch.channel_id:
                log.warning("Could not resolve channel ID for %s, skipping", ch.handle)
                continue

        # Get uploads playlist
        if not ch.uploads_playlist_id:
            ch.uploads_playlist_id = get_uploads_playlist_id(youtube, ch.channel_id)
            if not ch.uploads_playlist_id:
                log.warning("Could not get uploads playlist for %s, skipping", ch.handle)
                continue

        # Fetch recent uploads
        videos = fetch_recent_uploads(
            youtube, ch.uploads_playlist_id, max_videos_per_channel, rate_limit
        )
        log.info("Fetched %d videos from %s", len(videos), ch.handle)

        # Process each video
        for video in videos:
            candidate = process_video(
                video, ch, openrouter_api_key, openrouter_model, openrouter_base_url
            )
            all_candidates.append(candidate)
            log.debug(
                "Candidate: %s | name=%s | keywords=%s",
                candidate.case_id,
                candidate.suspect_name or "(none)",
                candidate.case_keywords or "(none)",
            )

        time.sleep(rate_limit)

    log.info("Intake complete: %d candidates from %d channels", len(all_candidates), len(channels))
    return all_candidates
