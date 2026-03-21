"""Stage 1 — YouTube Intake & Field Extraction.
site: https://colab.research.google.com/drive/1B547J_-Zm7L-RtQABWB8CcrD6wJp_gQa?authuser=0
Monitors uploads playlists for tracked agency channels.
Extracts case signals from video title and description.
"""

import re
import time
from datetime import datetime
from typing import Optional

import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .logger import get_logger
from .models import CaseCandidate, ChannelConfig

log = get_logger()

# --- Regex patterns for field extraction ---

# Suspect name patterns (common in LE video titles)
NAME_PATTERNS = [
    # "John Doe Arrested" / "John Doe Sentenced" / "John A. Doe ..."
    r"(?:Body\s*Cam|BWC|Bodycam)?[:\s\-|]*([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})(?:\s*,?\s*(?:\d+|Jr\.|Sr\.|III|II))?(?:\s*[-,]?\s*(?:Arrested|Charged|Sentenced|Convicted|Shot|Killed|Suspect|Defendant))",
    # "State v. John Doe" / "State vs. Doe"
    r"(?:State|People|Commonwealth)\s+(?:v\.?|vs\.?)\s+([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})",
    # "Arrest of John Doe" / "Sentencing of John Doe"
    r"(?:Arrest|Sentencing|Conviction|Murder|Shooting|Interrogation)\s+of\s+([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})",
    # "NN-year-old John Doe" / "NN year old John Doe" (common LE press release style)
    r"\b\d{1,2}[\s-]+year[\s-]+old\s+([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})",
    # "identified as John Doe" / "identified the suspect as John Doe"
    r"(?:identified|known)\s+(?:as|the\s+\w+\s+as)\s+([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})",
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
        "Extract the primary suspect or defendant full name from this law enforcement video. "
        "The name may appear in the title OR anywhere in the description — look carefully. "
        "Common patterns: 'NN-year-old [Name]', 'arrested [Name]', 'charged [Name]', "
        "'identified as [Name]', or just a name mentioned in context of a crime.\n"
        "Return ONLY the full name (first and last), or 'NONE' if no clear suspect name is present. "
        "Do not guess or hallucinate.\n\n"
        f"Title: {title}\n"
        f"Description: {description[:2000]}\n\n"
        "Suspect name:"
    )

    try:
        resp = requests.post(
            f"{openrouter_base_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {openrouter_api_key.strip()}",
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
    """Resolve a YouTube @handle to a channel ID.

    Uses channels.list with forHandle (1 quota unit) first, only falls
    back to search.list (100 units) if the handle can't be resolved.
    """
    clean_handle = handle.lstrip("@")

    # Primary: channels.list with forHandle — 1 quota unit
    try:
        resp = youtube.channels().list(
            part="id,contentDetails",
            forHandle=clean_handle,
        ).execute()
        items = resp.get("items", [])
        if items:
            return items[0]["id"]
    except HttpError as e:
        if e.resp.status == 403 and "quotaExceeded" in str(e):
            raise  # Let caller handle quota exhaustion
        log.debug("channels.list forHandle failed for %s: %s", handle, e)
    except Exception as e:
        log.debug("channels.list forHandle failed for %s: %s", handle, e)

    # Fallback: search.list — 100 quota units
    try:
        resp = youtube.search().list(
            part="snippet",
            q=f"@{clean_handle}",
            type="channel",
            maxResults=1,
        ).execute()
        items = resp.get("items", [])
        if items:
            return items[0]["snippet"]["channelId"]
    except HttpError as e:
        if e.resp.status == 403 and "quotaExceeded" in str(e):
            raise
        log.warning("Failed to resolve channel ID for %s: %s", handle, e)
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


def _hydrate_full_descriptions(youtube, videos: list[dict], rate_limit: float = 1.0) -> None:
    """Replace truncated descriptions with full ones via videos.list API.

    Both search.list and playlistItems.list return truncated descriptions.
    This batches video IDs (up to 50 per call) and fetches full snippets.
    Costs 1 quota unit per call (same as playlistItems).
    """
    if not videos:
        return

    for i in range(0, len(videos), 50):
        batch = videos[i:i + 50]
        video_ids = ",".join(v["video_id"] for v in batch)
        try:
            resp = youtube.videos().list(
                part="snippet",
                id=video_ids,
            ).execute()

            # Build lookup by video ID
            full_snippets = {}
            for item in resp.get("items", []):
                full_snippets[item["id"]] = item["snippet"]

            # Update descriptions in place
            for v in batch:
                snippet = full_snippets.get(v["video_id"])
                if snippet:
                    v["description"] = snippet.get("description", v["description"])

            if i + 50 < len(videos):
                time.sleep(rate_limit)

        except Exception as e:
            log.error("Error hydrating descriptions: %s", e)


def fetch_channel_videos_by_date(
    youtube,
    channel_id: str,
    max_results: int = 50,
    rate_limit: float = 1.0,
    published_before: str = "",
    published_after: str = "",
) -> list[dict]:
    """Fetch videos from a channel using search API with date filtering.

    Uses YouTube's search.list endpoint which supports publishedBefore/After
    server-side, so only videos in the target date range are returned.
    More quota-efficient than fetching all uploads and filtering client-side.

    Note: search.list costs 100 quota units vs 1 for playlistItems.list,
    but avoids fetching hundreds of irrelevant recent videos.
    """
    videos = []
    page_token = None

    # Build search params
    search_params = {
        "part": "snippet",
        "channelId": channel_id,
        "type": "video",
        "order": "date",
        "maxResults": min(50, max_results),
    }
    if published_before:
        # YouTube API requires RFC 3339: "2024-07-01T00:00:00Z"
        if "T" not in published_before:
            published_before = f"{published_before}T00:00:00Z"
        search_params["publishedBefore"] = published_before
    if published_after:
        if "T" not in published_after:
            published_after = f"{published_after}T00:00:00Z"
        search_params["publishedAfter"] = published_after

    while len(videos) < max_results:
        try:
            if page_token:
                search_params["pageToken"] = page_token
            search_params["maxResults"] = min(50, max_results - len(videos))

            resp = youtube.search().list(**search_params).execute()

            for item in resp.get("items", []):
                snippet = item["snippet"]
                videos.append({
                    "video_id": item["id"]["videoId"],
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
            log.error("Error searching channel %s: %s", channel_id, e)
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
        video_description=description,
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
    max_total_videos: int = 0,
    video_published_before: str = "",
    video_published_after: str = "",
    on_channel_complete: callable = None,
) -> list[CaseCandidate]:
    """Run YouTube intake for all configured channels.

    Returns a list of CaseCandidates for new uploads.
    If max_total_videos > 0, limits the total number of videos fetched
    across all channels to save YouTube API quota.
    If video_published_before is set (ISO date like "2025-06-01"), only
    videos published before that date are processed — skips recent videos
    that are unlikely to have closed cases.
    If on_channel_complete is provided, it's called with the channel's
    candidates after each channel finishes — enables incremental writes.
    """
    youtube = build("youtube", "v3", developerKey=youtube_api_key)
    all_candidates = []
    total_videos_fetched = 0
    use_search_api = bool(video_published_before)

    if use_search_api:
        log.info("Using search API — filtering videos published before %s", video_published_before)
    else:
        log.info("Using playlist API (no date filter)")

    for ch in channels:
        log.info("Processing channel: %s (%s)", ch.handle, ch.agency_name)

        # Resolve channel ID if needed
        if not ch.channel_id:
            try:
                ch.channel_id = resolve_channel_id(youtube, ch.handle)
            except HttpError as e:
                if e.resp.status == 403 and "quotaExceeded" in str(e):
                    log.error("YouTube API quota exhausted — stopping intake. Quota resets at midnight Pacific.")
                    break
                raise
            if not ch.channel_id:
                log.warning("Could not resolve channel ID for %s, skipping", ch.handle)
                continue

        # Cap videos to fetch based on remaining quota
        videos_to_fetch = max_videos_per_channel
        if max_total_videos > 0:
            remaining = max_total_videos - total_videos_fetched
            if remaining <= 0:
                log.info("Reached max_total_videos cap (%d), stopping intake", max_total_videos)
                break
            videos_to_fetch = min(videos_to_fetch, remaining)

        # Fetch videos — use search API with date filter, or playlist API
        if use_search_api:
            videos = fetch_channel_videos_by_date(
                youtube, ch.channel_id, videos_to_fetch, rate_limit,
                published_before=video_published_before,
                published_after=video_published_after,
            )
        else:
            # Need uploads playlist for playlist API
            if not ch.uploads_playlist_id:
                ch.uploads_playlist_id = get_uploads_playlist_id(youtube, ch.channel_id)
                if not ch.uploads_playlist_id:
                    log.warning("Could not get uploads playlist for %s, skipping", ch.handle)
                    continue
            videos = fetch_recent_uploads(
                youtube, ch.uploads_playlist_id, videos_to_fetch, rate_limit
            )

        total_videos_fetched += len(videos)
        log.info("Fetched %d videos from %s (total fetched: %d)", len(videos), ch.handle, total_videos_fetched)

        # Hydrate full descriptions (search/playlist APIs return truncated ones)
        _hydrate_full_descriptions(youtube, videos, rate_limit)

        # Process each video — only keep videos with crime-related signals
        channel_candidates = []
        skipped = 0
        for video in videos:
            candidate = process_video(
                video, ch, openrouter_api_key, openrouter_model, openrouter_base_url
            )
            # Filter: must have at least a suspect name OR crime keywords
            if not candidate.suspect_name and not candidate.case_keywords:
                skipped += 1
                log.debug("Skipped (no signals): %s — %s", candidate.video_id, candidate.video_title[:80])
                continue
            channel_candidates.append(candidate)
            log.debug(
                "Candidate: %s | name=%s | keywords=%s",
                candidate.case_id,
                candidate.suspect_name or "(none)",
                candidate.case_keywords or "(none)",
            )
        if skipped:
            log.info("Skipped %d/%d videos from %s (no crime signals)", skipped, len(videos), ch.handle)

        all_candidates.extend(channel_candidates)

        # Incremental callback — lets caller write to sheet per-channel
        if on_channel_complete and channel_candidates:
            on_channel_complete(channel_candidates)

        time.sleep(rate_limit)

    log.info("Intake complete: %d candidates from %d channels, %d videos fetched", len(all_candidates), len(channels), total_videos_fetched)
    return all_candidates
