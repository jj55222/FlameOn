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

# --- Video content classification ---
# Detect non-crime content and context that causes name misattribution.
# Applied BEFORE name extraction so we can skip or adjust behavior.

# Videos matching these title patterns are NOT crime cases — skip entirely
NON_CRIME_TITLE_PATTERNS = [
    r"\b(?:behind\s+the\s+badge|meet\s+(?:our|the)\s+(?:officer|detective|sergeant))\b",
    r"\b(?:memorial|fallen\s+officers?|in\s+memoriam|ultimate\s+sacrifice)\b",
    r"\b(?:swearing[- ]in|graduation|academy|recruit(?:ment|ing)?)\b",
    r"\b(?:community\s+(?:event|outreach|day)|national\s+night\s+out)\b",
    r"\b(?:ride[- ]along|day\s+in\s+the\s+life|meet\s+your)\b",
    r"\b(?:toy\s+drive|charity|fundraiser|holiday|christmas|thanksgiving)\b",
    r"\b(?:k[- ]?9\s+demo|canine\s+demo|open\s+house)\b",
]

# Cold case / unsolved case patterns — these are about VICTIMS, not suspects
COLD_CASE_PATTERNS = [
    r"\bcold\s+case\b",
    r"\bunsolved\b",
    r"\bunidentified\b",
    r"\b(?:anyone\s+with|if\s+you\s+have)\s+(?:any\s+)?information\b",
    r"\bcrime\s+stoppers?\b",
    r"\b(?:come\s+forward|tip\s+line|anonymous\s+tip)\b",
    r"\bwho\s+(?:killed|shot|murdered)\b",
]

# Title/description signals that the video is about an OPERATION or STING
# These are interesting but have no single named suspect
OPERATION_PATTERNS = [
    r"\boperation\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\b",  # "Operation Lucky Charm"
    r"\b(?:sting|bust|sweep|raid|crackdown|roundup)\b",
    r"\b(?:\d+\s+(?:arrested|suspects?|individuals?)\s+(?:in|during|after))\b",
    r"\b(?:arrested?\s+\d+|charged?\s+\d+)\b",  # "arrested 10", "charged 15"
    r"\b(?:drug\s+bust|prostitution\s+sting|trafficking\s+(?:operation|ring))\b",
]

# Context clues that the name extracted is an OFFICER, not a suspect
OFFICER_CONTEXT_PATTERNS = [
    # "Officer X was/responded/arrived/shot" — X is the cop, not the suspect
    r"(?:Officer|Deputy|Detective|Sergeant|Sgt\.|Cpl\.|Corporal|Lt\.|Lieutenant|Captain|Chief|Trooper|Agent)\s+([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})",
    # "Officer X" in a title about OIS
    r"Officer[- ]Involved[- ]Shooting",
]

# Words in description context that indicate the extracted name is an officer
OFFICER_ROLE_PHRASES = [
    "was on patrol", "responded to", "arrived on scene", "arrived at the scene",
    "was in the area", "rendered aid", "the officer shot", "officer was not injured",
    "officer fired", "returned fire", "officer-involved",
    "serves jacksonville", "serves our community", "homicide unit",
    "detective unit", "patrol division", "careers on",
    "hang up his helmet", "joining detective", "behind the badge",
    "sacrificed everything", "fallen officers", "etched a new name",
    "passed away", "killed in the line", "line of duty",
]


def classify_video_content(title: str, description: str) -> dict:
    """Classify video content to detect non-crime videos and misattribution risks.

    Returns dict with:
        skip: bool — True if video is not a crime case (memorial, recruitment, etc.)
        skip_reason: str — Why it was skipped
        is_operation: bool — True if it's a multi-suspect sting/operation
        operation_name: str — Extracted operation name if found
        operation_arrest_count: int — Number of arrests if detectable
        officer_names: list[str] — Names identified as officers (not suspects)
        is_ois: bool — True if officer-involved shooting
        is_cold_case: bool — True if unsolved/cold case (name = victim, not suspect)
    """
    text = f"{title}\n{description}"
    title_lower = title.lower()
    text_lower = text.lower()

    result = {
        "skip": False,
        "skip_reason": "",
        "is_operation": False,
        "operation_name": "",
        "operation_arrest_count": 0,
        "officer_names": [],
        "is_ois": False,
        "is_cold_case": False,
    }

    # Check non-crime title patterns
    for pattern in NON_CRIME_TITLE_PATTERNS:
        if re.search(pattern, title_lower, re.IGNORECASE):
            result["skip"] = True
            result["skip_reason"] = f"Non-crime content: {pattern}"
            return result

    # Check for officer-involved shooting
    if re.search(r"officer[- ]involved[- ]shoot", text_lower):
        result["is_ois"] = True

    # Check for cold cases / unsolved (name extracted = victim, not suspect)
    cold_signals = 0
    for pattern in COLD_CASE_PATTERNS:
        if re.search(pattern, text_lower):
            cold_signals += 1
    if cold_signals >= 2 or "cold case" in title_lower:
        result["is_cold_case"] = True

    # Check for operations/stings
    for pattern in OPERATION_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            result["is_operation"] = True
            # Try to extract the operation name
            op_match = re.search(r"[Oo]peration\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)", text)
            if op_match:
                result["operation_name"] = op_match.group(0)
            # Extract arrest count (e.g. "ten individuals were arrested", "15 arrested")
            count_match = re.search(
                r"(\d+|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|"
                r"thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty"
                r"(?:[\s-](?:one|two|three|four|five|six|seven|eight|nine))?)\s+"
                r"(?:individuals?|people|persons?|suspects?|men|women|were)\s+(?:were\s+)?arrested",
                text_lower,
            )
            if count_match:
                num_word = count_match.group(1)
                word_to_num = {
                    "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
                    "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11,
                    "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
                    "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19,
                    "twenty": 20,
                }
                result["operation_arrest_count"] = (
                    int(num_word) if num_word.isdigit() else word_to_num.get(num_word, 0)
                )
            break

    # Extract officer names — these should NOT be used as suspect names
    for pattern in OFFICER_CONTEXT_PATTERNS:
        for match in re.finditer(pattern, text):
            if match.lastindex and match.group(1):
                result["officer_names"].append(match.group(1).strip())

    # Also check if description context indicates the name belongs to an officer
    # by looking for officer role phrases near any extracted names
    for phrase in OFFICER_ROLE_PHRASES:
        if phrase in text_lower:
            # If behind-the-badge or memorial, skip entirely
            if any(w in phrase for w in ["sacrificed", "fallen", "behind the badge",
                                          "careers on", "hang up", "joining detective"]):
                result["skip"] = True
                result["skip_reason"] = f"Officer profile/memorial: '{phrase}'"
                return result

    return result



# Suspect name patterns (common in LE video titles)
NAME_PATTERNS = [
    # "John Doe Arrested" / "John Doe Sentenced" / "John A. Doe ..."
    r"(?:Body\s*Cam|BWC|Bodycam)?[:\s\-|]*([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})(?:\s*,?\s*(?:\d+|Jr\.|Sr\.|III|II))?(?:\s*[-,]?\s*(?:Arrested|Charged|Sentenced|Convicted|Shot|Killed|Suspect|Defendant))",
    # "John Doe was/were arrested" / "John Doe has been charged" (LE press release body text)
    r"([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})(?:\s+(?:was|were|has\s+been|is))\s+(?:arrested|charged|sentenced|convicted|indicted|booked)",
    # "State v. John Doe" / "State vs. Doe"
    r"(?:State|People|Commonwealth)\s+(?:v\.?|vs\.?)\s+([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})",
    # "Arrest of John Doe" / "Sentencing of John Doe"
    r"(?:Arrest|Sentencing|Conviction|Murder|Shooting|Interrogation)\s+of\s+([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})",
    # "NN-year-old John Doe" / "NN year old John Doe" (common LE press release style)
    r"\b\d{1,2}[\s-]+year[\s-]+old\s+([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})",
    # "identified as John Doe" / "identified the suspect as John Doe"
    r"(?:identified|known)\s+(?:as|the\s+\w+\s+as)\s+([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})",
]

# Date patterns — ordered from most specific to least specific
DATE_PATTERNS = [
    # YYYY-MM-DD (must be before MM-DD-YYYY to avoid partial matches)
    r"(\d{4}-\d{2}-\d{2})",
    # Month DD, YYYY (with optional ordinal suffix)
    r"((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4})",
    # MM/DD/YYYY or MM-DD-YYYY
    r"(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})",
    # "Month DDth" without year (common in LE press releases — e.g. "January 24th", "on March 18")
    r"(?:on\s+)?((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?)",
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


# Common words that look like names but aren't (false positive filter)
NOT_A_NAME = {
    "teen", "teens", "teenage", "teenager",
    "man", "woman", "men", "women", "boy", "girl",
    "suspect", "suspects", "defendant", "defendants",
    "gunman", "gunmen", "shooter", "shooters",
    "victim", "victims", "officer", "officers",
    "deputy", "deputies", "sergeant", "detective",
    "police", "sheriff", "trooper", "agent",
    "child", "children", "juvenile", "minor",
    "year", "old", "male", "female",
    "body", "cam", "bodycam", "footage",
    "arrested", "charged", "sentenced", "convicted",
    "killed", "shot", "wanted", "missing",
    "critical", "incident", "response",
}


def _is_plausible_name(name: str) -> bool:
    """Check if extracted text looks like an actual person name, not a descriptor."""
    parts = name.split()
    if len(parts) < 2 or len(name) <= 5:
        return False
    # Reject if ANY word is a known non-name word
    for p in parts:
        if p.lower() in NOT_A_NAME:
            return False
    # All parts should start with uppercase
    if not all(p[0].isupper() for p in parts if p[0].isalpha()):
        return False
    return True


def _extract_name_regex(text: str) -> str:
    """Try to extract a suspect name using regex patterns.

    Scans ALL patterns and ALL matches, returning the first plausible name.
    This avoids bailing early on false positives like 'Teen Gunmen'.
    """
    for pattern in NAME_PATTERNS:
        for match in re.finditer(pattern, text):
            name = match.group(1).strip()
            if _is_plausible_name(name):
                return name
    return ""


def _extract_date_regex(text: str, published_at: str = "") -> str:
    """Try to extract an incident date from text.

    For month-day-only matches (no year), infers the year from published_at
    if available (the incident likely occurred the same year or previous year).
    """
    for pattern in DATE_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            date_str = match.group(1).strip()
            # If this is a month-day-only match (no 4-digit year), try to add one
            if not re.search(r"\d{4}", date_str) and published_at:
                try:
                    pub_year = published_at[:4]
                    if pub_year.isdigit():
                        date_str = f"{date_str}, {pub_year}"
                except (IndexError, TypeError):
                    pass
            return date_str
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
) -> Optional[CaseCandidate]:
    """Extract case signals from a single video and build a CaseCandidate.

    Returns None if the video is classified as non-crime content
    (memorials, officer profiles, recruitment, etc.).
    """
    title = video.get("title", "")
    description = video.get("description", "")
    combined_text = f"{title} {description}"
    video_id = video["video_id"]

    # Classify video content FIRST to avoid misattribution
    classification = classify_video_content(title, description)

    if classification["skip"]:
        log.info("Skipping non-crime video %s: %s", video_id, classification["skip_reason"])
        return None

    # Extract fields with regex first
    suspect_name = _extract_name_regex(combined_text)
    incident_date = _extract_date_regex(combined_text, video.get("published_at", ""))
    keywords = _extract_keywords(combined_text)

    # Check if extracted name is actually an officer
    officer_names_lower = [n.lower() for n in classification["officer_names"]]
    if suspect_name and suspect_name.lower() in officer_names_lower:
        log.info(
            "Rejected officer name '%s' for %s (is_ois=%s)",
            suspect_name, video_id, classification["is_ois"],
        )
        suspect_name = ""

    # For OIS videos, the first name found is usually the officer — be extra cautious
    if classification["is_ois"] and suspect_name:
        # Check if the name appears near officer-role context
        name_lower = suspect_name.lower()
        desc_lower = description.lower()
        name_pos = desc_lower.find(name_lower)
        if name_pos >= 0:
            # Look at 200 chars around the name for officer context
            context_start = max(0, name_pos - 100)
            context_end = min(len(desc_lower), name_pos + len(name_lower) + 100)
            context = desc_lower[context_start:context_end]
            for phrase in OFFICER_ROLE_PHRASES:
                if phrase in context:
                    log.info(
                        "Rejected OIS officer name '%s' for %s (context: '%s')",
                        suspect_name, video_id, phrase,
                    )
                    suspect_name = ""
                    break

    # If regex missed the name, try LLM
    if not suspect_name and openrouter_api_key:
        log.debug("Regex missed name for %s, trying LLM", video_id)
        suspect_name = _extract_name_llm(
            title, description, openrouter_api_key, openrouter_model, openrouter_base_url
        )
        if suspect_name:
            # Double-check LLM result against officer names too
            if suspect_name.lower() in officer_names_lower:
                log.info("Rejected LLM officer name '%s' for %s", suspect_name, video_id)
                suspect_name = ""
            else:
                log.debug("LLM extracted name: %s", suspect_name)

    # Cold cases: no suspect exists — skip entirely
    if classification["is_cold_case"]:
        log.info("Skipping cold case %s — '%s' is the victim, not a suspect", video_id, suspect_name)
        return None

    # Tag operations/stings in keywords AND use operation name as identifier
    if classification["is_operation"]:
        op_tag = classification["operation_name"] or "sting_operation"
        if classification["operation_arrest_count"]:
            op_tag = f"{op_tag} ({classification['operation_arrest_count']} arrests)"
        if keywords:
            keywords = f"{keywords}, {op_tag}"
        else:
            keywords = op_tag
        # Use operation name as the case identifier when no suspect name
        if not suspect_name and classification["operation_name"]:
            suspect_name = classification["operation_name"]

    if classification["is_ois"]:
        if "officer-involved" not in (keywords or ""):
            keywords = f"{keywords}, officer-involved" if keywords else "officer-involved"

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

    # Parse cutoff dates for client-side filtering (used with playlist API)
    cutoff_before = None
    cutoff_after = None
    if video_published_before:
        try:
            raw = video_published_before.split("T")[0]
            cutoff_before = datetime.fromisoformat(raw)
            if cutoff_before.tzinfo is None:
                from datetime import timezone
                cutoff_before = cutoff_before.replace(tzinfo=timezone.utc)
        except ValueError:
            log.warning("Invalid video_published_before '%s', ignoring", video_published_before)
    if video_published_after:
        try:
            raw = video_published_after.split("T")[0]
            cutoff_after = datetime.fromisoformat(raw)
            if cutoff_after.tzinfo is None:
                from datetime import timezone
                cutoff_after = cutoff_after.replace(tzinfo=timezone.utc)
        except ValueError:
            log.warning("Invalid video_published_after '%s', ignoring", video_published_after)

    if video_published_before:
        log.info("Date filter: videos published before %s", video_published_before)
    if video_published_after:
        log.info("Date filter: videos published after %s", video_published_after)

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

        # Derive uploads playlist ID if not set (UC -> UU swap)
        if not ch.uploads_playlist_id and ch.channel_id.startswith("UC"):
            ch.uploads_playlist_id = "UU" + ch.channel_id[2:]

        # Cap videos to fetch based on remaining quota
        videos_to_fetch = max_videos_per_channel
        if max_total_videos > 0:
            remaining = max_total_videos - total_videos_fetched
            if remaining <= 0:
                log.info("Reached max_total_videos cap (%d), stopping intake", max_total_videos)
                break
            videos_to_fetch = min(videos_to_fetch, remaining)

        # Fetch strategy: prefer playlist API (1 unit/page) over search API (100 units/page).
        # Playlist API doesn't support date filtering, so we filter client-side.
        # Only fall back to search API if we can't get the uploads playlist.
        if ch.uploads_playlist_id:
            videos = fetch_recent_uploads(
                youtube, ch.uploads_playlist_id, videos_to_fetch, rate_limit
            )
            # Client-side date filtering
            if cutoff_before or cutoff_after:
                filtered = []
                for v in videos:
                    pub = v.get("published_at", "")
                    if not pub:
                        filtered.append(v)
                        continue
                    try:
                        pub_dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                        if cutoff_before and pub_dt >= cutoff_before:
                            continue
                        if cutoff_after and pub_dt < cutoff_after:
                            continue
                        filtered.append(v)
                    except ValueError:
                        filtered.append(v)
                log.info("Date-filtered %d -> %d videos from %s", len(videos), len(filtered), ch.handle)
                videos = filtered
        else:
            # Fallback: search API with server-side date filtering
            videos = fetch_channel_videos_by_date(
                youtube, ch.channel_id, videos_to_fetch, rate_limit,
                published_before=video_published_before,
                published_after=video_published_after,
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
            # Skip non-crime content (memorials, profiles, etc.)
            if candidate is None:
                skipped += 1
                continue
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
