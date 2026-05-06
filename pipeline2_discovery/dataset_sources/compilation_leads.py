"""Compilation-channel case-lead intake.

Inverts the per-case YouTube search direction (PRs #34–#42's
``ytsearch:`` lane). Instead of searching YouTube for each
candidate, this module reads metadata for *compilation* videos —
typically lightly-edited bodycam / dashcam compilations from
channels like NYDETECTIVE, Real Body Cams, Crime Time Cam,
ThisIsBodycam, Uncovered — and extracts per-case leads from their
descriptions, chapter markers, and titles.

Why this lane: the v2 25-candidate per-case YouTube smoke (post-
PR #41) produced 1 high-confidence URL out of 25 candidates (4%).
Compilation videos publish multi-case footage with descriptions
that name each case, so a single video can yield multiple leads.

Input shape (this PR ships parsing only — no live fetch):

    {
      "video_id": "abc123",
      "url": "https://www.youtube.com/watch?v=abc123",
      "title": "Bodycam compilation: 5 wild traffic stops",
      "uploader": "Real Body Cams",
      "description": "...",
      "chapters": [
        {"start_time": 0, "title": "..."},
        {"start_time": 123, "title": "..."}
      ],
      "upload_date": "20240615"
    }

The parser scans title / description / chapter titles for:
  - subject names (proper-noun bigrams / trigrams)
  - agency names ("X Police Department" / "Y County Sheriff")
  - city + state
  - dates / years
  - incident terms (bodycam, dashcam, pursuit, OIS, …)
  - links (YouTube, agency, news, MuckRock URLs)

Each anchor produces one lead. Leads are deduplicated (subject
last name + agency/city/state, or chapter+source if no subject)
and emitted as :class:`DatasetCandidate` rows ready for the
existing dataset-intake → enrichment → transcript-scoring path.

Out of scope:
  - Live yt-dlp metadata fetch (deferred — operator supplies
    metadata in JSON/JSONL).
  - Channel-level scraping.
  - Caption/transcript fetch (PR #40/#42 lane already exists).
  - Whisper / speech-to-text.
  - Video / audio downloads.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .models import DatasetCandidate, NextActionHint
from .scoring import assign_grade, evidence_strength_from_grade


SOURCE_LANE = "compilation_leads"
SOURCE_DATASET = "youtube_compilation"


# ---- term / pattern constants ---------------------------------------


# Incident artifact terms — finding any of these in title/description
# /chapter signals "this is about an actual incident, not a generic
# discussion" and contributes to grade scoring.
INCIDENT_TERMS: Tuple[str, ...] = (
    "bodycam", "body cam", "body camera", "body-worn camera",
    "body worn camera", "bwc",
    "dashcam", "dash cam", "dash-cam",
    "police chase", "pursuit", "high-speed chase",
    "officer-involved shooting", "officer involved shooting",
    "deputy-involved shooting", "deputy involved shooting",
    "police shooting",
    "critical incident",
    "interrogation", "police interview",
    "traffic stop",
    "arrest", "arrested",
    "ois", "ovi", "dui",
    "in-custody death", "in custody death",
)

# Strong-incident terms — same set as MuckRock's INCIDENT_TERMS;
# specifically signal "the case has an investigation-worthy
# incident" rather than just "video footage".
STRONG_INCIDENT_TERMS: Tuple[str, ...] = (
    "officer-involved shooting", "officer involved shooting",
    "deputy-involved shooting", "deputy involved shooting",
    "police shooting",
    "critical incident",
    "in-custody death", "in custody death",
)

# Channel-name uploaders that should never be treated as a "subject
# name" — they're publishing entities, not case subjects.
COMPILATION_CHANNEL_UPLOADERS: Tuple[str, ...] = (
    "real body cams", "real bodycam",
    "nydetective", "ny detective",
    "crime time cam", "crime time",
    "thisisbodycam", "this is bodycam",
    "uncovered", "officer footage",
    "police activity", "audit the audit",
    "nydaily", "midwest safety",
)

# US state name → 2-letter postal code. Mirrors the YouTube provider's
# US_STATE_NAMES but kept local so the dataset_sources package doesn't
# import from enrichment.
US_STATE_POSTAL: Dict[str, str] = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT",
    "delaware": "DE", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME",
    "maryland": "MD", "massachusetts": "MA", "michigan": "MI",
    "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
    "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND",
    "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA",
    "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}
STATE_ABBRS: frozenset = frozenset(US_STATE_POSTAL.values())

# Regex patterns -------------------------------------------------------

# Proper-noun candidate. Matches strict 2-word names ("Maria Garcia")
# OR 3-word names with an explicit middle initial ("Joe W. Gold").
# Avoids 3-word patterns without a middle initial because those
# routinely chain into city / agency words (e.g. "John Smith Phoenix"
# from "John Smith Phoenix Police Department"). Filtered downstream
# against NAME_STOPWORDS to drop "Officer X" / month names / etc.
_PROPER_NOUN_RE = re.compile(
    r"\b("
    r"[A-Z][a-z]+\s+[A-Z]\.\s+[A-Z][a-z]+"     # 3-word with middle initial
    r"|"
    r"[A-Z][a-z]+\s+[A-Z][a-z]+"                # 2-word name
    r")\b"
)

# Agency suffix patterns (used by the back-walk extractor below).
# We deliberately do NOT capture the qualifier tokens in the regex:
# leftmost-longest matching makes "John Smith Phoenix Police
# Department" capture all 5 words, blending a person name into the
# agency. Instead we match only the suffix and walk back from there.
_AGENCY_SUFFIX_RE = re.compile(
    r"\b("
    r"Police\s+Department|Police\s+Services|"
    r"Sheriff(?:'s)?\s+Office|Sheriff\s+Department|"
    r"County\s+Sheriff|"
    r"Highway\s+Patrol|State\s+Police|DPS"
    r")\b"
)

# Words that justify extending the qualifier to 2 cap tokens (e.g.
# "Pinal County Sheriff's Office", "Crystal City Police Department",
# "Oklahoma City Police Department"). Without one of these as the
# last cap token, we keep the qualifier at 1 word — that's how we
# avoid blending a 2-word person name into the agency.
_JURISDICTION_WORDS = frozenset({
    "county", "city", "parish", "borough", "township", "village",
})

# Date patterns: ISO (YYYY-MM-DD), US (MM/DD/YYYY), bare year.
_ISO_DATE_RE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")
_US_DATE_RE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b")
_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")

# City + state pattern: "Phoenix, AZ" / "San Francisco, CA"
_CITY_STATE_RE = re.compile(
    r"\b([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,3}),\s+([A-Z]{2})\b"
)

# URL-extraction regexes (kept narrow so generic words don't match)
_YT_URL_RE = re.compile(
    r"https?://(?:www\.|m\.)?(?:youtube\.com/watch\?[^\s]+|youtu\.be/[A-Za-z0-9_\-]+[^\s]*)",
    re.IGNORECASE,
)
_MUCKROCK_URL_RE = re.compile(
    r"https?://(?:www\.)?muckrock\.com/[^\s]+",
    re.IGNORECASE,
)
_GOV_URL_RE = re.compile(
    r"https?://[^\s]*\.(?:gov|us)/[^\s]*",
    re.IGNORECASE,
)
_NEWS_HOST_RE = re.compile(
    r"https?://(?:[\w\-]+\.)*"
    r"(?:abc\d*|nbc\d*|cbs\d*|fox\d*|cnn|usatoday|reuters|ap|"
    r"apnews|nytimes|washingtonpost|wsj|tampabay|tucson|gvwire|"
    r"latimes|chicagotribune|sfchronicle|kvue|wsbtv|wfaa|wfla|"
    r"news\d*|local\d*|wbtv|wtsp|wkbn|wlwt|kake|kshb|wjla|wsoctv|"
    r"news4jax|click2houston|wftv|nbcdfw|bnn|wlrn|whio)\."
    r"\w+(?:/[^\s]*)?",
    re.IGNORECASE,
)


# Stopwords that look like proper nouns but aren't subjects.
NAME_STOPWORDS = frozenset({
    # months
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
    # generic title prefixes
    "officer", "officers", "deputy", "deputies", "sergeant", "sgt",
    "lieutenant", "lt", "captain", "cpt", "chief",
    "detective", "trooper",
    # agency / institutional words
    "police", "department", "sheriff", "county", "city", "state",
    "patrol", "highway", "force", "bureau", "agency",
    # camera / footage words
    "bodycam", "dashcam", "camera", "footage", "video",
    # cardinal / generic
    "north", "south", "east", "west", "central",
    # incident artifact words that show up in title-case headlines.
    # Note: "chase" is deliberately NOT here — "Chase" is a real
    # first name (e.g. Chase James Bulmahn from sfchronicle_pursuits:
    # 2848). Artifact-term detection for "police chase" / "high-speed
    # chase" lives in INCIDENT_TERMS, not in this name-side filter.
    "arrest", "stop", "shooting", "incident",
    "before", "after", "during",
})


# Everyday English short words that show up in title-case news /
# entertainment headlines but are never personal names. Together
# with HEADLINE_ROLE_NOUNS + HEADLINE_ACTION_VERBS, these define
# the "this is a headline phrase, not a name" filter — see
# :func:`_looks_like_headline_phrase`.
HEADLINE_FILLER_TOKENS = frozenset({
    # number words
    "one", "two", "three", "four", "five", "six", "seven", "eight",
    "nine", "ten",
    # pronouns
    "his", "her", "him", "she", "they", "them", "their", "our",
    "your", "who", "whom", "whose", "we", "us",
    # temporal
    "day", "days", "week", "weeks", "month", "months", "year",
    "years", "hour", "hours", "minute", "minutes",
    "today", "yesterday", "tomorrow", "now", "then", "soon",
    "again", "still",
    # positional / directional
    "up", "down", "out", "off", "into", "over", "under", "back",
    "around", "ahead", "behind", "above", "below",
    # adverbs / adjectives common in title-case
    "late", "early", "well", "badly", "quickly", "slowly", "very",
    "almost", "just", "even", "much", "much",
    # quantity
    "all", "many", "few", "some", "more", "most", "less", "least",
    "any",
    # conjunctions / particles
    "but", "and", "yet", "so", "nor", "as",
    # demonstratives
    "this", "that", "these", "those", "here", "there",
    # common verbs that appear standalone in title-case
    "find", "finds", "found", "give", "gives", "gave", "take",
    "takes", "took", "get", "gets", "got", "make", "makes", "made",
    "say", "says", "said", "see", "sees", "saw",
    "end", "ends", "ended", "begin", "begins", "started",
    "backfires", "succeeded",
    # short nouns common in entertainment-bodycam headlines
    "son", "daughter", "child", "kid", "car", "house", "phone",
    "club", "badge", "thing", "things", "place", "time", "way",
    "fiance", "fiancee",
    # generic adjectives
    "fake", "real", "missing", "dead", "alive", "lost", "new",
    "old", "young", "good", "bad", "best", "worst",
    "wild", "crazy", "weird", "strange", "epic", "terribly",
})


# Common English nouns / role labels that show up in entertainment-
# bodycam channel titles ("Mom Reports...", "Brat Threatens...",
# "Entitled Customer Refuses..."). When a 2-word "subject" candidate
# starts with one of these, it is almost never a real name; reject.
HEADLINE_ROLE_NOUNS = frozenset({
    "mom", "moms", "mum", "dad", "dads", "father", "mother", "parent",
    "son", "daughter", "kid", "kids", "child", "children", "teen", "teens",
    "boy", "girl", "man", "woman", "men", "women", "guy", "lady",
    "couple", "neighbor", "neighbors", "stranger",
    "passenger", "passengers", "driver", "drivers", "pedestrian",
    "customer", "customers", "patron", "guest",
    "suspect", "suspects", "victim", "victims",
    "cop", "cops", "officer", "officers", "deputy", "deputies",
    "trooper", "troopers", "detective",
    "boss", "employee", "worker", "manager",
    "brat", "punk", "thug", "thief", "robber", "criminal",
    "entitled", "fake", "missing", "wild", "drunk", "angry", "young",
    "old", "former", "new",
})


# Verbs and headline-action words that appear as the second word of a
# title-case sentence ("X Reports...", "X Threatens...", "X Refuses...").
# When a 2-word "subject" candidate ends with one of these, it is almost
# never a real name; reject.
HEADLINE_ACTION_VERBS = frozenset({
    "reports", "threatens", "refuses", "demands", "flashes", "shows",
    "pulls", "caught", "goes", "finds", "arrives", "calls", "claims",
    "tells", "asks", "argues", "fights", "tries", "attempts",
    "drives", "runs", "flees", "walks", "stops", "starts", "begins",
    "ends", "leaves", "returns", "appears", "disappears",
    "discovers", "reveals", "admits", "denies", "confronts", "exposes",
    "warns", "yells", "screams", "kicks", "throws", "hits", "punches",
    "shoots", "shouts",
    "loses", "wins", "fails", "succeeds",
    "during", "after", "before", "while", "until",
})


def _looks_like_headline_phrase(name: str) -> bool:
    """Heuristic: True when ``name`` is almost certainly a title-case
    headline phrase rather than a real subject name.

    Rule: reject if **any** token is in any of the four stopword
    sets — :data:`HEADLINE_ROLE_NOUNS`, :data:`HEADLINE_ACTION_VERBS`,
    :data:`HEADLINE_FILLER_TOKENS`, :data:`NAME_STOPWORDS`. This is
    deliberately conservative — a single English-headline token
    anywhere in the candidate (e.g. "But Cops", "Two Days",
    "Find Him", "End Well") flips the candidate to "headline phrase".
    Real names like "Christopher Vang" / "Ivonne Reyes" / "John
    Smith" / "Maria Garcia" / "Joe Gold" / "Michael Beaver" /
    "Chase Bulmahn" pass cleanly because none of their tokens match.

    Returns ``True`` for empty / whitespace-only input as a safety
    fallback.
    """
    if not name or not name.strip():
        return True
    tokens = re.findall(r"[a-z]+", name.lower())
    if not tokens:
        return True
    # Token-length sanity: a name token shorter than 3 chars is
    # almost certainly a particle / pronoun (Up, He, It, To, On,
    # An). Reject without looking at the stopword sets.
    if any(len(t) < 3 for t in tokens):
        return True
    headline_universe = (
        HEADLINE_ROLE_NOUNS
        | HEADLINE_ACTION_VERBS
        | HEADLINE_FILLER_TOKENS
        | NAME_STOPWORDS
    )
    return any(t in headline_universe for t in tokens)


# ---- input record + helpers -----------------------------------------


@dataclass
class CompilationVideo:
    """One compilation-video metadata record loaded from the operator
    input JSONL. All fields optional except ``video_id`` and ``url``."""
    video_id: str
    url: str
    title: str = ""
    uploader: str = ""
    description: str = ""
    chapters: List[Dict[str, Any]] = field(default_factory=list)
    upload_date: str = ""  # YYYYMMDD per yt-dlp convention


def _norm(s: Optional[str]) -> str:
    return (s or "").lower()


def parse_compilation_video(record: Dict[str, Any]) -> CompilationVideo:
    """Construct a CompilationVideo from a JSON record. Tolerant —
    missing fields default to empty strings or empty lists."""
    return CompilationVideo(
        video_id=str(record.get("video_id") or "").strip(),
        url=str(record.get("url") or "").strip(),
        title=str(record.get("title") or "").strip(),
        uploader=str(record.get("uploader") or "").strip(),
        description=str(record.get("description") or ""),
        chapters=list(record.get("chapters") or []),
        upload_date=str(record.get("upload_date") or "").strip(),
    )


# ---- per-segment lead extraction ------------------------------------


@dataclass
class _Segment:
    """One source-text segment from which a lead is extracted.

    A compilation video typically yields N+1 segments — one per
    chapter, plus a "video-level" segment for any leads found in
    the title or whole description.
    """
    text: str  # the chapter title or video title used as the segment header
    body: str  # the chapter or video body used to scan for fields
    start_time: Optional[int] = None  # chapter start in seconds (None for video-level)


def _segments_for(video: CompilationVideo) -> List[_Segment]:
    """Build the list of source-text segments for one video.

    If the video has chapters, each chapter becomes one segment with
    the chapter title + a slice of the description body identifiable
    via the chapter title. If the video has no chapters, a single
    video-level segment covers the whole title + description.
    """
    if video.chapters:
        out: List[_Segment] = []
        for ch in video.chapters:
            ch_title = str(ch.get("title") or "").strip()
            start = ch.get("start_time")
            try:
                start_int = int(start) if start is not None else None
            except (TypeError, ValueError):
                start_int = None
            # Body for chapter scoring: chapter title + a small excerpt
            # of the description that mentions the chapter title (when
            # operators paste per-chapter notes). For a first-pass
            # parser we just use the chapter title + the entire video
            # description — keeps the heuristic simple, accepts some
            # cross-chapter leakage in dedup logic downstream.
            body_parts: List[str] = [ch_title]
            if video.description:
                body_parts.append(video.description)
            out.append(_Segment(
                text=ch_title or video.title,
                body="\n".join(body_parts),
                start_time=start_int,
            ))
        return out

    # No chapters → single video-level segment
    return [_Segment(
        text=video.title,
        body="\n".join((video.title or "", video.description or "")),
        start_time=None,
    )]


def _extract_subject_name(text: str, *, uploader: str) -> Optional[str]:
    """Return a best-effort subject name from a text segment, or
    None. Scans line-by-line so the regex never spans a newline
    boundary (which would let "John Smith\\nPhoenix" merge into a
    single false-positive candidate). Filters out compilation-
    channel uploader names and common non-subject capitalised
    tokens."""
    if not text:
        return None
    uploader_l = _norm(uploader)
    is_compilation_channel = any(
        token in uploader_l for token in COMPILATION_CHANNEL_UPLOADERS
    )

    for line in text.splitlines():
        for raw in _PROPER_NOUN_RE.findall(line):
            cand = raw.strip()
            if not cand:
                continue
            words = [w for w in re.split(r"\s+", cand) if w]
            if len(words) < 2:
                continue
            if any(w.lower().rstrip(".,;:'") in NAME_STOPWORDS for w in words):
                continue
            if any(w.lower() in {"police", "department", "sheriff", "county",
                                  "city", "state"} for w in words):
                continue
            if is_compilation_channel and _norm(cand) in uploader_l:
                continue
            if _norm(cand) in US_STATE_POSTAL:
                continue
            # Reject sensational title-case headline phrases like
            # "Mom Reports" / "Brat Threatens" / "Entitled Customer"
            # / "Flashes Fake" — these match the proper-noun regex
            # but are almost never real subject names.
            if _looks_like_headline_phrase(cand):
                continue
            return cand
    return None


def _is_capitalised_token(tok: str) -> bool:
    """True for tokens that look like a proper-noun word (initial
    capital, rest lowercase). Punctuation is stripped before checking."""
    t = (tok or "").strip(".,;:'\"")
    if not t or len(t) < 2:
        return False
    return t[0].isupper() and t[1:].islower()


def _walk_back_qualifier(tokens_before_suffix: List[str]) -> List[str]:
    """Given the tokens preceding an agency suffix, walk backward
    to capture the agency's qualifier (e.g. "Pinal County" or
    "Phoenix" or "Crystal City"). Returns the qualifier as a list
    of original tokens (with punctuation preserved).

    Heuristic: take exactly 1 cap token by default. Extend to 2 cap
    tokens only when the last cap token is a jurisdiction word
    (county / city / parish / borough / township / village). This
    avoids gobbling person-name prefixes like "John Smith" before
    a city: ``"John Smith Phoenix Police Department"`` walks back
    to ``["Phoenix"]``, not ``["Smith", "Phoenix"]``.
    """
    if not tokens_before_suffix:
        return []
    last = tokens_before_suffix[-1]
    if not _is_capitalised_token(last):
        return []
    qualifier: List[str] = [last]
    last_clean = last.strip(".,;:'\"").lower()
    if last_clean in _JURISDICTION_WORDS and len(tokens_before_suffix) >= 2:
        prev = tokens_before_suffix[-2]
        if _is_capitalised_token(prev):
            qualifier.insert(0, prev)
    return qualifier


def _extract_agency(text: str) -> Optional[str]:
    """Return the first agency-style phrase found in text, or None.

    Scans line-by-line, then for each line: locates an agency-suffix
    match (``Police Department`` / ``Sheriff's Office`` / etc.) and
    walks back via :func:`_walk_back_qualifier` to capture the
    qualifier. Pinned by tests so a person name preceding an
    agency suffix never gets concatenated into the agency.
    """
    if not text:
        return None
    for line in text.splitlines():
        m = _AGENCY_SUFFIX_RE.search(line)
        if not m:
            continue
        prefix_text = line[: m.start()].rstrip()
        if not prefix_text:
            continue
        tokens_before = re.split(r"\s+", prefix_text)
        qualifier = _walk_back_qualifier(tokens_before)
        if not qualifier:
            continue
        agency = " ".join(qualifier) + " " + m.group(1).strip()
        return agency.strip()
    return None


def _extract_city_state(text: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (city, state_abbr) from a "City, ST" pattern, else
    (None, None)."""
    if not text:
        return None, None
    m = _CITY_STATE_RE.search(text)
    if m:
        city = m.group(1).strip()
        state = m.group(2).strip().upper()
        if state in STATE_ABBRS:
            return city, state
    return None, None


def _extract_date(text: str) -> Optional[str]:
    """Best-effort incident-date extraction. Returns ISO if parsable,
    else year-only as ``YYYY``, else None."""
    if not text:
        return None
    m = _ISO_DATE_RE.search(text)
    if m:
        return m.group(0)
    m = _US_DATE_RE.search(text)
    if m:
        mm, dd, yy = m.group(1), m.group(2), m.group(3)
        if len(yy) == 2:
            yy = "20" + yy if int(yy) < 50 else "19" + yy
        try:
            return f"{int(yy):04d}-{int(mm):02d}-{int(dd):02d}"
        except ValueError:
            return None
    m = _YEAR_RE.search(text)
    if m:
        return m.group(1)
    return None


def _extract_artifact_terms(text: str) -> List[str]:
    if not text:
        return []
    text_l = text.lower()
    out: List[str] = []
    for term in INCIDENT_TERMS:
        if term in text_l and term not in out:
            out.append(term)
    return out


def _extract_links(text: str) -> Dict[str, List[str]]:
    """Return links grouped by destination: youtube / muckrock /
    official (.gov / .us) / news (curated host list)."""
    out = {"youtube": [], "muckrock": [], "official": [], "news": []}
    if not text:
        return out
    for url in _YT_URL_RE.findall(text):
        if url not in out["youtube"]:
            out["youtube"].append(url)
    for url in _MUCKROCK_URL_RE.findall(text):
        if url not in out["muckrock"]:
            out["muckrock"].append(url)
    for url in _GOV_URL_RE.findall(text):
        if url not in out["official"]:
            out["official"].append(url)
    for url in _NEWS_HOST_RE.findall(text):
        if url not in out["news"]:
            out["news"].append(url)
    return out


# ---- scoring + grading ----------------------------------------------


def score_lead(
    *,
    has_subject: bool,
    has_agency: bool,
    has_city_or_state: bool,
    has_artifact: bool,
    has_strong_incident: bool,
    has_date: bool,
) -> int:
    """Compute the lead's packet_priority_score.

    Rubric (mirrors muckrock_leads' / sfchronicle_pursuits' shape so
    the shared :func:`assign_grade` thresholds map cleanly):

      +5 subject + agency + (city OR state) + incident artifact
         (both routes to grade A)
      +3 subject + agency
      +3 agency + city + artifact
      +2 strong incident term (OIS / CIB / in-custody)
      +1 each: subject, agency, city/state, artifact, date
    """
    score = 0
    if has_subject and has_agency and has_city_or_state and has_artifact:
        score += 5
    elif has_subject and has_agency:
        score += 3
    elif has_agency and has_city_or_state and has_artifact:
        score += 3

    if has_strong_incident:
        score += 2

    score += sum([
        1 if has_subject else 0,
        1 if has_agency else 0,
        1 if has_city_or_state else 0,
        1 if has_artifact else 0,
        1 if has_date else 0,
    ])
    return score


# ---- search-task generation -----------------------------------------


def _youtube_queries(
    *, subject: Optional[str], agency: Optional[str],
    city: Optional[str], state: Optional[str],
) -> List[str]:
    out: List[str] = []
    if subject and agency:
        out.append(f"{subject} {agency} bodycam")
        out.append(f"{subject} {agency} officer involved")
    if subject and city:
        out.append(f"{subject} {city} police pursuit")
    if agency and city:
        out.append(f"{agency} {city} critical incident briefing")
    return list(dict.fromkeys(q for q in out if q))


def _muckrock_queries(
    *, subject: Optional[str], agency: Optional[str],
    city: Optional[str],
) -> List[str]:
    out: List[str] = []
    if subject and agency:
        out.append(f"{subject} {agency} body-worn camera")
        out.append(f"{subject} {agency} incident report")
    if agency:
        out.append(f"{agency} body-worn camera policy")
    return list(dict.fromkeys(q for q in out if q))


def _outcome_queries(
    *, subject: Optional[str], agency: Optional[str],
) -> List[str]:
    out: List[str] = []
    if subject:
        out.append(f"{subject} charged")
        out.append(f"{subject} settlement lawsuit")
    if agency:
        out.append(f"{agency} investigation outcome")
    return list(dict.fromkeys(q for q in out if q))


# ---- candidate construction -----------------------------------------


def _slugify(s: str, *, max_len: int = 40) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")
    return s[:max_len] or "lead"


def _excerpt(body: str, max_len: int = 240) -> str:
    text = (body or "").strip()
    if len(text) > max_len:
        text = text[:max_len - 3] + "..."
    return text


def _build_candidate_for_segment(
    video: CompilationVideo,
    seg: _Segment,
) -> DatasetCandidate:
    haystack = "\n".join((seg.text or "", seg.body or ""))

    subject = _extract_subject_name(haystack, uploader=video.uploader)
    agency = _extract_agency(haystack)
    city, state = _extract_city_state(haystack)
    incident_date = _extract_date(haystack)
    artifact_terms = _extract_artifact_terms(haystack)
    links = _extract_links(haystack)

    has_subject = bool(subject)
    has_agency = bool(agency)
    has_city_or_state = bool(city or state)
    has_artifact = bool(artifact_terms)
    has_strong_incident = any(
        t in [a.lower() for a in artifact_terms]
        for t in STRONG_INCIDENT_TERMS
    )
    has_date = bool(incident_date)

    score = score_lead(
        has_subject=has_subject,
        has_agency=has_agency,
        has_city_or_state=has_city_or_state,
        has_artifact=has_artifact,
        has_strong_incident=has_strong_incident,
        has_date=has_date,
    )
    grade = assign_grade(score)
    strength = evidence_strength_from_grade(grade)

    # Stable candidate_id: <video_id>__<chapter_start_or_none>__<slug>
    seg_part = (
        f"t{seg.start_time}"
        if seg.start_time is not None
        else "video"
    )
    slug_seed = subject or agency or city or seg.text or "lead"
    candidate_id = (
        f"compilation_leads:{video.video_id}__{seg_part}__"
        f"{_slugify(slug_seed)}"
    )

    notes: List[str] = []
    if seg.start_time is not None:
        notes.append(f"chapter_start={seg.start_time}")
    if seg.text:
        notes.append(f"segment_title={seg.text}")
    if video.uploader:
        notes.append(f"source_uploader={video.uploader}")
    if artifact_terms:
        notes.append(f"artifact_terms={','.join(artifact_terms)}")
    notes.append(f"description_excerpt={_excerpt(seg.body)!r}")

    bodycam_likelihood = "high" if any(
        t in artifact_terms for t in ("bodycam", "body cam",
                                     "body camera", "body-worn camera",
                                     "body worn camera", "bwc")
    ) else "unknown"
    footage_likelihood = "high" if has_artifact else "unknown"

    candidate = DatasetCandidate(
        candidate_id=candidate_id,
        source_lane=SOURCE_LANE,
        source_dataset=SOURCE_DATASET,
        source_row_id=video.video_id,
        source_url=video.url,
        case_title=seg.text or video.title or None,
        agency_name=agency,
        jurisdiction_city=city,
        jurisdiction_state=state,
        incident_date=incident_date,
        subject_name=subject,
        artifact_types_detected=list(artifact_terms),
        news_urls=list(links["news"]),
        official_urls=list(links["official"]),
        muckrock_urls=list(links["muckrock"]),
        youtube_queries=_youtube_queries(
            subject=subject, agency=agency, city=city, state=state,
        ),
        muckrock_queries=_muckrock_queries(
            subject=subject, agency=agency, city=city,
        ),
        outcome_queries=_outcome_queries(subject=subject, agency=agency),
        bodycam_likelihood=bodycam_likelihood,
        footage_likelihood=footage_likelihood,
        outcome_validation_needed=True,
        evidence_strength=strength,
        packet_priority_score=score,
        grade=grade,
        notes=notes,
    )

    candidate.next_actions_hint = _next_actions(candidate, links)
    return candidate


def _next_actions(
    candidate: DatasetCandidate,
    links: Dict[str, List[str]],
) -> List[str]:
    out: List[str] = []
    if candidate.muckrock_urls:
        out.append(NextActionHint.MUCKROCK_PARSE_RELEASED_FILES)
    if candidate.official_urls:
        out.append(NextActionHint.PORTAL_LIVE_VALIDATE)
    if links.get("youtube") or candidate.youtube_queries:
        out.append(NextActionHint.YOUTUBE_METADATA_TRANSCRIPT)
    if (
        not candidate.muckrock_urls
        and not candidate.official_urls
        and candidate.subject_name
        and candidate.agency_name
    ):
        out.append(NextActionHint.ARTIFACT_SEARCH)
    if candidate.outcome_validation_needed:
        out.append(NextActionHint.OUTCOME_VALIDATE)
    return out


# ---- dedup -----------------------------------------------------------


def _dedupe_key(c: DatasetCandidate) -> str:
    """Normalised key for deduplicating leads:

      - Prefer ``(subject_last_name, agency_or_city_or_state)`` when
        a subject is present.
      - Otherwise fall back to ``(source_video_id, segment_start)``
        so the same chapter from the same video doesn't generate
        multiple candidates.
    """
    if c.subject_name:
        last = (c.subject_name or "").lower().strip().split()[-1]
        anchor = (
            (c.agency_name or "").lower().strip()
            or (c.jurisdiction_city or "").lower().strip()
            or (c.jurisdiction_state or "").lower().strip()
        )
        return f"subj:{last}|anchor:{anchor}"
    seg = ""
    for n in c.notes or []:
        if n.startswith("chapter_start="):
            seg = n.split("=", 1)[1]
            break
    return f"vid:{c.source_row_id}|seg:{seg}"


# ---- top-level entry points -----------------------------------------


def parse_compilation_records(
    records: Iterable[Dict[str, Any]],
) -> List[DatasetCandidate]:
    """Top-level: take a stream of compilation-video metadata records
    and return a deduplicated list of :class:`DatasetCandidate` rows."""
    candidates: List[DatasetCandidate] = []
    for raw in records:
        if not isinstance(raw, dict):
            continue
        video = parse_compilation_video(raw)
        if not video.video_id and not video.url:
            continue
        for seg in _segments_for(video):
            candidates.append(_build_candidate_for_segment(video, seg))

    # Dedupe: keep the highest-scoring candidate per dedupe key.
    by_key: Dict[str, DatasetCandidate] = {}
    for c in candidates:
        key = _dedupe_key(c)
        existing = by_key.get(key)
        if existing is None or c.packet_priority_score > existing.packet_priority_score:
            by_key[key] = c
    return list(by_key.values())


def parse_compilation_jsonl_text(text: str) -> List[DatasetCandidate]:
    """Parse a JSONL document (one JSON object per line) of
    compilation-video metadata records."""
    records: List[Dict[str, Any]] = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            records.append(obj)
    return parse_compilation_records(records)


def parse_compilation_json_text(text: str) -> List[DatasetCandidate]:
    """Parse a JSON document containing either a single record dict
    or a list of records."""
    if not text or not text.strip():
        return []
    obj = json.loads(text)
    if isinstance(obj, dict):
        return parse_compilation_records([obj])
    if isinstance(obj, list):
        return parse_compilation_records(
            [r for r in obj if isinstance(r, dict)]
        )
    raise ValueError(
        "compilation JSON input must be a dict or a list of dicts"
    )


def parse_compilation_input_file(path: Path) -> List[DatasetCandidate]:
    """Auto-detect JSONL vs JSON by file extension. ``.jsonl`` →
    line-by-line; everything else → whole-document JSON."""
    text = Path(path).read_text(encoding="utf-8")
    if str(path).lower().endswith(".jsonl"):
        return parse_compilation_jsonl_text(text)
    return parse_compilation_json_text(text)


__all__ = [
    "COMPILATION_CHANNEL_UPLOADERS",
    "HEADLINE_ACTION_VERBS",
    "HEADLINE_FILLER_TOKENS",
    "HEADLINE_ROLE_NOUNS",
    "INCIDENT_TERMS",
    "NAME_STOPWORDS",
    "SOURCE_DATASET",
    "SOURCE_LANE",
    "STRONG_INCIDENT_TERMS",
    "CompilationVideo",
    "parse_compilation_input_file",
    "parse_compilation_json_text",
    "parse_compilation_jsonl_text",
    "parse_compilation_records",
    "parse_compilation_video",
    "score_lead",
]
