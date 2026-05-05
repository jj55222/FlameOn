"""YouTube caption / transcript extraction for high-confidence enrichment results.

Read-only over an existing ``EnrichmentResult`` JSON (typically the
output of a YouTube enrichment smoke). For each surviving high-
confidence URL with a ``youtube_metadata`` next-action hint, this
module fetches **captions only** via yt-dlp (no video, no audio,
no Whisper), parses the VTT to plain text, and scores transcript
relevance against the source enrichment task's identity context.

Why this exists: the YouTube relevance gate (PRs #34 / #35) decides
whether a URL is worth a closer look based on title + uploader +
description metadata only. Transcripts are how we *ground* that
decision — does the actual spoken content of the briefing /
news segment mention the case-specific subject and circumstances?
A high-confidence URL whose transcript names the right subject is
strong signal; a high-confidence URL whose transcript is generic
police-policy boilerplate is the pipeline learning where the
gate's title-based ceiling is.

Out of scope (and explicitly NOT done here):

- No video / audio downloads.
- No Whisper / speech-to-text.
- No new YouTube searches (operates only on URLs in the input JSON).
- No portal-live invocations.
- No MuckRock changes.
- No Brave / Exa / Tavily.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


# ---- selection thresholds + constants -------------------------------


CONFIDENCE_RANK: Dict[str, int] = {
    "unknown": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}
DEFAULT_MIN_CONFIDENCE = "high"
DEFAULT_MAX_VIDEOS = 10
DEFAULT_SOCKET_TIMEOUT = 15
DEFAULT_RATE_LIMIT_SECONDS = 1.0

# Languages we want from yt-dlp; passed verbatim as ``subtitleslangs``.
ENGLISH_LANGS: Tuple[str, ...] = ("en", "en-US", "en-GB", "en.*")

# Domain artifact terms used in transcript scoring. Matching one of
# these in the transcript signals the recording is about a real
# incident, not e.g. a press conference about budgets.
ARTIFACT_TERMS: Tuple[str, ...] = (
    "bodycam", "body cam", "body camera", "body-worn camera",
    "body worn camera", "bwc",
    "critical incident",
    "officer-involved shooting", "officer involved shooting",
    "deputy-involved shooting", "deputy involved shooting",
    "in-custody death", "in custody death",
    "police shooting",
    "pursuit", "police chase", "high-speed chase",
    "dashcam", "dash cam",
    "crash", "fatal crash",
    "911 audio", "911 call",
)

# Generic English / police-procedural words we strip from the source
# query before treating remaining tokens as case-distinctive anchors.
# Keeps the transcript scorer from "matching" every video that says
# "police" or "officer".
QUERY_STOPWORDS = frozenset({
    "police", "department", "departments", "officer", "officers", "official",
    "the", "of", "and", "a", "an", "to", "in", "on", "at", "for", "by",
    "bodycam", "body", "camera", "worn", "cam",
    "critical", "incident", "briefing", "involved", "shooting", "pursuit",
    "report", "policy", "policies",
    "services", "service", "office", "offices", "county", "sheriff", "sheriffs",
    "city", "state", "patrol", "force",
    "highway", "marshals", "marshal", "deputies", "deputy",
})


# ---- input selection ------------------------------------------------


@dataclass
class SelectedVideo:
    """One URL the operator wants captions for."""
    url: str
    title: str
    candidate_id: str
    source_query: str
    source_confidence: str


def select_urls_for_extraction(
    results: Iterable[Dict[str, Any]],
    *,
    min_confidence: str = DEFAULT_MIN_CONFIDENCE,
    max_videos: int = DEFAULT_MAX_VIDEOS,
) -> List[SelectedVideo]:
    """Pick URLs from an enrichment result list that match the
    transcript-extraction criteria:

    - ``provider == "youtube"``
    - ``confidence`` >= ``min_confidence``
    - ``next_actions_hint`` contains ``"youtube_metadata"``
    - at least one URL in ``result_urls``

    The result list preserves the input row order so an operator can
    predict which URL gets captured in what order. Capped at
    ``max_videos`` total URLs (not per-row).
    """
    threshold = CONFIDENCE_RANK.get(min_confidence, CONFIDENCE_RANK["high"])
    cap = max(1, int(max_videos))

    selected: List[SelectedVideo] = []
    for row in results:
        if not isinstance(row, dict):
            continue
        if row.get("provider") != "youtube":
            continue
        conf = row.get("confidence", "unknown")
        if CONFIDENCE_RANK.get(conf, 0) < threshold:
            continue
        hints = row.get("next_actions_hint") or []
        if "youtube_metadata" not in hints:
            continue
        urls = row.get("result_urls") or []
        titles = row.get("result_titles") or []
        for i, url in enumerate(urls):
            if not url:
                continue
            title = titles[i] if i < len(titles) else ""
            selected.append(SelectedVideo(
                url=str(url),
                title=str(title),
                candidate_id=str(row.get("candidate_id", "")),
                source_query=str(row.get("query", "")),
                source_confidence=str(conf),
            ))
            if len(selected) >= cap:
                return selected
    return selected


# ---- VTT -> plaintext ------------------------------------------------


_VTT_HEADER_RE = re.compile(r"^WEBVTT\b.*", re.IGNORECASE)
_VTT_TIMESTAMP_RE = re.compile(
    r"^\d{1,2}:\d{2}(?::\d{2})?\.\d{3}\s+-->\s+\d{1,2}:\d{2}(?::\d{2})?\.\d{3}.*"
)
_VTT_CUE_NUM_RE = re.compile(r"^\d+$")
_VTT_NOTE_RE = re.compile(r"^(NOTE|STYLE|REGION)\b", re.IGNORECASE)
# Inline VTT styling tags: <c.colorE5E5E5>, <00:00:01.000>, etc.
_VTT_INLINE_TAG_RE = re.compile(r"<[^>]*>")


def vtt_to_plaintext(vtt_text: str) -> str:
    """Convert a VTT subtitle file to plain text.

    Strips WEBVTT header, cue numbers, timestamp lines, NOTE / STYLE
    / REGION blocks, and inline styling tags. Collapses consecutive
    duplicate lines (YouTube auto-captions often emit "rolling"
    cues that repeat each line a few times). Collapses runs of
    blank lines into a single paragraph break.
    """
    if not vtt_text:
        return ""
    raw_lines: List[str] = []
    for raw in vtt_text.splitlines():
        line = raw.strip()
        if not line:
            raw_lines.append("")
            continue
        if _VTT_HEADER_RE.match(line):
            continue
        if _VTT_TIMESTAMP_RE.match(line):
            continue
        if _VTT_CUE_NUM_RE.match(line):
            continue
        if _VTT_NOTE_RE.match(line):
            continue
        line = _VTT_INLINE_TAG_RE.sub("", line).strip()
        if line:
            raw_lines.append(line)

    # Dedupe consecutive identical lines (common in YouTube auto-VTT).
    deduped: List[str] = []
    last: Optional[str] = None
    for line in raw_lines:
        if line and line == last:
            continue
        deduped.append(line)
        last = line if line else last

    # Collapse runs of blank lines into a single one.
    collapsed: List[str] = []
    prev_blank = False
    for line in deduped:
        if not line:
            if not prev_blank and collapsed:
                collapsed.append("")
            prev_blank = True
            continue
        collapsed.append(line)
        prev_blank = False

    return "\n".join(collapsed).strip()


# ---- relevance scoring ----------------------------------------------


def _query_distinctive_tokens(query: str) -> List[str]:
    """Strip generic words from a query and return remaining tokens
    of length >= 4. These are the case-distinctive anchors a
    transcript ought to mention if it's actually about the case
    (subject names, jurisdictions, distinctive agency words)."""
    if not query:
        return []
    tokens = re.findall(r"[a-z]+", query.lower())
    return [
        t for t in tokens
        if t not in QUERY_STOPWORDS and len(t) >= 4
    ]


def _word_re(token: str) -> re.Pattern:
    return re.compile(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])")


@dataclass
class _Scored:
    relevance: str  # high | medium | low | unknown
    matched_terms: List[str] = field(default_factory=list)
    distinctive_query_hits: List[str] = field(default_factory=list)
    artifact_hits: List[str] = field(default_factory=list)


def score_transcript(
    transcript: str,
    *,
    source_query: str,
    context: Optional[Dict[str, Any]] = None,
) -> _Scored:
    """Score a transcript against the source task's identity signals.

    ``source_query`` is the original enrichment task query (e.g.
    ``"ryan keyon williams oklahoma city police department bodycam"``);
    distinctive tokens are extracted from it. ``context`` is an
    optional ``search_tasks.json``-shaped dict ({agency, subject_name,
    city, state}) — when supplied it takes precedence and produces
    cleaner anchor matching than query-token tokenisation.

    Relevance:

      ``high``   - subject full name OR last name AND any artifact
                   term, OR (agency token + city + artifact term).
      ``medium`` - any distinctive query token + any artifact term;
                   OR an agency / city anchor on its own.
      ``low``    - artifact term but no anchor; OR captions exist
                   but neither anchor nor artifact term appears.
      ``unknown`` - empty transcript.
    """
    if not transcript or not transcript.strip():
        return _Scored(relevance="unknown")

    text = transcript.lower()

    # Build anchor candidates from context (preferred) + query (fallback).
    distinctive_anchors: List[str] = []
    subject_full = ""
    subject_last = ""
    agency_tokens: List[str] = []
    city_token = ""
    state_full = ""
    state_abbr = ""

    if context:
        subject_full = (context.get("subject_name") or "").lower().strip()
        if subject_full:
            parts = re.findall(r"[a-z']+", subject_full)
            suffixes = {"jr", "sr", "ii", "iii", "iv", "v"}
            parts = [p for p in parts if p not in suffixes]
            if parts:
                subject_last = parts[-1]
        agency_raw = (context.get("agency") or "").lower()
        agency_tokens = [
            t for t in re.findall(r"[a-z][a-z']+", agency_raw)
            if t not in QUERY_STOPWORDS and len(t) >= 4
        ]
        city_token = (context.get("city") or "").lower().strip()
        state_abbr = (context.get("state") or "").upper().strip()

    # Always also pull distinctive tokens from the query as a
    # superset — covers cases where context is missing or partial.
    distinctive_anchors = _query_distinctive_tokens(source_query)

    matched: List[str] = []
    distinctive_hits: List[str] = []
    artifact_hits: List[str] = []

    has_subject = False
    if subject_full and len(subject_full) >= 4 and subject_full in text:
        matched.append("full_subject_name")
        has_subject = True
    elif subject_last and len(subject_last) >= 3 and _word_re(subject_last).search(text):
        matched.append("last_name")
        has_subject = True

    has_agency = False
    for tok in agency_tokens:
        if _word_re(tok).search(text):
            matched.append(f"agency_token:{tok}")
            has_agency = True
            break

    has_city = bool(city_token and _word_re(city_token).search(text))
    if has_city:
        matched.append("city")

    has_state = False
    if state_abbr and len(state_abbr) == 2 and re.search(
        rf"(?<![A-Za-z0-9]){re.escape(state_abbr)}(?![A-Za-z0-9])",
        transcript,
    ):
        matched.append("state_abbr")
        has_state = True

    # Distinctive query-token superset (covers tokens the context
    # doesn't directly expose, e.g. unusual middle names).
    for tok in distinctive_anchors:
        if _word_re(tok).search(text):
            distinctive_hits.append(tok)

    # Artifact terms.
    for term in ARTIFACT_TERMS:
        if term in text:
            artifact_hits.append(term)
            matched.append(f"artifact:{term}")

    has_anchor = has_subject or has_agency or has_city or bool(distinctive_hits)
    has_artifact = bool(artifact_hits)

    # Decide relevance.
    if has_subject and has_artifact:
        relevance = "high"
    elif has_agency and has_city and has_artifact:
        relevance = "high"
    elif has_anchor and has_artifact:
        relevance = "medium"
    elif has_anchor or has_artifact:
        relevance = "medium" if has_anchor else "low"
    else:
        relevance = "low"

    return _Scored(
        relevance=relevance,
        matched_terms=matched,
        distinctive_query_hits=distinctive_hits,
        artifact_hits=artifact_hits,
    )


# ---- yt-dlp wrapper -------------------------------------------------


class _DefaultYdlClient:
    """Thin wrapper around ``yt_dlp.YoutubeDL`` that hides the import.

    Lazy: the actual yt-dlp import happens only when the operator
    runs ``--run`` mode. Tests inject a fake class entirely.
    """

    def __init__(self) -> None:
        import yt_dlp  # type: ignore
        self._YoutubeDL = yt_dlp.YoutubeDL

    def __call__(self, opts: Dict[str, Any]):
        return self._YoutubeDL(opts)


@dataclass
class CaptionExtraction:
    """Result of one yt-dlp caption fetch."""
    status: str  # downloaded | no_captions_found | failed | dry_run
    video_id: str
    vtt_path: Optional[str] = None
    transcript_path: Optional[str] = None
    metadata_path: Optional[str] = None
    error: Optional[str] = None
    notes: List[str] = field(default_factory=list)


def _video_id_from_url(url: str) -> str:
    """Best-effort YouTube video-id extraction. Matches both the
    ``watch?v=ID`` and ``youtu.be/ID`` patterns. Falls back to a
    sanitised slug if neither shape matches."""
    m = re.search(r"[?&]v=([A-Za-z0-9_\-]{6,})", url)
    if m:
        return m.group(1)
    m = re.search(r"youtu\.be/([A-Za-z0-9_\-]{6,})", url)
    if m:
        return m.group(1)
    return re.sub(r"[^A-Za-z0-9_\-]+", "_", url)[:40]


class YouTubeCaptionExtractor:
    """yt-dlp backed caption-only extractor.

    All knobs explicit to keep the behaviour auditable from outside:

    - ``skip_download=True``  — never fetch the video stream.
    - ``writesubtitles=True`` — manually-uploaded captions.
    - ``writeautomaticsub=True`` — auto-generated captions fallback.
    - ``subtitleslangs=ENGLISH_LANGS``
    - ``subtitlesformat="vtt"``
    - ``outtmpl=<output_dir>/<video_id>.%(ext)s``
    - ``noplaylist=True``, ``quiet=True``, ``no_warnings=True``,
      ``socket_timeout=...``

    No video / audio / thumbnail / description-file flags are set.
    Tests inject a fake ``ydl_factory`` (callable that mimics the
    ``yt_dlp.YoutubeDL`` constructor) to capture options + simulate
    on-disk VTT output.
    """

    def __init__(
        self,
        *,
        socket_timeout: int = DEFAULT_SOCKET_TIMEOUT,
        rate_limit_seconds: float = DEFAULT_RATE_LIMIT_SECONDS,
        ydl_factory: Optional[Any] = None,
        sleeper: Optional[Any] = None,
    ) -> None:
        self._socket_timeout = int(socket_timeout)
        self._rate_limit_seconds = float(rate_limit_seconds)
        self._ydl_factory = ydl_factory
        self._sleeper = sleeper or time.sleep

    def _factory(self) -> Any:
        if self._ydl_factory is not None:
            return self._ydl_factory
        self._ydl_factory = _DefaultYdlClient()
        return self._ydl_factory

    def _build_opts(self, video_dir: Path) -> Dict[str, Any]:
        return {
            "skip_download": True,
            "writesubtitles": True,
            "writeautomaticsub": True,
            "subtitleslangs": list(ENGLISH_LANGS),
            "subtitlesformat": "vtt",
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "socket_timeout": self._socket_timeout,
            "outtmpl": str(video_dir / "%(id)s.%(ext)s"),
        }

    def extract(self, url: str, *, output_dir: Path) -> CaptionExtraction:
        video_id = _video_id_from_url(url)
        video_dir = Path(output_dir) / video_id
        video_dir.mkdir(parents=True, exist_ok=True)

        if self._rate_limit_seconds > 0:
            try:
                self._sleeper(self._rate_limit_seconds)
            except Exception:
                pass

        opts = self._build_opts(video_dir)
        factory = self._factory()
        try:
            with factory(opts) as ydl:
                ydl.download([url])
        except Exception as exc:
            return CaptionExtraction(
                status="failed",
                video_id=video_id,
                error=f"{type(exc).__name__}: {exc}",
            )

        # Find the resulting VTT file. Prefer manual ("en" without
        # the auto-generation marker yt-dlp uses) over auto.
        candidate_paths = sorted(video_dir.glob(f"{video_id}*.vtt"))
        if not candidate_paths:
            candidate_paths = sorted(video_dir.glob("*.vtt"))
        if not candidate_paths:
            return CaptionExtraction(
                status="no_captions_found",
                video_id=video_id,
                notes=[f"no vtt files found in {video_dir}"],
            )

        # yt-dlp filenames look like ``<id>.<lang>.vtt``. Manually-
        # uploaded captions use plain ``en``; auto-generated uses
        # ``en`` too but typically with .vtt extension only — the
        # safest priority is shortest filename (manual) wins ties.
        chosen = sorted(
            candidate_paths,
            key=lambda p: (len(p.name), p.name),
        )[0]
        return CaptionExtraction(
            status="downloaded",
            video_id=video_id,
            vtt_path=str(chosen),
            notes=[f"selected_vtt={chosen.name}"],
        )


# ---- top-level operator helper --------------------------------------


@dataclass
class TranscriptResult:
    """One row of the transcript extractor's summary output."""
    video_url: str
    video_id: str
    candidate_id: str
    source_query: str
    source_confidence: str
    caption_status: str  # downloaded | no_captions_found | failed | dry_run
    caption_path: Optional[str] = None
    transcript_path: Optional[str] = None
    metadata_path: Optional[str] = None
    transcript_relevance: str = "unknown"
    matched_terms: List[str] = field(default_factory=list)
    error: Optional[str] = None
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "video_url": self.video_url,
            "video_id": self.video_id,
            "candidate_id": self.candidate_id,
            "source_query": self.source_query,
            "source_confidence": self.source_confidence,
            "caption_status": self.caption_status,
            "caption_path": self.caption_path,
            "transcript_path": self.transcript_path,
            "metadata_path": self.metadata_path,
            "transcript_relevance": self.transcript_relevance,
            "matched_terms": list(self.matched_terms),
            "error": self.error,
            "notes": list(self.notes),
        }


def write_artifacts_for_video(
    *,
    output_dir: Path,
    video_id: str,
    extraction: CaptionExtraction,
    transcript_text: str,
    metadata: Dict[str, Any],
) -> Tuple[Optional[str], Optional[str]]:
    """Write the parsed transcript text + per-video metadata JSON.
    Returns ``(transcript_path, metadata_path)``."""
    video_dir = Path(output_dir) / video_id
    video_dir.mkdir(parents=True, exist_ok=True)

    transcript_path: Optional[str] = None
    if transcript_text:
        tp = video_dir / f"{video_id}.txt"
        tp.write_text(transcript_text, encoding="utf-8")
        transcript_path = str(tp)

    mp = video_dir / "metadata.json"
    mp.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    metadata_path = str(mp)

    return transcript_path, metadata_path


def load_context_lookup(
    tasks_file: Optional[Path],
) -> Dict[str, Dict[str, Any]]:
    """Build a ``candidate_id -> context`` map from a search-tasks
    JSON file. Returns an empty dict when ``tasks_file`` is None or
    unreadable — the transcript scorer will fall back to query-token
    matching."""
    if tasks_file is None:
        return {}
    try:
        data = json.loads(Path(tasks_file).read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for t in data.get("tasks") or []:
        cid = t.get("candidate_id")
        ctx = t.get("context") or {}
        if cid and cid not in out:
            out[cid] = ctx
    return out


__all__ = [
    "ARTIFACT_TERMS",
    "CONFIDENCE_RANK",
    "CaptionExtraction",
    "DEFAULT_MAX_VIDEOS",
    "DEFAULT_MIN_CONFIDENCE",
    "ENGLISH_LANGS",
    "QUERY_STOPWORDS",
    "SelectedVideo",
    "TranscriptResult",
    "YouTubeCaptionExtractor",
    "load_context_lookup",
    "score_transcript",
    "select_urls_for_extraction",
    "vtt_to_plaintext",
    "write_artifacts_for_video",
]
