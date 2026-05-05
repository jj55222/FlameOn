"""yt-dlp backed YouTube enrichment provider with deterministic
relevance filtering.

Background: a raw ``ytsearchN:`` call surfaces topical-but-unrelated
police / bodycam / officer videos when the case in question has no
YouTube coverage. The first live smoke (PR #34) returned 11 results
across 3 tasks for *Joe William Gold / Longmont Police Services*,
**none** of which were about that case — but the provider still
labelled every batch ``confidence="medium"``.

This module wraps the same yt-dlp metadata search and adds a
**zero-network, deterministic** relevance gate: each candidate is
scored against the originating task's identity context (subject
name / agency / city / state) plus supporting domain signals
(bodycam / CIB / pursuit). Results without a real anchor are dropped
and ``confidence`` is keyed to the surviving anchors, not to whether
yt-dlp returned any rows at all.

Out of scope (and explicitly NOT done here):

- No video / audio / subtitle / caption downloads.
- No transcript extraction.
- No YouTube Data API.
- No Brave / Exa / Tavily / MuckRock calls.
- No portal-live invocations.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .models import EnrichmentResult, EnrichmentTask, TaskStatus


# ---- relevance helpers ----------------------------------------------


# US state name <-> abbreviation lookup, used as one of the four
# anchor types ("state name in title/uploader/description").
US_STATE_NAMES: Dict[str, str] = {
    "AL": "alabama", "AK": "alaska", "AZ": "arizona", "AR": "arkansas",
    "CA": "california", "CO": "colorado", "CT": "connecticut",
    "DE": "delaware", "FL": "florida", "GA": "georgia", "HI": "hawaii",
    "ID": "idaho", "IL": "illinois", "IN": "indiana", "IA": "iowa",
    "KS": "kansas", "KY": "kentucky", "LA": "louisiana", "ME": "maine",
    "MD": "maryland", "MA": "massachusetts", "MI": "michigan",
    "MN": "minnesota", "MS": "mississippi", "MO": "missouri",
    "MT": "montana", "NE": "nebraska", "NV": "nevada",
    "NH": "new hampshire", "NJ": "new jersey", "NM": "new mexico",
    "NY": "new york", "NC": "north carolina", "ND": "north dakota",
    "OH": "ohio", "OK": "oklahoma", "OR": "oregon", "PA": "pennsylvania",
    "RI": "rhode island", "SC": "south carolina", "SD": "south dakota",
    "TN": "tennessee", "TX": "texas", "UT": "utah", "VT": "vermont",
    "VA": "virginia", "WA": "washington", "WV": "west virginia",
    "WI": "wisconsin", "WY": "wyoming", "DC": "district of columbia",
}

# Reverse name -> abbr, used for location-conflict detection.
US_STATE_ABBR: Dict[str, str] = {v: k for k, v in US_STATE_NAMES.items()}


# Tokens that appear in nearly every agency name and are useless as
# distinctive anchors ("X Police Department" -> we want only "X").
AGENCY_STOPWORDS = frozenset({
    "police", "department", "departments", "sheriff", "sheriffs", "office",
    "offices", "services", "service", "the", "of", "and", "county",
    "city", "state", "patrol", "force", "metropolitan", "metro",
    "highway", "marshals", "marshal", "constable", "deputies", "deputy",
    "law", "enforcement", "bureau", "agency",
})

# Generational / honorific suffixes to skip when extracting last name.
NAME_SUFFIXES = frozenset({"jr", "sr", "ii", "iii", "iv", "v"})


# Supporting domain signal sets — these are NOT anchors on their own
# but boost score and confidence when an anchor is present.
BODYCAM_TERMS: Tuple[str, ...] = (
    "bodycam", "body cam", "body camera", "body-worn camera",
    "body worn camera", "bwc",
)
CIB_TERMS: Tuple[str, ...] = (
    "critical incident briefing",
    "critical incident",
)
PURSUIT_TERMS: Tuple[str, ...] = (
    "dashcam", "dash cam", "dash-cam", "pursuit", "high-speed chase",
    "police chase",
)
OFFICIAL_CHANNEL_HINTS: Tuple[str, ...] = (
    "official", "city of", "police department", "sheriff's office",
    "sheriffs office",
)


def _norm(s: Optional[str]) -> str:
    return (s or "").lower()


def _word_re(token: str) -> re.Pattern:
    return re.compile(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])")


def _last_name(subject_name: str) -> str:
    parts = re.findall(r"[a-z']+", _norm(subject_name))
    parts = [p for p in parts if p not in NAME_SUFFIXES]
    return parts[-1] if parts else ""


def _agency_distinctive_tokens(agency: str) -> List[str]:
    """Strip agency boilerplate ("police", "department", ...) and
    return only locally distinctive tokens. ``"longmont police
    services"`` -> ``["longmont"]``; ``"zavala county sheriff's
    office, crystal city police department"`` ->
    ``["zavala", "crystal"]``."""
    raw = re.findall(r"[a-z][a-z']+", _norm(agency))
    return [t for t in raw if t not in AGENCY_STOPWORDS and len(t) > 2]


@dataclass
class _Scored:
    url: str
    title: str
    uploader: str
    description: str
    score: int = 0
    matched: List[str] = field(default_factory=list)
    has_anchor: bool = False
    has_subject: bool = False
    has_agency: bool = False
    has_city: bool = False
    has_state: bool = False
    has_supporting: bool = False
    has_official_channel: bool = False
    drop_reason: Optional[str] = None


def _score_entry(
    *,
    title: str,
    uploader: str,
    description: str,
    context: Dict[str, Any],
) -> _Scored:
    """Score one yt-dlp entry against the task's identity context.

    Returns a :class:`_Scored` carrying the raw text fields, the
    integer score, the list of matched signal labels, and per-axis
    booleans the caller uses to derive confidence + drop decisions.
    """
    title_l = _norm(title)
    upl_l = _norm(uploader)
    desc_l = _norm(description)
    haystack_l = " ".join((title_l, upl_l, desc_l))

    raw = " ".join((title or "", uploader or "", description or ""))

    subject = _norm(context.get("subject_name", ""))
    last = _last_name(subject)
    agency = _norm(context.get("agency", ""))
    city = _norm(context.get("city", ""))
    distinctive = _agency_distinctive_tokens(agency)
    state_abbr = (str(context.get("state") or "")).upper().strip()
    state_full = US_STATE_NAMES.get(state_abbr, "")

    s = _Scored(url="", title=title or "", uploader=uploader or "", description=description or "")

    # ---- subject anchors ---------------------------------------------
    if subject and len(subject) >= 4 and subject in haystack_l:
        s.matched.append("full_subject_name")
        s.score += 3
        s.has_subject = True
    elif last and len(last) >= 3 and _word_re(last).search(haystack_l):
        s.matched.append("last_name")
        s.score += 2
        s.has_subject = True

    # ---- agency-distinctive token anchor -----------------------------
    for tok in distinctive:
        if _word_re(tok).search(haystack_l):
            s.matched.append(f"agency_token:{tok}")
            s.score += 2
            s.has_agency = True
            break  # one distinctive token is enough

    # ---- city anchor -------------------------------------------------
    if city and _word_re(city).search(haystack_l):
        s.matched.append("city")
        s.score += 1
        s.has_city = True

    # ---- state anchor ------------------------------------------------
    # Full state name in normalised haystack (case-insensitive).
    if state_full and _word_re(state_full).search(haystack_l):
        s.matched.append("state_name")
        s.score += 1
        s.has_state = True
    # Abbreviation: case-sensitive on the raw text so "CO" doesn't
    # collide with the substring "co" in "company" / "cops" / etc.
    if state_abbr and len(state_abbr) == 2 and re.search(
        rf"(?<![A-Za-z0-9]){re.escape(state_abbr)}(?![A-Za-z0-9])", raw
    ):
        if "state_name" not in s.matched:
            s.matched.append("state_abbr")
            s.score += 1
        s.has_state = True

    s.has_anchor = (
        s.has_subject or s.has_agency or s.has_city or s.has_state
    )

    # ---- supporting domain signals -----------------------------------
    if any(t in haystack_l for t in BODYCAM_TERMS):
        s.matched.append("bodycam_term")
        s.score += 1
        s.has_supporting = True
    if any(t in haystack_l for t in CIB_TERMS):
        s.matched.append("cib_term")
        s.score += 1
        s.has_supporting = True
    if any(t in haystack_l for t in PURSUIT_TERMS):
        s.matched.append("pursuit_term")
        s.score += 1
        s.has_supporting = True

    # ---- official channel hint ---------------------------------------
    if (
        s.has_agency
        and any(h in upl_l for h in OFFICIAL_CHANNEL_HINTS)
    ):
        s.matched.append("official_channel_hint")
        s.score += 3
        s.has_official_channel = True

    # ---- drop reason (computed even if kept, useful for diagnostics) -
    if not s.has_anchor:
        s.drop_reason = "no_anchor_match"
    return s


def _confidence_from(kept: Sequence[_Scored]) -> str:
    """Reduce a kept-result list to a single confidence label.

    - ``high``: at least one result with an official-looking agency
      channel, OR with both an agency token and a subject hit.
    - ``medium``: best surviving result has at least one anchor
      *plus* a supporting (bodycam / CIB / pursuit) signal.
    - ``low``: kept only weak (e.g. state-only) anchors, or no
      results survived at all.
    """
    if not kept:
        return "low"
    best = max(kept, key=lambda r: r.score)
    if best.has_official_channel:
        return "high"
    if best.has_agency and best.has_subject:
        return "high"
    if (best.has_subject or best.has_agency or best.has_city) and best.has_supporting:
        return "medium"
    return "low"


def _format_kept_note(s: _Scored) -> str:
    return f"kept score={s.score} anchors={','.join(s.matched) or '-'}"


def _format_dropped_summary(dropped: Sequence[_Scored]) -> List[str]:
    """Return up to three short example notes summarising why
    individual results were dropped — useful when an operator
    inspects the JSON and wants to know what raw YouTube returned."""
    out: List[str] = []
    for s in dropped[:3]:
        snippet = (s.title or "").strip()
        if len(snippet) > 80:
            snippet = snippet[:77] + "..."
        out.append(
            f"dropped reason={s.drop_reason or 'no_anchor_match'} "
            f"title={snippet!r}"
        )
    return out


# ---- provider --------------------------------------------------------


class YtDlpYouTubeSearchClient:
    """yt-dlp backed YouTube search provider with relevance filtering.

    Searches YouTube via yt-dlp's ``ytsearchN:`` pseudo-URL.  No YouTube
    Data API key, no quota consumption.  Metadata-only: runs in
    ``extract_flat`` mode — no media, audio, subtitle, or caption
    downloads, no writes to disk.

    Each yt-dlp candidate is then run through a deterministic
    relevance gate (see :func:`_score_entry`) that anchors on the
    task's subject name / agency tokens / city / state. Candidates
    with no anchor match are dropped with a diagnostic note;
    surviving candidates feed ``result_urls`` / ``result_titles``
    and drive ``confidence``.

    ``ydl_cls`` is the YoutubeDL class to instantiate; default is
    ``yt_dlp.YoutubeDL``.  Inject a fake for tests.
    """

    name = "youtube"

    def __init__(
        self,
        *,
        max_results: int = 5,
        socket_timeout: int = 10,
        ydl_cls: Optional[Any] = None,
    ) -> None:
        self._max_results = max(1, min(max_results, 20))
        self._socket_timeout = socket_timeout
        self._ydl_cls = ydl_cls

    def _load_yt_dlp(self) -> Any:
        try:
            import yt_dlp  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "yt-dlp is required for the youtube provider; pip install yt-dlp"
            ) from exc
        return yt_dlp.YoutubeDL

    def execute(self, task: EnrichmentTask) -> EnrichmentResult:
        ydl_cls = self._ydl_cls or self._load_yt_dlp()
        opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
            "noplaylist": True,
            "socket_timeout": self._socket_timeout,
        }
        search_url = f"ytsearch{self._max_results}:{task.query}"
        try:
            with ydl_cls(opts) as ydl:
                info = ydl.extract_info(search_url, download=False)
        except Exception as exc:
            return EnrichmentResult(
                candidate_id=task.candidate_id,
                task_type=task.task_type,
                query=task.query,
                status=TaskStatus.FAILED,
                provider=self.name,
                error=f"{type(exc).__name__}: {exc}",
            )

        raw_entries: List[Dict[str, Any]] = list(
            (info or {}).get("entries", [])[: self._max_results]
        )

        scored: List[_Scored] = []
        for entry in raw_entries:
            vid_id = str(entry.get("id") or "")
            if not vid_id:
                continue
            raw_url = str(entry.get("url") or "")
            url = (
                raw_url
                if raw_url.startswith("http")
                else f"https://www.youtube.com/watch?v={vid_id}"
            )
            title = str(entry.get("title") or vid_id)
            uploader = str(
                entry.get("uploader")
                or entry.get("channel")
                or entry.get("uploader_id")
                or ""
            )
            description = str(entry.get("description") or "")

            s = _score_entry(
                title=title,
                uploader=uploader,
                description=description,
                context=task.context or {},
            )
            s.url = url
            s.title = title
            s.uploader = uploader
            s.description = description
            scored.append(s)

        kept = [s for s in scored if s.has_anchor]
        dropped = [s for s in scored if not s.has_anchor]
        # Stable ranking: highest score first, then keep yt-dlp order
        # for ties so the output is deterministic given the same input.
        kept.sort(key=lambda r: (-r.score,))

        urls = [s.url for s in kept]
        titles = [s.title for s in kept]
        confidence = _confidence_from(kept)
        next_actions = ["youtube_metadata"] if kept else []

        notes: List[str] = [
            f"raw_result_count={len(scored)}",
            f"filtered_result_count={len(kept)}",
            f"dropped_irrelevant_count={len(dropped)}",
        ]
        if kept:
            for s in kept:
                notes.append(_format_kept_note(s))
        else:
            notes.append(
                "all yt-dlp results filtered as irrelevant — no result "
                "had a subject/agency/city/state anchor match"
            )
        notes.extend(_format_dropped_summary(dropped))

        return EnrichmentResult(
            candidate_id=task.candidate_id,
            task_type=task.task_type,
            query=task.query,
            status=TaskStatus.COMPLETED,
            provider=self.name,
            result_urls=urls,
            result_titles=titles,
            confidence=confidence,
            next_actions_hint=next_actions,
            notes=notes,
        )


__all__ = [
    "AGENCY_STOPWORDS",
    "BODYCAM_TERMS",
    "CIB_TERMS",
    "OFFICIAL_CHANNEL_HINTS",
    "PURSUIT_TERMS",
    "US_STATE_NAMES",
    "YtDlpYouTubeSearchClient",
]
