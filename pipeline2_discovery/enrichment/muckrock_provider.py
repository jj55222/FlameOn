"""MuckRock enrichment provider.

Consumes ``muckrock_query`` enrichment tasks emitted by dataset
intake and returns candidate MuckRock request URLs / titles /
statuses / file signals via the **public MuckRock API v2**.

This is GET-only, metadata-only, and budget-bounded:

- No FOIA submission (no POST/PUT/PATCH/DELETE in this module).
- No file downloads — even when the API surfaces ``files[]`` URLs,
  they are recorded in the result, never fetched.
- No video / audio / caption downloads.
- No broad crawling — the runner caps the task count, this provider
  caps each task to ``max_results`` results from a single GET.
- An optional ``MUCKROCK_API_TOKEN`` env var is honored for live runs
  but is **never required**; the public search endpoint allows
  anonymous access. Tests must run zero-network and never read the
  env var.

Result payload contract:

    EnrichmentResult.status:
        completed | failed | skipped
    EnrichmentResult.confidence:
        high   - incident-anchored AND (released files OR strong
                 BWC / OIS / pursuit / dashcam / 911 / CIB terms)
        medium - incident-anchored with bodycam / pursuit support but
                 no released files / weak metadata
        low    - state-only anchor, no surviving results, or
                 demoted policy-only results
        unknown only when a transport problem occurs before scoring.

    EnrichmentResult.next_actions_hint (de-duped, ordered):
        MUCKROCK_PARSE_RELEASED_FILES — emitted whenever any kept
            result was found (the downstream parser will inspect the
            request page).
        ARTIFACT_SEARCH — emitted when at least one kept result has
            an explicit released-file count > 0 in its API payload.
        YOUTUBE_METADATA_TRANSCRIPT — emitted ONLY when a kept
            result's metadata embeds a YouTube URL — the YouTube
            provider's own relevance gate will then re-score that
            URL. We never emit this hint just because the request
            mentions video.
        OUTCOME_VALIDATE — emitted when the kept result's status
            is not in the "done/completed" set and the original task
            still has an unresolved outcome path.

    EnrichmentResult.notes (diagnostics):
        raw_result_count, returned_result_count, terms_matched,
        released_file_count, policy_only_demoted, api_url
        (token redacted if ever present), per-kept "kept score=N
        anchors=...", and up to 3 "dropped reason=... title=..."
        examples.
"""
from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .models import EnrichmentResult, EnrichmentTask, TaskStatus
from .youtube_provider import (
    AGENCY_STOPWORDS,
    NAME_SUFFIXES,
    US_STATE_NAMES,
    _agency_distinctive_tokens,
    _last_name,
    _word_re,
)


# ---- API client config -----------------------------------------------


MUCKROCK_API_BASE = "https://www.muckrock.com/api_v2/requests/"
DEFAULT_PAGE_SIZE = 20
DEFAULT_MAX_RESULTS = 5
DEFAULT_TIMEOUT = 15
DEFAULT_RATE_LIMIT_SECONDS = 1.0
DEFAULT_MAX_QUERY_ATTEMPTS = 3
MIN_LAST_NAME_LEN_FOR_QUERY = 4

# Possessive / plural variants the shared ``AGENCY_STOPWORDS``
# (which lives in youtube_provider) misses because the apostrophe
# defeats the membership check. Keep this list MuckRock-local so the
# YouTube provider stays unchanged.
EXTRA_AGENCY_STOPWORDS = frozenset({
    "sheriff's", "officer's", "officers", "officials", "official",
    "deputy's", "deputies", "patrol's",
})

# Canonical broad-form topic terms used in title-search queries.
# Order: most-specific first; the picker stops at the first hit.
TOPIC_TERMS_BY_SIGNAL: Tuple[Tuple[Tuple[str, ...], str], ...] = (
    (("officer-involved shooting", "officer involved shooting",
      "deputy-involved", "deputy involved"), "officer-involved shooting"),
    (("critical incident briefing", "critical incident"), "critical incident"),
    (("body-worn camera", "body worn camera", "bodycam", "body cam",
      "body camera", "bwc"), "body-worn camera"),
    (("dashcam", "dash cam", "dash-cam"), "dashcam"),
    (("pursuit", "high-speed chase", "police chase"), "pursuit"),
    (("incident report",), "incident report"),
    (("911 audio", "911 call"), "911 audio"),
)


# ---- term sets (public for tests) -----------------------------------


# Strong positive signals — boost score AND count toward "incident
# specific" classification when paired with an identity anchor.
INCIDENT_TERMS: Tuple[str, ...] = (
    "officer involved shooting",
    "officer-involved shooting",
    "deputy involved shooting",
    "deputy-involved shooting",
    "police shooting",
    "critical incident briefing",
    "critical incident",
    "in-custody death",
    "in custody death",
)
BODYCAM_TERMS: Tuple[str, ...] = (
    "bodycam", "body cam", "body camera", "body-worn camera",
    "body worn camera", "bwc",
)
PURSUIT_TERMS: Tuple[str, ...] = (
    "pursuit", "high-speed chase", "police chase",
    "dashcam", "dash cam", "dash-cam",
)
AUDIO_TERMS: Tuple[str, ...] = (
    "911 audio", "911 call", "cad log", "cad logs",
    "computer aided dispatch", "incident report",
)

# Negative signals — demote policy-only / generic-records requests.
# These are titles that look responsive on a keyword scan but rarely
# carry the per-incident artifacts a downstream stage cares about.
POLICY_ONLY_TERMS: Tuple[str, ...] = (
    # policy / procedure boilerplate
    "policy manual", "general orders", "general order",
    "all policies", "all body camera policies", "any policies",
    "body camera policy", "body-worn camera policy", "bwc policy",
    "medical policies", "abuse policies", "intimate partner",
    # training
    "training materials", "training manual", "training material",
    # admin / financial
    "audit", "annual report", "annual budget", "budget",
    "procurement", "contract listing", "contract",
    "communication services",
    # rosters / org charts
    "agency roster", "roster", "org chart",
)

# Status values MuckRock uses for "request fulfilled / records released".
DONE_STATUSES = frozenset({"done", "completed", "complete", "released"})

# Hint identifiers used in next_actions_hint; mirrored from
# pipeline2_discovery.dataset_sources.models.NextActionHint.
HINT_MUCKROCK_PARSE = "MUCKROCK_PARSE_RELEASED_FILES"
HINT_ARTIFACT_SEARCH = "ARTIFACT_SEARCH"
HINT_YOUTUBE = "YOUTUBE_METADATA_TRANSCRIPT"
HINT_OUTCOME = "OUTCOME_VALIDATE"

YOUTUBE_URL_RE = re.compile(
    r"https?://(?:www\.)?(?:youtube\.com/watch|youtu\.be/)[^\s\"'<>)]+",
    re.IGNORECASE,
)


# ---- scoring ---------------------------------------------------------


@dataclass
class _ScoredResult:
    """One MuckRock API record after scoring.

    The flag set distinguishes ``has_released_files`` (true only when
    the API payload actually carries ``files[]`` or explicit file URL
    metadata) from ``is_completed_status`` (true when the request's
    ``status`` is one of ``done`` / ``completed`` / ``complete`` /
    ``released``). The two are NOT interchangeable: a request can be
    marked ``done`` without ever surfacing released files (e.g.
    "responsive records: none", policy-deflected, withdrawn). The
    Bulmahn smoke surfaced 5 Pinal admin records all marked
    ``status=done`` with ``file_count=0``; treating that as
    artifact-bearing is the false-positive failure mode this gate
    tightening is fixing.
    """
    url: str
    title: str
    agency: str
    jurisdiction: str
    status: str
    submitted_at: str
    completed_at: str
    file_count: int
    file_urls: List[str]
    body_snippet: str
    embedded_youtube_urls: List[str] = field(default_factory=list)

    score: int = 0
    matched_terms: List[str] = field(default_factory=list)
    matched_anchors: List[str] = field(default_factory=list)
    has_anchor: bool = False
    has_subject: bool = False
    has_agency: bool = False
    has_city: bool = False
    has_state: bool = False
    has_strong_terms: bool = False
    has_supporting_terms: bool = False
    has_released_files: bool = False
    is_completed_status: bool = False
    is_agency_admin_only: bool = False
    is_policy_only: bool = False
    drop_reason: Optional[str] = None


def _norm(s: Any) -> str:
    if s is None:
        return ""
    return str(s).lower()


def _score_record(record: _ScoredResult, *, context: Dict[str, Any]) -> None:
    """Mutate ``record`` in place with anchor / term / score fields.

    Anchors are derived from task.context (subject_name / agency /
    city / state) and matched against
    ``title + body_snippet + agency + jurisdiction``.
    """
    haystack_l = " ".join((
        _norm(record.title),
        _norm(record.body_snippet),
        _norm(record.agency),
        _norm(record.jurisdiction),
    ))
    raw = " ".join((
        record.title or "", record.body_snippet or "",
        record.agency or "", record.jurisdiction or "",
    ))

    subject = _norm(context.get("subject_name", ""))
    last = _last_name(subject)
    agency_ctx = _norm(context.get("agency", ""))
    distinctive = _agency_distinctive_tokens(agency_ctx)
    city = _norm(context.get("city", ""))
    state_abbr = (str(context.get("state") or "")).upper().strip()
    state_full = US_STATE_NAMES.get(state_abbr, "")

    # Subject anchor
    if subject and len(subject) >= 4 and subject in haystack_l:
        record.matched_anchors.append("full_subject_name")
        record.score += 3
        record.has_subject = True
    elif last and len(last) >= 3 and _word_re(last).search(haystack_l):
        record.matched_anchors.append("last_name")
        record.score += 2
        record.has_subject = True

    # Agency-distinctive token
    for tok in distinctive:
        if _word_re(tok).search(haystack_l):
            record.matched_anchors.append(f"agency_token:{tok}")
            record.score += 2
            record.has_agency = True
            break

    # City
    if city and _word_re(city).search(haystack_l):
        record.matched_anchors.append("city")
        record.score += 1
        record.has_city = True

    # State name + standalone abbr (case-sensitive on raw text)
    if state_full and _word_re(state_full).search(haystack_l):
        record.matched_anchors.append("state_name")
        record.score += 1
        record.has_state = True
    if state_abbr and len(state_abbr) == 2 and re.search(
        rf"(?<![A-Za-z0-9]){re.escape(state_abbr)}(?![A-Za-z0-9])", raw
    ):
        if "state_name" not in record.matched_anchors:
            record.matched_anchors.append("state_abbr")
            record.score += 1
        record.has_state = True

    record.has_anchor = (
        record.has_subject or record.has_agency
        or record.has_city or record.has_state
    )

    # Strong incident-specific terms (OIS / CIB / in-custody)
    for term in INCIDENT_TERMS:
        if term in haystack_l:
            record.matched_terms.append(term)
            record.score += 3
            record.has_strong_terms = True
            break

    # Bodycam family
    for term in BODYCAM_TERMS:
        if term in haystack_l:
            record.matched_terms.append(term)
            record.score += 2
            record.has_supporting_terms = True
            break

    # Pursuit family
    for term in PURSUIT_TERMS:
        if term in haystack_l:
            record.matched_terms.append(term)
            record.score += 2
            record.has_supporting_terms = True
            break

    # 911 / CAD / incident-report family — weaker than bodycam but
    # still useful.
    for term in AUDIO_TERMS:
        if term in haystack_l:
            record.matched_terms.append(term)
            record.score += 1
            record.has_supporting_terms = True
            break

    # Released files signal — STRICT: only actual files / file URLs
    # in the API payload set has_released_files=True. Status alone
    # does not imply artifact availability (Bulmahn lesson).
    if record.file_count > 0 or record.file_urls:
        record.has_released_files = True
        record.score += 5

    # Completed-status signal — separate from artifact availability.
    # Provides a small positive nudge (so a request truly closed by
    # the agency ranks above an open one) but does NOT count as an
    # artifact signal anywhere downstream.
    if _norm(record.status) in DONE_STATUSES:
        record.is_completed_status = True
        record.score += 1

    # Policy-only demotion — strong negative, can flip a result to
    # is_policy_only=True so the caller can drop it even if anchored.
    for term in POLICY_ONLY_TERMS:
        if term in haystack_l:
            record.is_policy_only = True
            record.score -= 4
            record.matched_terms.append(f"-{term}")
            break

    # "Agency-admin-only": kept by anchor (agency or city) but with no
    # subject anchor, no actual files, and no incident / artifact term.
    # These are the Pinal-roster / Pinal-contract / Pinal-medical-policy
    # records that cleared the v0 gate as `high` confidence in the
    # Bulmahn smoke. Flagging them lets the confidence + hint reducers
    # downgrade them and the diagnostic block surface a count.
    record.is_agency_admin_only = (
        (record.has_agency or record.has_city)
        and not record.has_subject
        and not record.has_released_files
        and not record.has_strong_terms
        and not record.has_supporting_terms
    )

    if not record.has_anchor:
        record.drop_reason = "no_anchor_match"
    elif record.is_policy_only and not record.has_released_files \
            and not record.has_strong_terms:
        record.drop_reason = "policy_only_no_release"


def _confidence_from(kept: Sequence[_ScoredResult]) -> str:
    """Reduce a kept-result list to a single confidence label.

    Rubric (post-Bulmahn tightening — see PR docstring):

      high if best surviving result has:
        - subject anchor AND actual released files, OR
        - subject anchor AND strong incident terms (OIS / CIB / in-
          custody death), OR
        - agency / city anchor AND actual released files AND any
          case-relevant term (OIS / CIB / bodycam / pursuit /
          dashcam / 911 / CAD / incident report).

      medium if best surviving result has:
        - agency / city anchor AND a case-relevant term but NO
          actual released files, OR
        - subject anchor with no files and no strong incident term.

      low otherwise — covers: state-only anchor; agency / city
        anchor with no case-relevant term and no files (the
        Pinal-admin / Pinal-roster / Pinal-contract pattern);
        completed status alone with no artifact / case term;
        no surviving results.
    """
    if not kept:
        return "low"
    best = max(kept, key=lambda r: r.score)
    has_files = best.has_released_files
    has_case_term = best.has_strong_terms or best.has_supporting_terms

    # Subject-anchored paths
    if best.has_subject:
        if has_files or best.has_strong_terms:
            return "high"
        return "medium"  # subject alone, no files, no strong terms

    # Agency / city anchored (no subject)
    if best.has_agency or best.has_city:
        if has_files and has_case_term:
            return "high"
        if has_case_term:
            return "medium"
        return "low"  # agency-only with no case term — the Bulmahn pattern

    # State-only or no anchor
    return "low"


def _next_actions(kept: Sequence[_ScoredResult]) -> List[str]:
    """Routing hints for downstream stages.

    Tightened post-Bulmahn (see PR #38 docstring):

    - ``MUCKROCK_PARSE_RELEASED_FILES`` is emitted **only** when at
      least one kept result actually has files or explicit file URL
      metadata. A request with ``status=done`` but ``file_count=0``
      does NOT trigger the parse hint — that's the Pinal-admin
      false-positive class.
    - ``ARTIFACT_SEARCH`` mirrors the same gate (actual files only).
    - ``YOUTUBE_METADATA_TRANSCRIPT`` only when a kept result's
      metadata embeds a YouTube URL — never auto-emitted from
      "request mentions video".
    - ``OUTCOME_VALIDATE`` when any kept result's status is not in
      ``DONE_STATUSES``.
    """
    if not kept:
        return []
    hints: List[str] = []
    has_actual_files = any(r.has_released_files for r in kept)
    if has_actual_files:
        hints.append(HINT_MUCKROCK_PARSE)
        hints.append(HINT_ARTIFACT_SEARCH)
    if any(r.embedded_youtube_urls for r in kept):
        hints.append(HINT_YOUTUBE)
    if any(_norm(r.status) not in DONE_STATUSES for r in kept):
        hints.append(HINT_OUTCOME)
    # de-dupe, preserve order
    seen: set = set()
    out: List[str] = []
    for h in hints:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out


# ---- record extraction -----------------------------------------------


def _extract_jurisdiction(record: Dict[str, Any]) -> str:
    j = record.get("jurisdiction")
    if isinstance(j, dict):
        parts = [j.get("name"), j.get("level")]
        return ", ".join(p for p in parts if p)
    if isinstance(j, str):
        return j
    return ""


def _extract_agency(record: Dict[str, Any]) -> str:
    a = record.get("agency")
    if isinstance(a, dict):
        return a.get("name") or ""
    if isinstance(a, str):
        return a
    return ""


def _extract_files(record: Dict[str, Any]) -> Tuple[int, List[str]]:
    files = record.get("files") or []
    if not isinstance(files, list):
        return 0, []
    urls: List[str] = []
    for f in files:
        if isinstance(f, dict):
            ffile = f.get("ffile") or f.get("file") or f.get("url")
            if isinstance(ffile, str) and ffile.startswith("http"):
                urls.append(ffile)
        elif isinstance(f, str) and f.startswith("http"):
            urls.append(f)
    return len(files), urls


def _absolute_url(record: Dict[str, Any]) -> str:
    """Best-available public URL for a MuckRock request record.

    MuckRock's ``/api_v2/requests/`` *list* endpoint omits both
    ``absolute_url`` and ``url`` (only the *detail* endpoint includes
    them), so we fall back to constructing a ``/foi/<id>-<slug>/`` URL
    from ``id`` + ``slug`` when those are present. Without this
    fallback every list-endpoint record was being silently dropped
    before scoring — visible in the first live smoke as
    ``deduped_raw_result_count=44`` paired with ``raw_result_count=0``.
    """
    url = str(record.get("absolute_url") or record.get("url") or "")
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"https://www.muckrock.com{url}"
    rid = record.get("id")
    slug = str(record.get("slug") or "").strip()
    if rid:
        if slug:
            return f"https://www.muckrock.com/foi/{rid}-{slug}/"
        return f"https://www.muckrock.com/foi/{rid}/"
    return ""


def _body_snippet(record: Dict[str, Any], max_chars: int = 400) -> str:
    parts: List[str] = []
    if record.get("body"):
        parts.append(str(record["body"]))
    for c in record.get("communications") or []:
        if isinstance(c, dict) and c.get("text"):
            parts.append(str(c["text"]))
    text = " ".join(parts).strip()
    if len(text) > max_chars:
        text = text[:max_chars] + "..."
    return text


def _extract_youtube_urls(text: str) -> List[str]:
    if not text:
        return []
    return list(dict.fromkeys(YOUTUBE_URL_RE.findall(text)))


# ---- query plan ------------------------------------------------------


def _select_topic_term(query_lower: str) -> str:
    """Pick the most-specific canonical topic term mentioned in the
    task's query, for re-issuing as a broad MuckRock title search.

    Returns "" if the task query mentions none of the recognised
    domain signals — in that case the caller falls back to a more
    generic policy (e.g. drop the topic-term attempt entirely)."""
    for needles, canonical in TOPIC_TERMS_BY_SIGNAL:
        if any(n in query_lower for n in needles):
            return canonical
    return ""


def build_query_plan(task: EnrichmentTask, max_attempts: int) -> List[str]:
    """Generate up to ``max_attempts`` MuckRock title-search query
    strings derived from the task's identity context + topic signal.

    Priority (most-precise first; later items are added only if a
    slot remains):

    1. Strongest agency-distinctive token (longest one), e.g.
       "longmont" from "longmont police services".
    2. Subject last name, only when ``len(last) >= 4``.
    3. Topic term picked from :data:`TOPIC_TERMS_BY_SIGNAL` if the
       task query mentions any recognised domain signal.
    4. Second agency-distinctive token (a multi-agency context like
       "zavala county sheriff's office, crystal city police
       department" yields "zavala" + "crystal" — both useful).
    5. Generic fallback term ("body-worn camera") when nothing else
       fits.

    Empty list is impossible for a well-formed muckrock_query task,
    but in the degenerate case (no agency, no subject, no topic) the
    plan falls back to ``[task.query]`` so the caller still issues
    one GET — preserving backward compatibility with the v0 single-
    query behaviour.
    """
    max_attempts = max(1, min(int(max_attempts), 5))

    ctx = task.context or {}
    plan: List[str] = []

    distinctive = _agency_distinctive_tokens(_norm(ctx.get("agency", "")))
    distinctive = [t for t in distinctive if t not in EXTRA_AGENCY_STOPWORDS]
    distinctive_sorted = sorted(distinctive, key=len, reverse=True)

    if distinctive_sorted:
        plan.append(distinctive_sorted[0])

    last = _last_name(_norm(ctx.get("subject_name", "")))
    if last and len(last) >= MIN_LAST_NAME_LEN_FOR_QUERY:
        if last not in plan:
            plan.append(last)

    topic = _select_topic_term(_norm(task.query))
    if topic and topic not in plan:
        plan.append(topic)

    # Fill remaining slots, in order: secondary agency tokens,
    # then any topic term we haven't tried yet.
    if len(plan) < max_attempts:
        for tok in distinctive_sorted[1:]:
            if len(plan) >= max_attempts:
                break
            if tok not in plan:
                plan.append(tok)

    if len(plan) < max_attempts:
        # Try other recognised topic terms from the query in case the
        # picker's first match isn't what landed.
        q_l = _norm(task.query)
        for needles, canonical in TOPIC_TERMS_BY_SIGNAL:
            if len(plan) >= max_attempts:
                break
            if any(n in q_l for n in needles) and canonical not in plan:
                plan.append(canonical)

    if not plan:
        # Pure backstop — never silently issue zero GETs.
        plan.append(task.query)

    return plan[:max_attempts]


def _record_dedupe_key(record: Dict[str, Any]) -> str:
    """Best-available stable identifier for an API record. Prefer
    integer ``id``; fall back to ``absolute_url``; last resort title."""
    rid = record.get("id")
    if rid is not None:
        return f"id:{rid}"
    abs_url = record.get("absolute_url") or record.get("url") or ""
    if abs_url:
        return f"url:{abs_url}"
    title = str(record.get("title") or "").strip().lower()
    return f"title:{title}"


def _build_scored(record: Dict[str, Any]) -> _ScoredResult:
    file_count, file_urls = _extract_files(record)
    body = _body_snippet(record)
    return _ScoredResult(
        url=_absolute_url(record),
        title=str(record.get("title") or "").strip(),
        agency=_extract_agency(record),
        jurisdiction=_extract_jurisdiction(record),
        status=str(record.get("status") or "").strip(),
        submitted_at=str(record.get("datetime_submitted") or "").strip(),
        completed_at=str(record.get("datetime_done") or "").strip(),
        file_count=file_count,
        file_urls=file_urls,
        body_snippet=body,
        embedded_youtube_urls=_extract_youtube_urls(
            " ".join((str(record.get("title") or ""), body))
        ),
    )


# ---- HTTP client (injectable) ---------------------------------------


class _DefaultHttpClient:
    """Thin wrapper around ``requests.Session`` that exposes only
    ``get()``. The provider holds this object via composition rather
    than inheritance so tests can swap it out wholesale."""

    def __init__(self) -> None:
        import requests  # type: ignore
        self._session = requests.Session()

    def get(self, url: str, *, params: Dict[str, Any], headers: Dict[str, str], timeout: int):
        return self._session.get(url, params=params, headers=headers, timeout=timeout)


# ---- provider --------------------------------------------------------


class MuckRockProvider:
    """MuckRock public-API enrichment provider.

    Parameters are keyword-only.

    ``http_client`` lets tests inject a fake transport. Production
    leaves it ``None`` so :class:`_DefaultHttpClient` is loaded
    lazily — the ``requests`` import never happens at module import
    time, which keeps dry-run paths zero-network.

    ``api_token_env`` defaults to ``"MUCKROCK_API_TOKEN"``. The env
    var is read **only at execute() time** and only included as an
    ``Authorization: Token <token>`` header if non-empty. Tests
    should set ``read_token=False`` to be explicit.
    """

    name = "muckrock"

    def __init__(
        self,
        *,
        max_results: int = DEFAULT_MAX_RESULTS,
        page_size: int = DEFAULT_PAGE_SIZE,
        timeout: int = DEFAULT_TIMEOUT,
        rate_limit_seconds: float = DEFAULT_RATE_LIMIT_SECONDS,
        max_query_attempts: int = DEFAULT_MAX_QUERY_ATTEMPTS,
        api_base: str = MUCKROCK_API_BASE,
        api_token_env: str = "MUCKROCK_API_TOKEN",
        read_token: bool = True,
        http_client: Optional[Any] = None,
        sleeper: Optional[Any] = None,
    ) -> None:
        self._max_results = max(1, min(int(max_results), 20))
        self._page_size = max(1, min(int(page_size), 50))
        self._timeout = int(timeout)
        self._rate_limit_seconds = float(rate_limit_seconds)
        self._max_query_attempts = max(1, min(int(max_query_attempts), 5))
        self._api_base = api_base.rstrip("/") + "/"
        self._api_token_env = api_token_env
        self._read_token = bool(read_token)
        self._http_client = http_client
        self._sleeper = sleeper or time.sleep

    # ---- helpers ----

    def _headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json", "User-Agent": "FlameOn-AutoResearch/1.0"}
        if self._read_token:
            tok = os.environ.get(self._api_token_env, "").strip()
            if tok:
                headers["Authorization"] = f"Token {tok}"
        return headers

    def _client(self) -> Any:
        if self._http_client is not None:
            return self._http_client
        self._http_client = _DefaultHttpClient()
        return self._http_client

    def _redacted_api_url(self, query: str) -> str:
        # The token is in the header, not the URL — but redact any
        # query value that looks tokenish just in case.
        return f"{self._api_base}?title={query}&page_size={self._page_size}"

    # ---- main ----

    def _fetch_one(
        self, query: str, *, headers: Dict[str, str],
    ) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
        """One title-search GET. Returns ``(results, error)``.

        On success ``error`` is ``None`` and ``results`` is a list
        (possibly empty). On HTTP / transport / parse failure
        ``results`` is ``None`` and ``error`` carries a short label.
        """
        params = {"title": query, "page_size": self._page_size}
        try:
            response = self._client().get(
                self._api_base, params=params, headers=headers,
                timeout=self._timeout,
            )
        except Exception as exc:
            return None, f"{type(exc).__name__}: {exc}"

        status_code = getattr(response, "status_code", 0)
        if status_code != 200:
            return None, f"http_{status_code}"

        try:
            payload = response.json()
        except Exception as exc:
            return None, f"json_decode_error: {type(exc).__name__}: {exc}"

        raw = payload.get("results") or []
        if not isinstance(raw, list):
            raw = []
        return raw, None

    def execute(self, task: EnrichmentTask) -> EnrichmentResult:
        if task.task_type != "muckrock_query":
            return EnrichmentResult(
                candidate_id=task.candidate_id,
                task_type=task.task_type,
                query=task.query,
                status=TaskStatus.SKIPPED,
                provider=self.name,
                notes=[
                    f"task_type={task.task_type!r} is not muckrock_query — "
                    f"this provider only handles muckrock_query tasks"
                ],
            )

        plan = build_query_plan(task, self._max_query_attempts)
        headers = self._headers()

        per_query_counts: List[Tuple[str, int]] = []
        api_urls: List[str] = []
        deduped: Dict[str, Dict[str, Any]] = {}

        first_failure: Optional[Tuple[str, str, str]] = None  # (query, api_url, err)

        for idx, q in enumerate(plan):
            # Rate-limit BETWEEN attempts (and once before the first
            # attempt) — sequential over a single task.
            if self._rate_limit_seconds > 0:
                try:
                    self._sleeper(self._rate_limit_seconds)
                except Exception:
                    pass

            api_urls.append(self._redacted_api_url(q))
            raw, err = self._fetch_one(q, headers=headers)
            if err is not None:
                per_query_counts.append((q, 0))
                if first_failure is None:
                    first_failure = (q, api_urls[-1], err)
                continue

            assert raw is not None
            per_query_counts.append((q, len(raw)))
            for record in raw:
                if not isinstance(record, dict):
                    continue
                key = _record_dedupe_key(record)
                if key in deduped:
                    continue
                deduped[key] = record

        # If every attempt failed at the transport layer, surface one
        # representative failure rather than reporting "0 results".
        if first_failure is not None and all(
            err for _, _, err in [first_failure] for err in [err]
        ) and not deduped and all(c == 0 for _, c in per_query_counts):
            q, api_url, err = first_failure
            return EnrichmentResult(
                candidate_id=task.candidate_id,
                task_type=task.task_type,
                query=task.query,
                status=TaskStatus.FAILED,
                provider=self.name,
                error=err,
                notes=[
                    f"query_attempts={len(plan)}",
                    f"api_urls={';'.join(api_urls)}",
                    f"failed_query={q!r}",
                ],
            )

        scored: List[_ScoredResult] = []
        for record in list(deduped.values())[: max(self._page_size, self._max_results) * len(plan)]:
            sr = _build_scored(record)
            if not sr.url:
                continue
            _score_record(sr, context=task.context or {})
            scored.append(sr)

        kept = [r for r in scored if r.has_anchor and r.drop_reason is None]
        dropped = [r for r in scored if r not in kept]
        kept.sort(key=lambda r: -r.score)
        kept = kept[: self._max_results]

        urls = [r.url for r in kept]
        titles = [r.title for r in kept]
        confidence = _confidence_from(kept)
        next_actions = _next_actions(kept)

        terms_matched = sorted({
            t.lstrip("-") for r in kept for t in r.matched_terms
        })
        total_files = sum(r.file_count for r in kept)
        policy_only_demoted = any(
            r.is_policy_only for r in dropped + kept
        )

        per_query_str = ",".join(
            f"{q!r}:{n}" for q, n in per_query_counts
        )
        agency_admin_only_count = sum(1 for r in kept if r.is_agency_admin_only)
        notes: List[str] = [
            f"query_attempts={len(plan)}",
            f"raw_result_count_by_query={per_query_str}",
            f"deduped_raw_result_count={len(deduped)}",
            f"raw_result_count={len(scored)}",
            f"returned_result_count={len(kept)}",
            f"dropped_irrelevant_count={len(dropped)}",
            f"released_file_count={total_files}",
            f"agency_admin_only_count={agency_admin_only_count}",
            f"policy_only_demoted={'true' if policy_only_demoted else 'false'}",
            f"api_urls={';'.join(api_urls)}",
        ]
        if terms_matched:
            notes.append(f"terms_matched={','.join(terms_matched)}")
        if kept:
            for r in kept:
                tag = " admin_only" if r.is_agency_admin_only else ""
                notes.append(
                    f"kept score={r.score} "
                    f"anchors={','.join(r.matched_anchors) or '-'} "
                    f"files={r.file_count} status={r.status or '-'}{tag}"
                )
        else:
            notes.append(
                "no MuckRock results survived the relevance gate "
                "— all dropped (no anchor, or policy-only without "
                "released files)"
            )
        for r in dropped[:3]:
            snippet = (r.title or "").strip()
            if len(snippet) > 80:
                snippet = snippet[:77] + "..."
            notes.append(
                f"dropped reason={r.drop_reason or 'no_anchor_match'} "
                f"title={snippet!r}"
            )

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
    "AUDIO_TERMS",
    "BODYCAM_TERMS",
    "DEFAULT_MAX_QUERY_ATTEMPTS",
    "DONE_STATUSES",
    "HINT_ARTIFACT_SEARCH",
    "HINT_MUCKROCK_PARSE",
    "HINT_OUTCOME",
    "HINT_YOUTUBE",
    "INCIDENT_TERMS",
    "MIN_LAST_NAME_LEN_FOR_QUERY",
    "MUCKROCK_API_BASE",
    "MuckRockProvider",
    "POLICY_ONLY_TERMS",
    "PURSUIT_TERMS",
    "TOPIC_TERMS_BY_SIGNAL",
    "build_query_plan",
]
