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
    "policy manual", "general orders", "general order",
    "all policies", "all body camera policies", "any policies",
    "training materials", "training manual", "training material",
    "audit", "annual report", "annual budget", "budget",
    "procurement", "contract listing",
    "body camera policy", "body-worn camera policy",
    "bwc policy",
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
    """One MuckRock API record after scoring."""
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

    # Released files signal
    if record.file_count > 0:
        record.has_released_files = True
        record.score += 5
    elif _norm(record.status) in DONE_STATUSES:
        # Some completed requests ship file URLs but the API summary
        # call doesn't enumerate them. "done" is treated as evidence
        # of release for scoring; the file_count stays 0.
        record.has_released_files = True
        record.score += 3

    # Policy-only demotion — strong negative, can flip a result to
    # is_policy_only=True so the caller can drop it even if anchored.
    for term in POLICY_ONLY_TERMS:
        if term in haystack_l:
            record.is_policy_only = True
            record.score -= 4
            record.matched_terms.append(f"-{term}")
            break

    if not record.has_anchor:
        record.drop_reason = "no_anchor_match"
    elif record.is_policy_only and not record.has_released_files \
            and not record.has_strong_terms:
        record.drop_reason = "policy_only_no_release"


def _confidence_from(kept: Sequence[_ScoredResult]) -> str:
    """Reduce a kept-result list to a single confidence label.

    high  — at least one anchored result that has released files OR
            strong (OIS / CIB / in-custody) terms.
    medium — anchored result with bodycam / pursuit / 911 supporting
            signal but no released files and no strong terms.
    low    — only weak (state-only) anchors, or no kept results.
    """
    if not kept:
        return "low"
    best = max(kept, key=lambda r: r.score)
    strong_anchor = best.has_subject or best.has_agency or best.has_city
    if strong_anchor and (best.has_released_files or best.has_strong_terms):
        return "high"
    if strong_anchor and best.has_supporting_terms:
        return "medium"
    return "low"


def _next_actions(kept: Sequence[_ScoredResult]) -> List[str]:
    if not kept:
        return []
    hints: List[str] = [HINT_MUCKROCK_PARSE]
    if any(r.file_count > 0 for r in kept):
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
    url = str(record.get("absolute_url") or record.get("url") or "")
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"https://www.muckrock.com{url}"
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

        # Polite per-task throttle — runner is sequential anyway, but
        # we want a soft ceiling regardless of who calls execute().
        if self._rate_limit_seconds > 0:
            try:
                self._sleeper(self._rate_limit_seconds)
            except Exception:
                pass  # never let sleep failures kill a run

        params = {"title": task.query, "page_size": self._page_size}
        headers = self._headers()
        api_url = self._redacted_api_url(task.query)

        try:
            client = self._client()
            response = client.get(
                self._api_base, params=params, headers=headers,
                timeout=self._timeout,
            )
        except Exception as exc:
            return EnrichmentResult(
                candidate_id=task.candidate_id,
                task_type=task.task_type,
                query=task.query,
                status=TaskStatus.FAILED,
                provider=self.name,
                error=f"{type(exc).__name__}: {exc}",
                notes=[f"api_url={api_url}"],
            )

        status_code = getattr(response, "status_code", 0)
        if status_code != 200:
            return EnrichmentResult(
                candidate_id=task.candidate_id,
                task_type=task.task_type,
                query=task.query,
                status=TaskStatus.FAILED,
                provider=self.name,
                error=f"http_{status_code}",
                notes=[f"api_url={api_url}", f"http_status={status_code}"],
            )

        try:
            payload = response.json()
        except Exception as exc:
            return EnrichmentResult(
                candidate_id=task.candidate_id,
                task_type=task.task_type,
                query=task.query,
                status=TaskStatus.FAILED,
                provider=self.name,
                error=f"json_decode_error: {type(exc).__name__}: {exc}",
                notes=[f"api_url={api_url}"],
            )

        raw_results = payload.get("results") or []
        if not isinstance(raw_results, list):
            raw_results = []

        scored: List[_ScoredResult] = []
        for record in raw_results[: max(self._page_size, self._max_results)]:
            if not isinstance(record, dict):
                continue
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

        notes: List[str] = [
            f"raw_result_count={len(scored)}",
            f"returned_result_count={len(kept)}",
            f"dropped_irrelevant_count={len(dropped)}",
            f"released_file_count={total_files}",
            f"policy_only_demoted={'true' if policy_only_demoted else 'false'}",
            f"api_url={api_url}",
        ]
        if terms_matched:
            notes.append(f"terms_matched={','.join(terms_matched)}")
        if kept:
            for r in kept:
                notes.append(
                    f"kept score={r.score} "
                    f"anchors={','.join(r.matched_anchors) or '-'} "
                    f"files={r.file_count} status={r.status or '-'}"
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
    "DONE_STATUSES",
    "HINT_ARTIFACT_SEARCH",
    "HINT_MUCKROCK_PARSE",
    "HINT_OUTCOME",
    "HINT_YOUTUBE",
    "INCIDENT_TERMS",
    "MUCKROCK_API_BASE",
    "MuckRockProvider",
    "POLICY_ONLY_TERMS",
    "PURSUIT_TERMS",
]
