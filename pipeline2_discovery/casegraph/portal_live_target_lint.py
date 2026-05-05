"""Preflight lint for portal-live target URLs.

Pure module that validates candidate URLs against an allowlist of
official agency hosts and known-good URL shapes. Used by the target
generator (``portal_live_target_generator.py``) and the
``tools/generate_portal_live_targets.py`` operator script. Never
fetches the web; never imports a networked module.

Current scope: Phoenix Police Department Newsroom article-detail
URLs only — host ``www.phoenix.gov``, path
``/newsroom/police-department-news/<id>.html`` where ``<id>`` is
numeric or a safe slug. Future PRs may extend
``PORTAL_LIVE_TARGET_HOSTS`` to additional agency hosts; until then
the lint deliberately rejects everything else so a curated-list typo
becomes a clean rejection rather than a silent live-fetch attempt.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple
from urllib.parse import urlparse, urlunparse


# ---- allowlist -------------------------------------------------------


PORTAL_LIVE_TARGET_HOSTS: Tuple[str, ...] = (
    "www.phoenix.gov",
)


# Per-host accepted path pattern. The capture group ``id`` feeds
# ``_derive_target_id_from_phoenix_path`` to build a stable
# ``phoenix_pd_newsroom_<id>`` target_id when the row supplies none.
_PHOENIX_NEWSROOM_PATH_RE = re.compile(
    r"^/newsroom/police-department-news/"
    r"(?P<id>[A-Za-z0-9][A-Za-z0-9_\-]*)\.html$"
)

_PER_HOST_PATH_PATTERNS = {
    "www.phoenix.gov": _PHOENIX_NEWSROOM_PATH_RE,
}


# Belt-and-suspenders denylist of path substrings. Any path on an
# allowlisted host that contains these substrings is rejected with a
# specific reason code, even if the per-host regex would also reject
# it. Keeps operator-facing rejections precise.
_DENYLISTED_PATH_SUBSTRINGS: Tuple[str, ...] = (
    "/search",
    "/login",
    "/auth",
    "/admin",
    "/private",
    "/account",
)

# File extensions that always reject regardless of host. PDFs and
# binary media never run through the agency_ois HTML extractor.
_DENYLISTED_FILE_EXTENSIONS: Tuple[str, ...] = (
    ".pdf",
    ".doc", ".docx",
    ".xls", ".xlsx",
    ".ppt", ".pptx",
    ".zip", ".tar", ".gz",
    ".jpg", ".jpeg", ".png", ".gif", ".webp",
    ".mp4", ".mp3", ".mov", ".avi", ".wav",
    ".csv", ".txt", ".json", ".xml",
)


# Safe pattern for operator-supplied custom target_ids. Defends
# against shell metachars, path traversal (``../``), whitespace, and
# case-mixing surprises in derived filenames. Allows ASCII alnum,
# underscore, and hyphen — same character set as URL slugs.
_SAFE_TARGET_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_\-]{2,79}$")


@dataclass(frozen=True)
class LintDecision:
    """Result of running ``lint_candidate`` on one row.

    ``reason`` is a stable snake_case code suitable for filtering or
    grouping in operator output. When ``accepted`` is True, ``reason``
    is ``"ok"`` and the populated fields describe the normalized
    candidate. When False, ``reason`` describes why the candidate was
    rejected and the URL-derived fields may be ``None``.
    """
    accepted: bool
    reason: str
    normalized_url: Optional[str] = None
    host: Optional[str] = None
    path: Optional[str] = None
    target_id_suggestion: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "accepted": self.accepted,
            "reason": self.reason,
            "normalized_url": self.normalized_url,
            "host": self.host,
            "path": self.path,
            "target_id_suggestion": self.target_id_suggestion,
        }


def lint_candidate(
    url: str,
    *,
    custom_target_id: Optional[str] = None,
) -> LintDecision:
    """Validate a candidate URL against the portal-live target allowlist.

    Strips ``#fragments`` silently (fragments are non-actionable for
    the fetcher anyway) and rejects everything else that fails the
    allowlist. The returned ``normalized_url`` is what the generator
    should write into the fixture's ``url`` field.

    ``custom_target_id`` is honored only if it passes the safe-id
    pattern; otherwise the decision is rejected with
    ``unsafe_target_id``.
    """
    if not isinstance(url, str) or not url.strip():
        return LintDecision(accepted=False, reason="empty_url")

    parsed = urlparse(url.strip())

    # Scheme: HTTPS only. http:// and any other scheme rejected.
    if parsed.scheme != "https":
        return LintDecision(accepted=False, reason="non_https_scheme")

    host = parsed.netloc.lower()
    if not host:
        return LintDecision(accepted=False, reason="missing_host")

    if host not in PORTAL_LIVE_TARGET_HOSTS:
        return LintDecision(
            accepted=False,
            reason="host_not_in_allowlist",
            host=host,
        )

    # Reject query strings entirely. None of the current allowlisted
    # hosts/paths require them; a query string is exactly the
    # operator-confusion vector this lint is designed to catch
    # (UTM-tagged URLs from emails, search results, etc.).
    if parsed.query:
        return LintDecision(
            accepted=False,
            reason="query_string_not_allowed",
            host=host,
            path=parsed.path,
        )

    path = parsed.path

    # Denylist: certain paths never ship a CIB.
    path_lower = path.lower()
    for substring in _DENYLISTED_PATH_SUBSTRINGS:
        if substring in path_lower:
            return LintDecision(
                accepted=False,
                reason="denylisted_path",
                host=host,
                path=path,
            )

    # File-extension denylist (PDFs, binary media).
    for ext in _DENYLISTED_FILE_EXTENSIONS:
        if path_lower.endswith(ext):
            return LintDecision(
                accepted=False,
                reason=f"denylisted_extension:{ext}",
                host=host,
                path=path,
            )

    # Per-host article-detail path pattern.
    pattern = _PER_HOST_PATH_PATTERNS[host]
    m = pattern.match(path)
    if not m:
        return LintDecision(
            accepted=False,
            reason="path_pattern_mismatch",
            host=host,
            path=path,
        )

    # Normalize: drop fragment; reassemble. Trailing slashes / case
    # are not normalized because allowlisted paths are exact pattern
    # matches above.
    normalized = urlunparse((
        "https",
        host,
        path,
        "",  # params
        "",  # query (already rejected if present)
        "",  # fragment stripped
    ))

    # Target-id selection.
    if custom_target_id is not None:
        if not _SAFE_TARGET_ID_RE.match(custom_target_id):
            return LintDecision(
                accepted=False,
                reason="unsafe_target_id",
                host=host,
                path=path,
                normalized_url=normalized,
            )
        target_id = custom_target_id
    else:
        target_id = _derive_target_id_from_phoenix_path(m.group("id"))

    return LintDecision(
        accepted=True,
        reason="ok",
        normalized_url=normalized,
        host=host,
        path=path,
        target_id_suggestion=target_id,
    )


def _derive_target_id_from_phoenix_path(numeric_or_slug_id: str) -> str:
    """``3286`` -> ``phoenix_pd_newsroom_3286``. Slug ids preserved
    verbatim with lowercasing."""
    return f"phoenix_pd_newsroom_{numeric_or_slug_id.lower()}"


__all__ = [
    "LintDecision",
    "PORTAL_LIVE_TARGET_HOSTS",
    "lint_candidate",
]
