"""MEDIA1 — Central media URL classification policy.

Single source of truth for whether a given URL is a media candidate, a
document candidate, or neither — and which CaseGraph artifact_type /
format / risk_flags / verification_method it carries.

The policy is deterministic, network-free, and download-free. It is
used as a *policy* (read-only classification), not a live verifier:
nothing here fetches a URL or checks HTTP headers. Caller-supplied
hints (e.g. ``bodycam_briefing``) take precedence over URL-derived
defaults so an agency's own labelling sticks.

The policy understands:

- Concrete media file URLs: ``.mp4``, ``.mov``, ``.webm``, ``.m3u8``
  (video) and ``.mp3``, ``.wav``, ``.m4a`` (audio)
- Concrete document file URLs: ``.pdf``, ``.doc``, ``.docx``, ``.rtf``
- Public video host URLs: youtube.com / youtu.be / vimeo.com (watch
  + embed paths)

It rejects:

- empty / non-http(s) URLs
- protected / private / login / auth-token / portal URLs
- thumbnail / preview / placeholder URLs
- generic webpages with no media indicator

CaseGraph artifact types this module knows about:

- ``bodycam``, ``dashcam``, ``surveillance``, ``interrogation``,
  ``court_video``, ``video_footage`` (media / video)
- ``dispatch_911``, ``audio`` (media / audio)
- ``docket_docs`` (document)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import urlparse


VIDEO_EXTENSIONS = (".mp4", ".mov", ".webm", ".m3u8")
AUDIO_EXTENSIONS = (".mp3", ".wav", ".m4a")
DOCUMENT_EXTENSIONS = (".pdf", ".doc", ".docx", ".rtf", ".txt")


VIDEO_HOSTS = frozenset(
    {
        "youtube.com",
        "www.youtube.com",
        "m.youtube.com",
        "youtu.be",
        "vimeo.com",
        "www.vimeo.com",
        "player.vimeo.com",
    }
)


# Substrings that indicate the URL is not a public, downloadable asset.
PROTECTED_MARKERS = (
    "login",
    "signin",
    "sign-in",
    "/private/",
    "/restricted/",
    "/internal/",
    "auth=",
    "token=",
    "session=",
    "redirect=",
    "permission",
    "placeholder",
    "/preview/",
    "/draft/",
)


# Substrings that indicate this is a thumbnail or preview, not the
# canonical media asset.
THUMBNAIL_MARKERS = (
    "/thumbnails/",
    "/thumbnail/",
    "/thumbs/",
    "/thumb/",
    "_thumb.",
    "_thumbnail.",
    "/poster/",
    "/preview-image/",
    "/cover/",
    "_small.",
    "_lowres.",
)


VIDEO_ARTIFACT_TYPES = frozenset(
    {
        "bodycam",
        "dashcam",
        "surveillance",
        "interrogation",
        "court_video",
        "video_footage",
    }
)
AUDIO_ARTIFACT_TYPES = frozenset({"dispatch_911", "audio"})
DOCUMENT_ARTIFACT_TYPES = frozenset({"docket_docs"})


# Map free-text hints (link_type, label, etc.) to canonical CaseGraph
# artifact_type values. Matching is case-insensitive and substring-based.
HINT_TO_ARTIFACT_TYPE = (
    ("bodycam", "bodycam"),
    ("body_cam", "bodycam"),
    ("body-cam", "bodycam"),
    ("body worn", "bodycam"),
    ("body-worn", "bodycam"),
    ("bwc", "bodycam"),
    ("dashcam", "dashcam"),
    ("dash_cam", "dashcam"),
    ("dash-cam", "dashcam"),
    ("surveillance", "surveillance"),
    ("cctv", "surveillance"),
    ("interrogation", "interrogation"),
    ("interview", "interrogation"),
    ("court_video", "court_video"),
    ("court video", "court_video"),
    ("sentencing", "court_video"),
    ("trial_video", "court_video"),
    ("trial video", "court_video"),
    ("dispatch", "dispatch_911"),
    ("911", "dispatch_911"),
    ("ia_report", "docket_docs"),
    ("ia report", "docket_docs"),
    ("incident report", "docket_docs"),
    ("incident_report", "docket_docs"),
    ("incident_summary", "docket_docs"),
    ("incident summary", "docket_docs"),
    ("police report", "docket_docs"),
    ("use of force", "docket_docs"),
    ("complaint", "docket_docs"),
    ("indictment", "docket_docs"),
    ("affidavit", "docket_docs"),
    ("probable cause", "docket_docs"),
)


@dataclass
class MediaClassification:
    """Result of classifying a single URL via the media policy.

    Always emits a deterministic value for every key. ``rejected`` is
    True when the URL is not graduatable (claim_only, protected,
    thumbnail, generic page); ``rejection_reason`` names the rule that
    fired. ``artifact_type`` / ``format`` / ``verification_method`` are
    None when ``rejected``."""

    url: str
    is_media: bool
    is_document: bool
    artifact_type: Optional[str]
    format: Optional[str]
    risk_flags: List[str] = field(default_factory=list)
    verification_method: Optional[str] = None
    rejected: bool = False
    rejection_reason: Optional[str] = None
    hint_used: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "is_media": self.is_media,
            "is_document": self.is_document,
            "artifact_type": self.artifact_type,
            "format": self.format,
            "risk_flags": list(self.risk_flags),
            "verification_method": self.verification_method,
            "rejected": self.rejected,
            "rejection_reason": self.rejection_reason,
            "hint_used": self.hint_used,
        }


def _looks_protected(url_lower: str) -> bool:
    return any(marker in url_lower for marker in PROTECTED_MARKERS)


def _looks_thumbnail(url_lower: str) -> bool:
    return any(marker in url_lower for marker in THUMBNAIL_MARKERS)


def _extension(url: str) -> str:
    path = urlparse(url).path.lower()
    for ext in VIDEO_EXTENSIONS + AUDIO_EXTENSIONS + DOCUMENT_EXTENSIONS:
        if path.endswith(ext):
            return ext
    return ""


def _host(url: str) -> str:
    return urlparse(url).netloc.lower()


def _hint_to_artifact_type(hint: Optional[str]) -> Optional[str]:
    if not hint:
        return None
    lower = hint.lower()
    for needle, art_type in HINT_TO_ARTIFACT_TYPE:
        if needle in lower:
            return art_type
    return None


def _is_video_host(url: str) -> bool:
    return _host(url) in VIDEO_HOSTS


def _video_host_video_id_present(url: str) -> bool:
    """For YouTube / Vimeo, only treat the URL as media if it actually
    points at a watch / embed endpoint (rules out, e.g., the bare
    youtube.com homepage)."""
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    query = (parsed.query or "").lower()
    if host == "youtu.be":
        return len(path.strip("/")) > 0
    if host in {"youtube.com", "www.youtube.com", "m.youtube.com"}:
        if path in {"/watch", "/watch/"} and "v=" in query:
            return True
        if path.startswith("/embed/") and len(path) > len("/embed/"):
            return True
        if path.startswith("/shorts/") and len(path) > len("/shorts/"):
            return True
        return False
    if host in {"vimeo.com", "www.vimeo.com"}:
        # vimeo.com/{numeric_id}
        parts = [p for p in path.split("/") if p]
        return bool(parts and parts[0].isdigit())
    if host == "player.vimeo.com":
        return path.startswith("/video/") and len(path) > len("/video/")
    return False


def classify_media_url(url: str, *, hint: Optional[str] = None) -> MediaClassification:
    """Classify a URL deterministically. Pure: no network, no
    download, no header check.

    Args:
        url: the URL to classify.
        hint: optional free-text hint about the artifact (e.g. an
            agency-supplied link_type label such as
            ``bodycam_briefing``). Hints take precedence over
            URL-derived defaults when they map to a known
            artifact_type.

    Returns:
        :class:`MediaClassification` with ``rejected=True`` and a
        ``rejection_reason`` when the URL is not graduatable; else
        ``rejected=False`` with concrete artifact_type / format /
        verification_method values.
    """

    cleaned = (url or "").strip().strip("()[]{}<>'\"")
    if not cleaned:
        return MediaClassification(
            url=cleaned,
            is_media=False,
            is_document=False,
            artifact_type=None,
            format=None,
            risk_flags=["empty_url"],
            rejected=True,
            rejection_reason="empty_url",
        )

    parsed = urlparse(cleaned)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return MediaClassification(
            url=cleaned,
            is_media=False,
            is_document=False,
            artifact_type=None,
            format=None,
            risk_flags=["non_public_scheme"],
            rejected=True,
            rejection_reason="non_public_scheme",
        )

    lower = cleaned.lower()

    if _looks_protected(lower):
        return MediaClassification(
            url=cleaned,
            is_media=False,
            is_document=False,
            artifact_type=None,
            format=None,
            risk_flags=["protected_or_nonpublic"],
            rejected=True,
            rejection_reason="protected_or_nonpublic",
        )

    if _looks_thumbnail(lower):
        return MediaClassification(
            url=cleaned,
            is_media=False,
            is_document=False,
            artifact_type=None,
            format=None,
            risk_flags=["thumbnail_or_preview"],
            rejected=True,
            rejection_reason="thumbnail_or_preview",
        )

    ext = _extension(cleaned)
    hint_type = _hint_to_artifact_type(hint)

    if ext in VIDEO_EXTENSIONS:
        artifact_type = hint_type if hint_type in VIDEO_ARTIFACT_TYPES else "video_footage"
        return MediaClassification(
            url=cleaned,
            is_media=True,
            is_document=False,
            artifact_type=artifact_type,
            format="video",
            verification_method=f"public_video_extension:{ext.lstrip('.')}",
            hint_used=hint,
        )

    if ext in AUDIO_EXTENSIONS:
        artifact_type = hint_type if hint_type in AUDIO_ARTIFACT_TYPES else "dispatch_911"
        return MediaClassification(
            url=cleaned,
            is_media=True,
            is_document=False,
            artifact_type=artifact_type,
            format="audio",
            verification_method=f"public_audio_extension:{ext.lstrip('.')}",
            hint_used=hint,
        )

    if ext == ".pdf":
        return MediaClassification(
            url=cleaned,
            is_media=False,
            is_document=True,
            artifact_type="docket_docs",
            format="pdf",
            verification_method="public_pdf_extension",
            hint_used=hint,
        )

    if ext in DOCUMENT_EXTENSIONS:
        return MediaClassification(
            url=cleaned,
            is_media=False,
            is_document=True,
            artifact_type="docket_docs",
            format="document",
            verification_method=f"public_document_extension:{ext.lstrip('.')}",
            hint_used=hint,
        )

    if _is_video_host(cleaned) and _video_host_video_id_present(cleaned):
        artifact_type = hint_type if hint_type in VIDEO_ARTIFACT_TYPES else "video_footage"
        host = _host(cleaned)
        provider = "youtube" if "youtu" in host else "vimeo"
        return MediaClassification(
            url=cleaned,
            is_media=True,
            is_document=False,
            artifact_type=artifact_type,
            format="video",
            verification_method=f"public_video_host:{provider}",
            hint_used=hint,
        )

    return MediaClassification(
        url=cleaned,
        is_media=False,
        is_document=False,
        artifact_type=None,
        format=None,
        risk_flags=["no_media_indicator"],
        rejected=True,
        rejection_reason="no_media_indicator",
    )


def classify_many(
    urls: Mapping[str, Optional[str]] | List[str],
) -> List[MediaClassification]:
    """Classify a batch of URLs.

    ``urls`` may be a list of URL strings (no hints) or a mapping of
    URL -> hint. Returns one :class:`MediaClassification` per input,
    in the same order.
    """
    if isinstance(urls, Mapping):
        return [classify_media_url(u, hint=h) for u, h in urls.items()]
    return [classify_media_url(u) for u in urls]
