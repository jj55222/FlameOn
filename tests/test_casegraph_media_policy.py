"""MEDIA1 — central media URL classification policy tests.

Asserts that ``classify_media_url`` is:

- deterministic (same URL + hint -> same classification)
- network-free (no requests.Session.get)
- download-free
- correctly classifies media file extensions
  (.mp4 / .mov / .webm / .m3u8 -> video,
   .mp3 / .wav / .m4a -> audio)
- correctly classifies document file extensions
  (.pdf -> document/pdf, .doc / .docx / .rtf / .txt -> document)
- correctly classifies YouTube watch / shorts / embed and Vimeo
  numeric / player.vimeo embed URLs as media (video) when a video
  ID is present
- rejects bare YouTube / Vimeo homepage URLs (no video id) as
  no_media_indicator
- rejects login / auth=token / /private/ / /restricted/ /
  /preview/ / /draft/ URLs as protected_or_nonpublic
- rejects thumbnail / preview / poster / cover URLs as
  thumbnail_or_preview
- rejects empty URLs as empty_url
- rejects non-http(s) URLs as non_public_scheme
- rejects generic webpages with no media indicator as
  no_media_indicator
- preserves caller-supplied hints (e.g. ``bodycam_briefing``) as the
  source of artifact_type when the URL alone defaults to
  video_footage
- output is JSON-serializable via to_dict()
- ``classify_many`` accepts both lists and hint mappings
"""
from __future__ import annotations

import json

import pytest

from pipeline2_discovery.casegraph import (
    MediaClassification,
    classify_many,
    classify_media_url,
)


# ---- Concrete media extensions ---------------------------------------


@pytest.mark.parametrize(
    "url",
    [
        "https://www.phoenix.gov/media/footage.mp4",
        "https://example.gov/clip.MP4",
        "https://example.gov/incident.mov",
        "https://example.gov/incident.webm",
        "https://example.gov/stream.m3u8",
    ],
)
def test_video_extension_classifies_as_media_video(url):
    c = classify_media_url(url)
    assert c.is_media is True
    assert c.is_document is False
    assert c.format == "video"
    assert c.artifact_type == "video_footage"
    assert c.rejected is False
    assert c.verification_method.startswith("public_video_extension:")


@pytest.mark.parametrize(
    "url",
    [
        "https://www.phoenix.gov/audio/dispatch.mp3",
        "https://example.gov/911.wav",
        "https://example.gov/dispatch.m4a",
    ],
)
def test_audio_extension_classifies_as_media_audio(url):
    c = classify_media_url(url)
    assert c.is_media is True
    assert c.is_document is False
    assert c.format == "audio"
    assert c.artifact_type == "dispatch_911"
    assert c.rejected is False


@pytest.mark.parametrize("url", ["https://example.gov/report.pdf"])
def test_pdf_classifies_as_document_pdf(url):
    c = classify_media_url(url)
    assert c.is_document is True
    assert c.is_media is False
    assert c.format == "pdf"
    assert c.artifact_type == "docket_docs"
    assert c.verification_method == "public_pdf_extension"


@pytest.mark.parametrize(
    "url",
    [
        "https://example.gov/file.doc",
        "https://example.gov/file.docx",
        "https://example.gov/file.rtf",
        "https://example.gov/file.txt",
    ],
)
def test_other_document_extensions_classify_as_document(url):
    c = classify_media_url(url)
    assert c.is_document is True
    assert c.format == "document"
    assert c.artifact_type == "docket_docs"


# ---- Video hosts ------------------------------------------------------


@pytest.mark.parametrize(
    "url",
    [
        "https://www.youtube.com/watch?v=abc12345",
        "https://youtube.com/watch?v=abc12345",
        "https://m.youtube.com/watch?v=abc12345",
        "https://youtu.be/abc12345",
        "https://www.youtube.com/embed/abc12345",
        "https://www.youtube.com/shorts/abc12345",
        "https://vimeo.com/123456789",
        "https://player.vimeo.com/video/123456789",
    ],
)
def test_video_host_with_video_id_classifies_as_media(url):
    c = classify_media_url(url)
    assert c.is_media is True
    assert c.format == "video"
    assert c.artifact_type == "video_footage"
    assert c.rejected is False


@pytest.mark.parametrize(
    "url",
    [
        "https://www.youtube.com/",
        "https://www.youtube.com/results?search_query=foo",
        "https://vimeo.com/",
    ],
)
def test_bare_video_host_homepage_rejected_as_no_media_indicator(url):
    c = classify_media_url(url)
    assert c.rejected is True
    assert c.rejection_reason == "no_media_indicator"


# ---- Protected / login / private --------------------------------------


@pytest.mark.parametrize(
    "url",
    [
        "https://portal.example.gov/login?redirect=/oa/2024-001.mp4",
        "https://example.gov/private/footage.mp4",
        "https://example.gov/restricted/clip.mp4",
        "https://example.gov/internal/clip.mp4",
        "https://example.gov/auth=secret",
        "https://example.gov/?token=ABC123",
        "https://example.gov/?session=xyz",
        "https://example.gov/preview/clip.mp4",
        "https://example.gov/draft/clip.mp4",
    ],
)
def test_protected_url_rejected(url):
    c = classify_media_url(url)
    assert c.rejected is True
    assert c.rejection_reason == "protected_or_nonpublic"
    assert "protected_or_nonpublic" in c.risk_flags
    assert c.is_media is False
    assert c.is_document is False


# ---- Thumbnails -------------------------------------------------------


@pytest.mark.parametrize(
    "url",
    [
        "https://example.gov/thumbnails/clip.jpg",
        "https://example.gov/thumbs/clip.jpg",
        "https://example.gov/thumb/clip.jpg",
        "https://example.gov/clip_thumb.jpg",
        "https://example.gov/clip_thumbnail.png",
        "https://example.gov/poster/cover.jpg",
        "https://example.gov/cover/incident.jpg",
        "https://example.gov/clip_small.jpg",
        "https://example.gov/clip_lowres.jpg",
    ],
)
def test_thumbnail_url_rejected(url):
    c = classify_media_url(url)
    assert c.rejected is True
    assert c.rejection_reason == "thumbnail_or_preview"
    assert "thumbnail_or_preview" in c.risk_flags


# ---- Empty / scheme ---------------------------------------------------


def test_empty_url_rejected():
    c = classify_media_url("")
    assert c.rejected is True
    assert c.rejection_reason == "empty_url"
    assert "empty_url" in c.risk_flags


def test_whitespace_url_rejected():
    c = classify_media_url("   ")
    assert c.rejected is True
    assert c.rejection_reason == "empty_url"


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.gov/clip.mp4",
        "file:///tmp/clip.mp4",
        "javascript:alert(1)",
        "data:text/plain,hello",
        "/path/only",
    ],
)
def test_non_http_scheme_rejected(url):
    c = classify_media_url(url)
    assert c.rejected is True
    assert c.rejection_reason == "non_public_scheme"


def test_generic_webpage_rejected_as_no_media_indicator():
    c = classify_media_url("https://example.gov/news/article-about-incident")
    assert c.rejected is True
    assert c.rejection_reason == "no_media_indicator"
    assert "no_media_indicator" in c.risk_flags


# ---- Hints -------------------------------------------------------------


@pytest.mark.parametrize(
    "hint,expected_type",
    [
        ("bodycam_briefing", "bodycam"),
        ("BWC", "bodycam"),
        ("body-worn camera", "bodycam"),
        ("dashcam", "dashcam"),
        ("surveillance", "surveillance"),
        ("CCTV", "surveillance"),
        ("interrogation", "interrogation"),
        ("police_interview", "interrogation"),
        ("court_video", "court_video"),
        ("sentencing", "court_video"),
        ("trial_video", "court_video"),
        ("dispatch_911", "dispatch_911"),
        ("911 audio", "dispatch_911"),
    ],
)
def test_video_hint_overrides_default_artifact_type(hint, expected_type):
    """When a URL is a video by extension, an agency-supplied hint
    should override the default video_footage / dispatch_911 type."""
    if expected_type in {"dispatch_911"}:
        url = "https://example.gov/audio.mp3"
    else:
        url = "https://example.gov/clip.mp4"
    c = classify_media_url(url, hint=hint)
    assert c.artifact_type == expected_type
    assert c.hint_used == hint


def test_unknown_hint_falls_back_to_default():
    c = classify_media_url("https://example.gov/clip.mp4", hint="something_obscure")
    assert c.artifact_type == "video_footage"
    assert c.is_media is True


def test_document_hint_keeps_docket_docs():
    c = classify_media_url("https://example.gov/file.pdf", hint="ia_report")
    assert c.artifact_type == "docket_docs"
    assert c.format == "pdf"


# ---- Determinism / serialization --------------------------------------


def test_classification_is_deterministic():
    url = "https://example.gov/clip.mp4"
    a = classify_media_url(url, hint="bodycam_briefing")
    b = classify_media_url(url, hint="bodycam_briefing")
    assert a.to_dict() == b.to_dict()


def test_classification_to_dict_is_json_serializable():
    c = classify_media_url("https://example.gov/clip.mp4", hint="bodycam")
    encoded = json.dumps(c.to_dict())
    decoded = json.loads(encoded)
    assert decoded["artifact_type"] == "bodycam"
    assert decoded["format"] == "video"


def test_classify_many_accepts_list_of_urls():
    out = classify_many(
        [
            "https://example.gov/clip.mp4",
            "https://example.gov/file.pdf",
            "https://example.gov/login",
        ]
    )
    assert len(out) == 3
    assert out[0].format == "video"
    assert out[1].format == "pdf"
    assert out[2].rejected is True


def test_classify_many_accepts_hint_mapping():
    out = classify_many(
        {
            "https://example.gov/clip.mp4": "bodycam_briefing",
            "https://example.gov/audio.mp3": "dispatch_911",
            "https://example.gov/file.pdf": None,
        }
    )
    assert out[0].artifact_type == "bodycam"
    assert out[1].artifact_type == "dispatch_911"
    assert out[2].artifact_type == "docket_docs"


# ---- Network invariance -----------------------------------------------


def test_classifier_makes_zero_network_calls(monkeypatch):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    classify_media_url("https://example.gov/clip.mp4")
    classify_media_url("https://www.youtube.com/watch?v=abc12345")
    classify_media_url("https://example.gov/login")
    assert calls == []


# ---- Package surface --------------------------------------------------


def test_media_classification_is_re_exported_from_package():
    from pipeline2_discovery.casegraph import MediaClassification as Reimport
    assert Reimport is MediaClassification
