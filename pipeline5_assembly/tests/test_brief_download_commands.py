"""Tests for source download/action commands in pipeline5_assemble.

Covers two pieces:

1. ``_source_download_command(source)`` -- the pure helper that maps a
   P2 source dict to one of three command strings:
     - ``yt-dlp "URL"``               (YouTube/Vimeo OR requires_download
                                       on video/audio)
     - ``Direct download: URL``       (format in {video, audio, document})
     - ``Open/review manually: URL``  (webpage / unknown)

2. End-to-end: ``build_brief`` enriches each source with
   ``download_command``, ``render_markdown`` shows it beside the URL,
   and the input ``case_research`` is never mutated.

Pure helper. No network calls. No URL fetching. Classification is
based only on URL hostname patterns + the source dict's own fields.
"""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

from pipeline5_assemble import (  # noqa: E402
    _source_download_command,
    _with_download_command,
    build_brief,
    render_markdown,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _source(**overrides):
    """Build a minimal P2-style source dict with overrides."""
    base = {
        "url": "https://example.com/file.mp4",
        "evidence_type": "bodycam",
        "format": "video",
        "source_domain": "example.com",
        "requires_download": False,
        "notes": "",
    }
    base.update(overrides)
    return base


def _verdict_minimal():
    return {
        "case_id": "dl_test_001",
        "verdict": "PRODUCE",
        "narrative_score": 70.0,
        "confidence": 0.7,
        "key_moments": [],
        "content_pitch": "test pitch",
        "narrative_arc_recommendation": "cold_open",
        "estimated_runtime_min": 20.0,
        "artifact_completeness": {"available": [], "missing_recommended": []},
        "scoring_breakdown": {},
        "_pipeline4_metadata": {},
    }


def _build_with_sources(sources):
    research = {
        "defendant": "Test Defendant",
        "sources": sources,
    }
    return build_brief(
        _verdict_minimal(),
        case_research=research,
        transcripts=[],
        weights=None,
    )


# ---------------------------------------------------------------------------
# _source_download_command unit tests
# ---------------------------------------------------------------------------


def test_youtube_url_gets_yt_dlp_command():
    s = _source(url="https://www.youtube.com/watch?v=ABCDEFG", format="video")
    cmd = _source_download_command(s)
    assert cmd == 'yt-dlp "https://www.youtube.com/watch?v=ABCDEFG"'


def test_youtube_short_url_gets_yt_dlp_command():
    """youtu.be short URLs are also treated as YouTube."""
    s = _source(url="https://youtu.be/ABCDEFG", format="video")
    cmd = _source_download_command(s)
    assert cmd == 'yt-dlp "https://youtu.be/ABCDEFG"'


def test_vimeo_url_gets_yt_dlp_command():
    s = _source(url="https://vimeo.com/123456789", format="video")
    cmd = _source_download_command(s)
    assert cmd == 'yt-dlp "https://vimeo.com/123456789"'


def test_youtube_url_yt_dlp_regardless_of_format():
    """Even if format is missing/unknown, a YouTube URL routes to
    yt-dlp -- the host pattern wins."""
    s = _source(url="https://www.youtube.com/watch?v=ABC", format=None)
    cmd = _source_download_command(s)
    assert cmd.startswith("yt-dlp ")


def test_requires_download_true_video_routes_to_yt_dlp():
    """Non-YouTube video that is flagged requires_download=True (e.g.
    a MuckRock CDN video that needs extraction) gets yt-dlp."""
    s = _source(
        url="https://cdn.muckrock.com/agency/videos/release_001.mp4",
        format="video",
        requires_download=True,
    )
    cmd = _source_download_command(s)
    assert cmd == 'yt-dlp "https://cdn.muckrock.com/agency/videos/release_001.mp4"'


def test_requires_download_true_audio_routes_to_yt_dlp():
    s = _source(
        url="https://example.org/911_call.mp3",
        format="audio",
        requires_download=True,
    )
    cmd = _source_download_command(s)
    assert cmd.startswith("yt-dlp ")


def test_requires_download_true_document_does_not_force_yt_dlp():
    """Documents never go through yt-dlp -- requires_download only
    escalates video/audio to yt-dlp. A document with requires_download
    still becomes a Direct download (the `requires_download` flag is
    media-extraction-specific in this rule set)."""
    s = _source(
        url="https://agency.gov/docs/report.pdf",
        format="document",
        requires_download=True,
    )
    cmd = _source_download_command(s)
    assert cmd.startswith("Direct download: ")


def test_pdf_document_url_gets_direct_download():
    s = _source(
        url="https://agency.gov/foia/release.pdf",
        format="document",
        requires_download=False,
    )
    cmd = _source_download_command(s)
    assert cmd == "Direct download: https://agency.gov/foia/release.pdf"


def test_audio_url_no_requires_download_gets_direct_download():
    s = _source(
        url="https://example.com/911.wav",
        format="audio",
        requires_download=False,
    )
    cmd = _source_download_command(s)
    assert cmd.startswith("Direct download: ")


def test_video_url_no_requires_download_non_yt_gets_direct_download():
    """Direct .mp4 link that isn't on YouTube/Vimeo and isn't flagged
    requires_download -- assume it's directly downloadable."""
    s = _source(
        url="https://example.com/release.mp4",
        format="video",
        requires_download=False,
    )
    cmd = _source_download_command(s)
    assert cmd.startswith("Direct download: ")


def test_webpage_url_gets_open_manually():
    """webpage format -> manual review (court dockets, news pages, etc.)"""
    s = _source(
        url="https://courthouse.gov/case/123",
        evidence_type="court_docket",
        format="webpage",
    )
    cmd = _source_download_command(s)
    assert cmd == "Open/review manually: https://courthouse.gov/case/123"


def test_news_article_url_with_webpage_format_gets_open_manually():
    s = _source(
        url="https://news.example.com/article",
        evidence_type="news_report",
        format="webpage",
    )
    cmd = _source_download_command(s)
    assert cmd.startswith("Open/review manually: ")


def test_unknown_format_gets_open_manually():
    """A source with no format / unknown format defaults to manual."""
    s = _source(
        url="https://example.com/something",
        format=None,
    )
    cmd = _source_download_command(s)
    assert cmd.startswith("Open/review manually: ")


# ---------------------------------------------------------------------------
# Defensive cases (no crash on missing / malformed input)
# ---------------------------------------------------------------------------


def test_missing_url_handled_defensively():
    """Source with no `url` key returns a placeholder command, no crash."""
    s = _source()
    s.pop("url")
    cmd = _source_download_command(s)
    assert "(no url)" in cmd
    assert cmd.startswith("Open/review manually: ")


def test_empty_url_handled_defensively():
    s = _source(url="")
    cmd = _source_download_command(s)
    assert "(no url)" in cmd


def test_whitespace_only_url_handled_defensively():
    s = _source(url="   ")
    cmd = _source_download_command(s)
    assert "(no url)" in cmd


def test_none_source_handled_defensively():
    """None input returns a placeholder, doesn't crash."""
    cmd = _source_download_command(None)
    assert "(no url)" in cmd


def test_empty_dict_source_handled_defensively():
    cmd = _source_download_command({})
    assert "(no url)" in cmd


# ---------------------------------------------------------------------------
# _with_download_command: returns copy, never mutates input
# ---------------------------------------------------------------------------


def test_with_download_command_adds_field():
    s = _source(url="https://www.youtube.com/watch?v=A", format="video")
    out = _with_download_command(s)
    assert out["download_command"] == 'yt-dlp "https://www.youtube.com/watch?v=A"'
    # Original fields preserved
    assert out["url"] == s["url"]
    assert out["evidence_type"] == s["evidence_type"]


def test_with_download_command_does_not_mutate_input():
    s = _source(url="https://www.youtube.com/watch?v=A", format="video")
    original_keys = set(s.keys())
    _ = _with_download_command(s)
    assert set(s.keys()) == original_keys
    assert "download_command" not in s


# ---------------------------------------------------------------------------
# build_brief integration
# ---------------------------------------------------------------------------


def test_brief_json_includes_download_command_per_source():
    sources = [
        _source(url="https://www.youtube.com/watch?v=YT1", format="video"),
        _source(url="https://agency.gov/docs/report.pdf", format="document",
                evidence_type="foia_document"),
    ]
    brief = _build_with_sources(sources)
    sbt = brief["sources_by_type"]
    # Both sources should be enriched
    all_sources = [s for items in sbt.values() for s in items]
    assert len(all_sources) == 2
    for s in all_sources:
        assert "download_command" in s
    # Verify the YouTube one specifically
    yt = [s for s in all_sources if "youtube" in (s.get("url") or "")][0]
    assert yt["download_command"].startswith("yt-dlp ")


def test_brief_grouping_by_evidence_type_preserved():
    """Adding download_command must not change how sources are
    grouped by evidence_type in the brief."""
    sources = [
        _source(url="https://x/1", evidence_type="bodycam"),
        _source(url="https://x/2", evidence_type="bodycam"),
        _source(url="https://x/3", evidence_type="interrogation"),
    ]
    brief = _build_with_sources(sources)
    sbt = brief["sources_by_type"]
    assert set(sbt.keys()) == {"bodycam", "interrogation"}
    assert len(sbt["bodycam"]) == 2
    assert len(sbt["interrogation"]) == 1


def test_build_brief_does_not_mutate_input_case_research():
    """Doctrine pin: enrichment must not reach back into the original
    case_research dict and add fields. Defensive against accidental
    aliasing that would corrupt the on-disk P2 case_research file if
    the caller writes it back later."""
    sources = [
        _source(url="https://www.youtube.com/watch?v=YT1", format="video"),
        _source(url="https://agency.gov/file.pdf", format="document"),
    ]
    research = {"defendant": "Test", "sources": sources}
    _ = build_brief(_verdict_minimal(), case_research=research,
                    transcripts=[], weights=None)
    # Original sources unchanged -- no download_command leaked back
    for s in research["sources"]:
        assert "download_command" not in s


# ---------------------------------------------------------------------------
# render_markdown integration
# ---------------------------------------------------------------------------


def test_markdown_renders_yt_dlp_command_beside_youtube_source():
    sources = [_source(url="https://www.youtube.com/watch?v=YT1", format="video",
                       evidence_type="bodycam")]
    md = render_markdown(_build_with_sources(sources))
    assert "## Sources by evidence type" in md
    assert 'yt-dlp "https://www.youtube.com/watch?v=YT1"' in md


def test_markdown_renders_direct_download_for_pdf_source():
    sources = [_source(url="https://agency.gov/x.pdf", format="document",
                       evidence_type="foia_document")]
    md = render_markdown(_build_with_sources(sources))
    assert "Direct download: https://agency.gov/x.pdf" in md


def test_markdown_renders_open_manually_for_webpage_source():
    sources = [_source(url="https://courthouse.gov/case/123",
                       format="webpage", evidence_type="court_docket")]
    md = render_markdown(_build_with_sources(sources))
    assert "Open/review manually: https://courthouse.gov/case/123" in md


def test_markdown_keeps_existing_link_format_alongside_command():
    """The pre-existing `[domain](url)` markdown link format must
    still render -- the command is appended, not a replacement."""
    sources = [_source(url="https://www.youtube.com/watch?v=YT1",
                       source_domain="youtube.com", format="video",
                       evidence_type="bodycam")]
    md = render_markdown(_build_with_sources(sources))
    assert "[youtube.com](https://www.youtube.com/watch?v=YT1)" in md
    assert 'yt-dlp ' in md


def test_markdown_renders_notes_alongside_command():
    """Existing `notes` field rendering must still work."""
    sources = [_source(url="https://x.com/1", evidence_type="bodycam",
                       notes="partial footage, starts mid-encounter",
                       format="video", requires_download=True)]
    md = render_markdown(_build_with_sources(sources))
    assert "partial footage" in md
    assert "yt-dlp " in md


def test_markdown_does_not_modify_verdict():
    """Doctrine pin: rendering download commands must not change the
    verdict shown in the brief header."""
    sources = [_source(url="https://x/1", format="video", evidence_type="bodycam")]
    md = render_markdown(_build_with_sources(sources))
    assert "**Verdict:** PRODUCE" in md


def test_markdown_no_sources_section_when_no_p2_research():
    """Backward compat: a brief with no case_research has no
    Sources section, nothing to render."""
    md = render_markdown(build_brief(
        _verdict_minimal(), case_research=None, transcripts=[], weights=None,
    ))
    assert "## Sources by evidence type" not in md
