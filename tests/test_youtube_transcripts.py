"""Zero-network tests for the YouTube caption / transcript extractor.

The HTTP layer (yt-dlp) is replaced with an injected fake
``ydl_factory`` per test, so nothing in this module ever touches the
network. Production code loads the real ``yt_dlp.YoutubeDL`` lazily
inside ``YouTubeCaptionExtractor.extract``.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from pipeline2_discovery.enrichment.youtube_transcripts import (
    ARTIFACT_TERMS,
    CONFIDENCE_RANK,
    DEFAULT_MAX_VIDEOS,
    DEFAULT_MIN_CONFIDENCE,
    ENGLISH_LANGS,
    SelectedVideo,
    YouTubeCaptionExtractor,
    load_context_lookup,
    score_transcript,
    select_urls_for_extraction,
    vtt_to_plaintext,
)


# ---- selection ------------------------------------------------------


def _row(*, provider="youtube", confidence="high",
         hints=("youtube_metadata",), urls=("https://www.youtube.com/watch?v=abc12345678",),
         titles=("Some Title",), cid="x:1", query="q"):
    return {
        "provider": provider,
        "confidence": confidence,
        "next_actions_hint": list(hints),
        "result_urls": list(urls),
        "result_titles": list(titles),
        "candidate_id": cid,
        "query": query,
    }


def test_select_only_youtube_provider():
    rows = [
        _row(provider="muckrock"),
        _row(provider="youtube"),
    ]
    sel = select_urls_for_extraction(rows)
    assert len(sel) == 1


def test_select_min_confidence_high_drops_lower_rows():
    rows = [
        _row(confidence="low"),
        _row(confidence="medium"),
        _row(confidence="high"),
    ]
    sel = select_urls_for_extraction(rows, min_confidence="high")
    assert len(sel) == 1
    assert sel[0].source_confidence == "high"


def test_select_min_confidence_medium_keeps_medium_and_high():
    rows = [
        _row(confidence="low"),
        _row(confidence="medium"),
        _row(confidence="high"),
    ]
    sel = select_urls_for_extraction(rows, min_confidence="medium")
    assert len(sel) == 2


def test_select_requires_youtube_metadata_hint():
    rows = [
        _row(hints=()),  # no hint
        _row(hints=("youtube_metadata",)),
        _row(hints=("MUCKROCK_PARSE_RELEASED_FILES",)),
    ]
    sel = select_urls_for_extraction(rows)
    assert len(sel) == 1


def test_select_skips_empty_url_lists():
    rows = [
        _row(urls=(), titles=()),
        _row(urls=("https://a",), titles=("t",)),
    ]
    sel = select_urls_for_extraction(rows)
    assert len(sel) == 1


def test_select_caps_at_max_videos():
    rows = [
        _row(
            urls=tuple(f"https://www.youtube.com/watch?v={i:08d}_x" for i in range(20)),
            titles=tuple(f"t{i}" for i in range(20)),
        ),
    ]
    sel = select_urls_for_extraction(rows, max_videos=5)
    assert len(sel) == 5


def test_select_preserves_input_order():
    rows = [
        _row(urls=("https://www.youtube.com/watch?v=aaaaaaaaaaa",), titles=("first",), cid="a"),
        _row(urls=("https://www.youtube.com/watch?v=bbbbbbbbbbb",), titles=("second",), cid="b"),
    ]
    sel = select_urls_for_extraction(rows)
    assert [s.candidate_id for s in sel] == ["a", "b"]


def test_select_preserves_query_and_confidence():
    rows = [_row(query="ryan williams oklahoma city bodycam", confidence="high")]
    sel = select_urls_for_extraction(rows)
    assert sel[0].source_query == "ryan williams oklahoma city bodycam"
    assert sel[0].source_confidence == "high"


# ---- VTT parsing ----------------------------------------------------


def test_vtt_to_plaintext_strips_header_and_timestamps():
    vtt = (
        "WEBVTT\n"
        "\n"
        "1\n"
        "00:00:00.000 --> 00:00:02.500\n"
        "Officer arrives at the scene.\n"
        "\n"
        "2\n"
        "00:00:02.500 --> 00:00:05.000\n"
        "Bodycam footage shows the suspect.\n"
    )
    out = vtt_to_plaintext(vtt)
    assert "WEBVTT" not in out
    assert "00:00" not in out
    assert "Officer arrives at the scene." in out
    assert "Bodycam footage shows the suspect." in out


def test_vtt_to_plaintext_strips_inline_styling_tags():
    vtt = (
        "WEBVTT\n"
        "\n"
        "00:00:00.000 --> 00:00:02.000\n"
        "<c.colorE5E5E5>Hello</c> <00:00:01.000>world\n"
    )
    out = vtt_to_plaintext(vtt)
    assert "<c.colorE5E5E5>" not in out
    assert "<00:00:01.000>" not in out
    assert "Hello world" in out


def test_vtt_to_plaintext_dedupes_consecutive_repeats():
    """YouTube auto-VTT often emits rolling cues that repeat each
    line several times. The parser collapses consecutive duplicates."""
    vtt = (
        "WEBVTT\n"
        "\n"
        "00:00:00.000 --> 00:00:02.000\n"
        "the suspect fled\n"
        "\n"
        "00:00:01.000 --> 00:00:03.000\n"
        "the suspect fled\n"
        "\n"
        "00:00:02.000 --> 00:00:04.000\n"
        "the suspect fled\n"
    )
    out = vtt_to_plaintext(vtt)
    assert out.count("the suspect fled") == 1


def test_vtt_to_plaintext_collapses_blank_runs():
    vtt = (
        "WEBVTT\n"
        "\n"
        "\n"
        "\n"
        "00:00:00.000 --> 00:00:02.000\n"
        "Line one.\n"
        "\n"
        "\n"
        "\n"
        "00:00:02.000 --> 00:00:04.000\n"
        "Line two.\n"
    )
    out = vtt_to_plaintext(vtt)
    # No more than one consecutive blank line
    assert "\n\n\n" not in out


def test_vtt_to_plaintext_empty_input():
    assert vtt_to_plaintext("") == ""


def test_vtt_to_plaintext_strips_note_blocks():
    vtt = (
        "WEBVTT\n"
        "\n"
        "NOTE\n"
        "this is a note\n"
        "\n"
        "00:00:00.000 --> 00:00:02.000\n"
        "Real text.\n"
    )
    out = vtt_to_plaintext(vtt)
    assert "Real text." in out
    assert "this is a note" in out  # note body lines are kept; only the NOTE marker is stripped
    assert "NOTE" not in out.split("\n")[0]  # NOTE marker line removed


# ---- relevance scoring ----------------------------------------------


def test_score_unknown_for_empty_transcript():
    s = score_transcript("", source_query="anything")
    assert s.relevance == "unknown"


def test_score_high_with_full_subject_name_and_artifact():
    transcript = (
        "Today the Oklahoma City police department released bodycam "
        "footage from the officer-involved shooting of ryan keyon williams."
    )
    s = score_transcript(
        transcript,
        source_query="ryan keyon williams oklahoma city police bodycam",
        context={"subject_name": "ryan keyon williams",
                 "agency": "oklahoma city police department",
                 "city": "oklahoma city", "state": "OK"},
    )
    assert s.relevance == "high"
    assert "full_subject_name" in s.matched_terms


def test_score_high_with_last_name_and_artifact():
    transcript = (
        "The suspect williams led police on a high-speed chase before "
        "the crash."
    )
    s = score_transcript(
        transcript,
        source_query="ryan keyon williams oklahoma city police pursuit",
        context={"subject_name": "ryan keyon williams",
                 "agency": "oklahoma city police department",
                 "city": "oklahoma city", "state": "OK"},
    )
    assert s.relevance == "high"


def test_score_high_with_agency_plus_city_plus_artifact():
    transcript = (
        "Oklahoma City police department officials at a critical incident "
        "briefing released body-worn camera footage today."
    )
    s = score_transcript(
        transcript,
        source_query="oklahoma city police critical incident",
        context={"subject_name": "ryan keyon williams",
                 "agency": "oklahoma city police department",
                 "city": "oklahoma city", "state": "OK"},
    )
    assert s.relevance == "high"


def test_score_medium_with_agency_anchor_plus_artifact():
    transcript = (
        "Kentucky State Police investigated an officer-involved shooting "
        "in a different town this week."
    )
    s = score_transcript(
        transcript,
        source_query="michael mike c beaver kentucky state police elizabethtown",
        context={"subject_name": "michael mike c beaver",
                 "agency": "kentucky state police",
                 "city": "elizabethtown", "state": "KY"},
    )
    # Has agency token (kentucky) but no subject (no "beaver" / "michael"),
    # no city ("elizabethtown" not in transcript), but has artifact term.
    assert s.relevance == "medium"


def test_score_low_for_unrelated_transcript():
    transcript = "This video is about cooking with sauces and spices."
    s = score_transcript(
        transcript,
        source_query="ryan keyon williams oklahoma city police bodycam",
        context={"subject_name": "ryan keyon williams",
                 "agency": "oklahoma city police department",
                 "city": "oklahoma city", "state": "OK"},
    )
    assert s.relevance == "low"


def test_score_low_for_artifact_only_no_anchor():
    transcript = "Bodycam footage from a routine traffic stop."
    s = score_transcript(
        transcript,
        source_query="ryan keyon williams oklahoma city police",
        context={"subject_name": "ryan keyon williams",
                 "agency": "oklahoma city police department",
                 "city": "oklahoma city", "state": "OK"},
    )
    assert s.relevance == "low"  # artifact-only is low per spec


def test_score_query_token_fallback_when_no_context():
    """Without a context dict, scoring falls back to query-token
    matching. A transcript containing distinctive query tokens
    + artifact reaches medium."""
    transcript = "Williams was named in connection with the bodycam release."
    s = score_transcript(
        transcript,
        source_query="ryan keyon williams oklahoma city bodycam",
        context=None,
    )
    # Query-token-only path: distinctive_hits + artifact → medium
    assert s.relevance in ("medium", "high")


def test_score_state_abbr_anchor_is_case_sensitive():
    """The case-sensitive standalone-token rule from the upstream
    YouTube provider carries through to transcript scoring."""
    # Lowercase 'co' inside 'company' must not anchor.
    transcript = "the company released a statement about the bodycam"
    s = score_transcript(
        transcript,
        source_query="some query",
        context={"subject_name": "joe gold", "agency": "longmont police",
                 "city": "longmont", "state": "CO"},
    )
    # No real anchor (subject not in text, city not in text, agency token
    # 'longmont' not in text); 'CO' not standalone in raw text.
    # Just artifact 'bodycam' → low (artifact-only path).
    assert s.relevance == "low"


# ---- caption extractor (yt-dlp wrapper) -----------------------------


class _FakeYdlSuccess:
    """Mimics yt_dlp.YoutubeDL — captures opts and writes a canned
    VTT file to the configured outtmpl path on download()."""

    instances: List["_FakeYdlSuccess"] = []
    captured_opts: List[Dict[str, Any]] = []

    def __init__(self, opts):
        self.opts = opts
        type(self).instances.append(self)
        type(self).captured_opts.append(dict(opts))

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def download(self, urls):
        # Resolve outtmpl — it has a %(id)s placeholder.
        outtmpl = self.opts.get("outtmpl", "")
        # Best-effort id from URL
        from pipeline2_discovery.enrichment.youtube_transcripts import (
            _video_id_from_url,
        )
        for url in urls:
            vid = _video_id_from_url(url)
            base = outtmpl.replace("%(id)s", vid)
            vtt_path = Path(base.replace("%(ext)s", "vtt"))
            vtt_path.parent.mkdir(parents=True, exist_ok=True)
            vtt_path.write_text(
                "WEBVTT\n\n"
                "00:00:00.000 --> 00:00:02.000\n"
                "Test transcript line one.\n"
                "\n"
                "00:00:02.000 --> 00:00:04.000\n"
                "Test transcript line two.\n",
                encoding="utf-8",
            )


class _FakeYdlNoCaptions:
    """Fake yt-dlp that succeeds at download() but writes no VTT."""

    def __init__(self, opts):
        self.opts = opts

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def download(self, urls):
        # Don't write any VTT — simulates 'no captions available'.
        return None


class _FakeYdlError:
    def __init__(self, opts):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def download(self, urls):
        raise OSError("network unreachable")


def test_extractor_passes_caption_only_options(tmp_path):
    _FakeYdlSuccess.instances.clear()
    _FakeYdlSuccess.captured_opts.clear()
    e = YouTubeCaptionExtractor(
        ydl_factory=_FakeYdlSuccess, sleeper=lambda _: None,
        rate_limit_seconds=0,
    )
    e.extract(
        "https://www.youtube.com/watch?v=abcdef12345",
        output_dir=tmp_path,
    )
    assert len(_FakeYdlSuccess.captured_opts) == 1
    opts = _FakeYdlSuccess.captured_opts[0]
    assert opts["skip_download"] is True
    assert opts["writesubtitles"] is True
    assert opts["writeautomaticsub"] is True
    assert opts["subtitlesformat"] == "vtt"
    assert opts["noplaylist"] is True
    assert opts["quiet"] is True
    # No video / audio download keys
    assert "format" not in opts
    assert "extractaudio" not in opts
    assert "writeinfojson" not in opts
    # English language preference
    assert any(lang.startswith("en") for lang in opts["subtitleslangs"])


def test_extractor_writes_vtt_to_per_video_dir(tmp_path):
    e = YouTubeCaptionExtractor(
        ydl_factory=_FakeYdlSuccess, sleeper=lambda _: None,
        rate_limit_seconds=0,
    )
    r = e.extract(
        "https://www.youtube.com/watch?v=vid12345abc",
        output_dir=tmp_path,
    )
    assert r.status == "downloaded"
    assert r.video_id == "vid12345abc"
    assert r.vtt_path is not None
    assert Path(r.vtt_path).exists()
    assert "vid12345abc" in r.vtt_path


def test_extractor_no_captions_when_yt_dlp_writes_no_vtt(tmp_path):
    e = YouTubeCaptionExtractor(
        ydl_factory=_FakeYdlNoCaptions, sleeper=lambda _: None,
        rate_limit_seconds=0,
    )
    r = e.extract(
        "https://www.youtube.com/watch?v=novttvideo1",
        output_dir=tmp_path,
    )
    assert r.status == "no_captions_found"
    assert r.vtt_path is None


def test_extractor_captures_yt_dlp_exception(tmp_path):
    e = YouTubeCaptionExtractor(
        ydl_factory=_FakeYdlError, sleeper=lambda _: None,
        rate_limit_seconds=0,
    )
    r = e.extract(
        "https://www.youtube.com/watch?v=errvid12345",
        output_dir=tmp_path,
    )
    assert r.status == "failed"
    assert "OSError" in (r.error or "")


def test_extractor_calls_rate_limit_sleeper(tmp_path):
    sleeps = []
    e = YouTubeCaptionExtractor(
        ydl_factory=_FakeYdlSuccess,
        sleeper=lambda d: sleeps.append(d),
        rate_limit_seconds=1.5,
    )
    e.extract(
        "https://www.youtube.com/watch?v=ratevid12345",
        output_dir=tmp_path,
    )
    assert sleeps == [1.5]


def test_extractor_no_video_or_audio_methods_referenced():
    """Static check: the module never references download verbs that
    would imply video / audio download. Pinned by reading the source."""
    import pipeline2_discovery.enrichment.youtube_transcripts as mod
    src = open(mod.__file__, encoding="utf-8").read()
    # No format selection (would imply video stream)
    assert '"format":' not in src
    assert "'format':" not in src
    # No audio extraction toggle
    assert "extractaudio" not in src
    # No writeinfojson which would dump full metadata
    assert "writeinfojson" not in src


# ---- context lookup ------------------------------------------------


def test_load_context_lookup_handles_missing_file(tmp_path):
    out = load_context_lookup(tmp_path / "no_such.json")
    assert out == {}


def test_load_context_lookup_parses_search_tasks(tmp_path):
    p = tmp_path / "tasks.json"
    p.write_text(json.dumps({
        "tasks": [
            {"candidate_id": "x:1", "context": {"agency": "Foo PD", "subject_name": "alice", "city": "phoenix", "state": "AZ"}},
            {"candidate_id": "x:1", "context": {"agency": "Foo PD"}},  # duplicate cid; first wins
            {"candidate_id": "x:2", "context": {"city": "tucson"}},
        ],
    }), encoding="utf-8")
    out = load_context_lookup(p)
    assert out["x:1"]["subject_name"] == "alice"
    assert out["x:2"]["city"] == "tucson"


# ---- end-to-end (extractor + parser + scorer) ----------------------


class _FakeYdlPartialThenError:
    """Simulates the live MZGgQC0JiuM behaviour: yt-dlp writes the
    primary English VTT to disk, then raises on a secondary
    translation language. The extractor must recover the VTT and
    return ``status="downloaded"`` with a partial-error note."""

    def __init__(self, opts):
        self.opts = opts

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def download(self, urls):
        from pipeline2_discovery.enrichment.youtube_transcripts import (
            _video_id_from_url,
        )
        outtmpl = self.opts.get("outtmpl", "")
        for url in urls:
            vid = _video_id_from_url(url)
            base = outtmpl.replace("%(id)s", vid)
            # Write the primary English VTT first…
            en_path = Path(base.replace(".%(ext)s", ".en.vtt"))
            en_path.parent.mkdir(parents=True, exist_ok=True)
            en_path.write_text(
                "WEBVTT\n\n"
                "00:00:00.000 --> 00:00:05.000\n"
                "An innocent driver was hit and killed by a police "
                "officer in Miami Beach during a pursuit.\n"
                "00:00:05.000 --> 00:00:10.000\n"
                "Yvonne Reyes was killed when police were in hot "
                "pursuit of a car thief.\n",
                encoding="utf-8",
            )
            # …then raise to simulate the secondary 429.
        raise OSError("HTTP Error 429: Too Many Requests")


def test_extractor_recovers_when_vtt_written_before_yt_dlp_raises(tmp_path):
    """yt-dlp wrote .en.vtt then raised on a secondary translation;
    the extractor must mark the run as downloaded, not failed."""
    e = YouTubeCaptionExtractor(
        ydl_factory=_FakeYdlPartialThenError, sleeper=lambda _: None,
        rate_limit_seconds=0,
    )
    r = e.extract(
        "https://www.youtube.com/watch?v=partialvid01",
        output_dir=tmp_path,
    )
    assert r.status == "downloaded"
    assert r.vtt_path is not None
    assert Path(r.vtt_path).exists()
    # Notes should record the partial error + recovery.
    notes_str = " ".join(r.notes)
    assert "partial_yt_dlp_error" in notes_str
    assert "recovered_from_existing_vtt=true" in notes_str
    assert "HTTP Error 429" in notes_str


def test_extractor_recovered_vtt_parses_and_scores_high(tmp_path):
    """End-to-end: a recovered VTT containing subject + pursuit
    terms parses into a transcript that the scorer flags as
    relevance=high. This validates the full
    failure-recovery → parse → score pipeline."""
    e = YouTubeCaptionExtractor(
        ydl_factory=_FakeYdlPartialThenError, sleeper=lambda _: None,
        rate_limit_seconds=0,
    )
    r = e.extract(
        "https://www.youtube.com/watch?v=reyesrecov01",
        output_dir=tmp_path,
    )
    assert r.status == "downloaded"

    text = vtt_to_plaintext(Path(r.vtt_path).read_text(encoding="utf-8"))
    s = score_transcript(
        text,
        source_query="ivonne reyes miami beach police pursuit",
        context={
            "subject_name": "ivonne reyes",
            "agency": "miami beach police department",
            "city": "miami beach",
            "state": "FL",
        },
    )
    assert s.relevance == "high"
    # Transcript scorer's last_name path matches "reyes"
    assert any("last_name" in m or "full_subject_name" in m for m in s.matched_terms)


class _FakeYdlNoFileThenError:
    """yt-dlp raises and writes NOTHING to disk — the original
    failure case. Extractor should still return failed."""

    def __init__(self, opts):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def download(self, urls):
        raise OSError("connection refused")


def test_extractor_still_failed_when_exception_and_no_vtt(tmp_path):
    e = YouTubeCaptionExtractor(
        ydl_factory=_FakeYdlNoFileThenError, sleeper=lambda _: None,
        rate_limit_seconds=0,
    )
    r = e.extract(
        "https://www.youtube.com/watch?v=novttafterr1",
        output_dir=tmp_path,
    )
    assert r.status == "failed"
    assert r.vtt_path is None
    assert "OSError" in (r.error or "")


def test_select_best_vtt_prefers_en_over_en_orig(tmp_path):
    from pipeline2_discovery.enrichment.youtube_transcripts import (
        _select_best_vtt,
    )
    vid = "vididabc1234"
    video_dir = tmp_path / vid
    video_dir.mkdir(parents=True, exist_ok=True)
    en = video_dir / f"{vid}.en.vtt"
    en_orig = video_dir / f"{vid}.en-orig.vtt"
    en.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nA\n", encoding="utf-8")
    en_orig.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nB\n", encoding="utf-8")
    chosen = _select_best_vtt(video_dir, vid)
    assert chosen is not None
    assert chosen.name == f"{vid}.en.vtt"


def test_select_best_vtt_prefers_english_over_other_languages(tmp_path):
    from pipeline2_discovery.enrichment.youtube_transcripts import (
        _select_best_vtt,
    )
    vid = "vididxyz9876"
    video_dir = tmp_path / vid
    video_dir.mkdir(parents=True, exist_ok=True)
    fr = video_dir / f"{vid}.fr.vtt"
    en_us = video_dir / f"{vid}.en-US.vtt"
    fr.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nFR\n", encoding="utf-8")
    en_us.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nEN-US\n", encoding="utf-8")
    chosen = _select_best_vtt(video_dir, vid)
    assert chosen is not None
    # en-US is in the English bucket; fr is not. en-US wins.
    assert chosen.name == f"{vid}.en-US.vtt"


def test_select_best_vtt_falls_back_to_non_english_when_no_english(tmp_path):
    from pipeline2_discovery.enrichment.youtube_transcripts import (
        _select_best_vtt,
    )
    vid = "vididonlyfr1"
    video_dir = tmp_path / vid
    video_dir.mkdir(parents=True, exist_ok=True)
    fr = video_dir / f"{vid}.fr.vtt"
    fr.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nFR\n", encoding="utf-8")
    chosen = _select_best_vtt(video_dir, vid)
    assert chosen is not None
    assert chosen.name == f"{vid}.fr.vtt"


def test_select_best_vtt_skips_zero_byte_files(tmp_path):
    from pipeline2_discovery.enrichment.youtube_transcripts import (
        _select_best_vtt,
    )
    vid = "videmptyfile"
    video_dir = tmp_path / vid
    video_dir.mkdir(parents=True, exist_ok=True)
    empty_en = video_dir / f"{vid}.en.vtt"
    en_orig = video_dir / f"{vid}.en-orig.vtt"
    empty_en.write_text("", encoding="utf-8")  # zero bytes
    en_orig.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nA\n", encoding="utf-8")
    chosen = _select_best_vtt(video_dir, vid)
    # The empty .en.vtt is filtered out; .en-orig.vtt is selected.
    assert chosen is not None
    assert chosen.name == f"{vid}.en-orig.vtt"


def test_select_best_vtt_returns_none_when_directory_empty(tmp_path):
    from pipeline2_discovery.enrichment.youtube_transcripts import (
        _select_best_vtt,
    )
    vid = "vidempydir01"
    video_dir = tmp_path / vid
    video_dir.mkdir(parents=True, exist_ok=True)
    assert _select_best_vtt(video_dir, vid) is None


def test_extractor_to_scorer_flow(tmp_path):
    """End-to-end: extract VTT (canned via fake yt-dlp), parse to
    plaintext, score against a context. Validates that the moving
    parts compose correctly."""

    class _FakeYdlScored:
        def __init__(self, opts):
            self.opts = opts

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def download(self, urls):
            from pipeline2_discovery.enrichment.youtube_transcripts import (
                _video_id_from_url,
            )
            outtmpl = self.opts.get("outtmpl", "")
            for url in urls:
                vid = _video_id_from_url(url)
                base = outtmpl.replace("%(id)s", vid)
                p = Path(base.replace("%(ext)s", "vtt"))
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(
                    "WEBVTT\n\n"
                    "00:00:00.000 --> 00:00:05.000\n"
                    "Today the oklahoma city police department released "
                    "body-worn camera footage from the officer-involved "
                    "shooting of ryan keyon williams.\n",
                    encoding="utf-8",
                )

    e = YouTubeCaptionExtractor(
        ydl_factory=_FakeYdlScored, sleeper=lambda _: None,
        rate_limit_seconds=0,
    )
    r = e.extract(
        "https://www.youtube.com/watch?v=okcvid12345",
        output_dir=tmp_path,
    )
    assert r.status == "downloaded"

    text = vtt_to_plaintext(Path(r.vtt_path).read_text(encoding="utf-8"))
    assert "ryan keyon williams" in text

    s = score_transcript(
        text,
        source_query="ryan keyon williams oklahoma city police bodycam",
        context={"subject_name": "ryan keyon williams",
                 "agency": "oklahoma city police department",
                 "city": "oklahoma city", "state": "OK"},
    )
    assert s.relevance == "high"
    assert "full_subject_name" in s.matched_terms
