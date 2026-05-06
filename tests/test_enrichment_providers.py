"""Zero-network tests for the provider interface + Mock + YouTube providers."""
from __future__ import annotations

import pytest

from pipeline2_discovery.enrichment import (
    DEFERRED_PROVIDERS,
    KNOWN_PROVIDERS,
    EnrichmentTask,
    MockProvider,
    YtDlpYouTubeSearchClient,
    TaskStatus,
    get_provider,
)


def _t(query="John Doe Phoenix Police bodycam", task_type="youtube_query",
       agency="Phoenix Police Department", subject_name="john doe",
       city="phoenix", state="AZ"):
    return EnrichmentTask(
        candidate_id="x:1",
        grade="A",
        task_type=task_type,
        query=query,
        context={
            "agency": agency,
            "subject_name": subject_name,
            "city": city,
            "state": state,
        },
    )


# ---- get_provider factory ------------------------------------------


def test_get_provider_returns_mock_for_mock_name():
    p = get_provider("mock")
    assert p.name == "mock"
    assert isinstance(p, MockProvider)


@pytest.mark.parametrize("name", DEFERRED_PROVIDERS)
def test_get_provider_raises_not_implemented_for_deferred_names(name):
    with pytest.raises(NotImplementedError, match="follow-up PR"):
        get_provider(name)


def test_get_provider_raises_value_error_for_unknown_name():
    with pytest.raises(ValueError, match="unknown provider"):
        get_provider("not-a-real-provider")


def test_known_and_deferred_providers_constants_are_disjoint():
    assert set(KNOWN_PROVIDERS).isdisjoint(set(DEFERRED_PROVIDERS))


def test_known_providers_includes_mock():
    assert "mock" in KNOWN_PROVIDERS


# ---- MockProvider ---------------------------------------------------


def test_mock_provider_returns_completed_status():
    r = MockProvider().execute(_t())
    assert r.status == TaskStatus.COMPLETED
    assert r.provider == "mock"


def test_mock_provider_emits_one_synthetic_url_per_task():
    r = MockProvider().execute(_t())
    assert len(r.result_urls) == 1
    assert r.result_urls[0].startswith("https://mock.example.com/")
    assert "youtube_query" in r.result_urls[0]
    assert len(r.result_titles) == 1
    assert "[mock]" in r.result_titles[0]


def test_mock_provider_is_deterministic_across_runs():
    """Same task → same URL. Critical for test stability and so
    operators can re-run a mock smoke without spurious diffs."""
    t = _t(query="Christopher Vang Fresno PD bodycam")
    r1 = MockProvider().execute(t)
    r2 = MockProvider().execute(t)
    assert r1.result_urls == r2.result_urls
    assert r1.result_titles == r2.result_titles


def test_mock_provider_url_varies_with_task_type():
    """Same query, different task_type → different URL. Lets the
    mock stand in for multi-lane provider routing."""
    yt = MockProvider().execute(_t(query="q", task_type="youtube_query"))
    mr = MockProvider().execute(_t(query="q", task_type="muckrock_query"))
    assert yt.result_urls != mr.result_urls


def test_mock_provider_marks_results_as_low_confidence():
    """Synthetic results MUST never be passed off as high-confidence;
    the mock's confidence is hardcoded to 'low' so any downstream
    code that treats them as real fails its grade gate."""
    r = MockProvider().execute(_t())
    assert r.confidence == "low"


def test_mock_provider_carries_explicit_synthetic_warning_in_notes():
    r = MockProvider().execute(_t())
    assert any("synthetic" in n.lower() for n in r.notes)


# ---- YtDlpYouTubeSearchClient (zero-network) ------------------------


class _FakeYdl:
    """Minimal yt-dlp context-manager stub that returns canned entries.

    Two of the three entries carry the default _t() context anchor
    (city = "phoenix") so existing plumbing tests (URL fallback,
    id-less entry skipping) aren't masked by the relevance gate."""

    def __init__(self, opts):
        self.opts = opts

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def extract_info(self, url, *, download):
        assert download is False
        return {
            "entries": [
                {
                    "id": "abc123",
                    "title": "Bodycam Arrest Phoenix",
                    "url": "https://www.youtube.com/watch?v=abc123",
                },
                {
                    "id": "def456",
                    "title": "Phoenix PD Officer Update",
                    "url": "",
                },
                {"id": "", "title": "No-ID entry"},  # should be skipped
            ]
        }


class _FakeYdlEmpty:
    def __init__(self, opts):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def extract_info(self, url, *, download):
        return {"entries": []}


class _FakeYdlError:
    def __init__(self, opts):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def extract_info(self, url, *, download):
        raise OSError("network unreachable")


def test_youtube_provider_name():
    assert YtDlpYouTubeSearchClient().name == "youtube"


def test_get_provider_returns_youtube_client():
    p = get_provider("youtube")
    assert isinstance(p, YtDlpYouTubeSearchClient)
    assert p.name == "youtube"


def test_known_providers_includes_youtube():
    assert "youtube" in KNOWN_PROVIDERS


def test_youtube_not_in_deferred_providers():
    assert "youtube" not in DEFERRED_PROVIDERS


def test_youtube_provider_completed_status_with_results():
    p = YtDlpYouTubeSearchClient(ydl_cls=_FakeYdl)
    r = p.execute(_t())
    assert r.status == TaskStatus.COMPLETED
    assert r.provider == "youtube"


def test_youtube_provider_returns_urls_and_titles():
    p = YtDlpYouTubeSearchClient(ydl_cls=_FakeYdl)
    r = p.execute(_t())
    # Both anchored entries are kept, no-id entry is skipped.
    # Order is by relevance score, not yt-dlp order, so we assert
    # set membership rather than indexed equality.
    assert len(r.result_urls) == 2
    assert len(r.result_titles) == 2
    assert "https://www.youtube.com/watch?v=abc123" in r.result_urls
    assert "https://www.youtube.com/watch?v=def456" in r.result_urls
    assert "Bodycam Arrest Phoenix" in r.result_titles
    assert "Phoenix PD Officer Update" in r.result_titles


def test_youtube_provider_skips_entries_without_id():
    p = YtDlpYouTubeSearchClient(ydl_cls=_FakeYdl)
    r = p.execute(_t())
    for url in r.result_urls:
        assert url  # no empty strings


def test_youtube_provider_empty_results_still_completed():
    p = YtDlpYouTubeSearchClient(ydl_cls=_FakeYdlEmpty)
    r = p.execute(_t())
    assert r.status == TaskStatus.COMPLETED
    assert r.result_urls == []
    assert r.result_titles == []


def test_youtube_provider_empty_results_have_low_confidence():
    p = YtDlpYouTubeSearchClient(ydl_cls=_FakeYdlEmpty)
    r = p.execute(_t())
    assert r.confidence == "low"
    assert r.next_actions_hint == []


def test_youtube_provider_with_results_has_medium_confidence_and_hint():
    p = YtDlpYouTubeSearchClient(ydl_cls=_FakeYdl)
    r = p.execute(_t())
    assert r.confidence == "medium"
    assert "youtube_metadata" in r.next_actions_hint


def test_youtube_provider_network_error_returns_failed():
    p = YtDlpYouTubeSearchClient(ydl_cls=_FakeYdlError)
    r = p.execute(_t())
    assert r.status == TaskStatus.FAILED
    assert r.provider == "youtube"
    assert "OSError" in (r.error or "")


def test_youtube_provider_max_results_clamped():
    # max_results is clamped to [1, 20]
    p_low = YtDlpYouTubeSearchClient(max_results=0, ydl_cls=_FakeYdl)
    assert p_low._max_results == 1
    p_high = YtDlpYouTubeSearchClient(max_results=999, ydl_cls=_FakeYdl)
    assert p_high._max_results == 20


def test_youtube_provider_respects_max_results_cap():
    # _FakeYdl returns 3 entries (one has no id → 2 valid); with max_results=1
    # the cap should trim to 1
    p = YtDlpYouTubeSearchClient(max_results=1, ydl_cls=_FakeYdl)
    r = p.execute(_t())
    assert len(r.result_urls) <= 1


def test_youtube_provider_uses_metadata_only_opts():
    """Verify the provider passes safe metadata-only options to yt-dlp:
    no media download, no caption download, no disk writes."""
    captured = {}

    class _CapturingYdl:
        def __init__(self, opts):
            captured["opts"] = opts

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def extract_info(self, url, *, download):
            captured["url"] = url
            captured["download"] = download
            return {"entries": []}

    p = YtDlpYouTubeSearchClient(ydl_cls=_CapturingYdl)
    p.execute(_t(query="some query"))

    opts = captured["opts"]
    assert opts.get("skip_download") is True
    assert opts.get("extract_flat") is True
    assert opts.get("noplaylist") is True
    assert opts.get("quiet") is True
    # No subtitle/caption keys should be enabled
    assert not opts.get("writesubtitles")
    assert not opts.get("writeautomaticsub")
    assert not opts.get("allsubtitles")
    # Should always pass download=False to extract_info
    assert captured["download"] is False
    # Search URL must use the ytsearchN: prefix
    assert captured["url"].startswith("ytsearch")
    assert "some query" in captured["url"]


# ---- relevance gate (zero-network) ----------------------------------


def _ctx(*, agency="Longmont Police Services",
         subject_name="joe william gold", city="longmont", state="CO"):
    return {
        "agency": agency,
        "subject_name": subject_name,
        "city": city,
        "state": state,
    }


def _fake_ydl_returning(entries):
    """Build a one-shot fake YoutubeDL class that returns the given
    yt-dlp entries from extract_info."""
    class _FakeYdlDyn:
        def __init__(self, opts):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def extract_info(self, url, *, download):
            return {"entries": list(entries)}

    return _FakeYdlDyn


def _entry(*, vid, title, url=None, uploader="", description=""):
    return {
        "id": vid,
        "title": title,
        "url": url if url is not None else f"https://www.youtube.com/watch?v={vid}",
        "uploader": uploader,
        "description": description,
    }


def _exec(entries, **ctx_kwargs):
    """Run the YouTube provider against the given canned entries
    and a Joe-William-Gold-shaped context (overridable)."""
    p = YtDlpYouTubeSearchClient(ydl_cls=_fake_ydl_returning(entries))
    task = EnrichmentTask(
        candidate_id="x:1",
        grade="A",
        task_type="youtube_query",
        query="joe william gold longmont police bodycam",
        context=_ctx(**ctx_kwargs),
    )
    return p.execute(task)


def test_relevance_keeps_subject_last_name_match():
    r = _exec([
        _entry(vid="v1", title="Joe Gold Longmont police pursuit footage"),
    ])
    assert r.status == TaskStatus.COMPLETED
    assert "https://www.youtube.com/watch?v=v1" in r.result_urls


def test_relevance_keeps_full_subject_name_match():
    r = _exec([
        _entry(vid="v1", title="Bodycam: Joe William Gold arrested"),
    ])
    assert "https://www.youtube.com/watch?v=v1" in r.result_urls


def test_relevance_keeps_agency_token_match_in_uploader():
    r = _exec([
        _entry(
            vid="v1", title="Officer-involved shooting briefing",
            uploader="Longmont Police Services Official",
        ),
    ])
    assert r.result_urls == ["https://www.youtube.com/watch?v=v1"]


def test_relevance_keeps_city_match():
    r = _exec([
        _entry(vid="v1", title="Longmont incident — community update"),
    ])
    assert r.result_urls == ["https://www.youtube.com/watch?v=v1"]


def test_relevance_keeps_state_match():
    r = _exec([
        _entry(vid="v1", title="Colorado police pursuit footage"),
    ])
    assert r.result_urls == ["https://www.youtube.com/watch?v=v1"]


def test_relevance_drops_bodycam_only_with_no_anchor():
    """A generic 'bodycam' result with no subject / agency / city /
    state anchor must be dropped — bodycam is supporting, not anchor."""
    r = _exec([
        _entry(vid="v1", title="Top 10 Bodycam Moments of the Year"),
    ])
    assert r.result_urls == []
    assert r.status == TaskStatus.COMPLETED


def test_relevance_drops_generic_unrelated_news():
    r = _exec([
        _entry(vid="v1", title="Three officers resign from Centralia police"),
        _entry(vid="v2", title="Capitol Police officer lies in honor"),
        _entry(vid="v3", title="Cincinnati Police officer's racist outburst caught on camera"),
    ])
    assert r.result_urls == []
    assert r.confidence == "low"
    assert r.next_actions_hint == []


def test_relevance_filters_mixed_to_only_relevant():
    r = _exec([
        _entry(vid="v_keep", title="Longmont Police bodycam: Joe Gold pursuit"),
        _entry(vid="v_drop1", title="Three officers resign from Centralia police"),
        _entry(vid="v_drop2", title="Random Florida bodycam compilation"),
    ])
    assert r.result_urls == ["https://www.youtube.com/watch?v=v_keep"]


def test_relevance_empty_after_filtering_returns_low_with_no_hint():
    r = _exec([
        _entry(vid="v1", title="Three officers resign from Centralia police"),
    ])
    assert r.status == TaskStatus.COMPLETED
    assert r.result_urls == []
    assert r.confidence == "low"
    assert r.next_actions_hint == []
    assert any("filtered as irrelevant" in n for n in r.notes)


def test_relevance_notes_include_raw_filtered_dropped_counts():
    r = _exec([
        _entry(vid="v_keep", title="Longmont Police bodycam: Joe Gold pursuit"),
        _entry(vid="v_drop1", title="Centralia police resign"),
        _entry(vid="v_drop2", title="Capitol Police memorial"),
    ])
    notes_str = " ".join(r.notes)
    assert "raw_result_count=3" in notes_str
    assert "filtered_result_count=1" in notes_str
    assert "dropped_irrelevant_count=2" in notes_str


def test_relevance_notes_include_dropped_title_examples():
    r = _exec([
        _entry(vid="v_drop1", title="Centralia police resign"),
        _entry(vid="v_drop2", title="Florida clerk armed robbery"),
    ])
    drop_notes = [n for n in r.notes if n.startswith("dropped ")]
    assert len(drop_notes) >= 1
    assert any("Centralia" in n or "Florida" in n for n in drop_notes)


def test_relevance_official_channel_alone_is_medium():
    """Tightened post-PR #41: an official agency channel alone (with
    a supporting term but no subject anchor and no date anchor) is
    medium, NOT high. The transcript validation smoke proved the
    OKCPD-style "we have an agency briefing, but it's about a
    different case" pattern was being over-labelled high."""
    r = _exec([
        _entry(
            vid="v1",
            title="Critical incident briefing — Longmont",
            uploader="City of Longmont Police Department Official",
        ),
    ])
    assert r.confidence == "medium"
    assert r.next_actions_hint == ["youtube_metadata"]


def test_relevance_subject_plus_official_channel_yields_high_confidence():
    """Path B from the new rubric: subject anchor + official agency
    channel reaches high. Same official-channel input as the
    medium test above, but with the subject's last name in title."""
    r = _exec([
        _entry(
            vid="v1",
            title="Joe Gold critical incident briefing — Longmont",
            uploader="City of Longmont Police Department Official",
        ),
    ])
    assert r.confidence == "high"


def test_relevance_subject_plus_agency_yields_high_confidence():
    r = _exec([
        _entry(
            vid="v1",
            title="Joe William Gold — Longmont Police Services bodycam",
        ),
    ])
    assert r.confidence == "high"


def test_relevance_anchor_plus_supporting_yields_medium_confidence():
    r = _exec([
        _entry(vid="v1", title="Longmont police bodycam release"),
    ])
    # has_agency (longmont token) + city + bodycam → medium
    assert r.confidence == "medium"


def test_relevance_state_only_anchor_yields_low_confidence():
    r = _exec([
        _entry(vid="v1", title="Colorado state highway patrol report"),
    ])
    # state-only anchor, no supporting domain term → low
    assert r.confidence == "low"


def test_relevance_state_abbr_must_be_standalone_token():
    """Bare 'CO' as a 2-letter token counts; 'co' embedded in 'company'
    or 'cops' must NOT trigger a state anchor."""
    r = _exec([
        _entry(vid="v1", title="Some random company posts video about cops"),
    ])
    # Should NOT anchor on 'co' substring
    assert r.result_urls == []


def test_relevance_state_abbr_standalone_is_anchor():
    r = _exec([
        _entry(vid="v1", title="Police report from Boulder CO"),
    ])
    assert r.result_urls == ["https://www.youtube.com/watch?v=v1"]


def test_relevance_ranks_higher_score_first():
    r = _exec([
        # weak: state-only
        _entry(vid="weak", title="Colorado highway patrol notes"),
        # strong: subject + agency + bodycam
        _entry(vid="strong", title="Joe Gold Longmont Police bodycam release"),
    ])
    # Both kept; strong should be ranked first
    assert r.result_urls[0] == "https://www.youtube.com/watch?v=strong"


def test_relevance_uploader_anchor_alone_is_enough():
    r = _exec([
        _entry(
            vid="v1", title="Body-worn camera footage — June",
            uploader="Longmont Police Services",
        ),
    ])
    assert r.result_urls == ["https://www.youtube.com/watch?v=v1"]


def test_relevance_description_anchor_used():
    r = _exec([
        _entry(
            vid="v1", title="Police bodycam release",
            description="Released by the Longmont Police Services on May 1.",
        ),
    ])
    assert r.result_urls == ["https://www.youtube.com/watch?v=v1"]


def test_relevance_kept_note_includes_score_and_anchors():
    r = _exec([
        _entry(vid="v1", title="Joe Gold Longmont bodycam"),
    ])
    kept_notes = [n for n in r.notes if n.startswith("kept ")]
    assert len(kept_notes) == 1
    assert "score=" in kept_notes[0]
    assert "anchors=" in kept_notes[0]


# ---- regression: failed yt-dlp run still returns no result_urls -----


def test_relevance_does_not_run_on_failed_yt_dlp():
    """If yt-dlp itself raises, relevance gate is moot — status
    must be FAILED with empty result_urls and the error captured."""
    class _ErrYdl:
        def __init__(self, opts): pass
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def extract_info(self, url, *, download):
            raise OSError("boom")

    p = YtDlpYouTubeSearchClient(ydl_cls=_ErrYdl)
    task = EnrichmentTask(
        candidate_id="x:1", grade="A", task_type="youtube_query",
        query="q", context=_ctx(),
    )
    r = p.execute(task)
    assert r.status == TaskStatus.FAILED
    assert r.result_urls == []
    assert "OSError" in (r.error or "")


# ---- confidence tightening (post-PR #41) ----------------------------


def _exec_with_date(entries, *, incident_date="2020-12-26", **ctx_kwargs):
    """Variant of _exec that supplies an incident_date in context so
    the date_anchor path can fire."""
    p = YtDlpYouTubeSearchClient(ydl_cls=_fake_ydl_returning(entries))
    ctx = _ctx(**ctx_kwargs)
    ctx["incident_date"] = incident_date
    task = EnrichmentTask(
        candidate_id="x:1",
        grade="A",
        task_type="youtube_query",
        query="joe william gold longmont police bodycam",
        context=ctx,
    )
    return p.execute(task)


def test_relevance_official_channel_plus_supporting_no_subject_is_medium():
    """OKCPD-shaped fixture: official agency uploader + bodycam term,
    no subject in title, no date anchor → medium (was high in v1).
    Uses 'bodycam' which is in BODYCAM_TERMS so has_supporting fires."""
    r = _exec([
        _entry(
            vid="v1",
            title="OKCPD bodycam release - critical incident briefing",
            uploader="Oklahoma City Police Department Official",
        ),
    ], agency="oklahoma city police department", subject_name="ryan keyon williams",
       city="oklahoma city", state="OK")
    assert r.confidence == "medium"


def test_relevance_official_channel_plus_subject_remains_high():
    """OKCPD-shaped fixture WITH subject name in title → high
    (Path B from the new rubric)."""
    r = _exec([
        _entry(
            vid="v1",
            title="OKCPD bodycam release - williams critical incident",
            uploader="Oklahoma City Police Department Official",
        ),
    ], agency="oklahoma city police department", subject_name="ryan keyon williams",
       city="oklahoma city", state="OK")
    assert r.confidence == "high"


def test_relevance_subject_alone_no_supporting_is_medium():
    """Subject anchor with no supporting term → medium (was medium
    in v1 too — unchanged behaviour, but now explicitly pinned)."""
    r = _exec([
        _entry(vid="v1", title="Joe Gold city council appearance"),
    ])
    assert r.confidence == "medium"


def test_relevance_agency_plus_date_plus_supporting_yields_high():
    """Path C: agency anchor + date matching context.incident_date +
    supporting term → high. Dormant under current dataset_intake
    (which doesn't propagate incident_date) but ready for it."""
    r = _exec_with_date([
        _entry(
            vid="v1",
            title="Longmont police bodycam release December 2020 incident",
        ),
    ], incident_date="2020-12-26")
    assert r.confidence == "high"


def test_relevance_agency_plus_wrong_year_does_not_fire_path_c():
    """Path C requires the year to match context.incident_date.
    A 2022 OKCPD briefing for a 2020 incident must NOT reach high
    via the date path."""
    r = _exec_with_date([
        _entry(
            vid="v1",
            title="Longmont police bodycam release 2022 case",
        ),
    ], incident_date="2020-12-26")
    # Has agency + supporting but date doesn't match → medium, not high
    assert r.confidence == "medium"


def test_relevance_date_anchor_dormant_without_context_incident_date():
    """If context lacks incident_date, the date_anchor flag never
    fires regardless of years in the title."""
    r = _exec([
        _entry(
            vid="v1",
            title="Longmont police bodycam release 2020 incident",
        ),
    ])  # no incident_date in context
    # Path C cannot fire → falls back to medium via agency+supporting
    assert r.confidence == "medium"


def test_relevance_generic_bodycam_unrelated_to_context_drops():
    """Generic bodycam content with no anchor → dropped at gate;
    confidence=low because no kept results."""
    r = _exec([
        _entry(vid="v1", title="Top 10 bodycam moments compilation"),
    ])
    assert r.result_urls == []
    assert r.confidence == "low"


def test_relevance_longmont_lex_smoke_remains_low():
    """Pre-existing Longmont false-positive pattern: city-only
    matches with no agency-distinctive token, no subject, no date
    → low. Unchanged from v1."""
    r = _exec([
        _entry(vid="v1", title="Random Longmont event 2018 city report"),
    ])  # context: longmont police services / joe william gold / longmont CO
    # 'longmont' anchors via city + agency_token. No supporting term.
    # → low under both old and new rubrics.
    assert r.confidence == "low"


def test_relevance_local_news_with_subject_remains_high():
    """A case-specific local TV news upload with subject name +
    pursuit term → high. The Miami Beach Reyes pattern from the
    25-candidate smoke must continue to reach high."""
    r = _exec([
        _entry(
            vid="v1",
            title="Family of Ivonne Reyes killed during police chase wants investigation",
        ),
    ], agency="miami beach police department", subject_name="ivonne reyes",
       city="miami beach", state="FL")
    assert r.confidence == "high"


def test_relevance_youtube_metadata_hint_pinned_when_kept_results_exist():
    """next_actions_hint behaviour unchanged: youtube_metadata
    fires whenever at least one result survives the anchor gate,
    regardless of confidence tier."""
    r = _exec([
        _entry(vid="v1", title="Longmont police bodycam release"),
    ])
    assert "youtube_metadata" in r.next_actions_hint
    # And empty kept → empty hint
    r2 = _exec([
        _entry(vid="v2", title="Top 10 bodycam moments compilation"),
    ])
    assert r2.next_actions_hint == []
