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


def _t(query="John Doe Phoenix Police bodycam", task_type="youtube_query"):
    return EnrichmentTask(
        candidate_id="x:1",
        grade="A",
        task_type=task_type,
        query=query,
        context={"agency": "Phoenix Police Department"},
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
    """Minimal yt-dlp context-manager stub that returns canned entries."""

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
                {"id": "abc123", "title": "Bodycam Arrest Phoenix", "url": "https://www.youtube.com/watch?v=abc123"},
                {"id": "def456", "title": "Interrogation Room Footage", "url": ""},
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
    # Entry with empty url gets vid-id fallback; entry with no id is skipped
    assert len(r.result_urls) == 2
    assert len(r.result_titles) == 2
    assert r.result_urls[0] == "https://www.youtube.com/watch?v=abc123"
    assert r.result_urls[1] == "https://www.youtube.com/watch?v=def456"
    assert r.result_titles[0] == "Bodycam Arrest Phoenix"
    assert r.result_titles[1] == "Interrogation Room Footage"


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
