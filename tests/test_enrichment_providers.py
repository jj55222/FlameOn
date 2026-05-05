"""Zero-network tests for the provider interface + Mock provider."""
from __future__ import annotations

import pytest

from pipeline2_discovery.enrichment import (
    DEFERRED_PROVIDERS,
    KNOWN_PROVIDERS,
    EnrichmentTask,
    MockProvider,
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
