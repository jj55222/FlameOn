"""Zero-network tests for the MuckRock enrichment provider.

The HTTP layer is replaced by an injected fake client per test, so
nothing in this module ever touches the network. Production code
loads the real ``requests``-backed client lazily inside ``execute``,
which means a misconfigured test would surface as ``ImportError``
or as a real HTTP call (caught by the suite-wide
``test_runner_makes_zero_network_calls`` regression guard).
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

import pytest

from pipeline2_discovery.enrichment import (
    DEFERRED_PROVIDERS,
    EnrichmentTask,
    KNOWN_PROVIDERS,
    MuckRockProvider,
    TaskStatus,
    get_provider,
)
from pipeline2_discovery.enrichment.muckrock_provider import (
    AUDIO_TERMS,
    BODYCAM_TERMS,
    DEFAULT_MAX_QUERY_ATTEMPTS,
    HINT_ARTIFACT_SEARCH,
    HINT_MUCKROCK_PARSE,
    HINT_OUTCOME,
    HINT_YOUTUBE,
    INCIDENT_TERMS,
    MIN_LAST_NAME_LEN_FOR_QUERY,
    MUCKROCK_API_BASE,
    POLICY_ONLY_TERMS,
    PURSUIT_TERMS,
    TOPIC_TERMS_BY_SIGNAL,
    build_query_plan,
)


# ---- helpers --------------------------------------------------------


def _ctx(*, agency="Longmont Police Services",
         subject_name="joe william gold", city="longmont", state="CO"):
    return {
        "agency": agency,
        "subject_name": subject_name,
        "city": city,
        "state": state,
    }


def _task(*, query="joe william gold longmont police body-worn camera",
          task_type="muckrock_query", **ctx_kwargs):
    return EnrichmentTask(
        candidate_id="x:1",
        grade="A",
        task_type=task_type,
        query=query,
        context=_ctx(**ctx_kwargs),
    )


def _record(*, rid=1, title="", agency=None, jurisdiction=None,
            status="ack", files=None, body=None, absolute_url=None,
            datetime_submitted="", datetime_done=""):
    out: Dict[str, Any] = {
        "id": rid,
        "title": title,
        "status": status,
        "datetime_submitted": datetime_submitted,
        "datetime_done": datetime_done,
        "absolute_url": absolute_url or f"/foi/longmont-co-9999/{rid}-test/",
    }
    if agency is not None:
        out["agency"] = agency
    if jurisdiction is not None:
        out["jurisdiction"] = jurisdiction
    if files is not None:
        out["files"] = files
    if body is not None:
        out["body"] = body
    return out


class _FakeResp:
    def __init__(self, *, status_code=200, json_body=None,
                 raise_on_json=False):
        self.status_code = status_code
        self._body = json_body if json_body is not None else {"results": []}
        self._raise = raise_on_json

    def json(self):
        if self._raise:
            raise ValueError("not json")
        return self._body


class _FakeClient:
    """Captures one GET call and returns a canned response."""

    def __init__(self, response: _FakeResp):
        self.response = response
        self.calls: List[Dict[str, Any]] = []

    def get(self, url, *, params, headers, timeout):
        self.calls.append({
            "url": url, "params": dict(params),
            "headers": dict(headers), "timeout": timeout,
        })
        return self.response


def _build(*, response, **provider_kwargs):
    """Build a MuckRockProvider with the fake client + a no-op
    sleeper, suitable for fast, deterministic tests."""
    client = _FakeClient(response)
    p = MuckRockProvider(
        http_client=client,
        sleeper=lambda _: None,
        rate_limit_seconds=0,
        read_token=False,
        **provider_kwargs,
    )
    return p, client


# ---- factory + registry --------------------------------------------


def test_factory_returns_muckrock_provider():
    p = get_provider("muckrock")
    assert isinstance(p, MuckRockProvider)
    assert p.name == "muckrock"


def test_muckrock_in_known_providers():
    assert "muckrock" in KNOWN_PROVIDERS


def test_muckrock_no_longer_in_deferred():
    assert "muckrock" not in DEFERRED_PROVIDERS


def test_factory_does_not_raise_for_muckrock():
    # Sanity: the previous PR raised NotImplementedError for this name
    p = get_provider("muckrock")
    assert p is not None


# ---- task-type gating ----------------------------------------------


def test_non_muckrock_task_is_skipped_cleanly():
    p, client = _build(response=_FakeResp(json_body={"results": []}))
    r = p.execute(_task(task_type="youtube_query"))
    assert r.status == TaskStatus.SKIPPED
    assert r.provider == "muckrock"
    assert r.result_urls == []
    assert client.calls == []  # never reached the HTTP layer
    assert any("not muckrock_query" in n for n in r.notes)


# ---- API URL + params ----------------------------------------------


def test_search_url_uses_documented_base():
    """The provider hits the documented MuckRock API base on every
    call. Per-call ``title`` values vary because the query plan
    derives short anchor / topic tokens from the task context — but
    the URL itself must always resolve to MUCKROCK_API_BASE."""
    p, client = _build(response=_FakeResp(json_body={"results": []}))
    p.execute(_task(query="some query"))
    assert len(client.calls) >= 1
    for call in client.calls:
        assert call["url"] == MUCKROCK_API_BASE
        # Each call must carry a non-empty title and a positive page size
        assert call["params"]["title"]
        assert call["params"]["page_size"] >= 1


def test_request_only_uses_get_method():
    """Smoke-test the entire module: only `.get()` is ever called on
    the HTTP client. There are no POST / PUT / PATCH / DELETE
    methods invoked or even imported."""
    p, client = _build(response=_FakeResp(json_body={"results": []}))
    p.execute(_task())
    # The fake client only implements .get; if the provider tried
    # any other verb it would AttributeError.
    assert hasattr(client, "get")
    assert not any(
        getattr(client, verb, None)
        for verb in ("post", "put", "patch", "delete")
    )
    # Static check on the source: no other HTTP verbs are referenced.
    import pipeline2_discovery.enrichment.muckrock_provider as mod
    src = open(mod.__file__, encoding="utf-8").read()
    assert ".post(" not in src
    assert ".put(" not in src
    assert ".patch(" not in src
    assert ".delete(" not in src


def test_default_headers_have_no_authorization_when_no_token():
    p, client = _build(response=_FakeResp(json_body={"results": []}))
    p.execute(_task())
    headers = client.calls[0]["headers"]
    assert "Authorization" not in headers
    # User-Agent + Accept always present
    assert "Accept" in headers
    assert "User-Agent" in headers


def test_token_env_is_only_read_when_read_token_true(monkeypatch):
    """Tests should never read the live env var; the provider uses
    read_token=False in this suite. With read_token=True it should
    pick up the env var."""
    client = _FakeClient(_FakeResp(json_body={"results": []}))
    monkeypatch.setenv("MUCKROCK_API_TOKEN", "fake-token-abc")
    p = MuckRockProvider(
        http_client=client, sleeper=lambda _: None,
        rate_limit_seconds=0, read_token=True,
    )
    p.execute(_task())
    assert client.calls[0]["headers"].get("Authorization") == "Token fake-token-abc"


# ---- parsing + scoring (records with released files) ---------------


def test_parses_record_with_released_files_high_confidence():
    record = _record(
        rid=1,
        title="Joe William Gold body-worn camera footage release",
        agency={"name": "Longmont Police Services"},
        jurisdiction={"name": "Longmont", "level": "city"},
        status="done",
        files=[{"ffile": "https://www.muckrock.com/files/x.mp4"}],
        datetime_done="2025-01-01",
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert r.status == TaskStatus.COMPLETED
    assert r.confidence == "high"
    assert len(r.result_urls) == 1
    assert "muckrock.com" in r.result_urls[0]
    # Hints: parse + artifact_search (file_count > 0)
    assert HINT_MUCKROCK_PARSE in r.next_actions_hint
    assert HINT_ARTIFACT_SEARCH in r.next_actions_hint
    # No unresolved outcome since status=done
    assert HINT_OUTCOME not in r.next_actions_hint


def test_parses_record_with_bodycam_term_in_title():
    """Agency anchor + bodycam term, no files → medium. Tightened
    semantics (PR #38): MUCKROCK_PARSE_RELEASED_FILES is NO LONGER
    emitted just because a kept result exists — only when at least
    one kept result actually has files. ARTIFACT_SEARCH likewise."""
    record = _record(
        rid=2,
        title="Longmont Police body-worn camera incident records",
        agency={"name": "Longmont Police Services"},
        status="ack",
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert r.status == TaskStatus.COMPLETED
    assert r.confidence == "medium"
    # status=ack, not done → outcome hint
    assert HINT_OUTCOME in r.next_actions_hint
    # No actual files → neither parse nor artifact-search hints fire
    assert HINT_MUCKROCK_PARSE not in r.next_actions_hint
    assert HINT_ARTIFACT_SEARCH not in r.next_actions_hint


def test_parses_record_with_strong_incident_terms_medium_when_no_files():
    """Tightened semantics (PR #38): agency anchor + strong incident
    terms but NO files yields medium, not high. high requires actual
    released files when there's no subject anchor."""
    record = _record(
        rid=3,
        title="Longmont officer-involved shooting investigation files",
        agency={"name": "Longmont Police Services"},
        status="ack",
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert r.confidence == "medium"
    notes_str = " ".join(r.notes)
    assert "officer-involved shooting" in notes_str
    # No files → no MUCKROCK_PARSE_RELEASED_FILES hint
    assert HINT_MUCKROCK_PARSE not in r.next_actions_hint


# ---- policy-only demotion ------------------------------------------


def test_demotes_policy_only_body_camera_policy_request():
    """A request titled 'Body-worn camera policy' anchored on
    Longmont must be DEMOTED — it is policy-only with no released
    files and no strong terms."""
    record = _record(
        rid=10,
        title="Longmont Police body-worn camera policy",
        agency={"name": "Longmont Police Services"},
        status="ack",
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    # Anchored, but policy-only with no files → dropped
    assert r.result_urls == []
    assert r.confidence == "low"
    notes_str = " ".join(r.notes)
    assert "policy_only_demoted=true" in notes_str


def test_keeps_policy_titled_request_if_files_released():
    """If the request is policy-titled BUT has released files, we
    keep it — files are the strongest signal we have."""
    record = _record(
        rid=11,
        title="Longmont Police body-worn camera policy",
        agency={"name": "Longmont Police Services"},
        status="done",
        files=[{"ffile": "https://www.muckrock.com/files/policy.pdf"}],
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert len(r.result_urls) == 1


# ---- anchor / drop logic -------------------------------------------


def test_drops_unanchored_topical_record():
    """A bodycam-themed MuckRock request that has no Longmont / Joe
    Gold / CO anchor must be dropped — bodycam is supporting, not
    anchor."""
    record = _record(
        rid=20,
        title="Generic body-worn camera footage compilation 2024",
        agency={"name": "Some Other PD"},
        jurisdiction={"name": "Atlanta", "level": "city"},
        status="ack",
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert r.result_urls == []
    assert r.confidence == "low"
    assert HINT_MUCKROCK_PARSE not in r.next_actions_hint


def test_filters_mixed_results_to_only_anchored():
    keep = _record(
        rid=30,
        title="Longmont body-worn camera release — Joe Gold incident",
        agency={"name": "Longmont Police Services"},
        status="done",
        files=[{"ffile": "https://www.muckrock.com/files/x.mp4"}],
    )
    drop1 = _record(
        rid=31,
        title="Some Other PD body-worn camera footage",
        agency={"name": "Some Other PD"},
    )
    drop2 = _record(
        rid=32,
        title="Random policy manual",
        agency={"name": "Yet Another"},
    )
    p, client = _build(response=_FakeResp(json_body={
        "results": [keep, drop1, drop2]
    }))
    r = p.execute(_task())
    assert len(r.result_urls) == 1
    assert "/30-" in r.result_urls[0]


# ---- empty + transport failure -------------------------------------


def test_empty_results_completed_low_confidence():
    p, client = _build(response=_FakeResp(json_body={"results": []}))
    r = p.execute(_task())
    assert r.status == TaskStatus.COMPLETED
    assert r.result_urls == []
    assert r.confidence == "low"
    assert r.next_actions_hint == []


def test_http_exception_returns_failed():
    class _ExplodingClient:
        def get(self, url, *, params, headers, timeout):
            raise OSError("connection refused")

    p = MuckRockProvider(
        http_client=_ExplodingClient(), sleeper=lambda _: None,
        rate_limit_seconds=0, read_token=False,
    )
    r = p.execute(_task())
    assert r.status == TaskStatus.FAILED
    assert "OSError" in (r.error or "")
    assert r.result_urls == []


def test_non_200_status_returns_failed():
    p, client = _build(response=_FakeResp(status_code=503, json_body={}))
    r = p.execute(_task())
    assert r.status == TaskStatus.FAILED
    assert "http_503" in (r.error or "")


def test_non_json_response_returns_failed():
    p, client = _build(response=_FakeResp(
        status_code=200, raise_on_json=True
    ))
    r = p.execute(_task())
    assert r.status == TaskStatus.FAILED
    assert "json_decode_error" in (r.error or "")


# ---- max_results cap -----------------------------------------------


def test_max_results_caps_returned_urls():
    records = [
        _record(
            rid=i,
            title=f"Longmont Police body-worn camera incident #{i}",
            agency={"name": "Longmont Police Services"},
            status="done",
            files=[{"ffile": f"https://www.muckrock.com/files/{i}.mp4"}],
        )
        for i in range(1, 11)
    ]
    p, client = _build(
        response=_FakeResp(json_body={"results": records}),
        max_results=3,
    )
    r = p.execute(_task())
    assert len(r.result_urls) == 3
    assert len(r.result_titles) == 3


def test_max_results_clamped_to_1_to_20():
    p_low = MuckRockProvider(max_results=0, http_client=_FakeClient(_FakeResp()))
    p_high = MuckRockProvider(max_results=999, http_client=_FakeClient(_FakeResp()))
    assert p_low._max_results == 1
    assert p_high._max_results == 20


# ---- diagnostics ----------------------------------------------------


def test_notes_include_raw_returned_dropped_counts():
    keep = _record(
        rid=1, title="Longmont body-worn camera release Joe Gold",
        agency={"name": "Longmont Police Services"}, status="done",
        files=[{"ffile": "https://www.muckrock.com/files/x.mp4"}],
    )
    drop = _record(rid=2, title="Generic Atlanta policy",
                   agency={"name": "Atlanta PD"})
    p, client = _build(
        response=_FakeResp(json_body={"results": [keep, drop]}),
    )
    r = p.execute(_task())
    notes_str = " ".join(r.notes)
    assert "returned_result_count=1" in notes_str
    assert "dropped_irrelevant_count=1" in notes_str
    assert "released_file_count=1" in notes_str
    assert "api_urls=" in notes_str
    assert "deduped_raw_result_count=" in notes_str
    assert "query_attempts=" in notes_str


def test_notes_include_terms_matched_for_kept_results():
    record = _record(
        rid=1,
        title="Longmont Police body-worn camera officer-involved shooting",
        agency={"name": "Longmont Police Services"},
        status="ack",
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    notes_str = " ".join(r.notes)
    assert "terms_matched=" in notes_str
    assert "body-worn camera" in notes_str
    assert "officer-involved shooting" in notes_str


def test_notes_include_dropped_title_examples():
    drops = [
        _record(rid=i,
                title=f"Some random body-worn camera record {i}",
                agency={"name": "Atlanta PD"})
        for i in range(5)
    ]
    p, client = _build(response=_FakeResp(json_body={"results": drops}))
    r = p.execute(_task())
    drop_notes = [n for n in r.notes if n.startswith("dropped ")]
    assert 1 <= len(drop_notes) <= 3  # capped at 3
    for n in drop_notes:
        assert "title=" in n


# ---- next_actions_hint --------------------------------------------


def test_next_actions_emits_youtube_hint_only_if_youtube_url_in_metadata():
    """A MuckRock record that embeds a YouTube URL in body should
    surface YOUTUBE_METADATA_TRANSCRIPT. A record without one
    must NOT — we don't auto-emit YouTube hints just because the
    record mentions video."""
    with_yt = _record(
        rid=1,
        title="Longmont body-worn camera release",
        agency={"name": "Longmont Police Services"},
        status="done",
        body="See video at https://www.youtube.com/watch?v=abc12345678",
        files=[{"ffile": "https://www.muckrock.com/files/x.mp4"}],
    )
    without_yt = _record(
        rid=2,
        title="Longmont body-worn camera release",
        agency={"name": "Longmont Police Services"},
        status="done",
        body="Records released; see attached PDF.",
        files=[{"ffile": "https://www.muckrock.com/files/x.pdf"}],
    )

    p1, _ = _build(response=_FakeResp(json_body={"results": [with_yt]}))
    r1 = p1.execute(_task())
    assert HINT_YOUTUBE in r1.next_actions_hint

    p2, _ = _build(response=_FakeResp(json_body={"results": [without_yt]}))
    r2 = p2.execute(_task())
    assert HINT_YOUTUBE not in r2.next_actions_hint


def test_next_actions_emits_outcome_validate_when_status_unresolved():
    record = _record(
        rid=1,
        title="Longmont Police body-worn camera release",
        agency={"name": "Longmont Police Services"},
        status="ack",  # still in flight
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert HINT_OUTCOME in r.next_actions_hint


def test_next_actions_no_outcome_validate_when_done():
    record = _record(
        rid=1,
        title="Longmont Police body-worn camera release",
        agency={"name": "Longmont Police Services"},
        status="done",
        files=[{"ffile": "https://www.muckrock.com/files/x.mp4"}],
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert HINT_OUTCOME not in r.next_actions_hint


# ---- sort order -----------------------------------------------------


def test_kept_results_ranked_by_score():
    weak = _record(
        rid=100,
        title="Longmont report",
        agency={"name": "Longmont Police Services"},
    )
    strong = _record(
        rid=200,
        title="Joe William Gold Longmont body-worn camera officer-involved shooting",
        agency={"name": "Longmont Police Services"},
        status="done",
        files=[{"ffile": "https://www.muckrock.com/files/x.mp4"}],
    )
    p, client = _build(response=_FakeResp(json_body={
        "results": [weak, strong]
    }))
    r = p.execute(_task())
    # strong first
    assert r.result_urls[0].endswith("/200-test/")


# ---- absolute_url normalisation -----------------------------------


def test_absolute_url_relative_path_is_normalised():
    record = _record(
        rid=1, title="Longmont body-worn camera",
        agency={"name": "Longmont Police Services"}, status="done",
        files=[{"ffile": "https://www.muckrock.com/files/x.mp4"}],
        absolute_url="/foi/longmont-co-9999/1-rec/",
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert r.result_urls[0].startswith("https://www.muckrock.com/foi/")


def test_absolute_url_falls_back_to_id_and_slug_for_list_endpoint():
    """The ``/api_v2/requests/`` list endpoint omits ``absolute_url`` /
    ``url`` and only carries ``id`` + ``slug``. The provider must
    construct a usable public URL from those fields — otherwise every
    list-endpoint record gets silently dropped before scoring."""
    record = {
        "id": 180304,
        "title": "Longmont Police body-worn camera footage",
        "slug": "longmont-police-body-worn-camera-footage",
        "status": "done",
        "agency": 40430,  # int ID, not a dict — list endpoint shape
        "files": [{"ffile": "https://www.muckrock.com/files/x.mp4"}],
        # NO absolute_url, NO url — exactly what the live API returns
    }
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert r.status == TaskStatus.COMPLETED
    # Record was NOT silently dropped — it has a constructed public URL
    assert any(
        "180304-longmont-police-body-worn-camera-footage" in url
        for url in r.result_urls
    )


# ---- term constants --------------------------------------------


def test_constants_export_expected_terms():
    assert "bwc" in BODYCAM_TERMS
    assert "officer-involved shooting" in INCIDENT_TERMS
    assert "pursuit" in PURSUIT_TERMS
    assert "policy manual" in POLICY_ONLY_TERMS
    assert "incident report" in AUDIO_TERMS


# ---- query plan -----------------------------------------------------


def test_default_max_query_attempts_is_3():
    assert DEFAULT_MAX_QUERY_ATTEMPTS == 3


def _qtask(query, **ctx_kwargs):
    return EnrichmentTask(
        candidate_id="x:1",
        grade="A",
        task_type="muckrock_query",
        query=query,
        context=_ctx(**ctx_kwargs),
    )


def test_query_plan_includes_agency_distinctive_token():
    plan = build_query_plan(
        _qtask("joe gold longmont police body-worn camera"),
        max_attempts=3,
    )
    assert "longmont" in plan


def test_query_plan_includes_subject_last_name_when_long_enough():
    plan = build_query_plan(
        _qtask("joe william gold longmont police body-worn camera"),
        max_attempts=3,
    )
    assert "gold" in plan  # 4 chars, passes the >=4 floor


def test_query_plan_excludes_short_last_name():
    plan = build_query_plan(
        _qtask(
            "ann doe pinal county sheriff body-worn camera",
            subject_name="ann doe",
        ),
        max_attempts=3,
    )
    # "doe" is 3 chars — must be excluded from the plan
    assert "doe" not in plan


def test_query_plan_includes_topic_term_for_bodycam_query():
    plan = build_query_plan(
        _qtask("joe gold longmont police body-worn camera"),
        max_attempts=3,
    )
    assert "body-worn camera" in plan


def test_query_plan_includes_topic_term_for_pursuit_query():
    plan = build_query_plan(
        _qtask("joe gold longmont police pursuit"),
        max_attempts=3,
    )
    assert "pursuit" in plan


def test_query_plan_includes_topic_term_for_incident_report_query():
    plan = build_query_plan(
        _qtask("joe gold longmont police incident report"),
        max_attempts=3,
    )
    assert "incident report" in plan


def test_query_plan_caps_at_max_attempts():
    plan = build_query_plan(
        _qtask("joe gold longmont police body-worn camera"),
        max_attempts=2,
    )
    assert len(plan) == 2


def test_query_plan_clamps_max_attempts_to_5():
    plan = build_query_plan(
        _qtask("joe gold longmont police body-worn camera"),
        max_attempts=999,
    )
    assert len(plan) <= 5


def test_query_plan_clamps_max_attempts_to_at_least_1():
    plan = build_query_plan(
        _qtask("joe gold longmont police body-worn camera"),
        max_attempts=0,
    )
    assert len(plan) >= 1


def test_query_plan_falls_back_to_task_query_when_no_anchors():
    plan = build_query_plan(
        EnrichmentTask(
            candidate_id="x:1", grade="A",
            task_type="muckrock_query",
            query="some random title",
            context={},  # no agency, no subject, no city, no state
        ),
        max_attempts=3,
    )
    # No agency token, no last name, no recognised topic → backstop
    assert plan == ["some random title"]


def test_query_plan_uses_longest_agency_token_first():
    plan = build_query_plan(
        _qtask(
            "subject zavala county sheriff crystal city pd body-worn camera",
            agency="zavala county sheriff's office, crystal city police department",
        ),
        max_attempts=3,
    )
    # "crystal" (7) and "zavala" (6) both eligible; "crystal" picked first
    assert plan[0] == "crystal"


def test_query_plan_deduplicates_overlapping_choices():
    """If the agency-distinctive token equals the topic term canonical
    or last name, the plan must not repeat it."""
    plan = build_query_plan(
        _qtask(
            "longmont longmont longmont body-worn camera",
            agency="longmont police",
            subject_name="longmont",  # contrived collision
        ),
        max_attempts=3,
    )
    assert len(plan) == len(set(plan))


# ---- multi-attempt execution ---------------------------------------


class _MultiResp:
    """Fake HTTP client that returns different canned results based
    on the ``title`` query parameter, so a single test can simulate
    a real multi-query plan against a fake MuckRock."""

    def __init__(self, by_title):
        self._by_title = by_title  # {title_query: [records]}
        self.calls = []

    def get(self, url, *, params, headers, timeout):
        self.calls.append({
            "url": url, "params": dict(params),
            "headers": dict(headers), "timeout": timeout,
        })
        title = params.get("title", "")
        results = self._by_title.get(title, [])

        class _R:
            status_code = 200

            def __init__(self, results):
                self._results = results

            def json(self):
                return {"results": self._results}

        return _R(results)


def test_run_mode_issues_multiple_gets_per_task():
    """One task should produce up to max_query_attempts GETs."""
    client = _MultiResp(by_title={})
    p = MuckRockProvider(
        http_client=client, sleeper=lambda _: None,
        rate_limit_seconds=0, read_token=False,
        max_query_attempts=3,
    )
    p.execute(_task())
    assert 1 <= len(client.calls) <= 3
    # Each call hits the documented base
    for call in client.calls:
        assert call["url"] == MUCKROCK_API_BASE


def test_rate_limit_sleeper_called_between_attempts():
    sleeps = []
    client = _MultiResp(by_title={})
    p = MuckRockProvider(
        http_client=client, sleeper=lambda d: sleeps.append(d),
        rate_limit_seconds=1.0, read_token=False,
        max_query_attempts=3,
    )
    p.execute(_task())
    # One sleep per attempt
    assert len(sleeps) == len(client.calls)
    assert all(d == 1.0 for d in sleeps)


def test_results_deduped_across_query_attempts():
    """Same record returned by multiple title queries should be
    counted once after dedupe."""
    shared = _record(
        rid=99,
        title="Longmont Police body-worn camera release",
        agency={"name": "Longmont Police Services"},
        status="done",
        files=[{"ffile": "https://www.muckrock.com/files/x.mp4"}],
    )
    # All 3 queries return the same one record
    client = _MultiResp(by_title={
        "longmont": [shared],
        "gold": [shared],
        "body-worn camera": [shared],
    })
    p = MuckRockProvider(
        http_client=client, sleeper=lambda _: None,
        rate_limit_seconds=0, read_token=False,
        max_query_attempts=3,
    )
    r = p.execute(_task())
    assert len(client.calls) == 3
    notes_str = " ".join(r.notes)
    assert "deduped_raw_result_count=1" in notes_str
    # Only one URL surfaced — not three
    assert len(r.result_urls) == 1


def test_unanchored_broad_results_still_dropped_by_gate():
    """A broad ?title=body-worn camera query may return real records
    that have no Longmont anchor — the relevance gate must still
    drop them."""
    unanchored = _record(
        rid=200,
        title="Generic body-worn camera footage policy",
        agency={"name": "Some Other PD"},
    )
    client = _MultiResp(by_title={
        "longmont": [],
        "gold": [],
        "body-worn camera": [unanchored],
    })
    p = MuckRockProvider(
        http_client=client, sleeper=lambda _: None,
        rate_limit_seconds=0, read_token=False,
        max_query_attempts=3,
    )
    r = p.execute(_task())
    assert r.result_urls == []
    assert r.confidence == "low"
    notes_str = " ".join(r.notes)
    assert "deduped_raw_result_count=1" in notes_str
    assert "returned_result_count=0" in notes_str


def test_anchored_broad_query_hit_survives():
    """A ?title=body-worn camera query that happens to return a
    Longmont request should be kept by the gate."""
    anchored = _record(
        rid=300,
        title="Longmont Police body-worn camera footage release",
        agency={"name": "Longmont Police Services"},
        status="done",
        files=[{"ffile": "https://www.muckrock.com/files/x.mp4"}],
    )
    client = _MultiResp(by_title={
        "longmont": [],
        "gold": [],
        "body-worn camera": [anchored],
    })
    p = MuckRockProvider(
        http_client=client, sleeper=lambda _: None,
        rate_limit_seconds=0, read_token=False,
        max_query_attempts=3,
    )
    r = p.execute(_task())
    assert "/300-test/" in r.result_urls[0]
    assert r.confidence == "high"


def test_diagnostics_include_per_query_counts_and_api_urls():
    """The notes block must surface raw_result_count_by_query so an
    operator can see which broad query produced which raw fanout."""
    record_a = _record(rid=1, title="Longmont incident bodycam",
                       agency={"name": "Longmont Police Services"})
    record_b = _record(rid=2, title="Generic Atlanta body-worn camera",
                       agency={"name": "Atlanta PD"})
    client = _MultiResp(by_title={
        "longmont": [record_a],
        "gold": [],
        "body-worn camera": [record_b],
    })
    p = MuckRockProvider(
        http_client=client, sleeper=lambda _: None,
        rate_limit_seconds=0, read_token=False,
        max_query_attempts=3,
    )
    r = p.execute(_task())
    notes_str = " ".join(r.notes)
    assert "query_attempts=3" in notes_str
    assert "raw_result_count_by_query=" in notes_str
    assert "deduped_raw_result_count=2" in notes_str
    # api_urls is a single semicolon-joined entry covering all 3 attempts
    assert "api_urls=" in notes_str
    assert notes_str.count(MUCKROCK_API_BASE) == 3


def test_query_plan_only_uses_get_method():
    """Multi-attempt path still GET-only — no POST/PUT/PATCH/DELETE
    introduced in the rewrite."""
    import pipeline2_discovery.enrichment.muckrock_provider as mod
    src = open(mod.__file__, encoding="utf-8").read()
    assert ".post(" not in src
    assert ".put(" not in src
    assert ".patch(" not in src
    assert ".delete(" not in src


def test_failed_attempts_surface_as_failed_status():
    """If every attempt hits a transport error, the provider returns
    status=failed with the first failure's diagnostics."""
    class _AllExplode:
        def get(self, url, *, params, headers, timeout):
            raise OSError("connection refused")

    p = MuckRockProvider(
        http_client=_AllExplode(), sleeper=lambda _: None,
        rate_limit_seconds=0, read_token=False,
        max_query_attempts=3,
    )
    r = p.execute(_task())
    assert r.status == TaskStatus.FAILED
    assert "OSError" in (r.error or "")
    notes_str = " ".join(r.notes)
    assert "query_attempts=3" in notes_str
    assert "api_urls=" in notes_str


# ---- confidence + hint tightening (post-Bulmahn) -------------------


def test_status_done_alone_does_not_set_has_released_files():
    """Tightened semantics: status=done WITHOUT files[] must NOT
    flip has_released_files. The Bulmahn smoke surfaced 5 Pinal
    admin records all marked status=done with file_count=0; treating
    those as artifact-bearing was the false-positive failure mode."""
    record = _record(
        rid=1,
        title="Longmont Police body-worn camera incident records",
        agency={"name": "Longmont Police Services"},
        status="done",
        # NO files
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    notes_str = " ".join(r.notes)
    assert "released_file_count=0" in notes_str
    # Status=done alone is NOT enough to fire MUCKROCK_PARSE
    assert HINT_MUCKROCK_PARSE not in r.next_actions_hint
    assert HINT_ARTIFACT_SEARCH not in r.next_actions_hint


def test_agency_only_done_no_files_is_low_confidence():
    """The Bulmahn pattern: agency anchor + status=done + no files +
    no incident/bodycam/pursuit term → confidence=low (was high in
    v0). The 'agency-admin firehose' problem."""
    record = _record(
        rid=1,
        title="Communication Services Contract - Arizona - 2018",
        agency={"name": "Pinal County Sheriff's Office"},
        status="done",
        # NO files, NO incident/bodycam/pursuit term
    )
    # Use a Pinal-shaped context so 'pinal' anchors via the agency
    # token. Subject name 'doe' is a placeholder that's too short to
    # anchor (length 3 < 4 floor).
    task = EnrichmentTask(
        candidate_id="x:1", grade="A", task_type="muckrock_query",
        query="pinal county sheriff body-worn camera",
        context={
            "agency": "pinal county sheriff's office",
            "subject_name": "ann doe",
            "city": "san tan valley",
            "state": "AZ",
        },
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(task)
    # Note: the contract title now demotes via expanded
    # POLICY_ONLY_TERMS, so it gets dropped at the gate. Either way
    # the confidence resolves to low.
    assert r.confidence == "low"


def test_agency_admin_only_count_in_diagnostics():
    """When kept results are agency-anchored admin-only (no subject,
    no files, no incident/bodycam/pursuit), the diagnostic block
    surfaces a count. Lets an operator see the 'we found agency
    records but no incident artifacts' shape directly."""
    # An admin-shaped record that survives the gate without case
    # signal: anchor-only on agency token. Using a non-policy title
    # so it doesn't get demoted, just to test the admin_only flag.
    record = _record(
        rid=1,
        title="Longmont city budget transmittal letter 2023",
        # 'budget' would demote via POLICY_ONLY_TERMS; use a plainer
        # title to isolate the admin_only flag specifically.
    )
    record["title"] = "Longmont general records request — 2023 calendar year"
    record["agency"] = {"name": "Longmont Police Services"}
    record["status"] = "ack"  # not done, doesn't matter for this flag
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    notes_str = " ".join(r.notes)
    assert "agency_admin_only_count=" in notes_str


def test_actual_files_emit_both_parse_and_artifact_hints():
    record = _record(
        rid=1,
        title="Longmont Police body-worn camera footage release",
        agency={"name": "Longmont Police Services"},
        status="done",
        files=[{"ffile": "https://www.muckrock.com/files/x.mp4"}],
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert HINT_MUCKROCK_PARSE in r.next_actions_hint
    assert HINT_ARTIFACT_SEARCH in r.next_actions_hint
    # Status=done → no OUTCOME_VALIDATE
    assert HINT_OUTCOME not in r.next_actions_hint


def test_subject_plus_strong_incident_reaches_high_without_files():
    """high path B from the spec: subject anchor + strong incident
    term reaches high even when no files have been released yet."""
    record = _record(
        rid=1,
        title="Joe William Gold officer-involved shooting investigation files",
        agency={"name": "Longmont Police Services"},
        status="ack",
        # NO files
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert r.confidence == "high"


def test_subject_plus_files_reaches_high():
    """high path A from the spec: subject anchor + actual released
    files reaches high regardless of incident term presence."""
    record = _record(
        rid=1,
        title="Joe William Gold case records release",
        agency={"name": "Longmont Police Services"},
        status="done",
        files=[{"ffile": "https://www.muckrock.com/files/x.pdf"}],
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert r.confidence == "high"


def test_subject_alone_no_files_no_strong_terms_is_medium():
    """medium path B from the spec: subject anchor with no files and
    no strong incident term yields medium (not high)."""
    record = _record(
        rid=1,
        title="Joe William Gold mention in records",  # subject-anchored, but no files / no strong terms
        agency={"name": "Longmont Police Services"},
        status="ack",
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert r.confidence == "medium"


def test_agency_plus_bodycam_no_files_is_medium_not_high():
    """medium path A: agency/city anchor + supporting term (bodycam)
    without files → medium. Was the source of the v0 false-high."""
    record = _record(
        rid=1,
        title="Longmont Police body-worn camera release request",
        agency={"name": "Longmont Police Services"},
        status="ack",
        # NO files
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert r.confidence == "medium"
    assert HINT_MUCKROCK_PARSE not in r.next_actions_hint


def test_agency_plus_files_plus_case_term_reaches_high():
    """high path C: agency/city anchor + actual released files +
    case-relevant term."""
    record = _record(
        rid=1,
        title="Longmont Police body-worn camera footage release",
        agency={"name": "Longmont Police Services"},
        status="done",
        files=[{"ffile": "https://www.muckrock.com/files/x.mp4"}],
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert r.confidence == "high"


def test_admin_terms_demote_contract_roster_medical_policies():
    """The 5 specific admin titles surfaced in the Bulmahn live smoke
    must now demote via POLICY_ONLY_TERMS. With no files and no
    strong incident terms, they should drop."""
    bad_titles = [
        "Communication Services Contract - Arizona - 2018",
        "Agency Roster",
        "Medical Policies for Blood Borne Pathogens",
        "Police Officer - Domestic (intimate partner) Abuse policies",
    ]
    records = [
        _record(
            rid=i, title=t,
            agency={"name": "Longmont Police Services"},  # anchored on agency
            status="done",  # status=done used to escape; no longer
        )
        for i, t in enumerate(bad_titles, start=10)
    ]
    p, client = _build(response=_FakeResp(json_body={"results": records}))
    r = p.execute(_task())
    # All four admin records should be dropped — only the agency
    # token anchors them, and the policy_only flag fires (no files,
    # no strong terms) so they hit drop_reason=policy_only_no_release.
    assert r.result_urls == []
    assert r.confidence == "low"


def test_bulmahn_shaped_response_returns_low_no_parse_hint():
    """End-to-end: simulate the actual Bulmahn smoke API response
    shape — 5 Pinal admin requests, all status=done, all
    file_count=0 — and confirm the tightened gate downgrades the
    whole batch to confidence=low with no parse / artifact hints."""
    bulmahn_like = [
        {
            "id": 54115,
            "title": "Communication Services Contract - Arizona - 2018",
            "slug": "communication-services-contract-arizona-2018",
            "status": "done",
            "agency": {"name": "Pinal County Sheriff's Office"},
        },
        {
            "id": 159632, "title": "Agency Roster",
            "slug": "agency-roster",
            "status": "done",
            "agency": {"name": "Pinal County Sheriff's Office"},
        },
        {
            "id": 184352,
            "title": "Medical Policies for Blood Borne Pathogens",
            "slug": "medical-policies",
            "status": "done",
            "agency": {"name": "Pinal County Sheriff's Office"},
        },
        {
            "id": 82312,
            "title": "Police Officer - Domestic (intimate partner) Abuse policies",
            "slug": "domestic-abuse-policies",
            "status": "done",
            "agency": {"name": "Pinal County Sheriff's Office"},
        },
    ]
    bulmahn_task = EnrichmentTask(
        candidate_id="sfchronicle_pursuits:2848", grade="A",
        task_type="muckrock_query",
        query="chase james bulmahn pinal county sheriff body-worn camera",
        context={
            "agency": "pinal county sheriff's office",
            "subject_name": "chase james bulmahn",
            "city": "san tan valley",
            "state": "AZ",
        },
    )
    p, client = _build(response=_FakeResp(json_body={"results": bulmahn_like}))
    r = p.execute(bulmahn_task)
    # All 4 records demoted via expanded POLICY_ONLY_TERMS (contract,
    # roster, medical policies, abuse policies). With no subject
    # anchor (no "bulmahn" in any title) and no files, all dropped.
    assert r.confidence == "low"
    assert r.result_urls == []
    assert HINT_MUCKROCK_PARSE not in r.next_actions_hint
    assert HINT_ARTIFACT_SEARCH not in r.next_actions_hint
