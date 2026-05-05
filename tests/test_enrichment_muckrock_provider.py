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
    HINT_ARTIFACT_SEARCH,
    HINT_MUCKROCK_PARSE,
    HINT_OUTCOME,
    HINT_YOUTUBE,
    INCIDENT_TERMS,
    MUCKROCK_API_BASE,
    POLICY_ONLY_TERMS,
    PURSUIT_TERMS,
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
    p, client = _build(response=_FakeResp(json_body={"results": []}))
    p.execute(_task(query="some query"))
    assert client.calls[0]["url"] == MUCKROCK_API_BASE
    assert client.calls[0]["params"]["title"] == "some query"
    assert client.calls[0]["params"]["page_size"] >= 1


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
    assert HINT_MUCKROCK_PARSE in r.next_actions_hint
    assert HINT_OUTCOME in r.next_actions_hint  # status=ack, not done
    # No artifact_search because file_count == 0
    assert HINT_ARTIFACT_SEARCH not in r.next_actions_hint


def test_parses_record_with_strong_incident_terms_high_confidence():
    record = _record(
        rid=3,
        title="Longmont officer-involved shooting investigation files",
        agency={"name": "Longmont Police Services"},
        status="ack",
    )
    p, client = _build(response=_FakeResp(json_body={"results": [record]}))
    r = p.execute(_task())
    assert r.confidence == "high"
    notes_str = " ".join(r.notes)
    assert "officer-involved shooting" in notes_str


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
    assert "raw_result_count=2" in notes_str
    assert "returned_result_count=1" in notes_str
    assert "dropped_irrelevant_count=1" in notes_str
    assert "released_file_count=1" in notes_str
    assert "api_url=" in notes_str


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


# ---- term constants --------------------------------------------


def test_constants_export_expected_terms():
    assert "bwc" in BODYCAM_TERMS
    assert "officer-involved shooting" in INCIDENT_TERMS
    assert "pursuit" in PURSUIT_TERMS
    assert "policy manual" in POLICY_ONLY_TERMS
    assert "incident report" in AUDIO_TERMS
