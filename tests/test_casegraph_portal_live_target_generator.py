"""Zero-network tests for ``portal_live_target_generator``.

Drives the generator with synthetic curated rows (and the two known
real Phoenix CIB URLs as positive anchors). Every test runs against
``tmp_path``; no fixtures are committed by these tests.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph.portal_live_target_generator import (
    DEFAULT_GENERATION_MODE,
    DEFAULT_MAX_TARGETS_PER_RUN,
    GENERATION_MODES,
    generate_targets,
)


PHOENIX_3286 = "https://www.phoenix.gov/newsroom/police-department-news/3286.html"
PHOENIX_3369 = "https://www.phoenix.gov/newsroom/police-department-news/3369.html"


def _row(url, target_id=None):
    r = {"url": url, "agency": "Phoenix Police Department"}
    if target_id is not None:
        r["target_id"] = target_id
    return r


# ---- per-variant fixture shape ---------------------------------------


def test_generates_fetch_only_fixture_with_exact_fields(tmp_path):
    result = generate_targets(
        [_row(PHOENIX_3369)],
        output_dir=tmp_path,
        mode="fetch_only",
    )
    assert result.accepted_count == 1
    assert result.rejected_count == 0
    assert len(result.written_paths) == 1

    written = result.written_paths[0]
    assert written.name == "phoenix_pd_newsroom_3369_real_fetch_only.json"
    fixture = json.loads(written.read_text(encoding="utf-8"))
    assert fixture == {
        "target_id": "phoenix_pd_newsroom_3369",
        "url": PHOENIX_3369,
        "profile_id": "agency_ois_detail",
        "fetcher": "requests",
        "max_pages": 1,
        "max_links": 5,
        "expected_response_status": 200,
        "save_raw_payload": True,
        "allowed_domains": ["www.phoenix.gov"],
        "save_extracted_payload": False,
        "replay_through_portal_replay": False,
        "require_extraction": False,
    }


def test_generates_extract_required_fixture_with_exact_fields(tmp_path):
    result = generate_targets(
        [_row(PHOENIX_3369)],
        output_dir=tmp_path,
        mode="extract_required",
    )
    assert result.accepted_count == 1
    written = result.written_paths[0]
    assert written.name == "phoenix_pd_newsroom_3369_real_extract_required.json"
    fixture = json.loads(written.read_text(encoding="utf-8"))
    assert fixture == {
        "target_id": "phoenix_pd_newsroom_3369_extract_required",
        "url": PHOENIX_3369,
        "profile_id": "agency_ois_detail",
        "fetcher": "requests",
        "max_pages": 1,
        "max_links": 5,
        "expected_response_status": 200,
        "save_raw_payload": True,
        "allowed_domains": ["www.phoenix.gov"],
        "save_extracted_payload": True,
        "replay_through_portal_replay": True,
        "require_extraction": True,
    }


def test_both_mode_emits_two_fixtures_per_row(tmp_path):
    result = generate_targets(
        [_row(PHOENIX_3286)],
        output_dir=tmp_path,
        mode="both",
    )
    assert result.accepted_count == 1
    assert len(result.written_paths) == 2
    names = sorted(p.name for p in result.written_paths)
    assert names == [
        "phoenix_pd_newsroom_3286_real_extract_required.json",
        "phoenix_pd_newsroom_3286_real_fetch_only.json",
    ]


# ---- dedup + cap -----------------------------------------------------


def test_dedupes_duplicate_normalized_urls(tmp_path):
    rows = [
        _row(PHOENIX_3369),
        _row(PHOENIX_3369 + "#video"),  # normalizes to same URL
        _row(PHOENIX_3369),
    ]
    result = generate_targets(rows, output_dir=tmp_path, mode="fetch_only")
    assert result.accepted_count == 1
    assert result.deduped_count == 2
    assert len(result.written_paths) == 1


def test_dedupes_by_target_id_collision(tmp_path):
    """Two distinct URLs that resolve to the same custom target_id
    must collapse to one fixture, not two."""
    rows = [
        _row(PHOENIX_3286, target_id="shared_id_phoenix"),
        _row(PHOENIX_3369, target_id="shared_id_phoenix"),
    ]
    result = generate_targets(rows, output_dir=tmp_path, mode="fetch_only")
    assert result.accepted_count == 1
    assert result.deduped_count == 1


def test_enforces_max_targets_cap(tmp_path):
    rows = [
        _row(
            f"https://www.phoenix.gov/newsroom/police-department-news/{i}.html"
        )
        for i in range(1000, 1010)
    ]
    result = generate_targets(
        rows,
        output_dir=tmp_path,
        mode="fetch_only",
        max_targets=3,
    )
    assert result.accepted_count == 3
    assert result.capped_count == 7
    assert len(result.written_paths) == 3


def test_default_max_targets_cap_is_5():
    assert DEFAULT_MAX_TARGETS_PER_RUN == 5


# ---- rejection paths -------------------------------------------------


def test_rejects_unsafe_custom_target_id_via_lint(tmp_path):
    rows = [_row(PHOENIX_3369, target_id="../../etc/passwd")]
    result = generate_targets(rows, output_dir=tmp_path, mode="fetch_only")
    assert result.accepted_count == 0
    assert result.rejected_count == 1
    assert result.outcomes[0].lint.reason == "unsafe_target_id"
    assert result.written_paths == []


def test_rejects_non_phoenix_host(tmp_path):
    rows = [
        _row(PHOENIX_3286),
        _row("https://www.muckrock.com/foi/phoenix/12345/"),
    ]
    result = generate_targets(rows, output_dir=tmp_path, mode="fetch_only")
    assert result.accepted_count == 1
    assert result.rejected_count == 1
    assert len(result.written_paths) == 1


def test_rejects_non_https(tmp_path):
    rows = [
        _row(
            "http://www.phoenix.gov/newsroom/police-department-news/3369.html"
        )
    ]
    result = generate_targets(rows, output_dir=tmp_path, mode="fetch_only")
    assert result.accepted_count == 0
    assert result.rejected_count == 1
    assert result.outcomes[0].lint.reason == "non_https_scheme"


# ---- dry-run ---------------------------------------------------------


def test_dry_run_writes_no_files(tmp_path):
    rows = [_row(PHOENIX_3286), _row(PHOENIX_3369)]
    result = generate_targets(
        rows,
        output_dir=tmp_path,
        mode="both",
        dry_run=True,
    )
    assert result.accepted_count == 2
    assert result.dry_run is True
    # written_paths populated for the report; no actual files exist:
    assert len(result.written_paths) == 4
    for p in result.written_paths:
        assert not p.exists(), f"dry-run wrote a file unexpectedly: {p}"
    assert list(tmp_path.iterdir()) == []


# ---- mode validation -------------------------------------------------


def test_invalid_mode_raises():
    with pytest.raises(ValueError, match="unsupported generation mode"):
        generate_targets([], output_dir=Path("."), mode="invalid")


def test_invalid_max_targets_raises():
    with pytest.raises(ValueError, match="max_targets must be >= 1"):
        generate_targets([], output_dir=Path("."), max_targets=0)


def test_generation_modes_constant_is_complete():
    assert set(GENERATION_MODES) == {"fetch_only", "extract_required", "both"}


def test_default_mode_is_fetch_only():
    assert DEFAULT_GENERATION_MODE == "fetch_only"


# ---- zero-network ----------------------------------------------------


def test_generator_makes_zero_network_calls(monkeypatch, tmp_path):
    import requests

    def fail_get(self, *args, **kwargs):
        raise AssertionError("generator must never touch the network")

    monkeypatch.setattr(requests.Session, "get", fail_get)
    generate_targets(
        [_row(PHOENIX_3369), _row(PHOENIX_3286)],
        output_dir=tmp_path,
        mode="both",
    )
