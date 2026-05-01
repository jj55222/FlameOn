"""Unit tests for transcript_loader.py — multi-source merge + LLM
formatting.

Covers:
- ``merge_transcripts`` — groups by case_id, assigns stable
  source_idx, aggregates duration, surfaces evidence types.
- ``format_for_llm`` — banner per source, [HH:MM:SS | S<idx> |
  SPK<n>] line format, max_chars truncation.
- ``discover_case_transcripts`` — directory scan + filter.
- error paths: mixed case_ids rejected, missing required keys.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from transcript_loader import (
    discover_case_transcripts,
    format_for_llm,
    load_single,
    merge_transcripts,
)


def _write_transcript(tmp_path, name, *, case_id, source_url, evidence_type,
                       duration, segments):
    """Write a single P3-style transcript to disk and return its path."""
    data = {
        "case_id": case_id,
        "source_url": source_url,
        "source_evidence_type": evidence_type,
        "original_duration_sec": duration,
        "processed_duration_sec": duration,
        "transcript": segments,
    }
    path = tmp_path / name
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


# ---- load_single + merge_transcripts ------------------------------------


def test_load_single_rejects_missing_case_id(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"transcript": []}), encoding="utf-8")
    with pytest.raises(ValueError):
        load_single(bad)


def test_load_single_rejects_missing_transcript_key(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"case_id": "x"}), encoding="utf-8")
    with pytest.raises(ValueError):
        load_single(bad)


def test_merge_transcripts_assigns_stable_source_idx(tmp_path):
    p1 = _write_transcript(
        tmp_path, "a.json", case_id="case_x",
        source_url="https://example.com/a", evidence_type="bodycam",
        duration=300.0, segments=[
            {"start_sec": 0.0, "end_sec": 5.0, "text": "hello"}
        ],
    )
    p2 = _write_transcript(
        tmp_path, "b.json", case_id="case_x",
        source_url="https://example.com/b", evidence_type="interrogation",
        duration=600.0, segments=[
            {"start_sec": 0.0, "end_sec": 10.0, "text": "world"}
        ],
    )
    merged = merge_transcripts([p2, p1])  # passed out of order
    # Sort is by source_url alphabetical, so 'a' becomes idx=0
    assert merged["sources"][0]["source_url"] == "https://example.com/a"
    assert merged["sources"][1]["source_url"] == "https://example.com/b"
    assert merged["sources"][0]["source_idx"] == 0
    assert merged["sources"][1]["source_idx"] == 1
    # Segments inherit their source's idx
    seg_idxs = {s["source_idx"] for s in merged["segments"]}
    assert seg_idxs == {0, 1}


def test_merge_transcripts_rejects_mixed_case_ids(tmp_path):
    p1 = _write_transcript(
        tmp_path, "a.json", case_id="case_x",
        source_url="x", evidence_type="bodycam", duration=10.0,
        segments=[],
    )
    p2 = _write_transcript(
        tmp_path, "b.json", case_id="case_y",
        source_url="y", evidence_type="bodycam", duration=10.0,
        segments=[],
    )
    with pytest.raises(ValueError, match="Multiple case_ids"):
        merge_transcripts([p1, p2])


def test_merge_transcripts_aggregates_duration(tmp_path):
    p1 = _write_transcript(tmp_path, "a.json", case_id="c",
                            source_url="a", evidence_type="bodycam",
                            duration=300.0, segments=[])
    p2 = _write_transcript(tmp_path, "b.json", case_id="c",
                            source_url="b", evidence_type="bodycam",
                            duration=600.0, segments=[])
    merged = merge_transcripts([p1, p2])
    assert merged["total_duration_sec"] == 900.0


def test_merge_transcripts_surfaces_unique_evidence_types(tmp_path):
    p1 = _write_transcript(tmp_path, "a.json", case_id="c",
                            source_url="a", evidence_type="bodycam",
                            duration=10.0, segments=[])
    p2 = _write_transcript(tmp_path, "b.json", case_id="c",
                            source_url="b", evidence_type="bodycam",
                            duration=10.0, segments=[])
    p3 = _write_transcript(tmp_path, "c.json", case_id="c",
                            source_url="c", evidence_type="interrogation",
                            duration=10.0, segments=[])
    merged = merge_transcripts([p1, p2, p3])
    assert sorted(merged["available_evidence_types"]) == ["bodycam", "interrogation"]


def test_merge_transcripts_raises_on_empty_list():
    with pytest.raises(ValueError, match="No transcript paths"):
        merge_transcripts([])


# ---- discover_case_transcripts ------------------------------------------


def test_discover_case_transcripts_groups_by_case_id(tmp_path):
    _write_transcript(tmp_path, "a.json", case_id="x",
                       source_url="a", evidence_type="bodycam",
                       duration=10.0, segments=[])
    _write_transcript(tmp_path, "b.json", case_id="x",
                       source_url="b", evidence_type="interrogation",
                       duration=10.0, segments=[])
    _write_transcript(tmp_path, "c.json", case_id="y",
                       source_url="c", evidence_type="bodycam",
                       duration=10.0, segments=[])
    groups = discover_case_transcripts(tmp_path)
    assert set(groups) == {"x", "y"}
    assert len(groups["x"]) == 2
    assert len(groups["y"]) == 1


def test_discover_case_transcripts_filters_by_case_id(tmp_path):
    _write_transcript(tmp_path, "a.json", case_id="x",
                       source_url="a", evidence_type="bodycam",
                       duration=10.0, segments=[])
    _write_transcript(tmp_path, "b.json", case_id="y",
                       source_url="b", evidence_type="bodycam",
                       duration=10.0, segments=[])
    groups = discover_case_transcripts(tmp_path, case_id="x")
    assert list(groups) == ["x"]
    assert len(groups["x"]) == 1


def test_discover_case_transcripts_skips_invalid_files(tmp_path):
    _write_transcript(tmp_path, "good.json", case_id="x",
                       source_url="a", evidence_type="bodycam",
                       duration=10.0, segments=[])
    (tmp_path / "bad.json").write_text("not json", encoding="utf-8")
    groups = discover_case_transcripts(tmp_path)
    assert set(groups) == {"x"}


# ---- format_for_llm ------------------------------------------------------


def test_format_for_llm_emits_banner_per_source(tmp_path):
    p1 = _write_transcript(
        tmp_path, "a.json", case_id="c",
        source_url="https://example.com/bwc.mp4", evidence_type="bodycam",
        duration=300.0, segments=[
            {"start_sec": 0.0, "end_sec": 5.0, "text": "officer arrives",
             "speaker": "SPK0"},
        ],
    )
    p2 = _write_transcript(
        tmp_path, "b.json", case_id="c",
        source_url="https://example.com/interview.mp3", evidence_type="interrogation",
        duration=600.0, segments=[
            {"start_sec": 12.0, "end_sec": 18.0, "text": "tell me what happened",
             "speaker": "SPK1"},
        ],
    )
    merged = merge_transcripts([p1, p2])
    rendered = format_for_llm(merged)
    assert "=== SOURCE S0:" in rendered
    assert "=== SOURCE S1:" in rendered
    assert "(bodycam," in rendered
    assert "(interrogation," in rendered


def test_format_for_llm_line_shape(tmp_path):
    p = _write_transcript(
        tmp_path, "a.json", case_id="c",
        source_url="x", evidence_type="bodycam",
        duration=300.0, segments=[
            {"start_sec": 65.5, "end_sec": 68.0, "text": "hands up",
             "speaker": "SPK0"},
        ],
    )
    merged = merge_transcripts([p])
    rendered = format_for_llm(merged)
    # Line shape: [HH:MM:SS | S0 | SPK0] hands up
    assert "[00:01:05 | S0 | SPK0] hands up" in rendered


def test_format_for_llm_handles_missing_speaker(tmp_path):
    p = _write_transcript(
        tmp_path, "a.json", case_id="c",
        source_url="x", evidence_type="bodycam",
        duration=300.0, segments=[
            {"start_sec": 5.0, "end_sec": 6.0, "text": "x"},  # no speaker
        ],
    )
    merged = merge_transcripts([p])
    rendered = format_for_llm(merged)
    assert "SPK?" in rendered


def test_format_for_llm_truncates_at_max_chars(tmp_path):
    big_text = "long " * 2000  # 10000 chars
    p = _write_transcript(
        tmp_path, "a.json", case_id="c",
        source_url="x", evidence_type="bodycam",
        duration=300.0, segments=[
            {"start_sec": 0.0, "end_sec": 1.0, "text": big_text}
        ],
    )
    merged = merge_transcripts([p])
    rendered = format_for_llm(merged, max_chars=500)
    # Truncation marker present
    assert "lines" in rendered and "chars omitted" in rendered
