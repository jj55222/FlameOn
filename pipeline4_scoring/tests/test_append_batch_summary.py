"""Tests for the batch_summary.tsv writer in pipeline4_score.

Covers two pieces of new behaviour added in Batch 2:

1. The 14-column schema (original 11 columns + resolution_status,
   gate_applied, pre_gate_verdict).
2. Auto-rotate-on-mismatch -- when an existing TSV has the old 11-column
   header, the next append must rename it to a timestamped backup and
   create a fresh 14-column file. Historical rows are preserved.

These tests use real file I/O against pytest tmp_path; no mocking.
append_batch_summary does not hit any LLMs.
"""
from __future__ import annotations

import copy

import pipeline4_score


_VALID_VERDICT_FIXTURE = {
    "case_id": "test_001",
    "verdict": "HOLD",
    "resolution_status": "missing",
    "narrative_score": 42.5,
    "confidence": 0.65,
    "scoring_breakdown": {
        "moment_density_score": 30.0,
        "arc_similarity_score": 70.0,
        "artifact_completeness_score": 50.0,
        "uniqueness_score": 25.0,
    },
    "_pipeline4_metadata": {
        "n_sources": 1,
        "n_final_moments": 5,
        "scored_at": "2026-05-01T12:00:00Z",
        "resolution_gate_applied": False,
        "pre_gate_verdict": "HOLD",
    },
}


def _verdict(**overrides):
    """Return a fresh deep-copy of the valid fixture with overrides
    applied. Keys prefixed with `meta_` go into _pipeline4_metadata;
    others go to the top level."""
    v = copy.deepcopy(_VALID_VERDICT_FIXTURE)
    meta = v["_pipeline4_metadata"]
    for k, val in overrides.items():
        if k.startswith("meta_"):
            meta[k[5:]] = val
        else:
            v[k] = val
    return v



# ---- Header schema --------------------------------------------------------


def test_append_writes_14_column_header_for_fresh_file(tmp_path):
    """First write into an empty directory creates the TSV with the
    full 14-column header (matching BATCH_SUMMARY_HEADER constant)."""
    pipeline4_score.append_batch_summary(_verdict(), tmp_path)
    tsv = (tmp_path / "batch_summary.tsv").read_text(encoding="utf-8")
    header = tsv.split("\n")[0]
    cols = header.split("\t")
    assert cols == [
        "case_id", "verdict", "narrative_score", "confidence", "n_sources",
        "n_moments", "density", "arc", "artifact", "unique", "scored_at",
        "resolution_status", "gate_applied", "pre_gate_verdict",
    ]


def test_append_header_matches_module_constant(tmp_path):
    """Sanity: the header on disk must be byte-identical to
    BATCH_SUMMARY_HEADER. If they drift, auto-rotate would loop forever."""
    pipeline4_score.append_batch_summary(_verdict(), tmp_path)
    on_disk_header = (tmp_path / "batch_summary.tsv").read_text(
        encoding="utf-8"
    ).split("\n")[0] + "\n"
    assert on_disk_header == pipeline4_score.BATCH_SUMMARY_HEADER


# ---- Data row content -----------------------------------------------------


def test_append_writes_14_column_data_row(tmp_path):
    pipeline4_score.append_batch_summary(_verdict(), tmp_path)
    tsv = (tmp_path / "batch_summary.tsv").read_text(encoding="utf-8")
    rows = [r for r in tsv.split("\n") if r]
    assert len(rows) == 2
    data = rows[1].split("\t")
    assert len(data) == 14
    assert data[0] == "test_001"
    assert data[1] == "HOLD"
    assert data[11] == "missing"
    assert data[12] == "false"
    assert data[13] == "HOLD"


def test_append_emits_true_when_gate_applied(tmp_path):
    """gate_applied=True must serialise as the lowercase literal 'true',
    not 'True' or '1'. Downstream parsers depend on this form."""
    v = _verdict(meta_resolution_gate_applied=True,
                 meta_pre_gate_verdict="PRODUCE")
    pipeline4_score.append_batch_summary(v, tmp_path)
    data = (tmp_path / "batch_summary.tsv").read_text(
        encoding="utf-8"
    ).split("\n")[1].split("\t")
    assert data[12] == "true"
    assert data[13] == "PRODUCE"


def test_append_falls_back_when_metadata_keys_missing(tmp_path):
    """Robustness: a verdict from before Batch 2 (no
    resolution_gate_applied / pre_gate_verdict in metadata) still emits
    a row with safe defaults instead of crashing."""
    legacy = copy.deepcopy(_VALID_VERDICT_FIXTURE)
    legacy["_pipeline4_metadata"].pop("resolution_gate_applied")
    legacy["_pipeline4_metadata"].pop("pre_gate_verdict")
    pipeline4_score.append_batch_summary(legacy, tmp_path)
    data = (tmp_path / "batch_summary.tsv").read_text(
        encoding="utf-8"
    ).split("\n")[1].split("\t")
    assert data[12] == "false"
    assert data[13] == legacy["verdict"]


# ---- Auto-rotate on schema drift -----------------------------------------


def test_append_rotates_old_11_column_file(tmp_path):
    """When an existing TSV has the OLD 11-column header, the next
    append must rename it to a timestamped backup AND create a fresh
    14-column file. Historical rows must be preserved in the backup."""
    summary = tmp_path / "batch_summary.tsv"
    old_header = (
        "case_id\tverdict\tnarrative_score\tconfidence\tn_sources\t"
        "n_moments\tdensity\tarc\tartifact\tunique\tscored_at\n"
    )
    old_row = (
        "old_case\tHOLD\t50.0\t0.7\t1\t5\t50.0\t80.0\t60.0\t30.0\t"
        "2026-04-01T00:00:00Z\n"
    )
    summary.write_text(old_header + old_row, encoding="utf-8")

    pipeline4_score.append_batch_summary(_verdict(), tmp_path)

    new_tsv = summary.read_text(encoding="utf-8")
    assert new_tsv.split("\n")[0].split("\t")[-1] == "pre_gate_verdict"
    assert "old_case" not in new_tsv

    backups = list(tmp_path.glob("batch_summary.tsv.bak.*"))
    assert len(backups) == 1
    assert backups[0].read_text(encoding="utf-8") == old_header + old_row


def test_append_does_not_rotate_when_header_matches(tmp_path):
    """A second append against the current schema should NOT trigger
    rotation -- the existing file is already valid."""
    pipeline4_score.append_batch_summary(_verdict(), tmp_path)
    pipeline4_score.append_batch_summary(_verdict(case_id="test_002"), tmp_path)
    backups = list(tmp_path.glob("batch_summary.tsv.bak.*"))
    assert len(backups) == 0
    rows = [r for r in (tmp_path / "batch_summary.tsv").read_text(
        encoding="utf-8"
    ).split("\n") if r]
    assert len(rows) == 3


def test_append_does_not_rotate_on_first_write(tmp_path):
    """Empty directory: rotating against nothing must not produce a
    backup file."""
    pipeline4_score.append_batch_summary(_verdict(), tmp_path)
    backups = list(tmp_path.glob("batch_summary.tsv.bak.*"))
    assert len(backups) == 0


# ---- Edge cases -----------------------------------------------------------


def test_append_handles_none_verdict_silently(tmp_path):
    """append_batch_summary(None, ...) is a documented no-op (Pass 1
    error path can return None). Must not crash, must not create file."""
    pipeline4_score.append_batch_summary(None, tmp_path)
    assert not (tmp_path / "batch_summary.tsv").exists()


def test_append_works_in_caller_supplied_subdirectory(tmp_path):
    """Caller is responsible for ensuring output dir exists; this
    documents the contract via positive case."""
    target = tmp_path / "verdicts_subdir"
    target.mkdir()
    pipeline4_score.append_batch_summary(_verdict(), target)
    assert (target / "batch_summary.tsv").exists()
