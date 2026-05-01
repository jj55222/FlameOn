"""Integration tests for the resolution-gate wiring inside score_case.

Exercises the orchestration layer end-to-end with mocked LLM backends
and a mocked compute_all, verifying:

  * the resolution priority chain runs (case_research / labels / pass1
    hint / default missing) and the resolved source is recorded in
    metadata;
  * the gate's verdict ceiling is applied correctly per env state;
  * the new fields land in the verdict dict (top-level
    resolution_status + four metadata fields).

These complement test_resolution_gate.py (pure-function gate +
priority-chain tests) and test_append_batch_summary.py (TSV writer
tests) by proving the wiring in score_case actually emits the right
shape -- it's the integration seam between them.

Why mock compute_all? The reconciliation block in score_case takes
det_verdict from compute_all's output. To keep gate-behaviour tests
deterministic regardless of the env-tunable PRODUCE/SKIP thresholds,
we control the deterministic verdict explicitly. Without this the
threshold defaults (env-overridable) could shift test outcomes.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pipeline4_score


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------


def _make_merged(case_id="test_case_001"):
    """Minimal merged-transcript dict that score_case accepts."""
    return {
        "case_id": case_id,
        "sources": [
            {
                "source_idx": 0,
                "source_url": "fake://test",
                "evidence_type": "bodycam",
                "duration_sec": 1800.0,
            }
        ],
        "segments": [],
        "total_duration_sec": 1800.0,
        "available_evidence_types": ["bodycam"],
        "transcript_refs": ["fake_transcript.json"],
    }


def _fake_pass1(hint=None):
    p = {
        "timeline": [],
        "moments": [
            {"source_idx": 0, "timestamp_sec": 100.0, "type": "contradiction",
             "provisional_importance": "critical"},
        ],
        "contradictions": [],
        "speaker_dynamics": [],
        "emotional_arc": [],
        "factual_anchors": [],
        "detected_structure_hint": "cold_open",
    }
    if hint is not None:
        p["resolution_status_hint"] = hint
    return p


def _fake_pass2(verdict="HOLD", resolution_status=None):
    p = {
        "verdict": verdict,
        "confidence": 0.7,
        "narrative_arc_recommendation": "cold_open",
        "final_moments": [],
        "content_pitch": "test pitch",
        "reasoning_summary": "test reasoning",
    }
    if resolution_status is not None:
        p["resolution_status"] = resolution_status
    return p


def _fake_scoring(verdict="HOLD"):
    """Deterministic scoring dict returned by patched compute_all."""
    return {
        "scoring_breakdown": {
            "moment_density_score": 50.0,
            "arc_similarity_score": 70.0,
            "artifact_completeness_score": 60.0,
            "uniqueness_score": 30.0,
        },
        "narrative_score": 60.0,
        "verdict": verdict,
        "confidence": 0.7,
        "estimated_runtime_min": 30.0,
        "missing_recommended_artifacts": [],
    }


def _fake_backend(model_name="fake-model"):
    b = MagicMock()
    b.model = model_name
    return b


def _run(monkeypatch, *, gate_env="0", pass1=None, pass2=None,
         case_research=None, labels=None, case_id="test_case_001",
         det_verdict="HOLD", llm_verdict=None):
    """Invoke score_case with mocked LLM backends, mocked compute_all,
    and (optionally) mocked load_resolution_labels.

    llm_verdict overrides pass2 verdict; det_verdict controls compute_all's
    verdict. By default both are HOLD so reconciliation = HOLD.
    """
    monkeypatch.setenv("P4_RESOLUTION_GATE", gate_env)
    if pass2 is None:
        pass2 = _fake_pass2(verdict=llm_verdict or det_verdict)
    if pass1 is None:
        pass1 = _fake_pass1()
    merged = _make_merged(case_id=case_id)
    weights = pipeline4_score.equal_weight_fallback()
    scoring = _fake_scoring(verdict=det_verdict)

    patches = [
        patch.object(pipeline4_score, "run_pass1", return_value=pass1),
        patch.object(pipeline4_score, "run_pass2", return_value=pass2),
        patch.object(pipeline4_score, "compute_all", return_value=scoring),
    ]
    if labels is not None:
        patches.append(patch.object(
            pipeline4_score, "load_resolution_labels", return_value=labels,
        ))

    for p in patches:
        p.start()
    try:
        return pipeline4_score.score_case(
            merged=merged,
            weights=weights,
            case_research=case_research,
            pass1_backend=_fake_backend(),
            pass2_backend=_fake_backend(),
            dry_run=False,
        )
    finally:
        for p in patches:
            p.stop()


# ----------------------------------------------------------------------
# Category 2: gate metadata fields appear in verdict output
# ----------------------------------------------------------------------


def test_score_case_emits_top_level_resolution_status(monkeypatch):
    verdict = _run(monkeypatch)
    assert "resolution_status" in verdict
    assert verdict["resolution_status"] in {
        "confirmed_final_outcome", "charges_filed_pending",
        "ongoing_or_unclear", "missing",
    }


def test_score_case_emits_all_four_metadata_resolution_fields(monkeypatch):
    verdict = _run(monkeypatch)
    meta = verdict["_pipeline4_metadata"]
    for k in ("resolution_source", "resolution_gate_enabled",
              "resolution_gate_applied", "pre_gate_verdict"):
        assert k in meta, f"missing key {k!r} in _pipeline4_metadata"


def test_score_case_metadata_field_types(monkeypatch):
    """Field types matter for downstream JSON consumers and TSV
    serialisation (gate_applied is rendered as 'true'/'false')."""
    verdict = _run(monkeypatch)
    meta = verdict["_pipeline4_metadata"]
    assert isinstance(meta["resolution_source"], str)
    assert isinstance(meta["resolution_gate_enabled"], bool)
    assert isinstance(meta["resolution_gate_applied"], bool)
    assert isinstance(meta["pre_gate_verdict"], str)
    assert isinstance(verdict["resolution_status"], str)


# ----------------------------------------------------------------------
# Category 3: gate OFF behavior
# ----------------------------------------------------------------------


def test_score_case_gate_off_records_status_but_does_not_change_verdict(
    monkeypatch,
):
    """Gate disabled (env=0): the resolved status is still recorded;
    pre_gate_verdict equals the emitted verdict; gate_applied is False;
    gate_enabled is False."""
    labels = {"test_case_001": {"resolution_status": "missing"}}
    verdict = _run(
        monkeypatch, gate_env="0",
        det_verdict="HOLD", llm_verdict="HOLD",
        labels=labels,
    )
    meta = verdict["_pipeline4_metadata"]
    assert meta["resolution_gate_enabled"] is False
    assert meta["resolution_gate_applied"] is False
    assert meta["pre_gate_verdict"] == verdict["verdict"]
    assert verdict["resolution_status"] == "missing"


def test_score_case_gate_off_records_status_for_pending(monkeypatch):
    """Status from labels file is recorded even when gate is OFF --
    so a later analyst can see what the gate WOULD have done."""
    labels = {"test_case_001": {"resolution_status": "charges_filed_pending"}}
    verdict = _run(
        monkeypatch, gate_env="0",
        det_verdict="HOLD", llm_verdict="HOLD",
        labels=labels,
    )
    assert verdict["resolution_status"] == "charges_filed_pending"
    assert verdict["_pipeline4_metadata"]["resolution_source"] == "labels_file"
    # Verdict not changed
    assert verdict["verdict"] == "HOLD"
    assert verdict["_pipeline4_metadata"]["resolution_gate_applied"] is False


# ----------------------------------------------------------------------
# Category 4: gate ON behavior
# ----------------------------------------------------------------------


def test_score_case_gate_on_charges_filed_pending_caps_produce_to_hold(
    monkeypatch,
):
    """The headline gate behaviour: a PRODUCE verdict against a
    charges_filed_pending case must be capped at HOLD."""
    labels = {"test_case_001": {"resolution_status": "charges_filed_pending"}}
    verdict = _run(
        monkeypatch, gate_env="1",
        det_verdict="PRODUCE", llm_verdict="PRODUCE",
        labels=labels,
    )
    meta = verdict["_pipeline4_metadata"]
    assert meta["resolution_gate_enabled"] is True
    assert meta["resolution_gate_applied"] is True
    assert meta["pre_gate_verdict"] == "PRODUCE"
    assert verdict["verdict"] == "HOLD"
    assert verdict["resolution_status"] == "charges_filed_pending"


def test_score_case_gate_on_missing_caps_produce_to_skip(monkeypatch):
    """The fail-closed guarantee: missing resolution data forces SKIP
    even from a PRODUCE-strength case."""
    labels = {"test_case_001": {"resolution_status": "missing"}}
    verdict = _run(
        monkeypatch, gate_env="1",
        det_verdict="PRODUCE", llm_verdict="PRODUCE",
        labels=labels,
    )
    assert verdict["verdict"] == "SKIP"
    assert verdict["_pipeline4_metadata"]["pre_gate_verdict"] == "PRODUCE"
    assert verdict["_pipeline4_metadata"]["resolution_gate_applied"] is True


def test_score_case_gate_on_missing_caps_hold_to_skip(monkeypatch):
    """Missing also caps HOLD -> SKIP, not just PRODUCE."""
    labels = {"test_case_001": {"resolution_status": "missing"}}
    verdict = _run(
        monkeypatch, gate_env="1",
        det_verdict="HOLD", llm_verdict="HOLD",
        labels=labels,
    )
    assert verdict["verdict"] == "SKIP"
    assert verdict["_pipeline4_metadata"]["pre_gate_verdict"] == "HOLD"


def test_score_case_gate_on_confirmed_final_outcome_allows_produce(monkeypatch):
    """Confirmed final outcome does NOT cap PRODUCE -- the gate only
    fires when the resolution is incomplete."""
    labels = {"test_case_001": {"resolution_status": "confirmed_final_outcome"}}
    verdict = _run(
        monkeypatch, gate_env="1",
        det_verdict="PRODUCE", llm_verdict="PRODUCE",
        labels=labels,
    )
    assert verdict["verdict"] == "PRODUCE"
    assert verdict["_pipeline4_metadata"]["resolution_gate_applied"] is False
    assert verdict["resolution_status"] == "confirmed_final_outcome"


def test_score_case_gate_on_no_labels_falls_back_to_default_missing(monkeypatch):
    """If no resolution source supplies a status, the default fallback
    'missing' is used and the gate caps accordingly. Belt-and-suspenders
    for cases that haven't been labeled yet."""
    verdict = _run(
        monkeypatch, gate_env="1",
        det_verdict="PRODUCE", llm_verdict="PRODUCE",
        labels={},  # empty labels file
    )
    assert verdict["resolution_status"] == "missing"
    assert verdict["_pipeline4_metadata"]["resolution_source"] == "default_missing"
    assert verdict["verdict"] == "SKIP"
    assert verdict["_pipeline4_metadata"]["resolution_gate_applied"] is True


# ----------------------------------------------------------------------
# Category 1 (integration): priority chain through score_case
# ----------------------------------------------------------------------


def test_score_case_case_research_status_takes_priority(monkeypatch):
    """case_research wins over labels file even when both supply a value."""
    case_research = {"resolution_status": "confirmed_final_outcome",
                     "sources": []}
    labels = {"test_case_001": {"resolution_status": "missing"}}
    verdict = _run(
        monkeypatch, gate_env="1",
        det_verdict="PRODUCE", llm_verdict="PRODUCE",
        case_research=case_research, labels=labels,
    )
    assert verdict["resolution_status"] == "confirmed_final_outcome"
    assert verdict["_pipeline4_metadata"]["resolution_source"] == "case_research"
    # And confirmed -> no cap, PRODUCE survives
    assert verdict["verdict"] == "PRODUCE"


def test_score_case_pass1_hint_used_when_only_source(monkeypatch):
    """Pass 1 hint is the lowest-confidence source but is still
    honoured when nothing else supplies a value."""
    pass1 = _fake_pass1(hint="ongoing_or_unclear")
    verdict = _run(
        monkeypatch, gate_env="1",
        det_verdict="PRODUCE", llm_verdict="PRODUCE",
        pass1=pass1, labels={},
    )
    assert verdict["resolution_status"] == "ongoing_or_unclear"
    assert verdict["_pipeline4_metadata"]["resolution_source"] == "pass1_hint"
    assert verdict["verdict"] == "SKIP"  # ongoing_or_unclear caps at SKIP
