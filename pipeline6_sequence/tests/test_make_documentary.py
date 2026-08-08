"""Zero-network tests for the make_documentary orchestrator (D6).

``build_plan`` is pure path arithmetic — it constructs the step sequence but
runs nothing. These tests assert the ordering invariants that matter for
correctness and safety: doc-mining before transcription (so the document can
direct the clipper), the paid fence on scoring, and the optional toggles.
"""
from __future__ import annotations

import argparse
import sys
from types import SimpleNamespace
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import make_documentary as md  # noqa: E402


def _args(**over):
    base = dict(basket=".tmp/case", case_id="vasquez_23117201", agency="Agency",
                media_dir=None, doc=None, phases="incident", kinds="bodycam",
                cold_open=None, no_align=False, skip_score=False, run=False,
                flagship=False, target_runtime=600.0,
                shape_model="deepseek/deepseek-v4-flash", judge_mock=False, judge_model=None,
                moments="beatminer", moments_model="deepseek/deepseek-v4-flash")
    base.update(over)
    return argparse.Namespace(**base)


def _labels(steps):
    return [s.label for s in steps]


def test_full_plan_with_doc_has_expected_order():
    steps = md.build_plan(_args(doc=".tmp/case/docs/d.pdf"))
    labels = _labels(steps)
    assert labels == ["stamp", "align-dashcam", "build-timeline", "doc-ocr",
                      "doc-extract", "transcribe", "mine-moments", "bridge-verdict", "render"]


def test_doc_mining_precedes_transcription():
    steps = md.build_plan(_args(doc=".tmp/case/docs/d.pdf"))
    labels = _labels(steps)
    assert labels.index("doc-extract") < labels.index("transcribe")


def test_transcribe_is_doc_directed_when_doc_given():
    steps = md.build_plan(_args(doc=".tmp/case/docs/d.pdf"))
    tr = next(s for s in steps if s.label == "transcribe")
    assert "--doc-extract" in tr.argv


def test_no_doc_skips_mining_and_doc_direction():
    steps = md.build_plan(_args(doc=None))
    labels = _labels(steps)
    assert "doc-ocr" not in labels and "doc-extract" not in labels
    tr = next(s for s in steps if s.label == "transcribe")
    assert "--doc-extract" not in tr.argv


def test_moment_step_is_the_paid_fence():
    # default moments=beatminer: the single paid step is mine-moments (bridge is free)
    steps = md.build_plan(_args())
    assert [s.label for s in steps if s.paid] == ["mine-moments"]
    # --moments p4 swaps the paid fence to the scorer
    p4 = md.build_plan(_args(moments="p4"))
    assert [s.label for s in p4 if s.paid] == ["score"]


def test_skip_score_removes_paid_step():
    steps = md.build_plan(_args(skip_score=True))
    labels = _labels(steps)
    assert "score" not in labels
    assert not any(s.paid for s in steps)
    # render still runs (reuses an existing verdict)
    assert "render" in labels


def test_no_align_drops_optional_dashcam_step():
    steps = md.build_plan(_args(no_align=True))
    assert "align-dashcam" not in _labels(steps)


def test_align_step_is_optional():
    steps = md.build_plan(_args())
    align = next(s for s in steps if s.label == "align-dashcam")
    assert align.optional is True


def test_cold_open_passed_through_to_render():
    steps = md.build_plan(_args(cold_open="climax"))
    render = next(s for s in steps if s.label == "render")
    assert "--cold-open" in render.argv
    assert "climax" in render.argv


def test_dry_run_main_executes_nothing(capsys):
    rc = md.main(["--basket", ".tmp/case", "--case-id", "x_1", "--skip-score"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "dry-run" in out
    assert "nothing executed" in out


# --- flagship chain (auto-anchor → shape → long-form render → judge gate) ---

def test_flagship_swaps_in_the_blueprint_chain():
    steps = _labels(md.build_plan(_args(doc=".tmp/case/docs/d.pdf", flagship=True)))
    assert steps[-6:] == ["blueprint", "shape", "paper-edit", "taste-veto", "judge",
                          "render-blueprint"]
    assert "render" not in steps          # the simple text-card render is replaced


def test_flagship_blueprint_auto_anchors_and_fits_runtime():
    steps = {s.label: s for s in md.build_plan(_args(flagship=True, target_runtime=720.0))}
    bp = " ".join(steps["blueprint"].argv)
    assert "--auto-anchor" in bp and "--target-runtime 720.0" in bp
    assert "--target-runtime 720.0" in " ".join(steps["render-blueprint"].argv)


def test_flagship_judge_is_a_gate():
    judge = {s.label: s for s in md.build_plan(_args(flagship=True))}["judge"]
    assert judge.abort_on_fail is True
    assert "--gate" in judge.argv


def test_flagship_pre_render_gates_precede_encoding():
    steps = md.build_plan(_args(flagship=True))
    labels = _labels(steps)
    render_i = labels.index("render-blueprint")
    for label in ("paper-edit", "taste-veto", "judge"):
        assert labels.index(label) < render_i
    assert next(s for s in steps if s.label == "taste-veto").abort_on_fail


def test_stop_at_paper_edit_omits_video_encoding():
    labels = _labels(md.build_plan(_args(flagship=True, stop_at_paper_edit=True)))
    assert "paper-edit" in labels and "taste-veto" in labels and "judge" in labels
    assert "render-blueprint" not in labels


def test_contract_inserts_thesis_gate_before_shaping():
    steps = md.build_plan(_args(flagship=True, contract=".tmp/case/d6_blueprint/c_contract.json",
                                reference_treatment="standoff_tactical_longform_v1"))
    labels = _labels(steps)
    blueprint_i = next(i for i, label in enumerate(labels) if label.startswith("blueprint"))
    assert blueprint_i < labels.index("thesis-gate") < labels.index("shape")
    assert next(s for s in steps if s.label == "thesis-gate").abort_on_fail


def test_pre_render_gate_failure_prevents_encoder(monkeypatch):
    calls = []

    def fake_run(argv, env=None):
        script = Path(argv[3]).name
        calls.append((script, list(argv)))
        if script == "taste_gate.py":
            return SimpleNamespace(returncode=3)
        return SimpleNamespace(returncode=0)

    monkeypatch.setenv("OPENROUTER_API_KEY", "test-only")
    monkeypatch.setattr(md.subprocess, "run", fake_run)
    rc = md.main(["--basket", ".tmp/case", "--case-id", "x", "--flagship",
                  "--skip-score", "--judge-mock", "--run"])
    assert rc == 3
    render_calls = [a for script, a in calls if script == "render_blueprint.py"]
    assert len(render_calls) == 1 and "--paper-edit-only" in render_calls[0]


def test_flagship_judge_mock_is_free_else_paid():
    free = {s.label: s for s in md.build_plan(_args(flagship=True, judge_mock=True))}["judge"]
    assert free.paid is False and "--mock" in free.argv
    paid = {s.label: s for s in md.build_plan(_args(flagship=True))}["judge"]
    assert paid.paid is True and "--mock" not in paid.argv


# --- moment source: beat_miner (recall) default vs P4 (precision) ----------

def test_flagship_default_moments_is_beatminer():
    labels = _labels(md.build_plan(_args(flagship=True)))
    assert "mine-moments" in labels and "bridge-verdict" in labels
    assert "score" not in labels                       # P4 replaced by beat_miner
    # order: mine -> bridge -> blueprint
    assert labels.index("mine-moments") < labels.index("bridge-verdict") < labels.index("blueprint")


def test_moments_p4_keeps_the_scorer():
    labels = _labels(md.build_plan(_args(flagship=True, moments="p4")))
    assert "score" in labels and "mine-moments" not in labels


def test_beatminer_step_is_paid_and_bridge_is_free():
    steps = {s.label: s for s in md.build_plan(_args(flagship=True))}
    assert steps["mine-moments"].paid is True
    assert steps["bridge-verdict"].paid is False       # deterministic
