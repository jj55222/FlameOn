"""Zero-network tests for the make_documentary orchestrator (D6).

``build_plan`` is pure path arithmetic — it constructs the step sequence but
runs nothing. These tests assert the ordering invariants that matter for
correctness and safety: doc-mining before transcription (so the document can
direct the clipper), the paid fence on scoring, and the optional toggles.
"""
from __future__ import annotations

import argparse
import sys
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
                      "doc-extract", "transcribe", "score", "render"]


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


def test_score_step_is_flagged_paid():
    steps = md.build_plan(_args())
    score = next(s for s in steps if s.label == "score")
    assert score.paid is True
    # nothing else is marked paid
    assert [s.label for s in steps if s.paid] == ["score"]


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
    assert steps[-4:] == ["blueprint", "shape", "render-blueprint", "judge"]
    assert "render" not in steps          # the simple text-card render is replaced


def test_flagship_blueprint_auto_anchors_and_fits_runtime():
    steps = {s.label: s for s in md.build_plan(_args(flagship=True, target_runtime=720.0))}
    bp = " ".join(steps["blueprint"].argv)
    assert "--auto-anchor" in bp and "--target-runtime 720.0" in bp
    assert "--target-runtime 720.0" in " ".join(steps["render-blueprint"].argv)


def test_flagship_judge_is_a_gate():
    judge = {s.label: s for s in md.build_plan(_args(flagship=True))}["judge"]
    assert judge.gate is True
    assert "--gate" in judge.argv


def test_flagship_judge_mock_is_free_else_paid():
    free = {s.label: s for s in md.build_plan(_args(flagship=True, judge_mock=True))}["judge"]
    assert free.paid is False and "--mock" in free.argv
    paid = {s.label: s for s in md.build_plan(_args(flagship=True))}["judge"]
    assert paid.paid is True and "--mock" not in paid.argv
