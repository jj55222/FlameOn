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
                cold_open=None, no_align=False, skip_score=False, run=False)
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
