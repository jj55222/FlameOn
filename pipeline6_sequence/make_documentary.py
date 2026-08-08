"""P3.5→P6 / GOAL_D D6 — one command: a media basket → a rough cut.

Chains the whole documentary engine so an operator runs ONE command instead of
the eight-step recipe by hand:

    stamp → (align dashcams) → build timeline → mine doc → transcribe
    (doc-directed) → score (P4) → render rough cut

Design rules baked in:
  * **Dry-run by default.** Prints the exact command plan and exits. Nothing
    runs, no media is touched, no paid API is called, until ``--run``.
  * **The paid step is fenced.** P4 scoring needs ``OPENROUTER_API_KEY``; if it
    is absent the plan still prints but ``--run`` stops before scoring (unless
    ``--skip-score`` reuses an existing verdict). The key is read from the
    environment only — never written, never logged.
  * **Doc before transcribe.** The case document is mined first so its
    clip_directions can DIRECT the targeted transcription (closes the D5 loop).
  * Global Python, ``-X utf8`` forced for children (Windows cp1252 guard).

    python pipeline6_sequence/make_documentary.py --basket .tmp/sac_poc \
        --case-id vasquez_23117201 --agency "Sacramento County Sheriff" \
        --doc ".tmp/sac_poc/docs_23117201/23-117201 Documents.pdf"
    # ...inspect the plan, then re-run with --run (set OPENROUTER_API_KEY first).
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

# Repo-root-relative locations of the pipeline scripts.
ROOT = Path(__file__).resolve().parent.parent
P3 = ROOT / "pipeline3_audio"
P4 = ROOT / "pipeline4_scoring"
P6 = ROOT / "pipeline6_sequence"


class Step:
    """One pipeline invocation: a label, the argv, and whether it needs the
    paid OpenRouter key. ``produces`` documents the artifact it writes."""

    def __init__(self, label: str, argv: List[str], *, paid: bool = False,
                 produces: str = "", optional: bool = False, gate: bool = False,
                 abort_on_fail: bool = False):
        self.label = label
        self.argv = argv
        self.paid = paid
        self.produces = produces
        self.optional = optional
        self.gate = gate          # release/report gate; retained for post-render compatibility
        self.abort_on_fail = abort_on_fail  # a pre-render gate: non-zero stops the chain immediately


def _py(script: Path, *args: str) -> List[str]:
    """A child Python invocation with -X utf8 (Windows console guard)."""
    return [sys.executable, "-X", "utf8", str(script), *args]


def build_plan(a: argparse.Namespace) -> List[Step]:
    basket = Path(a.basket)
    video_dir = Path(a.media_dir) if a.media_dir else basket / "video" / "Video"
    tdir = basket / "timeline"
    arts = tdir / "artifacts.json"
    timeline = tdir / "case_timeline.json"
    d2 = basket / "d2"
    verdict = d2 / "verdicts" / f"{a.case_id}_verdict.json"
    docs_dir = Path(a.doc).parent if a.doc else None
    doc_ocr_dir = (docs_dir / "ocr") if docs_dir else None
    doc_pages = (doc_ocr_dir / "pages.json") if doc_ocr_dir else None
    doc_extract = (docs_dir / "doc_extract.json") if docs_dir else None

    steps: List[Step] = []
    # 1. Stamp every artifact with an absolute time.
    steps.append(Step(
        "stamp", _py(P3 / "timeline_stamp.py", "--basket", str(basket / "video"),
                     "--out", str(tdir)),
        produces=str(arts)))
    # 2. (optional) Align un-clocked dashcams by audio cross-correlation.
    if not a.no_align:
        steps.append(Step(
            "align-dashcam", _py(P3 / "timeline_align_dashcam.py",
                                 "--artifacts", str(arts)),
            produces=str(arts), optional=True))
    # 3. Bucket artifacts into phases.
    steps.append(Step(
        "build-timeline", _py(P3 / "timeline_build.py", "--artifacts", str(arts),
                              "--case-id", a.case_id, "--out", str(timeline)),
        produces=str(timeline)))
    # 4. Mine the case document (so its clip_directions can direct transcription).
    if a.doc:
        steps.append(Step(
            "doc-ocr", _py(P3 / "doc_ocr.py", "--pdf", str(a.doc),
                           "--out", str(doc_ocr_dir)),
            produces=str(doc_pages)))
        steps.append(Step(
            "doc-extract", _py(P3 / "doc_extract.py", "--pages", str(doc_pages),
                               "--out", str(doc_extract)),
            produces=str(doc_extract)))
    # 5. Targeted transcription of the chosen phases (doc-directed when available).
    tr_argv = ["--artifacts", str(arts), "--timeline", str(timeline),
               "--phases", a.phases, "--case-id", a.case_id, "--out", str(d2)]
    if a.kinds:
        tr_argv += ["--kinds", a.kinds]
    if a.doc:
        tr_argv += ["--doc-extract", str(doc_extract)]
    steps.append(Step(
        "transcribe", _py(P3 / "timeline_transcribe.py", *tr_argv),
        produces=str(d2 / "transcripts")))
    # 6. Moments → verdict — paid. TWO sources, by --moments:
    #    beatminer (default): beat_miner (RECALL) → bridge_verdict. RIGHT for documentary
    #       assembly — surfaces ALL grounded moments so the cut fills its runtime.
    #    p4: pipeline4_score (PRECISION / case-selection). Caps ~top-10 moments → a thin,
    #       draggy cut; it's a triage tool ("should I produce this case?"), not an assembler.
    if not a.skip_score:
        if a.moments == "p4":
            steps.append(Step(
                "score", _py(P4 / "pipeline4_score.py", "--force",
                             "--transcript-dir", str(d2 / "transcripts"), "--case-id", a.case_id,
                             "--weights", str(ROOT / "pipeline1_winners" / "scoring_weights.json"),
                             "--output", str(d2 / "verdicts")),
                paid=True, produces=str(verdict)))
        else:  # beatminer (default)
            mined = d2 / "mined.json"
            steps.append(Step(
                "mine-moments", _py(P4 / "beat_miner.py", "--transcripts", str(d2 / "transcripts"),
                             "--case-id", a.case_id, "--agency", a.agency,
                             "--model", a.moments_model, "--out", str(mined)),
                paid=True, produces=str(mined)))
            steps.append(Step(
                "bridge-verdict",
                _py(P6 / "bridge_verdict.py", str(mined), str(verdict), str(d2 / "transcripts")),
                produces=str(verdict)))
    if a.flagship:
        # FLAGSHIP: deterministic editorial spine → thesis gate → shaping →
        # final-equivalent paper edit → deterministic veto → one critic → encode.
        # Every pre-render gate can actually abort; --stop-at-paper-edit omits encode.
        bp_dir = basket / "d6_blueprint"
        blueprint_json = bp_dir / f"{a.case_id}_blueprint.json"
        shaped_json = bp_dir / f"{a.case_id}_blueprint_shaped.json"
        cuts_dir = basket / "d6_cuts"
        cut_dir = cuts_dir / a.case_id
        # render_blueprint writes the paper_edit to the out-dir ROOT, the mp4 to a
        # per-case subdir under it.
        paper_edit = cuts_dir / f"{a.case_id}_paper_edit.json"
        final_mp4 = cut_dir / f"{a.case_id}_rough_cut.mp4"
        judge_json = cut_dir / f"{a.case_id}_judge.json"
        taste_json = cuts_dir / f"VALIDATION_{a.case_id}.json"
        T = str(a.target_runtime)
        reference_treatment = getattr(a, "reference_treatment", None)
        legacy_template = getattr(a, "template", None)
        contract_path = Path(getattr(a, "contract", None)) if getattr(a, "contract", None) else None
        taste_rules = Path(getattr(a, "taste_rules", None) or (P6 / "TASTE_RULES.json"))
        fmt = getattr(a, "format", "longform")
        platform = getattr(a, "platform", "youtube")

        bp_argv = ["--artifacts", str(arts), "--timeline", str(timeline),
                   "--verdict", str(verdict), "--media-dir", str(video_dir),
                   "--agency", a.agency, "--target-runtime", T,
                   "--auto-anchor", "--out", str(bp_dir)]
        if a.doc:
            bp_argv += ["--doc-extract", str(doc_extract)]
        bp_label = "blueprint"
        if reference_treatment:
            bp_argv += ["--reference-treatment", reference_treatment]
            bp_label = f"blueprint [treatment: {reference_treatment}]"
        elif legacy_template:
            bp_argv += ["--template", legacy_template]
            bp_label = f"blueprint [template: {legacy_template}]"
        if getattr(a, "snap_transients", False):
            bp_argv.append("--snap-transients")
            bp_label += " +snap"
        steps.append(Step(bp_label, _py(P6 / "blueprint.py", *bp_argv),
                          produces=str(blueprint_json)))

        if contract_path:
            thesis_out = bp_dir / f"{a.case_id}_thesis_validation.json"
            thesis_argv = ["--blueprint", str(blueprint_json), "--contract", str(contract_path),
                           "--out", str(bp_dir), "--gate"]
            if reference_treatment and Path(reference_treatment).suffix == ".json":
                thesis_argv += ["--reference-treatment", reference_treatment]
            if getattr(a, "thesis_model", None) and not getattr(a, "thesis_mock", False):
                thesis_argv += ["--model", a.thesis_model]
            steps.append(Step(
                "thesis-gate", _py(P6 / "thesis_gate.py", *thesis_argv),
                paid=bool(getattr(a, "thesis_model", None) and not getattr(a, "thesis_mock", False)),
                produces=str(thesis_out), abort_on_fail=True))

        shape_argv = ["--blueprint", str(blueprint_json), "--model", a.shape_model,
                      "--out", str(bp_dir)]
        if reference_treatment and Path(reference_treatment).suffix == ".json":
            shape_argv += ["--reference-treatment", reference_treatment]
        steps.append(Step(
            "shape", _py(P6 / "blueprint_shape.py", *shape_argv),
            paid=True, produces=str(shaped_json)))

        rb_argv = ["--blueprint", str(shaped_json), "--media-dir", str(video_dir),
                   "--transcripts", str(d2 / "transcripts"),
                   "--target-runtime", T, "--out", str(cuts_dir)]
        steps.append(Step("paper-edit", _py(P6 / "render_blueprint.py", *rb_argv,
                                             "--paper-edit-only"), produces=str(paper_edit)))

        taste_argv = ["--blueprint", str(shaped_json), "--paper-edit", str(paper_edit),
                      "--rules", str(taste_rules), "--format", fmt, "--platform", platform,
                      "--out", str(cuts_dir), "--gate"]
        steps.append(Step("taste-veto", _py(P6 / "taste_gate.py", *taste_argv),
                          produces=str(taste_json), abort_on_fail=True))

        judge_argv = ["--bundle", str(shaped_json), "--paper-edit", str(paper_edit),
                      "--taste-report", str(taste_json), "--enum-only",
                      "--gate", "--out", str(judge_json)]
        if a.judge_mock:
            judge_argv.append("--mock")
        elif a.judge_model:
            judge_argv += ["--model", a.judge_model]
        steps.append(Step("judge", _py(P6 / "judge.py", *judge_argv),
                          paid=not a.judge_mock, produces=str(judge_json), abort_on_fail=True))
        if not getattr(a, "stop_at_paper_edit", False):
            steps.append(Step("render-blueprint", _py(P6 / "render_blueprint.py", *rb_argv),
                              produces=str(final_mp4)))
        return steps

    # 7. Render the SIMPLE rough cut (text-card path).
    rc_argv = ["--verdict", str(verdict), "--media-dir", str(video_dir),
               "--timeline", str(timeline), "--agency", a.agency,
               "--out", str(basket / "d6_cuts")]
    if a.doc:
        rc_argv += ["--doc-extract", str(doc_extract)]
    if a.cold_open:
        rc_argv += ["--cold-open", a.cold_open]
    steps.append(Step(
        "render", _py(P6 / "render_rough_cut.py", *rc_argv),
        produces=str(basket / "d6_cuts" / a.case_id / f"{a.case_id}_rough_cut.mp4")))
    return steps


def _show(arg: str) -> str:
    """Quote an arg for display only (real execution passes a list, no shell)."""
    return f'"{arg}"' if (" " in arg or not arg) else arg


def _print_plan(steps: List[Step], have_key: bool) -> None:
    print("documentary plan  (basket → rough cut):\n")
    for i, s in enumerate(steps, 1):
        tag = " [PAID]" if s.paid else (" [optional]" if s.optional else "")
        gate = ""
        if s.paid and not have_key:
            gate = "   ⚠ needs OPENROUTER_API_KEY (set it or pass --skip-score)"
        print(f"  {i}. {s.label}{tag}{gate}")
        print(f"       {' '.join(_show(x) for x in s.argv)}")
        if s.produces:
            print(f"       → {s.produces}")
    print()


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="GOAL_D D6 — one command: media basket → documentary rough cut")
    ap.add_argument("--basket", required=True, help="case working dir (holds video/, timeline/, d2/, ...)")
    ap.add_argument("--case-id", required=True)
    ap.add_argument("--agency", default="Releasing agency")
    ap.add_argument("--media-dir", default=None, help="override; default <basket>/video/Video")
    ap.add_argument("--doc", default=None, help="case PDF to mine (enables doc-directed clip + outcome card)")
    ap.add_argument("--phases", default="incident", help="timeline phases to transcribe (comma list)")
    ap.add_argument("--kinds", default="bodycam", help="artifact-kind filter for transcription")
    ap.add_argument("--cold-open", default=None, choices=["scene_set", "climax", "none"],
                    help="render cold-open style (passed through to the renderer)")
    ap.add_argument("--no-align", action="store_true", help="skip dashcam audio alignment")
    ap.add_argument("--skip-score", action="store_true", help="reuse an existing verdict (no paid moments call)")
    ap.add_argument("--moments", choices=["beatminer", "p4"], default="beatminer",
                    help="moment source for the cut: beatminer = recall (default, fills the runtime); "
                         "p4 = pipeline4_score precision/case-selection (thinner)")
    ap.add_argument("--moments-model", default="deepseek/deepseek-v4-flash",
                    help="OpenRouter model for beat_miner moment proposal (high-recall)")
    ap.add_argument("--flagship", action="store_true",
                    help="flagship chain: blueprint(--auto-anchor) → shape → long-form render"
                         "(--target-runtime) → judge gate, instead of the simple text-card cut")
    ap.add_argument("--target-runtime", type=float, default=600.0,
                    help="flagship cut length in seconds (auto-fit, never padded); default 600")
    ap.add_argument("--template", default=None,
                    help="flagship act-skeleton template (name in pipeline6_sequence/templates/, "
                         "e.g. solvedfiles_standoff | solvedfiles_discovery, or a path to a .json)")
    ap.add_argument("--reference-treatment", default=None,
                    help="flagship v1 treatment id/path; replaces the generic chronological skeleton")
    ap.add_argument("--contract", default=None,
                    help="persisted upstream W1 contract; enables the pre-shaping thesis gate")
    ap.add_argument("--taste-rules", default=None,
                    help="operator-authored TASTE_RULES.json (default pipeline6_sequence/TASTE_RULES.json)")
    ap.add_argument("--format", choices=["longform", "shortform"], default="longform")
    ap.add_argument("--platform", choices=["youtube", "tiktok", "instagram"], default="youtube")
    ap.add_argument("--thesis-model", default=None,
                    help="optional one-call live thesis critic; omitted = deterministic thesis gate")
    ap.add_argument("--thesis-mock", action="store_true",
                    help="force the thesis gate to stay deterministic even when a model is configured")
    ap.add_argument("--stop-at-paper-edit", action="store_true",
                    help="produce blueprint, validation reports, paper edit and critic report; never encode video")
    ap.add_argument("--snap-transients", action="store_true",
                    help="flagship: snap salient force-onset beats to the audio bang (ffmpeg; opt-in)")
    ap.add_argument("--shape-model", default="deepseek/deepseek-v4-flash",
                    help="OpenRouter model for the editorial shaping tier")
    ap.add_argument("--judge-mock", action="store_true",
                    help="gate on the deterministic craft report only (no paid judge call)")
    ap.add_argument("--judge-model", default=None, help="OpenRouter model for the LLM judge")
    ap.add_argument("--run", action="store_true", help="actually execute (default is dry-run plan only)")
    args = ap.parse_args(argv)

    steps = build_plan(args)
    have_key = bool(os.environ.get("OPENROUTER_API_KEY"))
    _print_plan(steps, have_key)

    if not args.run:
        print("[dry-run] nothing executed. Re-run with --run to build the cut.")
        return 0

    gate_code = 0
    for i, s in enumerate(steps, 1):
        if s.paid and not have_key:
            print(f"[stop] step {i} ({s.label}) needs OPENROUTER_API_KEY and it is not set.\n"
                  f"       Set the key inline, or pass --skip-score / --judge-mock as appropriate.")
            return 2
        print(f"\n=== [{i}/{len(steps)}] {s.label} ===")
        env = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
        r = subprocess.run(s.argv, env=env)
        if s.abort_on_fail and r.returncode != 0:
            print(f"[gate] '{s.label}' -> HOLD (pre-render abort; rc={r.returncode})")
            return r.returncode
        if s.gate:
            # The release gate's exit code IS the run's verdict — the cut is already
            # rendered, so we never abort on it; we report and propagate it.
            gate_code = r.returncode
            print(f"[gate] '{s.label}' -> {'PASS (emit)' if gate_code == 0 else 'HOLD (rework)'}")
            continue
        if r.returncode != 0:
            if s.optional:
                print(f"[warn] optional step '{s.label}' failed (rc={r.returncode}); continuing.")
                continue
            print(f"[fail] step '{s.label}' exited {r.returncode}; aborting.")
            return r.returncode
    done_msg = ("flagship paper edit + gates" if args.flagship and args.stop_at_paper_edit
                else "flagship cut + gates" if args.flagship else "rough cut")
    print(f"\n[done] basket → {done_msg} complete"
          + (f"  (gate: {'PASS' if gate_code == 0 else 'HOLD — needs rework'})" if args.flagship else "")
          + ".")
    return gate_code


if __name__ == "__main__":
    raise SystemExit(main())
