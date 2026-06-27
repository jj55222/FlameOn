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
                 produces: str = "", optional: bool = False, gate: bool = False):
        self.label = label
        self.argv = argv
        self.paid = paid
        self.produces = produces
        self.optional = optional
        self.gate = gate          # a release gate: its exit code is the run's verdict, never aborts


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
    # 6. Score (P4) — paid. Use --transcript-dir + --case-id (no shell glob in a
    #    subprocess); P4 auto-groups that case_id's transcripts into one verdict.
    if not a.skip_score:
        steps.append(Step(
            "score", _py(P4 / "pipeline4_score.py", "--force",
                         "--transcript-dir", str(d2 / "transcripts"),
                         "--case-id", a.case_id,
                         "--weights", str(ROOT / "pipeline1_winners" / "scoring_weights.json"),
                         "--output", str(d2 / "verdicts")),
            paid=True, produces=str(verdict)))
    # 7. Render the rough cut.
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
    ap.add_argument("--skip-score", action="store_true", help="reuse an existing P4 verdict (no paid call)")
    ap.add_argument("--run", action="store_true", help="actually execute (default is dry-run plan only)")
    args = ap.parse_args(argv)

    steps = build_plan(args)
    have_key = bool(os.environ.get("OPENROUTER_API_KEY"))
    _print_plan(steps, have_key)

    if not args.run:
        print("[dry-run] nothing executed. Re-run with --run to build the cut.")
        return 0

    for i, s in enumerate(steps, 1):
        if s.paid and not have_key:
            print(f"[stop] step {i} ({s.label}) needs OPENROUTER_API_KEY and it is not set.\n"
                  f"       Set the key inline, or pass --skip-score to reuse an existing verdict.")
            return 2
        print(f"\n=== [{i}/{len(steps)}] {s.label} ===")
        env = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
        r = subprocess.run(s.argv, env=env)
        if r.returncode != 0:
            if s.optional:
                print(f"[warn] optional step '{s.label}' failed (rc={r.returncode}); continuing.")
                continue
            print(f"[fail] step '{s.label}' exited {r.returncode}; aborting.")
            return r.returncode
    print("\n[done] basket → rough cut complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
