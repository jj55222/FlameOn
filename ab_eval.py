"""
ab_eval.py — A/B experiment wrapper for FlameOn AutoResearch
============================================================

Runs a baseline version of research.py and a candidate version on the
SAME case slice, then diffs the resulting scores + per-component metrics
+ API cost. Scores come from the immutable evaluate.py — so this is
a fair head-to-head.

Two input modes:
  1. --baseline-py and --candidate-py: point at two different research.py
     paths. Each is copied into place as pipeline2_discovery/research.py,
     evaluate.py is invoked, and the result is captured. The original
     file is restored at the end.
  2. --baseline-ref and --candidate-ref: git refs/commits/branches. The
     runner uses `git show <ref>:pipeline2_discovery/research.py` to grab
     the file content.

Outputs:
  - runs/<stamp>_baseline/evaluate.log + results.tsv snapshot
  - runs/<stamp>_candidate/evaluate.log + results.tsv snapshot
  - runs/<stamp>_diff.json  — structured diff
  - runs/<stamp>_diff.md    — human-readable report

CLI:
    # Compare two files side-by-side
    python ab_eval.py \\
        --baseline-py /path/to/research_baseline.py \\
        --candidate-py pipeline2_discovery/research.py \\
        --tier ENOUGH \\
        --hypothesis "MuckRock has_files lane + Brave redistribution"

    # Compare two git refs
    python ab_eval.py --baseline-ref HEAD~3 --candidate-ref HEAD --tier ENOUGH

    # Dry run — print plan, no runs executed
    python ab_eval.py --baseline-py a.py --candidate-py b.py --dry-run
"""

import argparse
import json
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).parent.resolve()
RESEARCH_PY_PATH = HERE / "pipeline2_discovery" / "research.py"
EVALUATE_PY_PATH = HERE / "pipeline2_discovery" / "evaluate.py"
RESULTS_TSV = HERE / "pipeline2_discovery" / "results.tsv"
RUNS_DIR = HERE / "ab_runs"


# ─────────────────────────────────────────────────────────────
# Input resolution
# ─────────────────────────────────────────────────────────────

def _resolve_source_file(arg_path, arg_ref, label):
    """
    Return a Path to a file containing research.py source for this variant.
    Writes a temp file when resolving from git.
    """
    if arg_path:
        p = Path(arg_path).resolve()
        if not p.exists():
            print(f"[ERR] {label}-py does not exist: {p}")
            sys.exit(2)
        return p
    if arg_ref:
        # Pull the file content at the given ref
        try:
            content = subprocess.check_output(
                ["git", "show", f"{arg_ref}:pipeline2_discovery/research.py"],
                cwd=HERE, stderr=subprocess.STDOUT,
            ).decode("utf-8", errors="replace")
        except subprocess.CalledProcessError as e:
            print(f"[ERR] could not `git show {arg_ref}:pipeline2_discovery/research.py`")
            print(e.output.decode("utf-8", errors="replace") if isinstance(e.output, bytes) else e.output)
            sys.exit(2)
        tmp = RUNS_DIR / f".source_{label}_{arg_ref.replace('/','_')}.py"
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(content, encoding="utf-8")
        return tmp
    return None  # Caller interprets None as "use current file as this variant"


def _ensure_baseline_or_candidate(args):
    """At least one side must be explicit; the other defaults to current file."""
    have_base = args.baseline_py or args.baseline_ref
    have_cand = args.candidate_py or args.candidate_ref
    if not have_base and not have_cand:
        print("[ERR] must specify at least one of --baseline-py/--baseline-ref/--candidate-py/--candidate-ref")
        sys.exit(2)


# ─────────────────────────────────────────────────────────────
# Run mechanics
# ─────────────────────────────────────────────────────────────

def _swap_research_py(new_source_file):
    """
    Backup current research.py, replace with new_source_file.
    Returns (backup_path, swapped_bool).
    """
    backup = RESEARCH_PY_PATH.with_suffix(".py.abbak")
    shutil.copy2(RESEARCH_PY_PATH, backup)
    if new_source_file and new_source_file.resolve() != RESEARCH_PY_PATH.resolve():
        shutil.copy2(new_source_file, RESEARCH_PY_PATH)
        return backup, True
    return backup, False


def _restore_research_py(backup):
    if backup.exists():
        shutil.copy2(backup, RESEARCH_PY_PATH)
        try:
            backup.unlink()
        except OSError:
            pass


def _snapshot_results_tsv(out_dir):
    """Copy the post-run results.tsv into out_dir for later diffing."""
    if RESULTS_TSV.exists():
        shutil.copy2(RESULTS_TSV, out_dir / "results_tsv_snapshot.tsv")


_RESEARCH_SCORE_RE = re.compile(r"RESEARCH SCORE:\s*([0-9.]+)\s*/\s*100")
_COMPONENT_RE = re.compile(r"^\s*(Evidence Recall|Source Discovery|Precision|Tier Accuracy):\s*([0-9.]+)\s*\(", re.M)
_API_COST_RE = re.compile(r"Est\. cost:\s*\$([0-9.]+)")
_API_CALLS_RE = re.compile(r"API calls:\s*MR=(\d+)\s*CL=(\d+)\s*Brave=(\d+)\s*YT=(\d+)")


def _parse_evaluate_output(stdout):
    """Extract headline metrics from evaluate.py stdout."""
    result = {
        "research_score": None,
        "components": {},
        "api_calls": {},
        "est_cost_usd": None,
    }
    m = _RESEARCH_SCORE_RE.search(stdout)
    if m:
        try:
            result["research_score"] = float(m.group(1))
        except ValueError:
            pass
    for label, val in _COMPONENT_RE.findall(stdout):
        key = label.lower().replace(" ", "_")
        try:
            result["components"][key] = float(val)
        except ValueError:
            pass
    m = _API_COST_RE.search(stdout)
    if m:
        try:
            result["est_cost_usd"] = float(m.group(1))
        except ValueError:
            pass
    m = _API_CALLS_RE.search(stdout)
    if m:
        result["api_calls"] = {
            "muckrock": int(m.group(1)),
            "courtlistener": int(m.group(2)),
            "brave": int(m.group(3)),
            "youtube": int(m.group(4)),
        }
    return result


def run_variant(label, source_file, tier, case_filter, out_dir, hypothesis, extra_args=None):
    """
    Execute evaluate.py after swapping in `source_file` (or current file if None).
    Returns parsed metrics dict.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    backup, swapped = _swap_research_py(source_file)

    cmd = [sys.executable, str(EVALUATE_PY_PATH), "--verbose"]
    if tier:
        cmd.extend(["--tier", tier])
    if case_filter is not None:
        cmd.extend(["--case", str(case_filter)])
    if extra_args:
        cmd.extend(extra_args)
    # We DO NOT append --log — leave results.tsv pristine across A/B runs
    # (a snapshot is captured below).

    print(f"\n[{label}] Running: {' '.join(cmd)}")
    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd,
            cwd=HERE / "pipeline2_discovery",
            capture_output=True, text=True,
            timeout=3600,
        )
        elapsed = time.time() - t0
    finally:
        if swapped:
            _restore_research_py(backup)
        else:
            # Still clean up the .abbak even if not swapped
            try:
                backup.unlink()
            except OSError:
                pass

    # Persist raw output
    (out_dir / "evaluate.stdout.log").write_text(proc.stdout, encoding="utf-8")
    (out_dir / "evaluate.stderr.log").write_text(proc.stderr, encoding="utf-8")
    _snapshot_results_tsv(out_dir)

    metrics = _parse_evaluate_output(proc.stdout)
    metrics["label"] = label
    metrics["elapsed_sec"] = round(elapsed, 1)
    metrics["exit_code"] = proc.returncode
    metrics["source_file"] = str(source_file) if source_file else f"(unchanged: {RESEARCH_PY_PATH})"
    metrics["hypothesis"] = hypothesis

    with open(out_dir / "metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)

    print(f"[{label}] research_score = {metrics['research_score']} | "
          f"cost = ${metrics['est_cost_usd']} | "
          f"elapsed = {metrics['elapsed_sec']}s")
    return metrics


# ─────────────────────────────────────────────────────────────
# Diff
# ─────────────────────────────────────────────────────────────

def build_diff(baseline, candidate, stamp, hypothesis):
    def _delta(a, b):
        if a is None or b is None:
            return None
        return round(b - a, 3)

    diff = {
        "stamp": stamp,
        "hypothesis": hypothesis,
        "baseline": baseline,
        "candidate": candidate,
        "delta": {
            "research_score": _delta(baseline.get("research_score"), candidate.get("research_score")),
            "components": {
                k: _delta(baseline.get("components", {}).get(k), candidate.get("components", {}).get(k))
                for k in set(baseline.get("components", {}).keys()) | set(candidate.get("components", {}).keys())
            },
            "est_cost_usd": _delta(baseline.get("est_cost_usd"), candidate.get("est_cost_usd")),
            "api_calls": {
                k: _delta(baseline.get("api_calls", {}).get(k), candidate.get("api_calls", {}).get(k))
                for k in set(baseline.get("api_calls", {}).keys()) | set(candidate.get("api_calls", {}).keys())
            },
        },
    }
    # Simple summary verdict
    rs_delta = diff["delta"]["research_score"]
    if rs_delta is None:
        diff["summary"] = "INCOMPLETE — could not parse both runs"
    elif rs_delta > 0.5:
        diff["summary"] = f"CANDIDATE WINS by {rs_delta:+.2f} pts"
    elif rs_delta < -0.5:
        diff["summary"] = f"BASELINE WINS (candidate {rs_delta:+.2f} pts)"
    else:
        diff["summary"] = f"NEUTRAL (delta {rs_delta:+.2f} pts within noise)"
    return diff


def render_diff_md(diff):
    lines = []
    lines.append(f"# A/B Run — {diff['stamp']}")
    lines.append("")
    if diff.get("hypothesis"):
        lines.append(f"**Hypothesis:** {diff['hypothesis']}")
        lines.append("")
    lines.append(f"**Verdict:** {diff['summary']}")
    lines.append("")
    lines.append("| Metric | Baseline | Candidate | Δ |")
    lines.append("| --- | ---: | ---: | ---: |")

    def _row(label, a, b, d, fmt="{:.2f}"):
        a_s = fmt.format(a) if isinstance(a, (int, float)) else str(a)
        b_s = fmt.format(b) if isinstance(b, (int, float)) else str(b)
        d_s = f"{d:+.2f}" if isinstance(d, (int, float)) else str(d)
        lines.append(f"| {label} | {a_s} | {b_s} | {d_s} |")

    b = diff["baseline"]
    c = diff["candidate"]
    d = diff["delta"]
    _row("research_score", b.get("research_score"), c.get("research_score"), d.get("research_score"))
    for k in sorted(d["components"].keys()):
        _row(k, b["components"].get(k), c["components"].get(k), d["components"][k])
    _row("est_cost_usd", b.get("est_cost_usd"), c.get("est_cost_usd"), d.get("est_cost_usd"), fmt="${:.2f}")
    for k in sorted(d["api_calls"].keys()):
        _row(f"api.{k}", b["api_calls"].get(k), c["api_calls"].get(k), d["api_calls"][k], fmt="{:.0f}")

    lines.append("")
    lines.append(f"- Baseline source: `{b.get('source_file')}`")
    lines.append(f"- Candidate source: `{c.get('source_file')}`")
    lines.append(f"- Elapsed: baseline {b.get('elapsed_sec')}s, candidate {c.get('elapsed_sec')}s")
    lines.append("")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="A/B experiment wrapper for FlameOn research.py variants")
    parser.add_argument("--baseline-py", help="Path to baseline research.py file")
    parser.add_argument("--candidate-py", help="Path to candidate research.py file")
    parser.add_argument("--baseline-ref", help="Git ref/commit/branch for baseline research.py")
    parser.add_argument("--candidate-ref", help="Git ref/commit/branch for candidate research.py")
    parser.add_argument("--tier", choices=["ENOUGH", "BORDERLINE", "INSUFFICIENT"],
                        help="Restrict both runs to a single tier")
    parser.add_argument("--case", type=int, help="Restrict both runs to a single calibration case_id")
    parser.add_argument("--hypothesis", default="", help="Short description of what's being tested")
    parser.add_argument("--output", default=str(RUNS_DIR),
                        help=f"Output directory root (default: {RUNS_DIR})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan; don't swap files or execute evaluate.py")
    parser.add_argument("--extra-args", default="",
                        help="Extra space-separated args forwarded to evaluate.py (e.g. '--verbose')")
    args = parser.parse_args()

    _ensure_baseline_or_candidate(args)

    baseline_src = _resolve_source_file(args.baseline_py, args.baseline_ref, "baseline")
    candidate_src = _resolve_source_file(args.candidate_py, args.candidate_ref, "candidate")

    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_root = Path(args.output) / stamp
    baseline_dir = out_root / "baseline"
    candidate_dir = out_root / "candidate"

    extra_args = args.extra_args.split() if args.extra_args else []

    # Plan printout
    print(f"A/B run {stamp}")
    print(f"  Hypothesis: {args.hypothesis or '(none)'}")
    print(f"  Baseline source:  {baseline_src or '(current research.py)'}")
    print(f"  Candidate source: {candidate_src or '(current research.py)'}")
    print(f"  Tier filter: {args.tier or 'all'}")
    print(f"  Case filter: {args.case if args.case is not None else 'all'}")
    print(f"  Output: {out_root}")

    if args.dry_run:
        print("\n[DRY RUN] no files swapped, no evaluate.py executed.")
        return

    out_root.mkdir(parents=True, exist_ok=True)

    baseline_metrics = run_variant(
        "baseline", baseline_src, args.tier, args.case, baseline_dir,
        args.hypothesis, extra_args=extra_args,
    )
    candidate_metrics = run_variant(
        "candidate", candidate_src, args.tier, args.case, candidate_dir,
        args.hypothesis, extra_args=extra_args,
    )

    diff = build_diff(baseline_metrics, candidate_metrics, stamp, args.hypothesis)
    diff_path = out_root / "diff.json"
    md_path = out_root / "diff.md"
    with open(diff_path, "w", encoding="utf-8") as f:
        json.dump(diff, f, indent=2, ensure_ascii=False)
    md_path.write_text(render_diff_md(diff), encoding="utf-8")

    print(f"\n{'=' * 60}")
    print(diff["summary"])
    print(f"Wrote: {diff_path}")
    print(f"Wrote: {md_path}")


if __name__ == "__main__":
    main()
