"""
evaluate.py — Pipeline 4 AutoResearch scorer (IMMUTABLE)
=========================================================

Reads calibration_data.json, runs pipeline4_score.score_case() for each
entry, compares output verdicts against ground truth, and produces a
narrative_scoring_score (0-100) plus per-component metrics.

NEVER modify this file. It is the fixed evaluator that every experiment
is measured against. Agents mutate prompts.py / scoring_math.py /
pipeline4_score.py reconciliation rules; never the rubric.

CLI:
    python evaluate.py --verbose --log --hypothesis "baseline" --changes "none"
    python evaluate.py --case t6h8Uae2Q_E --verbose
    python evaluate.py --dry-run
"""

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

# Local .env so OPENROUTER_API_KEY / GEMINI_API_KEY load
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
    load_dotenv(Path(__file__).parent.parent / ".env")
    load_dotenv(Path(__file__).parent.parent / "pipeline1_winners" / ".env")
except ImportError:
    pass

SCRIPT_DIR = Path(__file__).parent.resolve()
CALIBRATION_PATH = SCRIPT_DIR / "calibration_data.json"
RESULTS_TSV = SCRIPT_DIR / "results.tsv"

# Append SCRIPT_DIR so imports work when run from any cwd
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))


# ──────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────

WEIGHTS = {
    "verdict_accuracy": 0.40,
    "narrative_calibration": 0.25,
    "arc_accuracy": 0.15,
    "moment_coverage": 0.15,
    "artifact_completeness": 0.05,
}

TIME_BUDGET_SECONDS = 3600  # 60 min max per evaluation run

VALID_ARCS = {"chronological", "cold_open", "parallel_timeline", "reveal_structure", "escalation"}
VALID_VERDICTS = {"PRODUCE", "HOLD", "SKIP"}
VALID_MOMENT_TYPES_OUT = {
    "contradiction", "emotional_peak", "procedural_violation",
    "reveal", "detail_noticed", "callback", "tension_shift",
}
VALID_IMPORTANCE = {"critical", "high", "medium", "low"}


def validate_verdict_against_schema(v):
    """
    Hand-validate a verdict dict against p4_to_p5_verdict in
    schemas/contracts.json. Returns (ok, errors).
    """
    errors = []
    if not isinstance(v, dict):
        return False, ["verdict is not a dict"]
    for f in ("case_id", "verdict", "narrative_score", "key_moments", "content_pitch"):
        if f not in v:
            errors.append(f"missing required field: {f}")
    if v.get("verdict") not in VALID_VERDICTS:
        errors.append(f"verdict must be PRODUCE|HOLD|SKIP (got {v.get('verdict')!r})")
    ns = v.get("narrative_score")
    if not isinstance(ns, (int, float)) or ns < 0 or ns > 100:
        errors.append(f"narrative_score out of [0,100]: {ns!r}")
    conf = v.get("confidence")
    if conf is not None and (not isinstance(conf, (int, float)) or conf < 0 or conf > 1):
        errors.append(f"confidence out of [0,1]: {conf!r}")
    arc = v.get("narrative_arc_recommendation")
    if arc is not None and arc not in VALID_ARCS:
        errors.append(f"narrative_arc_recommendation invalid: {arc!r}")
    moments = v.get("key_moments") or []
    if not isinstance(moments, list):
        errors.append("key_moments must be a list")
    else:
        for i, m in enumerate(moments):
            if m.get("moment_type") not in VALID_MOMENT_TYPES_OUT:
                errors.append(f"key_moments[{i}].moment_type invalid: {m.get('moment_type')!r}")
            if not isinstance(m.get("timestamp_sec"), (int, float)):
                errors.append(f"key_moments[{i}].timestamp_sec not numeric")
            if not isinstance(m.get("description"), str):
                errors.append(f"key_moments[{i}].description not string")
            if m.get("importance") not in VALID_IMPORTANCE:
                errors.append(f"key_moments[{i}].importance invalid: {m.get('importance')!r}")
    return (len(errors) == 0, errors)

WINNER_TRANSCRIPT_DIR = SCRIPT_DIR.parent / "pipeline1_winners" / "winners"


# ──────────────────────────────────────────────────────────────
# Loaders
# ──────────────────────────────────────────────────────────────

def load_calibration():
    with open(CALIBRATION_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_path(ref, base=SCRIPT_DIR):
    p = Path(ref)
    if not p.is_absolute():
        p = (base / p).resolve()
    return p


def adapt_winner_to_merged(entry):
    """
    Turn a Pipeline 1 winner transcript ({video_id, segments[]}) into a
    merged-transcript dict that pipeline4_score.score_case can consume.

    available_evidence_types is populated from the winner's profile
    artifact_combination (so artifact_completeness scoring uses real
    artifact data, not a uniform 'other' default that flattens all
    winners into the same neutral score).
    """
    path = _resolve_path(entry["transcript_path"])
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    segs_in = raw.get("segments") or []
    if not segs_in:
        return None
    total_dur = max((s.get("end_sec", 0) for s in segs_in), default=0)

    # Pull artifact_combination from the matching winner profile (sibling
    # file <case_id>.json next to the transcript). This is the discriminating
    # signal that separates true winners (4-artifact combo) from
    # admin/single-source content (1-2 artifacts).
    profile_path = path.parent / f"{entry['case_id']}.json"
    artifacts = []
    if profile_path.exists():
        try:
            with open(profile_path, "r", encoding="utf-8") as f:
                prof = json.load(f)
            artifacts = [a for a in (prof.get("artifact_combination") or []) if a]
        except (json.JSONDecodeError, OSError):
            pass
    if not artifacts:
        artifacts = ["other"]  # safe fallback

    # Build one source entry per artifact so available_evidence_types
    # populates correctly via merge_transcripts logic in score_case.
    # Each source carries the same segments — a simplification, but
    # importantly drives the artifact_completeness lookup correctly.
    sources = []
    for idx, art in enumerate(artifacts):
        sources.append({
            "source_idx": idx,
            "source_url": f"https://youtube.com/watch?v={entry['case_id']}#{art}",
            "evidence_type": art,
            "duration_sec": float(total_dur),
            "processed_duration_sec": float(total_dur),
            "transcript_path": str(path),
        })
    return {
        "case_id": entry["case_id"],
        "sources": sources,
        "segments": [{
            "source_idx": 0,  # all moments tagged to source 0; artifact set is what matters for scoring
            "start_sec": float(s.get("start_sec", 0)),
            "end_sec": float(s.get("end_sec", 0)),
            "text": (s.get("text") or "").strip(),
            "speaker": None,
            "confidence": None,
        } for s in segs_in],
        "total_duration_sec": round(float(total_dur), 3),
        "transcript_refs": [str(path)],
        "available_evidence_types": artifacts,
    }


def adapt_p3_to_merged(entry):
    """
    Adapter for P3-format transcripts. Supports:
      - entry["transcript_path"]  — single file
      - entry["transcript_paths"] — list of files (auto-merged by case_id)
      - entry["transcript_dir"]   — directory; auto-discover by case_id
    """
    from transcript_loader import merge_transcripts, discover_case_transcripts
    paths = []
    if entry.get("transcript_paths"):
        paths = [_resolve_path(p) for p in entry["transcript_paths"]]
    elif entry.get("transcript_path"):
        paths = [_resolve_path(entry["transcript_path"])]
    elif entry.get("transcript_dir"):
        td = _resolve_path(entry["transcript_dir"])
        groups = discover_case_transcripts(str(td), case_id=entry.get("case_id"))
        if groups:
            cid = entry.get("case_id") or next(iter(groups.keys()))
            paths = groups.get(cid, [])
    paths = [p for p in paths if p.exists()]
    if not paths:
        return None
    return merge_transcripts(paths)


def adapt_for_p4(entry):
    """Pick the right adapter by entry["adapter"]."""
    adapter = entry.get("adapter", "p3")
    if adapter == "winner":
        return adapt_winner_to_merged(entry)
    if adapter == "p3":
        return adapt_p3_to_merged(entry)
    raise ValueError(f"Unknown adapter: {adapter!r}")


# ──────────────────────────────────────────────────────────────
# Scoring functions
# ──────────────────────────────────────────────────────────────

def score_verdict_accuracy(cases, results):
    """% of cases where output verdict matches ground-truth verdict."""
    n_total = 0
    n_correct = 0
    for case, verdict in zip(cases, results):
        gt = (case.get("ground_truth") or {}).get("verdict")
        if gt not in VALID_VERDICTS:
            continue
        n_total += 1
        if verdict and verdict.get("verdict") == gt:
            n_correct += 1
    return (n_correct / n_total * 100) if n_total > 0 else 0.0


def score_narrative_calibration(cases, results):
    """
    % of cases where narrative_score falls in the expected band:
      - If ground_truth.min_narrative_score: score >= min
      - If ground_truth.max_narrative_score: score <= max
      - Both can be set for SKIP/HOLD-class cases that should score LOW.
    """
    n_total = 0
    n_hit = 0
    for case, verdict in zip(cases, results):
        gt = case.get("ground_truth") or {}
        mn = gt.get("min_narrative_score")
        mx = gt.get("max_narrative_score")
        if mn is None and mx is None:
            continue
        n_total += 1
        ns = (verdict or {}).get("narrative_score", 0)
        ok = True
        if mn is not None and ns < mn:
            ok = False
        if mx is not None and ns > mx:
            ok = False
        if ok:
            n_hit += 1
    return (n_hit / n_total * 100) if n_total > 0 else 0.0


def score_arc_accuracy(cases, results):
    """% of cases where arc recommendation matches expected_arc."""
    n_total = 0
    n_match = 0
    for case, verdict in zip(cases, results):
        gt = (case.get("ground_truth") or {}).get("expected_arc")
        if gt not in VALID_ARCS:
            continue
        n_total += 1
        if verdict and verdict.get("narrative_arc_recommendation") == gt:
            n_match += 1
    return (n_match / n_total * 100) if n_total > 0 else 0.0


def score_moment_coverage(cases, results):
    """% of cases returning at least min_key_moments."""
    n_total = 0
    n_hit = 0
    for case, verdict in zip(cases, results):
        gt = (case.get("ground_truth") or {}).get("min_key_moments")
        if gt is None:
            continue
        n_total += 1
        if verdict and len(verdict.get("key_moments") or []) >= gt:
            n_hit += 1
    return (n_hit / n_total * 100) if n_total > 0 else 0.0


def score_artifact_completeness(cases, results):
    """
    Jaccard-like: how much overlap between the artifacts reported as
    `available` and the `expected_artifacts`? Averaged across cases.
    """
    scores = []
    for case, verdict in zip(cases, results):
        gt = set((case.get("ground_truth") or {}).get("expected_artifacts") or [])
        if not gt:
            continue
        avail = set(((verdict or {}).get("artifact_completeness") or {}).get("available") or [])
        if not avail and not gt:
            scores.append(100.0)
            continue
        if not avail:
            scores.append(0.0)
            continue
        inter = len(gt & avail)
        union = len(gt | avail)
        scores.append((inter / union) * 100 if union > 0 else 0.0)
    if not scores:
        return 0.0
    return sum(scores) / len(scores)


# ──────────────────────────────────────────────────────────────
# Run orchestration
# ──────────────────────────────────────────────────────────────

def _load_weights(path):
    """Load Pipeline 1 scoring weights JSON. Returns None on failure."""
    if not path:
        return None
    p = Path(path)
    if not p.is_absolute():
        p = (SCRIPT_DIR / p).resolve()
    if not p.exists():
        # Try common defaults relative to repo root
        for candidate in [
            SCRIPT_DIR.parent / "pipeline1_winners" / "scoring_weights_joint.json",
            SCRIPT_DIR.parent / "pipeline1_winners" / "scoring_weights.json",
        ]:
            if candidate.exists():
                p = candidate
                break
        else:
            return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"  [WARN] failed to load weights from {p}: {e}")
        return None


def run_one_case(entry, dry_run, pass1_model, pass2_model, weights=None):
    """Adapt + score one calibration entry. Returns verdict dict or None."""
    merged = adapt_for_p4(entry)
    if merged is None:
        print(f"  [ERR] could not load transcript for {entry['case_id']}")
        return None

    # Lazy import so dry-run doesn't require API keys
    from pipeline4_score import score_case
    from llm_backends import LLMBackend

    # Backend setup (lazy — no calls yet)
    try:
        pb1 = LLMBackend(model=pass1_model)
        pb2 = LLMBackend(model=pass2_model)
    except Exception as e:
        print(f"  [ERR] backend init: {e}")
        return None

    try:
        verdict = score_case(
            merged=merged,
            weights=weights,                # P1 scoring weights if provided
            case_research=None,
            pass1_backend=pb1,
            pass2_backend=pb2,
            dry_run=dry_run,
        )
    except Exception as e:
        print(f"  [ERR] score_case: {e}")
        traceback.print_exc()
        return None
    return verdict


def evaluate(case_filter=None, verbose=False, dry_run=False,
             pass1_model=None, pass2_model=None, weights_path=None,
             parallel=1):
    cases_all = load_calibration()
    if case_filter:
        cases = [c for c in cases_all if c.get("case_id") == case_filter]
    else:
        cases = cases_all
    if not cases:
        print("No cases match filter")
        return None

    weights = _load_weights(weights_path) if weights_path is not None else None

    print(f"Pipeline 4 AutoResearch Evaluation")
    print(f"{'=' * 56}")
    print(f"Cases: {len(cases)} | Dry run: {dry_run}")
    print(f"Pass 1 model: {pass1_model or '(default)'}")
    print(f"Pass 2 model: {pass2_model or '(default)'}")
    if weights:
        print(f"Weights: loaded ({'joint' if 'moment_artifact_weights' in weights else 'standard'}; "
              f"{len(weights.get('artifact_value', {}))} artifact combos)")
    else:
        print(f"Weights: equal-weight fallback")
    if parallel > 1:
        print(f"Parallelism: {parallel} workers (per-case concurrency)")
    print()

    results = [None] * len(cases)
    start = time.time()

    def _score_indexed(idx_case):
        idx, c = idx_case
        cid = c["case_id"]
        t0 = time.time()
        v = run_one_case(c, dry_run, pass1_model, pass2_model, weights=weights)
        elapsed = time.time() - t0
        return idx, c, v, elapsed

    if parallel > 1 and not dry_run:
        # Parallel per-case execution. Print results as cases complete (out of order).
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=parallel) as exe:
            futures = {exe.submit(_score_indexed, (i, c)): i for i, c in enumerate(cases)}
            done_count = 0
            for fut in as_completed(futures):
                idx, c, v, elapsed = fut.result()
                results[idx] = v
                done_count += 1
                cid = c["case_id"]
                if v is None:
                    print(f"  [{done_count}/{len(cases)}] {cid}: FAILED ({elapsed:.1f}s)")
                else:
                    gt = (c.get("ground_truth") or {}).get("verdict", "?")
                    match = "OK" if v.get("verdict") == gt else "MISS"
                    print(f"  [{done_count}/{len(cases)}] {cid:20s} {v.get('verdict'):8s} (gt={gt}) "
                          f"score={v.get('narrative_score'):.1f} moments={len(v.get('key_moments') or [])} "
                          f"[{match}] ({elapsed:.1f}s)")
                if time.time() - start > TIME_BUDGET_SECONDS:
                    print(f"  TIME BUDGET EXCEEDED — letting in-flight workers finish, no new submits")
                    # Can't easily cancel running futures cleanly; let them complete.
    else:
        # Serial path (original). Honored when parallel=1 OR dry_run.
        for i, c in enumerate(cases):
            if time.time() - start > TIME_BUDGET_SECONDS:
                print(f"  TIME BUDGET EXCEEDED at case {i}/{len(cases)}")
                break
            cid = c["case_id"]
            print(f"  [{i+1}/{len(cases)}] {cid} ({c.get('channel', '?')}) — {c.get('title','')[:40]}")
            t0 = time.time()
            v = run_one_case(c, dry_run, pass1_model, pass2_model, weights=weights)
            t1 = time.time()
            if dry_run:
                print(f"    [DRY RUN] rendered in {t1-t0:.1f}s")
                continue
            results[i] = v
            if v is None:
                print(f"    FAILED in {t1-t0:.1f}s")
            else:
                gt = (c.get("ground_truth") or {}).get("verdict", "?")
                match = "OK" if v.get("verdict") == gt else "MISS"
                print(f"    {v.get('verdict'):8s} (gt={gt}) score={v.get('narrative_score'):.1f} moments={len(v.get('key_moments') or [])}  [{match}]  {t1-t0:.1f}s")

    if dry_run:
        print("\n[DRY RUN] No scoring performed.")
        return None

    # Component scoring
    va = score_verdict_accuracy(cases, results)
    nc = score_narrative_calibration(cases, results)
    aa = score_arc_accuracy(cases, results)
    mc = score_moment_coverage(cases, results)
    ac = score_artifact_completeness(cases, results)

    narrative_scoring_score = (
        va * WEIGHTS["verdict_accuracy"]
        + nc * WEIGHTS["narrative_calibration"]
        + aa * WEIGHTS["arc_accuracy"]
        + mc * WEIGHTS["moment_coverage"]
        + ac * WEIGHTS["artifact_completeness"]
    )

    total_time = time.time() - start
    cases_completed = sum(1 for r in results if r is not None)

    print(f"\n{'=' * 56}")
    print(f"  Verdict Accuracy:      {va:6.2f} (x{WEIGHTS['verdict_accuracy']:.0%})")
    print(f"  Narrative Calibration: {nc:6.2f} (x{WEIGHTS['narrative_calibration']:.0%})")
    print(f"  Arc Accuracy:          {aa:6.2f} (x{WEIGHTS['arc_accuracy']:.0%})")
    print(f"  Moment Coverage:       {mc:6.2f} (x{WEIGHTS['moment_coverage']:.0%})")
    print(f"  Artifact Completeness: {ac:6.2f} (x{WEIGHTS['artifact_completeness']:.0%})")
    print(f"  {'-'*48}")
    print(f"  NARRATIVE SCORING:     {narrative_scoring_score:6.2f} / 100")
    print(f"  Cases completed:       {cases_completed}/{len(cases)}")
    print(f"  Time:                  {total_time:.0f}s")

    if verbose:
        print(f"\n  Per-case breakdown:")
        for c, r in zip(cases, results):
            if r is None:
                print(f"    {c['case_id']:20s} FAILED")
                continue
            gt = c.get("ground_truth") or {}
            arc_m = "ARC" if r.get("narrative_arc_recommendation") == gt.get("expected_arc") else "arc"
            ver_m = "VRD" if r.get("verdict") == gt.get("verdict") else "vrd"
            print(f"    {c['case_id']:20s} "
                  f"{r.get('verdict'):8s} (gt {gt.get('verdict')}) "
                  f"score={r.get('narrative_score'):5.1f}/≥{gt.get('min_narrative_score','?')} "
                  f"arc={r.get('narrative_arc_recommendation')}/{gt.get('expected_arc')} "
                  f"[{ver_m} {arc_m}]")

    return {
        "narrative_scoring_score": narrative_scoring_score,
        "verdict_accuracy": va,
        "narrative_calibration": nc,
        "arc_accuracy": aa,
        "moment_coverage": mc,
        "artifact_completeness": ac,
        "cases_attempted": len(cases),
        "cases_completed": cases_completed,
        "total_seconds": total_time,
        "results": results,
    }


# ──────────────────────────────────────────────────────────────
# Experiment logging
# ──────────────────────────────────────────────────────────────

RESULTS_HEADER = [
    "experiment_id", "timestamp", "narrative_scoring_score",
    "verdict_accuracy", "narrative_calibration", "arc_accuracy",
    "moment_coverage", "artifact_completeness",
    "cases_attempted", "cases_completed", "total_seconds",
    "pass1_model", "pass2_model", "hypothesis", "changes_made", "commit_hash",
]


def _next_experiment_id():
    if not RESULTS_TSV.exists():
        return 0
    try:
        with open(RESULTS_TSV, "r", encoding="utf-8") as f:
            lines = [line for line in f.read().splitlines() if line.strip()]
        if len(lines) < 2:
            return 0
        last = lines[-1].split("\t")
        return int(last[0]) + 1
    except (ValueError, IndexError):
        return 0


def _get_commit_hash():
    try:
        import subprocess
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL, cwd=SCRIPT_DIR,
        ).decode().strip()
    except Exception:
        return ""


def log_experiment(metrics, pass1_model, pass2_model, hypothesis, changes):
    is_new = not RESULTS_TSV.exists()
    exp_id = _next_experiment_id()
    row = [
        str(exp_id),
        datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        f"{metrics['narrative_scoring_score']:.2f}",
        f"{metrics['verdict_accuracy']:.2f}",
        f"{metrics['narrative_calibration']:.2f}",
        f"{metrics['arc_accuracy']:.2f}",
        f"{metrics['moment_coverage']:.2f}",
        f"{metrics['artifact_completeness']:.2f}",
        str(metrics["cases_attempted"]),
        str(metrics["cases_completed"]),
        f"{metrics['total_seconds']:.1f}",
        pass1_model or "",
        pass2_model or "",
        hypothesis.replace("\t", " "),
        changes.replace("\t", " "),
        _get_commit_hash(),
    ]
    with open(RESULTS_TSV, "a", encoding="utf-8") as f:
        if is_new:
            f.write("\t".join(RESULTS_HEADER) + "\n")
        f.write("\t".join(row) + "\n")
    print(f"\n  Experiment {exp_id} logged to {RESULTS_TSV.name}")


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline 4 AutoResearch evaluator (immutable scorer)"
    )
    parser.add_argument("--case", help="Only evaluate this case_id")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                        help="Render prompts without API calls")
    parser.add_argument("--log", action="store_true",
                        help="Append this run to results.tsv")
    parser.add_argument("--hypothesis", default="baseline", help="Short description of what you changed")
    parser.add_argument("--changes", default="none", help="What specifically was modified since last run")
    parser.add_argument("--pass1-model", default=os.environ.get("P4_PASS1_MODEL"),
                        help="Override Pass 1 model")
    parser.add_argument("--pass2-model", default=os.environ.get("P4_PASS2_MODEL"),
                        help="Override Pass 2 model")
    parser.add_argument("--weights", default=os.environ.get("P4_WEIGHTS_PATH"),
                        help="Path to scoring_weights.json (auto-discovers ../pipeline1_winners/scoring_weights_joint.json if omitted)")
    parser.add_argument("--parallel", type=int, default=int(os.environ.get("P4_PARALLEL", "1")),
                        help="Number of cases to score concurrently (default: 1 = serial). 3-4 recommended.")
    args = parser.parse_args()

    # If --weights not given, default to the auto-discovery path
    weights_path = args.weights if args.weights is not None else "auto"

    metrics = evaluate(
        case_filter=args.case,
        verbose=args.verbose,
        dry_run=args.dry_run,
        pass1_model=args.pass1_model,
        pass2_model=args.pass2_model,
        weights_path=weights_path,
        parallel=max(1, args.parallel),
    )

    if metrics and args.log and not args.dry_run:
        log_experiment(
            metrics,
            pass1_model=args.pass1_model or "default",
            pass2_model=args.pass2_model or "default",
            hypothesis=args.hypothesis,
            changes=args.changes,
        )


if __name__ == "__main__":
    main()
