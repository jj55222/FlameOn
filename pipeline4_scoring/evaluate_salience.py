"""
evaluate_salience.py — IMMUTABLE moment-level scorer for Pipeline 4 (v0).

WHERE THIS FITS
  The existing `evaluate.py` scores a case at the *case level* (did the harness
  return the right PRODUCE/HOLD/SKIP verdict, ≥ min_key_moments, right arc?).
  It never checks whether the agent found the SPECIFIC salient moments that
  actually exist, at the right timestamps, without inventing any.

  This file closes that gap. Given:
    --golden  a human-authored answer key (schema: golden_moments in
              ../schemas/contracts.json) — the salient moments that exist.
    --pred    the harness output (a p4_to_p5_verdict JSON, or any {moments:[...]}
              / {key_moments:[...]} / bare list of moment dicts).
  it measures how well the agent's moments cover the answer key.

WHY SALIENCE-FIRST
  Recall is weighted by each golden moment's `salience` (1..5), so finding the
  moment the whole piece turns on counts for far more than a throwaway beat.
  `precision` penalises hallucinated/spurious moments — the moment-level
  faithfulness proxy. Richness that invents moments LOSES points here.

TWO MODES (auto-detected from the golden file)
  • temporal   — golden moments have start_sec set. Full scoring incl.
                 timestamp accuracy. This is the real eval.
  • provisional— golden timestamps are still null (status timestamps_pending).
                 Matches by moment_type coverage only; timestamp accuracy is
                 NOT scored and the result is flagged provisional. Lets you run
                 the harness before the footage is hand-labeled.

This file is IMMUTABLE (like evaluate.py / program.md's contract): the agent
iterates on prompts/scoring_math/extraction, NEVER on the yardstick. Pure
stdlib — no LLM, no deps.

    python evaluate_salience.py --golden golden/sac_so_20-269838_dutchess_way.golden.json --pred verdicts/run1.json
    python evaluate_salience.py --selftest          # runs the temporal-mode math on synthetic data
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

RESULTS_TSV = Path(__file__).with_name("results_salience.tsv")
TSV_COLUMNS = [
    "ts", "case_id", "mode", "salience_score",
    "salience_weighted_recall", "recall", "precision",
    "timestamp_accuracy", "type_accuracy", "must_find_recall",
    "n_golden", "n_pred", "n_matched", "hypothesis", "changes",
]

# Composite weights. Must each sum to 1.0.
W_TEMPORAL = {"swr": 0.45, "precision": 0.20, "timestamp": 0.15, "type": 0.10, "must_find": 0.10}
# Provisional mode can't score timestamps → fold that weight into salience recall.
W_PROVISIONAL = {"swr": 0.55, "precision": 0.20, "timestamp": 0.00, "type": 0.15, "must_find": 0.10}


# ---------------------------------------------------------------- normalisation

def _center(start: Optional[float], end: Optional[float]) -> Optional[float]:
    if start is None:
        return None
    return start if end is None else (start + end) / 2.0


def normalize_pred(raw) -> list:
    """Accept a p4_to_p5_verdict dict, {moments|key_moments:[...]}, or a bare list."""
    if isinstance(raw, dict):
        items = raw.get("key_moments") or raw.get("moments") or []
    elif isinstance(raw, list):
        items = raw
    else:
        items = []
    out = []
    for i, m in enumerate(items):
        start = m.get("start_sec", m.get("timestamp_sec"))
        end = m.get("end_sec", m.get("end_timestamp_sec"))
        out.append({
            "id": m.get("moment_id") or f"pred_{i}",
            "type": m.get("moment_type") or m.get("type", ""),
            "start": start,
            "end": end,
            "center": _center(start, end),
        })
    return out


def normalize_golden(g: dict) -> tuple:
    moments = []
    for m in g.get("moments", []):
        start = m.get("start_sec")
        end = m.get("end_sec")
        moments.append({
            "id": m["moment_id"],
            "type": m.get("moment_type", ""),
            "salience": int(m.get("salience", 1)),
            "must_find": bool(m.get("must_find", False)),
            "start": start,
            "end": end,
            "center": _center(start, end),
        })
    has_ts = any(m["start"] is not None for m in moments)
    return moments, ("temporal" if has_ts else "provisional")


# --------------------------------------------------------------------- matching

def _temporal_quality(p: dict, g: dict, tol: float) -> float:
    """0..1 — how well a matched prediction lines up in time with the golden moment."""
    # Window IoU when both have spans; else proximity of centers within tolerance.
    if None not in (p["start"], p["end"], g["start"], g["end"]) and p["end"] > p["start"] and g["end"] > g["start"]:
        inter = max(0.0, min(p["end"], g["end"]) - max(p["start"], g["start"]))
        union = max(p["end"], g["end"]) - min(p["start"], g["start"])
        return inter / union if union > 0 else 0.0
    if p["center"] is None or g["center"] is None:
        return 0.0
    return max(0.0, 1.0 - abs(p["center"] - g["center"]) / tol)


def match_temporal(golden: list, pred: list, tol: float) -> list:
    """Greedy, salience-first: each golden moment claims its best unused prediction
    within `tol` seconds. Returns list of (g, p_or_None, quality)."""
    used = set()
    results = []
    for g in sorted(golden, key=lambda x: -x["salience"]):
        best, best_q = None, 0.0
        for j, p in enumerate(pred):
            if j in used or p["center"] is None:
                continue
            if abs((p["center"] or 0) - (g["center"] or 0)) > tol and _temporal_quality(p, g, tol) <= 0:
                continue
            q = _temporal_quality(p, g, tol)
            if q > best_q:
                best, best_q, best_j = p, q, j
        if best is not None:
            used.add(best_j)
        results.append((g, best, best_q))
    return results


def match_provisional(golden: list, pred: list) -> list:
    """No timestamps yet: match one-to-one by moment_type. Timestamp quality unscored."""
    used = set()
    results = []
    for g in sorted(golden, key=lambda x: -x["salience"]):
        best_j = None
        for j, p in enumerate(pred):
            if j in used:
                continue
            if p["type"] == g["type"]:
                best_j = j
                break
        if best_j is not None:
            used.add(best_j)
            results.append((g, pred[best_j], None))
        else:
            results.append((g, None, None))
    return results


# ---------------------------------------------------------------------- scoring

def score(golden: list, pred: list, mode: str, tol: float) -> dict:
    matches = match_temporal(golden, pred, tol) if mode == "temporal" else match_provisional(golden, pred)

    matched = [(g, p, q) for (g, p, q) in matches if p is not None]
    missed = [g for (g, p, q) in matches if p is None]
    n_pred_matched = sum(1 for (_, p, _) in matched)

    sal_total = sum(g["salience"] for g in golden) or 1
    sal_hit = sum(g["salience"] for (g, _, _) in matched)
    swr = sal_hit / sal_total

    recall = len(matched) / len(golden) if golden else 0.0
    precision = n_pred_matched / len(pred) if pred else 0.0

    # type correctness among matched (in provisional mode types match by construction)
    type_ok = sum(1 for (g, p, _) in matched if p["type"] == g["type"])
    type_accuracy = type_ok / len(matched) if matched else 0.0

    mf_total = [g for g in golden if g["must_find"]]
    mf_hit = [g for (g, _, _) in matched if g["must_find"]]
    must_find_recall = (len(mf_hit) / len(mf_total)) if mf_total else 1.0

    if mode == "temporal":
        ts_qs = [q for (_, _, q) in matched if q is not None]
        timestamp_accuracy = (sum(ts_qs) / len(ts_qs)) if ts_qs else 0.0
        w = W_TEMPORAL
    else:
        timestamp_accuracy = None
        w = W_PROVISIONAL

    composite = (
        w["swr"] * swr
        + w["precision"] * precision
        + w["timestamp"] * (timestamp_accuracy or 0.0)
        + w["type"] * type_accuracy
        + w["must_find"] * must_find_recall
    )

    return {
        "mode": mode,
        "salience_score": round(100 * composite, 2),
        "salience_weighted_recall": round(swr, 4),
        "recall": round(recall, 4),
        "precision": round(precision, 4),
        "timestamp_accuracy": None if timestamp_accuracy is None else round(timestamp_accuracy, 4),
        "type_accuracy": round(type_accuracy, 4),
        "must_find_recall": round(must_find_recall, 4),
        "n_golden": len(golden),
        "n_pred": len(pred),
        "n_matched": len(matched),
        "missed_moment_ids": [g["id"] for g in missed],
        "missed_must_find_ids": [g["id"] for g in mf_total if g not in mf_hit],
        "spurious_pred_count": len(pred) - n_pred_matched,
    }


# -------------------------------------------------------------------------- log

def log_result(case_id: str, res: dict, hypothesis: str, changes: str) -> None:
    new = not RESULTS_TSV.exists()
    row = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "case_id": case_id, "mode": res["mode"], "salience_score": res["salience_score"],
        "salience_weighted_recall": res["salience_weighted_recall"], "recall": res["recall"],
        "precision": res["precision"], "timestamp_accuracy": res["timestamp_accuracy"],
        "type_accuracy": res["type_accuracy"], "must_find_recall": res["must_find_recall"],
        "n_golden": res["n_golden"], "n_pred": res["n_pred"], "n_matched": res["n_matched"],
        "hypothesis": hypothesis, "changes": changes,
    }
    with RESULTS_TSV.open("a", encoding="utf-8") as f:
        if new:
            f.write("\t".join(TSV_COLUMNS) + "\n")
        f.write("\t".join(str(row[c]) for c in TSV_COLUMNS) + "\n")


def _print(case_id: str, res: dict) -> None:
    print(f"\n=== salience eval — {case_id} [{res['mode']}] ===")
    if res["mode"] == "provisional":
        print("  ⚠ PROVISIONAL: golden timestamps are null (timestamps_pending).")
        print("    Matching by moment_type only; timestamp accuracy NOT scored.")
    print(f"  salience_score ............ {res['salience_score']}/100")
    print(f"  salience_weighted_recall .. {res['salience_weighted_recall']}")
    print(f"  recall .................... {res['recall']}  ({res['n_matched']}/{res['n_golden']})")
    print(f"  precision ................. {res['precision']}  (spurious preds: {res['spurious_pred_count']})")
    ta = res["timestamp_accuracy"]
    print(f"  timestamp_accuracy ........ {'n/a (provisional)' if ta is None else ta}")
    print(f"  type_accuracy ............. {res['type_accuracy']}")
    print(f"  must_find_recall .......... {res['must_find_recall']}")
    if res["missed_must_find_ids"]:
        print(f"  ✗ MISSED must-find moments: {', '.join(res['missed_must_find_ids'])}")
    elif res["missed_moment_ids"]:
        print(f"  missed (non-critical): {', '.join(res['missed_moment_ids'])}")


# --------------------------------------------------------------------- selftest

def selftest() -> int:
    """Exercise the temporal-mode math end-to-end on synthetic data (no footage needed)."""
    golden = {
        "case_id": "selftest",
        "moments": [
            {"moment_id": "g1", "moment_type": "reveal", "salience": 5, "importance": "critical", "start_sec": 100, "end_sec": 110, "must_find": True, "summary": ""},
            {"moment_id": "g2", "moment_type": "emotional_peak", "salience": 3, "importance": "high", "start_sec": 300, "end_sec": 312, "must_find": False, "summary": ""},
            {"moment_id": "g3", "moment_type": "tension_shift", "salience": 2, "importance": "medium", "start_sec": 500, "end_sec": 505, "must_find": False, "summary": ""},
        ],
    }
    gm, mode = normalize_golden(golden)
    perfect = [
        {"moment_type": "reveal", "timestamp_sec": 101, "end_timestamp_sec": 111},
        {"moment_type": "emotional_peak", "timestamp_sec": 301, "end_timestamp_sec": 313},
        {"moment_type": "tension_shift", "timestamp_sec": 500, "end_timestamp_sec": 505},
    ]
    partial = [  # finds the throwaway, misses the must-find reveal, invents one
        {"moment_type": "tension_shift", "timestamp_sec": 501, "end_timestamp_sec": 506},
        {"moment_type": "contradiction", "timestamp_sec": 999, "end_timestamp_sec": 1002},
    ]
    p = score(gm, normalize_pred(perfect), mode, tol=30)
    q = score(gm, normalize_pred(partial), mode, tol=30)
    _print("selftest/perfect", p)
    _print("selftest/partial", q)
    ok = p["salience_score"] > 90 and q["salience_score"] < p["salience_score"] and q["must_find_recall"] == 0.0
    print(f"\nselftest {'PASS' if ok else 'FAIL'}: perfect={p['salience_score']} partial={q['salience_score']}")
    return 0 if ok else 1


# -------------------------------------------------------------------------- cli

def main() -> int:
    ap = argparse.ArgumentParser(description="Moment-level salience scorer (immutable yardstick).")
    ap.add_argument("--golden", type=Path, help="golden_moments answer-key JSON")
    ap.add_argument("--pred", type=Path, help="harness output (p4_to_p5_verdict or {moments:[...]})")
    ap.add_argument("--tolerance-sec", type=float, default=30.0, help="temporal match window (default 30s)")
    ap.add_argument("--hypothesis", default="", help="what you changed, for results_salience.tsv")
    ap.add_argument("--changes", default="", help="diff summary, for results_salience.tsv")
    ap.add_argument("--log", action="store_true", help="append the result to results_salience.tsv")
    ap.add_argument("--dry-run", action="store_true", help="show what would be scored, no scoring/log")
    ap.add_argument("--selftest", action="store_true", help="run the synthetic temporal-mode check")
    args = ap.parse_args()

    if args.selftest:
        return selftest()
    if not args.golden or not args.pred:
        ap.error("--golden and --pred are required (or use --selftest)")

    g = json.loads(args.golden.read_text(encoding="utf-8"))
    case_id = g.get("case_id", args.golden.stem)
    golden, mode = normalize_golden(g)

    if args.dry_run:
        pred = normalize_pred(json.loads(args.pred.read_text(encoding="utf-8")))
        print(f"[dry-run] case={case_id} mode={mode} golden_moments={len(golden)} pred_moments={len(pred)}")
        print(f"[dry-run] tolerance={args.tolerance_sec}s; would score and "
              f"{'append to ' + RESULTS_TSV.name if args.log else 'NOT log'}")
        return 0

    pred = normalize_pred(json.loads(args.pred.read_text(encoding="utf-8")))
    res = score(golden, pred, mode, tol=args.tolerance_sec)
    _print(case_id, res)
    if args.log:
        log_result(case_id, res, args.hypothesis, args.changes)
        print(f"\nlogged → {RESULTS_TSV.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
