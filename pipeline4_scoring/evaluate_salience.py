"""
evaluate_salience.py — IMMUTABLE moment-level scorer for Pipeline 4 (v0.1).

WHERE THIS FITS
  `evaluate.py` scores a case at the *case level* (right PRODUCE/HOLD/SKIP
  verdict, ≥ min_key_moments, right arc). It never checks whether the agent
  found the SPECIFIC salient moments that exist, at the right timestamps,
  without inventing any. This file closes that gap.

OPEN-WORLD scoring (the v0.1 idea)
  The answer key is never complete — a good harness will surface compelling
  moments we didn't pre-label. So we do NOT punish "moment not in golden".
  The discriminator between a *good novel angle* and a *hallucination* is
  GROUNDING, not golden-membership:
    • grounded   — the moment's quote resolves in the real P3 transcript
                   (near-verbatim). Real. If it's not in golden, it's a
                   DISCOVERY → surfaced for promotion into the golden set.
    • ungrounded — invented quote, no source. THIS is the hallucination we
                   punish (it drags down grounding_precision).
  This mirrors the p6_blueprint.integrity_ledger contract: every claim → source.

INPUTS
  --golden      golden_moments answer key (schema in ../schemas/contracts.json)
  --pred        harness output: a p4_to_p5_verdict JSON, or {moments|key_moments:[...]},
                or a bare list. Each moment should carry a quote
                (transcript_excerpt / evidence_quote / description) for grounding.
  --transcript  one or more P3 transcript JSONs (file or dir, repeatable). When
                provided, precision becomes GROUNDING precision and discoveries
                are computed. Without it, falls back to closed-world precision.

SCORE  (salience-first composite, 0..100)
    salience_weighted_recall   0.45   did it find the high-salience known beats?
    precision/grounding        0.20   are returned moments REAL (not invented)?
    timestamp_accuracy         0.15   how tight is the time match? (temporal mode)
    type_accuracy              0.10   right moment_type among matched
    must_find_recall           0.10   gate: the moments a cut must not omit
  Optional --novelty-bonus adds up to +5 (capped) for grounded high-importance
  discoveries — provisional until a salience judge confirms them.

MODES (auto-detected from the golden file)
  • temporal    — golden moments have start_sec → full scoring incl. timestamps.
  • provisional — golden timestamps still null → match by moment_type only,
                  timestamp accuracy NOT scored, result flagged provisional.

This file is IMMUTABLE: the agent iterates on prompts/scoring_math/extraction,
never on the yardstick. Pure stdlib — no LLM, no deps. The grounding helpers
are exposed for re-use by the beat-miner so faithfulness is checked one way.

    python evaluate_salience.py --golden golden/sac_so_20-269838_dutchess_way.golden.json \
        --pred verdicts/run1.json --transcript ../pipeline3_audio/transcripts/
    python evaluate_salience.py --selftest
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

RESULTS_TSV = Path(__file__).with_name("results_salience.tsv")
TSV_COLUMNS = [
    "ts", "case_id", "mode", "salience_score",
    "salience_weighted_recall", "recall", "precision",
    "timestamp_accuracy", "type_accuracy", "must_find_recall",
    "n_golden", "n_pred", "n_matched", "n_discoveries", "n_ungrounded",
    "hypothesis", "changes",
]

# Composite weights. Each set sums to 1.0. The "precision" slot holds grounding
# precision when a transcript is supplied, else closed-world precision.
W_TEMPORAL = {"swr": 0.45, "precision": 0.20, "timestamp": 0.15, "type": 0.10, "must_find": 0.10}
W_PROVISIONAL = {"swr": 0.55, "precision": 0.20, "timestamp": 0.00, "type": 0.15, "must_find": 0.10}

NOVELTY_PER = 2.0      # points per grounded high-importance discovery
NOVELTY_CAP = 5.0      # hard cap so novelty can't dominate or be farmed
FUZZY_GROUND_THRESH = 0.8   # token-overlap fallback when quote isn't a verbatim substring
MIN_QUOTE_CHARS = 12        # quotes shorter than this can't be grounded reliably


# ----------------------------------------------------------------- grounding
# Shared faithfulness check: does a quote actually appear in the evidence?

def _norm(s: str) -> str:
    """Lowercase, strip accents/punctuation, collapse whitespace."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9 ]+", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()


def load_transcript_blob(paths: list) -> tuple:
    """Load P3 transcript JSON(s) → (normalized_blob_str, token_set).
    `paths` may be files or dirs (dirs are globbed for *.json)."""
    files = []
    for p in paths or []:
        p = Path(p)
        if p.is_dir():
            files.extend(sorted(p.glob("*.json")))
        elif p.exists():
            files.append(p)
    texts = []
    for f in files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        segs = data.get("transcript", data) if isinstance(data, dict) else data
        if isinstance(segs, list):
            for seg in segs:
                if isinstance(seg, dict) and seg.get("text"):
                    texts.append(seg["text"])
    blob = _norm(" ".join(texts))
    return blob, set(blob.split())


def grounding_of(quote: str, blob: str, tokens: set) -> Optional[str]:
    """Return 'strong' (verbatim substring), 'fuzzy' (token overlap), or None
    (ungrounded → hallucination). The anti-fabrication check."""
    q = _norm(quote)
    if not blob or len(q) < MIN_QUOTE_CHARS:
        return None
    if q in blob:
        return "strong"
    qtok = set(q.split())
    if qtok and len(qtok & tokens) / len(qtok) >= FUZZY_GROUND_THRESH:
        return "fuzzy"
    return None


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
            "importance": m.get("importance", ""),
            "quote": m.get("transcript_excerpt") or m.get("evidence_quote") or m.get("description", ""),
            "start": start,
            "end": end,
            "center": _center(start, end),
            "grounding": None,
        })
    return out


def normalize_golden(g: dict) -> tuple:
    moments = []
    for m in g.get("moments", []):
        start, end = m.get("start_sec"), m.get("end_sec")
        moments.append({
            "id": m["moment_id"],
            "type": m.get("moment_type", ""),
            "salience": int(m.get("salience", 1)),
            "must_find": bool(m.get("must_find", False)),
            "start": start, "end": end, "center": _center(start, end),
        })
    has_ts = any(m["start"] is not None for m in moments)
    return moments, ("temporal" if has_ts else "provisional")


# --------------------------------------------------------------------- matching

def _temporal_quality(p: dict, g: dict, tol: float) -> float:
    if None not in (p["start"], p["end"], g["start"], g["end"]) and p["end"] > p["start"] and g["end"] > g["start"]:
        inter = max(0.0, min(p["end"], g["end"]) - max(p["start"], g["start"]))
        union = max(p["end"], g["end"]) - min(p["start"], g["start"])
        return inter / union if union > 0 else 0.0
    if p["center"] is None or g["center"] is None:
        return 0.0
    return max(0.0, 1.0 - abs(p["center"] - g["center"]) / tol)


def match_temporal(golden: list, pred: list, tol: float) -> list:
    used = set()
    results = []
    for g in sorted(golden, key=lambda x: -x["salience"]):
        best, best_j, best_q = None, None, 0.0
        for j, p in enumerate(pred):
            if j in used or p["center"] is None:
                continue
            q = _temporal_quality(p, g, tol)
            if q > best_q:
                best, best_j, best_q = p, j, q
        if best is not None:
            used.add(best_j)
        results.append((g, best, best_q))
    return results


def match_provisional(golden: list, pred: list) -> list:
    used = set()
    results = []
    for g in sorted(golden, key=lambda x: -x["salience"]):
        hit = None
        for j, p in enumerate(pred):
            if j not in used and p["type"] == g["type"]:
                hit, used = p, used | {j}
                break
        results.append((g, hit, None))
    return results


# ---------------------------------------------------------------------- scoring

def score(golden: list, pred: list, mode: str, tol: float,
          ground: Optional[tuple] = None, novelty: bool = False) -> dict:
    # Grounding pass (open-world precision) — only when a transcript is supplied.
    grounded_flagged = ground is not None
    if grounded_flagged:
        blob, tokens = ground
        for p in pred:
            p["grounding"] = grounding_of(p["quote"], blob, tokens)

    matches = match_temporal(golden, pred, tol) if mode == "temporal" else match_provisional(golden, pred)
    matched = [(g, p, q) for (g, p, q) in matches if p is not None]
    missed = [g for (g, p, q) in matches if p is None]
    matched_pred_ids = {id(p) for (_, p, _) in matched}

    sal_total = sum(g["salience"] for g in golden) or 1
    swr = sum(g["salience"] for (g, _, _) in matched) / sal_total
    recall = len(matched) / len(golden) if golden else 0.0

    if grounded_flagged:
        n_grounded = sum(1 for p in pred if p["grounding"])
        precision = n_grounded / len(pred) if pred else 0.0           # GROUNDING precision
        discoveries = [p for p in pred if id(p) not in matched_pred_ids
                       and p["grounding"] and p["importance"] in ("critical", "high")]
        hallucinations = [p for p in pred if p["grounding"] is None]
    else:
        precision = len(matched_pred_ids) / len(pred) if pred else 0.0  # closed-world fallback
        discoveries, hallucinations = [], []

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
    base = 100 * composite
    novelty_bonus = min(NOVELTY_CAP, NOVELTY_PER * len(discoveries)) if (novelty and grounded_flagged) else 0.0
    salience_score = round(min(100.0, base + novelty_bonus), 2)

    return {
        "mode": mode,
        "grounded": grounded_flagged,
        "salience_score": salience_score,
        "novelty_bonus": round(novelty_bonus, 2),
        "salience_weighted_recall": round(swr, 4),
        "recall": round(recall, 4),
        "precision": round(precision, 4),
        "precision_kind": "grounding" if grounded_flagged else "closed_world",
        "timestamp_accuracy": None if timestamp_accuracy is None else round(timestamp_accuracy, 4),
        "type_accuracy": round(type_accuracy, 4),
        "must_find_recall": round(must_find_recall, 4),
        "n_golden": len(golden),
        "n_pred": len(pred),
        "n_matched": len(matched),
        "n_discoveries": len(discoveries),
        "n_ungrounded": len(hallucinations),
        "missed_moment_ids": [g["id"] for g in missed],
        "missed_must_find_ids": [g["id"] for g in mf_total if g not in mf_hit],
        "discoveries": [{"id": p["id"], "type": p["type"], "importance": p["importance"],
                         "start_sec": p["start"], "quote": p["quote"][:160], "grounding": p["grounding"]}
                        for p in discoveries],
        "hallucinations": [{"id": p["id"], "type": p["type"], "quote": p["quote"][:160]}
                           for p in hallucinations],
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
        "n_discoveries": res["n_discoveries"], "n_ungrounded": res["n_ungrounded"],
        "hypothesis": hypothesis, "changes": changes,
    }
    with RESULTS_TSV.open("a", encoding="utf-8") as f:
        if new:
            f.write("\t".join(TSV_COLUMNS) + "\n")
        f.write("\t".join(str(row[c]) for c in TSV_COLUMNS) + "\n")


def _print(case_id: str, res: dict) -> None:
    print(f"\n=== salience eval — {case_id} [{res['mode']}{', grounded' if res['grounded'] else ''}] ===")
    if res["mode"] == "provisional":
        print("  ⚠ PROVISIONAL: golden timestamps null; matching by moment_type only.")
    if not res["grounded"]:
        print("  ⚠ no --transcript: precision is closed-world (matched/total), grounding NOT checked.")
    print(f"  salience_score ............ {res['salience_score']}/100"
          + (f"  (+{res['novelty_bonus']} novelty)" if res["novelty_bonus"] else ""))
    print(f"  salience_weighted_recall .. {res['salience_weighted_recall']}")
    print(f"  recall .................... {res['recall']}  ({res['n_matched']}/{res['n_golden']})")
    print(f"  precision ({res['precision_kind']:<12}) {res['precision']}")
    ta = res["timestamp_accuracy"]
    print(f"  timestamp_accuracy ........ {'n/a' if ta is None else ta}")
    print(f"  type_accuracy ............. {res['type_accuracy']}")
    print(f"  must_find_recall .......... {res['must_find_recall']}")
    if res["missed_must_find_ids"]:
        print(f"  ✗ MISSED must-find: {', '.join(res['missed_must_find_ids'])}")
    if res["n_ungrounded"]:
        print(f"  ✗ HALLUCINATIONS (ungrounded): {res['n_ungrounded']}")
        for h in res["hallucinations"][:5]:
            print(f"      - [{h['type']}] {h['quote']!r}")
    if res["n_discoveries"]:
        print(f"  ★ DISCOVERIES (grounded, novel → promote?): {res['n_discoveries']}")
        for d in res["discoveries"][:8]:
            print(f"      + [{d['type']}/{d['importance']}] @{d['start_sec']} ({d['grounding']}) {d['quote']!r}")


# --------------------------------------------------------------------- selftest

def selftest() -> int:
    blob, tokens = (_norm(
        "dispatch we have shots fired shots fired on dutchess way "
        "he just shot her oh my god she is down "
        "i am hit i am hit get me cover now"), None)
    tokens = set(blob.split())
    golden = {"case_id": "selftest", "moments": [
        {"moment_id": "g1", "moment_type": "reveal", "salience": 5, "importance": "critical", "start_sec": 100, "end_sec": 110, "must_find": True, "summary": ""},
        {"moment_id": "g2", "moment_type": "emotional_peak", "salience": 3, "importance": "high", "start_sec": 300, "end_sec": 312, "must_find": False, "summary": ""},
        {"moment_id": "g3", "moment_type": "tension_shift", "salience": 2, "importance": "medium", "start_sec": 500, "end_sec": 505, "must_find": False, "summary": ""},
    ]}
    gm, mode = normalize_golden(golden)
    perfect = [
        {"moment_type": "reveal", "timestamp_sec": 101, "end_timestamp_sec": 111, "transcript_excerpt": "he just shot her oh my god she is down"},
        {"moment_type": "emotional_peak", "timestamp_sec": 301, "end_timestamp_sec": 313, "transcript_excerpt": "i am hit i am hit"},
        {"moment_type": "tension_shift", "timestamp_sec": 500, "end_timestamp_sec": 505, "transcript_excerpt": "get me cover now"},
    ]
    cheater = [  # misses the must-find, invents a quote, but adds a real novel beat
        {"moment_type": "tension_shift", "timestamp_sec": 501, "end_timestamp_sec": 506, "transcript_excerpt": "get me cover now"},
        {"moment_type": "contradiction", "timestamp_sec": 999, "transcript_excerpt": "the suspect confessed he planned it for weeks"},  # NOT in transcript → hallucination
        {"moment_type": "emotional_peak", "importance": "high", "timestamp_sec": 50, "transcript_excerpt": "shots fired shots fired on dutchess way"},  # grounded, novel → discovery
    ]
    g_ctx = (blob, tokens)
    p = score(gm, normalize_pred(perfect), mode, 30, ground=g_ctx, novelty=True)
    c = score(gm, normalize_pred(cheater), mode, 30, ground=g_ctx, novelty=True)
    _print("selftest/perfect", p)
    _print("selftest/cheater", c)
    ok = (p["salience_score"] > 90 and c["salience_score"] < p["salience_score"]
          and c["must_find_recall"] == 0.0 and c["n_ungrounded"] == 1 and c["n_discoveries"] == 1)
    print(f"\nselftest {'PASS' if ok else 'FAIL'}: perfect={p['salience_score']} cheater={c['salience_score']} "
          f"(cheater: {c['n_ungrounded']} hallucinated, {c['n_discoveries']} discovered)")
    return 0 if ok else 1


# -------------------------------------------------------------------------- cli

def main() -> int:
    ap = argparse.ArgumentParser(description="Moment-level salience scorer (immutable yardstick).")
    ap.add_argument("--golden", type=Path)
    ap.add_argument("--pred", type=Path)
    ap.add_argument("--transcript", action="append", default=[],
                    help="P3 transcript JSON file or dir (repeatable). Enables grounding precision + discoveries.")
    ap.add_argument("--tolerance-sec", type=float, default=30.0)
    ap.add_argument("--novelty-bonus", action="store_true", help="award up to +5 for grounded high-importance discoveries (provisional)")
    ap.add_argument("--hypothesis", default="")
    ap.add_argument("--changes", default="")
    ap.add_argument("--log", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        return selftest()
    if not args.golden or not args.pred:
        ap.error("--golden and --pred are required (or use --selftest)")

    g = json.loads(args.golden.read_text(encoding="utf-8"))
    case_id = g.get("case_id", args.golden.stem)
    golden, mode = normalize_golden(g)
    ground = load_transcript_blob(args.transcript) if args.transcript else None

    if args.dry_run:
        pred = normalize_pred(json.loads(args.pred.read_text(encoding="utf-8")))
        gtxt = f"grounding ON ({len(ground[1])} tokens)" if ground else "grounding OFF"
        print(f"[dry-run] case={case_id} mode={mode} golden={len(golden)} pred={len(pred)} {gtxt}")
        return 0

    pred = normalize_pred(json.loads(args.pred.read_text(encoding="utf-8")))
    res = score(golden, pred, mode, args.tolerance_sec, ground=ground, novelty=args.novelty_bonus)
    _print(case_id, res)
    if args.log:
        log_result(case_id, res, args.hypothesis, args.changes)
        print(f"\nlogged → {RESULTS_TSV.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
