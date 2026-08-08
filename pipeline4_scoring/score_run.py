"""Mixed-key scorer: score a harness run (e.g. a Pass-2 judge output) against the frozen
golden key. The key is MIXED — audio beats are timestamped, doc beats are page-cited with a
quote — so this matches by QUOTE for the document layer (and would match by time for audio).

Recall vs. the human-frozen key is the headline: of the moments the human kept (and the
must-finds), how many did the run independently surface? Reuses _norm from evaluate_salience.

    python score_run.py --golden golden/sac_so_20-269838_dutchess_way.golden.json --layer document \
        --pred ../.tmp/dutchess_269838/pass2/minimax_3m.json ../.tmp/.../v4pro_3m.json ../.tmp/.../panel.json
"""
import argparse, json, os, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from evaluate_salience import _norm  # noqa: E402


def qtokens(s):
    return set(_norm(s or "").split())


def overlap(gq, pq):
    """Fraction of golden-quote tokens present in the predicted quote."""
    gt = qtokens(gq)
    return (len(gt & qtokens(pq)) / len(gt)) if gt else 0.0


def load_golden(path, layer):
    ms = json.load(open(path)).get("moments", [])
    if layer != "all":
        ms = [m for m in ms if m.get("source_layer") == layer]
    return [{"id": m["moment_id"], "type": m.get("moment_type"), "salience": int(m.get("salience", 1)),
             "must_find": bool(m.get("must_find")), "quote": m.get("evidence_quote", "")} for m in ms]


def load_pred(path):
    beats = json.load(open(path)).get("beats", [])
    return [{"quote": b.get("evidence_quote") or b.get("transcript_excerpt") or b.get("description", ""),
             "type": b.get("moment_type", "")} for b in beats]


def score(golden, preds, qthresh):
    used, matched = set(), []
    for g in sorted(golden, key=lambda x: -x["salience"]):
        best_j, best_q = None, 0.0
        for j, p in enumerate(preds):
            if j in used:
                continue
            o = overlap(g["quote"], p["quote"])
            if o > best_q:
                best_j, best_q = j, o
        if best_j is not None and best_q >= qthresh:
            used.add(best_j)
            matched.append((g, preds[best_j], best_q))
        else:
            matched.append((g, None, 0.0))
    hit = [(g, p, q) for g, p, q in matched if p is not None]
    hit_ids = {g["id"] for g, _, _ in hit}
    sal_tot = sum(g["salience"] for g in golden) or 1
    mft = [g for g in golden if g["must_find"]]
    return {
        "recall": len(hit) / len(golden) if golden else 0.0,
        "swr": sum(g["salience"] for g, _, _ in hit) / sal_tot,
        "must_find_recall": (len([g for g in mft if g["id"] in hit_ids]) / len(mft)) if mft else 1.0,
        "precision": len(hit) / len(preds) if preds else 0.0,
        "type_acc": (sum(1 for g, p, _ in hit if p["type"] == g["type"]) / len(hit)) if hit else 0.0,
        "n_golden": len(golden), "n_pred": len(preds), "n_hit": len(hit),
        "missed": [g["id"] for g, p, _ in matched if p is None],
        "missed_must_find": [g["id"] for g in mft if g["id"] not in hit_ids],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--golden", required=True)
    ap.add_argument("--pred", nargs="+", required=True)
    ap.add_argument("--layer", default="document", choices=["document", "audio", "all"])
    ap.add_argument("--qthresh", type=float, default=0.5)
    args = ap.parse_args()
    golden = load_golden(args.golden, args.layer)
    print(f"golden [{args.layer}]: {len(golden)} moments ({sum(1 for g in golden if g['must_find'])} must-find)\n")
    print(f"{'run':<26}{'recall':>8}{'swr':>7}{'mf_rec':>8}{'prec':>7}{'type':>7}  missed must-finds")
    print("-" * 92)
    for pf in args.pred:
        r = score(golden, load_pred(pf), args.qthresh)
        name = os.path.basename(pf).replace(".json", "")
        mm = ",".join(r["missed_must_find"]) or "—"
        print(f"{name:<26}{r['recall']:>8.2f}{r['swr']:>7.2f}{r['must_find_recall']:>8.2f}"
              f"{r['precision']:>7.2f}{r['type_acc']:>7.2f}  {mm}")


if __name__ == "__main__":
    main()
