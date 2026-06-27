"""Append/union grounded Pass-1 candidates from multiple extractor models into one pile.

Dedups beats by normalized quote — tracking WHICH models found each (a consensus signal:
a moment several models independently surfaced is more likely genuinely salient) — and
unions the incident timelines + candidate contradictions. Feeds pass2_judge.py.

    python pass1_union.py --pass1 ../.tmp/.../deepseek_v2.json ../.tmp/.../mimo25.json ../.tmp/.../minimax_m3.json \
        --out ../.tmp/dutchess_269838/pass1_union.json
"""
import argparse, json, os, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from evaluate_salience import _norm  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pass1", nargs="+", required=True, help="Pass-1 doc_pass1 output JSONs")
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args()

    beats = {}        # norm-quote -> merged beat with found_by[]
    timeline, seen_tl = [], set()
    contradictions = []
    for f in args.pass1:
        d = json.load(open(f))
        model = d.get("_meta", {}).get("model", os.path.basename(f))
        for b in d.get("beats", []):
            if not b.get("grounding"):           # grounded candidates only
                continue
            k = _norm(b.get("evidence_quote", ""))[:120]
            if not k:
                continue
            if k in beats:
                if model not in beats[k]["found_by"]:
                    beats[k]["found_by"].append(model)
            else:
                beats[k] = {"evidence_quote": b.get("evidence_quote"), "page": b.get("page"),
                            "moment_type": b.get("moment_type"), "summary": b.get("summary"),
                            "found_by": [model]}
        for t in d.get("incident_timeline", []):
            tk = (str(t.get("time")), _norm(str(t.get("event")))[:60])
            if tk not in seen_tl:
                seen_tl.add(tk)
                timeline.append(t)
        for c in d.get("contradictions", []):
            contradictions.append({**c, "_model": model})

    beatlist = sorted(beats.values(), key=lambda b: -len(b["found_by"]))
    timeline.sort(key=lambda t: str(t.get("time")))
    n_consensus = sum(1 for b in beatlist if len(b["found_by"]) > 1)
    out = {
        "case_id": "sac_so_20-269838",
        "_meta": {"sources": args.pass1, "n_candidates": len(beatlist),
                  "n_consensus": n_consensus, "n_timeline": len(timeline),
                  "n_contradictions": len(contradictions)},
        "candidates": beatlist, "timeline": timeline, "contradictions": contradictions,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    json.dump(out, open(args.out, "w"), indent=1)
    print(f"union: {len(beatlist)} candidates ({n_consensus} found by >1 model), "
          f"{len(timeline)} timeline, {len(contradictions)} contradictions -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
