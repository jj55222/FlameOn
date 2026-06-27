"""Pass-2 judgment: a reasoning model JUDGES the unioned Pass-1 candidate pile —
rates real salience, fixes types, prunes redundant/trivial moments, and surfaces the
strongest contradictions — producing a judged golden_moments-style beat set.

Run per-model for head-to-head (minimax-m3 vs deepseek-v4-pro); merge two judged
outputs with --panel for the "together" consensus.

    python pass2_judge.py --candidates ../.tmp/.../pass1_union.json --model minimax/minimax-m3 \
        --doc ../.tmp/.../docs/"20-269838 Part 1.json" --out ../.tmp/.../pass2/minimax.json
    python pass2_judge.py --panel ../.tmp/.../pass2/minimax.json ../.tmp/.../pass2/v4pro.json \
        --out ../.tmp/.../pass2/panel.json
"""
import argparse, json, sys, time
from collections import Counter
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(str(Path(__file__).resolve().parents[1] / ".env"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from llm_backends import LLMBackend, LLMError          # noqa: E402
from evaluate_salience import _norm, grounding_of      # noqa: E402

TYPES = "contradiction|emotional_peak|procedural_violation|reveal|detail_noticed|callback|tension_shift"
SYSTEM = (
    "You are a senior true-crime story editor JUDGING candidate narrative moments mined from a real police "
    "investigation by several extractor models. Rate genuine salience, fix the moment type, prune redundant or "
    "trivial candidates, and surface the strongest contradictions. Be discriminating: most candidates are NOT "
    "salience-5 — reserve 5 for the few moments the whole story turns on. Return ONLY JSON."
)


def build_prompt(cand: dict) -> str:
    beats = cand["candidates"]
    lines = [f'{i}. [{b.get("moment_type")}] (found_by {len(b.get("found_by", []))}) '
             f'p{b.get("page")}: "{(b.get("evidence_quote") or "")[:200]}"' for i, b in enumerate(beats)]
    tl = "\n".join(f'{t.get("time")}: {t.get("event")}' for t in cand.get("timeline", [])[:40])
    cons = "\n".join(f'- {c.get("nature")}' for c in cand.get("contradictions", [])[:20])
    return (
        "Below: an incident TIMELINE, candidate CONTRADICTIONS, and candidate MOMENTS (each tagged with how many "
        "independent extractor models found it — higher = more likely real). JUDGE the moments.\n"
        'Return JSON: {"beats": [{"evidence_quote": copy the candidate\'s quote VERBATIM, "page": int, '
        f'"moment_type": one of [{TYPES}], "salience": 1-5 (discriminate; reserve 5 for pivotal), '
        '"importance": "critical|high|medium|low", "angle": "tragedy|heroism|accountability|procedural|reveal|human_interest", '
        '"must_find": bool, "summary": one line}], "top_contradictions": [{"a":.., "b":.., "nature":..}]}\n'
        "PRUNE redundant/trivial candidates (just omit them). Prefer candidates multiple models found. Keep quotes verbatim.\n\n"
        f"TIMELINE:\n{tl}\n\nCANDIDATE CONTRADICTIONS:\n{cons}\n\nCANDIDATE MOMENTS:\n" + "\n".join(lines)
    )


def judge(cand: dict, model: str, doc: str, max_tokens: int) -> dict:
    backend = LLMBackend(model=model, timeout=600, max_retries=2)
    t = time.time()
    raw = backend.complete(system=SYSTEM, user=build_prompt(cand), max_tokens=max_tokens, temperature=0.2)
    dt = time.time() - t
    data = json.loads(raw)
    beats = data.get("beats", [])
    gr = None
    if doc:
        from doc_pass1 import load_doc_text
        text, _ = load_doc_text(Path(doc))
        blob = _norm(text); toks = set(blob.split())
        for b in beats:
            b["grounding"] = grounding_of(b.get("evidence_quote", ""), blob, toks)
        gr = sum(1 for b in beats if b.get("grounding"))
    trace = backend.last_reasoning
    return {"case_id": cand.get("case_id"),
            "_meta": {"model": model, "elapsed_sec": round(dt, 1), "n_judged": len(beats),
                      "n_grounded": gr, "reasoning_chars": len(trace or ""),
                      "salience_dist": dict(sorted(Counter(b.get("salience") for b in beats).items(), reverse=True))},
            "beats": beats, "top_contradictions": data.get("top_contradictions", []),
            "reasoning_trace": trace}


def panel_merge(a: dict, b: dict) -> dict:
    """'Together': match two judges' beats by quote, average salience, keep if mean>=3.5;
    must_find requires both; record agreement."""
    def key(x): return _norm(x.get("evidence_quote", ""))[:100]
    bb = {key(x): x for x in b["beats"]}
    merged, matched = [], set()
    for x in a["beats"]:
        k = key(x); y = bb.get(k)
        if y:
            matched.add(k)
            s = (int(x.get("salience", 1)) + int(y.get("salience", 1))) / 2
            merged.append({**x, "salience": round(s, 1),
                           "must_find": bool(x.get("must_find") and y.get("must_find")),
                           "agreement": "both", "salience_a": x.get("salience"), "salience_b": y.get("salience")})
        else:
            merged.append({**x, "agreement": a["_meta"]["model"]})
    for y in b["beats"]:
        if key(y) not in matched:
            merged.append({**y, "agreement": b["_meta"]["model"]})
    kept = [m for m in merged if float(m.get("salience", 0)) >= 3.5]
    return {"case_id": a.get("case_id"),
            "_meta": {"panel": [a["_meta"]["model"], b["_meta"]["model"]],
                      "n_merged": len(merged), "n_kept_ge3.5": len(kept),
                      "n_both": sum(1 for m in merged if m.get("agreement") == "both")},
            "beats": sorted(kept, key=lambda m: -float(m.get("salience", 0)))}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidates")
    ap.add_argument("--model")
    ap.add_argument("--doc")
    ap.add_argument("--max-tokens", type=int, default=16000)
    ap.add_argument("--panel", nargs=2, help="two judged JSONs to merge into a consensus")
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)

    if args.panel:
        a = json.load(open(args.panel[0])); b = json.load(open(args.panel[1]))
        out = panel_merge(a, b)
        json.dump(out, open(args.out, "w"), indent=1)
        print(f"[panel] {out['_meta']['panel']} merged={out['_meta']['n_merged']} "
              f"both-agree={out['_meta']['n_both']} kept(>=3.5)={out['_meta']['n_kept_ge3.5']} -> {args.out}")
        return 0

    if not args.candidates or not args.model:
        ap.error("--candidates and --model required (or use --panel)")
    cand = json.load(open(args.candidates))
    try:
        out = judge(cand, args.model, args.doc, args.max_tokens)
    except (LLMError, json.JSONDecodeError) as e:
        print(f"[{args.model}] judge failed: {str(e)[:120]}")
        return 1
    json.dump(out, open(args.out, "w"), indent=1)
    m = out["_meta"]
    print(f"[{args.model}] {m['elapsed_sec']}s judged={m['n_judged']} grounded={m['n_grounded']} "
          f"salience_dist={m['salience_dist']} -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
