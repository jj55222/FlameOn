"""Pass-1 document extraction (the 'first pass').

Feeds a FULL OCR'd investigation report to a long-context model and extracts the
structured narrative material a documentary needs — incident timeline, candidate
beats (with VERBATIM quotes + page cites), contradictions, factual anchors — then
GROUNDS every quote against the OCR text (mechanical, reused from evaluate_salience).

The Pass-1 model is a swappable --model flag so you can compare what different
models hand downstream to the Pass-2 reasoning panel:

    python doc_pass1.py --doc ../.tmp/dutchess_269838/docs/"20-269838 Part 1.json" \
        --model google/gemini-3.5-flash --out ../.tmp/dutchess_269838/pass1/gemini.json
    python doc_pass1.py --doc ... --model deepseek/deepseek-v4-flash --out .../deepseek.json

Output `_meta` reports beats / grounded / timeline / contradictions so two models
are directly comparable.
"""
import argparse, json, sys, time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(str(Path(__file__).resolve().parents[1] / ".env"))
from llm_backends import LLMBackend, LLMError, clean_llm_output   # noqa: E402
from evaluate_salience import _norm, grounding_of                  # noqa: E402

SYSTEM = (
    "You are a true-crime story editor AND investigative analyst examining an OCR'd "
    "(scanned, may contain OCR errors) law-enforcement investigation report. Extract the "
    "narrative material a documentary would need. Be precise and grounded. Return ONLY valid JSON."
)
TYPES = "contradiction|emotional_peak|procedural_violation|reveal|detail_noticed|callback|tension_shift"


def load_doc_text(path: Path):
    if path.suffix == ".json":
        d = json.load(open(path))
        pages = d.get("pages", [])
        return "\n".join(f"[p{pg['page']}] {pg['text']}" for pg in pages), d.get("source_document", path.name)
    return path.read_text(encoding="utf-8"), path.name


def build_prompt(text: str, maxbeats: int) -> str:
    # Pass-1 is EXTRACTION, not judgment. A later reasoning pass rates salience and prunes,
    # so here we maximize RECALL of grounded material and do NOT ask for salience scores
    # (models can't discriminate it well at this stage and flat-rate everything).
    return (
        "You are extracting raw narrative material from an OCR'd (scanned, may contain OCR errors) police "
        "investigation report for a SEPARATE later judgment pass. Extract BROADLY, maximizing RECALL: capture "
        "every narratively-relevant, grounded moment. Do NOT score importance or salience — that is judged later.\n"
        "Return JSON with EXACTLY these keys:\n"
        '  "incident_timeline": [{"time": "HHMM or descriptive", "event": str, "page": int}],\n'
        f'  "beats": [up to {maxbeats}; each {{"moment_type": one of [{TYPES}] (rough best-guess is fine), '
        '"summary": one line, "evidence_quote": VERBATIM substring copied exactly from the report, "page": int}],\n'
        '  "contradictions": [{"a": str, "b": str, "nature": str}],\n'
        '  "factual_anchors": [{"type": "name|time|location|charge|badge|weapon", "value": str, "page": int}]\n'
        "Rules: evidence_quote MUST be copied verbatim from the report text (it is verified mechanically; "
        "paraphrased quotes are discarded). Favor MORE grounded candidates over fewer. Surface contradictions "
        "between different accounts (witnesses, suspect, officers, forensics) wherever they appear.\n\n"
        f"REPORT:\n{text}"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--doc", required=True, type=Path)
    ap.add_argument("--model", required=True)
    ap.add_argument("--max-beats", type=int, default=40)
    ap.add_argument("--max-tokens", type=int, default=24000)
    ap.add_argument("--char-limit", type=int, default=1_100_000, help="truncate doc (cost/context guard)")
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args()

    text, src = load_doc_text(args.doc)
    truncated = len(text) > args.char_limit
    if truncated:
        text = text[:args.char_limit]
    approx_tok = len(text) // 4
    print(f"[{args.model}] {src}: {len(text):,} chars (~{approx_tok:,} tok){' [TRUNCATED]' if truncated else ''}")

    backend = LLMBackend(model=args.model, timeout=600, max_retries=2)
    t = time.time()
    try:
        raw = backend.complete(system=SYSTEM, user=build_prompt(text, args.max_beats),
                               max_tokens=args.max_tokens, temperature=0.2)
    except LLMError as e:
        print(f"[{args.model}] LLM error: {e}")
        return 1
    dt = time.time() - t

    try:
        data = json.loads(raw)
    except Exception as e:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        (args.out.with_suffix(".raw.txt")).write_text(raw, encoding="utf-8")
        print(f"[{args.model}] JSON parse failed ({e}); raw saved")
        return 1

    blob = _norm(text)
    tokens = set(blob.split())
    beats = data.get("beats", [])
    for b in beats:
        b["grounding"] = grounding_of(b.get("evidence_quote", ""), blob, tokens)
    grounded = [b for b in beats if b["grounding"]]
    data["_meta"] = {
        "model": args.model, "source": src, "elapsed_sec": round(dt, 1),
        "input_chars": len(text), "truncated": truncated,
        "n_beats": len(beats), "n_grounded": len(grounded),
        "n_timeline": len(data.get("incident_timeline", [])),
        "n_contradictions": len(data.get("contradictions", [])),
        "n_anchors": len(data.get("factual_anchors", [])),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    json.dump(data, open(args.out, "w"), indent=1)
    print(f"[{args.model}] {dt:.0f}s | beats={len(beats)} grounded={len(grounded)} "
          f"timeline={len(data.get('incident_timeline', []))} "
          f"contradictions={len(data.get('contradictions', []))} -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
