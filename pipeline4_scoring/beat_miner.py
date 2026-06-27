"""
beat_miner.py — ground-truth EXPANDER for the salience eval harness.

The golden answer key (golden_moments) starts hand-drafted and small. This bot
grows it: it reads the P3 transcripts for a case, proposes candidate salient
beats, FAITHFULNESS-FILTERS them (every beat's quote must resolve in the real
transcript, via the same grounding check evaluate_salience.py uses), snaps each
beat's timestamp to the matching segment, and emits a golden_moments DRAFT for
a human to confirm salience and freeze.

Pipeline:  transcripts ──► propose ──► ground-filter + snap ──► dedup ──► draft golden
                          (LLM | mock)   (mechanical, no model)

Faithfulness is mechanical, never a model's opinion — so even the LLM path
can't smuggle in an invented quote: if the quote isn't in the transcript, the
beat is dropped. That keeps the answer key honest (anti-circularity guardrail).

Two candidate generators:
  • default  — LLMBackend (OpenRouter) long-context extraction. Needs
               OPENROUTER_API_KEY. Model defaults to the P4 Pass-1 model.
  • --mock   — keyword/heuristic salience scan. No API key, no network. Runs
               today so the harness is testable before keys/footage exist.

    # once Dutchess transcripts exist:
    python beat_miner.py --transcripts ../pipeline3_audio/transcripts/20-269838/ \
        --case-id sac_so_20-269838 --agency "Sacramento County Sheriff's Office" \
        --out golden/sac_so_20-269838_dutchess_way.mined.json

    # works now, no key:
    python beat_miner.py --transcripts <dir> --case-id demo --mock --out /tmp/mined.json

NOTE: output is status="draft" — a human reviews salience and sets "reviewed"
before it's used as a yardstick. The bot proposes; it does not freeze.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

# Reuse the IMMUTABLE grounding check — one definition of "is this quote real".
from evaluate_salience import _norm, load_transcript_blob, grounding_of

VALID_TYPES = ["contradiction", "emotional_peak", "procedural_violation",
               "reveal", "detail_noticed", "callback", "tension_shift"]

# Heuristic salience cues for --mock: phrase → (moment_type, salience, angle).
# Deliberately conservative; the human review pass is what confers real salience.
CUES = [
    (("shots fired", "shot her", "shot him", "he shot", "open fire", "gunshot"), "reveal", 5, "tragedy"),
    (("i'm hit", "im hit", "i am hit", "officer down", "deputy down", "man down"), "emotional_peak", 5, "heroism"),
    (("he's got a gun", "hes got a gun", "gun gun gun", "drop the gun", "got a gun"), "tension_shift", 4, "procedural"),
    (("oh my god", "oh god", "please help", "help me", "she's dead", "he's dead"), "emotional_peak", 4, "tragedy"),
    (("self inflicted", "shot himself", "turned the gun"), "reveal", 4, "reveal"),
    (("get back", "drop it", "show me your hands", "hands up", "stop moving"), "tension_shift", 3, "procedural"),
    (("rendering aid", "start cpr", "medical", "ambulance", "paramedic"), "emotional_peak", 3, "human_interest"),
    (("i never", "that's not what", "you said", "but you told", "didn't happen"), "contradiction", 3, "accountability"),
]


def load_segments(paths: list) -> list:
    """Flatten P3 transcript JSON(s) → [{start,end,text,source}] sorted by start."""
    files = []
    for p in paths or []:
        p = Path(p)
        files.extend(sorted(p.glob("*.json")) if p.is_dir() else [p])
    segs = []
    for f in files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        rows = data.get("transcript", data) if isinstance(data, dict) else data
        if not isinstance(rows, list):
            continue
        for r in rows:
            if isinstance(r, dict) and r.get("text"):
                segs.append({"start": r.get("start_sec"), "end": r.get("end_sec"),
                             "text": r["text"], "source": f.name})
    segs.sort(key=lambda s: (s["start"] is None, s["start"] or 0))
    return segs


# --------------------------------------------------------------- candidate gen

def propose_mock(segments: list, max_beats: int) -> list:
    """Heuristic: score each segment by cue matches, keep the strongest, distinct."""
    # Normalize cue phrases the SAME way as the text (e.g. "i'm hit" → "i m hit"),
    # so contraction cues actually match the normalized transcript.
    cues_n = [(tuple(_norm(p) for p in phrases), mtype, sal, angle) for (phrases, mtype, sal, angle) in CUES]
    scored = []
    for seg in segments:
        n = _norm(seg["text"])
        for phrases, mtype, sal, angle in cues_n:
            if any(ph and ph in n for ph in phrases):
                scored.append({"moment_type": mtype, "salience": sal, "angle": angle,
                               "artifact_id": seg["source"],
                               "importance": "critical" if sal >= 5 else ("high" if sal == 4 else "medium"),
                               "start_sec": seg["start"], "end_sec": seg["end"],
                               "evidence_quote": seg["text"].strip(),
                               "summary": f"[mock] {mtype} cue in {seg['source']}: {seg['text'].strip()[:80]}"})
                break
    scored.sort(key=lambda b: -b["salience"])
    return scored[:max_beats]


def propose_llm(segments: list, case_id: str, model: str, max_beats: int) -> list:
    """LLM long-context extraction via the repo's OpenRouter backend."""
    from llm_backends import build_backend, LLMError
    # Render a timecoded transcript the model can quote from verbatim.
    lines = [f"[{int(s['start'] or 0)}s] {s['text'].strip()}" for s in segments]
    transcript_text = "\n".join(lines)
    system = (
        "You are a true-crime story editor mining law-enforcement transcripts for the "
        "most SALIENT narrative moments (the beats an audience reacts to). Return ONLY JSON."
    )
    user = (
        f"Case {case_id}. From the timecoded transcript below, extract up to {max_beats} of the "
        "most compelling moments. Return a JSON array; each item:\n"
        '{"moment_type": one of ' + str(VALID_TYPES) + ',\n'
        ' "salience": 1-5 (5=the moment the whole piece turns on),\n'
        ' "importance": "critical|high|medium|low",\n'
        ' "start_sec": integer seconds from the [Ns] tag of the line,\n'
        ' "evidence_quote": the VERBATIM transcript text for this moment (copy it exactly),\n'
        ' "angle": "tragedy|heroism|accountability|procedural|reveal|human_interest",\n'
        ' "summary": one sentence}\n'
        "Quote EXACTLY from the transcript — do not paraphrase; invented quotes are discarded.\n\n"
        f"TRANSCRIPT:\n{transcript_text}"
    )
    try:
        raw = build_backend(model).complete(system=system, user=user, max_tokens=8000, temperature=0.2)
        data = json.loads(raw)
        return data if isinstance(data, list) else data.get("moments", [])
    except (LLMError, json.JSONDecodeError, Exception) as e:
        print(f"[beat_miner] LLM proposal failed ({str(e)[:120]}). Try --mock.", file=sys.stderr)
        return []


# ------------------------------------------------------ ground-filter + assemble

def ground_filter(candidates: list, blob: str, tokens: set, segments: list) -> tuple:
    """Drop beats whose quote isn't in the transcript; snap start_sec to the
    real matching segment. Returns (kept, dropped)."""
    kept, dropped = [], []
    for c in candidates:
        if c.get("moment_type") not in VALID_TYPES:
            c["moment_type"] = "detail_noticed"
        g = grounding_of(c.get("evidence_quote", ""), blob, tokens)
        if not g:
            dropped.append(c)
            continue
        # Snap timestamp to the best-matching segment so it's grounded, not LLM-guessed.
        nq = _norm(c.get("evidence_quote", ""))
        best = max(segments, key=lambda s: len(set(nq.split()) & set(_norm(s["text"]).split())),
                   default=None)
        if best and best["start"] is not None:
            c["start_sec"], c["end_sec"] = best["start"], best.get("end")
        c["_grounding"] = g
        kept.append(c)
    return kept, dropped


def dedup(beats: list, window: float = 8.0) -> list:
    """Merge near-duplicate beats of the same type within `window` seconds."""
    out = []
    for b in sorted(beats, key=lambda x: (x.get("start_sec") is None, x.get("start_sec") or 0)):
        dup = next((o for o in out if o["moment_type"] == b["moment_type"]
                    and o.get("start_sec") is not None and b.get("start_sec") is not None
                    and abs(o["start_sec"] - b["start_sec"]) <= window), None)
        if dup:
            if b["salience"] > dup["salience"]:
                dup.update(b)
        else:
            out.append(b)
    return out


def assemble_golden(beats: list, case_id: str, agency: str, source: str) -> dict:
    beats = sorted(beats, key=lambda x: (x.get("start_sec") is None, x.get("start_sec") or 0))
    moments = []
    for i, b in enumerate(beats, 1):
        moments.append({
            "moment_id": f"m{i:02d}_mined",
            "artifact_id": None,
            "moment_type": b["moment_type"],
            "salience": int(b.get("salience", 2)),
            "importance": b.get("importance", "medium"),
            "start_sec": b.get("start_sec"),
            "end_sec": b.get("end_sec"),
            "summary": b.get("summary", ""),
            "evidence_quote": b.get("evidence_quote", ""),
            "angle": b.get("angle"),
            "must_find": int(b.get("salience", 2)) >= 5,
        })
    return {
        "case_id": case_id,
        "agency": agency,
        "labeling": {
            "labeler": f"beat-miner bot ({source})",
            "status": "draft",
            "notes": "Auto-mined candidate beats; every quote was ground-checked against the "
                     "transcript and timestamps snapped to real segments. A HUMAN must confirm "
                     "salience and set status='reviewed' before this gates anything.",
        },
        "moments": moments,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Mine candidate golden moments from P3 transcripts.")
    ap.add_argument("--transcripts", action="append", required=True, help="P3 transcript file/dir (repeatable)")
    ap.add_argument("--case-id", required=True)
    ap.add_argument("--agency", default="")
    ap.add_argument("--model", default="google/gemini-3.1-flash-lite-preview", help="OpenRouter model for proposal")
    ap.add_argument("--max-beats", type=int, default=40)
    ap.add_argument("--mock", action="store_true", help="heuristic proposer (no LLM/key/network)")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    segments = load_segments(args.transcripts)
    if not segments:
        print("[beat_miner] no transcript segments found.", file=sys.stderr)
        return 1
    blob, tokens = load_transcript_blob(args.transcripts)
    print(f"[beat_miner] {len(segments)} segments; proposing via {'mock heuristic' if args.mock else args.model} ...")

    candidates = propose_mock(segments, args.max_beats) if args.mock else propose_llm(segments, args.case_id, args.model, args.max_beats)
    kept, dropped = ground_filter(candidates, blob, tokens, segments)
    beats = dedup(kept)
    print(f"[beat_miner] proposed={len(candidates)} grounded={len(kept)} dropped_ungrounded={len(dropped)} after_dedup={len(beats)}")

    golden = assemble_golden(beats, args.case_id, args.agency, "mock" if args.mock else args.model)
    if args.dry_run:
        print(json.dumps(golden, indent=2)[:1500] + "\n... [dry-run, not written]")
        return 0
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(golden, indent=2) + "\n", encoding="utf-8")
    print(f"[beat_miner] wrote {len(beats)} candidate moments → {args.out}")
    print("  next: review salience by hand, then set labeling.status='reviewed'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
