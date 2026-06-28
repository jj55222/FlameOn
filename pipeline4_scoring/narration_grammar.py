"""Narration-grammar miner — learn WHERE and HOW analysis channels narrate.

The salience harness learned WHICH moments creators air. This learns the other
half: the channel's narration GRAMMAR — what analytical MOVE they make, in which
PHASE of the story, at what cadence. That grammar is case-independent STYLE (learn
it from many of a channel's videos), orthogonal to our case FACTS (the grounding).
Output feeds two places: a prompt directive for the analysis-narration tier
(blueprint_shape --analysis) and, later, a scorer for our own cuts.

Method (per creator video):
  1. SEGREGATE narrator VO from footage audio. Strong prior: match each creator
     line against our raw bodycam transcripts — a line that resolves in the footage
     IS footage (grounding-as-precision, pointed at the creator). Our raw transcripts
     are sparse (hot-window), so the match UNDER-detects footage; an LLM register
     pass (third-person/past = VO; shouted/imperative/present = footage) corrects the
     leak. The footage matches double as PHASE ANCHORS (the matched bodycam's phase).
  2. CLASSIFY each VO segment's analytical move (taxonomy below).
  3. PHASE-MAP each VO move via the nearest footage anchor.
  4. AGGREGATE -> {cadence, move distribution, phase x move matrix, open/close,
     prompt_directive}.

    python pipeline4_scoring/narration_grammar.py \
        --creator .tmp/2023psb0530/creator_transcripts/EWU_t-Hlipw6TaQ.json \
        --footage .tmp/2023psb0530/transcripts --timeline .tmp/2023psb0530/timeline/case_timeline.json \
        --model google/gemini-3.1-flash-lite-preview --out .tmp/2023psb0530/grammar
"""
from __future__ import annotations

import argparse
import glob
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PHASE_ORDER = ["pre_incident", "incident", "aftermath", "transport", "investigation", "outcome", "unknown"]

# Analytical move taxonomy — the vocabulary of an accountability-channel voiceover.
MOVES: Dict[str, str] = {
    "hook": "opening tease that grabs attention / promises a payoff",
    "scene_set": "describe what is happening or where we are right now",
    "foreshadow": "hint at what is coming; build dread",
    "procedure": "explain how policing / policy / procedure normally works",
    "contradiction": "flag an inconsistency, discrepancy, or what doesn't add up",
    "credibility": "assess someone's truthfulness, motive, or character",
    "question": "pose a rhetorical question to the viewer",
    "stakes": "emotional weight, consequences, what is at risk",
    "context": "background fact — who someone is, prior events, identity",
    "legal_cover": "disclaimer: presumed innocent / based on official records",
    "transition": "recap, or move the story to the next phase",
    "verdict": "the accountability conclusion — findings, discipline, outcome",
}


# Phase keywords — let a VO line self-identify its phase from the creator's own
# chronology, so the back half (investigation/outcome) isn't mis-anchored to the
# nearest footage (which only covers the front of the case). Order = specificity.
_PHASE_KEYWORDS = [
    ("outcome", re.compile(r"\b(terminat\w*|resign\w*|fired|sustained|disciplin\w*|convict\w*|"
                           r"sentenc\w*|pleaded|guilty|acquitt\w*|charged with|recommended (?:him )?for)\b", re.I)),
    ("investigation", re.compile(r"\b(investigat\w*|internal affairs|\bIA\b|interview\w*|"
                                 r"determined|reviewed|detectives?|the report|findings?)\b", re.I)),
    ("aftermath", re.compile(r"\b(transport\w*|hospital|ambulance|paramedic\w*|narcan|aftermath|"
                             r"secured the scene|life-?saving|cpr)\b", re.I)),
    ("pre_incident", re.compile(r"\b(earlier|hours before|before he|his shift|field contact|"
                                r"traffic stop|confiscat\w*|seiz\w*|responded to|that morning|that day)\b", re.I)),
    ("incident", re.compile(r"\b(found unresponsive|collaps\w*|passed out|unconscious|the moment|"
                            r"discover\w*|on the floor|in the (?:restroom|bathroom|stall))\b", re.I)),
]


def keyword_phase(text: str) -> Optional[str]:
    for ph, pat in _PHASE_KEYWORDS:
        if pat.search(text or ""):
            return ph
    return None


def _toks(t: str) -> List[str]:
    return re.sub(r"[^a-z0-9 ]", " ", (t or "").lower()).split()


def _grams(ws: List[str], n: int = 4) -> List[Tuple[str, ...]]:
    return [tuple(ws[i:i + n]) for i in range(len(ws) - n + 1)]


def load_segments(creator_path: Path) -> List[Dict[str, Any]]:
    d = json.loads(creator_path.read_text(encoding="utf-8"))
    segs = d.get("transcript") or d.get("segments") or (d if isinstance(d, list) else [])
    out = []
    for i, s in enumerate(segs):
        out.append({"i": i, "start_sec": float(s.get("start_sec") or s.get("start") or 0.0),
                    "text": (s.get("text") or "").strip()})
    return out


def phase_of_artifacts(timeline_path: Optional[Path]) -> Dict[str, str]:
    if not timeline_path or not Path(timeline_path).exists():
        return {}
    tl = json.loads(Path(timeline_path).read_text(encoding="utf-8"))
    out: Dict[str, str] = {}
    for ph, recs in (tl.get("phases") or {}).items():
        for r in recs:
            out[r["artifact_id"]] = ph
    return out


def build_footage_index(footage_dir: Optional[Path], phase_of: Dict[str, str],
                        n: int = 4) -> Dict[Tuple[str, ...], Counter]:
    """shingle -> Counter(phase): every 4-gram of our raw footage, tagged with the
    phase of the bodycam it came from (so a creator match votes on a phase)."""
    idx: Dict[Tuple[str, ...], Counter] = defaultdict(Counter)
    if not footage_dir:
        return idx
    for f in sorted(glob.glob(str(Path(footage_dir) / "*.json"))):
        stem = Path(f).stem                       # e.g. BWC-3a  == artifact_id
        ph = phase_of.get(stem, "unknown")
        try:
            d = json.loads(Path(f).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for s in (d.get("transcript") or []):
            for g in _grams(_toks(s.get("text", "")), n):
                idx[g][ph] += 1
    return idx


def footage_signal(text: str, idx: Dict[Tuple[str, ...], Counter], n: int = 4) -> Tuple[float, Optional[str]]:
    """Return (footage-overlap fraction, voted phase) for a creator segment."""
    g = _grams(_toks(text), n)
    if not g:
        return 0.0, None
    pc: Counter = Counter()
    matched = 0
    for gg in g:
        if gg in idx:
            matched += 1
            pc.update(idx[gg])
    overlap = matched / len(g)
    phase = pc.most_common(1)[0][0] if pc else None
    return overlap, phase


def assign_phases(segs: List[Dict[str, Any]]) -> None:
    """Phase each segment. Footage segments use their match anchor. VO segments
    prefer their OWN chronology keywords (so 'recommended for termination' -> outcome
    even with no footage there), then fall back to the nearest footage anchor."""
    anchors = [(s["i"], s.get("anchor_phase")) for s in segs
               if s["role"] == "footage" and s.get("anchor_phase")]
    for s in segs:
        if s["role"] == "footage":
            s["phase"] = s.get("anchor_phase") or "unknown"
            continue
        kw = keyword_phase(s["text"])
        if kw:
            s["phase"] = kw
        elif anchors:
            s["phase"] = min(anchors, key=lambda a: abs(a[0] - s["i"]))[1]
        else:
            s["phase"] = "unknown"


# ---------------------------------------------------------------------------
# LLM pass: confirm role (correct the sparse-footage leak) + classify VO move
# ---------------------------------------------------------------------------

def _classify_llm(segs: List[Dict[str, Any]], model: str, chunk: int = 50) -> None:
    from llm_backends import build_backend, clean_llm_output  # type: ignore
    backend = build_backend(model)
    moves_doc = "; ".join(f"{k} = {v}" for k, v in MOVES.items())
    system = (
        "You label segments of a true-crime YouTube transcript. Each segment is "
        "either NARRATION ('vo' — the creator's studio voiceover: third-person, "
        "past-tense, analytical, scene-setting) or FOOTAGE ('footage' — audio from "
        "the bodycam/scene: first/second person, shouted, imperative, in-the-moment). "
        "A 'footage_match' hint of true means it matched the raw bodycam (likely "
        "footage) — but the raw transcript is sparse, so trust the register too. "
        "For each 'vo' segment pick the single best MOVE; for 'footage' set move=null.\n"
        f"MOVES: {moves_doc}.\n"
        "Return ONLY a JSON array, one object per input in order: "
        '{"i": <int>, "role": "vo"|"footage", "move": "<move>"|null}.')
    for start in range(0, len(segs), chunk):
        batch = segs[start:start + chunk]
        payload = [{"i": s["i"], "text": s["text"][:240],
                    "footage_match": s["footage_overlap"] >= 0.34} for s in batch]
        try:
            raw = backend.complete(system=system, user=json.dumps(payload, ensure_ascii=False),
                                   max_tokens=4000, temperature=0.0)
            arr = json.loads(clean_llm_output(raw))
            by_i = {int(o["i"]): o for o in arr if isinstance(o, dict) and "i" in o}
        except Exception as e:  # noqa: BLE001
            by_i = {}
            print(f"[grammar] chunk @{start} LLM/parse failed ({type(e).__name__}); "
                  f"falling back to footage_match prior")
        for s in batch:
            o = by_i.get(s["i"])
            if o and o.get("role") in ("vo", "footage"):
                s["role"] = o["role"]
                s["move"] = o.get("move") if o["role"] == "vo" and o.get("move") in MOVES else (
                    None if o["role"] == "footage" else "scene_set")
            else:                                  # fall back to the deterministic prior
                s["role"] = "footage" if s["footage_overlap"] >= 0.34 else "vo"
                s["move"] = None if s["role"] == "footage" else "scene_set"


# ---------------------------------------------------------------------------
# Aggregate -> grammar profile
# ---------------------------------------------------------------------------

def build_profile(segs: List[Dict[str, Any]], channel: str) -> Dict[str, Any]:
    vo = [s for s in segs if s["role"] == "vo"]
    foot = [s for s in segs if s["role"] == "footage"]
    vo_words = sum(len(_toks(s["text"])) for s in vo)
    foot_words = sum(len(_toks(s["text"])) for s in foot)

    # footage runs (consecutive footage segments) -> "how long they let it play"
    runs, cur = [], 0
    for s in segs:
        if s["role"] == "footage":
            cur += 1
        elif cur:
            runs.append(cur); cur = 0
    if cur:
        runs.append(cur)

    move_dist = Counter(s["move"] for s in vo if s.get("move"))
    phase_move: Dict[str, Counter] = defaultdict(Counter)
    for s in vo:
        if s.get("move"):
            phase_move[s.get("phase", "unknown")][s["move"]] += 1

    matrix = {ph: dict(phase_move[ph].most_common()) for ph in PHASE_ORDER if phase_move.get(ph)}
    directive = _prompt_directive(matrix, vo)

    return {
        "channel": channel,
        "cadence": {
            "vo_segments": len(vo), "footage_segments": len(foot),
            "vo_word_share": round(vo_words / (vo_words + foot_words), 3) if (vo_words + foot_words) else None,
            "mean_vo_words": round(vo_words / len(vo), 1) if vo else 0,
            "mean_footage_run_segments": round(sum(runs) / len(runs), 1) if runs else 0,
            "max_footage_run_segments": max(runs) if runs else 0,
        },
        "move_distribution": dict(move_dist.most_common()),
        "phase_x_move": matrix,
        "open_moves": [s.get("move") for s in vo[:6] if s.get("move")],
        "close_moves": [s.get("move") for s in vo[-6:] if s.get("move")],
        "prompt_directive": directive,
    }


def _prompt_directive(matrix: Dict[str, Dict[str, int]], vo: List[Dict[str, Any]]) -> str:
    """The reusable guidance string for blueprint_shape --analysis: per phase, the
    moves this channel favors (top 3)."""
    lines = ["Narration grammar (learned from this channel) — per phase, favor these analytical moves:"]
    for ph in PHASE_ORDER:
        if ph in matrix:
            top = list(matrix[ph].keys())[:3]
            lines.append(f"- {ph}: {', '.join(top)}")
    opener = [s.get("move") for s in vo[:4] if s.get("move")]
    if opener:
        lines.append(f"- open the cut with: {', '.join(dict.fromkeys(opener))}")
    return "\n".join(lines)


def mine(creator_path: Path, footage_dir: Optional[Path], timeline_path: Optional[Path],
         model: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    segs = load_segments(creator_path)
    phase_of = phase_of_artifacts(timeline_path)
    idx = build_footage_index(footage_dir, phase_of)
    for s in segs:
        ov, ph = footage_signal(s["text"], idx)
        s["footage_overlap"] = round(ov, 3)
        s["anchor_phase"] = ph
        s["role"] = "footage" if ov >= 0.34 else "vo"   # prior; LLM refines
        s["move"] = None
    _classify_llm(segs, model)
    assign_phases(segs)
    channel = json.loads(creator_path.read_text(encoding="utf-8")).get("channel", creator_path.stem)
    return build_profile(segs, channel), segs


# ---------------------------------------------------------------------------

def _selftest() -> int:
    # deterministic pieces: footage-match overlap + phase vote + aggregation.
    idx: Dict[Tuple[str, ...], Counter] = defaultdict(Counter)
    for g in _grams(_toks("morales wake up bro come on")):
        idx[g]["incident"] += 1
    ov, ph = footage_signal("Morales wake up bro come on", idx)
    assert ov > 0.5 and ph == "incident", (ov, ph)
    ov2, _ = footage_signal("On October 24th a deputy was found unresponsive", idx)
    assert ov2 == 0.0, ov2                         # pure VO -> no footage overlap
    segs = [{"i": 0, "text": "hook", "role": "vo", "move": "hook", "phase": "pre_incident"},
            {"i": 1, "text": "x", "role": "footage", "move": None, "phase": "incident", "anchor_phase": "incident"},
            {"i": 2, "text": "the record sustained neglect", "role": "vo", "move": "verdict", "phase": "investigation"}]
    prof = build_profile(segs, "Test")
    assert prof["cadence"]["vo_segments"] == 2 and prof["move_distribution"]["verdict"] == 1
    assert prof["phase_x_move"]["investigation"] == {"verdict": 1}
    print("narration_grammar selftest: OK")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--creator", type=Path, help="creator transcript json (channel, transcript[])")
    ap.add_argument("--footage", type=Path, default=None, help="dir of our raw bodycam transcripts (footage-match + phase)")
    ap.add_argument("--timeline", type=Path, default=None, help="case_timeline.json (artifact -> phase)")
    ap.add_argument("--model", default="google/gemini-3.1-flash-lite-preview")
    ap.add_argument("--out", type=Path, default=None, help="output dir (profile json + md + segments)")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        return _selftest()
    if not args.creator:
        ap.error("--creator is required")

    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(Path(__file__).resolve().parent.parent / ".env"); load_dotenv()
    except Exception:
        pass
    profile, segs = mine(args.creator, args.footage, args.timeline, args.model)

    out = Path(args.out) if args.out else args.creator.parent
    out.mkdir(parents=True, exist_ok=True)
    stem = profile["channel"].replace(" ", "_") + "_grammar"
    (out / f"{stem}.json").write_text(json.dumps(profile, indent=2, ensure_ascii=False), encoding="utf-8")
    (out / f"{stem}.segments.json").write_text(json.dumps(segs, indent=2, ensure_ascii=False), encoding="utf-8")

    c = profile["cadence"]
    print(f"[grammar] {profile['channel']}: {c['vo_segments']} VO / {c['footage_segments']} footage segments, "
          f"VO word share {c['vo_word_share']}, mean VO {c['mean_vo_words']}w, "
          f"footage runs avg {c['mean_footage_run_segments']} (max {c['max_footage_run_segments']})")
    print("[grammar] move distribution:", profile["move_distribution"])
    print("[grammar] phase x move:")
    for ph, mv in profile["phase_x_move"].items():
        print(f"    {ph:13s} {mv}")
    print("\n" + profile["prompt_directive"])
    print(f"\n[grammar] -> {out / (stem + '.json')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
