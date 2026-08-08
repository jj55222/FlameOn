"""Build the held-out creator-consensus salience key for 2023PSB-0530 (v2 — sharp-signal weighted).

"In both cuts" was too generous (channels air CONTINUOUS footage). v2 leads with the signals that
actually mark salience:
  • comment peak   — the audience reacted there (cuts through continuous-airing noise)   [strongest]
  • in EWU's cut   — EWU compressed to 9.5 min, so what they kept is genuinely selective
  • EWU narration  — EWU segments that DON'T match raw = the host talking; narration sitting on/near
                     an aired clip means EWU spotlighted that moment
A moment is KEPT only if it's in EWU's tight cut OR on a comment peak (PT-only continuous airing is dropped).
Output is grounded in the RAW evidence (real BWC timestamps).
"""
import json, glob, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from evaluate_salience import _norm

RAW = ".tmp/2023psb0530/transcripts"
CRE = ".tmp/2023psb0530/creator_transcripts"
OUT = "pipeline4_scoring/golden/sac_so_2023psb-0530.creator_key.json"
AIR, NARR = 0.5, 0.3   # overlap thresholds: >=AIR = aired raw line; <NARR = creator narration
PEAKS = {"EWU": [(559, 4), (105, 3)], "PoliceTransparency": [(2554, 6), (2256, 4), (915, 3), (2370, 3)]}
STOP = set("the a an and to of is in it i you he we they that this on at for so im its are was have "
           "has had do did not no yes ok okay hes im got get go going right just like what your".split())


def toks(s):
    return {w for w in _norm(s).split() if w not in STOP and len(w) > 1}


# raw BWC lines (token-sets) for narration classification
raw_lines = []
for f in glob.glob(RAW + "/BWC*.json"):
    for s in json.load(open(f))["transcript"]:
        t = toks(s["text"])
        if len(t) >= 4:
            raw_lines.append(t)

creators = {}
for f in glob.glob(CRE + "/*.json"):
    d = json.load(open(f))
    segs = [(s["start_sec"], toks(s["text"])) for s in d["transcript"]]
    # classify narration: a creator cue whose best overlap with ANY raw line < NARR
    narr_times = [st for st, ct in segs if ct and max((len(ct & r) / len(ct) for r in raw_lines), default=0) < NARR]
    creators[d["channel"]] = {"segs": segs, "narr": narr_times}
print(f"creators: " + ", ".join(f"{k} ({len(v['segs'])} cues, {len(v['narr'])} narration)" for k, v in creators.items()))


def aired_at(rt, segs):
    best, bt = 0.0, None
    for st, ct in segs:
        if rt:
            o = len(rt & ct) / len(rt)
            if o > best:
                best, bt = o, st
    return (best, bt) if best >= AIR else (0.0, None)


moments, seen = [], set()
for f in sorted(glob.glob(RAW + "/BWC*.json")):
    d = json.load(open(f))
    src = d["source_file"]
    for s in d["transcript"]:
        rt = toks(s["text"])
        if len(rt) < 4:
            continue
        ewu_o, ewu_t = aired_at(rt, creators["EWU"]["segs"]) if "EWU" in creators else (0, None)
        pt_o, pt_t = aired_at(rt, creators["PoliceTransparency"]["segs"]) if "PoliceTransparency" in creators else (0, None)
        in_ewu, in_pt = ewu_t is not None, pt_t is not None
        peak = ((ewu_t is not None and any(abs(ewu_t - p) <= 20 for p, _ in PEAKS["EWU"])) or
                (pt_t is not None and any(abs(pt_t - p) <= 20 for p, _ in PEAKS["PoliceTransparency"])))
        if not (in_ewu or peak):          # drop PT-only continuous airing
            continue
        # EWU narration emphasis: how many EWU narration cues sit within ±15s of where EWU aired this
        emph = 0
        if in_ewu and "EWU" in creators:
            emph = sum(1 for nt in creators["EWU"]["narr"] if abs(nt - ewu_t) <= 15)
        key = _norm(s["text"])[:55]
        if key in seen:
            continue
        seen.add(key)
        # salience: audience peak > in-both-cuts > EWU-only. narration emphasis is a RANK hint only
        # (EWU narrates constantly, so it can't gate must-find).
        sal = 3
        if in_ewu and in_pt:
            sal = 4
        if peak:
            sal = 5
        moments.append({"artifact_id": src, "start_sec": s["start_sec"], "end_sec": s["end_sec"],
                        "evidence_quote": s["text"].strip(),
                        "in_creators": ([c for c in ("EWU",) if in_ewu] + [c for c in ("PoliceTransparency",) if in_pt]),
                        "comment_peak": peak, "ewu_narration_emphasis": emph, "salience": sal,
                        "must_find": bool(peak)})

moments.sort(key=lambda m: (m["artifact_id"], m["start_sec"]))
for i, m in enumerate(moments, 1):
    m["moment_id"] = f"c{i:02d}"
key = {"case_id": "sac_so_2023psb-0530", "agency": "Sacramento County Sheriff's Office",
       "labeling": {"labeler": "creator-consensus v2 (EWU tight cut + comment peaks + EWU narration emphasis)",
                    "status": "auto-derived", "notes": "HELD-OUT key candidate. KEEP = in EWU's 9.5-min cut OR on an "
                    "audience comment peak; PT-only continuous airing dropped. Human spot-check before freezing."},
       "moments": moments}
Path(OUT).write_text(json.dumps(key, indent=1) + "\n")
pk = sum(1 for m in moments if m["comment_peak"])
mf = sum(1 for m in moments if m["must_find"])
print(f"\ncreator key v2: {len(moments)} moments | {pk} comment-peak | {mf} must-find -> {OUT}")
