"""Build the held-out creator-consensus salience key for 2023PSB-0530.

The creators played the actual bodycam audio, so a raw BWC transcript line that appears in a
creator's cut = a moment that creator chose to include. Score each raw moment by how many
creator cuts included it (consensus) × whether EWU's tight cut kept it × proximity to an
audience comment peak. The result is a golden_moments key grounded in the RAW evidence
(real BWC timestamps), derived entirely from what creators + viewers converged on.
"""
import json, glob, os, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from evaluate_salience import _norm

RAW = ".tmp/2023psb0530/transcripts"
CRE = ".tmp/2023psb0530/creator_transcripts"
OUT = "pipeline4_scoring/golden/sac_so_2023psb-0530.creator_key.json"
THRESH = 0.5
STOP = set("the a an and to of is in it i you he we they that this on at for so im its are was "
           "have has had do did not no yes ok okay he's i'm got get go going right just like".split())

# Audience comment peaks in CREATOR-video time (sec, weight) from the comment parse.
PEAKS = {"EWU": [(559, 4), (105, 3)],
         "PoliceTransparency": [(2554, 6), (2256, 4), (915, 3), (2370, 3)]}


def toks(s):
    return {w for w in _norm(s).split() if w not in STOP and len(w) > 1}


creators = {}
for f in glob.glob(CRE + "/*.json"):
    d = json.load(open(f))
    creators[d["channel"]] = [(s["start_sec"], toks(s["text"])) for s in d["transcript"]]
print(f"creators loaded: {', '.join(f'{k} ({len(v)} cues)' for k, v in creators.items())}")


def best_match(rt, segs):
    best, bt = 0.0, None
    for st, ct in segs:
        if rt:
            o = len(rt & ct) / len(rt)
            if o > best:
                best, bt = o, st
    return best, bt


moments, seen = [], set()
for f in sorted(glob.glob(RAW + "/BWC*.json")):
    d = json.load(open(f))
    src = d["source_file"]
    for s in d["transcript"]:
        rt = toks(s["text"])
        if len(rt) < 4:
            continue
        incl, peak = [], False
        for chan, segs in creators.items():
            o, mt = best_match(rt, segs)
            if o >= THRESH:
                incl.append(chan)
                if mt is not None and any(abs(mt - pt) <= 20 for pt, _ in PEAKS.get(chan, [])):
                    peak = True
        if not incl:
            continue
        key = _norm(s["text"])[:60]
        if key in seen:
            continue
        seen.add(key)
        n = len(incl)
        sal = 5 if n >= 2 else (4 if "EWU" in incl else 3)
        if peak:
            sal = min(5, sal + 1)
        moments.append({"artifact_id": src, "start_sec": s["start_sec"], "end_sec": s["end_sec"],
                        "evidence_quote": s["text"].strip(), "in_creators": incl, "comment_peak": peak,
                        "salience": sal, "must_find": (n >= 2 or peak)})

moments.sort(key=lambda m: (m["artifact_id"], m["start_sec"]))
for i, m in enumerate(moments, 1):
    m["moment_id"] = f"c{i:02d}"
key = {"case_id": "sac_so_2023psb-0530", "agency": "Sacramento County Sheriff's Office",
       "labeling": {"labeler": "creator-consensus (EWU + Police Transparency cuts + comment peaks)",
                    "status": "auto-derived",
                    "notes": "HELD-OUT grading key. Each moment is a raw BWC line a creator chose to air. "
                             "salience = consensus(in N cuts) + EWU-tight-cut + audience-comment-peak. "
                             "PoliceActivity cut unavailable (yt-dlp blocked). Human spot-check before freezing."},
       "moments": moments}
Path(OUT).write_text(json.dumps(key, indent=1) + "\n")
both = sum(1 for m in moments if len(m["in_creators"]) == 2)
pk = sum(1 for m in moments if m["comment_peak"])
print(f"\ncreator key: {len(moments)} moments | {both} in BOTH cuts | {pk} on a comment peak | "
      f"{sum(1 for m in moments if m['must_find'])} must-find -> {OUT}")
