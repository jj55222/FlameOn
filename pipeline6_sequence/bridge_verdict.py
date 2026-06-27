"""Bridge a moments file (creator key, or a blind_harness run) into the P4 'verdict' shape that
render_rough_cut.py consumes — so this session's validated moments can drive the existing assembler.

    python pipeline6_sequence/bridge_verdict.py <moments.json> <out_verdict.json> [transcript_dir]
"""
import json, sys
from pathlib import Path

SRC = Path(sys.argv[1])
OUT = Path(sys.argv[2])
TDIR = sys.argv[3] if len(sys.argv) > 3 else ".tmp/2023psb0530/transcripts"

d = json.load(open(SRC))
moments = d.get("moments") or d.get("beats") or []


def art(m):
    return m.get("artifact_id") or ""


# transcript_refs: one per BWC source; source_idx indexes into it
stems = sorted({Path(art(m)).stem for m in moments if "BWC" in art(m)})
refs = [f"{TDIR}/{s}.json" for s in stems]
idx = {s: i for i, s in enumerate(stems)}

km = []
for m in sorted(moments, key=lambda x: (Path(art(x)).stem, x.get("start_sec") or 0)):
    a = art(m)
    if "BWC" not in a:
        continue
    s0 = m.get("start_sec") or 0
    km.append({
        "moment_type": m.get("moment_type", "reveal"),
        "source_idx": idx[Path(a).stem],
        "timestamp_sec": int(s0),
        "end_timestamp_sec": int(m.get("end_sec") or s0 + 8),
        "description": (m.get("summary") or m.get("why") or m.get("evidence_quote", ""))[:100],
        "importance": "critical" if (m.get("must_find") or (m.get("salience") or 0) >= 5) else "high",
        "transcript_excerpt": m.get("evidence_quote", ""),
    })

verdict = {
    "case_id": "sac_so_2023psb-0530", "verdict": "PRODUCE", "narrative_score": 75, "confidence": 0.8,
    "key_moments": km,
    "content_pitch": "A Sacramento County Sheriff's deputy overdoses on seized fentanyl at the station; "
                     "internal-affairs bodycam captures the discovery, the medical response, and the "
                     "investigation into whether he stole the seized drugs.",
    "narrative_arc_recommendation": "cold_open", "estimated_runtime_min": 5,
    "artifact_completeness": {"available": ["bodycam"], "missing_recommended": ["documents", "narration"]},
    "transcript_refs": refs, "source_refs": [],
}
OUT.parent.mkdir(parents=True, exist_ok=True)
json.dump(verdict, open(OUT, "w"), indent=1)
print(f"verdict: {len(km)} key_moments across {len(refs)} sources -> {OUT}")
