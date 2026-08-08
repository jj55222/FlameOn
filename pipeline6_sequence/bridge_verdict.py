"""Bridge a moments file into the P4 'verdict' shape the blueprint / renderers consume.

Source can be a beat_miner draft, a creator key, or a blind_harness run — anything with
a `moments`/`beats`/`key_moments` list whose items carry an `artifact_id`. Case-agnostic:
each moment's artifact_id is resolved to its ACTUAL transcript file in transcript_dir (by
the transcript's source_url media stem, then filename), so it works whether transcripts are
named `BWC-3a.json` or `<case>_<artifact>_transcript.json`.

    python pipeline6_sequence/bridge_verdict.py <moments.json> <out_verdict.json> [transcript_dir]
"""
import glob
import json
import sys
from pathlib import Path

SRC = Path(sys.argv[1])
OUT = Path(sys.argv[2])
TDIR = sys.argv[3] if len(sys.argv) > 3 else "."

d = json.load(open(SRC))
moments = d.get("moments") or d.get("beats") or d.get("key_moments") or []
case_id = d.get("case_id") or "case"


def art(m):
    """The moment's source artifact id, normalized to a stem (e.g. 'BWC-3a')."""
    a = m.get("artifact_id") or m.get("artifact") or m.get("member") or ""
    return Path(str(a)).stem


# Map each artifact stem -> its real transcript file. Prefer the transcript's source_url
# media stem (the true artifact id) over the filename, so naming schemes don't matter.
tx_by_art = {}
for f in sorted(glob.glob(str(Path(TDIR) / "*.json"))):
    try:
        t = json.load(open(f))
    except (OSError, ValueError):
        continue
    stem = Path((isinstance(t, dict) and t.get("source_url")) or f).stem
    tx_by_art.setdefault(stem, f)

# Keep moments whose artifact resolves to a transcript; index the used sources.
used = sorted({art(m) for m in moments if art(m) in tx_by_art})
idx = {a: i for i, a in enumerate(used)}
refs = [tx_by_art[a] for a in used]

km = []
for m in sorted(moments, key=lambda x: (art(x), x.get("start_sec") or 0)):
    a = art(m)
    if a not in idx:
        continue
    s0 = m.get("start_sec") or 0
    km.append({
        "moment_type": m.get("moment_type", "reveal"),
        "source_idx": idx[a],
        "timestamp_sec": int(s0),
        "end_timestamp_sec": int(m.get("end_sec") or s0 + 8),
        "description": (m.get("summary") or m.get("why") or m.get("evidence_quote", ""))[:100],
        "importance": "critical" if (m.get("must_find") or (m.get("salience") or 0) >= 5) else "high",
        "transcript_excerpt": m.get("evidence_quote", ""),
    })

verdict = {
    "case_id": case_id,
    "verdict": "PRODUCE", "narrative_score": 75, "confidence": 0.8,
    "key_moments": km,
    "content_pitch": d.get("content_pitch") or f"Auto-bridged from {len(km)} mined moments.",
    "narrative_arc_recommendation": "cold_open", "estimated_runtime_min": 10,
    "artifact_completeness": {"available": ["bodycam"], "missing_recommended": ["documents", "narration"]},
    "transcript_refs": refs, "source_refs": [],
}
OUT.parent.mkdir(parents=True, exist_ok=True)
json.dump(verdict, open(OUT, "w"), indent=1)
print(f"verdict: {len(km)} key_moments across {len(refs)} sources ({case_id}) -> {OUT}")
