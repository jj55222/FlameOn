"""Assemble the Dutchess golden key: audio-mined beats + doc Pass-2 panel beats → one golden_moments file.

Audio beats carry start_sec + an audio artifact_id; doc beats carry a page + the PDF artifact_id.
Status is 'auto-judged', NOT 'reviewed' — a human must confirm salience/prune before this is frozen
as the eval yardstick (mutating a frozen key is the reward-hacking risk the literature warns about).
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AUDIO = ROOT / "pipeline4_scoring/golden/sac_so_20-269838_dutchess_way.mined.json"
DOC = ROOT / ".tmp/dutchess_269838/pass2/panel.json"
OUT = ROOT / "pipeline4_scoring/golden/sac_so_20-269838_dutchess_way.golden.merged.json"

audio = json.load(open(AUDIO))
doc = json.load(open(DOC))
moments = []

for i, m in enumerate(audio["moments"], 1):
    moments.append({
        "moment_id": f"a{i:02d}", "source_layer": "audio",
        "artifact_id": m.get("artifact_id"), "moment_type": m.get("moment_type"),
        "salience": m.get("salience"), "importance": m.get("importance"),
        "start_sec": m.get("start_sec"), "end_sec": m.get("end_sec"), "page": None,
        "summary": m.get("summary"), "evidence_quote": m.get("evidence_quote"),
        "angle": m.get("angle"), "must_find": m.get("must_find"),
    })

for j, b in enumerate(doc["beats"], 1):
    sal = b.get("salience")
    moments.append({
        "moment_id": f"d{j:02d}", "source_layer": "document",
        "artifact_id": "20-269838 Part 1.pdf", "moment_type": b.get("moment_type"),
        "salience": round(sal) if isinstance(sal, (int, float)) else sal,
        "importance": b.get("importance"), "start_sec": None, "end_sec": None,
        "page": b.get("page"), "summary": b.get("summary"),
        "evidence_quote": b.get("evidence_quote"), "angle": b.get("angle"),
        "must_find": b.get("must_find"), "judge_agreement": b.get("agreement"),
    })

out = {
    "case_id": "sac_so_20-269838", "agency": "Sacramento County Sheriff's Office",
    "labeling": {
        "labeler": "auto: audio beat_miner(cue) + 3-model Pass-1 ensemble + minimax/v4-pro Pass-2 panel",
        "status": "auto-judged",
        "notes": "NOT human-reviewed. Audio salience is provisional (cue heuristic); doc beats are LLM-judged + "
                 "grounded to a page. A human must confirm salience and prune before status=reviewed and freezing "
                 "this as the immutable eval yardstick.",
    },
    "moments": moments,
}
OUT.write_text(json.dumps(out, indent=1) + "\n", encoding="utf-8")
na = sum(1 for m in moments if m["source_layer"] == "audio")
nd = len(moments) - na
print(f"merged golden: {len(moments)} moments ({na} audio + {nd} doc), "
      f"must_find={sum(1 for m in moments if m.get('must_find'))} -> {OUT.name}")
