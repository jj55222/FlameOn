"""Blind harness run: extract salient moments from the RAW BWC transcripts (the harness never sees
the creator videos), ground each quote, tag its BWC file. Output feeds score_run vs the frozen
creator key — measuring whether the harness rediscovers what creators + viewers converged on."""
import json, glob, sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(str(Path(__file__).resolve().parents[1] / ".env"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from llm_backends import LLMBackend, LLMError
from evaluate_salience import _norm, grounding_of

MODEL = sys.argv[1] if len(sys.argv) > 1 else "deepseek/deepseek-v4-flash"
TYPES = "reveal|emotional_peak|procedural_violation|detail_noticed|tension_shift|contradiction|callback"
SYS = ("You are a true-crime story editor finding the most SALIENT moments in a police body-worn-camera "
       "transcript (an incident where a deputy overdosed on seized fentanyl at the station). Return ONLY JSON.")


def prompt(segs):
    lines = "\n".join(f"[{int(s['start_sec'])}s] {s['text']}" for s in segs)
    return (f'Extract up to 20 of the most salient moments as JSON: {{"beats":[{{"start_sec": int (from the [Ns] tag), '
            f'"evidence_quote": VERBATIM transcript text, "moment_type": one of [{TYPES}], "why": one line}}]}}. '
            'Quote EXACTLY from the transcript.\n\nTRANSCRIPT:\n' + lines)


backend = LLMBackend(model=MODEL, timeout=600, max_retries=2)
allbeats = []
for f in sorted(glob.glob(".tmp/2023psb0530/transcripts/BWC*.json")):
    d = json.load(open(f))
    segs, src = d["transcript"], d["source_file"]
    blob = _norm(" ".join(s["text"] for s in segs))
    toks = set(blob.split())
    try:
        raw = backend.complete(system=SYS, user=prompt(segs), max_tokens=8000, temperature=0.2)
        beats = json.loads(raw).get("beats", [])
    except (LLMError, json.JSONDecodeError) as e:
        print(f"{src}: FAILED {str(e)[:80]}")
        continue
    for b in beats:
        b["artifact_id"] = src
        b["grounding"] = grounding_of(b.get("evidence_quote", ""), blob, toks)
    grounded = [b for b in beats if b["grounding"]]
    allbeats += grounded
    print(f"{src}: {len(beats)} proposed, {len(grounded)} grounded")

json.dump({"case_id": "sac_so_2023psb-0530", "model": MODEL, "beats": allbeats},
          open(".tmp/2023psb0530/harness_run.json", "w"), indent=1)
print(f"TOTAL: {len(allbeats)} grounded harness beats -> .tmp/2023psb0530/harness_run.json")
