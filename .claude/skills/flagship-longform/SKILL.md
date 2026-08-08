---
name: flagship-longform
description: Produce a longform EWU/Dr.Insanity-style narrated documentary cut (Path B) from a case basket — doc-driven arc, fact-checked narration, ~10-40 min. Use when a high-severity case has a rich case DOC (IA report / FOIA PDF) plus footage and you want the flagship narrated documentary, not a raw bodycam walk. This is the primary path toward "EWU-level" output.
---

# Flagship longform documentary (Path B)

The narrated, doc-driven EWU/Dr.Insanity path. Turns a case basket into a fact-checked longform cut.

## Prereqs (every render-touching command)
```
source .venv/bin/activate
eval "$(/opt/homebrew/bin/brew shellenv)"      # brew ffmpeg/ffprobe MUST be on PATH
```
Basket layout: `.tmp/<case_id>/` with `video/Video/` (media), the case PDF, and (after run)
`timeline/`, `d2/transcripts/`, `d6_blueprint/`, `cuts/`. Media zips live in `~/Downloads/`.

## The one command (does the whole chain)
`make_documentary.py --flagship` chains all 11 steps internally:
stamp → (align dashcams) → build-timeline → doc-ocr → doc-extract → transcribe →
beat_miner (RECALL) → bridge_verdict → blueprint (auto-anchor + auto-runtime) → shape → render →
judge gate.

```
# dry-run plan first (default), then add --run to execute:
python pipeline6_sequence/make_documentary.py \
  --basket .tmp/<case_id> --case-id <case_id> \
  --agency "San Diego Police Dept." \
  --doc ".tmp/<case_id>/docs/<CASE> Documents.pdf" \
  --flagship --target-runtime 600 --run
```
- **Dry-run by default** — omit `--run` to see the step plan without executing.
- `--target-runtime SEC` — auto-fits clip lengths to hit the runtime exactly, never pads past
  available footage (binary-searches `--min-clip-sec`). 600 ≈ 10 min.
- `--moments beatminer` (default) is the RIGHT selector for documentary — recall-optimized. Do NOT
  use `--moments p4`; P4's density gate is mis-calibrated for interview/doc-driven cases (see below).
- Paid steps (shape, and beat_miner/score) need `OPENROUTER_API_KEY`. `--judge-mock` gives a free
  deterministic gate. `--shape-model` / `--moments-model` default to `deepseek/deepseek-v4-flash`.
- Output: `.tmp/<case_id>/cuts/<case_id>/<case_id>_rough_cut.mp4` + `_judge.json` gate verdict.
  The judge gate exits 3 (HOLD) on REWORK/unsourced footage, else 0 — it never aborts the run
  (the cut is already rendered). `open` the mp4 when it finishes.

## For true EWU / Dr.Insanity *voice* (analytical narration)
The one-shot produces a grounded connective cut. To get the analytical creator voice, re-shape the
blueprint with a learned narration grammar, then re-render:
```
python pipeline6_sequence/blueprint_shape.py \
  --blueprint .tmp/<cid>/d6_blueprint/<cid>_blueprint.json \
  --analysis --grammar .tmp/grammar_corpus/grammar/EWU_Flagship_grammar.json \
  --model deepseek/deepseek-v4-flash --max-tokens 12000 \
  --out .tmp/<cid>/d6_blueprint/
# then render_blueprint.py on the _blueprint_shaped.json (see render step)
```
- Grammars available: `EWU_Flagship_grammar.json`, `DrInsanity_grammar.json` (in `.tmp/grammar_corpus/grammar/`).
- `--analysis` swaps to `_SYSTEM_ANALYSIS` + an `ANALYSIS_BASIS` (SUSTAINED findings = the ONLY
  judgments narration may assert). `--max-tokens 12000` — analytical output is verbose; under ~7k it
  TRUNCATES → JSON parse fails → silently degrades to skeleton.

## Non-negotiables (defamation + chronology)
- **Narration is grounded on the documented OUTCOME**, never the transcript alone. `blueprint_shape`
  injects a `CASE_FACTS` block + `audit_narration()` rail that quarantines invented framing and
  ungrounded accusations. Never disable it. (Transcript-only narration once called a fatal shooting
  "he complied.")
- **Incident anchor.** Flagship auto-anchors via salience × camera-convergence. If it anchors wrong
  (story plays backwards — aftermath first), override the timeline step: rebuild with
  `timeline_build.py --incident "YYYY-MM-DD HH:MM:SS"` (UTC). Re-anchoring cascades acts
  deterministically; the shaper only reorders WITHIN acts.
- **Only trust `axon_ocr`-stamped cams for chronology** — some BWCs fall back to a wrong export-date
  metadata stamp.

## Manual chain (only when debugging a single step)
Each step above is a real script under `pipeline3_audio/`, `pipeline4_scoring/`,
`pipeline6_sequence/`. Run `make_documentary.py --flagship` (no `--run`) to print the exact argv for
every step, then run just the one you're debugging. Key scripts: `timeline_stamp.py`,
`timeline_build.py`, `doc_ocr.py` → `doc_extract.py`, `timeline_transcribe.py`, `beat_miner.py` →
`bridge_verdict.py`, `blueprint.py` → `blueprint_shape.py` → `render_blueprint.py` → `judge.py`.

Reference: [HANDOFF_LONGFORM_EWU_STYLE.md](../../../HANDOFF_LONGFORM_EWU_STYLE.md), [STATE.md](../../../STATE.md).
