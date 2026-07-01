---
name: case-selection
description: Decide whether a case is worth producing using the cheap Tier-1 selector — severity × story-shape × human-proximity from doc-OCR + 911 only, NO interview transcription, <10 min/case. Use to triage discovered cases into PRODUCE/HOLD/SKIP before spending download+transcription budget. Do NOT use P4's pipeline4_score gate for this — it is mis-calibrated for ranking good cases (see below).
---

# Case selection — Tier-1 cheap selector

Reproduces the operator's boring/interesting split 9/10 from CHEAP inputs only. The worth model was
reverse-engineered from 428 EWU Bodycam + Dr.Insanity titles: **worth = SEVERITY (gate) × STORY-SHAPE
(small→big / reveal) × HUMAN-PROXIMITY**, with **contradiction a MULTIPLIER, not the gate.**

## Why not just use P4 (important)
`pipeline4_scoring/pipeline4_score.py` gates PRODUCE on `moment_density ≥ 60` — a *discovery-triage*
metric. Interview/doc-driven cases (911, interrogation, IA reports) can't reach that density even
when fatal, so P4 says HOLD/SKIP where a strong LLM says PRODUCE. **P4 is the wrong selector for
EWU-worthy cases.** Use this Tier-1 selector to gate, and `beat_miner` (recall) for moments on
survivors. Detail: [P4_VS_CLAUDE_CONCORDANCE.md](../../../P4_VS_CLAUDE_CONCORDANCE.md).

## Tiering
- **Tier-0** — free registry prior (`discovered_cases/CASE_BUNDLE_AGG.json`).
- **Tier-1** — this: doc+911 LLM gate (below).
- **Tier-2** — `beat_miner` mines full transcripts, PRODUCE survivors only. Never open an interview
  at Tier-1; the real cost is DOWNLOAD, so fetch only doc+911, not the GB of video.

## Recipe
```
source .venv/bin/activate
eval "$(/opt/homebrew/bin/brew shellenv)"

# 1. get the case bundle into a basket (case_id, media manifest, docs)
python discovered_cases/bundle_to_basket.py ...

# 2. cheap doc OCR — cap the front matter (~30 pages), Apple Vision
python pipeline3_audio/doc_ocr.py --pdf <CASE>.pdf --end 30 --out <basket>/docs/

# 3. run the Tier-1 selector harness (severity × shape × proximity, doc + 1-3 911/dispatch only)
python .tmp/_tier1_prep.py   ...      # assembles the cheap inputs
#   judge against .tmp/_tier1_rubric.md → PRODUCE / HOLD / SKIP + worth
python .tmp/_tier1_compare.py ...     # compares verdicts vs the operator's split
```
Harness lives in `.tmp/`: `_tier1_prep.py`, `_tier1_rubric.md`, `_tier1_compare.py`. Creator
catalogs (the worth-model corpus): `.tmp/creator_catalog/`.

## Calibration notes
- **Recall-leaning** — never SKIP a high-severity death case on a dry doc.
- **911-presence is a free severity tell** — boring cases had zero dispatch; every interesting one
  had 911/dispatch traffic.
- **Multiplier cap** — a use-of-force case with no death (e.g. fanuel, sev 55) can ride
  contradiction×1.2 + footage×1.2 to a PRODUCE it maybe shouldn't; cap multiplier lift below ~sev70,
  or accept it as borderline.
- All the "interesting" cases clamp to worth 100 — good GATE, poor top-RANKER. Ranking survivors
  needs an un-clamped score or the deeper Tier-2 pass.

## After a PRODUCE verdict
Route by case shape to a production skill:
- Bodycam-driven incident that reads chronologically → **rawwalk-shortform**.
- High-severity + rich case DOC + footage → **flagship-longform** (the EWU path).

Reference: memory `tier1-cheap-selector`; [STATE.md](../../../STATE.md).
