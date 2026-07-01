# STATE — current project state (2026-07-01)

Single source of truth for "where things stand." Supersedes the scattered `HANDOFF_*` docs.
Start from [CLAUDE.md](CLAUDE.md); this doc is the current-state layer under it.

---

## Where we are

- **Documentary engine (P6) is proven.** The blueprint → shape → render chain produces watchable,
  fact-checked cuts. Both production paths work end to end (see the skills in `.claude/skills/`).
- **Two golden keys frozen** for calibration: `sac_so_20-269838_dutchess_way` (41 moments, reviewed)
  and `sac_so_2023psb-0530` (24 moments, creator-consensus validated).
- **P3 tests green:** `cd pipeline3_audio && python -m pytest -q` → 76 pass. P6: ~107 tests pass.

## Active task (EWU-longform)

Find an EWU/Dr.Insanity-**shape** case (high-severity crime + rich case DOC + footage —
murder / predator / discovery, *not* just OIS accountability), build a **longform flagship cut**,
and generalize into **shape-aware templates** (pick the assembly path from case shape + evidence).

- **Flagship candidate:** `sdpd_01_05_2025_4400_fanuel_street` (SDPD Use of Force, Jan 2025). BWC
  shows closed-fist head strikes; officer reports claim open-hand — a built-in footage-vs-report
  **contradiction** as the documentary spine. 5 BWC cams + interview audio + doc PDF + photos PDF.
  Basket already downloaded at `.tmp/sdpd_01_05_2025_4400_fanuel_street/`. Rawwalk short cut done.
- **Full runbook:** [HANDOFF_LONGFORM_EWU_STYLE.md](HANDOFF_LONGFORM_EWU_STYLE.md).
- **Loop-readiness:** triage → first-cut is ~loop-ready; template-building + editorial finishing are
  design work — do interactively, loop the draft-generation later.

## What "EWU-level" means (worth model)

Reverse-engineered from 428 EWU Bodycam + Dr.Insanity titles (catalogs in `.tmp/creator_catalog/`):
death/murder ~23%, discovery/reveal ~24%, domestic ~24%, comeuppance ~10%, predator ~10%,
contradiction <1%. So **worth = SEVERITY (gate) × STORY-SHAPE (small→big / reveal) × HUMAN-PROXIMITY**,
with **contradiction a MULTIPLIER, not the gate.** Editorial hallmarks: authored on-screen top-third
narration (no TTS yet), real chronology (not investigator order), cold-open on the most intense
citizen 911 call when one exists, and defamation-safe factual grounding (narration tied to the
documented outcome + case facts, never the transcript alone).

## The end-to-end flow (raw case → cut)

- **Tier-0** — coarse registry prior (`discovered_cases/CASE_BUNDLE_AGG.json`).
- **Tier-1 cheap selector** (`.tmp/_tier1_rubric.md`, <10 min/case) — severity × shape × proximity
  from doc-OCR (front ~30pp) + 911/dispatch only, NO interview transcription → PRODUCE/HOLD/SKIP.
  Reproduces the operator's boring/interesting split 9/10. See skill `case-selection`.
- **Tier-2 (PRODUCE only)** — download media, `timeline_stamp` (AXON clock OCR → real chronology),
  `mlx` transcribe full slate, then one of:
  - **Path A rawwalk** (short, bodycam-driven) → skill `rawwalk-shortform`.
  - **Path B flagship** (longform, doc/narration-driven) → skill `flagship-longform`.
    One-shot: `make_documentary.py --flagship --run`.
- **Finish** — `judge.py` gate (SHIP/REVISE/REWORK) + operator review. Not loop-ready.

## Known issues / tuning backlog

- **P4 mis-calibration (important):** P4's `moment_density ≥ 60` PRODUCE gate is a *discovery-triage*
  metric; interview/doc-driven cases can't reach it even when fatal, so P4 says HOLD where a strong
  LLM says PRODUCE. **For EWU longform, select with Tier-1 + `beat_miner`, not P4.** Either build a
  separate "rank good cases" mode with relaxed thresholds, or keep P4 only for wide triage. Detail:
  [P4_VS_CLAUDE_CONCORDANCE.md](P4_VS_CLAUDE_CONCORDANCE.md).
- **Narration timing** — top-slide lingers the whole clip and can describe events not yet on screen;
  wants a TIMED overlay (~first 6–8s) that advances with sub-events. TTS still needed to actually
  *hear* the EWU vs Dr.Insanity style difference (3s cards too short to read analytical lines).
- **Post-teaser runs too long** — tighten windows after the cold-open teaser.
- **Silent drive-up stretches** have no fill when there's no citizen 911 call — light trim or narrate.
- **Caption font** ~30px reads small; bump ~40px.
- **AXON OCR trust** — only trust `axon_ocr`-stamped cams for chronology; some BWCs fall back to a
  wrong export-date metadata stamp (la jolla BWC_1). Distrust metadata stamps.

## Load-bearing fixes already made (don't re-hit)

- `render_blueprint --no-audio-aware` is **required** for chronological cuts (audio-aware snap drags
  establishing clips to the loudest audio = gunfire, putting the shooting at the front).
- **Incident anchoring:** timeline phases anchor on the earliest *trusted* stamp; if that's wrong the
  story plays backwards. Use `timeline_build --incident "YYYY-MM-DD HH:MM:SS"` (UTC) or
  `blueprint.py --auto-anchor` (salience × camera-convergence, not raw argmax). 2023PSB-0530 correct
  anchor = `2023-10-24 20:45:00` (the OD, not the 17:42 seizure).
- **Narration grounding is defamation-critical:** ground on documented outcome (`doc_extract` →
  else tier1 verdict), not transcript alone — transcript-only narration once narrated a fatal
  shooting as "he complied." `blueprint_shape` injects a `CASE_FACTS` block + `audit_narration()`
  fact-check rail (quarantines invented civilian-detainee framing + ungrounded accusations).
- **Clip audio codec:** extracting BWC (AAC) clips with `-c copy` to `.mp3` makes silent/invalid
  files — must re-encode `-vn -c:a libmp3lame`.
- **`source_url` per transcript:** every transcript needs `source_url` = absolute media path, else
  same-type-no-person refs collapse onto the first file.
- **PEAK detection** uses force-ONSET cues ("shots fired", "drop the knife", "taser"), not bare
  "shot"/"shooting" (recurs in medical aftermath).

## Doc map — what's current vs archived

**Current (keep, load on demand):**
- [CLAUDE.md](CLAUDE.md) — root map (start here).
- [HANDOFF_LONGFORM_EWU_STYLE.md](HANDOFF_LONGFORM_EWU_STYLE.md) — the EWU playbook + active task.
- [HANDOFF.md](HANDOFF.md) — macOS setup, factual-grounding + incident-anchor fixes.
- [HANDOFF_A_TIER_PROCESSING.md](HANDOFF_A_TIER_PROCESSING.md) — top-10 SDPD A-tier processing plan.
- [HANDOFF_MUCKROCK_INTAKE.md](HANDOFF_MUCKROCK_INTAKE.md) — MuckRock case-discovery harvester.
- [HANDOFF_2026-06-26.md](HANDOFF_2026-06-26.md) — salience eval harness + golden keys.
- [P4_VS_CLAUDE_CONCORDANCE.md](P4_VS_CLAUDE_CONCORDANCE.md) — the P4 selector landmine.
- [SYSTEM.md](SYSTEM.md) — **original 5-pipeline design vision. Historical** — predates P6 and marks
  most stages "Not started." Read for intent, NOT for current reality (this STATE.md is reality).

**Archived** (moved to `docs/archive/` — stale, superseded, or one-off external handoffs):
`HANDOFF_2026-04-29.md`, `HANDOFF_P2_FOR_CHATGPT.md`, `CODEX_HANDOFF.md`, `experiments.md`.
