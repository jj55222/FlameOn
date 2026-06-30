# HANDOFF — find an EWU/Dr.Insanity-shape case, make a LONGFORM cut, build shape-aware templates

Read first: `MEMORY.md` (auto-memory index) and the memory files it points to — especially
`rawwalk-template.md`, `tier1-cheap-selector.md`, `p4-selector-vs-llm-concordance.md`,
`auto-open-cuts.md`. Then `HANDOFF.md` (the flagship doc-engine) and this runbook.

---

## THE GOAL
1. **Find & explore an EWU / Dr.Insanity-shape case** in our files — i.e. a strong CRIME
   NARRATIVE with a rich case DOCUMENT + footage, not just a police-accountability OIS.
   The creator worth-model (learned from 428 EWU/Dr.Insanity titles, in
   `.tmp/creator_catalog/`): **death/murder 23%, discovery/reveal 24%, domestic 24%,
   comeuppance 10%, predator/CSA 10%, contradiction <1%.** So look for: a murder /
   predator-sting / kidnapping / "small thing unfolds into a big crime" / a sly-detective
   investigation — high SEVERITY × a DISCOVERY/REVEAL story-shape × HUMAN proximity.
2. **Make a LONGFORM video from it** (the raw-walk below is SHORT-form; longform = the
   FLAGSHIP narrated path — see "Two assembly paths").
3. **Build shape-aware template structure** — pick the assembly path/shape from the case's
   SHAPE + the EVIDENCE present (bodycam-driven vs doc/investigation-driven vs interview-rich).

## OPERATOR PREFERENCES (do these)
- **Auto-open every cut the instant it renders** (`&& open "<cut>.mp4"`); tight watch→note loop. ([[auto-open-cuts]])
- Style ref = **Midwest Safety / EWU / Dr.Insanity**: mostly-raw, lightly-edited, CHRONOLOGICAL,
  narrated (on-screen top-third text, NO TTS yet), burned captions, **NO post-incident interviews**
  unless paired with artifacts.
- **Cold-open on the most emotionally intense CITIZEN 911 call when one exists**; else a short
  climax teaser (last command → shots). NOT dispatch coordination.
- Contradiction is a MULTIPLIER, not the gate. SEVERITY (did someone die? murder? predator?) leads.

---

## CURRENT STATE (what's built & proven)
- **Selection**: `pipeline4_score.py` (P4, LLM) is mis-calibrated for ranking good cases (it
  never PRODUCEs A-tier; density-gate). Replaced by the **Tier-1 cheap selector** (severity ×
  story × human, doc+911 only, <10 min, no transcription) — reproduces the operator's
  boring/interesting split 9/10. ([[tier1-cheap-selector]])
- **Raw-walk template** = `pipeline6_sequence/rawwalk.py` — auto primary-camera + action-peak +
  chronological continuous runs + cold-open teaser + narration; SHORT-form, for OIS/UoF-with-bodycam.
  Proven on 2 cases: **logan LOCKED** (`.tmp/sdpd_01_20_2023_logan_avenue/LOCKED/` +
  `~/Downloads/FlameOn_logan_rawwalk_LOCKED.mp4`) and **la jolla** (auto-built 2nd case). ([[rawwalk-template]])
- **Flagship longform path** validated earlier on **Morales** (`.tmp/2023psb0530/`, a ~10-min
  narrated cut) — see `HANDOFF.md`. THIS is the path for longform.

## TWO ASSEMBLY PATHS (pick by case shape)
- **RAW-WALK (short, bodycam-driven)** — `rawwalk.py` → `render_blueprint --no-audio-aware`.
  Use when the story IS the footage (a stop/shooting/UoF on bodycam). Chronological replay.
- **FLAGSHIP (longform, doc/narration-driven)** — `blueprint.py` → `blueprint_shape.py` (authors
  acts/theses/narration, fact-checked) → `render_blueprint.py`. Use for EWU-FLAGSHIP longform: a
  narrated ARC over clips + the case doc + photos. THIS is the goal for "more longform."
  beat_miner (per-source, gemini-flash-lite — quotes verbatim) → bridge_verdict → blueprint → shape.

## PROCESSING STRUCTURE (the recipe; always do the env first)
```bash
cd /Users/jmoney/FlameOn-main
eval "$(/opt/homebrew/bin/brew shellenv)"      # ffmpeg/ffprobe on PATH (NOTE: this ffmpeg has NO libass/drawtext — all text is PIL→PNG→overlay)
set -a; . ./.env; set +a                       # OPENROUTER_API_KEY (paid LLM steps). NOTE: YOUTUBE_API_KEY is a dead placeholder — use yt-dlp for YouTube metadata.
```
Tier-0 (free): registry score (`discovered_cases/sdpd_candidates.json` `score`) → coarse prior.
Tier-1 (cheap triage, <10 min): `discovered_cases/bundle_to_basket.py` (download; fetch doc+911 only to stay cheap) →
   `pipeline3_audio/doc_ocr.py` (Vision, cap `--end 30`) → `.tmp/_tier1_prep.py` → blind selector via
   `.tmp/_tier1_rubric.md` → `.tmp/_tier1_compare.py`.
Tier-2 (full production, PRODUCE cases only — "process the FULL slate"):
   `pipeline3_audio/timeline_stamp.py --basket <B>/video --out <B>/timeline`  (AXON clock OCR; the
       `detect_kind` fix matches SDPD `Officer1BodyCameraVideo`; metadata stamps = export date, distrust) →
   `.venv/bin/python .tmp/_p4_transcribe.py --basket <B> --case-id <cid> --include-video --clean`  (mlx-whisper, full slate) →
   RAW-WALK: `pipeline6_sequence/rawwalk.py --basket <B> --case-id <cid> --agency "..." --out <bp> --narrate`
       → `pipeline6_sequence/render_blueprint.py --blueprint <bp>/<cid>_rawwalk.json --media-dir <B>/video/Video
          --transcripts <B>/d2/transcripts --no-audio-aware --out <cuts> && open "<cuts>/<cid>/<cid>_rough_cut.mp4"`
   FLAGSHIP (longform): timeline_build (--incident anchor) → doc_extract → beat_miner(per-source) →
       bridge_verdict → blueprint(--auto-anchor) → blueprint_shape(--model deepseek/deepseek-v4-flash) → render_blueprint.

## WHERE EVERYTHING IS
- **Assembly tools**: `pipeline6_sequence/` — `rawwalk.py`, `blueprint.py`, `blueprint_shape.py`,
  `render_blueprint.py`, `render_rough_cut.py`, `bridge_verdict.py`, `make_documentary.py`.
- **Media/doc tools**: `pipeline3_audio/` — `timeline_stamp.py`, `timeline_build.py`, `doc_ocr.py`
  (Vision OCR), `doc_extract.py`. Transcriber: `.tmp/_p4_transcribe.py` (mlx, `--include-video --clean --only <rx> --max-files N`).
- **Selection/scoring**: `pipeline4_scoring/` — `pipeline4_score.py` (P4), `beat_miner.py`,
  `llm_backends.py` (OpenRouter), golden keys. Tier-1: `.tmp/_tier1_rubric.md`, `_tier1_prep.py`, `_tier1_compare.py`.
- **Case registry**: `discovered_cases/CASE_BUNDLE_AGG.json` (all bundles + tier), `sdpd_candidates.json`,
  `SDPD_VIDEO_CATALOG.md`, `bundle_to_basket.py`. (Sources incl. SDPD/OIS, COPA, MuckRock, Long Beach —
  SDPD is mostly OIS/UoF/sustained; an EWU/Dr.Insanity murder/predator case may need a non-SDPD source.)
- **Creator worth-model data**: `.tmp/creator_catalog/` — `EWU_Bodycam_titles.txt`, `DrInsanity_titles.txt`,
  `*_desc.txt`, harvester `.tmp/_creator_catalog.py` (yt-dlp; Dr.Insanity descriptions are full case synopses).
- **Case baskets**: `.tmp/<cid>/` — `video/Video/` (media), `docs/` (PDFs), `d2/transcripts/`,
  `d2/doc_ocr*/`, `d2/verdicts_p4/`, `d2/tier1_verdict.json`, `d2/claude_verdict.json`,
  `d2/dossier.md` (per-case interrogation), `timeline/artifacts.json`+`case_timeline.json`,
  `blueprint*/`, `d6_cuts*/`, `LOCKED/`.
- **Locked reference cuts**: logan (above); Morales longform in `.tmp/2023psb0530/`.
- **Per-case dossiers** (deep reads): `.tmp/<cid>/d2/dossier.md` for the top OIS cases.

## KNOWN TUNING ITEMS (from operator notes — fix in the template)
- **Runs too long after the teaser** (la jolla note: "first 7s perfect, after that too long"):
  tighten the post-teaser window lengths in `rawwalk.windows()` (PRE=115/POST=45 is too long; try
  shorter, and/or split into more, shorter beats).
- **Narration timing/sync** (flagged twice): the top-third slide lingers the whole run and describes
  events not yet on screen. Make narration a TIMED overlay (show ~6-8s at the moment it describes,
  then clear) and/or split long runs into sub-beats so it advances. (Currently baked into the
  lower-third PNG for the whole clip in `render_rough_cut.make_lower_third_png` top_text.)
- **Silent stretches** (e.g. la jolla's ~1-min silent drive-up) have no fill when no 911 call exists —
  consider a light trim, or narration over them.
- **Caption font** ~30px reads small — bump ~40 in `render_rough_cut.make_caption_png`.
- **NARRATION ACCURACY (defamation-critical)**: narration MUST be grounded on the documented OUTCOME
  (doc_extract → tier1_verdict), not the transcript alone — gunfire isn't in the words, so transcript-only
  narration once said a man who was shot "complied." `rawwalk.narrate(facts)` + a guard handle this; keep it.
- **AXON OCR robustness**: some bodycams' clocks don't OCR (la jolla BWC_1 → wrong export-date metadata);
  only trust `axon_ocr`-stamped cams for chronology.

## IS THIS /loop-READY?  (operator's question — honest answer)
- **Tier-1 SELECTION + Tier-2 FIRST-CUT generation: ~loop-ready.** The recipe is deterministic and
  produces a watchable first cut autonomously. A /loop could batch the registry → triage → emit first
  raw-walk cuts for review. Safe, repeatable, clear outputs.
- **EDITORIAL FINISHING: NOT loop-ready.** Every cut so far needed human notes (chronology, teaser,
  narration timing, run lengths). The template auto-builds a DRAFT; a human still finishes it.
- **THIS task (EWU-shape longform + new shape-aware templates): NOT loop-ready yet** — it's a
  build/design task (the longform/flagship-for-EWU template + the shape-router don't exist yet), which
  needs iterative human judgment, not a fixed loop.
- **Bottom line**: loop the *pipeline-to-first-draft* (triage → first cut → queue for review). Do the
  *template-building + finishing* interactively first; once the shape-router + longform template are
  proven on a few cases, more of it becomes loop-able. Recommend: build it interactively now, loop later.
