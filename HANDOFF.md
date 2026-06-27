# HANDOFF — FlameOn (2026-06-26/27, macOS M4 Pro)

Read this first. It supersedes the dated `HANDOFF_*` files and `CODEX_HANDOFF.md` (those are Windows/P2-era).
Cross-refs: `MIGRATION.md` (macOS quickstart), auto-memory `MEMORY.md`, `schemas/contracts.json`.

---

## ⏭️ IMMEDIATE NEXT TASK — fix the documentary's flat act structure

The Pipeline-6 documentary engine now RUNS on macOS and produces a real `.mp4`, and the **footage is
correct** (the OD-response cams dominate). But the narrative **acts are flat**:
`{hook:0, escalate:5, aftermath:21, accountability:0}` — everything piles into "aftermath".

**Root cause:** `timeline_build` anchored "the incident" on the earlier **drug seizure (17:42, BWC-4/1)**,
so it treats the **overdose (20:45, BWC-3a/5/3c)** as *aftermath*. The story is backwards.

**The fix:** anchor the incident on the OVERDOSE (~20:45) so phases bucket right:
`pre_incident=seizure → cold open`, `incident=OD discovery → hook/escalate`, `aftermath=transport/hospital`,
`investigation(23:09)+IA finding → accountability`. Check `timeline_build.py` for an incident-time arg
(like `--incident "YYYY-MM-DD HH:MM"`, as `zip_triage` has); if absent, the phase heuristic needs the
anchor passed in. Then rebuild: blueprint → blueprint_shape → render_blueprint. NOTE: even with correct
phases, `blueprint_shape` may not redistribute beats across acts (it wrote theses but left beats in their
skeleton buckets in two runs) — verify the shaper actually reassigns `beat_ids` to hook/accountability,
or that's a second fix.

Reproduce the current (footage-correct, structure-flat) cut:
```bash
eval "$(/opt/homebrew/bin/brew shellenv)"; set -a; . ./.env; set +a   # ffmpeg on PATH + OPENROUTER key
# (scaffolding already built in .tmp/2023psb0530/; to rebuild from scratch see "Documentary chain" below)
.venv/bin/python -X utf8 pipeline6_sequence/blueprint.py --artifacts .tmp/2023psb0530/timeline/artifacts.json \
  --timeline .tmp/2023psb0530/timeline/case_timeline.json --verdict .tmp/2023psb0530/verdict_creatorkey.json \
  --doc-extract .tmp/2023psb0530/docs/doc_extract.json --media-dir ~/Downloads/2023PSB-0530 \
  --target-runtime 300 --out .tmp/2023psb0530/blueprint
.venv/bin/python -X utf8 pipeline6_sequence/blueprint_shape.py --blueprint .tmp/2023psb0530/blueprint/sac_so_2023psb-0530_blueprint.json --model deepseek/deepseek-v4-flash --out .tmp/2023psb0530/blueprint
.venv/bin/python -X utf8 pipeline6_sequence/render_blueprint.py --blueprint .tmp/2023psb0530/blueprint/sac_so_2023psb-0530_blueprint_shaped.json --media-dir ~/Downloads/2023PSB-0530 --transcripts .tmp/2023psb0530/transcripts --out .tmp/2023psb0530/long_cuts
```

---

## Environment (M4 Pro, 24 GB)
- Python 3.12 via Homebrew; venv `.venv`. `requirements.txt` + torch (MPS). 76 P3 tests pass.
- **GOTCHA: ffmpeg/ffprobe come from brew (`/opt/homebrew/bin`), resolved only via PATH.** Always
  `eval "$(/opt/homebrew/bin/brew shellenv)"` before any ffmpeg-touching tool (finders, renderers, whisper,
  yt-dlp). `imageio-ffmpeg` lacks ffprobe; `static_ffmpeg` not installed.
- **Local AI stack (fast, private, no keys):** `mlx-whisper` (large-v3 on GPU ~10× realtime),
  `ocrmac`+PyMuPDF (Apple Vision OCR on the Neural Engine ~1.8 pg/s), `macmon`. RAM tight (~23/24 GB) when
  GPU+ANE both busy.
- `.env` has a LIVE `OPENROUTER_API_KEY` (**ROTATE** — pasted in chat) + MuckRock creds (user added).
  Reaches DeepSeek/MiniMax/MiMo/GLM/Gemini/owl-alpha etc.

## The salience eval harness — `pipeline4_scoring/` (WORKS, validated)
- **`evaluate_salience.py`** — IMMUTABLE moment scorer. Salience-weighted recall + **grounding-as-precision**
  (a quote is "real" iff it resolves in the source) + temporal/quote match. Discoveries (grounded-but-novel)
  vs hallucinations (ungrounded). `--selftest` passes.
- **Two-pass doc pipeline:** `doc_pass1.py` (extract beats from a full OCR'd report, swappable `--model`) →
  `pass1_union.py` (union N extractors' grounded candidates) → `pass2_judge.py` (reasoning model rates/prunes;
  `--panel A B` = consensus). `beat_miner.py` (audio-transcript beats, cue/LLM). `score_run.py` (mixed-key
  scorer: audio=temporal, doc=quote-match). `blind_harness.py` (extract from raw transcripts → score vs key).
- **`llm_backends.py`** — OpenRouter facade; now CAPTURES reasoning traces (`backend.last_reasoning` /
  `extract_think`). Surfaced in `pass2_judge` output. Use traces to refine prompts (next loop).
- **Model bake-off (254K-tok doc):** viable (1M ctx) = `deepseek-v4-flash` (fast/cheap, best timeline),
  `mimo-v2.5` (typed breadth), `gemini-3.5-flash` (best contradictions, pricey), `owl-alpha` (FREE), `glm-5.2`.
  DNF = `nemotron-3`, `hy3-preview` (advertised 1M but provider-served 262K + null responses). Pass-2 judges:
  `deepseek-v4-pro` (tighter) vs `minimax-m3` (more inclusive, 2× slower). **Pass-1 flat-rates salience — JUDGE in Pass-2.**
- **OPEN scorer refinement (flagged repeatedly):** quote-match is exact-token, too strict cross-model →
  under-counts recall (e.g. blind test missed 2 must-finds that were found via adjacent lines). Add
  SEMANTIC/page matching. This would make `score_run` report true recall.

## Two FROZEN golden keys
1. **Dutchess Way (Sac SO 20-269838)** — `golden/sac_so_20-269838_dutchess_way.golden.json` (reviewed,
   41 moments, 22 must-find) + `.reviewed-v1.json`. Self-judged (LLM panel + human). NOT bodycam (in-car/aerial).
   Evidence `~/Downloads/Media.zip` (68 GB). First scored run is partly circular (key from the panel).
2. **2023PSB-0530 (BWC fentanyl-OD, Dep. Marvin Morales)** — `golden/sac_so_2023psb-0530.golden.json`
   (reviewed, 24 moments, 8 must-find) + snapshot. **CREATOR-CONSENSUS validated** (held-out): built from what
   2 YouTube channels aired (EWU 9.5M views/9.5min + PoliceTransparency 0.94M/51min) + audience comment-peak
   timestamps, grounded to raw BWC, human keep/cut. **BLIND TEST PASSED:** `blind_harness.py` (deepseek-v4-flash,
   never saw creators) recall 0.67 / must-find 0.75 measured — but the 2 "missed" must-finds were found via
   adjacent lines → effective must-find ≈ 8/8. The harness blind-rediscovers audience-validated salience.

## 2023PSB-0530 — the ACTIVE case (BWC, manageable, ~10 GB)
- Local `~/Downloads/2023PSB-0530.zip` (FLAT): **7 BWC** (BWC-1..5) + 2 exacqVision station cams + 2 PDFs.
  Real AXON bodycam (timeline_stamp clock OCR WORKS here, unlike Dutchess's non-AXON cams).
- Processed in `.tmp/2023psb0530/`: `transcripts/` (9 mlx-whisper, **patched with `source_url`** — see gotcha),
  `creator_transcripts/` (EWU + PoliceTransparency; PoliceActivity FAILED — yt-dlp YouTube bot-check/DRM,
  cookies declined), `docs/` (Vision OCR: 468-pg IA report + photos; `doc_extract.json`), `comments/` (3 videos'
  top comments → parsed peaks), `basket/video/` (symlinks for timeline tools), `timeline/`, `blueprint/`, cuts.
- Case = a deputy who OD'd on **seized fentanyl** he'd taken at the station 3 hrs after the seizure; IA found
  "inexcusable neglect / discredit to the agency." Narrative irony: the OD victim IS a deputy.

## Documentary engine — Pipeline 6 (RUNS on macOS)
Two paths in `pipeline6_sequence/`: simple `render_rough_cut.py` (verdict → text-card cut) and the FLAGSHIP
`blueprint.py` → `blueprint_shape.py` (LLM authors acts/theses/narration + inserts; VALIDATOR drops anything
invented — faithfulness spine) → `render_blueprint.py` → `judge.py` (craft+coverage eval). `make_documentary.py`
orchestrates the native chain. **Both render paths verified on macOS** (ffmpeg + Pillow text cards).
- **Documentary chain for 2023PSB-0530** (all run, outputs in `.tmp/2023psb0530/`): `timeline_stamp` (AXON
  clock → real chronology: seizure 17:42 → OD 20:45 → investigation 23:09) → `timeline_build` (phases) →
  `doc_extract` (needs pages as a BARE LIST — reshape our OCR's `{pages:[...]}`) → **`bridge_verdict.py`** (NEW;
  our moments → P4 verdict shape) → blueprint → shape → render. Target runtime is a `--target-runtime` param
  (short now, scale to flagship 20-40 min later).
- User intent: EWU-FLAGSHIP style (authored narration + clips + photos + doc excerpts → arc), NOT EWU-Bodycam
  replay. **Text-only narration** for now (TTS future). Faithfulness (integrity_ledger) is non-negotiable
  (real person, real IA case = defamation risk).

## Bugs FIXED this session (don't re-hit)
- **`source_url` resolver bug (critical):** `render_rough_cut.map_sources` resolves a transcript_ref→media via
  the transcript's `source_url` field; on miss it falls back to matching by evidence-type+person, which collapses
  ALL same-type-no-person refs (our `BWC-*`) onto the FIRST file. FIX: every transcript needs `source_url` =
  absolute media path (we patched `.tmp/2023psb0530/transcripts/`). Native P4 transcripts have this; ours didn't.
- **AAC-in-mp3 clips:** extracting clips from the BWC (AAC audio) with `ffmpeg -c copy ...mp3` makes invalid
  files (silent players). Must RE-ENCODE: `-vn -c:a libmp3lame`. (Dutchess sources were already .mp3, so copy worked there.) Fixed in `build_creator_review.py`.
- **doc_extract input:** wants a bare `[{page,text}]` list; our OCR writes `{...,"pages":[...]}` → reshape first.

## Other open threads
- **Source comparison (user wanted):** render from harness output vs native chain vs creator key, compare cuts.
- **Narration polish:** brief-grade text; foreground the deputy-irony (it framed Morales as "an unconscious man").
- **Port harness to P2 (FOIA case-finding):** grounded-judge-panel + reasoning-traces. grounding-as-precision ==
  P2's actionability ("a case is real iff its source URLs resolve to footage"). P2 already has the immutable-scorer
  loop (`autoresearch/`, `calibration_data.json`, score 63.73 → target 75-80). Strong fit.
- **Generalization:** label golden keys across genres (Dutchess=tragedy, Investment Circle 20-285120=contested
  fake-gun, 2023PSB-0530=accountability) — held-out set to avoid overfitting (the Goodhart risk from the lit review).
- **Research done:** deep-research validated the architecture (autoresearch frozen-scorer, reward-overoptimization
  /Goodhart, judge-juries PoLL, FActScore/RAGAS/AIS grounding, QVHighlights/Moment-DETR, OpenBWC domain prior art).

## New files this session (key ones)
`pipeline4_scoring/`: evaluate_salience.py, doc_pass1.py, pass1_union.py, pass2_judge.py, score_run.py,
beat_miner.py, blind_harness.py, build_creator_key.py, build_creator_review.py, build_review.py,
build_audio_review.py, golden_review.py, merge_golden.py, bridge_verdict.py (in pipeline6_sequence/).
Golden keys in `pipeline4_scoring/golden/`. Case work in `.tmp/2023psb0530/` and `.tmp/dutchess_269838/`.
