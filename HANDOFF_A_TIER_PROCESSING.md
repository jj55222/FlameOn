# HANDOFF — top-10 A-tier cases: cuts + a P4-vs-LLM assessment test

**Two goals:**
1. **Pick the TOP 10 most-interesting A-tier cases** (don't process all 28), download
   them, and run them through the documentary engine → grounded cuts.
2. **In tandem, validate P4's CASE-ASSESSMENT** against a strong LLM: does
   `pipeline4_score` (PRODUCE/HOLD/SKIP) agree with a strong model's independent
   verdict on the same 10 cases? A concordance check on the SELECTOR.

**Critical role split (don't conflate — see memory):**
- **`pipeline4_score` (P4) = assess whether a case is GOOD** (PRODUCE/HOLD/SKIP — selection).
- **`beat_miner` = generate the CONTENT/moments** around a case you've chosen to make (the cut).
The experiment in goal #2 tests **P4 (assessment)**, NOT beat_miner. The cut-making
in goal #1 uses **beat_miner** (the moment source). They are different tools for
different jobs.

A-tier = 28 SDPD cases (gold: video+audio+docs, every file verified live). All in
`discovered_cases/CASE_BUNDLE_AGG.json` (`tier=="A"`, `source=="sdpd"`).

Read first: `MEMORY.md` (auto-memory), `HANDOFF.md` (the documentary engine),
`discovered_cases/CASE_BUNDLE_SPEC.md` (tiers). This doc is the runbook.

---

## Step 0 — pick the TOP 10 most-interesting cases (before downloading)
The 28 A-tier cases total ~138 GB (0.3–24 GB each) — **don't download all of them.**
First rank them on a CHEAP pre-download signal and pick 10:
- **Incident type + outcome** — the `case_url` carries a `cat=` (e.g. *Sustained
  Findings* = misconduct upheld → strong accountability story; *Officer-Involved
  Shooting* = dramatic). Fetch the case page (browser UA) for a one-line summary.
- **Media richness** — more cameras/footage (`n_video`, `n_audio`) = more to cut with.
- Have the strong LLM read the 28 titles/categories (+ optional case-page summaries)
  and rank by **documentary potential**; take the top 10. This is a heuristic
  pre-filter — actual compellingness only shows after processing.

```bash
# the 28 A cases (case_id, V, A, D, files, category from case_url):
.venv/bin/python -c "import json,urllib.parse as u;d=json.load(open('discovered_cases/CASE_BUNDLE_AGG.json'))['bundles'];A=[b for b in d if b.get('tier')=='A'];[print(b['case_id'],b['n_video'],b['n_audio'],b['n_docs'], u.parse_qs(u.urlparse(b['case_url']).query).get('cat',['?'])[0]) for b in sorted(A,key=lambda b:b['case_id'])]"
```
Agency string for all: **"San Diego Police Department"**. Process the 10 **one at a
time** (download → cut → optionally delete the media → next) to respect disk.

## ⚠️ Read before you spend anything
**SDPD has NEVER been run through the pipeline.** Everything proven so far ran on
Sac County Sheriff (Morales, local) and SF DPA (`sfdpa_0409_18`). So the FIRST SDPD
case is a generalization probe — three things can differ and you must check them
BEFORE the paid steps:
1. **Chronology** — does SDPD BWC carry a readable on-screen clock (`timeline_stamp`
   → `axon_ocr`)? If not, the auto-anchor leans on salience+convergence only.
2. **Muted audio** — SF DPA's BWC was redaction-MUTED (mean −54 dB → ~0 transcript →
   thin cut). CHECK each case's BWC loudness; if muted, it's an analysis-style cut
   (narration over footage), not a transcript-driven one.
3. **Doc format** — SDPD's IA report PDF may differ from the Sac format `doc_extract`
   was tuned on. The doc-OCR now routes Vision (fast) on scanned PDFs; verify it
   extracts subject/findings.

## Always, before any command
```bash
cd /Users/jmoney/FlameOn-main
eval "$(/opt/homebrew/bin/brew shellenv)"      # ffmpeg/ffprobe on PATH (brew)
set -a; . ./.env; set +a                       # OPENROUTER_API_KEY for paid steps
```

---

## Step 1 — validate ONE small case STAGED (cheap, ~0.3–0.5 GB)
Recommended first case: **`sdpd_08_20_2023_ia_2023_009`** (0.3 GB, V1 A3 D2) or
**`sdpd_02_09_2023_ia_2023_0027`** (0.5 GB, V2 A2 D2).

```bash
CID=sdpd_08_20_2023_ia_2023_009 ; B=.tmp/$CID
# download bundle into the basket layout (handles SDPD's browser-UA requirement)
.venv/bin/python discovered_cases/bundle_to_basket.py --case-id $CID --basket $B
# stamp — DOES CHRONOLOGY SURVIVE? (look for axon_ocr vs none/filename)
.venv/bin/python pipeline3_audio/timeline_stamp.py --basket $B/video --out $B/timeline
# is the BWC audio usable, or muted like SF DPA? (>-40 dB mean = ok)
for f in $B/video/Video/*.mp4; do ffmpeg -hide_banner -i "$f" -af volumedetect -f null - 2>&1 | grep mean_volume; done
```
If chronology holds and audio isn't muted → continue. If muted → expect an
analysis-style cut (still fine; the narration carries it).

## Step 2 — run the flagship chain (one command)
`make_documentary --flagship` already chains everything and **defaults to
`--moments beatminer`** (recall — the RIGHT source for assembly; do NOT use
`--moments p4`, that's the case-selector and gives thin cuts):
```bash
DOC=$(ls $B/docs/*.pdf | head -1)
.venv/bin/python pipeline6_sequence/make_documentary.py \
  --basket $B --case-id $CID --agency "San Diego Police Department" \
  --doc "$DOC" --flagship --run        # add --judge-mock to keep the gate free
```
Chain: stamp → build-timeline → doc-ocr(Vision) → doc-extract → transcribe(incident) →
**mine-moments (beat_miner, PAID)** → bridge-verdict → blueprint(`--auto-anchor`) →
**shape (PAID)** → render-blueprint(auto-fit) → judge(gate).
Output: `$B/d6_cuts/$CID/${CID}_rough_cut.mp4` + a SHIP/REVISE/REWORK gate.
Paid steps are fenced on `OPENROUTER_API_KEY`; the gate is free with `--judge-mock`.

## Step 3 — process the other 9 of your top-10 (smallest-first, one at a time)
Once the first case validates, loop the remaining 9 you picked in Step 0. Process
**one at a time** (download + cut + free the media) to respect disk:
```bash
for CID in $(cat .tmp/top10_case_ids.txt); do   # the 10 you ranked in Step 0
  B=.tmp/$CID
  .venv/bin/python discovered_cases/bundle_to_basket.py --case-id $CID --basket $B
  DOC=$(ls $B/docs/*.pdf 2>/dev/null | head -1)
  .venv/bin/python pipeline6_sequence/make_documentary.py --basket $B --case-id $CID \
    --agency "San Diego Police Department" ${DOC:+--doc "$DOC"} --flagship --run
  # then run the Goal-#2 assessment block below on $B, and
  # optional: rm -rf $B/video to reclaim disk after the cut renders
done
```
Go smallest-first so you find SDPD-specific issues on a cheap case.

---

## Goal #2 — the P4-vs-LLM assessment test (runs alongside the cuts)
Tests the **SELECTOR (P4)** — does its PRODUCE/HOLD/SKIP verdict agree with a strong
LLM's? — NOT beat_miner. For each of the 10 cases, after the transcripts exist:

```bash
# A) P4's verdict — the structured 2-pass scorer (winner-weighted, precision-calibrated)
.venv/bin/python pipeline4_scoring/pipeline4_score.py --force \
  --transcript-dir $B/d2/transcripts --case-id $CID \
  --weights pipeline1_winners/scoring_weights.json --output $B/d2/verdicts_p4
# -> PRODUCE/HOLD/SKIP + narrative_score in $B/d2/verdicts_p4/${CID}_verdict.json

# B) a STRONG LLM's INDEPENDENT verdict — same rubric, holistic. Write a small
#    prompt (reuse pipeline4_scoring/llm_backends.build_backend(<STRONG_MODEL>)):
#    give it the transcript (+ doc summary), ask for PRODUCE/HOLD/SKIP + score 0-100
#    + one-line why, using P4's own criteria (narrative tension, contradictions,
#    emotional peaks, accountability arc). Keep its reasoning grounded in the material.

# C) compare across all 10: an agreement table (P4 verdict vs LLM verdict, score gap).
```

**Read the result honestly:**
- **It's a structured pipeline vs a raw strong LLM.** P4 isn't just an LLM: it's a
  **deterministic winner-weighted `narrative_score`** (the real spine) + a Gemini-Flash
  *extraction* pass + a Qwen *modulation* pass, reconciled so the LLM can't swing
  SKIP↔PRODUCE (see `P4_TRUST_DETERMINISTIC_PRODUCE`). So you're testing whether that
  *structure + calibration* tracks a strong model's holistic judgment — not "model vs
  non-model." Agreement ≠ ground truth; the useful signal is **where they DISAGREE.**
- **All 10 are A-tier GOLD cases**, so a good documentary selector should rate most
  PRODUCE. If **P4 says HOLD/SKIP on cases the LLM finds compelling** (it rated Morales
  HOLD), that's evidence P4 is calibrated for *case-DISCOVERY triage* (find the rare gem
  among thousands, <30% PRODUCE) — **the wrong calibration for ranking among
  already-good cases.** That mismatch is the finding worth chasing → may need a
  documentary-selection recalibration of P4 (or a separate "rank good cases" mode).
- Do NOT use beat_miner here — that's the content tool; this validates the selector.

## Cost / scale
- **Cut (goal #1):** 2 paid LLM calls/case (beat_miner + shape; deepseek-flash, cheap).
- **Assessment (goal #2):** ~2 more paid calls/case (P4's 2-pass + the strong-LLM verdict).
- So **10 cases ≈ ~40 paid calls total** (~20 cut + ~20 assessment), all cheap models,
  + the top-10's download (pick small ones first) + hours of local transcribe/render.
  `--judge-mock` keeps the gate free.
- Cheapest path to coverage: smallest-first, one at a time, free the media after each.

## Known-good facts (don't re-derive)
- Pipeline runs **tools-only** end-to-end (validated on Morales): stamp + timeline +
  Vision-OCR doc + beat_miner + bridge + blueprint(auto-anchor) + shape + render + judge.
- **beat_miner (recall) is the moment source**, wired as the flagship default; it
  natively tags each moment's camera now (`artifact_id`).
- **Vision OCR** is the doc-OCR fast path (router: text-layer vs scanned), ~0.8 s/page
  vs easyocr's 5–20 s — essential for big scanned IA PDFs.
- **Auto-anchor** picks the incident by salience×camera-convergence + the doc's clip
  timestamps; works even on thin moment sets.
- The **fact-check rail** + integrity ledger keep narration grounded (defamation-safe);
  `--analysis --grammar` can style it (EWU/Dr.Insanity) — optional for these.

## Tools & docs
- `discovered_cases/bundle_to_basket.py` — registry bundle → pipeline basket.
- `pipeline6_sequence/make_documentary.py --flagship` — the one-command chain.
- `pipeline4_scoring/beat_miner.py`, `pipeline6_sequence/bridge_verdict.py` — moments→verdict.
- `pipeline3_audio/doc_ocr.py` (Vision router), `doc_extract.py` — the doc spine.
- `pipeline6_sequence/{blueprint,blueprint_shape,render_blueprint,judge}.py` — assembly.
- `discovered_cases/validate_ab.py` + `validate_ab_report.md` — the A/B validation (all live).
- Memory: `salience-eval-harness.md`, `p6-incident-anchor-and-grounding.md`, `transparency-portals.md`.
