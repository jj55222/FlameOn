# WEEK PLAN — 2026-07-01 → 07-05 · executor runbook (Opus / Codex)

**Keystone EOW goals:** ① one last public-source sweep → new case bundles in the aggregator
(**WS1**), ② an EWU cut rendered **through Remotion** from an evidence-complete case (**WS3 → WS4**).
Supporting: ③ P0 FOIA finder running autonomously (**WS2**). **TTS is explicitly deprioritized this
week** (operator call, 2026-07-01) — do not spend time on it.

---

## Executor protocol (read first)

1. Entry context: `CLAUDE.md` → `STATE.md` → this plan → the one workstream section you own.
   Load the matching skill (`.claude/skills/…`) only when producing. Don't read every handoff.
2. **One workstream per session/branch** (`ws1-source-sweep`, `ws2-p0-autonomy`, `ws3-ewu-selector`,
   `ws4-remotion`). Auto-commit is on; tag milestones (`git tag ws1-done`).
3. **The goal script is the definition of done.** Each WS specifies `goals/wsN_*.py` — a
   deterministic checker (exit 0 = done, 1 = not yet, prints WHY). Build it FIRST if it doesn't
   exist, then iterate the work until it exits 0. This is the house AutoResearch pattern
   (immutable checker + iterating sandbox).
4. **Loop where marked loop-safe**: in an interactive session use `/loop <interval> <prompt>`;
   headless use the launchd/cron lines given inline. Never loop editorial-finishing work.
5. Sacred files (never edit): `pipeline4_scoring/evaluate.py`, `pipeline2_discovery/calibration_data.json`,
   `pipeline4_scoring/golden/*.golden.json`. `CASE_BUNDLE_AGG.json` is **generated** — never hand-edit;
   add sources as `discovered_cases/<portal>_candidates.json` and re-run `case_bundle_agg.py`.
6. Env for anything touching media: `eval "$(/opt/homebrew/bin/brew shellenv)"` + `source .venv/bin/activate`;
   keys: `set -a; . ./.env; set +a` (values carry trailing `# comments` — never `grep|cut` them).

---

## WS1 — Final public-source sweep → aggregator (KEY EOW)

**Objective.** Add the remaining worthwhile released-records portals to
`discovered_cases/CASE_BUNDLE_AGG.json`. Registry today: 2,769 bundles / ~449 media bundles from 5
sources (COPA 2128, Long Beach 443, SFDPA 95, MuckRock 53, SDPD 50).

**Done (goal script `goals/ws1_registry_check.py`)** — exit 0 when, vs. `goals/ws1_baseline.json`:
- ≥ 2 NEW `source` values present in the AGG, and
- ≥ 40 new bundles with `n_video > 0` (media bundles, not doc-only), and
- `case_bundle_agg.py --selftest` passes, and
- new A/B-tier entries validate ≥ 95% live via `discovered_cases/validate_ab.py`
  (**probe NextRequest-style portals GENTLY** — low concurrency, 429 backoff; 6 workers → false 429s).

**Steps.**
1. Recover the ranked 24-agency portal table from the prior session (search `discovered_cases/`,
   `.tmp/`, `*.md` for it); if unrecoverable, rebuild quickly: for each large agency with a
   released-cases page, score = (bundled video+docs per case?) × (open directory / stable URLs?) ×
   (volume). Known-good archetypes: Sacramento County SO released-cases page; agency YouTube/Vimeo
   critical-incident channels (LAPD, Austin PD, Phoenix PD, Seattle PD…); NextRequest/Laserfiche
   portals of big cities not yet covered.
2. Top 2–4 targets → clone the house harvester pattern (`sdpd_harvest.py` / `copa_harvest.py` /
   `longbeach_harvest.py` — resumable, incremental save, `_checked` skip flags) →
   `<portal>_candidates.json` with the registry schema
   (`source,case_id,agency,title,case_url,n_video,n_audio,n_docs,n_photos,n_files,tier,downloadable,score`).
3. `python discovered_cases/case_bundle_agg.py` → regenerate AGG (+ `.md` index) → run validate_ab
   on new A/B rows → run the goal script.

**Loop-safe:** yes for the harvest/validate iteration (`/loop 30m` on "advance WS1 until
goals/ws1_registry_check.py exits 0, then stop"). Est: 1–2 days.

**Kickoff prompt (verbatim):**
> Read CLAUDE.md, STATE.md, then docs/plans/WEEK_2026-07-01_goals.md §WS1. Work on branch
> ws1-source-sweep. First build goals/ws1_registry_check.py per spec (baseline is committed at
> goals/ws1_baseline.json). Then find and harvest 2–4 new released-records portals into
> discovered_cases/<portal>_candidates.json using the existing harvester pattern, regenerate
> CASE_BUNDLE_AGG via case_bundle_agg.py, validate new A/B rows gently, and iterate until the goal
> script exits 0. Never hand-edit CASE_BUNDLE_AGG.json. Report new-source counts when done.

---

## WS2 — P0 FOIA finder → autonomous daily ingest, weekly review

**Objective.** `pipeline0_sourcing/` (built 2026-07-01, offline-tested) runs on a schedule and
surfaces genuinely interesting cases. **Cadence decision: DAILY ingest, WEEKLY human review.**
Rationale: news links decay and BWC retention clocks run (daily capture is ~free — keyless ingest +
cents of LLM), while FOIA responses take weeks (weekly filing batch is plenty).

**Done (goal script `goals/ws2_p0_health.py`)** — exit 0 when:
- a LIVE run has succeeded (real network ingest + LLM extract — today only `--mock` is proven):
  `.tmp/p0/foia_queue.md` exists, non-empty, < 26 h old, and `seen.json` grew across ≥ 2 runs;
- the launchd job is loaded (`launchctl list | grep flameon.p0`);
- a spot-check of the top-5 queue rows shows sane state/agency extraction (write the check as
  assertions on required fields, not vibes).

**Steps.**
1. One live run: `cd pipeline0_sourcing && python sourcing_run.py --since 3 --tier1-only
   --seen .tmp/p0/seen.json --out .tmp/p0`. Fix what breaks (Google News RSS shape, GDELT quirks).
   Tune `DEFAULT_TERMS` / `score.py` thresholds against what comes back — target: top-10 queue rows
   an operator would agree are worth filing (severity-led; contradiction multiplier).
2. Wrap as `pipeline0_sourcing/run_daily.sh` (brew shellenv + venv + the command above + `|| true`
   logging to `.tmp/p0/run.log`). Pure Python — **no Claude needed in the loop**.
3. Install launchd agent `~/Library/LaunchAgents/com.flameon.p0.plist` (RunCalendarInterval 07:30
   daily → run_daily.sh; launchd survives sleep/wake better than cron on macOS). `launchctl load`.
4. Weekly review ritual (operator, ~15 min, Fri): `open .tmp/p0/foia_queue.md` → verify state rows
   (`verified:false` flags) → fill brackets → submit manually. Optionally a Fri `/loop` session
   that re-ranks the week's queue and drafts a summary.

**Loop-safe:** the daily job is fully autonomous (launchd, no LLM-agent). Tuning pass is a normal
session. Est: 0.5–1 day.

**Kickoff prompt (verbatim):**
> Read CLAUDE.md, STATE.md, docs/plans/WEEK_2026-07-01_goals.md §WS2, and pipeline0_sourcing/CLAUDE.md.
> Branch ws2-p0-autonomy. Do a LIVE sourcing_run.py run (network + LLM; keys auto-load from .env),
> fix any live-path breakage, tune DEFAULT_TERMS and score thresholds until the top-10 queue rows are
> credibly file-worthy, then build run_daily.sh + the com.flameon.p0 launchd agent per spec and
> goals/ws2_p0_health.py, iterating until it exits 0. P0 drafts only — it must never submit requests.

---

## WS3 — Evidence-completeness selector: registry → "EWU-ready" shortlist (KEY EOW, gates WS4 content)

**Objective.** What the operator calls the "P5 selector": a selection pass that finds cases with
**enough evidence to produce** — BWC **and** case doc **and** ideally interrogation/interview +
911 — i.e. the EWU full-package. (Naming note: P5 in this repo is the brief assembler; this
workstream extends the **Tier-1 selector + registry**, not `pipeline5_assembly/`. Do NOT use P4's
PRODUCE gate — mis-calibrated for exactly these cases, see STATE.md.)

**Done (goal script `goals/ws3_shortlist_check.py`)** — exit 0 when
`discovered_cases/ewu_shortlist.json` (+ `.md`) exists with ≥ 10 ranked cases where each row has:
- `evidence`: classified artifact counts {bwc, interrogation/interview, 911/dispatch, doc, photos}
  derived from registry file lists (reuse `muckrock_harvest.py`'s artifact-bundle classifier — it
  already types BWC/interrogation/SB1421 by filename);
- `completeness_score` (full-package bonus when BWC+doc+interrogation co-occur) and
  `worth` (Tier-1 severity × story-shape × human-proximity model, `.tmp/_tier1_rubric.md`);
- every shortlisted case is `downloadable` + A/B tier;
- and the TOP pick has been pulled to a basket (`bundle_to_basket.py`) with doc+911 fetched (cheap
  tier-1 inputs only) and a `d2/tier1_verdict.json` PRODUCE.

**Steps.** (1) build the classifier-over-registry (`discovered_cases/ewu_shortlist.py` — new file,
reads AGG incl. WS1's new sources); (2) rank; (3) tier-1-verify the top 3 cheaply (doc+911 only);
(4) hand the #1 case to WS4. Loop-safe for steps 1–3. Est: 1 day. Registry gives you 2,769 rows —
the interesting question is classification quality, so eyeball 10 random classifications.

**Kickoff prompt (verbatim):**
> Read CLAUDE.md, STATE.md, docs/plans/WEEK_2026-07-01_goals.md §WS3, and the case-selection skill.
> Branch ws3-ewu-selector. Build discovered_cases/ewu_shortlist.py: classify every CASE_BUNDLE_AGG
> bundle's files into {bwc, interrogation, 911, doc, photos} (reuse muckrock_harvest's classifier),
> score completeness × Tier-1 worth, emit ewu_shortlist.json/.md. Build goals/ws3_shortlist_check.py
> per spec and iterate to exit 0, including cheap tier-1 verification (doc+911 only, no interview
> transcription) of the top 3. Do not use pipeline4_score's PRODUCE gate for this.

---

## WS4 — Remotion mograph templates + render lane (KEY EOW, with WS3)

**Objective.** Promote the `.tmp/remotion_test/` seed (working: Node 26, package.json, props.json,
rendered `out/`) into a first-class render lane: `pipeline6_remotion/` — a Remotion project whose
compositions consume **blueprint JSON as props**, replacing the PIL-PNG overlay look with real
mograph.

**Architecture decision (encode it, don't relitigate):** Remotion is the **overlay/assembly layer**;
ffmpeg (`render_blueprint.py`) keeps doing heavy clip extraction. Feed Remotion the existing
`*_blueprint_shaped.json` + `paper_edit.json` + the pre-cut `segments/*.mp4` (render_blueprint
already emits them) via `<OffthreadVideo>`. This keeps renders fast, keeps the fact-check rail
upstream untouched, and — critically — the old renderer remains a working fallback so the EOW cut
is never blocked on Remotion.

**Core template set (build in this order):**
1. `NarrationBand` — top-third narration as a TIMED overlay (in ~6–8 s, then clears; subscribes to
   beat timing from props) — this natively fixes the #1 operator note (lingering narration).
2. `CaptionTrack` — burned captions from transcript captions in the blueprint (word-timed).
3. `TitleCard` / `CaseCard` — cold-open + act titles (case id, agency, date).
4. `DocumentCallout` — the `is_document` record-card beats: doc crop pan + highlighted quote.
5. `OutcomeCard` — findings/disposition end card.
6. `LowerThird` (speaker/context) + a subtle timestamp/location chip.

**Done (goal script `goals/ws4_remotion_smoke.py`)** — exit 0 when:
- `pipeline6_remotion/` renders headless: `npx remotion render <comp> --props=<fixture>.json` →
  mp4 exists, duration within ±1 s of the fixture paper-edit total;
- a `blueprint_to_props.py` converter turns a real shaped blueprint into props losslessly
  (every beat/caption/doc-card represented; unknown beat kinds fail loudly, not silently);
- **the WS3 #1 case (or Morales/fanuel as fallback) renders end-to-end through the Remotion lane**
  — the EOW keystone artifact — and `open`s on completion.

**Steps.** (1) `git mv`-style promote remotion_test → `pipeline6_remotion/` (fresh `npm i`,
pin remotion version; note Remotion's company-license terms — fine for individual/small, verify);
(2) props schema + converter; (3) templates 1–3; (4) smoke goal script; (5) templates 4–6;
(6) the keystone render. Loop-safe: 2–5 against the smoke script. Est: 2 days. Good Codex fit
(TS/React), with the converter (Python) fine for either executor.

**Kickoff prompt (verbatim):**
> Read CLAUDE.md, STATE.md, docs/plans/WEEK_2026-07-01_goals.md §WS4, and pipeline6_sequence/CLAUDE.md
> (render/blueprint contracts). Branch ws4-remotion. Promote .tmp/remotion_test into
> pipeline6_remotion/ and build the template set + blueprint_to_props.py per spec; Remotion consumes
> shaped-blueprint props + pre-cut segments (ffmpeg lane stays the fallback). Build
> goals/ws4_remotion_smoke.py first and iterate to exit 0; finish by rendering the WS3 top case
> through the lane and opening the mp4. TTS is out of scope this week.

---

## Sequencing

| Day | Track A (python/Opus) | Track B (TS/Codex-or-Opus) |
|-----|----------------------|---------------------------|
| Tue | WS2 live-run + launchd (short) → start WS1 harvesters | WS4 steps 1–2 (promote + props schema) |
| Wed | WS1 loop to green | WS4 templates 1–3 + smoke green |
| Thu | WS3 classifier + shortlist | WS4 templates 4–6 |
| Fri | WS3 tier-1 verify top-3 → hand #1 to WS4 · P0 weekly review | **Keystone: WS3 case through Remotion lane** |

Dependencies: WS1 → enriches WS3's input (but WS3 can start on today's 2,769 rows). WS3 → gives WS4
its content. WS2 independent. Parallel-safe: WS1/2/3 vs WS4 touch disjoint files.

## Risks / decisions on record
- **Remotion is additive, not a rewrite** — ffmpeg lane stays the fallback; if Remotion slips, the
  EOW cut still ships old-skin. Check Remotion license tier before publishing revenue content.
- **NextRequest portals rate-limit** — gentle probes only (learned 2026-06-28, in validate_ab.py).
- **P4 is not the selector** for WS3 (density gate; STATE.md landmine). Tier-1 + completeness is.
- **TTS deferred** by operator (2026-07-01) despite docs' earlier push — narration stays on-screen
  text this week; the Remotion NarrationBand improves its timing instead.
- P0 never files; operator submits. FOIA returns are a weeks-later payoff, not EOW.
