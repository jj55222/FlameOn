# FlameOn — root map

> **Read this first, then pull only what the task needs.** This file is the map, not the
> encyclopedia. Deep detail lives in [STATE.md](STATE.md) (current state), the `.claude/skills/`
> (per-workflow runbooks, load on demand), and the topic handoffs linked below. Do **not** read
> every handoff up front — most detail is irrelevant to any single task and will just burn context.

## What FlameOn is

A **true-crime documentary factory**: it turns raw public-records footage (police bodycam, 911,
interrogation, FOIA/IA PDFs) into **EWU / Dr.Insanity-style narrated, fact-checked video cuts**.
The north star is producing cuts at the quality bar of the EWU Bodycam and Dr.Insanity YouTube
channels. See [HANDOFF_LONGFORM_EWU_STYLE.md](HANDOFF_LONGFORM_EWU_STYLE.md) for what "EWU-level"
means and the current editorial task.

## The pipeline (discovery → finished cut)

Six numbered stages, each a directory, each with a clean handoff to the next:

| Stage | Dir | Job | Primary entry point |
|-------|-----|-----|---------------------|
| P0 | `pipeline0_sourcing/` | Find FOIA-worthy cases from news/OSINT → draft requests to file | `sourcing_run.py` |
| P1 | `pipeline1_winners/` | Learn what makes winning cuts work → scoring weights | `analyze_winner.py` |
| P2 | `pipeline2_discovery/` | Discover cases with downloadable footage | `evaluate.py` (drives `research.py`) |
| P3 | `pipeline3_audio/` | Footage → clean timestamped transcripts | `pipeline3_transcribe.py` |
| P4 | `pipeline4_scoring/` | Score cases, pick moments, PRODUCE/HOLD/SKIP | `pipeline4_score.py` |
| P5 | `pipeline5_assembly/` | Assemble a production brief (no LLM) | `pipeline5_assemble.py` |
| P6 | `pipeline6_sequence/` | **Sequence + render the actual video** | `make_documentary.py` |

**Each pipeline dir has its own `CLAUDE.md`** — the detailed context for that stage. When you work
inside a pipeline, read its local `CLAUDE.md` first (Claude Code auto-loads it). `pipeline6_sequence/`
is the editorial engine and has the richest one.

**The editorial action is in P6.** That's where a case becomes a watchable cut. The two production
paths and their runbooks are skills — load them when you're producing:

- **`.claude/skills/flagship-longform/`** — Path B: longform narrated EWU/Dr.Insanity documentary
  (doc-driven arc, ~10–40 min). One-shot: `make_documentary.py --flagship`.
- **`.claude/skills/rawwalk-shortform/`** — Path A: chronological raw-bodycam replay (~7–10 min).
- **`.claude/skills/case-selection/`** — the cheap Tier-1 selector that decides what's worth producing.
- **`.claude/skills/foia-sourcing/`** — P0: scan news/OSINT for FOIA-worthy cases → draft requests
  (targets permissive "sunshine states"; runs autonomously, operator submits). MVP built.

## Current focus (2026-07)

Branch `p6-documentary-assembly`. **Active task:** find an EWU/Dr.Insanity-*shape* case
(high-severity crime + rich case doc + footage) and build a **longform flagship cut** + shape-aware
templates. Flagship candidate: `sdpd_01_05_2025_4400_fanuel_street` (footage-vs-report contradiction).
Full runbook: [HANDOFF_LONGFORM_EWU_STYLE.md](HANDOFF_LONGFORM_EWU_STYLE.md). Details: [STATE.md](STATE.md).
**This week's executor plan** (4 workstreams + goal scripts, incl. the new **Remotion render lane**):
[docs/plans/WEEK_2026-07-01_goals.md](docs/plans/WEEK_2026-07-01_goals.md) + `goals/`.

## Setup & the gotchas that will bite you

- **macOS Apple-Silicon (M4 Pro), Python 3.12, venv at `.venv/`.** Activate it.
- **ffmpeg/ffprobe MUST be brew's**, resolved via PATH. `imageio-ffmpeg` ships no ffprobe. Run
  `eval "$(/opt/homebrew/bin/brew shellenv)"` **before any tool that touches ffmpeg** (transcribe,
  render, yt-dlp, timeline stamping). Silent failures otherwise.
- **`.env` values have trailing `# comments`** (with em-dashes). Naive `grep|cut` grabs the comment
  → `UnicodeEncodeError` on HTTP headers. Use `from muckrock_harvest import _read_env_file`.
  `pipeline4_score.py` reads keys from `os.environ` only — export first.
- **zsh does not word-split unquoted `$VARS`.** For `nargs="+"` args, pass a glob
  (`--transcripts dir/kolb_*.json`), not a `$FILES` variable.
- **Local AI stack (no API keys, private):** `mlx-whisper` (GPU, ~10× realtime), `ocrmac`+PyMuPDF
  (Apple Vision OCR). ~23/24 GB RAM is tight when GPU + ANE run together.

## Do-not-touch (sacred / immutable)

- `pipeline4_scoring/evaluate.py` — immutable moment scorer. Patch externally, never edit.
- `pipeline2_discovery/calibration_data.json` — 38 frozen ground-truth cases.
- `pipeline4_scoring/golden/*.golden.json` — reviewed golden keys (Dutchess Way, 2023PSB-0530).
- `discovered_cases/CASE_BUNDLE_AGG.json` — append-only case registry.

## Known landmine: P4 is the wrong selector for EWU longform

P4's PRODUCE gate keys on `moment_density` (≥60), which is calibrated for *discovery triage*, not
*documentary worth*. Interview/doc-driven cases (911, interrogation, IA reports) never hit that
density even when fatal — P4 says HOLD where a strong LLM says PRODUCE. **For EWU longform, select
with the Tier-1 selector + `beat_miner`, not P4's gate.** See [STATE.md](STATE.md) and
[P4_VS_CLAUDE_CONCORDANCE.md](P4_VS_CLAUDE_CONCORDANCE.md).

## Conventions

- `--dry-run` exists on most tools; `make_documentary.py` is dry-run by default (`--run` to execute).
- Per-case working dirs (baskets): `.tmp/<case_id>/` (holds `video/`, `docs/`, `d2/transcripts/`,
  `timeline/`, `blueprint/`, `cuts/`). Large media zips live in `~/Downloads/`.
- Creator worth-model catalogs (428 EWU/Dr.Insanity titles): `.tmp/creator_catalog/`.
- Auto-open each rendered cut as it finishes (`&& open <cut>.mp4`).
