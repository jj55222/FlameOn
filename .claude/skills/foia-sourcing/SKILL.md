---
name: foia-sourcing
description: Find FOIA-worthy cases from public news/OSINT and draft records requests to file. Use when the user wants to source NEW cases (not already-released footage) — scan news for serious incidents worth requesting bodycam/911/IA records for, score them, and produce ready-to-submit FOIA write-ups targeting permissive "sunshine states". Runs autonomously and de-dupes across runs; the operator submits manually.
---

# FOIA sourcing (Pipeline 0)

Autonomous top-of-funnel: public news/OSINT signals → ranked queue of FOIA write-ups the operator
submits by hand. Code + detail: [`pipeline0_sourcing/`](../../../pipeline0_sourcing/) (read its
CLAUDE.md). Design: [`docs/plans/P0_foia_sourcing.md`](../../../docs/plans/P0_foia_sourcing.md).

## Prereqs
```
source .venv/bin/activate
eval "$(/opt/homebrew/bin/brew shellenv)"
```
Live LLM extraction needs `OPENROUTER_API_KEY` in `.env`. Ingestion (Google News RSS + GDELT) is free
and keyless. Use `--mock` to run the whole thing offline with no key.

## Run
```
cd pipeline0_sourcing
# live autonomous run — free ingest + cheap LLM extract, tier-1 sunshine states, only-new incidents:
python sourcing_run.py --since 3 --limit 40 --tier1-only --seen .tmp/p0/seen.json --out .tmp/p0
open .tmp/p0/foia_queue.md
```
Then: review each write-up, confirm agency + jurisdiction, verify the state-law row against its RCFP
link (rows are `verified: false`), fill the `[BRACKETS]`, and submit via MuckRock or the agency portal.

## Flow
`ingest → cluster → extract → score → draft → report`. `foia_worth = severity × records ×
jurisdiction_access × filing_window` (contradiction is a multiplier). Gates on `SEV_GATE`, sunshine-
state tier, and residency (VA-style states are flagged, not filed). Reads jurisdiction rules from
`discovered_cases/foia/state_access_profiles.json`.

## Autonomy (cron / loop)
Pass `--seen <store.json>`; each run surfaces only NEW incidents. P0 DRAFTS, never files — manual
submission is intentional. To schedule, use the `schedule` or `loop` skills to run the command above.

## Tuning
- Search terms (what counts as "worth following"): top of `ingest.py` (`DEFAULT_TERMS`), or `--terms`.
- Thresholds + record weights: top of `score.py`.
- Sunshine-state targeting: `--tier1-only` (FL/WA/CA), or edit the state table's tiers.

## Guardrails
Extraction produces LEADS, not findings — it asserts no facts. Letters request records by category and
must be verified before sending. Public records only; be targeted (the severity gate exists so we don't
spam agencies). FOIA turnaround is weeks–months — this fills the funnel for later, it is not same-week.

Reference: memory `foia-sourcing-p0-plan`; [STATE.md](../../../STATE.md) (known-gaps section).
