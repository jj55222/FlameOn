# Pipeline 0 — FOIA sourcing (top of funnel)

**Turns public news/OSINT signals into a ranked queue of FOIA requests to file.** The stage
UPSTREAM of P2: where P2+ source from records that are *already released*, P0 finds serious incidents
whose records likely exist but aren't public yet, and drafts the request to obtain them. Design +
rationale: [/docs/plans/P0_foia_sourcing.md](../docs/plans/P0_foia_sourcing.md). Root: [/CLAUDE.md](../CLAUDE.md).

## Flow & modules
`ingest → cluster → extract → score → draft → report`

| Module | Job |
|--------|-----|
| `ingest.py` | Pull signals from FREE keyless sources (Google News RSS + GDELT 2.0). Graceful on network failure. |
| `cluster.py` | Dedupe multi-outlet coverage into one incident. Pure stdlib; state-aware blocking (different states never merge). |
| `extract.py` | Incident → structured record (agency, state, severity, likely record types, EWU shape). LLM (OpenRouter) or offline `--mock` heuristic. |
| `score.py` | `foia_worth = severity × records × jurisdiction_access × filing_window` (+contradiction mult). Reads the state table; gates on severity, sunshine-state tier, residency. |
| `draft.py` | FILE incidents → a submit-ready FOIA write-up + request letter with the correct state statute cite + caveats. Deterministic (no LLM). |
| `sourcing_run.py` | **Orchestrator / entry point.** Chains it all; writes `foia_queue.md` + `.json`. |

## Run
```
source ../.venv/bin/activate
eval "$(/opt/homebrew/bin/brew shellenv)"

# offline smoke test — no network, no key (uses bundled fixtures):
python sourcing_run.py --mock --signals fixtures/sample_signals.json --include-watch --out .tmp/p0

# live: free ingestion + cheap LLM extraction (needs OPENROUTER_API_KEY in ../.env):
python sourcing_run.py --since 3 --limit 40 --tier1-only --seen .tmp/p0/seen.json --out .tmp/p0

# plan only (touches nothing):
python sourcing_run.py --dry-run
```
Output: `<out>/foia_queue.md` (human — review, fill `[BRACKETS]`, submit by hand), `foia_queue.json`
(machine), `scored.json` (all incidents + components). Append `&& open <out>/foia_queue.md`.

## Autonomy
Safe to cron / `/loop`: pass `--seen <store.json>` and each run surfaces only NEW incidents (already-
surfaced keys are skipped). The operator still submits manually — P0 drafts, it does not file.

## Key facts / tunables
- **Reads** `../discovered_cases/foia/state_access_profiles.json` for `jurisdiction_access` + statute
  + residency. Those rows are `verified: false` — the queue flags `verify_before_send` until a human
  checks them against RCFP.
- Tunables at the top of `score.py`: `SEV_GATE` (50), `FILE_THRESHOLD` (35), `WATCH_THRESHOLD` (18),
  record weights. Default search terms (the EWU-shape query set) at the top of `ingest.py`.
- **Leads, not findings.** Extraction reads what reporting says + reasons about likely records; it
  asserts no facts. Letters ask for records by category and must be verified before sending.

## Tests
`python -m pytest -q` (8 tests, offline/deterministic — clustering, severity gate, sunshine-vs-avoid
ranking, residency flag, end-to-end mock, statute-cite drafting).

## Known limitations (v1)
- Headline-only clustering; state-aware but can still over/under-merge — a post-extract re-cluster on
  (state, agency, date) would be more precise.
- `records_likely` is inferred from reporting, not agency BWC-policy data — some agencies don't wear
  cameras. `filing_window` uses `incident_date` when present, else a 0.7 default.
- No auto-filing (by design). If MuckRock exposes a request-create API for the account, step 6 could
  submit; today it drafts for manual submission.
