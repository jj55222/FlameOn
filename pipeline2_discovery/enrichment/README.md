# `pipeline2_discovery/enrichment/`

Pipeline 1 enrichment: the bridge between dataset-intake search tasks and downstream validators.

## What this package is

A small, **pure** harness that consumes the `search_tasks.json` document produced by [`tools/run_dataset_intake.py`](../../tools/run_dataset_intake.py) (PR #31 / #32), filters / sorts / caps the tasks, and dispatches each one to a provider. The provider returns candidate URLs / titles / metadata; the runner aggregates everything into a per-task results array + a summary JSON.

## What this PR ships

This skeleton PR ships **only**:

- The harness — task loading, validation, filtering, sorting, capping, dispatch, aggregation.
- A `MockProvider` that returns deterministic synthetic results for tests and CLI smoke runs.
- The CLI ([`tools/run_enrichment_tasks.py`](../../tools/run_enrichment_tasks.py)).
- Tests covering every code path zero-network.

Live providers are **deferred to follow-up PRs** and explicitly raise `NotImplementedError`:

- **PR 2 — YouTube provider.** Handles `youtube_query` tasks. Searches via YouTube Data API or `yt-dlp`'s search interface; prefers official agency channels; returns video URLs + channel + transcript availability. Output feeds future media preprocessing.
- **PR 3 — MuckRock API provider.** Handles `muckrock_query` tasks. API-gated and rate-limited. Returns request URLs / status / files. Output feeds the existing `muckrock_curated` parser.
- **PR 4 — Official-source web search provider.** Handles `official_source_query` tasks. Uses Brave / Exa / Tavily search behind explicit env gates. Returns official agency URLs only. Output feeds the portal-live curated scaffold + generator.
- **PR 5 (or later) — Outcome provider.** Handles `outcome_query` tasks. Returns court / news / prosecutor outcome candidates.

## What this PR is NOT

- **Not a crawler.** Reads only the local input JSON.
- **Not a live web caller.** No network in any code path; `MockProvider` returns synthetic URLs at `mock.example.com` so misuse fails obviously.
- **Not a YouTube / MuckRock / Brave / Exa / Tavily client.** Those names raise `NotImplementedError` if passed via `--provider`.
- **Not a portal-live caller.** The runner only emits next-action hints; routing happens downstream.

## Pipeline shape

```
dataset-intake search_tasks.json
    ├─ task loader                      (task_loader.py)
    │       loads + validates rows
    ▼
list[EnrichmentTask]
    │
    ├─ filter_tasks                     (runner.py)
    │       --task-type / --candidate-id / --grade
    ▼
filtered tasks (sorted + capped)
    │
    ├─ provider.execute(task)           (providers.py)
    │       MockProvider in this PR;
    │       YouTube/MuckRock/Brave deferred
    ▼
list[EnrichmentResult]
    │
    ▼
summary.json (aggregate counts + per-task results)
    │
    └─ downstream feeds (operator-driven for now):
       official URLs   ->  portal-live scaffold + generator
       MuckRock URLs   ->  muckrock_curated parser
       YouTube URLs    ->  future media preprocessing
       outcome URLs    ->  identity / outcome validation
```

## Operator workflow (current PR)

```bash
# Dry-run: filter + cap + report; no provider invocation
python tools/run_enrichment_tasks.py \
    --input .tmp/dataset_intake_ranked/search_tasks.json \
    --output-json .tmp/enrichment/results.json \
    --summary-out .tmp/enrichment/summary.json \
    --task-type youtube_query \
    --grade A \
    --max-tasks 10

# Mock run: invoke MockProvider per task (deterministic synthetic results)
python tools/run_enrichment_tasks.py \
    --input .tmp/dataset_intake_ranked/search_tasks.json \
    --output-json .tmp/enrichment/results.json \
    --summary-out .tmp/enrichment/summary.json \
    --task-type youtube_query \
    --grade A \
    --max-tasks 5 \
    --run \
    --provider mock \
    --json
```

All output paths sit under `.tmp/` (gitignored after PR #26).

## CLI surface

```
python tools/run_enrichment_tasks.py
    --input <path>                    required (search_tasks.json)
    [--output-json <path>]            per-task results JSON (safe-path-gated)
    [--summary-out <path>]            aggregate summary JSON (safe-path-gated)
    [--task-type youtube_query|muckrock_query|official_source_query|outcome_query]   repeatable filter
    [--candidate-id <id>]             repeatable filter
    [--grade A|B|C|D]                 repeatable filter
    [--max-tasks N]                   default 10
    [--run]                           required to invoke provider; default dry-run
    [--provider mock]                 default mock; only mock allowed in this PR
    [--json]                          machine-readable stdout
```

Both invocation forms work: `python tools/run_enrichment_tasks.py ...` and `python -m tools.run_enrichment_tasks ...` (sys.path bootstrap mirrors PR #28).

## Result schema

Per-task `EnrichmentResult`:

```json
{
  "candidate_id":      "sfchronicle_pursuits:1797",
  "task_type":         "youtube_query",
  "query":             "Christopher Vang Fresno Police Department bodycam",
  "status":            "dry_run" | "completed" | "failed" | "skipped",
  "provider":          "mock",
  "result_urls":       ["https://..."],
  "result_titles":     ["..."],
  "confidence":        "high" | "medium" | "low" | "unknown",
  "next_actions_hint": ["PORTAL_LIVE_VALIDATE", ...],
  "error":             null,
  "notes":             ["..."]
}
```

Summary:

```json
{
  "run_id":            "<UTC compact timestamp>",
  "started_at":        "<ISO>",
  "finished_at":       "<ISO>",
  "elapsed_seconds":   <float>,
  "dry_run":           <bool>,
  "provider":          "mock",
  "selected_count":    <int>,
  "attempted_count":   <int>,
  "completed_count":   <int>,
  "failed_count":      <int>,
  "skipped_count":     <int>,
  "max_tasks":         <int>,
  "results":           [...]
}
```

## Out of scope for this PR

- **No live search providers.** YouTube, MuckRock API, Brave / Exa / Tavily — all gated; `NotImplementedError` if requested.
- **No media downloads.** Even live providers (when added) only return URLs + metadata; bytes pull is a separate later stage.
- **No portal-live invocations.** The runner emits hints; routing is downstream.
- **No automatic provider selection.** Each task type currently goes through the same provider (the operator picks one); per-task-type provider routing comes when more than one provider exists.
- **No retries / backoff.** Single attempt per task; failures land at `status: "failed"` with the exception type in `error`.
- **No parallelism.** Sequential by design.
