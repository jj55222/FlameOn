# `pipeline2_discovery/enrichment/`

Pipeline 1 enrichment: the bridge between dataset-intake search tasks and downstream validators.

## What this package is

A small, **pure** harness that consumes the `search_tasks.json` document produced by [`tools/run_dataset_intake.py`](../../tools/run_dataset_intake.py) (PR #31 / #32), filters / sorts / caps the tasks, and dispatches each one to a provider. The provider returns candidate URLs / titles / metadata; the runner aggregates everything into a per-task results array + a summary JSON.

## What this PR ships

This PR adds the **YouTube provider** on top of the harness + Mock skeleton:

- The harness — task loading, validation, filtering, sorting, capping, dispatch, aggregation.
- A `MockProvider` that returns deterministic synthetic results for tests and CLI smoke runs.
- A `YtDlpYouTubeSearchClient` (provider name: `youtube`) — see [YouTube provider](#youtube-provider) below.
- A `MuckRockProvider` (provider name: `muckrock`) — see [MuckRock provider](#muckrock-provider) below.
- The CLI ([`tools/run_enrichment_tasks.py`](../../tools/run_enrichment_tasks.py)).
- Tests covering every code path zero-network (yt-dlp + MuckRock HTTP are monkeypatched in tests).

Remaining live providers are **still deferred** and raise `NotImplementedError`:

- **PR 4 — Official-source web search provider.** Handles `official_source_query` tasks. Uses Brave / Exa / Tavily search behind explicit env gates. Returns official agency URLs only. Output feeds the portal-live curated scaffold + generator.
- **PR 5 (or later) — Outcome provider.** Handles `outcome_query` tasks. Returns court / news / prosecutor outcome candidates.

## YouTube provider

`youtube` (internal class `YtDlpYouTubeSearchClient`) is the in-PR live provider for `youtube_query` tasks.

- **Backend:** `yt-dlp`'s `ytsearchN:<query>` pseudo-URL — **not** the YouTube Data API.
- **No API key required.** Nothing to provision, no per-project quota burn.
- **Metadata only.** Runs with `extract_flat=True`, `skip_download=True`, `noplaylist=True`. No video, audio, subtitle, or caption downloads. No writes to disk.
- **No transcripts yet.** Caption pulls are deliberately deferred to a later media-preprocessing stage.
- **Rate limit etiquette:** the runner is sequential by design; a 10s socket timeout caps any single search.
- **Relevance gate.** Each yt-dlp candidate is scored against the task's identity context (subject name / agency-distinctive token / city / state name or standalone abbreviation) plus supporting domain signals (bodycam / CIB / pursuit). Candidates with no anchor match are dropped.
  - **Anchors** (any one is sufficient to keep): subject last name, full subject name, agency-distinctive token (after stripping boilerplate like "police" / "department" / "sheriff"), city, state name or standalone state abbreviation.
  - **Supporting signals** (boost score + confidence but never sufficient alone): "bodycam" / "body cam" / "BWC", "critical incident briefing", "dashcam" / "pursuit" / "police chase".
  - **Confidence:** `high` when an official-channel hint is present (uploader contains "city of" / "police department" / "sheriff's office" / "official") OR when both an agency token and a subject hit are present; `medium` when an anchor is present alongside a supporting signal; `low` for state-only anchors or when nothing survived filtering.
  - **`next_actions_hint`:** `["youtube_metadata"]` only when at least one result survives; `[]` otherwise.
  - **Diagnostics in `notes`:** `raw_result_count`, `filtered_result_count`, `dropped_irrelevant_count`, per-kept-result `score=N anchors=...`, and up to three example `dropped reason=... title='...'` lines so an operator inspecting the JSON can see what raw YouTube returned.
- **Failure mode:** any exception during search (network, parse, etc.) becomes `status: "failed"` with `error` set to `<ExceptionType>: <message>`; the run continues.
- **Live invocation only with `--run --provider youtube`.** Default dry-run never imports yt-dlp or hits the network — it just reports the selected tasks.
- **Future alternate backends.** YouTube Data API, Brave, Exa, etc. can be plugged in later as separate providers (or as a backend swap inside the same provider) without changing the runner contract.

## MuckRock provider

`muckrock` (internal class `MuckRockProvider`) is the in-PR live provider for `muckrock_query` tasks.

- **Backend:** GET-only against the public MuckRock API v2 (`https://www.muckrock.com/api_v2/requests/?title=<query>&page_size=20`).
- **No FOIA submission.** No POST / PUT / PATCH / DELETE methods are imported, defined, or called anywhere in the module — the CLI cannot, by construction, file requests on the operator's behalf. Pinned by `test_request_only_uses_get_method` (static + behavioural check).
- **Optional auth.** If `MUCKROCK_API_TOKEN` is set in the operator's environment, an `Authorization: Token …` header is added. Anonymous calls work for the public search endpoint; tests run with `read_token=False` so the env is never read.
- **No file downloads.** Even when the API surfaces `files[]`, the file URLs are recorded in `notes` / scoring inputs, never fetched.
- **Rate limit etiquette.** Default 1.0s sleep between executions; per-call `socket_timeout` 15s.
- **Relevance gate** (mirrors the YouTube gate's anchor logic):
  - **Anchors** (any one keeps a result): full subject name, subject last name, agency-distinctive token (after stripping boilerplate like "police"/"sheriff"/"office"), city, state name or standalone state abbreviation. State-only anchor stays at confidence `low`.
  - **Strong incident terms** (+3, classify as "incident-specific"): `officer-involved shooting`, `deputy-involved shooting`, `police shooting`, `critical incident briefing`, `in-custody death`.
  - **Bodycam family** (+2): `bodycam`, `body cam`, `body camera`, `body-worn camera`, `BWC`.
  - **Pursuit family** (+2): `pursuit`, `dashcam`, `police chase`, `high-speed chase`.
  - **Audio / report family** (+1): `911 audio`, `911 call`, `CAD logs`, `incident report`.
  - **Released-files signal** (+5 if `len(files) > 0`, +3 if `status` is "done"/"completed"/"complete"/"released").
  - **Policy-only demotion** (-4 + flagged `is_policy_only=True`): `policy manual`, `general orders`, `all policies`, `training materials`, `body-worn camera policy`, `BWC policy`, `audit`, `budget`, etc. A policy-only result is dropped unless it has released files OR strong incident terms (a released BWC policy still passes — file presence beats title heuristic).
- **Confidence:**
  - `high` — anchored AND (released files OR strong incident terms).
  - `medium` — anchored AND a supporting signal but no released files / strong terms.
  - `low` — state-only anchor, no surviving results, or all policy-only-demoted.
- **`next_actions_hint`:**
  - `MUCKROCK_PARSE_RELEASED_FILES` — emitted whenever ≥1 result survives.
  - `ARTIFACT_SEARCH` — added when any kept result has `file_count > 0`.
  - `YOUTUBE_METADATA_TRANSCRIPT` — added **only** when a kept result's metadata embeds a YouTube URL. The YouTube provider's own relevance gate then re-scores that URL. We never auto-emit YouTube hints just because a request mentions video.
  - `OUTCOME_VALIDATE` — added when any kept result's status is not in the `done`/`completed`/`complete`/`released` set.
- **Diagnostics in `notes`:** `raw_result_count`, `returned_result_count`, `dropped_irrelevant_count`, `released_file_count`, `policy_only_demoted=true|false`, `api_url=…` (token redacted — token rides in headers, never the URL), `terms_matched=…`, per-kept `kept score=N anchors=… files=K status=…`, and up to 3 `dropped reason=… title='…'` examples.
- **Non-`muckrock_query` task input** is returned as `status="skipped"` with a clear note; the runner counts it under `skipped_count`.
- **Live invocation only with `--run --provider muckrock`.** Default dry-run never instantiates the HTTP client.

## What this PR is NOT

- **Not a crawler.** Reads only the local input JSON.
- **Not a YouTube Data API caller.** The `youtube` provider uses `yt-dlp`'s metadata search, no Google API key.
- **Not a FOIA-submission tool.** The `muckrock` provider is GET-only — no POST/PUT/PATCH/DELETE — and cannot file requests on the operator's behalf.
- **Not a media downloader.** Even the live `youtube` and `muckrock` providers only return URLs + titles; bytes pull (video / audio / captions / MuckRock release files) is a separate later stage.
- **Not a Brave / Exa / Tavily client.** Those names raise `NotImplementedError` if passed via `--provider`.
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
    │       MockProvider + YtDlpYouTubeSearchClient +
    │       MuckRockProvider in this PR;
    │       Brave/Exa/Tavily deferred
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

# Live YouTube run: invoke yt-dlp search per task (metadata only, no media)
#   Requires `pip install yt-dlp`. No API key. Sequential, ~1 search per task.
python tools/run_enrichment_tasks.py \
    --input .tmp/dataset_intake_ranked/search_tasks.json \
    --output-json .tmp/enrichment/results.json \
    --summary-out .tmp/enrichment/summary.json \
    --task-type youtube_query \
    --grade A \
    --max-tasks 5 \
    --run \
    --provider youtube \
    --json

# Live MuckRock run: GET-only public API search (metadata only, no FOIA submission)
#   No API key required. Optional MUCKROCK_API_TOKEN env var honored if set.
#   Sequential with 1s default rate limit. No file downloads.
python tools/run_enrichment_tasks.py \
    --input .tmp/dataset_intake_ranked/search_tasks.json \
    --output-json .tmp/enrichment/results.json \
    --summary-out .tmp/enrichment/summary.json \
    --task-type muckrock_query \
    --grade A \
    --max-tasks 5 \
    --run \
    --provider muckrock \
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
    [--provider mock|youtube|muckrock] default mock; youtube = yt-dlp; muckrock = MuckRock API v2 (GET-only)
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

- **No live Brave / Exa / Tavily search.** Still gated; `NotImplementedError` if requested.
- **No YouTube Data API.** The in-PR `youtube` provider uses yt-dlp metadata search only.
- **No FOIA submission.** The in-PR `muckrock` provider is GET-only; it cannot file requests.
- **No media downloads.** The live `youtube` and `muckrock` providers only return URLs + titles; bytes pull (video / audio / captions / transcripts / MuckRock release files) is a separate later stage.
- **No portal-live invocations.** The runner emits hints; routing is downstream.
- **No automatic provider selection.** Each task type currently goes through the same provider (the operator picks one); per-task-type provider routing comes when more than one provider exists.
- **No retries / backoff.** Single attempt per task; failures land at `status: "failed"` with the exception type in `error`.
- **No parallelism.** Sequential by design.
