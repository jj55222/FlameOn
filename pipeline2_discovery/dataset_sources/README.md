# `pipeline2_discovery/dataset_sources/`

Pipeline 1 discovery lane for curated public datasets and saved API responses.

## What this package is

A small, **pure** module family that takes operator-curated public datasets (CSV files, saved JSON API responses, hand-collected URL lists) and emits normalized [`DatasetCandidate`](models.py) rows + downstream search tasks + rough packet stubs. The output feeds into the rest of the pipeline:

- candidates with **official URLs** → Pipeline 2 portal-live (via `tools/run_portal_live_batch.py`)
- candidates with **MuckRock URLs** → existing MuckRock file resolver
- candidates with **YouTube URLs/queries** → YouTube metadata + transcript extraction
- every candidate → identity / outcome validation tasks for the search-task lane

## What this package is NOT

- **Not a crawler.** No URL discovery beyond what an operator hands in.
- **Not a web fetcher.** No HTTP calls of any kind.
- **Not a media downloader.** No bytes pulled for video/audio/PDFs.
- **Not a portal-live caller.** Dataset intake only emits next-action hints; routing / fetching is downstream.
- **Not a MuckRock API client.** Live API support is gated to a separate (currently unimplemented) `--run-live-muckrock` flag and is not in scope for the initial PR.
- **Not a packet author.** Outputs at most a markdown stub per top-N candidate; full PRODUCE-shaped CasePackets stay in Pipeline 2 / Pipeline 4.

## Source-lane priorities

In rough order of expected packet yield:

1. **MuckRock released records** — public-records requests where the agency has actually produced files. Highest signal; lowest noise.
2. **SF Chronicle fatal pursuits** — well-curated CSV with named decedents, agencies, news links. Wide coverage; good for batch packet generation.
3. **Official YouTube** — agency-uploaded briefing videos. Already pulled into Pipeline 2 via the existing YouTube resolver; this lane will surface candidates whose only artifact is a YouTube link.
4. **Official portals** — agency newsroom pages (Phoenix-style). Pipeline 2 portal-live consumes these directly via the generated target fixtures.
5. **FARS validation** — Fatality Analysis Reporting System reconciliation; not a primary discovery lane but a useful corroboration source.
6. **Illinois / PA reports** — state-level pursuit/OIS PDFs and reports. Future lanes; require PDF parsing.

This PR adds lanes 1 and 2.

## Architecture

```
operator-curated input
    ├─ SF Chronicle CSV         ── sfchronicle_pursuits.parse_csv_text ──┐
    └─ MuckRock URLs / records  ── muckrock_leads.parse_url_list_text  ──┤
                                                                         ▼
                                                        list[DatasetCandidate]
                                                                         │
                                                                         ▼
                          tools/run_dataset_intake.py
                                                                         │
                              ┌──────────────────────────┬───────────────┴────────┐
                              ▼                          ▼                        ▼
                        candidates.csv            search_tasks.json       packet_stubs/
                        candidates.json
```

`DatasetCandidate` is a single shared dataclass. Per-lane parsers all emit it; downstream consumers (CLI, packet stub writer, eventual portal-live router) only ever speak that one shape.

## Scoring + grades

Each lane has its own deterministic per-row scoring rubric (see [`sfchronicle_pursuits.score_pursuit_row`](sfchronicle_pursuits.py) and [`muckrock_leads.score_muckrock_record`](muckrock_leads.py)). Scores are integers; the shared [`scoring.assign_grade`](scoring.py) maps them to A / B / C / D:

| Grade | Score | Evidence strength |
|---|---|---|
| A | ≥ 9 | strong |
| B | 6–8 | moderate |
| C | 3–5 | weak |
| D | < 3 | weak |

Top-graded candidates flow into packet stubs first.

## Operator workflow

```bash
# 1. SF Chronicle dataset → top-25 candidates + packet stubs
python tools/run_dataset_intake.py \
    --source sfchronicle_pursuits \
    --input .tmp/dataset_intake/sfchronicle_pursuits.csv \
    --output-csv .tmp/dataset_intake/candidates.csv \
    --output-json .tmp/dataset_intake/candidates.json \
    --search-tasks-out .tmp/dataset_intake/search_tasks.json \
    --packet-stubs-dir .tmp/dataset_intake/packets \
    --top-n 25 \
    --target-states AZ,FL,TX,OH,IL,CA \
    --json

# 2. MuckRock curated URL list → candidates + packet stubs
python tools/run_dataset_intake.py \
    --source muckrock_curated \
    --input .tmp/dataset_intake/muckrock_urls.txt \
    --output-csv .tmp/dataset_intake/muckrock_candidates.csv \
    --output-json .tmp/dataset_intake/muckrock_candidates.json \
    --packet-stubs-dir .tmp/dataset_intake/muckrock_packets \
    --top-n 25 \
    --json
```

All output paths sit under `.tmp/` (gitignored after PR #26).

## Out of scope for the initial PR

- Full FARS parser
- Illinois / PA pursuit-report PDF extractors
- PA pursuit-report scraper
- Broad MuckRock API search (live API gate exists conceptually but is unimplemented)
- Mass YouTube channel ingestion
- Direct portal-live invocation from intake (intake only emits hints)
- Packet production beyond markdown stubs (full CasePackets stay in P2 / P4)
