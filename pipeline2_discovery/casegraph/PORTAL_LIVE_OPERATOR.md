# CaseGraph `--portal-live` operator guide

Operator-facing reference for running the CaseGraph CLI's `--portal-live` mode against real agency portals (e.g. the Phoenix Police Department Newsroom).

This doc covers **the actual current CLI surface** — what flags exist, what env vars exist, and where outputs land. PR bodies and earlier drafts referenced flags that don't exist on the merged CLI; if you read those and got argparse errors, you're in the right place.

---

## What the mode does

```
target fixture JSON  ──►  KnownUrlLiveSmokeTarget safety preflight
                          allowed_domains enforcement
                          fetch client (mock | requests | firecrawl)
                          save raw payload (gated by fixture)
                          per-template extract → agency_ois payload (gated by fixture)
                          save extracted payload (gated by fixture)
                          replay through portal-replay (gated by fixture)
                          single canonical bundle JSON
```

Exactly one HTTP call per run, against exactly one URL — the URL named in the target fixture. The fixture's `allowed_domains` list is enforced; any redirect or fetch outside the list is rejected before bytes hit disk.

## Env gates

Live HTTP requires **both** of these environment variables to be set to `"1"`:

| Variable | Required | Purpose |
|---|---|---|
| `FLAMEON_RUN_LIVE_CASEGRAPH` | yes | Top-level CaseGraph live-mode gate (also used by `--live-dry`) |
| `FLAMEON_RUN_LIVE_PORTAL_FETCH` | yes | Portal-fetch-specific gate (only `--portal-live`) |
| `FIRECRAWL_API_KEY` | only if `fetcher: firecrawl` | Firecrawl SDK credential (not used by `requests` fetcher) |

There is **no** `FLAMEON_PORTAL_LIVE_FETCH_REAL_HTTP` env var. If you see that name in old PR bodies or drafts, ignore it — the actual gates are the two above.

Without both env gates, the orchestrator records `live_fetch.status = "blocked"` with `blocked_reason` naming the missing var, and the run exits without making an HTTP call.

## Target-fixture fields (what controls behavior)

There are no CLI flags for the per-run knobs — they all live in the target fixture JSON. The fixture is the source of truth.

| Field | Type | Default | Effect |
|---|---|---|---|
| `target_id` | string | required | Identifies the run; embedded in saved-payload filenames |
| `url` | string | required | The single URL to fetch |
| `profile_id` | string | required | Portal profile (e.g. `agency_ois_detail`) |
| `fetcher` | string | `"mock"` | `mock` (offline), `requests` (live HTTP), `firecrawl` (deferred) |
| `allowed_domains` | string[] | `[]` | Hard whitelist; any other host is rejected |
| `max_pages` | int | 1 | Per-run page cap |
| `max_links` | int | 5 | Per-run discovered-link cap |
| `expected_response_status` | int | 200 | Asserted HTTP status |
| `save_raw_payload` | bool | `true` | Write raw HTTP response JSON |
| `save_extracted_payload` | bool | `true` | Write extracted `agency_ois` payload JSON |
| `require_extraction` | bool | `true` | Run a per-template extractor on the raw payload |
| `replay_through_portal_replay` | bool | `true` | Feed extracted payload into the existing portal-replay path |

For a fetch-only smoke (no extractor, no replay): set `save_extracted_payload=false`, `require_extraction=false`, `replay_through_portal_replay=false`.

For the full extraction+replay smoke: leave them all `true` (or set explicitly).

## CLI surface (only flags that actually exist)

```
python -m pipeline2_discovery.casegraph.cli \
    --portal-live \
    --target-fixture <path/to/target.json> \
    [--bundle-out <path>] \
    [--allow-unsafe-bundle-path] \
    [--json]
```

`--portal-live`-relevant flags:

| Flag | Effect |
|---|---|
| `--target-fixture <path>` | **Required.** Path to the target fixture JSON. |
| `--bundle-out <path>` | Optional. Write a single canonical run bundle JSON. Subject to the safe-path policy (next section). |
| `--allow-unsafe-bundle-path` | Override the safe-path policy. Only use if you genuinely want the bundle outside a gitignored dir. |
| `--json` | Emit the full result as JSON on stdout instead of human-readable text. |

Flags that do **not** exist on the current CLI (do not pass them — argparse will reject the run):

- `--require-extraction` — not a flag; control via the fixture's `require_extraction` field.
- `--payloads-dir` — not a flag; raw + extracted payloads land under the orchestrator's default payloads dir (`autoresearch/.runs/live_payloads/`).
- `--verbose` — not a flag.

`--emit-handoffs` exists on the parser but is **default-mode only** (CasePacket dry-run path), not `--portal-live`. The portal-live bundle has `live_fetch` + `portal_replay` + `result` blocks but no separate `handoffs` block.

## Bundle-output safe paths

`--bundle-out` enforces a safe-path allowlist so transient artifacts aren't accidentally committed. The path must resolve to one of:

- `.tmp/...`, `.runs/...`, `.artifacts/...`, `.cache/...`, `.logs/...` at the **repo root**, OR
- `autoresearch/.tmp/...`, `autoresearch/.runs/...`, `autoresearch/.artifacts/...`, `autoresearch/.cache/...`, `autoresearch/.logs/...`, OR
- A path **outside the repo entirely**.

All ten of these paths are now in `.gitignore`. Anything else requires `--allow-unsafe-bundle-path` to opt in explicitly.

Recommended bundle path for one-off operator runs: `.tmp/portal_live/<target_id>/bundle.json` — short, scoped, gitignored.

## Where outputs land

| Artifact | Location | Filename pattern |
|---|---|---|
| Raw payload (HTTP response) | `autoresearch/.runs/live_payloads/` | `<ISO-8601>_<target_id>.raw.json` |
| Extracted `agency_ois` payload | `autoresearch/.runs/live_payloads/` | `<ISO-8601>_<target_id>.extracted.json` |
| Run bundle (if `--bundle-out` passed) | wherever you point it | `bundle.json` |

The two payload paths surface in the result as `live_fetch.raw_payload_path` and `live_fetch.extracted_payload_path`.

## Worked example: fetch-only smoke

```bash
export FLAMEON_RUN_LIVE_CASEGRAPH=1
export FLAMEON_RUN_LIVE_PORTAL_FETCH=1

python -m pipeline2_discovery.casegraph.cli \
    --portal-live \
    --target-fixture tests/fixtures/portal_live_targets/<target>_real_fetch_only.json \
    --bundle-out .tmp/portal_live/<target>_fetch_only/bundle.json \
    --json
```

The fetch-only fixture sets `require_extraction=false` and `replay_through_portal_replay=false`. Expect:

- `live_fetch.status = "completed"`, `status_code = 200`, `api_calls = {"requests": 1}`
- Raw payload written; `extracted_payload_path = null`; `replayed = false`
- Bundle written with `live_fetch` + `input_summary` only (no `result`/`portal_replay` blocks)

## Worked example: extraction-required smoke

```bash
export FLAMEON_RUN_LIVE_CASEGRAPH=1
export FLAMEON_RUN_LIVE_PORTAL_FETCH=1

python -m pipeline2_discovery.casegraph.cli \
    --portal-live \
    --target-fixture tests/fixtures/portal_live_targets/<target>_real_extract_required.json \
    --bundle-out .tmp/portal_live/<target>_extract/bundle.json \
    --json
```

The extract-required fixture sets `require_extraction=true`, `save_extracted_payload=true`, `replay_through_portal_replay=true`. Expect:

- Same `live_fetch.status = "completed"` + `status_code = 200` + one HTTP call
- Both raw and extracted payload paths populated; `replayed = true`
- Bundle includes `live_fetch`, `portal_replay`, `result`, `packet_summary`, `verified_artifacts`, `artifact_claims`, `ledger_entry`

For Phoenix CIB pages specifically: subjects are typically empty (Phoenix CIBs don't name subjects pre-adjudication), `identity_confidence` lands `medium` (sub-HIGH), and `result.verdict` is `HOLD`. The bodycam media row still graduates to verified `bodycam` from the embedded YouTube briefing.

## Common error paths

| `live_fetch.status` | `blocked_reason` | Cause | Fix |
|---|---|---|---|
| `blocked` | `missing_env_gates:FLAMEON_RUN_LIVE_CASEGRAPH[,FLAMEON_RUN_LIVE_PORTAL_FETCH]` | One or both env gates not set | Export both `=1` |
| `blocked` | `target_allowed_domains_empty` | Fixture's `allowed_domains` is missing or empty | Add the URL's host to the fixture |
| `blocked` | one of the strings returned by the domain check (e.g. host mismatch) | Target URL host not in fixture's `allowed_domains` | Update fixture |
| `blocked` | `unexpected_status_code:<code>` | HTTP status didn't match `expected_response_status` | Check the URL or update the expectation |
| `blocked` | `missing_FIRECRAWL_API_KEY` | `fetcher: firecrawl` without `FIRECRAWL_API_KEY` set | Use `fetcher: requests` (the production live path) or set the key |
| `blocked` | `extract_failed:<exception>` | Per-template extractor raised on the saved raw payload | Inspect the raw payload at `live_fetch.raw_payload_path` and the extractor's gate logic |

`argparse: error: unrecognized arguments: --require-extraction` (or `--payloads-dir`, `--verbose`): you're using a flag that doesn't exist. Remove it — control extraction via the fixture's `require_extraction` field; let payloads default to `autoresearch/.runs/live_payloads/`.

## Generating target fixtures from curated URL lists

When you have a small list of curated official Phoenix Newsroom CIB URLs and want to drive each through `--portal-live`, hand-authoring one fixture per URL gets tedious. The `tools/generate_portal_live_targets.py` script accepts a curated JSON input and writes one (or both) target-fixture variants per accepted URL.

**This is not a crawler.** The script only validates URL shapes against the lint allowlist (`pipeline2_discovery/casegraph/portal_live_target_lint.py`) and serializes fixtures. It does **not** fetch any URL. Discovery — the act of finding which Phoenix Newsroom IDs exist — remains an operator action (Google site-search, manual browsing, etc.), and any live smoke against a generated fixture is a separate operator action documented above.

### Lint scope (current)

The lint module accepts only:

- **Scheme:** `https://` — `http://` rejected.
- **Host:** `www.phoenix.gov` only (extend `PORTAL_LIVE_TARGET_HOSTS` in a future PR for additional agencies).
- **Path:** `/newsroom/police-department-news/<id>.html` where `<id>` is alnum, hyphen, or underscore.
- **No query strings**, **no fragments** (fragments are silently stripped).
- Path/extension denylists reject `/search`, `/login`, `/auth`, `/admin`, `/private`, `/account`, plus `.pdf` / binary-media file extensions.

Each rejection returns a stable snake_case reason code (`non_https_scheme`, `host_not_in_allowlist`, `query_string_not_allowed`, `denylisted_path`, `denylisted_extension:<.ext>`, `path_pattern_mismatch`, `unsafe_target_id`, etc.) so operator output groups cleanly.

### Input shape

A JSON file containing an array of row objects:

```json
[
  {
    "target_id": "phoenix_pd_2025_02_12_higley_cib",
    "url": "https://www.phoenix.gov/newsroom/police-department-news/3369.html",
    "agency": "Phoenix Police Department",
    "jurisdiction": "Phoenix, Maricopa County, Arizona",
    "notes": "Higley Rd CIB"
  },
  {
    "url": "https://www.phoenix.gov/newsroom/police-department-news/3286.html"
  }
]
```

Only `url` is required. `target_id` is optional — supply it to override the default `phoenix_pd_newsroom_<id>` derivation; otherwise the lint derives a stable id from the URL path. `agency`, `jurisdiction`, `notes` are tolerated as free-form metadata but not read by the script.

### Generator output

For each accepted row the generator writes one or both fixture variants (depending on `--mode`):

| Filename pattern | Variant | `target_id` field | Fixture booleans |
|---|---|---|---|
| `<id>_real_fetch_only.json` | fetch-only | `<id>` | `save_extracted_payload: false`, `replay_through_portal_replay: false`, `require_extraction: false` |
| `<id>_real_extract_required.json` | extract-required | `<id>_extract_required` | all three `true` |

Both variants share `profile_id: "agency_ois_detail"`, `fetcher: "requests"`, `max_pages: 1`, `max_links: 5`, `expected_response_status: 200`, `save_raw_payload: true`, and `allowed_domains: [<host>]`.

The generator deduplicates by both normalized URL and `target_id` and caps acceptance at `--max-targets` (default 5) per run.

### Example: dry-run

```bash
python tools/generate_portal_live_targets.py \
    --input tools/example_phoenix_curated.json \
    --output-dir .tmp/generated_targets \
    --mode both \
    --dry-run
```

Reports what would be written (and lists rejected rows with reason codes) without creating any files. The output directory is **not** created in dry-run mode.

### Example: write to a gitignored output dir

```bash
python tools/generate_portal_live_targets.py \
    --input tools/example_phoenix_curated.json \
    --output-dir .tmp/generated_targets \
    --mode fetch_only \
    --max-targets 3 \
    --json
```

Writes up to 3 `*_real_fetch_only.json` fixtures into `.tmp/generated_targets/` (which is gitignored after PR #26's safe-path cleanup) and emits a JSON report on stdout for downstream piping. To then run a live smoke against a generated fixture, follow the **Worked example: extraction-required smoke** section above with the generated fixture path. Live smokes remain a separate operator action — the generator never fetches the web.

### Module-style invocation (equivalent)

Both forms work and produce identical results — pick whichever fits your shell history better:

```bash
# Direct script invocation (the form shown in the examples above):
python tools/generate_portal_live_targets.py --input ... --output-dir ...

# Module-style invocation:
python -m tools.generate_portal_live_targets --input ... --output-dir ...
```

The script auto-bootstraps the repo root onto `sys.path` so the direct form works from the repo root without `PYTHONPATH=.` or any other shell wrapper.

## Curating Phoenix Newsroom URLs

The generator above takes a **curated** input JSON of Phoenix Newsroom URLs. This section covers how to acquire that input safely. **The acquisition workflow is manual on purpose** — there's no crawling, no live fetching of candidate URLs, and no automated discovery. Operators do the discovery; the helper script (`tools/scaffold_phoenix_curated_urls.py`) only validates URL shape and reshapes a manually reviewed list into the generator's input form.

### Step 1: manual site-search for candidate URLs

Run one of these queries in a regular browser (or Google/Bing's search interface — never via an API or scraper):

```
site:phoenix.gov/newsroom/police-department-news "Critical Incident Briefing"
site:phoenix.gov/newsroom/police-department-news "officer-involved shooting"
site:phoenix.gov/newsroom/police-department-news "Phoenix Police" "Critical Incident Briefing"
```

These return Phoenix Police Department CIB pages on the canonical newsroom path. Other phoenix.gov paths (parks news, water dept news, etc.) and any non-phoenix.gov result get rejected at lint.

### Step 2: manual review

Operator reviews each result by hand and keeps only URLs that meet **all** of these criteria:

- Host is exactly `www.phoenix.gov` (no subdomains, no PDFs hosted elsewhere).
- Path matches `/newsroom/police-department-news/<id>.html` (numeric IDs *or* slug-form IDs both work).
- The page is a **Phoenix Police Critical Incident Briefing or officer-involved shooting briefing** — not a parks/department-news article that happens to mention police.
- The page is *not* a PDF, a search result page, a listing/index page, or a press release that does not embed a briefing video.
- The URL is *not* from MuckRock, news aggregators, social media, or any non-phoenix.gov host (these would all be rejected at lint anyway, but rejecting them earlier saves cycles).
- Prefer pages with an embedded YouTube briefing video — the agency_ois resolver chain graduates that to a verified `bodycam` artifact downstream.

### Step 3: paste reviewed URLs into a local file

Any of these formats is accepted (auto-detected from the file extension; override with `--format`):

| Format | Example file | Notes |
|---|---|---|
| Plain text (`*.txt`, default) | `.tmp/phoenix_search_urls.txt` | One URL per line. Blank lines and lines starting with `#` are ignored — paste with comments freely. |
| CSV (`*.csv`) | `.tmp/phoenix_search_urls.csv` | Must have a `url` column header. Other columns are ignored at this layer. |
| TSV (`*.tsv`) | `.tmp/phoenix_search_urls.tsv` | Same as CSV. |
| JSON (`*.json`) | `.tmp/phoenix_search_urls.json` | Either an array of URL strings, or an array of objects each with a `url` key. |

Save it under `.tmp/` so it's gitignored.

### Step 4: scaffold dry-run

```bash
python tools/scaffold_phoenix_curated_urls.py \
    --input .tmp/phoenix_search_urls.txt \
    --output .tmp/portal_live_generator/phoenix_curated.json \
    --rejected-output .tmp/portal_live_generator/phoenix_rejected.json \
    --json
```

Default mode is **dry-run** — the script reports what would be accepted, rejected, and deduplicated, but **writes no files**. The `--rejected-output` path is also dry-run unless `--write-reviewed` is also passed. Inspect the output and remove anything that shouldn't end up in the curated file.

### Step 5: scaffold write

```bash
python tools/scaffold_phoenix_curated_urls.py \
    --input .tmp/phoenix_search_urls.txt \
    --output .tmp/portal_live_generator/phoenix_curated.json \
    --rejected-output .tmp/portal_live_generator/phoenix_rejected.json \
    --write-reviewed \
    --json
```

The `--write-reviewed` flag is deliberately verbose to keep operators in the loop: only pass it after reviewing the dry-run output. With it, the curated JSON gets written (and the rejected report too, when its path is supplied).

### Step 6: feed the curated file to the existing generator

```bash
python tools/generate_portal_live_targets.py \
    --input .tmp/portal_live_generator/phoenix_curated.json \
    --output-dir .tmp/portal_live_generator/generated_targets \
    --mode both \
    --dry-run \
    --json
```

(Then re-run without `--dry-run` to actually write fixtures, exactly as the generator section above documents.)

### Step 7: one live smoke after human review

Run **one** `--portal-live` smoke against one generated fetch-only or extract-required fixture, exactly as the *Worked example: extraction-required smoke* section above documents. This is the only step that touches the network, and it's a separate explicit operator action.

### What this workflow is NOT

- It is **not crawling**. Nothing in this script fetches `phoenix.gov` or any other URL.
- It is **not browser automation**. The site-search queries are run by a human in a regular browser.
- It is **not search-API integration**. The script never calls Google's API, Bing's API, or any other search service.
- It is **not Firecrawl**. Firecrawl remains a deferred fetcher for the live-fetch path; the scaffold workflow does not invoke it.
- It is **not a batch smoke runner**. Live smokes remain one-at-a-time operator actions; the batch runner is a separate roadmap item.

## Hardening expectations

- Live network is always opt-in via env gates.
- `allowed_domains` is enforced in code, not just suggested.
- The `mock` fetcher returns a canned response and never touches the network — it's the test default.
- `requests` is the production live path; `firecrawl` is deferred.
- Saved raw payloads are byte-faithful; extracted payloads are deterministic given the raw input.
- The `agency_ois` extractor is **pure** (no I/O) and is the operator's first line of defence against a redesigned page template.
- The target generator is **pure**: lint + serialize, no network. Discovery and live smoke remain explicit, separate operator actions.
- The curated-URL scaffold is **pure**: lint + reshape, no network. Acquisition and review remain explicit, manual operator actions.
