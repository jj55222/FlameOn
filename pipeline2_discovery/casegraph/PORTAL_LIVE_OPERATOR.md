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

## Hardening expectations

- Live network is always opt-in via env gates.
- `allowed_domains` is enforced in code, not just suggested.
- The `mock` fetcher returns a canned response and never touches the network — it's the test default.
- `requests` is the production live path; `firecrawl` is deferred.
- Saved raw payloads are byte-faithful; extracted payloads are deterministic given the raw input.
- The `agency_ois` extractor is **pure** (no I/O) and is the operator's first line of defence against a redesigned page template.
