# Portal Replay Fixture Authoring Guide

This directory holds the offline portal-replay test inputs that drive
`--portal-replay` CLI runs and the integration tests in
`tests/test_casegraph_portal_replay_to_handoffs.py`,
`tests/test_casegraph_portal_replay_manifest.py`,
and `tests/test_casegraph_cli_portal_replay.py`.

A "portal fixture" is a saved JSON payload that mimics what a portal
page (police OIS, DA office, sheriff transparency portal, etc.) would
look like once parsed. The CaseGraph pipeline runs entirely offline
against these saved payloads — no live HTTP, no Firecrawl, no browser
automation. Adding a new fixture is the canonical way to extend
coverage of new portal shapes.

This guide walks through:

- the manifest entry schema (`portal_replay_manifest.json`)
- the agency-OIS payload shape (`tests/fixtures/agency_ois/*.json`)
- the alternative `source_records` payload shape
- which fields drive identity lock, claim extraction, artifact
  graduation, and protected-URL rejection
- how to compute and update `expected_*` counts
- common pitfalls

If anything in this guide diverges from the code, the code wins —
treat this as documentation, not contract.

---

## 1. Step-by-step: adding a new portal replay fixture

1. **Author a payload JSON** in either:
   - `tests/fixtures/agency_ois/<descriptive_name>.json` (agency-OIS
     incident-detail or listing page), or
   - `tests/fixtures/portal_replay/<descriptive_name>.json` (manifest
     `source_records` shape — used for non-agency portals such as a
     YouTube agency channel).

2. **Pick a unique `case_id`** (integer) that doesn't collide with
   any existing entry in `portal_replay_manifest.json`. Existing
   `case_id`s are 31, 32, 33, 34, 37 — use 38+ for new entries.

3. **Pick a `portal_profile_id`** that exists in
   `tests/fixtures/portal_profiles/portal_profiles.json`. The full
   list of legal IDs is loaded at test time; the canonical set
   includes `agency_ois_listing`, `agency_ois_detail`,
   `da_critical_incident`, `city_critical_incident`,
   `sheriff_critical_incident`, `court_docket_search`,
   `court_case_detail`, `foia_request_page`,
   `document_release_page`, `youtube_agency_channel`,
   `vimeo_agency_channel`, `documentcloud_search`,
   `muckrock_request`, `courtlistener_search`.

4. **Add a manifest entry** to `portal_replay_manifest.json`. See
   schema reference below.

5. **Compute the `expected_*` counts** by running the dry-replay
   harness once locally:
   ```bash
   .venv/Scripts/python.exe -m pytest \
     tests/test_casegraph_portal_replay_manifest.py::test_portal_replay_manifest_runs_and_matches_expectations -v
   ```
   The first run will fail with the actual counts produced by the
   executor — paste those into the manifest entry, then re-run.

6. **Add `notes`** describing what the fixture demonstrates (e.g.
   "agency OIS official page with YouTube embed and bodycam
   context"). The lint test refuses empty notes — they're how future
   contributors understand what each entry is for.

7. **Run the lint sweep** before opening a PR:
   ```bash
   .venv/Scripts/python.exe -m pytest \
     tests/test_casegraph_portal_fixture_lint.py \
     tests/test_casegraph_portal_replay_manifest.py \
     tests/test_casegraph_portal_replay_to_handoffs.py \
     tests/test_casegraph_cli_portal_replay.py -v
   ```

8. **Optional**: smoke-test the new entry through the operator CLI:
   ```bash
   .venv/Scripts/python.exe -m pipeline2_discovery.casegraph.cli \
     --portal-replay --portal-manifest-entry <new_case_id> \
     --emit-handoffs --json
   ```

---

## 2. Manifest entry schema

`portal_replay_manifest.json` is a single object with two top-level
keys:

```json
{
  "version": 1,
  "entries": [ ... ]
}
```

Each entry in `entries` is a JSON object with these fields:

| Field | Type | Required | Description |
|---|---|---|---|
| `case_id` | int | yes | Operator-facing identifier. Must be unique across the manifest. |
| `portal_profile_id` | str | yes | Must match a `profile_id` in `portal_profiles.json`. Determines the executor's expected fetcher / cap policy. |
| `mocked_payload_fixture` | str | yes | Repo-relative path to the saved JSON payload. The path must resolve relative to the repo root. |
| `expected_source_records` | int (≥0) | yes | Count of `SourceRecord` objects the executor should emit for this payload. |
| `expected_artifact_claims` | int (≥0) | yes | Count of `ArtifactClaim`s the claim extractor should surface from the executor's records. |
| `expected_candidate_urls` | int (≥0) | yes | Count of `possible_artifact_source` URLs that pass the resolver's safety preflight (i.e. would be candidates for graduation). |
| `expected_rejected_urls` | int (≥0) | yes | Count of media/document URLs rejected for protected/login/private patterns. |
| `expected_resolver_actions` | int (≥0) | yes | Count of resolver-action diagnostic strings emitted for this case (`candidate_ready_for_resolver:<url>` + `reject_protected_or_nonpublic:<url>`). Usually `expected_candidate_urls + expected_rejected_urls`. |
| `expected_blockers` | List[str] | optional | Recognized blocker codes that should appear in `result.blockers`. Today's only legitimate value is `protected_or_nonpublic` (and the test-only `mock_payload_missing`). |
| `notes` | List[str] | optional but expected non-empty | Human-readable description of what this entry demonstrates. The lint test enforces non-empty. |

The dataclass that backs this is `PortalReplayManifestEntry` in
`pipeline2_discovery/casegraph/portal_dry_replay.py`. Missing
required fields raise `TypeError` at load time. Extra unknown fields
also raise `TypeError`.

### Example

```json
{
  "case_id": 31,
  "portal_profile_id": "agency_ois_detail",
  "mocked_payload_fixture": "tests/fixtures/agency_ois/incident_detail_with_youtube_embed.json",
  "expected_source_records": 2,
  "expected_artifact_claims": 2,
  "expected_candidate_urls": 1,
  "expected_rejected_urls": 0,
  "expected_resolver_actions": 1,
  "expected_blockers": [],
  "notes": ["agency OIS official page with YouTube embed and bodycam context"]
}
```

---

## 3. Agency-OIS payload shape

A payload at `tests/fixtures/agency_ois/*.json` is consumed by
`AgencyOISConnector` (see `pipeline2_discovery/casegraph/connectors/agency_ois.py`).

| Field | Type | Required | Description |
|---|---|---|---|
| `page_type` | `"agency_listing"` \| `"incident_detail"` | yes | Selects whether the fixture represents an agency listing page or a single-incident detail page. |
| `agency` | str | yes | Free-form agency name (e.g. `"Phoenix Police Department"`). Drives the page-level record's `metadata.agency` and the manual router's jurisdiction inference. |
| `agency_url_root` | str | optional | Informational only; not currently consumed by the connector. |
| `url` | http(s) URL | yes | The page's canonical URL. |
| `title` | str | optional | Page title. Surfaces in the page-level `SourceRecord.title`. |
| `narrative` | str | optional | Page body text. Surfaces in `SourceRecord.snippet` / `raw_text` for identity / outcome scanning. |
| `subjects` | List[str] | optional but recommended | Defendant / decedent / suspect names. Drives identity lock — see §5. |
| `incident_date` | str | optional | ISO date string. Surfaces as a `matched_case_fields` anchor when present. |
| `case_number` | str | optional | Case identifier. Anchor for identity scoring. |
| `outcome_text` | str \| null | optional | Free-form outcome description (e.g. `"subject sentenced 2024"`). Surfaces in the page-level record's outcome scoring scan. |
| `media_links` | List[link] | optional | Media URLs (video, audio). See link shape below. |
| `document_links` | List[link] | optional | Document URLs (PDF, etc.). See link shape below. |
| `claims` | List[claim] | optional | Free-form release claims with no concrete URL — surfaces as `ArtifactClaim`s only. |

### Link shape (used in both `media_links` and `document_links`)

```json
{
  "url": "https://www.phoenix.gov/police/media/2024-OIS-014-briefing.mp4",
  "label": "Critical Incident Briefing video",
  "type": "bodycam_briefing"
}
```

| Field | Type | Required |
|---|---|---|
| `url` | http(s) URL | yes |
| `label` | str | yes (may be `""`) |
| `type` | str | yes |

### Recognized `type` hints (drive `artifact_type`)

The agency-OIS resolver's `LINK_TYPE_TO_ARTIFACT_TYPE` map — using
one of these values produces a known `artifact_type`:

| `type` value | maps to `artifact_type` |
|---|---|
| `bodycam_briefing`, `bodycam`, `bwc` | `bodycam` |
| `dashcam` | `dashcam` |
| `surveillance` | `surveillance` |
| `interrogation`, `police_interview` | `interrogation` |
| `court_video`, `sentencing_video`, `trial_video` | `court_video` |
| `dispatch_911`, `911_audio` | `dispatch_911` |
| `incident_report`, `incident_summary`, `ia_report`, `use_of_force_report`, `police_report`, `agency_document` | `docket_docs` |

Any string not in this map is accepted by the connector. The
resolver then falls back to URL-extension / format inference:

- `.mp4` / `.mov` / `.webm` / `.m3u8` or YouTube/Vimeo host →
  `format=video`, `artifact_type="other_video"`
- `.mp3` / `.wav` / `.m4a` → `format=audio`,
  `artifact_type="dispatch_911"`
- `.pdf` → `format=pdf`, `artifact_type="docket_docs"`

When in doubt, prefer a recognized hint; the lint will warn (not fail)
if an unknown hint is used.

### Claim shape

```json
{
  "text": "Body-worn camera footage will be released ...",
  "label": "release_pending"
}
```

| Field | Type | Required |
|---|---|---|
| `text` | str (non-empty) | yes |
| `label` | str | optional |

Claim text alone never graduates — that's the non-negotiable
`claim_source != possible_artifact_source` doctrine.

### Worked example

```json
{
  "page_type": "incident_detail",
  "agency": "Phoenix Police Department",
  "agency_url_root": "https://www.phoenix.gov/police",
  "url": "https://www.phoenix.gov/police/critical-incidents/2024-OIS-014",
  "title": "Critical Incident Briefing 2024-OIS-014",
  "narrative": "On 2024-05-12 officers responded ... Body-worn camera (BWC) footage is included in the briefing video below.",
  "subjects": ["John Example"],
  "incident_date": "2024-05-12",
  "case_number": "2024-OIS-014",
  "outcome_text": "subject sentenced 2024",
  "media_links": [
    {
      "url": "https://www.phoenix.gov/police/media/2024-OIS-014-briefing.mp4",
      "label": "Critical Incident Briefing video",
      "type": "bodycam_briefing"
    }
  ],
  "document_links": [],
  "claims": []
}
```

---

## 4. Alternative payload shape: `source_records`

Some portals (e.g. a YouTube agency channel) don't fit the agency-OIS
page shape. For those, the payload may instead contain a
`source_records` array of pre-built `SourceRecord` objects. The
canonical example is
`tests/fixtures/portal_replay/generic_youtube_weak_media.json`.

```json
{
  "portal_profile_id": "youtube_agency_channel",
  "source_records": [
    {
      "source_id": "portal_replay::generic_youtube",
      "url": "https://www.youtube.com/watch?v=genericWeak001",
      "title": "Community update video",
      "snippet": "...",
      "raw_text": "...",
      "source_type": "video_host",
      "source_authority": "media",
      "source_roles": ["possible_artifact_source"],
      "api_name": "youtube",
      "metadata": {
        "media_link_type": "video",
        "fixture_kind": "generic_weak_media"
      }
    }
  ]
}
```

When the executor sees `source_records`, it loads them directly as
`SourceRecord` objects and bypasses the agency-OIS connector. The
records still flow through the assembly pipeline (identity / outcome
/ claim extraction / metadata-only resolvers / score) when the CLI
or the integration harness builds a CasePacket from this payload.

---

## 5. Field semantics for downstream behavior

### What drives identity lock

For an assembled `CasePacket` to reach `identity_confidence == "high"`
(a prerequisite for PRODUCE), identity scoring needs:

- a `defendant_full_name` match (the page-level record carries
  `subjects[0]` as the defendant when the manual router is used)
- at least one jurisdiction anchor (city, county, or state) — the CLI
  derives the city from `agency` (stripping suffixes like `" Police
  Department"`) and pairs it with `AZ` as a placeholder
- at least one disambiguator: agency, incident_date, case_number,
  victim_name, or source_authority

Practical rule of thumb for a HIGH-identity fixture: include
`subjects` + `agency` + `case_number` + `incident_date`, and use
agency-OIS shape so the page record carries `source_authority="official"`.

### What creates artifact claims

The claim extractor runs over each `SourceRecord`'s combined
`title + snippet + raw_text`. Claims are produced from:

- agency-OIS `claims[]` items (text-only release language)
- `media_links[].label` strings containing release keywords (`"released"`,
  `"published"`, `"available"`, etc.)
- `narrative` text containing release language

Claims are diagnostic only; they never graduate into
`verified_artifacts`.

### What allows verified artifact graduation

A media or document URL graduates into `verified_artifacts` when:

- it appears in `media_links[]` or `document_links[]`
- the URL is concrete (http or https) and not protected (see below)
- the source has the `possible_artifact_source` role (the connector
  assigns this to every link record, but never to the page record or
  claim records)
- the `type` hint maps to a known `artifact_type` via
  `LINK_TYPE_TO_ARTIFACT_TYPE`, OR the URL extension / host yields a
  recognized format

### What triggers protected/private rejection

The agency-OIS connector flags any link URL containing one of these
case-insensitive markers as `protected_or_nonpublic`:

- `login`, `signin`, `auth`, `token=`, `session=`
- `/private/`, `/restricted/`
- `pacer`

The resolver then refuses to graduate flagged URLs. Protected URLs
are legitimate in fixtures — they exercise the rejection path. When
adding such a fixture, set `expected_rejected_urls >= 1` and add
`"protected_or_nonpublic"` to `expected_blockers`.

---

## 6. Computing and updating `expected_*` counts

The simplest path:

1. Add the fixture file and a draft manifest entry with all
   `expected_*` set to `0`.
2. Run:
   ```bash
   .venv/Scripts/python.exe -m pytest \
     tests/test_casegraph_portal_replay_manifest.py::test_portal_replay_manifest_runs_and_matches_expectations -v
   ```
3. Read the failure message, which reports the actual counts the
   executor produced.
4. Paste those counts into the manifest entry.
5. Re-run the test until green.

For agency-OIS payloads, the typical counts are:

- `expected_source_records` = 1 (page record) + len(media_links) +
  len(document_links) + len(claims)
- `expected_artifact_claims` = len(claims) + claim-language hits in
  `media_links[].label` and `narrative`
- `expected_candidate_urls` = number of media/document links that
  are NOT protected
- `expected_rejected_urls` = number of media/document links that ARE
  protected
- `expected_resolver_actions` = expected_candidate_urls +
  expected_rejected_urls

These are heuristics — always verify against the actual executor
output.

---

## 7. CLI examples

```bash
# Run by manifest case_id (recommended for known cases)
.venv/Scripts/python.exe -m pipeline2_discovery.casegraph.cli \
  --portal-replay --portal-manifest-entry 31 \
  --emit-handoffs --json

# Run by direct fixture path (useful for new fixtures not yet in
# the manifest)
.venv/Scripts/python.exe -m pipeline2_discovery.casegraph.cli \
  --portal-replay \
  --fixture tests/fixtures/agency_ois/incident_detail_with_bodycam_video.json \
  --emit-handoffs --json

# Combine with --bundle-out to capture a full run bundle. The bundle
# carries the same portal_replay metadata as the JSON output.
.venv/Scripts/python.exe -m pipeline2_discovery.casegraph.cli \
  --portal-replay --portal-manifest-entry 31 \
  --emit-handoffs \
  --bundle-out autoresearch/.runs/portal_31_bundle.json \
  --json
```

`--portal-replay` rejects http(s) URLs as the `--fixture` value with
exit code 5 (`EXIT_LIVE_BLOCKED`). It will never fetch — pass a saved
fixture path or a manifest case_id.

---

## 8. Common pitfalls

- **Duplicate `case_id`** — silently overwrites in the dry-replay's
  internal payload-by-case_id map. The lint test catches this.
- **Bad `portal_profile_id`** — typo not in `portal_profiles.json`'s
  loaded set. Lint catches this.
- **Missing `type` hint on a link** — the lint refuses empty
  strings. Consider whether a recognized hint applies; if not, the
  resolver falls back to URL-extension / format inference.
- **Protected URLs accidentally expected to graduate** — if a media
  link URL contains `login`/`signin`/`auth`/`token=`/`session=`/
  `/private/`/`/restricted/`/`pacer`, it WILL be rejected. Set
  `expected_rejected_urls >= 1` and add `"protected_or_nonpublic"`
  to `expected_blockers`. Don't expect a protected URL to appear in
  `expected_candidate_urls`.
- **Claim-only payloads expected to create P3 rows** — claim text
  without a concrete URL never graduates. P3 row count for a
  claim-only fixture is `0`. Set `expected_candidate_urls` to `0`
  and don't expect bodycam media in `verified_artifacts`.
- **Document-only fixtures expected to PRODUCE** — a fixture with
  only `document_links` (e.g. a PDF) graduates the document but
  doesn't unlock PRODUCE. Verdict will be HOLD (or SKIP), not
  PRODUCE — that's the media gate doctrine, not a bug.
- **Live URLs in `--fixture`** — the CLI refuses any value starting
  with `http://` or `https://` in `--portal-replay` mode. Use a
  saved fixture path; never paste a live URL.
- **Notes left empty** — the lint refuses empty `notes` arrays.
  Future contributors need a one-line explanation of what each entry
  demonstrates.

---

## 9. Out of scope for this guide

- Live HTTP fetching (deferred until offline coverage is broader)
- Firecrawl execution (deferred — `firecrawl_safety.py` is the
  preflight diagnostic only; no fetcher is wired)
- Browser automation
- Adding new `portal_profile_id` entries — that's a separate change
  to `tests/fixtures/portal_profiles/portal_profiles.json` and the
  `REQUIRED_PROFILE_IDS` set in `portal_profiles.py`
- Adding new blocker codes — extend `KNOWN_BLOCKER_CODES` in the
  fixture lint test if you intentionally introduce one

If you need to do any of the above, that's a separate PR with its
own scope review.
