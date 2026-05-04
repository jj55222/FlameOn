# FlameOn — CaseGraph (P2) Handoff for the Next Claude Code Session

**Last updated:** 2026-05-04
**Author:** Claude Code (Opus 4.7), session ending after PR #21 merged
**Repo:** `C:\FlameON\FlameOn-main` on Windows (mingw bash + PowerShell)
**Working dir for new sessions:** `C:\FlameON\FlameOn-main\autoresearch`
**Canonical work branch:** `p2-casegraph-rebuild`

---

## What this doc is

A pragmatic handoff. Read it once before touching anything — it captures the durable patterns, doctrine, and tooling state from the current chain of P2 CaseGraph PRs. It is **not** a substitute for the in-tree docs:

- `SYSTEM.md` — full 5-pipeline architecture overview.
- `autoresearch/CLAUDE.md` — auto-loaded project instructions for Claude.
- `pipeline{1..5}_*/CLAUDE.md` — per-pipeline context.
- `pipeline2_discovery/casegraph/README.md` — the CaseGraph package reference.
- `tests/fixtures/portal_replay/README.md` — operator guide for authoring portal replay fixtures (PR #13).

This doc adds: **what the previous session learned about working in this repo with this user**.

---

## Where you are right now

```
branch:        p2-casegraph-rebuild  (the canonical merge target for P2 PRs)
HEAD:          2e2f1b1 Merge pull request #21 from jj55222/p2-casegraph-portal-live-phoenix-cib-target
sync:          0/0 with origin/p2-casegraph-rebuild
working tree:  clean
```

There is one in-flight feature branch:
- **`p2-casegraph-phoenix-newsroom-extractor`** at commit `b595331` — adds a Phoenix Newsroom HTML extractor + dispatch. Pushed to origin. Not yet PR'd at handoff time. Inspect with `git log p2-casegraph-phoenix-newsroom-extractor` before assuming anything.

If the user asks "what was I working on?", that's the answer.

---

## PR ledger (P2 CaseGraph rebuild)

Each PR is **single-purpose, small-blast-radius, on a dedicated feature branch**, merged into `p2-casegraph-rebuild`. The flow shape is meaningful — read the chain in order if you need to understand "why is the code like this?":

| #  | Branch | Theme |
|----|--------|-------|
| 2  | `p4-p5-step5-work` | Earlier P4/P5 baseline merge (touch with caution; out of P2 scope) |
| 3  | `p2-casegraph-orchestrator-assembly` | Wired `run_metadata_only_resolvers` into assembly (was MuckRock-only) |
| 4  | `p2-casegraph-agency-ois-orchestrator` | Added `agency_ois` resolver to orchestrator's `RESOLVER_NAMES` |
| 5  | `p2-casegraph-outcome-gate-advisory` | Made the outcome hard gate optional (`P2_OUTCOME_GATE` env, default OFF). Outcome is now advisory by default |
| 6  | `p2-casegraph-cli-handoffs` | Added `--emit-handoffs` to the CLI; surfaces `p2_to_p3` / `p2_to_p4` / `p2_to_p5` |
| 7  | `p2-casegraph-cli-golden-smoke` | First subprocess black-box CLI smoke suite |
| 8  | `p2-casegraph-portal-replay-handoffs` | Offline portal-replay → handoffs integration test harness |
| 9  | `p2-casegraph-cli-portal-replay` | `--portal-replay --fixture <path>` CLI mode (offline) |
| 10 | `p2-casegraph-portal-manifest-entry` | `--portal-manifest-entry <case_id>` convenience flag |
| 11 | `p2-casegraph-portal-replay-bundle-metadata` | `--bundle-out` carries `portal_replay` section |
| 12 | `p2-casegraph-handoff-advisory-consistency` | Routed fresh `ActionabilityResult` advisories into P4/P5 handoffs (kept scoring pure; adapters now accept optional `score_result=` kwarg) |
| 13 | `p2-casegraph-portal-fixture-authoring-kit` | README + manifest lint + agency_ois fixture lint |
| 14 | `p2-casegraph-sheriff-bodycam-portal-fixture` | First non-Phoenix-PD fixture (Maricopa County Sheriff's Office bodycam) |
| 15 | `p2-casegraph-portal-replay-identity-enrichment` | Enriched `case_identity` from agency_ois page metadata (lifts identity past MEDIUM) |
| 16 | `p2-casegraph-score-verdict-coherence` | Surface fresh score verdict in JSON output and P5 handoff |
| 17 | `p2-casegraph-stale-router-flag-filter` | Filter stale router-default risk flags from result/export views |
| 18 | `p2-casegraph-portal-live-fetch-skeleton` | `--portal-live` scaffolding (mock fetcher only, **no real network**) |
| 19 | `p2-casegraph-portal-live-requests-fetcher` | First real `requests`-based fetcher + HTML marker extractor |
| 20 | `p2-casegraph-portal-live-fetch-only-mode` | `require_extraction=false` mode |
| 21 | `p2-casegraph-portal-live-phoenix-cib-target` | First real Phoenix PD CIB target fixture for manual fetch-only smoke |

> **Note for future you:** PRs #15–21 happened in a different session than the one that wrote this doc. The summaries above are from commit titles only; if you need detail, `git show <sha>` and read the body. PRs #3–14 are described in detail through the conversation history in the ChatGPT/Claude transcript that produced this handoff.

The chain went **offline-only → controlled live fetch with HTML extraction**. PR #18 was the doctrinal turning point.

---

## Doctrine that survived the whole chain

These rules are encoded in code + tests. Don't relax them without an explicit doctrine PR with the user's approval.

### CasePacket flow

```
CaseInput
  → query_planner
  → connector.collect → SourceRecord[]
  → resolver(packet) → VerifiedArtifact[]
  → resolve_identity / resolve_outcome / extract_artifact_claims
  → score_case_packet → ActionabilityResult (PRODUCE | HOLD | SKIP)
  → export_p2_to_p{3,4,5}
```

### Hard rules (never relax silently)

- **`score_case_packet` is pure.** It does NOT mutate `packet.risk_flags` / `packet.next_actions`. If you need fresh advisory data downstream, route it through the `score_result=` kwarg on `export_p2_to_p4` / `export_p2_to_p5` (PR #12 pattern). The contract test in `tests/test_casegraph_contracts.py::test_score_case_packet_remains_pure_after_handoff_export` enforces this.
- **`claim_source != possible_artifact_source`.** Claim text alone never graduates into a `VerifiedArtifact`. The only thing that creates a `VerifiedArtifact` is a resolver that finds a concrete public URL on a source whose roles include `possible_artifact_source`.
- **No live HTTP** anywhere except inside the explicit `--portal-live` path (PR #19+) gated by `FLAMEON_RUN_LIVE_CASEGRAPH=1` AND `FLAMEON_RUN_LIVE_PORTAL_FETCH=1`. The default-OFF stance is non-negotiable. Tests have a `requests.Session.get` monkeypatch guard pattern — preserve it.
- **No Firecrawl execution** in production code. There is `firecrawl_safety.py` for safety preflight diagnostics; that's it.
- **Protected/login/private/token/auth/PACER URLs are rejected** by every resolver. Risk flag: `protected_or_nonpublic` or `pacer_or_paywalled`. Don't let them graduate.
- **Outcome is advisory by default** (PR #5). Strict mode: `P2_OUTCOME_GATE=1`. The `is_outcome_gate_enabled()` helper reads env at call time so monkeypatch tests work cleanly. Don't replace it with a module-level constant.
- **Handoff schemas are stable.** `schemas/p2_to_p{3,4,5}.schema.json`. Adding new fields requires a schema change PR with the user's explicit approval.

### Verdict gate (current)

A packet PRODUCEs iff:
- `identity_confidence == "high"`
- `len(media_artifacts) >= 1`
- `production_score >= 70`
- no severe risks (`weak_identity`, `protected_or_nonpublic_only`, `identity_unconfirmed`, `artifact_unverified`, `conflicting_jurisdiction`)
- AND **only when `P2_OUTCOME_GATE=1`** also requires `outcome_status ∈ {sentenced, closed, convicted}`

Default mode emits advisory signals (`outcome_not_concluded_advisory`, `produce_with_pending_outcome`) for non-concluded outcomes without blocking PRODUCE.

---

## Working patterns / collaboration style

This is the most important section. The user has a very deliberate cadence. Match it.

### The PR loop

```
1. User asks for inspection (no code)
2. You inspect, propose a small implementation plan, list approved files
3. User approves (often with corrections / scope tightening)
4. You implement the smallest version that meets the spec
5. Run targeted tests + full CaseGraph sweep
6. If any test outside the approved files breaks → STOP and report
7. Consolidate auto-commits into one semantic commit
8. Report: commit SHA, git show --stat, scope confirmation, test results
9. User says "push" — you push the branch, open a PR via gh
10. User merges (you don't merge yourself)
11. Repeat
```

### Hard collaboration rules

- **Inspection-only first.** When the user says "inspect" or "do not code", you do NOT touch any file. Report the plan, get approval, then implement.
- **Stop and report on cascade.** If a test outside the approved file list fails because of your change, you do not fix it silently. You stop, surface it, and ask whether to expand scope. The user will say yes ~always, but they want to make the call.
- **Single semantic commit per PR.** The repo has an auto-commit hook that fires on every Edit/Write (creating "auto: HH:MM" commits). At the end of an implementation, soft-reset to `origin/<base-branch>` and create one descriptive commit:
  ```
  git reset --soft origin/p2-casegraph-rebuild
  git commit -m "$(cat <<'EOF'
  P2 CaseGraph: <one-line summary>

  <multi-paragraph body explaining intent, doctrine, files touched,
  test coverage, non-goals>

  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  EOF
  )"
  ```
  Reflog preserves the auto-commit history for recovery.
- **Auto-commit hook details.** It runs `git add -A` after Write/Edit. Before any write, run `git status --short`; if dirty (with files you didn't touch), stop and report.
- **Push routine.**
  ```
  git push origin HEAD:<descriptive-branch-name>
  gh pr create --base p2-casegraph-rebuild --head <branch> --title "..." --body "$(cat <<'EOF' ... EOF)"
  ```
- **Branch naming.** Always `p2-casegraph-<scope>` for P2 work. Never push to `main` or `p2-casegraph-rebuild` directly.
- **Never merge yourself.** Push the branch and report the PR URL. The user merges.
- **Never use `--no-verify`, `--force-push`, or skip hooks** unless the user explicitly asks. If a pre-commit hook fails, do NOT amend; create a new commit after fixing the issue.
- **`tests/ -k casegraph -x`** is the canonical green-bar check before reporting. Targeted suites first, full sweep last.

### PR body shape

The user has a consistent PR body template:
```markdown
## Summary
- 3–5 bullets

## Behavior
- ✅ what works now

## Files changed (N)
- file paths

## Tests
- per-suite counts + delta vs baseline

## Non-goals
- ❌ what was explicitly NOT changed (this is critical — protects scope)

## Test plan
- [x] checked items
```

Always include the "Non-goals" section. The user uses it as a reading aid.

### Branch hygiene

- After a PR merges, the user expects the local `p2-casegraph-rebuild` to be reset to origin (via `git fetch && git reset --hard origin/p2-casegraph-rebuild`) before starting the next inspection.
- Local feature branches accumulate; that's fine. Reflog handles recovery.

---

## Tooling state

- **Python:** `.venv\Scripts\python.exe` (Windows path) — pinned to 3.12.x. **Always** invoke via this path; do not rely on `python` resolving correctly.
- **`gh` (GitHub CLI):** **installed and authenticated** as of 2026-05-04. Version 2.92.0. Path: `/c/Program Files/GitHub CLI/gh.exe`. In a fresh Claude Code shell, plain `gh` works after the parent's PATH refresh. Auth is in the OS keyring as `jj55222`.
- **Test runner:** `pytest`. The `casegraph` keyword filter (`pytest tests/ -k casegraph -x`) is the standard fast-feedback sweep.
- **Schemas:** `schemas/p2_case_packet.schema.json`, `schemas/p2_to_p3.schema.json`, `schemas/p2_to_p4.schema.json`, `schemas/p2_to_p5.schema.json`. Validated via `jsonschema` (already in the venv).
- **Live env gates:** `FLAMEON_RUN_LIVE_CASEGRAPH=1` AND `FLAMEON_RUN_LIVE_PORTAL_FETCH=1` for any portal-live work. Both must be set; either alone is rejected.
- **Brave / Firecrawl billing guards** in `pipeline2_discovery/brave_guard.py` and `pipeline2_discovery/casegraph/firecrawl_safety.py`. Default-OFF. Don't disable.

---

## Known follow-ups (deferred during the chain)

- **`--portal-manifest-path` override flag.** Today the manifest path is hardcoded to `tests/fixtures/portal_replay/portal_replay_manifest.json`. Adding an override is a small follow-up.
- **`--defendant-name` / `--jurisdiction` override flags** for `--portal-replay`. Currently the bridge derives them from the payload.
- **Bundle output schema** — there is no `.schema.json` file for the run bundle today. Operators rely on `REQUIRED_BUNDLE_KEYS` in `tests/test_casegraph_run_bundle.py` as the de-facto contract. If you add bundle-shape constraints, consider adding a schema.
- **`agency_ois` artifact_type naming inconsistency** — the resolver returns `"dashcam"` (no underscore) and `"surveillance"` (no `_video` suffix), while scoring's `MEDIA_ARTIFACT_TYPES` uses `"dash_cam"` and `"surveillance_video"`. Format-based fallback masks this today, but a future cleanup PR could align them.
- **The hardcoded `(31, 32, 33, 34, 37)` list** in `test_casegraph_portal_replay_to_handoffs.py::test_portal_replay_harness_makes_zero_network_calls`. Should auto-expand when manifest entries are added (case 38 is currently NOT in the network-isolation guard list).
- **`portal_replay.manifest_path`** is repo-relative. If the manifest ever moves, that lookup needs updating.
- **More agency variants.** The corpus is heavily Phoenix-PD weighted (8 of 9 agency_ois fixtures); only one sheriff variant exists (case 38). DA / city / court variants would diversify identity and outcome paths.

---

## Where to start the next session

1. **Read this file (`HANDOFF.md`) and `autoresearch/CLAUDE.md`.** Both auto-load.
2. **Sync to latest:**
   ```bash
   cd /c/FlameON/FlameOn-main
   git fetch origin --prune
   git checkout p2-casegraph-rebuild
   git reset --hard origin/p2-casegraph-rebuild
   ```
3. **Confirm clean tree + green sweep before any work:**
   ```bash
   git status --short        # must be empty
   .venv/Scripts/python.exe -m pytest tests/ -k casegraph -x
   ```
4. **Check the in-flight Phoenix Newsroom extractor branch:**
   ```bash
   git log origin/p2-casegraph-phoenix-newsroom-extractor
   ```
   Ask the user whether to PR it, continue it, or shelve it.
5. **Wait for the user's next prompt.** Do not start work without an explicit task. The user opens with "Next P2 task: ..." and that's your cue.

### Sanity-check commands you'll use a lot

```bash
# Where am I?
git log -1 --oneline && git status --short && git rev-list --left-right --count p2-casegraph-rebuild...origin/p2-casegraph-rebuild

# Quick CLI smoke (offline, no env gate)
.venv/Scripts/python.exe -m pipeline2_discovery.casegraph.cli \
    --fixture tests/fixtures/casegraph_scenarios/media_rich_produce.json --emit-handoffs --json | head -40

# Quick portal-replay smoke (offline)
.venv/Scripts/python.exe -m pipeline2_discovery.casegraph.cli \
    --portal-replay --portal-manifest-entry 31 --emit-handoffs --json | head -40

# Open a PR end-to-end (after a single semantic commit on a feature branch):
git push origin HEAD:p2-casegraph-<scope>
gh pr create --base p2-casegraph-rebuild --head p2-casegraph-<scope> \
    --title "P2 CaseGraph: <summary>" \
    --body "$(cat <<'EOF'
## Summary
...
EOF
)"
```

---

## Things the previous session learned the hard way

- **The auto-commit hook will fire on every Edit/Write.** Don't fight it; consolidate at the end with `git reset --soft origin/<base>` + a single semantic commit. Reflog preserves the auto-commits.
- **A failed test outside the approved file list is always a stop-and-report moment.** The user has approved scope expansions every time so far, but they want to be asked. Don't fix outside-scope tests silently.
- **Predictions about identity/outcome/scoring counts are often wrong.** The README §6 workflow ("run the manifest test, paste actual counts") is the right pattern. Trust the executor over your prediction.
- **Test-only first, production-code-after.** Several PRs (#7 golden smoke, #8 portal-replay-to-handoffs harness, #13 fixture lint) landed as test-only infrastructure first, then a CLI/operator surface PR followed (#9, #10, #18+). This pattern reduces blast radius and gives a regression net before the operator surface ships.
- **Doctrine changes need their own PR.** The outcome-gate-advisory change (PR #5) was scoped narrowly to the verdict logic; resolver changes (PR #3, #4) were separate; handoff consistency (PR #12) was separate. Don't bundle doctrine shifts into feature work.
- **`route_manual_defendant_jurisdiction` is the bridge into manual-input mode.** It only sets `defendant_names` + `jurisdiction`; PR #15 added agency_ois-page-metadata enrichment to lift identity above MEDIUM for portal cases. Other input shapes (YouTube weak input, structured rows) have their own assemblers.
- **Test fixture totals are sometimes hardcoded** (`test_connector_yields_count_matches_fixtures`). When you add a fixture to `tests/fixtures/agency_ois/`, expect a count-bump cascade in that test. The README at `tests/fixtures/portal_replay/README.md` should call this out (it didn't as of PR #14; if you add a new fixture, consider updating the README too).
- **Schema enums are unconstrained for `risk_flags` / `next_actions` / `source_quality_notes`.** This is by design — advisory signals can be added without schema bumps.
- **The user prefers terse PR titles + rich PR bodies.** Title: "P2 CaseGraph: \<verb-phrase\>". Body: structured per the template above.
- **The user's instructions sometimes truncate mid-sentence in the chat UI.** When an instruction looks incomplete (ends mid-codeblock, mid-list, mid-paragraph), stop and ask for the rest before guessing.

---

## What "done" looks like for P2 (the user's stated goal)

```
P2 current win:    non-MuckRock artifacts graduate during assembly. ✅ shipped via PR #3, #4
Default doctrine:  outcome status is advisory.                         ✅ shipped via PR #5
Operator surface:  CLI emits handoffs and bundles.                     ✅ shipped via PR #6, #7, #11
Portal replay:     offline path proven and operator-usable.            ✅ shipped via PR #8, #9, #10
Authoring kit:     fixture authoring guide + lint.                     ✅ shipped via PR #13
Live fetch:        controlled, gated, with HTML extraction.            🚧 in progress via PR #18-21 + Phoenix Newsroom branch
```

The next big arc is **completing the live-fetch story** (extractors per portal type, then graduation of live-fetched artifacts through the same chain) without compromising any of the doctrine above.

---

## Final note from the previous Claude

The user is a careful operator. They will:
- Stop you and ask "are you sure?" when you propose something they don't fully understand.
- Approve scope expansions when justified, but always want to be asked.
- Reject relaxing doctrine without an explicit doctrine PR.
- Notice when a prediction was wrong (and expect you to notice it too).
- Not push or merge for you. They merge after the PR opens and CI is green.

Match their cadence. Inspect first, plan small, execute clean, report exactly. The chain of 21 PRs in this rebuild was built on that pattern — keep it.

Good luck.
