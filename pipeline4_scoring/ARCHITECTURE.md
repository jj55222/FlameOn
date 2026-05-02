# Pipeline 4 Architecture Notes

Companion to `CLAUDE.md` (the spec). This file documents internal
contracts and non-obvious design decisions that aren't visible from
the spec alone — read it before changing module boundaries.

## Two-pass scoring flow

```
score_case(merged, weights, case_research, pass1_backend, pass2_backend)
  │
  ├─ Pass 1 — Gemini 3.1 Flash Lite (max_tokens P4_PASS1_MAX_TOKENS, default 16000)
  │     Structural extraction → JSON with timeline / moments[≤60] /
  │     contradictions / speaker_dynamics / emotional_arc /
  │     factual_anchors / detected_structure_hint
  │
  ├─ scoring_math.compute_all() — PURE PYTHON, deterministic
  │     moment_density_score   ← weighted moments/min ÷ REFERENCE_DENSITY × 60
  │     arc_similarity_score   ← detected_arc match ÷ best pattern × 100
  │                             (returns None when no arc data)
  │     artifact_completeness  ← best subset-fit ÷ artifact_value combos × 100
  │     uniqueness_score       ← distinct moment types + bonuses (cap 80)
  │     combine() blends with weights (0.40, 0.30, 0.20, 0.10);
  │              when any subscore is None, substitutes the per-subscore
  │              floor from SUBSCORE_MISSING_FLOOR (no weight
  │              redistribution — see "Missing-evidence penalty policy
  │              in combine" below)
  │     decide_verdict() applies env-tunable PRODUCE/SKIP gates
  │
  ├─ Pass 2 — Qwen 3.6 Plus (max_tokens P4_PASS2_MAX_TOKENS, default 3000)
  │     Narrative judgment → final verdict + final_moments[5–12] +
  │     content_pitch + arc_recommendation + reasoning_summary
  │     Validator: anti-hallucination guard drops final_moments whose
  │     (source_idx, timestamp) doesn't match a Pass 1 entry within ±2s.
  │
  ├─ Reconcile (pipeline4_score.py:score_case)
  │     SKIP × PRODUCE → HOLD            (no big swings allowed)
  │     PRODUCE × SKIP → HOLD            (no big swings allowed)
  │     PRODUCE × HOLD + P4_TRUST_DETERMINISTIC_PRODUCE=1 → PRODUCE
  │     else → LLM verdict
  │
  └─ Resolution gate (pipeline4_score.py:score_case, env-gated)
        Reads P4_RESOLUTION_GATE (default 0 = OFF).
        Resolves resolution_status via 4-tier priority chain:
          case_research → resolution_labels.json → pass1.resolution_status_hint → "missing"
        Pass 2 may downgrade only — never upgrade — if it emits a
        more restrictive resolution_status.
        Caps the reconciled verdict at the per-status ceiling:
          confirmed_final_outcome → PRODUCE allowed
          charges_filed_pending   → max HOLD
          ongoing_or_unclear      → max SKIP
          missing                 → max SKIP
        Records pre_gate_verdict + gate_applied flag in metadata.
        See "Critical contract 5: Resolution gate" below.
```

## Mutability boundaries

| File | Mutability | Why |
|---|---|---|
| `evaluate.py` | **IMMUTABLE** | The fixed rubric every experiment is measured against. Header self-documents. |
| `calibration_data.json` | **IMMUTABLE** | The 10-case ground truth (9 winners + 1 SFDPA SKIP). |
| `resolution_labels.json` | mutable, manually maintained | Per-case `resolution_status` backfill consumed by the resolution gate as a fallback when `case_research` lacks the field. Loaded fresh per `score_case` call. Schema-versioned via top-level `_schema_version`. |
| `pipeline4_score.py` | mutable | Orchestration + reconciliation rules + observability |
| `prompts.py` | mutable | Pass 1 + Pass 2 templates |
| `scoring_math.py` | mutable, env-tunable via `P4_*` vars | Numeric layer |
| `transcript_loader.py` | mutable but stable | Multi-source merge + LLM formatting |
| `llm_backends.py` | mutable but stable | OpenRouter facade + JSON cleanup + truncation repair |

## Env-tunable knobs

| Variable | Default | What it controls |
|---|---|---|
| `P4_PASS1_MODEL` | `google/gemini-3.1-flash-lite-preview` | Pass 1 model id |
| `P4_PASS2_MODEL` | `qwen/qwen3.6-plus` | Pass 2 model id |
| `P4_PASS1_MAX_TOKENS` | `16000` | Pass 1 output cap; was 8000 — bumped because high-density Pass 1 outputs exceeded that and silently truncated. |
| `P4_PASS2_MAX_TOKENS` | `3000` | Pass 2 output cap; comfortably above the ~900-token max output. |
| `P4_NEAR_CAP_WARN_FRAC` | `0.9` | Threshold for emitting a near-cap warning during runs. |
| `P4_REFERENCE_DENSITY` | `0.02` (V9b) | weighted moments/min normalized to this → 60 |
| `P4_PRODUCE_SCORE_THRESH` | `40` (V9b) | min narrative_score for PRODUCE |
| `P4_PRODUCE_DENSITY_THRESH` | `20` (V9b) | min moment_density for PRODUCE |
| `P4_SKIP_SCORE_THRESH` | `15` (V9b) | below this → SKIP |
| `P4_TRUST_DETERMINISTIC_PRODUCE` | `0` | when 1, math-says-PRODUCE wins over LLM-says-HOLD |
| `P4_RESOLUTION_GATE` | `0` | OPTIONAL conservative mode. When `1`, applies a verdict ceiling based on `resolution_status` (see "Critical contract 5" below). Default OFF — the default scoring path treats resolution status as advisory metadata only, surfaced via the `production_status_flag` field. |
| `P4_PARALLEL` | `1` | concurrent cases in evaluate.py |

## Critical contracts (DON'T BREAK)

### 1. The per-source-idx hack in `evaluate.adapt_winner_to_merged`

This is the discriminator that lifted V6 from 35.50 → 71.44. It's
load-bearing.

`adapt_winner_to_merged` ingests a winner profile and produces a
merged-transcript dict that:

- has **N source entries** (one per artifact in the winner's
  `artifact_combination`)
- tags **all transcript segments with `source_idx=0`**, even though
  there are N source entries
- populates `available_evidence_types` from the winner profile's
  `artifact_combination`, not from segment groupings

The hack works because the only consumer of "what artifacts exist
for this case" is `available_evidence_types`. Pass 2's prompt reads
it directly (`AVAILABLE ARTIFACTS:` line). `compute_all`'s
`available_artifacts` set is built from it in `score_case`. Neither
re-derives artifacts by walking segment-source groupings.

**The contract**: `available_evidence_types` is the canonical source
of truth for what artifacts a case has, independent of segment
groupings.

A future change that violates this contract — e.g. making
`format_for_llm` derive evidence types from segments, or making
`score_case` re-walk sources to build the artifact set — collapses
the artifact_completeness_score back to the partial-credit fallback
and we lose ~35 points on every winner.

**Guardrail**: `tests/test_adapter_contracts.py` pins this contract.
If those tests start failing, investigate before "fixing".

### 2. Pass 2 anti-hallucination guard

`validate_pass2` drops `final_moments` whose `(source_idx, timestamp_sec)`
doesn't match any Pass 1 entry within ±2s. This prevents Pass 2
from inventing moments (a real failure mode with smaller models).

If you change this tolerance, run `evaluate.py` against the full
calibration set first — the 2s tolerance was tuned against winner
transcripts and gives the best precision/recall trade.

### 3. Reconciliation never allows verdict swings

`SKIP ↔ PRODUCE` always lands at `HOLD`. This bounds the damage from
either Pass 2 going off the rails or scoring math hitting an edge
case. Don't relax this rule without a documented experiment.

### 4. Missing-evidence penalty policy in `combine`

The narrative scorer is editorially ruthless: a case with missing
narrative-critical evidence MUST score lower than an otherwise-
identical case where that evidence is present. We do NOT redistribute
the weight of a missing subscore onto the remaining subscores —
that would silently let weak cases score as well as complete cases.

Two distinct missing-states are handled differently:

| State | What it means | Policy | Implementation |
|---|---|---|---|
| **Severe / case-intrinsic** | The case itself lacks the data | Subscore returns numeric **0** | Pass 1 found no detected_structure → `arc_similarity_score=0`; case has no available artifacts → `artifact_completeness_score=0` (partial-credit fallback); no extracted moments → `moment_density_score=0` and `uniqueness_score=0` |
| **Moderate / reference-data missing** | We can't measure (e.g. no P1 weights loaded) | Subscore returns **None** → `combine()` substitutes `SUBSCORE_MISSING_FLOOR` | No P1 `arc_patterns` → `arc_similarity_score=None` → floor 15; no P1 `artifact_value` → `artifact_completeness_score=None` → floor 20 |

Floors are punitive but non-annihilating. Total annihilation (full
weight × 0) is reserved for the severe case-intrinsic path; the
moderate path keeps cases from being totally suppressed when running
P4 without `--weights` for a sanity-check pass.

`SUBSCORE_MISSING_FLOOR` is module-level in `scoring_math.py` so the
policy is auditable and tunable. To experiment with stricter policies
(e.g., floor=0 for arc) without editing the module constant, pass a
custom `missing_floors` dict to `combine()`.

The TSV writer (`append_batch_summary`) and the per-case debug print
both render the substituted floor explicitly — missing-critical
penalties are **visible** in run output (e.g. `arc=MISS:floor=15`)
rather than hidden as `n/a`.

If you add a new subscore that can return `None`, you MUST add a
matching entry to `SUBSCORE_MISSING_FLOOR`. The
`test_subscore_missing_floor_constants_have_expected_keys` test
fails fast if the dict goes out of sync with `combine()`'s reads.

#### Why this matters editorially

A case missing arc reference data + missing artifact reference data,
with otherwise strong density and uniqueness, lands ~30–35 points
below an identical case with full reference data. That's the design:
the scorer favors cases that can actually become finished, evidence-
backed episodes — not cases that *might* be promising if we had more
data.

Out of scope for this version: explicit subscores for missing
source-backed chronology / offender-victim identity. These are critical
signals per the editorial spec but currently land implicitly
(chronology in moment_density, identity in factual_anchors →
uniqueness). Promoting them to explicit subscores is a structural
change for a future pass.

Resolution status is now an explicit signal — primarily as an
ADVISORY FLAG (default doctrine), with an optional verdict-ceiling
GATE available for conservative workflows. See "Critical contract 5"
below.

### 5. Resolution gate (optional) and advisory flag (default)

**Default doctrine — advisory only.** Resolution / sentencing / final
adjudication is recorded as ADVISORY metadata on the verdict. It does
NOT alter `narrative_score`, the deterministic verdict, the LLM
verdict, the reconciliation, or the emitted final verdict. The default
scorer measures narrative quality / winner-likeness ("is this a
strong story?"), NOT court-disposition production clearance. The
advisory output is the `production_status_flag` field — emitted on
every verdict, no env var needed.

**Why advisory and not a hard cap by default?** The calibration set
is composed of known YouTube winners — emulation targets. A pending
case can be a narrative winner exactly the same way a sentenced case
can. Pulling sentencing into pass/fail would conflate two unrelated
signals (story quality + court status). The advisory split keeps them
distinct: `narrative_score` / `verdict` says "is this a strong
story?"; `production_status_flag` says "what's the production
caveat?". The producer / Pipeline 5 reads the flag and decides.

**Optional gate (`P4_RESOLUTION_GATE=1`).** A verdict-ceiling gate
(`apply_resolution_gate`) is implemented and tested, but DEFAULT OFF.
Conservative production workflows that explicitly want pending cases
capped may enable it. With the gate disabled (default),
`resolution_status` is still resolved and recorded as advisory
metadata, but the verdict is unchanged.

**Resolution status enum, advisory flag (default), and verdict ceiling (optional gate):**

| Status | Meaning | `production_status_flag` (default, always emitted) | Verdict ceiling (only when gate ON) |
|---|---|---|---|
| `confirmed_final_outcome` | Conviction, plea, acquittal, dismissal, sentencing, or any final court disposition | `null` (no flag — case is ready) | `PRODUCE` (no cap) |
| `charges_filed_pending` | Charges filed but no final disposition yet | `"pending_case_review"` | `HOLD` |
| `ongoing_or_unclear` | Investigation only / no charges / status unknown | `"ongoing_status_review"` | `SKIP` |
| `missing` | No resolution data could be resolved (fail-closed default) | `"resolution_unknown"` | `SKIP` |

The advisory flag map lives in `RESOLUTION_PRODUCTION_FLAG` in
`scoring_math.py` (always emitted on every verdict). The verdict-ceiling
map lives in `RESOLUTION_VERDICT_CEILING` (only consulted when
`P4_RESOLUTION_GATE=1`). Unknown / `None` / invalid statuses fail
closed to `"resolution_unknown"` on the flag side.

**Resolution source priority** (`pipeline4_score._resolve_resolution_status`):

```
1. case_research.resolution_status         → source = "case_research"
2. resolution_labels.json[case_id]         → source = "labels_file"
3. pass1.resolution_status_hint            → source = "pass1_hint"
4. fallback "missing"                      → source = "default_missing"
```

Invalid / unknown values at any tier fall through to the next tier (do
NOT silently become "missing"). If all tiers exhaust without a valid
value, the resolved status is `"missing"` and the source is
`"default_missing"` — fail-closed.

**Pass 2 may downgrade only.** If Pass 2's output dict contains a
valid `resolution_status` AND that status maps to a STRICTLY MORE
RESTRICTIVE ceiling than the resolved status, the resolved status is
replaced and `resolution_source` is recorded as `"pass2_downgrade"`.
Pass 2 may NEVER upgrade. Less-restrictive / invalid / absent Pass 2
values are silently ignored. (Pass 2 prompt does not currently emit
this field — the plumbing is in place for a future prompt change.)

**The gate function is pure** (`apply_resolution_gate` in
`scoring_math.py`): no env reads, takes `gate_enabled` as a parameter.
The orchestration layer (`pipeline4_score.score_case`) reads
`P4_RESOLUTION_GATE` and passes the bool. This keeps the gate trivially
testable without env-stubbing and prevents test pollution across runs.

**Verdict JSON fields (additive — Pipeline 5 + existing readers
unaffected):**

- `resolution_status` (top-level) — the resolved enum value
- `production_status_flag` (top-level) — human-friendly advisory flag
  (always present; `null` for confirmed cases means "no flag needed";
  consumers can always look up the key without dispatching on absence)
- `_pipeline4_metadata.resolution_source` — which tier supplied the status
- `_pipeline4_metadata.resolution_gate_enabled` — was the gate ON
- `_pipeline4_metadata.resolution_gate_applied` — did the ceiling fire
- `_pipeline4_metadata.pre_gate_verdict` — verdict BEFORE the cap

`pre_gate_verdict` is always recorded so audits can answer "what would
the verdict have been without the gate?" even on gate-OFF runs.

**Worked example (default doctrine, gate OFF) — strong pending case:**

```json
{
  "verdict": "PRODUCE",
  "narrative_score": 73.3,
  "resolution_status": "charges_filed_pending",
  "production_status_flag": "pending_case_review",
  "_pipeline4_metadata": {
    "resolution_source": "labels_file",
    "resolution_gate_enabled": false,
    "resolution_gate_applied": false,
    "pre_gate_verdict": "PRODUCE"
  }
}
```

`verdict` says "this is a strong story." `production_status_flag`
says "but there is a production caveat — pending case review needed."
The producer / Pipeline 5 sees both signals and decides. The default
doctrine does NOT auto-demote pending cases; the case is allowed to
remain `PRODUCE` because narrative quality and court status are
independent signals.

**New `batch_summary.tsv` columns (auto-rotated on schema mismatch):**

The TSV gained 3 columns appended to the original 11 (now 14 total):
`resolution_status`, `gate_applied` (literal `"true"`/`"false"`),
`pre_gate_verdict`. The header is enforced via the
`BATCH_SUMMARY_HEADER` module constant.

If `append_batch_summary` finds an existing TSV whose header doesn't
match `BATCH_SUMMARY_HEADER`, it renames the old file to
`batch_summary.tsv.bak.<UTC-timestamp>` and creates a fresh file with
the current header. Historical rows are preserved in the backup; no
manual cleanup needed; no ragged-row TSV ever produced.

**Why this is separate from the subscore math:** resolution status
is not a "narrative quality" signal — it's a production / legal /
research caveat. Putting it in `combine()` would conflate two
unrelated signals into one weighted voice. The default approach
surfaces it as an independent advisory flag; the optional gate
provides a conservative verdict ceiling on top of that. Either way,
the four-subscore weighting and `narrative_score` itself stay
untouched.

**Guardrails:**

- `tests/test_resolution_gate.py` — pure-function gate behaviour
  (`apply_resolution_gate`) plus the `_resolve_resolution_status`
  priority chain (44 cases total).
- `tests/test_score_case_resolution.py` — orchestration integration
  (12 cases): verdict-shape checks, gate ON/OFF behaviour, priority
  chain through the wired-up flow.
- `tests/test_append_batch_summary.py` — TSV header + auto-rotate
  contracts (10 cases).

If you add a new `resolution_status` enum value, you MUST add a matching
entry to `RESOLUTION_VERDICT_CEILING`. The
`test_resolution_verdict_ceiling_keys_match_valid_statuses` test fails
fast if the dict goes out of sync with `VALID_RESOLUTION_STATUSES`.

**Calibration framing.** A full V9b configuration run (gate OFF,
default doctrine) recognized **9/9 known YouTube winners as PRODUCE**
— the calibration recovers the historical signal. This is the correct
result: the calibration set is composed of emulation targets, and
those 9 cases are all narrative winners regardless of court status (3
of them happen to be pending — they are still narrative winners). With
the optional gate enabled, those 3 pending cases would cap to HOLD,
but that is conservative-mode behavior, not the default doctrine. The
default surfaces the same information as a non-blocking advisory flag
instead.

Pending status is NOT determinant for narrative pass/fail. A pending
case may correctly be `verdict=PRODUCE` with
`production_status_flag="pending_case_review"` — strong story,
visible production caveat.

## Output format gotchas

- `scoring_breakdown` may contain `None` for `arc_similarity_score`
  or `artifact_completeness_score`. Downstream readers must handle
  this. The TSV writer (`append_batch_summary`) uses `_fmt_subscore`
  which renders None as the per-subscore floor value (numeric, e.g.
  `15.0`) so TSV columns stay machine-parseable. The human-facing
  run log uses `_fmt` which renders None as `MISS:floor=N` (label
  form, e.g. `arc=MISS:floor=15`) so missing-data penalties are
  visible during review. The JSON verdict preserves None as `null`
  so downstream consumers can still detect missingness.
- `verdict["scoring_breakdown"]` is JSON-serialized verbatim from
  `compute_all`'s output; no transformation between.
- `_pipeline4_metadata.degraded=True` flags cases where Pass 2 failed
  and we fell back to deterministic-only verdict + top-10 Pass 1
  moments. Treat these as lower-confidence.
- `resolution_status`, `production_status_flag` (top-level) plus four
  `_pipeline4_metadata` fields (`resolution_source`,
  `resolution_gate_enabled`, `resolution_gate_applied`,
  `pre_gate_verdict`) are added by the resolution layer.
  `production_status_flag` is the DEFAULT advisory output — `null`
  for `confirmed_final_outcome`, otherwise a short string consumers
  display as a non-blocking caveat (e.g. `"pending_case_review"`).
  The four `resolution_*` gate fields are advisory by default too:
  when `P4_RESOLUTION_GATE=0` (the default), `resolution_gate_applied`
  is always `False` and `pre_gate_verdict` equals the emitted verdict,
  but the resolved status + source are still surfaced for audit. See
  "Critical contract 5" above for the full doctrine — the gate is
  optional / conservative-mode only; sentencing and final adjudication
  are NOT determinant for narrative pass/fail in the default doctrine.
- `batch_summary.tsv` has 14 columns (the original 11 plus
  `resolution_status`, `gate_applied`, `pre_gate_verdict`). The header
  is enforced via the `BATCH_SUMMARY_HEADER` constant. If
  `append_batch_summary` finds an existing TSV with a mismatching
  header, it auto-rotates the old file to
  `batch_summary.tsv.bak.<UTC-timestamp>` and starts fresh.

## Observability (added in Tier 1 #1)

`_log_response_size` warns when an LLM response is at or above
`P4_NEAR_CAP_WARN_FRAC × max_tokens`. This catches output-token-cap
hits before they manifest as JSON parse failures.

`_parse_with_repair` attempts `repair_truncated_json` on parse
failure. When repair succeeds, it logs a `[WARN]` so the operator
knows to consider raising `P4_PASS1_MAX_TOKENS` /
`P4_PASS2_MAX_TOKENS`. This means a single output-token-cap hit no
longer kills the whole case — the scoring run continues with a
slightly-trimmed Pass 1 output (the partial trailing entry is
dropped, structurally-valid JSON is recovered).

## Score history

See `results.tsv`. Peak: **93.94 / 100** at Exp 14 (V9b) on
2026-04-26. Achieved with V8 settings + Pass 2 SKIP gate
(`arc<30 AND artifact<70`) + reverted `TRUST_DETERMINISTIC_PRODUCE`.
The remaining 6.06 points: most likely in `narrative_calibration`
(only 90/100 in V9b — at least one winner is scoring just below its
`min_narrative_score=55`). Tier 2 #4 in the review queue.
