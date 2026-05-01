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
  └─ Reconcile (pipeline4_score.py:_run_score_case)
        SKIP × PRODUCE → HOLD            (no big swings allowed)
        PRODUCE × SKIP → HOLD            (no big swings allowed)
        PRODUCE × HOLD + P4_TRUST_DETERMINISTIC_PRODUCE=1 → PRODUCE
        else → LLM verdict
```

## Mutability boundaries

| File | Mutability | Why |
|---|---|---|
| `evaluate.py` | **IMMUTABLE** | The fixed rubric every experiment is measured against. Header self-documents. |
| `calibration_data.json` | **IMMUTABLE** | The 10-case ground truth (9 winners + 1 SFDPA SKIP). |
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
resolution / source-backed chronology / offender-victim identity.
These are critical signals per the editorial spec but currently land
implicitly (chronology in moment_density, identity in factual_anchors
→ uniqueness, resolution in case_research input). Promoting them to
explicit subscores is a structural change for a future pass.

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
