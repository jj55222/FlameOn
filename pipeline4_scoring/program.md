# program.md — Pipeline 4 AutoResearch directives

## Task

Given a case transcript (from Pipeline 3) and Pipeline 1 winner weights,
classify the case's narrative potential as **PRODUCE**, **HOLD**, or
**SKIP** and identify key narrative moments.

## 3-file architecture

| File | Who touches it | Purpose |
|---|---|---|
| `program.md` | **Human only** | These directives |
| `prompts.py` + `scoring_math.py` | **Agent** | Mutable rubric/prompts |
| `evaluate.py` (this pipeline) | **Nobody** (immutable) | Score against ground truth |

Mutable state the agent may iterate on:
- `prompts.py` — Pass 1 / Pass 2 prompt templates
- `scoring_math.py` — deterministic pre-scoring weights, verdict thresholds
- `pipeline4_score.py` — orchestration + Pass-2 reconciliation rules

DO NOT modify `evaluate.py`, `transcript_loader.py`, or the
`p4_to_p5_verdict` schema.

## Verdicts defined

- **PRODUCE** — case has clear narrative momentum: multiple moments across
  bodycam/interrogation/narration, at least one contradiction or reveal,
  arc matches a winner pattern (cold_open preferred). Justifies full
  production effort.
- **HOLD** — narrative present but thin or unclear. Artifact set incomplete.
  Worth revisiting when more evidence surfaces.
- **SKIP** — no usable narrative. Low moment density, flat emotional arc,
  missing core artifacts. Not worth production time.

## Scoring rubric (what evaluate.py measures)

| Component | Weight | What it measures |
|---|---|---|
| `verdict_accuracy` | 40% | Does output verdict match ground-truth? |
| `narrative_calibration` | 25% | Does `narrative_score` fall in expected range? |
| `arc_accuracy` | 15% | Does `narrative_arc_recommendation` match ground-truth arc? |
| `moment_coverage` | 15% | Are `>= min_key_moments` returned? |
| `artifact_completeness` | 5% | Is `artifact_completeness.available` correct? |

Aggregate = weighted sum. Target: **>= 80** on the calibration set.

## Calibration ground truth sources

1. **Pipeline 1 winners** (known PRODUCE): the video got millions of views,
   so by definition the narrative was producible. Ground truth verdict =
   PRODUCE, min_narrative_score >= 60, expected_arc from the winner
   profile, min_key_moments = 3.
2. **Negative class**: transcripts from random non-case YouTube (arbitrary
   talking heads, vlogs, etc.) — ground truth verdict = SKIP.
3. **Human-labeled HOLD**: cases the user has manually reviewed and tagged.

Agent should iterate on prompts/weights until `evaluate.py` score crosses
the target. Each iteration logs to `results.tsv` with hypothesis + changes.

## Precision bias

Most cases should score HOLD or SKIP. If >40% of calibration returns
PRODUCE on a mixed set, thresholds are too loose. The cost of a false
PRODUCE is a wasted production sprint; a false SKIP only misses one case.

## Constraints

- Pass 1 must call a long-context model capable of full-transcript
  extraction (default: `google/gemini-3.1-flash-lite-preview`).
- Pass 2 must call a reasoning model (default: `qwen/qwen3.6-plus`).
- Reconciliation rule holds: if deterministic verdict = SKIP and Pass 2 =
  PRODUCE, compromise to HOLD. If deterministic = PRODUCE and Pass 2 =
  SKIP, compromise to HOLD. Prevents either signal from overriding.
- `--dry-run` must show both prompts without API calls.
