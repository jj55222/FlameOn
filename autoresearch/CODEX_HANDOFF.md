# CODEX_HANDOFF.md — FlameOn Case-Graph Harness Experiment Loop

## Goal

Build a reliable case-information and artifact-discovery harness that can move from structured inputs to weak inputs.

North-star flow:

```text
partial case input
→ locked case identity
→ outcome/concluded status
→ artifact claims
→ verified public artifact URLs
→ production-ready PRODUCE/HOLD/SKIP packet
```

## Can Codex run this as a loop?

Yes, but do not run it as an unbounded autonomous loop.

Use bounded experiment cycles:

```text
1. choose one experiment
2. state hypothesis
3. make one controlled change
4. run a small subset
5. run the broader subset only if the small subset improves
6. log metrics and failure examples
7. keep, revert, or park the change
8. commit only kept changes
```

Recommended max per cycle:

```text
- 1 code/config change
- 1 focused metric
- 1-3 smoke cases first
- then one tier run, e.g. ENOUGH or INSUFFICIENT
- full run only after subset passes
```

## Stop criteria

Codex should stop and report when any of the following happen:

```text
- schema output breaks
- false-high INSUFFICIENT cases increase
- runtime grows by >25% without artifact-yield improvement
- API cost/calls exceed configured cap
- same change fails twice
- experiment logs are missing or malformed
- code starts optimizing old recall score at the expense of actionability
```

## Priority order

Do not chase broad recall first.

Optimize in this order:

```text
1. schema stability
2. false-high reduction
3. identity lock rate
4. verified artifact rate
5. downloadable artifact rate
6. runtime/cost
7. weak-input generalization
```

## Files Codex should treat as core

```text
autoresearch/research_case_graph.py
autoresearch/evaluate_actionability.py
autoresearch/CASE_GRAPH_README.md
autoresearch/CODEX_HANDOFF.md
autoresearch/calibration_data.json
autoresearch/results.tsv
```

## Files Codex should not edit unless explicitly instructed

```text
autoresearch/evaluate.py
autoresearch/calibration_data.json
autoresearch/program.md
```

`evaluate.py` is the legacy immutable scorer. If it must be changed, create a new evaluator instead.

## Local file policy

Experiments will create lots of local files. That is okay if they are organized and ignored.

Recommended local-only directories:

```text
autoresearch/.cache/
autoresearch/.runs/
autoresearch/.artifacts/
autoresearch/.tmp/
autoresearch/.logs/
```

Add or preserve `.gitignore` rules so these do not get committed by accident:

```gitignore
# FlameOn experiment artifacts
autoresearch/.cache/
autoresearch/.runs/
autoresearch/.artifacts/
autoresearch/.tmp/
autoresearch/.logs/
*.sqlite
*.db
*.jsonl.tmp
*.html
*.pdf
*.mp4
*.mp3
*.wav
*.m4a
```

Codex can read files in the repo/workspace. It can also read local files created during the run if they stay inside the workspace. It cannot read arbitrary files on the user's machine unless they are present in the working directory/environment that Codex receives.

## Required experiment log

Every experiment must append a JSONL row to:

```text
autoresearch/.runs/experiments.jsonl
```

Recommended row shape:

```json
{
  "experiment_id": "E001",
  "timestamp": "2026-04-29T00:00:00Z",
  "hypothesis": "Stricter identity lock will reduce false-highs",
  "change_summary": "Raised high confidence requirement from 2 anchors to 3 anchors",
  "commands": ["python evaluate_actionability.py --case 4 --verbose"],
  "metrics": {
    "identity_lock_rate": 0.0,
    "verified_artifact_rate": 0.0,
    "downloadable_artifact_rate": 0.0,
    "insufficient_false_high_count": 0,
    "runtime_seconds": 12.4,
    "api_calls": {"brave": 3, "youtube": 4}
  },
  "failure_examples": [],
  "decision": "keep|revert|park",
  "commit_sha": null
}
```

## Minimum smoke suite

Run these before and after every material change:

```bash
cd autoresearch
python evaluate_actionability.py --case 1 --verbose
python evaluate_actionability.py --case 4 --verbose
python evaluate_actionability.py --tier INSUFFICIENT
```

Only run full evaluations after the smoke suite passes:

```bash
python evaluate_actionability.py --all
```

## Recommended bounded loop prompt for Codex

```text
You are working in the FlameOn repo on branch case-graph-harness.

Goal: improve the case-graph harness for production actionability, not broad recall.

For this run:
1. Read autoresearch/CODEX_HANDOFF.md, CASE_GRAPH_README.md, and evaluate_actionability.py.
2. Select exactly one experiment from the experiment matrix.
3. State the hypothesis in a new JSONL experiment row.
4. Make one controlled code/config change.
5. Run the smoke suite:
   - python evaluate_actionability.py --case 1 --verbose
   - python evaluate_actionability.py --case 4 --verbose
   - python evaluate_actionability.py --tier INSUFFICIENT
6. If the smoke suite passes, run the smallest broader tier needed.
7. Append metrics and failure examples to autoresearch/.runs/experiments.jsonl.
8. Keep the change only if it improves the target metric without increasing false-highs or breaking schema.
9. Commit kept changes with a descriptive message.
10. Stop and report results. Do not start a second experiment in the same run.
```

## Deterministic vs LLM rules

Use deterministic code for:

```text
- name/date/jurisdiction/case-number normalization
- source deduplication
- URL/media detection
- downloadability probes
- API budgets/rate limits
- identity confidence thresholds
- final confidence decisions
- metrics and logging
```

Use LLMs for:

```text
- messy text extraction from news/FOIA pages
- artifact claim classification
- query expansion
- ambiguous same-name comparison
- FOIA/public-records timeline summarization
- final production packet synthesis
```

LLM outputs must be strict JSON and should never directly set final HIGH confidence. Final confidence should be computed by deterministic thresholds.

## Local files and Codex behavior

Local files matter in three ways:

1. **Visibility**: Codex can read files present in its workspace. It will not automatically see files outside the repo unless they are mounted/copied into the workspace.
2. **Persistence**: Files created during one Codex session may not be available in a later fresh session unless committed, uploaded, or stored in a persistent workspace.
3. **Commit hygiene**: Large artifacts, caches, downloaded HTML/PDF/video/audio, and API responses should stay out of git unless deliberately promoted as fixtures.

Recommended pattern:

```text
- commit source code, configs, small fixtures, and documentation
- do not commit raw downloaded pages/videos/audio
- keep reproducible logs as JSONL summaries
- if a raw artifact is useful as a test fixture, save a small redacted text/JSON fixture instead of the whole file
```

## Desired end state

The harness should support these input modes:

```text
1. defendant + jurisdiction
2. structured dataset row, e.g. WaPo UoF
3. MuckRock request metadata
4. CourtListener docket/case metadata
5. YouTube URL/title/description
6. news article URL/text
```

Each input mode should output the same case graph schema.
