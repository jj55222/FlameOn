# FlameOn AutoResearch — program.md

> **This file is written and maintained by the human. The agent never modifies it.**
> It defines the research goal, constraints, and rules of engagement.

---

## Goal

Train a **research agent** that, given minimal case information (defendant name + jurisdiction),
independently discovers and validates the supporting artifacts needed to produce content:

- Body-worn camera (BWC) footage
- Interrogation recordings / confession videos
- Court video / audio (hearings, trials, sentencing)
- Docket documents (complaints, affidavits, motions, rulings)
- 911 / dispatch audio
- News coverage with primary-source links

The agent is measured against a **calibration set** of 38 cases where ground truth is known.
The metric is a single number: **research_score** (higher is better, max 100).

---

## Architecture

```
program.md          ← YOU ARE HERE. Human-only. Agent never touches this.
research.py         ← Agent's sandbox. All research methodology lives here.
evaluate.py         ← Immutable scoring. Loads calibration_data.json, scores agent output.
calibration_data.json ← Frozen ground truth exported from CASE ANCHOR sheet.
results.tsv         ← Experiment log. Append-only record of every run.
```

---

## Setup (run once)

1. Confirm all files exist: `program.md`, `research.py`, `evaluate.py`, `calibration_data.json`
2. Confirm API keys are set:
   - `MUCKROCK_API_TOKEN` (optional for unauthenticated read access)
   - `ANTHROPIC_API_KEY` (for Claude-powered extraction in later phases)
3. Run baseline: execute `research.py` unmodified, then score with `evaluate.py`
4. Record baseline in `results.tsv`
5. Begin experiment loop

---

## Experiment Loop

```
while not interrupted:
    1. Read program.md (re-read every iteration — human may update directives)
    2. Review results.tsv — analyze what worked and what didn't
    3. Form hypothesis about how to improve research methodology
    4. Modify research.py — change query strategies, parsing, validation, source ranking
    5. Run experiment: python evaluate.py
    6. Record result in results.tsv
    7. If research_score improved → git commit with descriptive message
    8. If research_score declined → git reset, try different approach
```

---

## What the agent CAN modify

**research.py is the only file you edit.** Everything is fair game:

- MuckRock API query construction (search terms, filters, field combinations)
- Query expansion strategies (synonyms, jurisdiction variations, name permutations)
- Source discovery logic (how to find BWC, interrogation, court footage)
- Result parsing and relevance scoring
- Cross-referencing between sources (MuckRock → court records → news → footage)
- Validation logic (how to distinguish true matches from false positives)
- Source prioritization order
- Rate limiting and retry strategies

---

## What the agent CANNOT modify

- **evaluate.py** — The scoring function is the ground truth. If you could change it,
  you'd just make the test easier instead of making the research better.
- **calibration_data.json** — Frozen evidence ground truth from validated cases.
- **program.md** — Human-written directives.
- Do NOT install new packages beyond what's in requirements.txt.

---

## Metric: research_score

`evaluate.py` computes a single score (0–100) combining:

1. **Evidence Type Recall** (40% weight)
   For ENOUGH-tier cases: did the agent correctly identify which evidence types exist?
   (BWC, interrogation, court video, docket docs, 911)

2. **Source Discovery Rate** (30% weight)
   For ENOUGH-tier cases: what fraction of known verified source URLs (or equivalent sources)
   did the agent independently find?

3. **Precision Penalty** (20% weight)
   Across all cases: what fraction of the agent's returned sources are actually relevant?
   False positives (wrong person, wrong case, entertainment links) reduce the score.

4. **Tier Accuracy** (10% weight)
   Did the agent correctly classify cases into ENOUGH / BORDERLINE / INSUFFICIENT?
   Calling an INSUFFICIENT case ENOUGH is heavily penalized.

---

## Constraints

- **Time budget**: Each experiment should complete within 10 minutes.
  Rate limits: MuckRock 1 req/sec, Brave 1 req/sec, CourtListener 5 req/min.
- **No cheating**: The agent receives ONLY `defendant_names` and `jurisdiction` as input.
  It must not read ground_truth from calibration_data.json during research.
  evaluate.py handles the comparison after research completes.
- **Determinism**: Given the same input, research.py should produce consistent results.
  Random sampling is allowed but must be seeded.

---

## Training Phases

### Phase 1: Structured APIs (current)
All JSON-response, rate-limited, controllable sources:

| API | What it finds | Auth |
|---|---|---|
| **MuckRock** (`api_v2/foia/`) | FOIA requests, responsive documents | Optional token |
| **CourtListener** (`courtlistener.com/api/rest/v4/`) | Court dockets, opinions, oral arguments | Free API key |
| **YouTube Data API v3** | Official dept channels, bodycam/interrogation uploads | Google API key |
| **Brave Search API** | News coverage, court records, case mentions | API key (have) |

Agent learns: query construction, cross-referencing between sources,
relevance validation, false positive filtering.

### Phase 2: Structured APIs + Jurisdiction Portals
Add Oxylabs-powered scraping of government FOIA portals (GovQA, NextRequest, JustFOIA).
Learn: navigating messy HTML, JS-rendered pages, inconsistent formats.
Upgrade jurisdiction tracker from MuckRock turnaround grades to real-time portal data.

### Phase 3: Generalized (YouTube / News input)
Accept a YouTube video URL or news article as input.
Extract case details, then run the research playbook from Phases 1–2.
Output: complete case dossier with all discoverable artifacts.

---

## Results Logging

Append every experiment to `results.tsv` with these columns:

```
experiment_id	timestamp	research_score	evidence_recall	source_discovery	precision	tier_accuracy	hypothesis	changes_made	commit_hash
```

- **experiment_id**: Sequential integer starting from 0 (baseline)
- **research_score**: The composite score from evaluate.py (0–100)
- **evidence_recall / source_discovery / precision / tier_accuracy**: Component scores
- **hypothesis**: One sentence describing what the agent was trying to improve
- **changes_made**: Brief description of what changed in research.py
- **commit_hash**: 7-char git hash if committed (or "reverted" if rolled back)

---

## Research Quality Standards

The agent should internalize these principles:

1. **Name specificity matters.** "Joseph Concialdi Phoenix" not "Joseph bodycam."
   Common first names + generic terms = false positive factory.

2. **Validate before claiming.** A link to a court docket is not evidence of court VIDEO.
   A news article mentioning bodycam is not the bodycam footage itself.

3. **Wrong person is worse than no result.** Returning Jaycee Dugard links for
   Anthony Hines is a precision failure. Return nothing rather than garbage.

4. **Cross-reference everything.** A MuckRock FOIA request mentioning an agency +
   a court docket with the same case number + news coverage = high confidence.
   A single search result with a partial name match = low confidence.

5. **Know when to stop.** If 3 different query strategies return nothing relevant,
   the evidence likely doesn't exist. Score it INSUFFICIENT and move on.
