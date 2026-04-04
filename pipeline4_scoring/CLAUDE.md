# CLAUDE.md — Pipeline 4: Transcript Analysis + Narrative Scoring

## What this pipeline does

Determines whether a case has content potential and identifies key moments.
Takes a transcript from Pipeline 3 and scoring weights from Pipeline 1,
outputs a PRODUCE / HOLD / SKIP verdict with key moments and narrative
recommendations.

## Architecture: Two-Pass Analysis

### Pass 1 — Structural Extraction (Gemini Flash)

Gemini Flash (long context, cheap) processes the full transcript and extracts:
- **Timeline reconstruction:** ordered sequence of events with timestamps
- **Speaker interaction map:** who talks to whom, power dynamics
- **Moment candidates:** potential narrative moments with type classification
- **Contradictions:** statements that conflict with other statements or known facts
- **Emotional arc:** tension level over time (rising, peak, falling)
- **Factual anchors:** names, dates, addresses, badge numbers, charges mentioned

**Why Gemini Flash:** Long context window handles full transcripts (1hr+ bodycam
= 50K+ tokens) in a single pass. Cheap enough to run on every case.

### Pass 2 — Narrative Scoring (Claude Sonnet)

Takes Pass 1 structural output + Pipeline 1 scoring weights and produces:
- **Moment scoring:** each candidate moment scored against winner profile weights
- **Narrative arc assessment:** how well does this case's arc match winner patterns
- **Artifact completeness:** what evidence types are available vs. what winners use
- **Verdict:** PRODUCE / HOLD / SKIP with confidence and reasoning
- **Content pitch:** one paragraph on why this case would work (for PRODUCE cases)

**Why Claude Sonnet:** Better judgment on nuanced narrative assessment.
The structural work is already done by Flash; Sonnet only sees the extracted
structure, not the raw transcript.

## What to build

### `pipeline4_score.py` — Main Scoring Script

**Input:**
- Transcript JSON from Pipeline 3 (matching `p3_to_p4_transcript` schema)
- Scoring weights JSON from Pipeline 1 (matching `p1_scoring_weights` schema)
- Case research JSON from Pipeline 2 (matching `p2_to_p3_case` schema) — for artifact completeness

**Processing:**

1. **Load and validate inputs** against schemas
2. **Pass 1 — Gemini Flash structural extraction**
   - Send full transcript text to Gemini Flash with structured extraction prompt
   - Prompt must request JSON output matching an internal extraction schema
   - Handle long transcripts: Gemini Flash 1.5 handles 1M tokens, so single-pass works
3. **Pass 2 — Claude Sonnet narrative scoring**
   - Send: Pass 1 extraction + scoring weights + artifact inventory
   - Sonnet scores each moment, assesses arc, determines verdict
   - Prompt calibrated toward precision: most cases should score HOLD or SKIP
4. **Output** JSON matching `p4_to_p5_verdict` schema

**CLI:**
```bash
# Score a single case
python pipeline4_score.py \
  --transcript transcripts/smith_harris_tx_2023.json \
  --weights scoring_weights.json \
  --case-research cases/smith_harris_tx_2023.json \
  --output verdicts/

# Dry run (shows what would be sent to each model)
python pipeline4_score.py --transcript t.json --weights w.json --dry-run

# Score without weights (uses default equal weights — for testing before P1 is done)
python pipeline4_score.py --transcript t.json --output verdicts/

# Batch score all transcripts in a directory
python pipeline4_score.py --transcript-dir transcripts/ --weights w.json --output verdicts/
```

### Pass 1 Prompt Design (Gemini Flash)

The extraction prompt is critical. It must:
- Process the FULL transcript (don't summarize, extract structure)
- Identify moments by type with exact timestamps
- Flag contradictions explicitly (these are gold for content)
- Track emotional intensity over time
- Note procedural details (Miranda warnings, use of force, policy references)

```
You are analyzing a law enforcement transcript for narrative structure.
Extract the following as JSON:

1. timeline: [{timestamp_sec, event, speakers_involved, emotional_intensity (1-5)}]
2. moments: [{timestamp_sec, end_timestamp_sec, type (contradiction|emotional_peak|procedural_violation|reveal|detail_noticed|callback|tension_shift), description, transcript_excerpt}]
3. contradictions: [{statement_a: {timestamp, speaker, text}, statement_b: {timestamp, speaker, text}, nature_of_contradiction}]
4. speaker_dynamics: [{speaker_pair, interaction_type (cooperative|adversarial|neutral), power_dynamic}]
5. emotional_arc: [{segment_start_sec, segment_end_sec, avg_intensity, trend (rising|falling|stable)}]
6. factual_anchors: [{type (name|date|location|badge_number|charge|statute), value, timestamp_sec}]
```

### Pass 2 Prompt Design (Claude Sonnet)

Scoring prompt receives structured extraction + weights. Must:
- Score each moment using `moment_weights` from scoring weights
- Compare narrative arc to `arc_patterns` from winners
- Assess artifact completeness against `artifact_value` scores
- Apply precision bias: default toward HOLD/SKIP, require strong signal for PRODUCE
- Generate content pitch only for PRODUCE verdicts

### `summarization_experiments.py` — Strategy Testing (future)

The AutoResearch loop pattern applies here too. Multiple summarization
strategies to test:
1. Sequential full-pass (current default)
2. Map-reduce chunking
3. KMeans clustering (WARNING: misses cross-segment contradictions)
4. Hybrid (clustering for structure + full-pass for contradictions)
5. Agentic chunking (Greg Kamradt pattern — narrative-aware routing)

Each strategy gets scored against Pipeline 1 winner profiles. But start with
#1 (sequential full-pass) — it works, Gemini handles the context length, and
you can optimize later.

## Output contract

Must match `p4_to_p5_verdict` in `../schemas/contracts.json`:
- `verdict` (PRODUCE / HOLD / SKIP)
- `narrative_score` (0-100)
- `key_moments[]` with timestamps, types, importance
- `content_pitch`
- `narrative_arc_recommendation`
- `scoring_breakdown`

## Dependencies

```
google-generativeai  # Gemini Flash
anthropic            # Claude Sonnet for Pass 2
```

## Environment variables

```
GEMINI_API_KEY=...     # Google AI Studio
ANTHROPIC_API_KEY=...  # For Claude Sonnet Pass 2
```

## Scoring calibration principles

- **Precision over recall.** A false PRODUCE is expensive (wasted production time).
  A false SKIP just means you miss one case. Calibrate aggressively.
- **Most cases should score HOLD or SKIP.** If >30% of cases score PRODUCE,
  thresholds are too loose.
- **Contradictions are disproportionately valuable.** Winner analysis will
  likely confirm this — audience engagement spikes on contradictions.
- **Artifact completeness is a multiplier, not a gate.** A case with amazing
  narrative but only bodycam can still be PRODUCE. A case with all artifacts
  but no narrative tension is SKIP.

## What success looks like

- Score 5+ cases from the calibration dataset
- PRODUCE rate < 30% (precision calibration)
- Key moments have correct original-video timestamps
- Dry run shows both prompts without calling APIs
- Works without scoring weights (equal-weight fallback for testing)
