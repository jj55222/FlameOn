# SYSTEM.md — True Crime Content Factory

## Overview

Five interconnected pipelines that turn raw public records and footage into
producible true crime content. Each pipeline has a single job and a clean
handoff to the next.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        SYSTEM ARCHITECTURE                             │
│                                                                        │
│   Pipeline 1              Pipeline 2              Pipeline 3           │
│   WINNER ANALYSIS         CASE DISCOVERY          AUDIO PREPROCESSING  │
│   (reference patterns)    (FlameOn AutoResearch)  (transcript-ready)   │
│         │                       │                       │              │
│         │    scoring weights    │    source URLs +      │              │
│         └───────────┐          │    evidence metadata   │              │
│                     ▼          ▼                        │              │
│               Pipeline 4: TRANSCRIPT ANALYSIS           │              │
│               (narrative scoring + moment ID)  ◄────────┘              │
│                          │                                             │
│                          │  PRODUCE / HOLD / SKIP                      │
│                          ▼                                             │
│               Pipeline 5: CONTENT ASSEMBLY BRIEF                       │
│               (production-ready dossier)                               │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Pipeline 1: Winner Analysis

**Purpose:** Understand what makes successful true crime content work.
Creates the reference patterns that Pipeline 4 learns from.

**Status:** Not started

| Field | Detail |
|-------|--------|
| Input | YouTube URLs of proven bodycam/true crime videos from channels being studied |
| Process | `yt-dlp` audio extraction → Whisper transcription → structural analysis via LLM (narrative arc, pacing, moment screen time, creator framing) |
| Output | JSON profiles per winner — narrative structure, moment types, average segment lengths, artifact combinations used (bodycam + interrogation + 911 = full package), sequencing patterns |
| Scale | Manual-ish at first — watch the videos, LLM processes transcripts, you validate and label. 20-30 winners = solid reference set |
| Feeds into | Pipeline 4 scoring weights |

### Comment Calibration Tool (one-time)

Extracts narrative signals from YouTube comments on top competitor videos
to empirically ground Pipeline 4's scoring weights.

**Architecture:**
- Pass 1 — Rules-based noise gate: strip comments <15 chars, pure emoji,
  political soapboxing, reply chain arguments. ~60-70% removal, zero API cost.
- Pass 2 — LLM extraction (Gemini Flash): classify surviving comments by
  moment type (contradiction, emotional_peak, procedural_violation, reveal,
  detail_noticed, pacing_note), extract timestamps if given, confidence score.
- Aggregation: count moment-type distribution across 20-30 calibration videos.
  If 40% flag contradictions and 5% flag procedural violations, that calibrates
  Pipeline 4's scoring weights empirically.
- Timestamp comments are gold — map back to transcripts to see what was
  being said at the moment the audience latched on.
- Source: `youtube-search-python` (no quota hit)
- Cost: minimal — Flash is cheap, one-time run across ~10-20k filtered comments

**Decision:** Build as one-time calibration tool, not recurring. Gets 80% of
the value at 20% of the complexity.

---

## Pipeline 2: Case Discovery (FlameOn AutoResearch)

**Purpose:** Find new cases with confirmed footage/artifact availability.
Research and score cases given defendant name + jurisdiction.

**Status:** Active — best score 63.73/100 (exp 45)
**Target:** 75-80 score + functional test (7-8/10 ENOUGH cases yield actionable source URLs)
**Detail:** See `CLAUDE.md` in the FlameON project folder

| Field | Detail |
|-------|--------|
| Input | Keyword searches against MuckRock API (`discover.py`), news sources |
| Process | Poll for completed FOIA requests → extract metadata → multi-API research via `research.py` (MuckRock, CourtListener, Brave, YouTube, Reddit) |
| Output | Ranked JSON of leads with research scores, evidence types, source URLs, named subjects |
| Feeds into | Pipeline 3 (downloadable source URLs for audio extraction) |

### Key downstream requirement

Pipeline 3 needs **actual downloadable URLs** for audio/video files.
`research.py` output format must include source URLs typed by evidence
category (bodycam_url, interrogation_url, court_video_url, docket_url).
Optimize for source actionability, not just detection.

### Current blockers
- Brave API billing guard needs per-case budget distribution (cases 14-38 have zero Brave coverage)
- Exp 50 (Jr./Sr. parse fix) incomplete — rerun needed with fresh April quota
- Tier accuracy plateau on INSUFFICIENT cases (4, 7, 14, 17, 35, 36)

---

## Pipeline 3: Audio Preprocessing + Transcription

**Purpose:** Turn raw footage into clean, analyzable text.

**Status:** Not started (architecture defined)

| Field | Detail |
|-------|--------|
| Input | Audio files from MuckRock CDN, YouTube (`yt-dlp`), or agency portals |
| Process | `ffmpeg silencedetect` → silence trimming (with timestamp mapping back to original) → dynamic range compression → loudness normalization → Whisper large-v3 on Colab GPU |
| Output | Timestamped transcript with speaker labels where possible, plus silence map showing what was cut and where |
| Feeds into | Pipeline 4 |

### Technical notes
- Timestamp mapping is critical — Pipeline 4 moments must reference
  original video timestamps, not trimmed-audio timestamps
- Speaker diarization (pyannote or similar) is a nice-to-have for
  distinguishing officer vs. suspect vs. dispatcher
- Whisper large-v3 on Colab T4 GPU handles ~1hr audio in ~10min

---

## Pipeline 4: Transcript Analysis + Narrative Scoring

**Purpose:** Determine whether a case has content potential and identify key moments.

**Status:** Not started (architecture defined, multiple approaches to test)

| Field | Detail |
|-------|--------|
| Input | Transcript from Pipeline 3 |
| Output | Narrative score, key moment timestamps, content pitch, recommended supplementary artifacts, PRODUCE / HOLD / SKIP verdict with reasoning |
| Feeds into | Pipeline 5 (PRODUCE cases only) |

### Summarization strategy experiments

This is where the AutoResearch loop methodology applies again.
Multiple approaches to test and score against Pipeline 1 winner profiles:

1. **Sequential full-pass** — send entire transcript to long-context model
   (Gemini Flash), extract structure in one shot
2. **Map-reduce** — chunk transcript, summarize each chunk, synthesize
3. **KMeans clustering** — embed chunks, cluster, summarize representatives
4. **Hybrid** — clustering for structure + full-pass for contradictions/callbacks
5. **Agentic chunking** — LLM-driven semantic grouping where each transcript
   segment is routed to a narrative "chunk" by topic similarity. Chunks maintain
   evolving summaries. Based on Greg Kamradt's agentic chunker pattern.
   Reference implementation: https://github.com/FullStackRetrieval-com/RetrievalTutorials/blob/main/tutorials/LevelsOfTextSplitting/agentic_chunker.py
   Key adaptation needed: replace generic "proposition" routing with narrative-aware
   routing (e.g., "this segment is part of the confrontation arc" vs "this is
   background/setup"). The chunk summaries become narrative beat descriptions.

The AutoResearch loop determines which strategy (or combination) produces
the best structural extraction, scored against winner analysis profiles.

### Scoring calibration
- Moment type weights derived from Pipeline 1 comment calibration tool
- Narrative arc similarity to winner profiles
- Artifact completeness (bodycam + interrogation + 911 > bodycam alone)

---

## Pipeline 5: Content Assembly Brief

**Purpose:** Give you everything needed to sit down and produce.

**Status:** Not started (architecture defined)

| Field | Detail |
|-------|--------|
| Input | A case that scored PRODUCE from Pipeline 4, plus all artifacts from Pipeline 2 |
| Process | Compile research dossier — transcript with key moments highlighted, all source URLs (court records, news coverage, FOIA docs), narrative structure outline, suggested sequencing based on winner patterns |
| Output | Single document / notebook view = production brief. Open it and start working. No hunting for sources or re-reading transcripts. |

### Brief contents
- Case summary (who, what, when, where, charges, outcome)
- Key moment timestamps with clip boundaries
- Narrative arc recommendation (based on winner analysis)
- All source URLs organized by type
- Suggested supplementary artifacts to request/acquire
- Estimated content length and format recommendation

---

## Build Order

| Priority | Pipeline | Dependency | Current State |
|----------|----------|------------|---------------|
| 1 | Pipeline 2 (FlameOn) | None | Active, score 63.73, target 75-80 |
| 2 | Pipeline 1 (Winner Analysis) | None (parallel) | Not started |
| 3 | Pipeline 3 (Audio Preprocessing) | Pipeline 2 output | Architecture defined |
| 4 | Pipeline 4 (Transcript Analysis) | Pipelines 1 + 3 | Architecture defined |
| 5 | Pipeline 5 (Assembly Brief) | Pipeline 4 | Architecture defined |

Pipeline 1 can be built in parallel with Pipeline 2 since they're independent.
Pipeline 4 needs both Pipeline 1 (scoring weights) and Pipeline 3 (transcripts)
before it can be properly tested.

---

## Cross-Pipeline Data Contracts

To keep pipelines decoupled but compatible, each one's output must conform
to a defined schema. Define these as JSON schemas before building each pipeline.

| Handoff | Key fields required |
|---------|--------------------|
| P2 → P3 | `source_url`, `evidence_type`, `format` (audio/video), `case_id`, `defendant`, `jurisdiction` |
| P3 → P4 | `transcript[]` (start_sec, end_sec, text, speaker?), `silence_map[]`, `original_duration`, `case_id` |
| P1 → P4 | `winner_profiles[]` (moment_types, weights, narrative_arc, segment_lengths) |
| P4 → P5 | `verdict`, `score`, `key_moments[]`, `content_pitch`, `case_id`, `artifact_refs[]` |

---

## Shared Infrastructure

| Component | Used by | Notes |
|-----------|---------|-------|
| Google Sheets | P2 (tracking) | May migrate to SQLite for local pipelines |
| Google Drive sync | All | Project folders synced via Drive for Desktop |
| CLAUDE.md per pipeline | All | Each pipeline folder gets its own context doc |
| Brave API | P2 | Only paid API; billing guard enforced |
| Whisper (Colab GPU) | P3 | Free on Colab T4 |
| Gemini Flash | P1, P4 | Cheap, long-context, good for structural analysis |
| yt-dlp | P1, P2, P3 | YouTube metadata + audio extraction |

---

## Guiding Principles

1. **Finish Pipeline 2 before starting Pipeline 3.** The research agent must
   reliably produce actionable source URLs before downstream pipelines have
   anything to process.
2. **Pipeline 1 can run in parallel.** Winner analysis doesn't depend on any
   other pipeline and directly improves Pipeline 4 quality.
3. **One pipeline folder, one CLAUDE.md.** Don't let scope creep across
   project boundaries. Each pipeline is its own Claude Code workspace.
4. **Transcript-first, always.** Never download video until text analysis
   confirms it's worth it. This is what makes the system economical.
5. **AutoResearch loop is reusable.** The 3-file architecture (program.md /
   agent script / immutable scorer) applies to Pipeline 4's summarization
   strategy experiments too.
6. **`--dry-run` on everything.** Every pipeline must have a mode that shows
   what it would do without side effects.
