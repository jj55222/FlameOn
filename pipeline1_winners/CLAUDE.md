# CLAUDE.md — Pipeline 1: Winner Analysis

## What this pipeline does

Analyzes successful true crime / bodycam YouTube content to extract patterns
that Pipeline 4 uses for scoring. Two outputs: **winner profiles** (per-video
structural analysis) and **scoring weights** (aggregated patterns across all
winners).

## What to build

### 1. `analyze_winner.py` — Single Video Analyzer

Takes a YouTube URL, produces a winner profile JSON.

**Steps:**
1. `yt-dlp` extract audio (mp3/wav) + metadata (title, views, duration, channel)
2. Whisper transcription (large-v3 on Colab T4, or medium for speed)
3. Send transcript to Gemini Flash with structural analysis prompt
4. Output: winner profile JSON per `schemas/contracts.json` → `p1_winner_profile`

**Gemini Flash prompt should extract:**
- Narrative arc type (chronological, cold_open, parallel_timeline, reveal_structure, escalation)
- Narrative beats with position (hook at 0-3%, setup at 3-15%, etc.)
- Moment types found (contradiction, emotional_peak, procedural_violation, reveal, etc.)
- Segment statistics (avg length, bodycam %, narration %, interrogation %)
- Artifact combination (which source types were woven together)

**CLI:**
```bash
python analyze_winner.py --url "https://youtube.com/watch?v=XXX" --output winners/
python analyze_winner.py --url "..." --dry-run  # shows what would be extracted
```

### 2. `comment_calibration.py` — Comment Signal Extractor (one-time tool)

Extracts audience engagement signals from YouTube comments on winner videos.

**Architecture (two-pass):**
- **Pass 1 — Rules-based noise gate (Python, zero API cost):**
  - Strip comments < 15 chars
  - Strip pure emoji / emoticon-only
  - Strip political soapboxing (keyword list: "trump", "biden", "liberal", "conservative", "democrat", "republican", "election", "vote", etc.)
  - Strip reply chain arguments (detect @mentions + aggressive sentiment)
  - Target: ~60-70% removal
- **Pass 2 — LLM extraction (Gemini Flash batch):**
  - Classify surviving comments by moment type: contradiction, emotional_peak, procedural_violation, reveal, detail_noticed, pacing_note
  - Extract timestamps if present (regex: `\d{1,2}:\d{2}` patterns)
  - Confidence score per classification
- **Aggregation:**
  - Count moment-type distribution across all calibration videos
  - Map timestamp comments back to transcript positions
  - Output: moment weight distribution for Pipeline 4

**Comment source:** `youtube-search-python` (free, no quota hit)

**CLI:**
```bash
python comment_calibration.py --video-ids ids.txt --output calibration/
python comment_calibration.py --video-ids ids.txt --dry-run
```

### 3. `aggregate_weights.py` — Scoring Weight Generator

Takes all winner profiles + comment calibration output → produces scoring weights JSON.

**Output:** `scoring_weights.json` per `schemas/contracts.json` → `p1_scoring_weights`

**CLI:**
```bash
python aggregate_weights.py --winners winners/ --comments calibration/ --output scoring_weights.json
```

## Output contracts

- Per-video: `p1_winner_profile` schema in `../schemas/contracts.json`
- Aggregated: `p1_scoring_weights` schema in `../schemas/contracts.json`

## Dependencies

```
yt-dlp
openai-whisper  # or faster-whisper for speed
google-generativeai  # Gemini Flash
youtube-search-python  # free comment extraction
```

## Environment variables

```
GEMINI_API_KEY=...  # Google AI Studio, free tier available
```

## Scale

Start with 20-30 winner videos. Manual curation of the URL list — watch the
videos yourself, confirm they represent the quality target. The LLM processes
transcripts; you validate and label.

## What success looks like

- 20+ winner profiles with consistent structural extraction
- Comment calibration showing clear moment-type distribution (expect contradictions and emotional peaks to dominate)
- Scoring weights JSON that Pipeline 4 can load directly
- `--dry-run` mode on all three scripts
