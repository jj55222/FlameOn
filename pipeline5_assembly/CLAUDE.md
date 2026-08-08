# CLAUDE.md — Pipeline 5: Content Assembly Brief

## What this pipeline does

Takes a PRODUCE-scored case from Pipeline 4 and compiles everything needed
to sit down and produce. Open the brief, start working. No hunting for
sources or re-reading transcripts.

## What to build

### `pipeline5_assemble.py` — Brief Generator

**Input:**
- Verdict JSON from Pipeline 4 (`p4_to_p5_verdict` schema)
- Transcript JSON(s) from Pipeline 3 (`p3_to_p4_transcript` schema)
- Case research JSON from Pipeline 2 (`p2_to_p3_case` schema)
- Scoring weights from Pipeline 1 (for narrative arc recommendations)

**Output:** A production brief as both structured JSON and rendered Markdown.

### Brief contents

1. **Case summary**
   - Defendant name, jurisdiction, charges, outcome (if known)
   - One-paragraph narrative summary
   - Incident date, key locations

2. **Key moment timestamps with clip boundaries**
   - Each moment from P4 verdict with:
     - Original video timestamp (start + end)
     - Moment type and importance level
     - Transcript excerpt
     - Suggested clip boundary (pad 5sec before, 3sec after for context)
   - Sorted by importance, then chronologically

3. **Narrative arc recommendation**
   - Recommended structure type (from P1 winner patterns)
   - Suggested beat sequence with approximate runtime per beat
   - Which moments to use for hook, climax, reveals
   - Opening options: cold open moment vs. chronological start

4. **Source URLs organized by type**
   - Bodycam footage URLs
   - Interrogation recordings
   - 911 audio
   - Court records / docket links
   - News coverage links
   - FOIA documents
   - Each with download instructions (yt-dlp command, direct download, etc.)

5. **Supplementary artifact recommendations**
   - What artifact types winners typically use that this case is missing
   - Where to look for them (specific agencies, FOIA targets)
   - Priority: critical vs. nice-to-have

6. **Production metadata**
   - Estimated content length (minutes) based on winner patterns
   - Transcript word count and speaker breakdown
   - Narrative score and confidence from P4
   - Artifact completeness percentage

### Rendered Markdown format

The markdown version should be directly usable as a production document:

```markdown
# Production Brief: [Case Name]
## Case Summary
...
## Key Moments (sorted by importance)
### 1. [Moment Type] — [HH:MM:SS] ★★★ CRITICAL
> "transcript excerpt..."
Clip: [HH:MM:SS] → [HH:MM:SS]
...
## Narrative Arc: [Recommended Structure]
### Suggested Beat Sheet
| Beat | Timing | Content | Source |
...
## Source Files
### Bodycam
- [URL] — `yt-dlp "URL"` or direct download
...
## Missing Artifacts
...
```

**CLI:**
```bash
# Generate brief for a single PRODUCE case
python pipeline5_assemble.py \
  --verdict verdicts/smith_harris_tx_2023.json \
  --transcript transcripts/smith_harris_tx_2023.json \
  --case-research cases/smith_harris_tx_2023.json \
  --weights scoring_weights.json \
  --output briefs/

# Dry run
python pipeline5_assemble.py --verdict v.json --dry-run

# Generate briefs for all PRODUCE cases in a directory
python pipeline5_assemble.py --verdict-dir verdicts/ --transcript-dir transcripts/ --output briefs/

# JSON only (skip markdown rendering)
python pipeline5_assemble.py --verdict v.json --json-only --output briefs/
```

## Output format

Two files per case:
- `{case_id}_brief.json` — structured data, machine-readable
- `{case_id}_brief.md` — rendered markdown, human-readable production doc

## Dependencies

```
jinja2  # for markdown template rendering (optional, can use f-strings)
jsonschema  # for input validation
```

## No API keys needed

Pipeline 5 is pure data assembly — no LLM calls, no API calls.
It reads outputs from Pipelines 1-4 and compiles them.

## What success looks like

- Generate a brief from a test case that includes all 6 sections
- Markdown renders cleanly and is immediately usable as a production doc
- All timestamps reference original video (not trimmed audio)
- Source URLs include download commands
- Dry run shows what would be assembled without writing files
- Handles cases with partial data gracefully (missing interrogation, etc.)
