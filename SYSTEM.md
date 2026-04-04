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

## Cross-Pipeline Data Contracts

All handoff schemas are defined in `schemas/contracts.json`.
Every pipeline MUST validate its output against these schemas before writing.

## Guiding Principles

1. Finish Pipeline 2 before starting Pipeline 3.
2. Pipeline 1 can run in parallel with Pipeline 2.
3. One pipeline folder, one CLAUDE.md.
4. Transcript-first, always — never download video until text analysis confirms worth.
5. AutoResearch loop pattern (program.md / agent script / immutable scorer) is reusable.
6. `--dry-run` on everything.
7. All code must be Colab-ready Python.

## Shared Infrastructure

| Component | Used by | Notes |
|-----------|---------|-------|
| Google Drive sync | All | Repo synced via Drive for Desktop |
| GitHub + auto-commit hook | All | Claude Code auto-pushes on every file write |
| CLAUDE.md per pipeline | All | Each pipeline folder gets its own context doc |
| Brave API | P2 | 2K free/month; billing guard enforced |
| Firecrawl | P2 | Jurisdiction portal scraping; 500 free credits/month |
| Exa | P2 | Supplemental search; reduces Brave quota burn |
| Whisper/WhisperX (Colab GPU) | P3 | Free on Colab T4 |
| Gemini Flash | P1, P4 | Cheap, long-context, structural analysis |
| Claude Sonnet | P4 | Narrative scoring (Pass 2) |
| yt-dlp | P1, P2, P3 | YouTube metadata + audio extraction |
