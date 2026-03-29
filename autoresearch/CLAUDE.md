# CLAUDE.md — FlameOn AutoResearch

## What this is

An AutoResearch loop (inspired by Karpathy's autoresearch) that trains a
**research agent** to find case artifacts (bodycam footage, interrogation
recordings, court video, docket docs, 911 audio) given only a defendant
name and jurisdiction.

## 3-File Architecture

| File | Who touches it | Purpose |
|---|---|---|
| `program.md` | Human only | Research directives, constraints, rules |
| `research.py` | **Agent only** | Research methodology — the sandbox |
| `evaluate.py` | Nobody (immutable) | Scores agent output against ground truth |

Supporting files:
- `calibration_data.json` — 38 cases with ground truth (15 ENOUGH, 5 BORDERLINE, 18 INSUFFICIENT)
- `results.tsv` — append-only experiment log

## Setup (Colab)

```python
import os
os.environ["BRAVE_API_KEY"] = "your-brave-key"
os.environ["COURTLISTENER_API_KEY"] = "your-cl-key"  # free: courtlistener.com/sign-in/
os.environ["YOUTUBE_API_KEY"] = "your-yt-key"
# os.environ["MUCKROCK_API_TOKEN"] = "optional"

# Then run:
!python evaluate.py --verbose --log --hypothesis "baseline" --changes "none"
```

## Setup (Claude Code)

```bash
# Set env vars in .env or export them
export BRAVE_API_KEY="..."
export COURTLISTENER_API_KEY="..."
export YOUTUBE_API_KEY="..."

# Baseline run
python evaluate.py --verbose --log --hypothesis "baseline" --changes "none"

# Single case test
python evaluate.py --case 4 --verbose

# The loop (Claude Code /loop)
# Iterate on research.py, run evaluate.py, keep improvements
```

## The metric

`research_score` (0-100) = weighted composite of:
- Evidence type recall (40%) — did you find the right evidence types?
- Source discovery rate (30%) — did you find known source domains?
- Precision (20%) — are your sources actually relevant?
- Tier accuracy (10%) — did you correctly assess confidence level?

## Current phase: Structured APIs (Phase 1)

| API | Endpoint | Rate Limit | Auth |
|---|---|---|---|
| MuckRock | `api_v2/foia/` | 1 req/sec | Optional token |
| CourtListener | `api/rest/v4/search/` | 5 req/min | Free API key |
| YouTube Data v3 | `googleapis.com/youtube/v3/search` | Quota-based | Google API key |
| Brave Search | `api.search.brave.com/res/v1/web/search` | 1 req/sec | API key |

## What the agent should iterate on

- Query construction in `build_muckrock_queries()`
- Result validation in `validate_muckrock_result()`
- Evidence detection keywords in `EVIDENCE_KEYWORDS`
- Confidence thresholds in `assess_confidence()`
- Adding new source types beyond MuckRock
- False positive filtering

## Key lessons from calibration data

The 53 false positives removed during data cleaning reveal common failure modes:
- **Name collision**: "Joseph" → wrong Joseph. "Riley" → Laken Riley. "Dexter" → TV show.
- **Entertainment contamination**: Anime, Spotify, IMDB results for case names.
- **Generic government pages**: County court homepages instead of specific dockets.
- **Wrong jurisdiction**: Same name, different state.

The agent must learn to avoid these patterns. Name specificity + jurisdiction
cross-referencing is critical.

## Training roadmap

1. **Phase 1 (current)**: MuckRock API — structured, controlled
2. **Phase 2**: MuckRock + Oxylabs jurisdiction portal scraping
3. **Phase 3**: Generalized — YouTube/news article → full case dossier
