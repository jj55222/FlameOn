# CLAUDE.md — Pipeline 2: Case Discovery (FlameOn AutoResearch)

## What this pipeline does

An AutoResearch loop that trains a **research agent** to find case artifacts
(bodycam footage, interrogation recordings, court video, docket docs, 911 audio)
given only a defendant name and jurisdiction. Outputs ranked cases with typed,
downloadable source URLs.

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

# Full run:
!python evaluate.py --verbose --log --hypothesis "baseline" --changes "none"

# Single case:
!python evaluate.py --case 4 --verbose
```

## Setup (Claude Code)

```bash
export BRAVE_API_KEY="..."
export COURTLISTENER_API_KEY="..."
export YOUTUBE_API_KEY="..."

# Baseline run
python evaluate.py --verbose --log --hypothesis "baseline" --changes "none"

# The loop (Claude Code /loop)
# Iterate on research.py, run evaluate.py, keep improvements
```

## The metric

`research_score` (0-100) = weighted composite of:
- Evidence type recall (40%) — did you find the right evidence types?
- Source discovery rate (30%) — did you find known source domains?
- Precision (20%) — are your sources actually relevant?
- Tier accuracy (10%) — did you correctly assess confidence level?

**Current best:** 63.73 (experiment 45)
**Target:** 75-80 + functional test (7-8/10 ENOUGH cases yield actionable source URLs)

## Current phase: Structured APIs (Phase 1)

| API | Endpoint | Rate Limit | Auth |
|---|---|---|---|
| MuckRock | `api_v2/foia/` | 1 req/sec | Optional token |
| CourtListener | `api/rest/v4/search/` | 5 req/min | Free API key |
| YouTube Data v3 | `googleapis.com/youtube/v3/search` | Quota-based | Google API key |
| Brave Search | `api.search.brave.com/res/v1/web/search` | 1 req/sec | API key |

## Current blockers (April 2026)

- [ ] Brave quota reset (April) — rerun with full 38-case coverage
- [ ] Exp 50 incomplete — Jr./Sr. parse fix applied but no full run yet
- [ ] Per-case Brave budget distribution — cases 14-38 get zero coverage
- [ ] Tier accuracy plateau on INSUFFICIENT cases (4, 7, 14, 17, 35, 36)

## CRITICAL: Downstream data contract (P2 → P3)

Pipeline 3 needs **actual downloadable URLs** typed by evidence category.
The `research_case()` output MUST include a `sources` array matching the
`p2_to_p3_case` schema in `../schemas/contracts.json`.

Each source needs:
- `url` — direct link (not a search results page)
- `evidence_type` — bodycam, interrogation, court_video, 911_audio, etc.
- `format` — video, audio, document, webpage
- `requires_download` — true if yt-dlp needed
- `source_domain` — where it came from

This is the key improvement needed beyond the score: **source actionability**.

## What the agent should iterate on

- Query construction in `build_muckrock_queries()`
- Result validation in `validate_muckrock_result()`
- Evidence detection keywords in `EVIDENCE_KEYWORDS`
- Confidence thresholds in `assess_confidence()`
- Brave per-case budget distribution (spread across all 38 cases)
- Source URL typing and actionability
- False positive filtering

## Key lessons from calibration data

Common failure modes from 53 false positives removed during data cleaning:
- **Name collision**: "Joseph" → wrong Joseph. "Riley" → Laken Riley. "Dexter" → TV show.
- **Entertainment contamination**: Anime, Spotify, IMDB results for case names.
- **Generic government pages**: County court homepages instead of specific dockets.
- **Wrong jurisdiction**: Same name, different state.

## Jurisdiction Portal Scraper (Phase 2 component)

Many law enforcement agencies publish bodycam footage, use-of-force reports,
and transparency data on their own portals. These are high-value, often missed
by search APIs.

### `portal_scraper.py` — Jurisdiction Portal Crawler

**Tool:** Firecrawl (free tier: 500 credits/month)
- Crawl agency transparency portals, DA video pages, sheriff media pages
- Extract structured content (video URLs, document links, case references)
- Handle inconsistent government site structures
- Map site to discover all published footage pages

**Known portal patterns to target:**
- `{city/county}.gov/police/bodycam` or `/transparency`
- `{da-office}.org/officer-involved-shootings`
- `{sheriff}.gov/media-releases`
- YouTube channels of PD media relations offices

**CLI:**
```bash
python portal_scraper.py --agency "Harris County Sheriff" --output portals/
python portal_scraper.py --agency-list agencies.txt --dry-run
```

**Integration:** Portal scraper feeds into `research.py` — discovered URLs get
added to the case's source list with `evidence_type` and `format` fields per
the P2→P3 contract.

### Supplemental search: Exa API

Exa (free tier) supplements Brave for case discovery. Particularly strong for:
- News article search (reduces Brave quota burn)
- People/entity lookup
- Academic/legal document search

Add as optional fallback in `research.py` when Brave quota is running low.

**Environment variable:**
```
FIRECRAWL_API_KEY=...  # firecrawl.dev, free tier
EXA_API_KEY=...        # exa.ai, free tier
```

## Training roadmap

1. **Phase 1 (current)**: MuckRock + CourtListener + Brave + YouTube — structured APIs
2. **Phase 2 (next)**: Add Firecrawl jurisdiction portal scraping + Exa supplemental search
3. **Phase 3**: Generalized — YouTube/news article → full case dossier
