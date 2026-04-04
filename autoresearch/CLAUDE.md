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
- `brave_quota.json` — auto-generated; persists Brave API billing state across runs (do not delete mid-month)

## Setup (Colab)

```python
import os
os.environ["BRAVE_API_KEY"] = "your-brave-key"
os.environ["COURTLISTENER_API_KEY"] = "your-cl-key"  # free: courtlistener.com/sign-in/
os.environ["BRAVE_SPEND_LIMIT_USD"] = "4.00"          # hard billing cap (default $4.00)
# os.environ["MUCKROCK_API_TOKEN"] = "optional"

# Then run:
!python evaluate.py --verbose --log --hypothesis "baseline" --changes "none"
```

## Setup (Claude Code)

```bash
# Set env vars in .env or export them
export BRAVE_API_KEY="..."
export COURTLISTENER_API_KEY="..."
export BRAVE_SPEND_LIMIT_USD="4.00"   # optional override, default is $4.00

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

## Current best score: 63.73 (exp 45)

Score trajectory since Brave API was added (exps 40→50):
```
Exp 40: 62.63  baseline with Brave
Exp 44: 63.06  broad court_video keywords added
Exp 45: 63.73  assess_confidence tuned (current best)
Exp 46: 61.17  REGRESSION — removed broad keywords (reverted)
Exp 47: 62.51  site:youtube.com Brave query hurt dedup (reverted)
Exp 49: 62.51  YouTube-before-Brave order hurt recall (reverted)
Exp 50: pending  parse_names Jr./Sr. suffix fix (run incomplete)
```

## Current phase: Structured APIs (Phase 1)

| API | Endpoint | Rate Limit | Auth | Cost |
|---|---|---|---|---|
| MuckRock | `api_v2/foia/` | 1 req/sec | Optional token | Free |
| CourtListener | `api/rest/v4/search/` | 5 req/min | Free API key | Free |
| YouTube (yt-dlp) | Internal InnerTube | ~1 req/sec | None | Free |
| Brave Search | `api.search.brave.com/res/v1/web/search` | 1 req/sec | API key | $0.005/req |
| Reddit (PRAW) | OAuth API | 1 req/sec | Client ID + Secret | Free |
| Wikipedia | MediaWiki API | Unlimited | None | Free |
| DailyMotion | Public API | Unlimited | None | Free |

## Brave billing guard

Brave is the only paid API. A persistent quota tracker (`brave_quota.json`) enforces a hard spend cap:

- **Dollar cap**: `BRAVE_SPEND_LIMIT_USD` env var (default `$4.00`). Blocks calls once `estimated_spend + $0.005 > cap`.
- **Monthly quota**: Brave's `x-ratelimit-remaining` header (second value) is read after every call and persisted. If it reaches 0, all subsequent calls are blocked without hitting the network.
- **402 handling**: If Brave returns HTTP 402 (quota exceeded), `monthly_remaining` is set to 0 and saved — no further calls that run.
- **Auto-reset**: `brave_quota.json` month key is checked on every call; spend counter resets automatically when the calendar month changes.

```
brave_quota.json structure:
{
  "month_key": "2026-04",
  "monthly_remaining": 1234,   # from x-ratelimit-remaining header
  "estimated_spend": 1.23,     # cumulative at $0.005/request
  "calls_this_month": 246
}
```

**Why the $57 overspend happened**: The old `BRAVE_MAX_CALLS_PER_RUN = 150` was a local Python counter reset once per `evaluate.py` run with no connection to Brave's billing API. With 50+ experiment runs × up to 150 calls each, 11,416 paid requests accumulated. The new tracker persists across runs and enforces a real dollar ceiling.

## Pipeline architecture (current)

`research_case()` calls APIs in this order per case:
1. **MuckRock** — FOIA requests (3 queries, 1 req/sec)
2. **CourtListener** — dockets + opinions (2 queries × 2 types, 3 sec/req)
3. **Brave Search** — web discovery (11 queries, count=6, 1.1 sec/req) — **paid**
4. **YouTube (yt-dlp)** — footage search (9 queries, ytsearch5, 1 sec/req)
5. **Reddit (PRAW)** — fallback if `total_sources < 20` (2 queries)
6. Wikipedia + DailyMotion — supplemental free sources

Evidence detection has 3 paths:
- **PATH 1**: `source.type` → evidence type map (e.g. `bodycam_footage` → `bodycam`)
- **PATH 2**: `EVIDENCE_KEYWORDS` keyword scan over all source descriptions + URLs
- **PATH 3**: Docket domain URL check (courtlistener, justia, etc. → `docket_docs`)

## What the agent should iterate on

- Query construction in `search_brave()`, `search_youtube()`, `search_muckrock()`
- Result validation / relevance scoring per source
- Evidence detection keywords in `EVIDENCE_KEYWORDS`
- Confidence thresholds in `assess_confidence()`
- Source ordering / dedup strategy (Brave before YouTube matters — richer descriptions enable PATH 2 hits)
- False positive filtering (jurisdiction cross-referencing is critical)

## Key lessons from calibration data

**Name parsing:**
- `parse_names()` now skips trailing generational suffixes (Jr., Sr., III, II) when extracting `last_name`. Without this, "William James McElroy Jr." → `last_name = "Jr."` → 2 sources found instead of 25.

**False positive patterns (53 removed during data cleaning):**
- **Name collision**: "Joseph" → wrong Joseph. "Riley" → Laken Riley. "Dexter" → TV show.
- **Entertainment contamination**: Anime, Spotify, IMDB results for case names.
- **Generic government pages**: County court homepages instead of specific dockets.
- **Wrong jurisdiction**: Same name, different state.

**Core tension — evidence recall vs tier accuracy:**
- Broad `court_video` keywords ("trial", "convicted", "sentenced", etc.) are essential for evidence recall of ENOUGH cases (+2.67 pts in exp 44) but inflate `evidence_count` for INSUFFICIENT cases → over-confident tier predictions.
- Cannot remove broad keywords without sacrificing ~6.67 recall points. Accept the tradeoff.

**Brave dedup order:**
- Brave runs BEFORE YouTube in `research_case()`. Brave can return YouTube URLs typed as `video_footage`, which get registered first. yt-dlp then finds the same URL as `court_footage` — dedup discards it, losing PATH 1 evidence detection.
- However, reversing order (YouTube before Brave) also hurts recall because Brave's description snippets contain richer text enabling more PATH 2 keyword matches.
- Current order (Brave first) is the better tradeoff. Never add `site:youtube.com` to Brave queries.

**Brave budget coverage gap:**
- With `BRAVE_MAX_CALLS_PER_RUN = 150` and 11 queries × 38 cases = 418 needed, only cases 1–13 got full Brave coverage per run. Cases 14–38 had zero Brave results throughout all experiments. This is now partially addressed by the billing guard reducing wasted repeat runs; a per-case cap is a future improvement.

**`assess_confidence()` current tuning (exp 45):**
- HIGH: `high_relevance >= 3 AND evidence_count >= 3`
- MEDIUM: `evidence_count >= 1 AND high_relevance >= 1 AND len(sources) >= 2`
- LOW: everything else
- Removing the old `len(sources) >= 25` HIGH fallback fixed a false-HIGH for sparse-source cases.
- Adding `len(sources) >= 2` for MEDIUM fixed single-source cases being stuck at LOW.

## Training roadmap

1. **Phase 1 (current)**: Structured APIs — MuckRock, CourtListener, Brave, YouTube, Reddit
2. **Phase 2**: MuckRock + Oxylabs jurisdiction portal scraping
3. **Phase 3**: Generalized — YouTube/news article → full case dossier

## Known open issues

- **Brave monthly quota exhausted** as of late March 2026 — resets when the billing month rolls over. All Brave calls currently return [] (blocked by quota tracker).
- **Exp 50 incomplete** — parse_names Jr./Sr. fix applied but full 38-case run not completed. Re-run to get official score.
- **Cases 14–38 Brave gap** — per-case call budget (not just per-run) would distribute Brave queries more evenly across all cases.
- **Tier accuracy plateau** — INSUFFICIENT cases 4, 7, 14, 17, 35, 36 consistently get HIGH confidence (should be LOW). Fixing requires either stricter evidence thresholds (hurts recall) or a jurisdiction-aware false-positive filter.
