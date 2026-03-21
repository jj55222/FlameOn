# FlameOn — Handoff & Development Plan

**Branch:** `claude/research-case-search-api-QCKBP`
**Last updated:** 2026-03-21

---

## What This Is

A pipeline that discovers, validates, and archives public court records and media for criminal cases sourced from law enforcement YouTube channels. It runs in Google Colab notebooks.

## Architecture

### Pipeline Stages

```
Stage 1: INTAKE        — Scan YouTube channels, extract suspect names, create CaseCandidate rows
Stage 1.5: REEXTRACT   — Re-run name extraction on existing rows (zero YT API cost)
Stage 2: VALIDATION    — Brave search + LLM to determine if case is CLOSED (sentenced/convicted)
Stage 3A: DISCOVERY    — Find court docs, news, BWC links for validated cases
Stage 3B: DOWNLOAD     — Fetch discovered artifacts to local storage
```

### Source Files

| File | Purpose |
|------|---------|
| `src/main.py` | Pipeline orchestrator, stage runners, name extraction (regex + LLM) |
| `src/intake.py` | YouTube channel scanning, video classification, name extraction helpers |
| `src/validation.py` | Brave search + LLM closure detection (CLOSED/OPEN/AMBIGUOUS) |
| `src/discovery.py` | Link discovery — Brave court/news/BWC searches, link classification |
| `src/case_search.py` | CourtListener API enrichment, direct portal URL construction, case number queries |
| `src/download.py` | Artifact downloader (PDFs, pages, etc.) |
| `src/models.py` | Data models: `CaseCandidate`, `DiscoveredLink`, `LinkInventory`, `SourceRank`, enums |
| `src/sheet.py` | Google Sheets integration (read/write case rows) |
| `src/storage.py` | Local file storage (folder creation, path management) |
| `src/dedup.py` | Deduplication logic |
| `src/logger.py` | Logging setup |
| `config/settings.yaml` | API keys and configuration |

### External APIs

| API | Used In | Purpose |
|-----|---------|---------|
| **Brave Search** | validation.py, discovery.py | Web search for court records, news, BWC footage |
| **CourtListener** | case_search.py | Federal court opinions, RECAP dockets, case numbers |
| **OpenRouter** (Gemini Flash) | main.py, validation.py | LLM name extraction, closure detection |
| **YouTube Data API** | intake.py | Channel video scanning |
| **Google Sheets API** | sheet.py | Case tracking spreadsheet |
| `exa_api_key` | (placeholder in settings.yaml) | Reserved for future Phase 1b semantic search |

---

## Discovery Flow (Stage 3A) — Current State

```
Phase 0:  CourtListener enrichment → case numbers, docket IDs, citations
Phase 0b: Direct portal URL construction (zero API cost) — from case numbers
Phase 1:  Brave → state-specific court/docket queries
Phase 1b: Brave → case-number-enhanced queries (only if Phase 0 found numbers)
Phase 2:  Brave → news articles
Phase 3:  Brave → BWC/interrogation footage
```

### Operation Name Handling

Cases with `suspect_name` = "Operation X" (multi-defendant stings):
- **Phase 0, 0b, 1:** SKIP — no court filing named "Operation Community Shield"
- **Phase 2 (news):** RUN — news articles reference operations by name
- **Phase 3 (BWC):** RUN — agencies tag footage by operation name
- Detection: `_is_operation_name()` regex `^operation\s+`

### State-Relevance Gate (just added)

Prevents wrong-person corroboration. Before marking a link as `official_corroboration=True`:
1. **State .gov URL check** — `snohomishcountywa.gov` for a FL case → rejected
2. **USAO district code** — `justice.gov/usao-sdga` for an AZ case → rejected (GA district)
3. **State name in snippet** — "sentenced in Georgia" for an AZ case → rejected

Logs `[corroboration-rejected]` with reason. Applied to `discover_court_links()` and `discover_case_number_links()`.

---

## Known Issues & Next Steps

### HIGH PRIORITY

1. **Duplicate suspect work** — Bruce Whitehead has 2 videos → discovery runs identical Brave queries twice (28 links each). Need dedup by suspect_name before discovery, or cache results per suspect.

2. **Phase 0b never fires** — Direct portal URL construction requires case numbers from CourtListener, but most state-level cases aren't in CL (it's mostly federal). Need an alternative case number source — possibly extract from news article text during Phase 2, then loop back.

3. **Operation cases need individual names** — "Operation Community Shield" videos likely mention individual defendants in descriptions. The pipeline sets `suspect_name` = operation name when no individual is extractable. Could try harder extraction from video descriptions, or search news for the operation name to find individual names.

4. **Corroboration `not_found` for valid cases** — Nelson Odige, Dallas Francis show `not_found` despite having news links. The corroboration check only looks at `.gov`/clerk URLs. Consider allowing strong news corroboration (e.g., multiple independent news sources confirming sentencing) as a secondary tier.

### MEDIUM PRIORITY

5. **CourtListener returns 0 for most state cases** — CL is primarily federal courts + some appellate. For pure state criminal cases, it often returns nothing. Consider adding state-specific court search APIs or scraping strategies.

6. **Direct portal URLs (Phase 0b) are speculative** — Most county portals are form-based, not direct-linkable. The URL templates in `COUNTY_PORTAL_URLS` are best guesses. Need to validate which ones actually work at download time and prune dead templates.

7. **Exa API integration** — `exa_api_key` placeholder added to settings. Exa's semantic search could corroborate cases by finding articles about sentencing without exact name matching. Good for cases with common names.

### LOW PRIORITY / FUTURE

8. **Download stage (3B)** — Not yet tested with the new Phase 0b URLs. Need to handle 404s gracefully and log hit/miss rates for direct portal URLs.

9. **Case number extraction from news** — When Brave Phase 2 finds news articles that mention case numbers (e.g., "Case No. CR2024-001234"), extract those and feed back into Phase 0b/1b for targeted court portal searches.

10. **Batch sheet updates** — Currently updates sheet per-case. Could batch for performance.

---

## Recent Commit History

```
1158433 Add state-relevance gate to prevent wrong-person corroboration matches
6860e7f Only skip court docket searches for operation names, keep news + BWC
574f2d2 Skip all Brave discovery phases for operation names (not just CourtListener)
f11c70a Add Phase 0b: direct portal URL construction from case numbers (zero API cost)
e0c24f4 Fix case search enrichment: replace dead case.law API, filter civil dockets, skip operations
bc42cdd Harden intake filters to reject non-case content, victim names, and entity misattribution
dbbf0d6 Fix Sheriff/Chief name misattribution + filter junk URLs from discovery
4d9e133 Reject cold cases entirely instead of labeling as VICTIM
ab5904f Improve date extraction, add cold case detection, enrich operation metadata
```

---

## Running in Colab

Reset/reload block (run after git pull):
```python
import subprocess, sys
subprocess.run(["git", "-C", "/content/FlameOn", "pull", "origin", "claude/research-case-search-api-QCKBP"], check=True)
for mod_name in sorted(sys.modules.keys()):
    if mod_name == "src" or mod_name.startswith("src."):
        del sys.modules[mod_name]
from src.main import run_pipeline, stage_reextract_names
```

If local edits conflict:
```python
subprocess.run(["git", "-C", "/content/FlameOn", "reset", "--hard", "origin/claude/research-case-search-api-QCKBP"], check=True)
```

---

## Key Design Decisions

- **No downloads in discovery** — Stage 3A only inventories links. Stage 3B downloads. This keeps discovery fast and lets us review before fetching.
- **Conservative corroboration** — Only `.gov` and county clerk URLs count as official. News = supporting evidence only.
- **Operation-aware** — Pipeline distinguishes between person cases and operation/sting cases throughout.
- **State-aware search** — Each state has custom Brave query templates targeting its specific court portal domains.
- **Logging-heavy** — Every phase logs decisions, skips, and rejections with prefixes like `[Phase 0b]`, `[corroboration-rejected]`, `[direct-url]` so we can grep and tune.
