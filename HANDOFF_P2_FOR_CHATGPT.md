# FlameOn Pipeline 2 (P2 Discovery) — Self-Contained Handoff for ChatGPT

**Date:** 2026-04-29
**Scope:** Pipeline 2 only. Other FlameOn pipelines (P1 ingest, P3 enrich, P4 brief, P5 publish) are intentionally out of scope.
**Audience:** An external LLM (ChatGPT) with no prior context, no file access, no shell. Everything you need to reason about P2 is inlined below.

---

## 1. What Pipeline 2 is

P2 is a **research agent** that takes `(defendant_name, jurisdiction)` and returns a list of sources with evidence-type tags (bodycam, interrogation, court video, dockets, dispatch audio) and a confidence tier (`high` / `medium` / `low`).

It is trained Karpathy-autoresearch style: a fixed scorer (`evaluate.py`) grades the agent (`research.py`) against 38 hand-curated cases. The agent is iterated on; the scorer is **immutable**.

```
research_case(defendant_names, jurisdiction)
    │
    ├── search_muckrock()       FOIA requests        free
    ├── search_courtlistener()  dockets + opinions   free
    ├── search_brave()          web discovery        $0.005/req
    ├── search_youtube()        yt-dlp search        free
    ├── search_reddit()         PRAW (fallback)      free
    ├── search_wikipedia()                           free
    ├── search_dailymotion()                         free
    │
    ├── apply_identity_scoring()  ← per-source, post-discovery
    ├── jurisdiction_filter (opt) ← rule-based geo gate
    │
    └── assess_confidence(sources, evidence_count) → "high"|"medium"|"low"
```

Files (count of lines):
- `research.py` (1437) — the agent. **Mutable.**
- `evaluate.py` (537) — the scorer. **Immutable.**
- `identity_score.py` (246) — per-source identity verification (cherry-picked from a fork).
- `jurisdiction_filter.py` (293) — geo-conflict filter w/ national-news whitelist.
- `llm_rerank.py` (340) — qwen rerank, currently default-OFF (validated negative).
- `calibration_data.json` — 38 cases (15 ENOUGH, 5 BORDERLINE, 18 INSUFFICIENT). **Frozen ground truth.**
- `run_calibration.py` — wrapper that monkey-patches evaluate.py's hardcoded 1600s time budget to 3600s without modifying it.

---

## 2. The metric

`evaluate.py` computes a composite (0–100) per case, averaged across the 38:

```
composite = 0.40 × evidence_recall      # did you tag the right evidence types?
          + 0.30 × source_discovery     # did you find known source domains?
          + 0.20 × precision            # are your sources actually relevant?
          + 0.10 × tier_accuracy        # high/medium/low matches ground truth?
```

`evidence_count` per case = number of distinct evidence types found among `{bodycam, interrogation, court_video, docket_docs, dispatch_audio}`.

Evidence detection has three paths:
1. **PATH 1** — `source.type` (set by yt-dlp) → evidence-type map (`bodycam_footage` → `bodycam`).
2. **PATH 2** — `EVIDENCE_KEYWORDS` keyword scan over titles/descriptions/URLs.
3. **PATH 3** — Domain check (courtlistener.com, justia.com, etc. → `docket_docs`).

---

## 3. Current state (as of 2026-04-29)

**Best calibration composite:** **63.51** — Exp 33, label `cherry_pick_v1`.

Exp 34 (`cherry_pick_v2`, threshold 3→4) regressed to 62.86 → **proposed action is to revert threshold to 3 and ship Exp 33's config as default.**

Recent experiment log:

| # | Date | Composite | Evidence | SrcDisc | Precision | Tier | Label | Verdict |
|---|---|---:|---:|---:|---:|---:|---|---|
| 29 | 04-26 | 62.85 | 70.67 | 59.46 | 52.79 | 61.84 | rerank_eval_baseline_clean | reference |
| 30 | 04-27 | 44.08 | 42.67 | 37.84 | 53.95 | 48.68 | rerank_eval_variant_qwen | KILL — 26/38 timeouts |
| 31 | 04-27 | 61.40 | 68.00 | 59.46 | 52.85 | 57.89 | filter_iter1 | revert — too aggressive |
| 32 | 04-27 | 21.00 | 13.33 | 8.11 | 50.38 | 31.58 | filter_iter2 | timeout cascade |
| 33 | 04-27 | **63.51** | **72.00** | 59.46 | 51.44 | **65.79** | cherry_pick_v1 (thr=3) | **SHIP** |
| 34 | 04-29 | 62.86 | 69.33 | 59.46 | 52.89 | 67.11 | cherry_pick_v2 (thr=4) | revert |

Currently default-ON in research.py:
- `FLAMEON_USE_JURISDICTION_FILTER=1` — rule-based geo filter w/ national-news whitelist.
- Identity-aware `assess_confidence` — uses `has_full_name_match` + `count_distinct_case_numbers`.
- Multi-case ambiguity gate — currently `n_distinct_cases >= 4`; **Exp 33's winning value was `>= 3`**.

Currently default-OFF (validated net-negative):
- `FLAMEON_USE_LLM_RERANK=0` — qwen rerank demotes legit ENOUGH cases.

---

## 4. The current `assess_confidence` (research.py ~line 1180–1228)

```python
def assess_confidence(sources, evidence_count):
    has_identity_data = any("identity_matched_fields" in s for s in sources)
    if has_identity_data:
        from identity_score import has_full_name_match, count_distinct_case_numbers
        high_relevance = sum(
            1 for s in sources
            if s.get("relevance_score", 0) >= 0.5 and has_full_name_match(s)
        )
        n_distinct_cases = count_distinct_case_numbers(sources)
    else:
        high_relevance = sum(1 for s in sources if s.get("relevance_score", 0) >= 0.5)
        n_distinct_cases = 0

    # Multi-case ambiguity gate. v1 used >=3, v2 raised to >=4 and lost 0.65pt.
    multi_case_ambiguous = n_distinct_cases >= 4   # ← revert candidate

    footage_types = {"bodycam_footage", "interrogation_footage", "court_footage", "dispatch_audio"}
    typed_footage = sum(1 for s in sources if s.get("type", "") in footage_types)
    api_set = set(s.get("api", "") for s in sources if s.get("relevance_score", 0) >= 0.5)
    api_diversity = len(api_set - {""})

    if high_relevance >= 3 and evidence_count >= 3 and typed_footage >= 1 and not multi_case_ambiguous:
        return "high"
    if high_relevance >= 5 and evidence_count >= 4 and api_diversity >= 3 and not multi_case_ambiguous:
        return "high"
    if evidence_count >= 1 and high_relevance >= 1 and len(sources) >= 2:
        return "medium"
    if multi_case_ambiguous and len(sources) >= 5:
        return "medium"
    return "low"
```

---

## 5. Identity scoring (identity_score.py — full module summary)

Per-source identity match against (defendant, jurisdiction). Annotates every source with `identity_score` ∈ [0,1] and `identity_matched_fields` list.

Scoring components (additive, clipped to [0,1]):

| Field | Weight | Match condition |
|---|---:|---|
| `defendant_full_name` | +0.50 | clean primary name (suffix/honorific stripped) appears in title+desc+url |
| `defendant_last_name` | +0.25 | last name only, length>3, word-boundary match (mutually exclusive with above) |
| `city` | +0.18 | jurisdiction city in text |
| `county` | +0.14 | county (with " county" stripped), length>2 |
| `state` | +0.12 | state name in text |
| `authority:court/foia/official` | +0.12 | URL domain hint match |
| `noise_domain` | **−0.50** | imdb/spotify/fandom/amazon/goodreads/etc |
| `entertainment_text` | **−0.30** | "movie"/"trailer"/"anime"/"lyrics"/"soundtrack"/"gameplay" in text |

Two predicates exported:
- `has_full_name_match(source)` → True iff `defendant_full_name` in matched fields.
- `count_distinct_case_numbers(sources, require_full_name=True)` → count of distinct court/agency case numbers (regex: `[A-Z]{1,4}\d{2,4}[- ]?[A-Z]{0,4}[- ]?\d{2,8}`) found in sources that ALSO have a full-name match. This is the **multi-case ambiguity** signal.

---

## 6. The four cases the threshold tuning targets

The Exp 33→34 iteration was specifically about these four. Per-case results from the v2 run:

| Case | Ground truth | v1 (thr=3) | v2 (thr=4) | Goal | Result |
|---|---|---|---|---|---|
| Marvin G. Johnson | ENOUGH | low ✗ | **low** ✗ | recover to high | failed |
| Angela D. McAnulty | ENOUGH | low ✗ | **low** ✗ | recover to high | failed |
| Katelynne Nelson et al | INSUFFICIENT | low ✓ | low ✓ | stay demoted | held |
| Miguel Mondaca | INSUFFICIENT | low ✓ | low ✓ | stay demoted | held |

The threshold raise didn't recover the legit ENOUGH cases. Tier-accuracy went up +1.32 from elsewhere, but evidence-recall dropped −2.67, net composite −0.65.

---

## 7. Persistent failure cases (the floor)

Three INSUFFICIENT cases still get HIGH confidence in v1:

- **Joshua Carrier (Colorado Springs)** — same-city same-name collision; identity scoring matches *a* Joshua Carrier, but the wrong one.
- **William Bracy et al** — 4-co-defendant case; identity hits inflated by multiple legit names.
- **Braulio Gonzalez (Miami)** — same-city collision.

**Root cause:** name+jurisdiction match on a different person at the same place. Identity-anchor scoring can't distinguish.

These cap tier accuracy at ~64–66 unless we add a cross-check (incident date, victim name, or a third disambiguator).

---

## 8. Constraints / invariants

1. **`evaluate.py` is immutable.** Period. Do not propose changes to it. (Time-budget patching happens externally in `run_calibration.py`.)
2. **`calibration_data.json` is frozen.** Never propose ground-truth edits to "make a case work."
3. **Anti-overfit gate:** any change to `research.py` must pass *both* the 38-case calibration (>=63.51 currently) AND the separate UoF benchmark (`uof_benchmark.py`, 15-case bodycam-discovery harness). This rule exists because earlier iterations overfit to one harness while regressing the other.
4. **Auto-revert rule:** if a one-line change drops composite by >1.0pt vs the prior shipped config, revert without further tuning. We're conservative because per-run cost is real (~$2 Brave per calibration).
5. **Don't reverse Brave/YouTube ordering.** Brave-before-YouTube is locked: Brave's snippet text feeds PATH 2 keyword detection; reversing it loses recall.
6. **Don't add `site:youtube.com` to Brave queries.** Tested negative — hurts dedup because Brave returns YouTube URLs typed as `video_footage`, then yt-dlp finds the same URL as `court_footage` and dedup drops the typed-stronger version.
7. **Do not use UoF benchmark for tuning.** Its name-text precision metric biases against semantic rerank. It's a validation harness, not a fitness function.

---

## 9. Tools / APIs

| API | Endpoint | Rate | Cost | Notes |
|---|---|---|---|---|
| MuckRock | `api_v2/foia/` | 1 req/sec | free | optional token |
| CourtListener | `api/rest/v4/search/` | 5 req/min | free | API key required |
| Brave Search | `api.search.brave.com/res/v1/web/search` | 1 req/sec | $0.005/req | hard cap via `brave_quota.json` |
| YouTube (yt-dlp) | InnerTube | ~1 req/sec | free | |
| Reddit (PRAW) | OAuth | 1 req/sec | free | fallback when total_sources<20 |
| Wikipedia | MediaWiki | unlimited | free | |
| DailyMotion | public | unlimited | free | |

Brave billing guard: `BRAVE_SPEND_LIMIT_USD` env (default $5) + persistent `brave_quota.json` reads `x-ratelimit-remaining` after every call. 38-case run ≈ $2.10.

---

## 10. What ChatGPT could usefully reason about

Open questions where outside thinking helps:

1. **Threshold revert sanity check.** Given the data in §3 + §6, do you agree v2 should revert? Any second-order reason to keep thr=4 despite the failed primary objective?

2. **Marvin Johnson + Angela McAnulty are stuck at LOW even at thr=4.** The full-name matches exist (sources counts of 59 and 27 respectively). Why doesn't `assess_confidence` reach `high`? Walk through which gate they fail. Hypothesis: `typed_footage >= 1` may be the real bottleneck, not the multi-case gate.

3. **Joshua Carrier same-city collision.** Without modifying ground truth, how would you propose distinguishing the wrong-Joshua-Carrier sources from the right one? Date-based cross-check? Victim-name? Something cheaper?

4. **Tier-accuracy plateau.** Composite has been hovering at 62–64 across 30+ experiments. Is the ceiling structural (calibration set design) or addressable (better disambiguators)? What signal is the metric *not* rewarding that we should be exploiting?

5. **Cherry-picking the case-graph fork.** A separate fork (`research_case_graph.py`) has a `matched_fields` design more granular than what `identity_score.py` cherry-picked. Worth fully merging? Risk: another 2hr work item with uncertain delta.

What you should NOT propose without strong justification:
- Modifying `evaluate.py` or `calibration_data.json`.
- Re-enabling LLM rerank (validated negative).
- Reversing Brave→YouTube order.
- Adding `site:` operators to Brave queries.

---

## 11. Reproducibility

```bash
# Full calibration with current cherry-picks
cd C:/FlameON/FlameOn-main/pipeline2_discovery
PYTHONIOENCODING=utf-8 BRAVE_SPEND_LIMIT_USD=15.00 \
  FLAMEON_USE_JURISDICTION_FILTER=1 \
  python -u run_calibration.py --verbose --log \
  --hypothesis "label" --changes "what changed" \
  > ../ab_runs/p2_calib_<label>.log 2>&1

# Single-case debug
PYTHONIOENCODING=utf-8 python run_calibration.py --case 35 --verbose

# Inspect latest experiment row
tail -1 pipeline2_discovery/results.tsv
```

`results.tsv` columns: `id  iso_ts  composite  evidence_recall  source_discovery  precision  tier_accuracy  cases_total  cases_completed  wallclock_s  hypothesis  changes`.

---

## 12. TL;DR for ChatGPT

- P2 is a research-agent + immutable-scorer loop. 38-case calibration. Best composite 63.51 (Exp 33, `cherry_pick_v1`).
- Recent threshold raise (3→4) regressed −0.65 and failed its stated goal of recovering Marvin Johnson / Angela McAnulty. Revert proposed.
- Plateau is real: 62–64 band across many experiments. Three INSUFFICIENT cases (Joshua Carrier, William Bracy, Braulio Gonzalez) cap tier-accuracy via same-city same-name collisions.
- Constraints: don't touch evaluate.py, don't touch calibration_data.json, must pass both calibration AND UoF benchmark.
- Open question to you: see §10.
