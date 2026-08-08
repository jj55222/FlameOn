"""
llm_rerank.py — Cross-API result re-ranker for Pipeline 2 discovery
====================================================================

Replaces keyword-based relevance scoring with semantic LLM scoring on the
combined source list from all P2 APIs (MuckRock, CourtListener, Brave,
YouTube, Wikipedia, etc.).

Why:
  Current scoring misses cases like "Officer Smith's body-worn camera footage
  from the September 12 incident" — relevance=0 against a defendant named
  "Maria Garcia" because no name match, even though the article is clearly
  about THE case (just officer-named, not suspect-named). An LLM reading
  title + snippet catches it.

Design choices:
  - ONE LLM call per case (not per-API) — sees full candidate set at once,
    can compare cross-API.
  - Wrapping, not replacing: keyword scores preserved as `_keyword_relevance`
    so we can A/B compare per-source.
  - Graceful degradation: any failure returns sources unchanged, logs WARN.
  - Opt-in via env flag FLAMEON_USE_LLM_RERANK=1 — keeps default keyword
    scoring intact for production runs that haven't validated rerank yet.

CLI:
    # Smoke test against a real case (1 LLM call, ~$0.01)
    python llm_rerank.py --smoke-test

    # Dry run (build prompt, no API call)
    python llm_rerank.py --dry-run

Module use:
    from llm_rerank import rerank_sources
    sources = rerank_sources(case_name, jurisdiction, sources, model="moonshotai/kimi-k2.6")
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
    load_dotenv(Path(__file__).parent.parent / ".env")
    load_dotenv(Path(__file__).parent.parent / "pipeline1_winners" / ".env")
except ImportError:
    pass

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────

DEFAULT_MODEL = os.environ.get("P2_RERANK_MODEL", "moonshotai/kimi-k2.6")
FALLBACK_MODEL = os.environ.get("P2_RERANK_FALLBACK_MODEL", "qwen/qwen3.6-plus")
OPENROUTER_BASE = "https://openrouter.ai/api/v1"
OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY", "")

# Caps to keep prompt sane and cost predictable
MAX_SOURCES_TO_RANK = int(os.environ.get("P2_RERANK_MAX_SOURCES", "50"))
MAX_DESC_CHARS = 200
MAX_URL_CHARS = 120
LLM_TIMEOUT_SEC = 45


# ─────────────────────────────────────────────────────────────
# Prompt construction
# ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert at judging whether web search results are about a specific criminal case.

Your job: score each source 0.0 to 1.0 for relevance to the named defendant and jurisdiction.

Scoring rubric:
  1.0 = clearly about this person and this specific case (defendant named or unambiguous case-id match)
  0.7 = strong match (last-name + jurisdiction + matching offense, or named officer in same incident)
  0.5 = ambiguous mention (name appears but could be a different person, or different case in same jurisdiction)
  0.3 = adjacent (related agency, related case, but not THE case)
  0.0 = irrelevant (different person, entertainment content, generic government page, name collision)

Special cases:
  - Officer-named bodycam articles ABOUT this defendant's incident: 0.7-1.0 (relevant — bodycam is the evidence)
  - News articles mentioning the city + a different person with similar name: 0.0 (name collision)
  - Generic court records pages with no case-specific info: 0.0
  - Wikipedia/IMDB/Spotify entertainment hits: 0.0

Output STRICT JSON only — no prose, no markdown, no thinking blocks. Format:
{"scores": [{"idx": 0, "score": 0.85, "reason": "brief why"}, ...]}"""


USER_TEMPLATE = """CASE
defendant: {name}
jurisdiction: {jurisdiction}

SOURCES TO SCORE (idx, title, description, url):
{source_block}

Return JSON {{"scores": [{{idx, score, reason}}, ...]}} — one entry per source, score 0.0-1.0."""


def _truncate(s, n):
    s = (s or "").strip()
    return s[:n] + "..." if len(s) > n else s


def _format_source_block(sources):
    """Render top sources as a numbered list."""
    lines = []
    for i, s in enumerate(sources):
        title = _truncate(s.get("description") or "", MAX_DESC_CHARS).replace("\n", " ")
        url = _truncate(s.get("url") or "", MAX_URL_CHARS)
        api = s.get("api") or "?"
        lines.append(f"{i}. [{api}] {title}\n   url: {url}")
    return "\n".join(lines)


def build_prompts(case_name, jurisdiction, sources):
    """Build (system, user) prompts. Returns the list of source indices being ranked."""
    rankable = sources[:MAX_SOURCES_TO_RANK]
    user = USER_TEMPLATE.format(
        name=case_name or "(unknown)",
        jurisdiction=jurisdiction or "(unknown)",
        source_block=_format_source_block(rankable),
    )
    return SYSTEM_PROMPT, user, rankable


# ─────────────────────────────────────────────────────────────
# LLM call with fallback chain
# ─────────────────────────────────────────────────────────────

def _strip_json_fences(s):
    if not s:
        return s
    s = re.sub(r"^```(?:json)?\s*\n?", "", s.strip())
    s = re.sub(r"\n?```\s*$", "", s.strip())
    s = re.sub(r"<think>.*?</think>", "", s, flags=re.DOTALL).strip()
    return s


def _call_model(client, model, system, user, max_tokens=3000):
    """Single attempt. Returns parsed dict or raises."""
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.1,
        max_tokens=max_tokens,
        timeout=LLM_TIMEOUT_SEC,
        extra_headers={
            "HTTP-Referer": "https://github.com/jj55222/FlameOn",
            "X-Title": "FlameOn P2 rerank",
        },
    )
    raw = resp.choices[0].message.content or ""
    raw = _strip_json_fences(raw)
    parsed = json.loads(raw)
    return parsed, resp.usage


def call_llm(case_name, jurisdiction, sources, model=None, fallback=None, verbose=False):
    """
    Call LLM rerank with primary→fallback chain.
    Returns (parsed_dict, model_used) or (None, None) on persistent failure.
    """
    try:
        from openai import OpenAI
    except ImportError:
        if verbose:
            print("[rerank] openai SDK not installed; skipping rerank")
        return None, None

    if not OPENROUTER_KEY:
        if verbose:
            print("[rerank] OPENROUTER_API_KEY not set; skipping rerank")
        return None, None

    client = OpenAI(api_key=OPENROUTER_KEY, base_url=OPENROUTER_BASE)
    system, user, rankable = build_prompts(case_name, jurisdiction, sources)
    primary = model or DEFAULT_MODEL
    fallback_model = fallback if fallback is not None else FALLBACK_MODEL

    for attempt_model in [primary, fallback_model]:
        if not attempt_model:
            continue
        try:
            t0 = time.time()
            parsed, usage = _call_model(client, attempt_model, system, user)
            elapsed = time.time() - t0
            if verbose:
                in_t = getattr(usage, "prompt_tokens", "?")
                out_t = getattr(usage, "completion_tokens", "?")
                print(f"[rerank] {attempt_model}: {elapsed:.1f}s  in={in_t} out={out_t}  scores={len(parsed.get('scores', []))}")
            return parsed, attempt_model
        except json.JSONDecodeError as e:
            if verbose:
                print(f"[rerank] {attempt_model}: JSON parse failed: {e}")
        except Exception as e:
            err = str(e)[:200]
            if verbose:
                print(f"[rerank] {attempt_model}: call failed: {err}")
            # On 404/auth errors, immediately try fallback (don't retry primary)
            if any(c in err for c in ("404", "401", "model_not_found", "Invalid model")):
                continue
    return None, None


# ─────────────────────────────────────────────────────────────
# Public API: rerank_sources
# ─────────────────────────────────────────────────────────────

def rerank_sources(case_name, jurisdiction, sources, *, model=None, fallback=None, verbose=False):
    """
    Re-rank a list of source dicts using LLM semantic relevance.

    Each source must have at minimum: url. Other fields (description, api, type)
    feed the prompt for context.

    Returns: same list, with each source's `relevance_score` replaced by the LLM
    score where available. Original keyword score preserved under
    `_keyword_relevance`. Adds `_rerank_reason` and `_rerank_model` fields.

    On any failure the input list is returned unchanged (graceful degradation).
    """
    if not sources:
        return sources

    parsed, model_used = call_llm(case_name, jurisdiction, sources, model=model, fallback=fallback, verbose=verbose)
    if not parsed:
        # Mark sources so downstream can see rerank was attempted but failed
        for s in sources:
            s.setdefault("_rerank_attempted", True)
            s.setdefault("_rerank_succeeded", False)
        return sources

    # Build idx → score map; only top-N were actually ranked
    score_map = {}
    for entry in parsed.get("scores", []) or []:
        try:
            idx = int(entry["idx"])
            score = float(entry["score"])
            reason = (entry.get("reason") or "").strip()[:200]
            score_map[idx] = (max(0.0, min(1.0, score)), reason)
        except (KeyError, ValueError, TypeError):
            continue

    for i, s in enumerate(sources):
        s["_rerank_attempted"] = True
        s["_keyword_relevance"] = s.get("relevance_score", 0.0)
        s["_rerank_model"] = model_used
        if i in score_map:
            new_score, reason = score_map[i]
            s["relevance_score"] = new_score
            s["_rerank_reason"] = reason
            s["_rerank_succeeded"] = True
        else:
            # Source wasn't in top-N (or LLM didn't score it) — leave keyword score
            s["_rerank_succeeded"] = False

    return sources


# ─────────────────────────────────────────────────────────────
# CLI: smoke test + dry-run
# ─────────────────────────────────────────────────────────────

SMOKE_FIXTURE = {
    "case_name": "Min Jian Guan",
    "jurisdiction": "San Francisco, San Francisco, CA",
    "sources": [
        {"url": "https://www.youtube.com/watch?v=ytap7WQnLK4",
         "description": "Bodycam: SFPD officer responds to elder abuse call in Chinatown",
         "api": "youtube_free", "relevance_score": 0.5},
        {"url": "https://en.wikipedia.org/wiki/Gui_Minhai",
         "description": "Gui Minhai - Wikipedia: Hong Kong bookseller detained in China",
         "api": "wikipedia", "relevance_score": 0.4},
        {"url": "https://www.cbsnews.com/sanfrancisco/news/sfpd-elder-assault-arrest",
         "description": "Man, 26, arrested in attack on 79-year-old woman in San Francisco",
         "api": "brave_paid", "relevance_score": 0.5},
        {"url": "https://imdb.com/name/nm0123456",
         "description": "Min Jian - IMDB filmography",
         "api": "brave_paid", "relevance_score": 0.3},
        {"url": "https://sfdpa.nextrequest.com/documents/22-7",
         "description": "DPA Investigation File 22-7 — body-worn camera footage of arrest",
         "api": "nextrequest_portal", "relevance_score": 0.35},
    ],
}


def main():
    parser = argparse.ArgumentParser(description="P2 LLM result re-ranker")
    parser.add_argument("--smoke-test", action="store_true",
                        help="Run rerank against a fixed mock case (1 LLM call)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Build prompts but don't call LLM")
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help=f"Primary model (default: {DEFAULT_MODEL})")
    parser.add_argument("--fallback", default=FALLBACK_MODEL,
                        help=f"Fallback model (default: {FALLBACK_MODEL})")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if not (args.smoke_test or args.dry_run):
        parser.print_help()
        sys.exit(1)

    fix = SMOKE_FIXTURE
    if args.dry_run:
        system, user, rankable = build_prompts(fix["case_name"], fix["jurisdiction"], fix["sources"])
        print(f"[DRY RUN] {len(rankable)} sources would be ranked by {args.model}")
        print(f"\n--- SYSTEM PROMPT ({len(system)} chars) ---\n{system}")
        print(f"\n--- USER PROMPT ({len(user)} chars) ---\n{user}")
        return

    print(f"[SMOKE TEST] Reranking {len(fix['sources'])} mock sources with {args.model}...")
    out = rerank_sources(
        fix["case_name"], fix["jurisdiction"], list(fix["sources"]),
        model=args.model, fallback=args.fallback, verbose=True,
    )
    print(f"\nResults:")
    for s in out:
        kw = s.get("_keyword_relevance", 0.0)
        new = s.get("relevance_score", 0.0)
        ok = s.get("_rerank_succeeded", False)
        reason = s.get("_rerank_reason", "")
        delta = new - kw
        marker = "+" if delta > 0 else ("-" if delta < 0 else "=")
        flag = " " if ok else "!"
        print(f"  {marker}{flag} kw={kw:.2f} → llm={new:.2f}  {s['url'][-50:]}")
        if reason:
            print(f"       reason: {reason[:120]}")


if __name__ == "__main__":
    main()
