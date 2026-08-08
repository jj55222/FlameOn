"""
evaluate.py — FlameOn AutoResearch Evaluation Harness
=====================================================
IMMUTABLE. The agent does not modify this file.

Loads calibration_data.json, runs research.py against each case,
and produces a single research_score (0-100).

Usage:
    python evaluate.py                  # Full evaluation
    python evaluate.py --case 1         # Single case by ID
    python evaluate.py --tier ENOUGH    # Only ENOUGH-tier cases
    python evaluate.py --verbose        # Show per-case breakdown
"""

import json
import time
import argparse
import sys
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse

# ──────────────────────────────────────────────────────────────
# CONSTANTS — do not modify
# ──────────────────────────────────────────────────────────────

CALIBRATION_PATH = Path(__file__).parent / "calibration_data.json"
RESULTS_PATH = Path(__file__).parent / "results.tsv"
TIME_BUDGET_SECONDS = 1600  # 20 minutes max per full evaluation run

WEIGHTS = {
    "evidence_recall": 0.40,
    "source_discovery": 0.30,
    "precision": 0.20,
    "tier_accuracy": 0.10,
}

EVIDENCE_TYPES = ["bodycam", "interrogation", "court_video", "docket_docs", "dispatch_911"]

# Mapping from ground truth YES/NO/MAYBE to numeric
EVIDENCE_MAP = {"YES": 1.0, "MAYBE": 0.5, "NO": 0.0}


# ──────────────────────────────────────────────────────────────
# Calibration loader
# ──────────────────────────────────────────────────────────────

def load_calibration():
    with open(CALIBRATION_PATH) as f:
        return json.load(f)


# ──────────────────────────────────────────────────────────────
# Research runner — calls into research.py
# ──────────────────────────────────────────────────────────────

def run_research(case):
    """
    Calls research.py's research_case() function with ONLY the input
    the agent is allowed to see: defendant_names and jurisdiction.

    Returns the agent's structured output (dict) or None on failure.
    """
    # Import research module (agent modifies this file)
    # Force reimport to pick up changes
    if "research" in sys.modules:
        del sys.modules["research"]

    try:
        from research import research_case

        result = research_case(
            defendant_names=case["defendant_names"],
            jurisdiction=case["jurisdiction"],
        )
        return result

    except Exception as e:
        print(f"  ERROR researching {case['defendant_names']}: {e}")
        return None


# ──────────────────────────────────────────────────────────────
# Scoring functions
# ──────────────────────────────────────────────────────────────

def score_evidence_recall(cases, results):
    """
    For ENOUGH-tier cases: did the agent correctly identify which
    evidence types exist? Compares agent's boolean flags against
    ground truth YES/NO/MAYBE values.

    Returns score 0-100.
    """
    enough_cases = [c for c in cases if c["tier"] == "ENOUGH"]
    if not enough_cases:
        return 0.0

    total_checks = 0
    correct = 0.0

    for case in enough_cases:
        result = results.get(case["case_id"])
        if result is None:
            total_checks += len(EVIDENCE_TYPES)
            continue

        gt = case["ground_truth"]
        agent_evidence = result.get("evidence_found", {})

        for etype in EVIDENCE_TYPES:
            gt_val = EVIDENCE_MAP.get(gt.get(etype, "NO"), 0.0)
            agent_val = 1.0 if agent_evidence.get(etype, False) else 0.0

            total_checks += 1

            # Correct if both agree it exists or both agree it doesn't
            if gt_val >= 0.5 and agent_val == 1.0:
                correct += 1.0  # True positive
            elif gt_val == 0.0 and agent_val == 0.0:
                correct += 1.0  # True negative
            elif gt_val == 0.5 and agent_val == 0.0:
                correct += 0.5  # MAYBE missed — half credit

    return (correct / total_checks * 100) if total_checks > 0 else 0.0


def score_source_discovery(cases, results):
    """
    For ENOUGH-tier cases: what fraction of known verified source
    domains did the agent independently discover?

    We compare at the DOMAIN level, not exact URL, because the agent
    may find the same source via a different path.

    Returns score 0-100.
    """
    enough_cases = [c for c in cases if c["tier"] == "ENOUGH"]
    if not enough_cases:
        return 0.0

    total_known = 0
    found = 0

    for case in enough_cases:
        result = results.get(case["case_id"])
        gt_sources = case["ground_truth"].get("verified_sources", [])

        if not gt_sources:
            continue

        # Extract domains from ground truth
        gt_domains = set()
        for url in gt_sources:
            try:
                domain = urlparse(url).netloc.replace("www.", "")
                if domain:
                    gt_domains.add(domain)
            except Exception:
                continue

        total_known += len(gt_domains)

        if result is None:
            continue

        # Extract domains from agent's findings
        agent_sources = result.get("sources_found", [])
        agent_domains = set()
        for src in agent_sources:
            url = src if isinstance(src, str) else src.get("url", "")
            try:
                domain = urlparse(url).netloc.replace("www.", "")
                if domain:
                    agent_domains.add(domain)
            except Exception:
                continue

        # Count domain overlaps
        found += len(gt_domains & agent_domains)

    return (found / total_known * 100) if total_known > 0 else 0.0


def score_precision(cases, results):
    """
    Across ALL cases: what fraction of the agent's returned sources
    are actually relevant to the case?

    A source is considered a false positive if:
    - Domain matches a known entertainment/unrelated pattern
    - Agent claims evidence for an INSUFFICIENT case with no ground truth support

    Returns score 0-100 (100 = perfect precision, 0 = all garbage).
    """
    total_returned = 0
    likely_valid = 0

    # Known false-positive domain patterns
    entertainment_domains = {
        "soapcentral.com", "imdb.com", "tvguide.com", "spotify.com",
        "invubu.com", "viberate.com", "showtime.com",
    }

    for case in cases:
        result = results.get(case["case_id"])
        if result is None:
            continue

        agent_sources = result.get("sources_found", [])
        if not agent_sources:
            continue

        gt_sources = case["ground_truth"].get("verified_sources", [])
        gt_domains = set()
        for url in gt_sources:
            try:
                gt_domains.add(urlparse(url).netloc.replace("www.", ""))
            except Exception:
                pass

        for src in agent_sources:
            url = src if isinstance(src, str) else src.get("url", "")
            total_returned += 1

            try:
                domain = urlparse(url).netloc.replace("www.", "")
            except Exception:
                continue

            # Check against known bad patterns
            if domain in entertainment_domains:
                continue  # false positive

            # If we have ground truth sources and this domain matches, it's valid
            if gt_domains and domain in gt_domains:
                likely_valid += 1
            elif not gt_domains:
                # No ground truth to check against — give benefit of doubt
                # if it looks like a legitimate source
                if any(legit in domain for legit in [
                    "courtlistener", "casetext", "justia", "findlaw",
                    "pacer", "muckrock", "documentcloud", "gov",
                    "courts", "docket", "youtube.com", "courttv",
                ]):
                    likely_valid += 1
            else:
                # Has ground truth but domain doesn't match — check if plausible
                if any(legit in domain for legit in [
                    "courtlistener", "casetext", "justia", "findlaw",
                    "pacer", "muckrock", "documentcloud", ".gov",
                    "courts", "docket",
                ]):
                    likely_valid += 0.5  # Plausible but unverified

    return (likely_valid / total_returned * 100) if total_returned > 0 else 50.0


def score_tier_accuracy(cases, results):
    """
    Did the agent correctly classify cases into confidence tiers?

    ENOUGH cases should get high confidence.
    INSUFFICIENT cases should get low confidence.
    Calling INSUFFICIENT → ENOUGH is heavily penalized.

    Returns score 0-100.
    """
    total = 0
    correct = 0.0

    tier_map = {"ENOUGH": "high", "BORDERLINE": "medium", "INSUFFICIENT": "low"}

    for case in cases:
        result = results.get(case["case_id"])
        gt_tier = case["tier"]
        total += 1

        if result is None:
            # No result — correct for INSUFFICIENT, wrong for ENOUGH
            if gt_tier == "INSUFFICIENT":
                correct += 0.5  # Half credit — should still return something
            continue

        agent_confidence = result.get("confidence", "medium")

        gt_level = tier_map.get(gt_tier, "medium")

        if agent_confidence == gt_level:
            correct += 1.0
        elif gt_tier == "INSUFFICIENT" and agent_confidence == "high":
            correct += 0.0  # Worst error: false confidence on a dry hole
        elif gt_tier == "ENOUGH" and agent_confidence == "low":
            correct += 0.25  # Underconfident on a known good case
        else:
            correct += 0.5  # Adjacent tier — partial credit

    return (correct / total * 100) if total > 0 else 0.0


# ──────────────────────────────────────────────────────────────
# Main evaluation loop
# ──────────────────────────────────────────────────────────────

def evaluate(case_filter=None, tier_filter=None, verbose=False):
    """Run full evaluation and return composite research_score."""

    cases = load_calibration()

    # Apply filters
    if case_filter is not None:
        cases = [c for c in cases if c["case_id"] == case_filter]
    if tier_filter:
        cases = [c for c in cases if c["tier"] == tier_filter]

    if not cases:
        print("No cases match filter criteria.")
        return None

    print(f"FlameOn AutoResearch Evaluation")
    print(f"{'='*50}")
    print(f"Cases: {len(cases)} | Time budget: {TIME_BUDGET_SECONDS}s")
    print(f"Tiers: {sum(1 for c in cases if c['tier']=='ENOUGH')} ENOUGH, "
          f"{sum(1 for c in cases if c['tier']=='BORDERLINE')} BORDERLINE, "
          f"{sum(1 for c in cases if c['tier']=='INSUFFICIENT')} INSUFFICIENT")
    print()

    # Run research on each case
    results = {}
    start_time = time.time()

    # Reset API budget counters for this run
    try:
        from research import reset_budget, get_budget_report
        reset_budget()
    except ImportError:
        pass

    for i, case in enumerate(cases):
        elapsed = time.time() - start_time
        if elapsed > TIME_BUDGET_SECONDS:
            print(f"  TIME BUDGET EXCEEDED at case {i}/{len(cases)}")
            break

        defendant = case["defendant_names"][:40]
        print(f"  [{i+1}/{len(cases)}] Researching: {defendant}...", end=" ", flush=True)

        case_start = time.time()
        result = run_research(case)
        case_time = time.time() - case_start

        if result:
            results[case["case_id"]] = result
            n_sources = len(result.get("sources_found", []))
            confidence = result.get("confidence", "?")
            print(f"done ({case_time:.1f}s, {n_sources} sources, {confidence} confidence)")
        else:
            print(f"FAILED ({case_time:.1f}s)")

    total_time = time.time() - start_time

    # Score
    print(f"\n{'='*50}")
    print(f"Scoring ({len(results)}/{len(cases)} cases researched, {total_time:.1f}s total)")
    print()

    ev_recall = score_evidence_recall(cases, results)
    src_disc = score_source_discovery(cases, results)
    prec = score_precision(cases, results)
    tier_acc = score_tier_accuracy(cases, results)

    research_score = (
        ev_recall * WEIGHTS["evidence_recall"]
        + src_disc * WEIGHTS["source_discovery"]
        + prec * WEIGHTS["precision"]
        + tier_acc * WEIGHTS["tier_accuracy"]
    )

    print(f"  Evidence Recall:   {ev_recall:6.2f} (x{WEIGHTS['evidence_recall']:.0%})")
    print(f"  Source Discovery:  {src_disc:6.2f} (x{WEIGHTS['source_discovery']:.0%})")
    print(f"  Precision:         {prec:6.2f} (x{WEIGHTS['precision']:.0%})")
    print(f"  Tier Accuracy:     {tier_acc:6.2f} (x{WEIGHTS['tier_accuracy']:.0%})")
    print(f"  {'─'*40}")
    print(f"  RESEARCH SCORE:    {research_score:6.2f} / 100")

    # ── Efficiency Report ──
    api_cost = 0.0
    budget = {}
    try:
        budget = get_budget_report()
    except Exception:
        pass

    # Cost estimation per API
    brave_calls = budget.get("brave", 0)
    cl_calls = budget.get("courtlistener", 0)
    yt_calls = budget.get("youtube", 0)
    mr_calls = budget.get("muckrock", 0)

    # Brave: ~$0.005/query on paid tier, free tier = $0
    brave_cost = brave_calls * 0.005
    # Everything else is free
    api_cost = brave_cost

    total_calls = sum(budget.values()) if budget else 0
    score_per_call = research_score / total_calls if total_calls > 0 else 0
    score_per_dollar = research_score / api_cost if api_cost > 0 else float('inf')

    print(f"\n  {'─'*40}")
    print(f"  EFFICIENCY REPORT")
    print(f"  API calls:    MR={mr_calls} CL={cl_calls} Brave={brave_calls} YT={yt_calls} (free)")
    print(f"  Est. cost:    ${api_cost:.2f}")
    print(f"  Score/call:   {score_per_call:.3f}")
    print(f"  Score/$:      {'∞ (free)' if api_cost == 0 else f'{score_per_dollar:.1f}'}")
    print(f"  Time:         {total_time:.0f}s ({total_time/len(cases):.1f}s/case)")

    # Verbose per-case breakdown
    if verbose:
        print(f"\n{'='*50}")
        print("Per-case breakdown:\n")
        for case in cases:
            result = results.get(case["case_id"])
            gt = case["ground_truth"]
            name = case["defendant_names"][:35]
            tier = case["tier"]

            if result:
                ev = result.get("evidence_found", {})
                srcs = result.get("sources_found", [])
                conf = result.get("confidence", "?")

                ev_flags = []
                for etype in EVIDENCE_TYPES:
                    gt_val = gt.get(etype, "NO")
                    agent_val = "Y" if ev.get(etype, False) else "N"
                    match = "✓" if (gt_val in ("YES", "MAYBE") and agent_val == "Y") or (gt_val == "NO" and agent_val == "N") else "✗"
                    ev_flags.append(f"{etype[:3]}:{agent_val}{match}")

                print(f"  {name:<37} [{tier:<12}] conf={conf:<6} sources={len(srcs):<3} {' '.join(ev_flags)}")
            else:
                print(f"  {name:<37} [{tier:<12}] NO RESULT")

    # Append to results.tsv
    result_row = {
        "research_score": f"{research_score:.2f}",
        "evidence_recall": f"{ev_recall:.2f}",
        "source_discovery": f"{src_disc:.2f}",
        "precision": f"{prec:.2f}",
        "tier_accuracy": f"{tier_acc:.2f}",
        "cases_attempted": len(cases),
        "cases_completed": len(results),
        "total_seconds": f"{total_time:.1f}",
        "api_cost": f"${api_cost:.2f}",
        "total_api_calls": total_calls,
    }

    return result_row


# ──────────────────────────────────────────────────────────────
# Results logger
# ──────────────────────────────────────────────────────────────

def log_result(result_row, hypothesis="", changes_made="", commit_hash=""):
    """Append a row to results.tsv."""
    headers = [
        "experiment_id", "timestamp", "research_score",
        "evidence_recall", "source_discovery", "precision", "tier_accuracy",
        "cases_attempted", "cases_completed", "total_seconds",
        "hypothesis", "changes_made", "commit_hash",
    ]

    # Determine experiment_id
    exp_id = 0
    if RESULTS_PATH.exists():
        with open(RESULTS_PATH) as f:
            lines = f.readlines()
            exp_id = len(lines) - 1  # subtract header
    else:
        with open(RESULTS_PATH, "w") as f:
            f.write("\t".join(headers) + "\n")

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    row_data = [
        str(exp_id),
        timestamp,
        result_row["research_score"],
        result_row["evidence_recall"],
        result_row["source_discovery"],
        result_row["precision"],
        result_row["tier_accuracy"],
        str(result_row["cases_attempted"]),
        str(result_row["cases_completed"]),
        result_row["total_seconds"],
        hypothesis,
        changes_made,
        commit_hash,
    ]

    with open(RESULTS_PATH, "a") as f:
        f.write("\t".join(row_data) + "\n")

    print(f"\nLogged experiment #{exp_id} → results.tsv")


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FlameOn AutoResearch Evaluator")
    parser.add_argument("--case", type=int, default=None, help="Evaluate single case by ID")
    parser.add_argument("--tier", type=str, default=None, help="Filter by tier (ENOUGH/BORDERLINE/INSUFFICIENT)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show per-case breakdown")
    parser.add_argument("--log", action="store_true", help="Log result to results.tsv")
    parser.add_argument("--hypothesis", type=str, default="", help="Hypothesis for this experiment")
    parser.add_argument("--changes", type=str, default="", help="Changes made to research.py")
    args = parser.parse_args()

    result = evaluate(
        case_filter=args.case,
        tier_filter=args.tier,
        verbose=args.verbose,
    )

    if result and args.log:
        log_result(result, hypothesis=args.hypothesis, changes_made=args.changes)
