"""
evaluate_actionability.py — Production-Oriented Case Graph Evaluator
====================================================================

This evaluator does NOT replace evaluate.py. It measures whether the research
harness produces production-usable case packets:

- Did it lock identity before making evidence claims?
- Did it find artifact claims?
- Did it resolve claims into concrete artifact URLs?
- Are any artifacts downloadable/transcript-ready?
- Did it avoid false-high confidence on INSUFFICIENT cases?

Usage:
    python evaluate_actionability.py --case 1
    python evaluate_actionability.py --tier ENOUGH
    python evaluate_actionability.py --all

By default this imports research_case_graph.py directly so you can test without
copying it over research.py.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

CALIBRATION_PATH = Path(__file__).parent / "calibration_data.json"
TIME_BUDGET_SECONDS = 1600


def load_calibration() -> List[Dict[str, Any]]:
    with open(CALIBRATION_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def run_case(case: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if "research_case_graph" in sys.modules:
        del sys.modules["research_case_graph"]
    try:
        from research_case_graph import research_case
        return research_case(case["defendant_names"], case["jurisdiction"])
    except Exception as exc:
        print(f"ERROR researching {case.get('defendant_names')}: {exc}")
        return None


def domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def case_metrics(case: Dict[str, Any], result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not result:
        return {
            "case_id": case["case_id"],
            "name": case["defendant_names"],
            "tier": case["tier"],
            "identity_locked": False,
            "claim_count": 0,
            "verified_artifact_count": 0,
            "downloadable_count": 0,
            "official_or_court_or_foia_count": 0,
            "confidence": "none",
            "artifact_status": "none",
            "score": 0.0,
        }

    identity = result.get("case_identity", {}) or {}
    claims = result.get("artifact_claims", []) or []
    artifacts = result.get("verified_artifacts", []) or []
    confidence = result.get("confidence", "low")

    identity_locked = identity.get("identity_confidence") in {"medium", "high"}
    downloadable_count = sum(1 for a in artifacts if a.get("downloadable"))
    official_count = sum(1 for a in artifacts if a.get("source_authority") in {"official", "court", "foia"})

    score = 0.0
    if identity_locked:
        score += 25.0
    if claims:
        score += min(20.0, len(claims) * 5.0)
    if artifacts:
        score += min(30.0, len(artifacts) * 15.0)
    if downloadable_count:
        score += min(20.0, downloadable_count * 20.0)
    if official_count:
        score += 5.0

    # Penalize the worst production error: high confidence on known insufficient cases.
    if case["tier"] == "INSUFFICIENT" and confidence == "high":
        score = min(score, 35.0)

    return {
        "case_id": case["case_id"],
        "name": case["defendant_names"],
        "tier": case["tier"],
        "identity_locked": identity_locked,
        "identity_confidence": identity.get("identity_confidence", "low"),
        "identity_anchors": identity.get("identity_anchors", []),
        "claim_count": len(claims),
        "verified_artifact_count": len(artifacts),
        "downloadable_count": downloadable_count,
        "official_or_court_or_foia_count": official_count,
        "confidence": confidence,
        "artifact_status": result.get("artifact_status", "none"),
        "score": round(score, 2),
        "artifact_domains": sorted({domain(a.get("artifact_url", "")) for a in artifacts if a.get("artifact_url")}),
    }


def summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {}
    enough = [r for r in rows if r["tier"] == "ENOUGH"]
    insufficient = [r for r in rows if r["tier"] == "INSUFFICIENT"]
    return {
        "cases": len(rows),
        "avg_actionability_score": round(sum(r["score"] for r in rows) / len(rows), 2),
        "identity_lock_rate": round(sum(1 for r in rows if r["identity_locked"]) / len(rows) * 100, 2),
        "artifact_claim_rate": round(sum(1 for r in rows if r["claim_count"] > 0) / len(rows) * 100, 2),
        "verified_artifact_rate": round(sum(1 for r in rows if r["verified_artifact_count"] > 0) / len(rows) * 100, 2),
        "downloadable_artifact_rate": round(sum(1 for r in rows if r["downloadable_count"] > 0) / len(rows) * 100, 2),
        "enough_downloadable_rate": round(sum(1 for r in enough if r["downloadable_count"] > 0) / len(enough) * 100, 2) if enough else 0.0,
        "insufficient_false_high_count": sum(1 for r in insufficient if r["confidence"] == "high"),
    }


def evaluate(case_filter: Optional[int] = None, tier_filter: Optional[str] = None, verbose: bool = False) -> Dict[str, Any]:
    cases = load_calibration()
    if case_filter is not None:
        cases = [c for c in cases if c["case_id"] == case_filter]
    if tier_filter:
        cases = [c for c in cases if c["tier"] == tier_filter]

    start = time.time()
    rows: List[Dict[str, Any]] = []
    print(f"FlameOn Case-Graph Actionability Evaluation")
    print("=" * 58)
    print(f"Cases: {len(cases)}")

    for idx, case in enumerate(cases, start=1):
        if time.time() - start > TIME_BUDGET_SECONDS:
            print("TIME BUDGET EXCEEDED")
            break
        print(f"[{idx}/{len(cases)}] {case['defendant_names'][:45]}...", end=" ", flush=True)
        case_start = time.time()
        result = run_case(case)
        row = case_metrics(case, result)
        rows.append(row)
        print(
            f"{time.time() - case_start:.1f}s | id={row['identity_confidence']} "
            f"claims={row['claim_count']} verified={row['verified_artifact_count']} "
            f"dl={row['downloadable_count']} conf={row['confidence']} score={row['score']}"
        )

    summary = summarize(rows)
    print("\n" + "=" * 58)
    print("Summary")
    print(json.dumps(summary, indent=2))

    if verbose:
        print("\nPer-case rows")
        print(json.dumps(rows, indent=2))

    return {"summary": summary, "rows": rows}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate FlameOn case graph actionability")
    parser.add_argument("--case", type=int, default=None, help="Evaluate one case_id")
    parser.add_argument("--tier", type=str, default=None, help="Filter ENOUGH/BORDERLINE/INSUFFICIENT")
    parser.add_argument("--all", action="store_true", help="Evaluate all cases; default if no filters")
    parser.add_argument("--verbose", "-v", action="store_true", help="Print per-case JSON rows")
    args = parser.parse_args()

    evaluate(case_filter=args.case, tier_filter=args.tier, verbose=args.verbose)
