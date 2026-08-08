#!/usr/bin/env python3
"""
goals/ws3_shortlist_check.py — deterministic DONE-checker for WS3.

Exit 0 = done, 1 = not yet (prints WHY). Read-only: it verifies, never fixes.
Spec: docs/plans/WEEK_2026-07-01_goals.md §WS3. Do NOT loosen this to make it
pass — that inverts the AutoResearch pattern (immutable checker + iterating work).

Passes when discovered_cases/ewu_shortlist.json (+ .md) exists with >= 10 ranked
cases where every row has:
  - evidence: {bwc, interrogation, 911, doc, photos} classified artifact counts,
  - completeness_score AND worth (Tier-1 severity x shape x proximity),
  - downloadable AND A/B tier (independently re-checked against the registry),
and the TOP pick has been pulled to a basket (docs + 911 on disk) with a
d2/tier1_verdict.json == PRODUCE. The top 3 must each be cheaply Tier-1 verified.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
SHORTLIST_JSON = ROOT / "discovered_cases" / "ewu_shortlist.json"
SHORTLIST_MD = ROOT / "discovered_cases" / "ewu_shortlist.md"
REGISTRY = ROOT / "discovered_cases" / "CASE_BUNDLE_AGG.json"
BASKET_ROOT = ROOT / ".tmp"

MIN_ROWS = 10
CORE = ("bwc", "interrogation", "911", "doc", "photos")
STREAM_MARKERS = ("youtube", "youtu.be", "vimeo", "player.", "jwplayer", "/embed/", "streaming")

FAILS: List[str] = []
NOTES: List[str] = []


def bad(msg: str) -> None:
    FAILS.append(msg)


def note(msg: str) -> None:
    NOTES.append(msg)


def _num(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def registry_index() -> Dict[str, Dict[str, Any]]:
    data = json.loads(REGISTRY.read_text(encoding="utf-8"))
    return {b.get("case_id"): b for b in data.get("bundles", [])}


def registry_downloadable(bundle: Dict[str, Any]) -> bool:
    """Independent re-derivation of downloadable: A/B tier with >=1 direct URL."""
    if bundle.get("tier") not in ("A", "B"):
        return False
    for f in bundle.get("files", []):
        u = (f.get("url") or "").lower()
        if u.startswith("http") and not any(m in u for m in STREAM_MARKERS):
            return True
    return False


def basket_docs(case_id: str) -> int:
    d = BASKET_ROOT / case_id / "docs"
    return len([p for p in d.iterdir() if p.is_file()]) if d.is_dir() else 0


def basket_911(case_id: str) -> int:
    """Count 911/dispatch/radio audio files fetched anywhere in the basket."""
    b = BASKET_ROOT / case_id
    if not b.is_dir():
        return 0
    n = 0
    for p in b.rglob("*"):
        if p.is_file():
            low = p.name.lower()
            if ("911" in low or "dispatch" in low or "radio" in low) and p.suffix.lower() in (
                    ".wav", ".mp3", ".m4a", ".aac", ".flac", ".ogg"):
                n += 1
    return n


def verdict_of(case_id: str) -> Any:
    vf = BASKET_ROOT / case_id / "d2" / "tier1_verdict.json"
    if not vf.exists():
        return None
    try:
        return json.loads(vf.read_text()).get("verdict")
    except (OSError, ValueError):
        return "<unreadable>"


def main() -> int:
    # 1. artifacts exist
    if not SHORTLIST_JSON.exists():
        bad(f"missing {SHORTLIST_JSON.relative_to(ROOT)} (run: python discovered_cases/ewu_shortlist.py)")
        return report()
    if not SHORTLIST_MD.exists():
        bad(f"missing {SHORTLIST_MD.relative_to(ROOT)}")
    try:
        data = json.loads(SHORTLIST_JSON.read_text(encoding="utf-8"))
    except ValueError as e:
        bad(f"ewu_shortlist.json is not valid JSON: {e}")
        return report()

    rows = data.get("shortlist")
    if not isinstance(rows, list):
        bad("shortlist key missing or not a list")
        return report()

    # 2. >= 10 ranked rows
    if len(rows) < MIN_ROWS:
        bad(f"only {len(rows)} shortlisted cases; need >= {MIN_ROWS}")

    reg = registry_index()

    # 3. per-row schema + downloadable/A-B re-check against the registry
    for i, r in enumerate(rows):
        tag = f"row[{i}] {r.get('case_id','?')}"
        ev = r.get("evidence")
        if not isinstance(ev, dict) or any(k not in ev for k in CORE):
            bad(f"{tag}: evidence must have all of {CORE}, got {ev}")
        else:
            for k in CORE:
                if not (isinstance(ev[k], int) and ev[k] >= 0):
                    bad(f"{tag}: evidence[{k}] must be a non-negative int, got {ev[k]!r}")
        if not _num(r.get("completeness_score")):
            bad(f"{tag}: completeness_score missing/non-numeric")
        if not _num(r.get("worth")):
            bad(f"{tag}: worth missing/non-numeric")
        if not r.get("downloadable"):
            bad(f"{tag}: downloadable is falsey")
        if r.get("tier") not in ("A", "B"):
            bad(f"{tag}: tier={r.get('tier')!r} not in A/B")
        # independent cross-check: the case really is A/B + downloadable in the registry
        b = reg.get(r.get("case_id"))
        if b is None:
            bad(f"{tag}: case_id not found in registry")
        elif not registry_downloadable(b):
            bad(f"{tag}: registry says not downloadable-A/B (tier={b.get('tier')!r})")

    # 4. actually ranked (rank_score non-increasing)
    scores = [r.get("rank_score") for r in rows if _num(r.get("rank_score"))]
    if len(scores) == len(rows) and rows:
        if any(scores[i] < scores[i + 1] for i in range(len(scores) - 1)):
            bad("rows not sorted by rank_score descending")
    elif rows:
        bad("some rows missing numeric rank_score")

    # 5. TOP pick production-ready: basket has doc + 911, verdict == PRODUCE
    if rows:
        top = rows[0].get("case_id")
        nd, n9 = basket_docs(top), basket_911(top)
        v = verdict_of(top)
        if nd < 1:
            bad(f"TOP pick {top}: no docs fetched under .tmp/{top}/docs/ (bundle_to_basket.py --doc-911-only)")
        if n9 < 1:
            bad(f"TOP pick {top}: no 911/dispatch audio fetched under .tmp/{top}/")
        if v != "PRODUCE":
            bad(f"TOP pick {top}: d2/tier1_verdict.json verdict={v!r}, need PRODUCE")
        else:
            note(f"TOP pick {top}: docs={nd} 911={n9} verdict=PRODUCE")

    # 6. top 3 cheaply Tier-1 verified (a verdict file exists for each)
    for r in rows[:3]:
        cid = r.get("case_id")
        if verdict_of(cid) is None:
            bad(f"top-3 case {cid}: no d2/tier1_verdict.json (cheap doc+911 verification missing)")

    return report()


def report() -> int:
    print("=" * 72)
    print("WS3 goal: EWU-ready evidence-completeness shortlist")
    print("=" * 72)
    for n in NOTES:
        print(f"  ✓ {n}")
    if FAILS:
        print(f"\nNOT DONE — {len(FAILS)} check(s) failed:")
        for f in FAILS:
            print(f"  ✗ {f}")
        return 1
    print("\nDONE ✓  ewu_shortlist.json/.md ranked, >=10 downloadable A/B rows,")
    print("        top pick basketed (doc+911) with a PRODUCE Tier-1 verdict.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
