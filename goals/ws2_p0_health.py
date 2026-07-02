#!/usr/bin/env python3
"""
ws2_p0_health.py — WS2 done-checker (docs/plans/WEEK_2026-07-01_goals.md §WS2).

P0 FOIA sourcing runs autonomously (launchd, daily) and surfaces genuinely
file-worthy cases. This script is the deterministic definition of done:

    exit 0  = P0 autonomous daily ingest is healthy
    exit 1  = not yet (prints WHY, one line per failed assertion)

Read-only. It verifies, it never fixes — do not loosen a gate to make it pass.

Three gates (all must pass):

  A. LIVE run + accumulation
     - foia_queue.md exists, non-empty, and fresh (< 26 h old);
     - run_history.jsonl records >= 2 LIVE (non --mock) runs and the de-dupe
       store (seen_total) strictly grew from the first live run to the last
       (proves real network+LLM autonomy accumulating over time, not one --mock).

  B. Scheduler
     - the launchd agent com.flameon.p0 is loaded (`launchctl list`).

  C. Top-5 queue sanity (assertions on required fields, not vibes)
     - each of the top-5 drafts has the fields an operator needs to actually
       file: resolved US-state jurisdiction, a cited statute, a non-empty
       records list, a real request letter, a positive worth; and a majority
       carry an extracted agency name.

Usage:
    python goals/ws2_p0_health.py                 # defaults to pipeline0_sourcing/.tmp/p0
    python goals/ws2_p0_health.py --p0-dir PATH
    python goals/ws2_p0_health.py --skip-launchd  # gates A+C only (debug)
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_P0_DIR = ROOT / "pipeline0_sourcing" / ".tmp" / "p0"

QUEUE_MAX_AGE_H = 26.0          # daily job + slack; older => stale
MIN_LIVE_RUNS = 2              # must prove live autonomy across >= 2 runs
LAUNCHD_LABEL = "flameon.p0"   # grep token for `launchctl list`

US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
    "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
}

# ---- reporting helpers --------------------------------------------------------

_FAILS: List[str] = []
_OKS: List[str] = []


def ok(msg: str) -> None:
    _OKS.append(msg)


def fail(msg: str) -> None:
    _FAILS.append(msg)


# ---- Gate A: live run + accumulation -----------------------------------------

def check_live_and_growth(p0: Path) -> None:
    md = p0 / "foia_queue.md"
    qjson = p0 / "foia_queue.json"
    hist = p0 / "run_history.jsonl"

    if not md.exists():
        fail(f"A: {md} does not exist — no live run has written a queue yet")
    else:
        age_h = (time.time() - md.stat().st_mtime) / 3600.0
        body = md.read_text(errors="replace")
        if len(body.strip()) < 80 or "##" not in body:
            fail(f"A: {md.name} exists but looks empty (no incident sections)")
        else:
            ok(f"A: {md.name} present with incident sections")
        if age_h > QUEUE_MAX_AGE_H:
            fail(f"A: {md.name} is stale — {age_h:.1f} h old (> {QUEUE_MAX_AGE_H} h)")
        else:
            ok(f"A: {md.name} is fresh ({age_h:.1f} h old)")

    if not qjson.exists():
        fail(f"A: {qjson} missing (machine queue not written)")

    if not hist.exists():
        fail(f"A: {hist} missing — cannot prove >= {MIN_LIVE_RUNS} live runs")
        return

    runs = []
    for line in hist.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            runs.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    live = [r for r in runs if not r.get("mock", False)]
    if len(live) < MIN_LIVE_RUNS:
        fail(f"A: only {len(live)} LIVE run(s) recorded in run_history.jsonl "
             f"(need >= {MIN_LIVE_RUNS}); mock runs do not count")
        return
    ok(f"A: {len(live)} live runs recorded (of {len(runs)} total)")

    if not runs or runs[-1].get("mock", False):
        fail("A: the most recent recorded run was --mock — the current queue is not live")
    else:
        ok("A: most recent run was live")

    first, last = live[0].get("seen_total", 0), live[-1].get("seen_total", 0)
    if last > first:
        ok(f"A: seen-store grew across live runs ({first} -> {last})")
    else:
        fail(f"A: seen-store did not grow across live runs ({first} -> {last}); "
             f"expected the de-dupe store to accumulate")


# ---- Gate B: launchd ----------------------------------------------------------

def check_launchd() -> None:
    try:
        out = subprocess.run(["launchctl", "list"], capture_output=True,
                             text=True, timeout=15)
    except (FileNotFoundError, subprocess.SubprocessError) as e:
        fail(f"B: could not run `launchctl list`: {e}")
        return
    if LAUNCHD_LABEL in out.stdout:
        ok(f"B: launchd agent com.{LAUNCHD_LABEL} is loaded")
    else:
        fail(f"B: launchd agent com.{LAUNCHD_LABEL} not found in `launchctl list` "
             f"(load it: launchctl load ~/Library/LaunchAgents/com.flameon.p0.plist)")


# ---- Gate C: top-5 queue sanity ----------------------------------------------

_REQUIRED = ("headline", "jurisdiction", "foia_worth", "verdict",
             "records_requested", "statute", "request_letter", "incident_type")


def _row_problems(d: Dict, i: int) -> List[str]:
    probs = []
    tag = f"row {i}"
    for k in _REQUIRED:
        if k not in d:
            probs.append(f"{tag}: missing field '{k}'")
    if not str(d.get("headline", "")).strip():
        probs.append(f"{tag}: empty headline")
    if d.get("verdict") not in ("FILE", "WATCH"):
        probs.append(f"{tag}: verdict '{d.get('verdict')}' not FILE/WATCH")
    w = d.get("foia_worth")
    if not isinstance(w, (int, float)) or w <= 0:
        probs.append(f"{tag}: foia_worth {w!r} not a positive number")
    j = d.get("jurisdiction", {}) or {}
    st = str(j.get("state", "")).upper()
    if not re.fullmatch(r"[A-Z]{2}", st) or st not in US_STATES:
        probs.append(f"{tag}: jurisdiction.state {st!r} is not a resolved US state")
    if not j.get("known", False):
        probs.append(f"{tag}: jurisdiction not in the state-access table (known=false)")
    recs = d.get("records_requested") or []
    if not isinstance(recs, list) or not recs or not all(str(r).strip() for r in recs):
        probs.append(f"{tag}: records_requested empty/malformed")
    if not str(d.get("statute", "")).strip():
        probs.append(f"{tag}: no statute cited")
    if not str(d.get("request_letter", "")).startswith("To the Public Records Officer"):
        probs.append(f"{tag}: request_letter missing/does not start with the salutation")
    return probs


def check_top5(p0: Path) -> None:
    qjson = p0 / "foia_queue.json"
    if not qjson.exists():
        fail("C: foia_queue.json missing — cannot spot-check rows")
        return
    try:
        drafts = json.loads(qjson.read_text())
    except json.JSONDecodeError as e:
        fail(f"C: foia_queue.json is not valid JSON: {e}")
        return
    if not isinstance(drafts, list) or not drafts:
        fail("C: queue is empty — no FILE/WATCH rows to file")
        return

    top = drafts[: min(5, len(drafts))]
    problems: List[str] = []
    for i, d in enumerate(top, 1):
        problems += _row_problems(d, i)
    if problems:
        for p in problems[:20]:
            fail("C: " + p)
    else:
        ok(f"C: all {len(top)} top rows pass required-field assertions")

    n_agency = sum(1 for d in top if str(d.get("agency", "")).strip())
    need = max(1, (len(top) + 1) // 2)   # a majority
    if n_agency >= need:
        ok(f"C: agency extracted on {n_agency}/{len(top)} top rows (>= {need})")
    else:
        fail(f"C: only {n_agency}/{len(top)} top rows have an agency (want >= {need}) "
             f"— state/agency extraction looks weak")


# ---- main ---------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="WS2 P0 autonomy health check.")
    ap.add_argument("--p0-dir", default=str(DEFAULT_P0_DIR),
                    help=f"P0 output dir (default: {DEFAULT_P0_DIR})")
    ap.add_argument("--skip-launchd", action="store_true",
                    help="skip the launchd gate (gates A+C only, for local debug)")
    a = ap.parse_args()
    p0 = Path(a.p0_dir)

    print(f"[ws2] checking P0 health in {p0}")
    check_live_and_growth(p0)
    if a.skip_launchd:
        _OKS.append("B: launchd gate SKIPPED (--skip-launchd)")
    else:
        check_launchd()
    check_top5(p0)

    print("\n  PASS:")
    for m in _OKS:
        print(f"    ✓ {m}")
    if _FAILS:
        print("\n  FAIL:")
        for m in _FAILS:
            print(f"    ✗ {m}")
        print(f"\n[ws2] NOT READY — {len(_FAILS)} check(s) failed.")
        return 1
    print("\n[ws2] HEALTHY — P0 autonomous daily ingest is live and file-worthy.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
