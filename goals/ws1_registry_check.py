#!/usr/bin/env python3
"""WS1 done-checker — final public-source sweep landed in the aggregator?

Deterministic gate for docs/plans/WEEK_2026-07-01_goals.md §WS1. Read-only: it
NEVER fixes anything, it only verifies and prints WHY on failure. Do not loosen a
check to make it pass — that inverts the house AutoResearch pattern (immutable
checker + iterating sandbox).

Exit 0 (done) iff, versus the frozen baseline goals/ws1_baseline.json:
  1. >= 2 NEW `source` values are present in CASE_BUNDLE_AGG.json, AND
  2. >= 40 NEW bundles carry n_video > 0 (real media bundles, not doc-only),
     counted over the new sources only, AND
  3. `case_bundle_agg.py --selftest` passes (the registry generator is healthy), AND
  4. the NEW A/B-tier bundles validate >= 95% live — every one of their file URLs
     must appear in discovered_cases/validate_ab_results.json (so validate_ab.py has
     actually probed them) and >= 95% must be live. If a new source produced no
     A/B bundles, this sub-check is vacuously satisfied (there is nothing to gate),
     but any UNVALIDATED A/B file fails the check and tells you to run validate_ab.

"New" = belonging to a `source` absent from the baseline. The baseline sources are
frozen for the week, and WS1 never hand-edits existing candidate files, so a new
source value is genuinely new work.

    python goals/ws1_registry_check.py            # check
    python goals/ws1_registry_check.py --verbose  # + per-source / per-bundle detail

Usage note: regenerate the AGG (python discovered_cases/case_bundle_agg.py) and run
discovered_cases/validate_ab.py on the new A/B rows BEFORE this checker — it reads
their outputs, it does not produce them.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DISCOVERED = REPO / "discovered_cases"
BASELINE = REPO / "goals" / "ws1_baseline.json"
AGG = DISCOVERED / "CASE_BUNDLE_AGG.json"
VALIDATE_CACHE = DISCOVERED / "validate_ab_results.json"
AGG_SCRIPT = DISCOVERED / "case_bundle_agg.py"

MIN_NEW_SOURCES = 2
MIN_NEW_VIDEO_BUNDLES = 40
MIN_LIVE_FRACTION = 0.95


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _bundle_file_urls(bundle: dict) -> list[str]:
    return [f["url"] for f in bundle.get("files", []) if f.get("url")]


def _selftest_ok() -> tuple[bool, str]:
    try:
        p = subprocess.run(
            [sys.executable, str(AGG_SCRIPT), "--selftest"],
            capture_output=True, text=True, timeout=120,
        )
    except (OSError, subprocess.TimeoutExpired) as e:  # pragma: no cover
        return False, f"could not run case_bundle_agg.py --selftest: {e}"
    out = (p.stdout + p.stderr).strip().splitlines()
    tail = out[-1] if out else "(no output)"
    return p.returncode == 0, tail


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--verbose", action="store_true", help="print per-source / per-bundle detail")
    args = ap.parse_args()

    for required in (BASELINE, AGG):
        if not required.exists():
            print(f"FAIL: missing {required.relative_to(REPO)} — "
                  f"regenerate the AGG first (python discovered_cases/case_bundle_agg.py).")
            return 1

    baseline = _load_json(BASELINE)
    baseline_sources = set(baseline.get("sources", {}))
    bundles = _load_json(AGG).get("bundles", [])

    per_source = Counter(b.get("source", "") for b in bundles)
    new_sources = sorted(s for s in per_source if s and s not in baseline_sources)
    new_bundles = [b for b in bundles if b.get("source") in set(new_sources)]
    new_video_bundles = [b for b in new_bundles if int(b.get("n_video") or 0) > 0]
    new_ab = [b for b in new_bundles if b.get("tier") in ("A", "B")]

    checks: list[tuple[str, bool, str]] = []

    # 1) >= 2 new sources
    checks.append((
        f">= {MIN_NEW_SOURCES} new source values",
        len(new_sources) >= MIN_NEW_SOURCES,
        f"{len(new_sources)} new: {new_sources or '(none)'}",
    ))

    # 2) >= 40 new bundles with n_video > 0
    checks.append((
        f">= {MIN_NEW_VIDEO_BUNDLES} new bundles with n_video>0",
        len(new_video_bundles) >= MIN_NEW_VIDEO_BUNDLES,
        f"{len(new_video_bundles)} new media bundles across {new_sources or '(none)'}",
    ))

    # 3) generator selftest
    ok3, tail3 = _selftest_ok()
    checks.append(("case_bundle_agg.py --selftest passes", ok3, tail3))

    # 4) new A/B bundles validate >= 95% live (and are actually validated)
    cache = _load_json(VALIDATE_CACHE) if VALIDATE_CACHE.exists() else {}
    ab_urls = sorted({u for b in new_ab for u in _bundle_file_urls(b)})
    unvalidated = [u for u in ab_urls if u not in cache]
    probed = [u for u in ab_urls if u in cache]
    live = [u for u in probed if (cache.get(u) or {}).get("live")]
    if not new_ab:
        detail4 = "no new A/B bundles — nothing to validate (vacuously OK)"
        ok4 = True
    elif unvalidated:
        detail4 = (f"{len(unvalidated)}/{len(ab_urls)} A/B file URL(s) NOT yet probed — "
                   f"run: python discovered_cases/validate_ab.py --workers 3")
        ok4 = False
    else:
        frac = len(live) / len(probed) if probed else 0.0
        ok4 = frac >= MIN_LIVE_FRACTION
        detail4 = (f"{len(live)}/{len(probed)} A/B files live ({frac*100:.1f}%) "
                   f"across {len(new_ab)} A/B bundle(s)")
    checks.append((f">= {int(MIN_LIVE_FRACTION*100)}% of new A/B files live", ok4, detail4))

    print("=" * 72)
    print("WS1 registry check — new sources landed in CASE_BUNDLE_AGG.json")
    print("=" * 72)
    print(f"baseline sources ({len(baseline_sources)}): {sorted(baseline_sources)}")
    print(f"AGG sources ({len(per_source)}): "
          f"{dict(sorted(per_source.items(), key=lambda kv: -kv[1]))}")
    print("-" * 72)
    all_ok = True
    for name, ok, detail in checks:
        all_ok &= ok
        print(f"[{'PASS' if ok else 'FAIL'}] {name}\n        {detail}")

    if args.verbose and new_sources:
        print("-" * 72)
        for s in new_sources:
            sv = [b for b in new_bundles if b.get("source") == s]
            vids = sum(1 for b in sv if int(b.get("n_video") or 0) > 0)
            tiers = Counter(b.get("tier") for b in sv)
            print(f"  {s}: {len(sv)} bundles, {vids} with video, tiers={dict(tiers)}")

    print("=" * 72)
    print("RESULT:", "DONE ✓ (exit 0)" if all_ok else "NOT YET (exit 1)")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
