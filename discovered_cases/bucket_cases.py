#!/usr/bin/env python3
"""
bucket_cases.py — TEMPLATE-BUCKETING (promoted from .tmp/case_buckets/bucketize.py).

Classify EVERY entry in discovered_cases/CASE_BUNDLE_AGG.json into a PRODUCTION
TEMPLATE bucket (deterministic, manifest+title only — no downloads, no LLM):

  flagship_ewu   interrogation/interview media AND case doc AND video
  police_solver  video (BWC/compiled) + case doc OR 911/dispatch, NO interrogation
                 required, high-stakes category (OIS / in-custody death / homicide /
                 fatal UOF). subshape = standoff | discovery | unknown.
                 briefing_recut=true for compiled single-video sources.
  rawwalk        video-driven incident, thin/no doc, low doc-narrative value.
  hold_thin      doc-only / photo-only / audio-only, low stakes, or insufficient media.

REUSES discovered_cases/ewu_shortlist.py (imported, never run — it has a __main__
guard) so the per-file evidence taxonomy + completeness + worth math stay identical
to the EWU shortlist. Adds only: a sexual-crime/in-custody severity patch (ewu's
severity_of understates those), a subshape/fatal detector, a briefing-recut
detector, and a COPA-style richness escalation for entries whose title carries no
severity word.

Writes discovered_cases/bucket_map.json (generated file — regenerate with
``python discovered_cases/bucket_cases.py``). Paths are derived from this file's
location, never hardcoded, so it runs from any checkout or worktree.
"""
from __future__ import annotations

import argparse
import collections
import json
import re
import sys
from pathlib import Path

# ewu_shortlist lives next to this file; put its dir on the path so the import works
# from any cwd (script run, pytest, worktree) without a hardcoded repo root.
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
import ewu_shortlist as E  # noqa: E402  (import-only; __main__ guard means no side effects)

REGISTRY = _HERE / "CASE_BUNDLE_AGG.json"
OUT = _HERE / "bucket_map.json"

BUCKETS = ("flagship_ewu", "police_solver", "rawwalk", "hold_thin")

# Portals that are BY DEFINITION agency-compiled critical-incident (OIS) briefing
# videos: one YouTube clip per case, doc/911/records baked into the edit.
KNOWN_BRIEFING_SOURCES = {"lapd_civ", "sjpd_civ"}
NEW_SOURCES = {"lapd_civ", "sjpd_civ", "longbeach_laserfiche", "chicago_copa"}

# --- severity patch: ewu's severity_of() has no sexual-crime tier (rubric: 70-89
#     sexual assault, 90-100 CSA) — it matches only "assault"->45. Patch first. -----
RE_CSA = re.compile(
    r"child (?:sexual|sex)|child (?:molest|abuse|porn)|\bcsam\b|\bcsa\b"
    r"|lewd\w*.{0,15}(?:child|minor|juvenile)|sexual abuse of (?:a )?(?:child|minor)", re.I)
RE_SEXUAL = re.compile(
    r"sexual assault|sex assault|sexual battery|\brape\b|\braped\b|sodom|molest"
    r"|human traffick|sex traffick|indecent (?:exposure|liberties|assault)"
    r"|\bpredator\b|sexual (?:abuse|misconduct|contact)", re.I)
RE_FATAL = re.compile(
    r"\bfatal\w*\b|\bdeath\b|\bdied\b|\bdeceased\b|\bkilled\b|homicide|murder|slain"
    r"|\bdead\b|custodial death|in.?custody death|\bicd\s?\d|dies in custody", re.I)
# in-custody-death code (longbeach "icd2020 001") or plain phrase -> 92
RE_CUSTODY = re.compile(r"\bicd\s?\d|in.?custody death|custodial death|died in custody", re.I)
RE_STANDOFF = re.compile(
    r"standoff|barricad|\bswat\b|hostage|shots?.fired|active shooter|\barmed\b"
    r"|firefight|gun ?battle|sniper|pursuit|\bchase\b|fled|foot pursuit"
    r"|vehicle pursuit|carjack|shootout", re.I)
RE_DISCOVERY = re.compile(
    r"welfare check|wellness check|found dead|body (?:found|discovered)|found deceased"
    r"|unresponsive|suspicious death|\bmissing\b|staged|decomp|located (?:the )?body"
    r"|check the well|discover", re.I)


def severity_ext(blob: str):
    """ewu.severity_of() + sexual-crime and in-custody-death pre-checks it lacks
    (or under-scores). Highest tier wins."""
    if RE_CSA.search(blob):
        return 95, "child-sexual-abuse"
    if RE_CUSTODY.search(blob):
        return 92, "in-custody-death"
    if RE_SEXUAL.search(blob):
        return 78, "sexual-assault"
    return E.severity_of(blob)


def normalize_blob(b: dict) -> str:
    """case_ids join tokens with '_' ("sustained_sexual_assault", "icd2020_001") and
    portals use '+' for URL-encoded spaces; both are regex word-chars so \\b anchors
    and space-separated phrases fail on them. Normalise [_+] -> space (same trick
    ewu uses for filenames) before matching."""
    return re.sub(r"[_+]+", " ", f"{b.get('title','')} {b.get('case_id','')} {b.get('agency','')}")


def subshape_of(blob: str) -> str:
    if RE_STANDOFF.search(blob):
        return "standoff"
    if RE_DISCOVERY.search(blob):
        return "discovery"
    return "unknown"


def bucket_one(b: dict) -> dict:
    ev = E.classify_bundle(b)  # {bwc,interrogation,911,doc,photos,surveillance,video,audio,other}
    source = b.get("source", "")
    footage = ev["bwc"] + ev["video"] + ev["surveillance"]
    has_bwc = (ev["bwc"] + ev["video"]) > 0          # officer/incident footage (typed or generic)
    has_surv = ev["surveillance"] > 0
    has_footage = footage > 0
    has_doc = ev["doc"] > 0
    has_911 = ev["911"] > 0
    has_int = ev["interrogation"] > 0

    blob = normalize_blob(b)
    sev, sev_label = severity_ext(blob)
    fatal_hint = bool(RE_FATAL.search(blob))
    subshape = subshape_of(blob)

    # --- briefing_recut detection --------------------------------------------
    briefing_recut = False
    if source in KNOWN_BRIEFING_SOURCES:
        briefing_recut = True
        if sev < 80:                                  # portal == critical-incident; floor to OIS
            sev, sev_label = 80, "officer-involved-shooting(briefing-portal)"
    elif (has_footage and ev["doc"] == 0 and ev["911"] == 0 and ev["interrogation"] == 0
          and ev["audio"] == 0 and ev["photos"] == 0 and footage <= 3):
        briefing_recut = True                         # lone compiled clip, nothing else in bundle

    high_stakes = sev >= 70                            # OIS / death / custody / serious-injury / sexual
    # richness escalation: title carries NO severity word (e.g. "Chicago COPA 123")
    # but the bundle is a heavy real-incident investigation (doc + many angles).
    rich_inferred = (sev_label == "unspecified" and has_doc and footage >= 5)

    comp, full = E.completeness_score(ev)
    worth, shape, prox = E.worth_of(ev, sev)
    rank_score = round(comp * worth / 100.0, 1)

    # solver_rank (0-100): produceability of a SolvedFiles-style cut = full evidence
    # SLATE (doc + 911 + footage all present) first, then DEPTH (counts), then
    # severity. Deliberately unlike completeness (which caps flat for no-interrogation
    # bundles) so a 12-doc/17-call/28-cam gem out-ranks a 186-cam no-doc footage dump.
    presence = 22 * has_doc + 20 * has_911 + 18 * has_footage                       # max 60
    depth = min(12.0, 2.0 * ev["doc"]) + min(8.0, 1.5 * ev["911"]) + min(5.0, 0.4 * footage)  # max 25
    sevb = 15 if sev >= 90 else 12 if sev >= 80 else 8 if sev >= 70 else 3 if rich_inferred else 0
    solver_rank = round(presence + depth + sevb, 1)

    # --- BUCKET decision tree (priority order; mutually exclusive) ------------
    if has_int and has_doc and has_footage:
        bucket, conf = "flagship_ewu", ("high" if sev_label != "unspecified" else "medium")
    elif briefing_recut and (high_stakes or source in KNOWN_BRIEFING_SOURCES):
        bucket, conf = "police_solver", "medium"      # compiled briefing; contents inferred
    elif has_footage and (has_doc or has_911) and (high_stakes or rich_inferred):
        bucket, conf = "police_solver", ("high" if high_stakes else "medium")
    elif has_footage:
        bucket, conf = "rawwalk", "medium"
    else:
        bucket, conf = "hold_thin", "high"

    # url reachability (spot-verify refines this in TASK 2)
    has_direct = any((f.get("url") or "").startswith("http") and not E.STREAM_RE.search(f.get("url") or "")
                     for f in b.get("files", []))
    has_stream = any(E.STREAM_RE.search(f.get("url") or "") for f in b.get("files", []))

    return {
        "case_id": b.get("case_id", ""),
        "source": source,
        "agency": b.get("agency", ""),
        "title": b.get("title", ""),
        "case_url": b.get("case_url", ""),
        "tier": b.get("tier"),
        "bucket": bucket,
        "subshape": subshape,
        "briefing_recut": briefing_recut,
        "evidence": {
            "has_bwc": has_bwc, "has_surveillance": has_surv, "has_911": has_911,
            "has_interrogation": has_int, "has_doc": has_doc,
            "footage_total": footage,
            "n_video": b.get("n_video", 0), "n_audio": b.get("n_audio", 0),
            "n_docs": b.get("n_docs", 0), "n_photos": b.get("n_photos", 0),
            "typed": {k: ev[k] for k in ("bwc", "video", "surveillance", "interrogation",
                                          "911", "doc", "photos", "audio")},
        },
        "stakes": {"category": sev_label, "severity": sev, "fatal_hint": fatal_hint},
        "worth": worth, "completeness": comp, "full_package": full,
        "richness_score": rank_score, "solver_rank": solver_rank,
        "downloadable": None,          # set by spot-verify (TASK 2); None = unprobed
        "has_direct_url": has_direct, "has_stream_url": has_stream,
        "confidence": conf,
    }


def bucketize_registry(registry_path: Path) -> list:
    """Load a CASE_BUNDLE_AGG.json and return one bucket row per bundle."""
    data = json.loads(Path(registry_path).read_text(encoding="utf-8"))
    return [bucket_one(b) for b in data["bundles"]]


def write_bucket_map(rows: list, out_path: Path, registry_path: Path) -> None:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(json.dumps({
        "what": "Every CASE_BUNDLE_AGG entry classified into a production-template bucket.",
        "generated_by": "discovered_cases/bucket_cases.py",
        "regenerate": "python discovered_cases/bucket_cases.py",
        "source_registry": Path(registry_path).name,
        "n": len(rows),
        "buckets": list(BUCKETS),
        "rows": rows,
    }, indent=1, ensure_ascii=False), encoding="utf-8")


def print_report(rows: list) -> None:
    by_bucket = collections.Counter(r["bucket"] for r in rows)
    print("== bucket counts ==")
    for k in BUCKETS:
        print(f"  {k:14} {by_bucket[k]}")
    print(f"  {'TOTAL':14} {len(rows)}")

    print("\n== source x bucket ==")
    srcs = sorted({r["source"] for r in rows})
    hdr = ["source", "flagship", "solver", "rawwalk", "hold", "total"]
    print("  " + " ".join(f"{h:>20}" if h == "source" else f"{h:>9}" for h in hdr))
    for s in srcs:
        sr = [r for r in rows if r["source"] == s]
        c = collections.Counter(r["bucket"] for r in sr)
        print(f"  {s:>20} {c['flagship_ewu']:>9} {c['police_solver']:>9} "
              f"{c['rawwalk']:>9} {c['hold_thin']:>9} {len(sr):>9}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Classify CASE_BUNDLE_AGG entries into production-template buckets.")
    ap.add_argument("--registry", type=Path, default=REGISTRY, help="CASE_BUNDLE_AGG.json path")
    ap.add_argument("--out", type=Path, default=OUT, help="bucket_map.json output path")
    ap.add_argument("--dry-run", action="store_true", help="compute + print counts, do not write")
    ap.add_argument("--quiet", action="store_true", help="suppress the report tables")
    args = ap.parse_args(argv)

    rows = bucketize_registry(args.registry)
    if not args.quiet:
        print_report(rows)
    if args.dry_run:
        print("\n[bucket_cases] dry-run — bucket_map.json not written")
        return 0
    write_bucket_map(rows, args.out, args.registry)
    print(f"\n[bucket_cases] wrote {len(rows)} rows -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
