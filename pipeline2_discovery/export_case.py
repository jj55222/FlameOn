"""
export_case.py — Pipeline 2 → Pipeline 3 contract-compliant case writer
=======================================================================

Wraps research_case() output in the p2_to_p3_case schema from
../schemas/contracts.json and writes a per-case JSON to disk that
Pipeline 3/4/5 can consume.

This is the single artifact that bridges P2 discovery into the rest of
the pipeline. Without it, P2 findings live only in evaluate.py memory
and E2E runs are blocked.

CLI:
    # Single calibration case
    python export_case.py --case-id 1 --output ../discovered_cases/

    # All calibration cases
    python export_case.py --all --output ../discovered_cases/

    # Tier filter
    python export_case.py --tier ENOUGH --output ../discovered_cases/

    # Ad-hoc case (not from calibration)
    python export_case.py --defendant "Jane Doe" --jurisdiction "Dallas, Dallas County, TX" \\
        --year 2023 --output ../discovered_cases/

    # Dry run — no network, no files written, prints would-be JSON
    python export_case.py --case-id 1 --dry-run
    python export_case.py --all --dry-run
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

# Make sure local imports resolve when running from any cwd
HERE = Path(__file__).parent.resolve()
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))


# ─────────────────────────────────────────────────────────────
# Schema constants (mirror schemas/contracts.json p2_to_p3_case)
# ─────────────────────────────────────────────────────────────

VALID_EVIDENCE_TYPES = {
    "bodycam", "interrogation", "court_video", "911_audio", "dash_cam",
    "news_report", "court_docket", "foia_document", "other",
}
VALID_FORMATS = {"video", "audio", "document", "webpage"}
VALID_TIERS = {"ENOUGH", "BORDERLINE", "INSUFFICIENT"}

# Internal confidence → schema confidence_tier
CONFIDENCE_TIER_MAP = {
    "high": "ENOUGH",
    "medium": "BORDERLINE",
    "low": "INSUFFICIENT",
}


# ─────────────────────────────────────────────────────────────
# case_id construction
# ─────────────────────────────────────────────────────────────

def _slugify(s):
    """Lowercase, collapse non-alphanumerics to single underscores, strip."""
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def build_case_id(defendant_names, jurisdiction, year=None):
    """
    Build a case_id slug matching the schema convention:
        lastname_jurisdiction_YYYY  (e.g. smith_harris_county_tx_2023)

    If year is not provided, substitute 'unknown'.
    """
    from research import parse_names, parse_jurisdiction
    n = parse_names(defendant_names)
    j = parse_jurisdiction(jurisdiction)

    last = _slugify(n.get("last_name") or "")
    # Prefer county + state; fall back to city + state; fall back to raw.
    jurisdiction_slug_parts = []
    if j.get("county"):
        jurisdiction_slug_parts.append(j["county"])
    elif j.get("city"):
        jurisdiction_slug_parts.append(j["city"])
    if j.get("state_abbrev"):
        jurisdiction_slug_parts.append(j["state_abbrev"])
    jurisdiction_slug = _slugify(" ".join(jurisdiction_slug_parts)) or _slugify(jurisdiction)

    year_part = str(year) if year else "unknown"

    parts = [p for p in [last, jurisdiction_slug, year_part] if p]
    return "_".join(parts) or "unknown_case"


# ─────────────────────────────────────────────────────────────
# Contract transformer
# ─────────────────────────────────────────────────────────────

def _to_contract_source(s):
    """
    Transform a research.py internal source dict into a p2_to_p3_case source.
    research.py's _type_sources_for_p3() already adds evidence_type, format,
    source_domain, requires_download — we validate + whitelist here.
    """
    url = s.get("url", "")
    evidence_type = s.get("evidence_type", "other")
    if evidence_type not in VALID_EVIDENCE_TYPES:
        evidence_type = "other"
    fmt = s.get("format", "webpage")
    if fmt not in VALID_FORMATS:
        fmt = "webpage"

    out = {
        "url": url,
        "evidence_type": evidence_type,
        "format": fmt,
    }
    if s.get("source_domain"):
        out["source_domain"] = s["source_domain"]
    if "requires_download" in s:
        out["requires_download"] = bool(s["requires_download"])

    # Pack auxiliary signals into notes (human-readable, non-schema-breaking)
    note_parts = []
    if s.get("description"):
        note_parts.append(str(s["description"])[:200])
    if s.get("api"):
        note_parts.append(f"via {s['api']}")
    if s.get("relevance_score") is not None:
        try:
            note_parts.append(f"relevance={float(s['relevance_score']):.2f}")
        except (TypeError, ValueError):
            pass
    if s.get("high_signal"):
        note_parts.append("high_signal")
    if s.get("file_count"):
        note_parts.append(f"files={int(s['file_count'])}")
    if note_parts:
        out["notes"] = " | ".join(note_parts)
    return out


def build_case_contract(
    defendant_names,
    jurisdiction,
    research_result,
    *,
    year=None,
    research_score=0.0,
    charges=None,
    incident_date=None,
    summary=None,
):
    """
    Build a p2_to_p3_case-shaped dict from research_case() output.
    """
    case_id = build_case_id(defendant_names, jurisdiction, year=year)

    sources_in = (research_result or {}).get("sources_found") or []
    sources_out = [_to_contract_source(s) for s in sources_in if s.get("url")]

    internal_conf = (research_result or {}).get("confidence", "low")
    confidence_tier = CONFIDENCE_TIER_MAP.get(internal_conf, "INSUFFICIENT")

    # parse_names gives us the cleaned primary name
    from research import parse_names
    primary = parse_names(defendant_names).get("primary") or defendant_names

    contract = {
        "case_id": case_id,
        "defendant": primary,
        "jurisdiction": jurisdiction,
        "sources": sources_out,
        "research_score": float(research_score or 0.0),
        "confidence_tier": confidence_tier,
    }
    if charges:
        contract["charges"] = list(charges)
    if incident_date:
        contract["incident_date"] = str(incident_date)
    if summary:
        contract["summary"] = str(summary)[:2000]
    return contract


# ─────────────────────────────────────────────────────────────
# Schema validation (hand-written — jsonschema not installed)
# ─────────────────────────────────────────────────────────────

def validate_case_contract(contract):
    """
    Hand-validate against p2_to_p3_case required fields + enums.
    Returns (ok: bool, errors: list[str]).
    """
    errors = []
    required_top = ["case_id", "defendant", "jurisdiction", "sources",
                    "research_score", "confidence_tier"]
    for f in required_top:
        if f not in contract:
            errors.append(f"missing required field: {f}")

    if not isinstance(contract.get("case_id"), str) or not contract.get("case_id"):
        errors.append("case_id must be a non-empty string")
    if not isinstance(contract.get("defendant"), str) or not contract.get("defendant"):
        errors.append("defendant must be a non-empty string")
    if not isinstance(contract.get("jurisdiction"), str) or not contract.get("jurisdiction"):
        errors.append("jurisdiction must be a non-empty string")

    rs = contract.get("research_score")
    if not isinstance(rs, (int, float)) or rs < 0 or rs > 100:
        errors.append(f"research_score must be number in [0, 100] (got {rs!r})")

    tier = contract.get("confidence_tier")
    if tier not in VALID_TIERS:
        errors.append(f"confidence_tier must be one of {sorted(VALID_TIERS)} (got {tier!r})")

    sources = contract.get("sources")
    if not isinstance(sources, list):
        errors.append("sources must be a list")
    else:
        for i, s in enumerate(sources):
            for f in ("url", "evidence_type", "format"):
                if f not in s:
                    errors.append(f"sources[{i}] missing {f}")
            if s.get("evidence_type") not in VALID_EVIDENCE_TYPES:
                errors.append(f"sources[{i}].evidence_type invalid: {s.get('evidence_type')!r}")
            if s.get("format") not in VALID_FORMATS:
                errors.append(f"sources[{i}].format invalid: {s.get('format')!r}")

    return (len(errors) == 0, errors)


# ─────────────────────────────────────────────────────────────
# Per-case scoring (optional — uses evaluate.py metrics on 1-case cohort)
# ─────────────────────────────────────────────────────────────

def score_single_case(case, result):
    """
    Run evaluate.py's weighted scoring on a 1-case cohort so each exported
    case has a meaningful research_score. Cohort-wide metrics that divide
    by tier population (e.g. evidence_recall which averages over ENOUGH)
    still work for a single case because the denominator collapses.
    Returns a float 0-100, or 0.0 if scoring isn't wired.
    """
    try:
        from evaluate import (
            score_evidence_recall,
            score_source_discovery,
            score_precision,
            score_tier_accuracy,
            WEIGHTS,
        )
    except Exception:
        return 0.0

    cases = [case]
    results = {case["case_id"]: result}
    try:
        ev = score_evidence_recall(cases, results)
        sd = score_source_discovery(cases, results)
        pr = score_precision(cases, results)
        ta = score_tier_accuracy(cases, results)
        return (
            ev * WEIGHTS["evidence_recall"]
            + sd * WEIGHTS["source_discovery"]
            + pr * WEIGHTS["precision"]
            + ta * WEIGHTS["tier_accuracy"]
        )
    except Exception as e:
        print(f"  [WARN] per-case scoring failed: {e}")
        return 0.0


# ─────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────

def load_calibration():
    path = HERE / "calibration_data.json"
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_cases(args):
    """Return a list of (defendant, jurisdiction, year, calibration_case_or_None)."""
    if args.defendant and args.jurisdiction:
        return [(args.defendant, args.jurisdiction, args.year, None)]

    calib = load_calibration()

    if args.case_id is not None:
        match = [c for c in calib if c["case_id"] == args.case_id]
        if not match:
            print(f"[ERR] No calibration case with case_id={args.case_id}")
            sys.exit(2)
        cases = match
    elif args.all:
        cases = calib
    elif args.tier:
        cases = [c for c in calib if c.get("tier") == args.tier]
        if not cases:
            print(f"[ERR] No calibration cases with tier={args.tier}")
            sys.exit(2)
    else:
        print("[ERR] Must specify --case-id N, --all, --tier TIER, or (--defendant + --jurisdiction)")
        sys.exit(2)

    return [(c["defendant_names"], c["jurisdiction"], args.year, c) for c in cases]


def run_one(defendant, jurisdiction, year, calib_case, dry_run, score):
    """Run one case. Returns (contract_dict, ok: bool, errors: list[str])."""
    if dry_run:
        # No network, no scoring. Stub an empty research result for schema preview.
        stub = {
            "evidence_found": {},
            "sources_found": [],
            "confidence": "low",
            "research_notes": "[DRY RUN] no network calls made",
        }
        contract = build_case_contract(
            defendant, jurisdiction, stub,
            year=year,
            research_score=0.0,
            summary=f"[DRY RUN] exported from calibration without discovery",
        )
    else:
        from research import research_case
        print(f"  running research_case({defendant!r}, {jurisdiction!r})...")
        result = research_case(defendant_names=defendant, jurisdiction=jurisdiction)
        if not result:
            return None, False, ["research_case returned None"]

        rs = 0.0
        if score and calib_case is not None:
            rs = score_single_case(calib_case, result)
            print(f"    single-case research_score={rs:.2f}")

        summary = None
        notes = result.get("research_notes") or ""
        if notes:
            summary = notes[:1500]

        contract = build_case_contract(
            defendant, jurisdiction, result,
            year=year,
            research_score=rs,
            summary=summary,
        )

    ok, errors = validate_case_contract(contract)
    return contract, ok, errors


def write_contract(contract, output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{contract['case_id']}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(contract, f, indent=2, ensure_ascii=False)
    return out_path


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="P2→P3 contract exporter. Writes p2_to_p3_case JSON per case."
    )
    src = parser.add_mutually_exclusive_group()
    src.add_argument("--case-id", type=int, help="Calibration case index (integer from calibration_data.json)")
    src.add_argument("--all", action="store_true", help="Export every calibration case")
    src.add_argument("--tier", choices=["ENOUGH", "BORDERLINE", "INSUFFICIENT"], help="Filter calibration cases by tier")

    parser.add_argument("--defendant", help="Ad-hoc defendant name (with --jurisdiction)")
    parser.add_argument("--jurisdiction", help="Ad-hoc jurisdiction (with --defendant)")
    parser.add_argument("--year", help="Incident year (used in case_id). Defaults to 'unknown'")

    parser.add_argument(
        "--output", default=str((HERE.parent / "discovered_cases").resolve()),
        help="Output directory for {case_id}.json files (default: ../discovered_cases/)"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip network calls; print would-be contract JSON to stdout")
    parser.add_argument("--score", action="store_true",
                        help="Compute per-case research_score via evaluate.py (calibration cases only)")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing {case_id}.json (default: skip)")
    args = parser.parse_args()

    cases = resolve_cases(args)

    # Declare case slice so research.py can fair-share the Brave per-case cap
    if not args.dry_run:
        try:
            from research import set_case_slice, reset_budget
            reset_budget()
            set_case_slice(len(cases))
        except Exception as e:
            print(f"  [WARN] couldn't set Brave case slice: {e}")

    print(f"Exporting {len(cases)} case(s){' [DRY RUN]' if args.dry_run else ''}")
    print(f"Output: {args.output}")

    written = 0
    failed = 0
    for i, (defendant, jurisdiction, year, calib) in enumerate(cases, 1):
        print(f"\n[{i}/{len(cases)}] {defendant} — {jurisdiction}")
        contract, ok, errors = run_one(defendant, jurisdiction, year, calib, args.dry_run, args.score)

        if contract is None:
            print(f"  [ERR] {errors}")
            failed += 1
            continue

        if not ok:
            print(f"  [SCHEMA FAIL] {errors}")
            failed += 1
            # Still show the output in dry-run so the user can see what was rejected
            if args.dry_run:
                print(json.dumps(contract, indent=2, ensure_ascii=False))
            continue

        if args.dry_run:
            print(f"  [DRY RUN] case_id={contract['case_id']} tier={contract['confidence_tier']} "
                  f"sources={len(contract['sources'])}")
            print(json.dumps(contract, indent=2, ensure_ascii=False))
        else:
            out_path = Path(args.output) / f"{contract['case_id']}.json"
            if out_path.exists() and not args.force:
                print(f"  [SKIP] {out_path.name} exists (use --force to overwrite)")
                continue
            saved = write_contract(contract, args.output)
            print(f"  ✓ {saved.name} — tier={contract['confidence_tier']} sources={len(contract['sources'])}")
            written += 1

    print(f"\n{'=' * 60}")
    print(f"Exported: {written} | Failed: {failed} | Total: {len(cases)}")


if __name__ == "__main__":
    main()
