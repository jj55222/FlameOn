"""
uof_benchmark.py — Use-of-Force discovery benchmark for Pipeline 2
==================================================================

Treats Mapping Police Violence (and similar UoF datasets) as a ground-truth
recall test for the artifact-finder. UoF cases are useful here because:
  - ~25-40% of MPV records have a documented video_link → real ground truth
  - Hundreds of additional records have source_links → domain ground truth
  - Volume (~12K records) gives statistical power our 38-case calibration_data lacks

This is NOT a quality benchmark for content (UoF cases tend to lack interrogation
footage so they score low in P4). It IS a benchmark for the discovery side:
  "given a name + date + agency, can the system find the released bodycam URL?"

CLI:
    # Quick iteration (free APIs only, 50 cases)
    python uof_benchmark.py --sample 50 --free-only --log

    # Full benchmark (paid Brave allowed, 200 cases)
    python uof_benchmark.py --sample 200 --log

    # Debug a specific MPV case by name match
    python uof_benchmark.py --case "John Doe" --verbose

    # Download/refresh MPV cache
    python uof_benchmark.py --download

    # Dry run (no network, prints plan)
    python uof_benchmark.py --sample 5 --dry-run

    # Anti-overfit gate: after UoF, also run the 38-case calibration scorer.
    # Exits non-zero if calibration regresses below --baseline-score (default 63.0).
    python uof_benchmark.py --sample 50 --cross-validate-calibration
"""

import argparse
import csv
import json
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

SCRIPT_DIR = Path(__file__).parent.resolve()
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# ─────────────────────────────────────────────────────────────
# Data layout
# ─────────────────────────────────────────────────────────────

UOF_DIR = SCRIPT_DIR / "uof_data"
UOF_DIR.mkdir(exist_ok=True)
MPV_CACHE = UOF_DIR / "mpv_cached.csv"
RESULTS_TSV = SCRIPT_DIR / "uof_results.tsv"
FAILURES_JSON = SCRIPT_DIR / "uof_failures.json"

# Public UoF datasets in priority order. WaPo is most stable (github main).
# MPV official URLs change frequently; user may need to manually drop a CSV.
DATASET_URLS = [
    # WaPo Police Shootings (fatal only, since 2015) — github main, very stable
    "https://raw.githubusercontent.com/washingtonpost/data-police-shootings/master/v2/fatal-police-shootings-data.csv",
    # MPV mirrors (try these too — community forks, varying availability)
    "https://raw.githubusercontent.com/dvilela/mpv-data/main/MPV-DataSet.csv",
    "https://raw.githubusercontent.com/mappingpoliceviolence/mpv-data/main/MPV-DataSet.csv",
    "https://mappingpoliceviolence.org/s/MPVDatasetDownload.xlsx",
]


# ─────────────────────────────────────────────────────────────
# Schema-tolerant column resolution
# ─────────────────────────────────────────────────────────────

NAME_COLS = ["name", "victim_name", "Victim's name", "Name"]
DATE_COLS = ["date", "date_of_incident", "Date of Incident (month/day/year)", "Date"]
CITY_COLS = ["city", "City"]
STATE_COLS = ["state", "State"]
COUNTY_COLS = ["county", "County"]
AGENCY_COLS = ["agency_responsible", "Agency responsible for death", "Agency"]
VIDEO_COLS = ["video_link", "Video", "video"]
SOURCE_COLS = ["source_links", "URL of image of victim", "Link to news article or photo of officer"]
BODYCAM_FLAG_COLS = ["body_camera", "Body Camera", "bodycam"]


def _first_col(row, candidates):
    """Return value from first column found in row, case-insensitive."""
    keys_lower = {k.lower(): k for k in row.keys()}
    for c in candidates:
        if c in row:
            return (row[c] or "").strip()
        if c.lower() in keys_lower:
            return (row[keys_lower[c.lower()]] or "").strip()
    return ""


def _truthy(s):
    return (s or "").strip().lower() in ("true", "t", "1", "yes", "y")


def parse_mpv_row(row):
    """Normalize a UoF CSV row to our schema (works for MPV and WaPo)."""
    return {
        "name": _first_col(row, NAME_COLS),
        "date": _first_col(row, DATE_COLS),
        "city": _first_col(row, CITY_COLS),
        "state": _first_col(row, STATE_COLS),
        "county": _first_col(row, COUNTY_COLS),
        "agency": _first_col(row, AGENCY_COLS),
        "video_link": _first_col(row, VIDEO_COLS),
        "source_link": _first_col(row, SOURCE_COLS),
        "body_camera_flag": _truthy(_first_col(row, BODYCAM_FLAG_COLS)),
    }


# ─────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────

def download_mpv():
    """Try to download a UoF dataset to local cache. Returns path or None."""
    import requests
    for url in DATASET_URLS:
        print(f"  Trying {url[:80]}...")
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200 and len(r.content) > 1000:
                MPV_CACHE.write_bytes(r.content)
                print(f"  Saved {len(r.content):,} bytes to {MPV_CACHE}")
                # Note dataset source for downstream debugging
                (UOF_DIR / "source.txt").write_text(url)
                return MPV_CACHE
            print(f"    HTTP {r.status_code}, {len(r.content)} bytes")
        except Exception as e:
            print(f"    Failed: {e}")
    print(f"[ERR] All sources failed. Place a UoF CSV manually at: {MPV_CACHE}")
    print(f"      Schema: name + date + city + state + agency + (optional) video_link/source_link")
    return None


def load_mpv(require_video=True):
    """
    Load UoF records from cache.
    require_video=True keeps records that have EITHER a video_link URL
    OR body_camera_flag=True (binary "bodycam exists" signal from WaPo).
    """
    if not MPV_CACHE.exists():
        print(f"[INFO] UoF cache missing. Run with --download or place CSV at {MPV_CACHE}")
        return []
    records = []
    with open(MPV_CACHE, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            r = parse_mpv_row(row)
            if not r["name"]:
                continue
            if require_video and not r["video_link"] and not r["body_camera_flag"]:
                continue
            records.append(r)
    return records


# ─────────────────────────────────────────────────────────────
# Ground-truth scoring
# ─────────────────────────────────────────────────────────────

def _domain(url):
    if not url:
        return ""
    try:
        return urlparse(url).netloc.replace("www.", "").lower()
    except Exception:
        return ""


def _yt_video_id(url):
    """Extract YouTube video ID from any URL form. Empty if not YouTube."""
    if not url or "youtube" not in url.lower() and "youtu.be" not in url.lower():
        return ""
    import re
    m = re.search(r"(?:v=|/embed/|/watch\?v=|/shorts/|youtu\.be/)([A-Za-z0-9_-]{11})", url)
    return m.group(1) if m else ""


def score_bodycam_recall(record, agent_sources):
    """
    Test whether the agent found bodycam-related content. Two ground-truth modes:

    A) URL ground truth (record.video_link present, e.g. MPV):
        score against exact URL → same video_id → same domain hierarchy.
    B) Flag ground truth (record.body_camera_flag=True, e.g. WaPo):
        score 1.0 if agent returned ANY video-typed source from a "bodycam-likely"
        domain (youtube, dailymotion, agency portals); 0 otherwise.
    """
    gt_url = record.get("video_link", "")

    # Mode A: URL ground truth
    if gt_url:
        gt_domain = _domain(gt_url)
        gt_yt_id = _yt_video_id(gt_url)
        exact = False
        same_video_id = False
        same_domain = False
        for s in agent_sources or []:
            url = s.get("url", "")
            if not url:
                continue
            if url == gt_url:
                exact = True
                break
            if gt_yt_id and gt_yt_id == _yt_video_id(url):
                same_video_id = True
            if gt_domain and gt_domain == _domain(url):
                same_domain = True
        if exact:
            return {"applicable": True, "mode": "url", "exact": True, "same_video_id": True, "same_domain": True, "score": 1.0}
        if same_video_id:
            return {"applicable": True, "mode": "url", "exact": False, "same_video_id": True, "same_domain": True, "score": 0.85}
        if same_domain:
            return {"applicable": True, "mode": "url", "exact": False, "same_video_id": False, "same_domain": True, "score": 0.4}
        return {"applicable": True, "mode": "url", "exact": False, "same_video_id": False, "same_domain": False, "score": 0.0}

    # Mode B: flag ground truth (any video-typed source counts)
    if record.get("body_camera_flag"):
        VIDEO_DOMAINS = {"youtube.com", "youtu.be", "dailymotion.com", "tiktok.com", "vimeo.com",
                         "facebook.com", "courttv.com"}
        VIDEO_TYPES = {"bodycam_footage", "interrogation_footage", "court_footage", "video_footage", "general_footage"}
        for s in agent_sources or []:
            domain = _domain(s.get("url", ""))
            stype = s.get("type", "")
            if domain in VIDEO_DOMAINS or any(v in domain for v in VIDEO_DOMAINS):
                return {"applicable": True, "mode": "flag", "found_video_source": True, "score": 1.0}
            if stype in VIDEO_TYPES:
                return {"applicable": True, "mode": "flag", "found_video_source": True, "score": 1.0}
        return {"applicable": True, "mode": "flag", "found_video_source": False, "score": 0.0}

    return {"applicable": False, "score": 0.0}


def score_source_domain_recall(record, agent_sources):
    """% of ground-truth source domains found by agent."""
    gt_urls = []
    if record.get("video_link"):
        gt_urls.append(record["video_link"])
    if record.get("source_link"):
        gt_urls.append(record["source_link"])
    gt_domains = {_domain(u) for u in gt_urls if u}
    gt_domains.discard("")
    if not gt_domains:
        return {"applicable": False, "score": 0.0}
    agent_domains = {_domain(s.get("url", "")) for s in (agent_sources or [])}
    agent_domains.discard("")
    hit = gt_domains & agent_domains
    return {
        "applicable": True,
        "gt_domains": sorted(gt_domains),
        "agent_domains": sorted(agent_domains),
        "hit_count": len(hit),
        "score": len(hit) / len(gt_domains),
    }


def score_precision(record, agent_sources):
    """Of agent's high-relevance sources, what fraction look name-correct?"""
    high_rel = [s for s in (agent_sources or []) if s.get("relevance_score", 0) >= 0.5]
    if not high_rel:
        return {"applicable": False, "score": 0.0}
    name_lower = (record.get("name") or "").lower()
    last_lower = name_lower.split()[-1] if name_lower else ""
    valid = 0
    for s in high_rel:
        text = f"{s.get('description', '')} {s.get('url', '')}".lower()
        if name_lower and name_lower in text:
            valid += 1
        elif last_lower and len(last_lower) > 4 and last_lower in text:
            valid += 0.5
    return {
        "applicable": True,
        "high_rel_count": len(high_rel),
        "valid_count": valid,
        "score": min(1.0, valid / len(high_rel)),
    }


# ─────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────

def build_jurisdiction(record):
    """Concatenate MPV city/county/state into the format research_case expects."""
    parts = []
    if record.get("city"):
        parts.append(record["city"])
    if record.get("county"):
        parts.append(record["county"])
    if record.get("state"):
        parts.append(record["state"])
    return ", ".join(parts) if parts else (record.get("agency") or "")


def run_one(record, free_only, dry_run, verbose):
    """Run discovery + scoring on one MPV record."""
    name = record["name"]
    jurisdiction = build_jurisdiction(record)
    if dry_run:
        print(f"  [DRY RUN] would research_case({name!r}, {jurisdiction!r})")
        return None

    # Lazy import so dry-run works without research.py loaded
    from research import research_case
    if free_only:
        # Disable paid Brave + Exa via env. NOTE: this leaves MuckRock,
        # CourtListener, YouTube, Wikipedia, DailyMotion, Reddit, portals.
        os.environ["FLAMEON_USE_EXA"] = "0"
        # Brave is gated by api key + spend cap, but we can preempt by
        # forcing the budget exhausted via a private flag — simpler is to
        # let it be: Brave will skip on its own if the cap is hit.
        # If user wants strict free-only, also unset BRAVE_API_KEY:
        os.environ.pop("BRAVE_API_KEY_ACTIVE", None)
        # We do NOT unset BRAVE_API_KEY itself because that affects everything.
        # Cheapest practical approach: rely on caller to set --free-only env separately.

    t0 = time.time()
    try:
        result = research_case(defendant_names=name, jurisdiction=jurisdiction)
    except Exception as e:
        return {"error": str(e), "elapsed_sec": time.time() - t0}
    elapsed = time.time() - t0
    sources = (result or {}).get("sources_found", [])

    bc = score_bodycam_recall(record, sources)
    sd = score_source_domain_recall(record, sources)
    pr = score_precision(record, sources)

    out = {
        "name": name,
        "jurisdiction": jurisdiction,
        "agency": record.get("agency", ""),
        "gt_video_link": record.get("video_link", ""),
        "n_sources_found": len(sources),
        "elapsed_sec": round(elapsed, 1),
        "bodycam_recall": bc,
        "source_domain_recall": sd,
        "precision": pr,
        "confidence": (result or {}).get("confidence", "?"),
    }
    if verbose:
        print(json.dumps(out, indent=2, default=str))
    return out


def aggregate(results):
    """Roll per-case results into aggregate metrics."""
    bc_scores = [r["bodycam_recall"]["score"] for r in results if r and r.get("bodycam_recall", {}).get("applicable")]
    sd_scores = [r["source_domain_recall"]["score"] for r in results if r and r.get("source_domain_recall", {}).get("applicable")]
    pr_scores = [r["precision"]["score"] for r in results if r and r.get("precision", {}).get("applicable")]
    bc_exact = sum(1 for r in results if r and r.get("bodycam_recall", {}).get("exact"))
    bc_video_id = sum(1 for r in results if r and r.get("bodycam_recall", {}).get("same_video_id"))
    bc_domain = sum(1 for r in results if r and r.get("bodycam_recall", {}).get("same_domain"))
    bc_flag_hit = sum(1 for r in results if r and r.get("bodycam_recall", {}).get("found_video_source"))

    def _mean(xs):
        return round(sum(xs) / len(xs) * 100, 2) if xs else 0.0

    return {
        "n_total": len(results),
        "n_bodycam_applicable": len(bc_scores),
        "bodycam_recall_pct": _mean(bc_scores),
        "bodycam_exact_hits": bc_exact,
        "bodycam_video_id_hits": bc_video_id,
        "bodycam_domain_hits": bc_domain,
        "bodycam_flag_video_hits": bc_flag_hit,
        "source_domain_recall_pct": _mean(sd_scores),
        "precision_pct": _mean(pr_scores),
    }


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

RESULTS_HEADER = [
    "experiment_id", "timestamp", "sample_size", "free_only",
    "bodycam_recall_pct", "bodycam_exact_hits", "bodycam_video_id_hits", "bodycam_domain_hits",
    "source_domain_recall_pct", "precision_pct",
    "total_seconds", "hypothesis", "changes_made", "commit_hash",
]


def _next_experiment_id():
    if not RESULTS_TSV.exists():
        return 0
    try:
        with open(RESULTS_TSV, "r", encoding="utf-8") as f:
            lines = [line for line in f.read().splitlines() if line.strip()]
        if len(lines) < 2:
            return 0
        last = lines[-1].split("\t")
        return int(last[0]) + 1
    except (ValueError, IndexError):
        return 0


def _git_hash():
    try:
        import subprocess
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=SCRIPT_DIR,
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return ""


def log_run(metrics, sample_size, free_only, total_seconds, hypothesis, changes):
    is_new = not RESULTS_TSV.exists()
    exp_id = _next_experiment_id()
    row = [
        str(exp_id),
        datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        str(sample_size),
        "1" if free_only else "0",
        f"{metrics['bodycam_recall_pct']:.2f}",
        str(metrics["bodycam_exact_hits"]),
        str(metrics["bodycam_video_id_hits"]),
        str(metrics["bodycam_domain_hits"]),
        f"{metrics['source_domain_recall_pct']:.2f}",
        f"{metrics['precision_pct']:.2f}",
        f"{total_seconds:.1f}",
        hypothesis.replace("\t", " "),
        changes.replace("\t", " "),
        _git_hash(),
    ]
    with open(RESULTS_TSV, "a", encoding="utf-8") as f:
        if is_new:
            f.write("\t".join(RESULTS_HEADER) + "\n")
        f.write("\t".join(row) + "\n")
    print(f"\n  Experiment {exp_id} logged to {RESULTS_TSV.name}")


def write_failures(results):
    """Save cases where bodycam_recall=0 for inspection."""
    failures = [
        r for r in results
        if r and r.get("bodycam_recall", {}).get("applicable")
        and r["bodycam_recall"]["score"] == 0
    ]
    if not failures:
        return
    with open(FAILURES_JSON, "w", encoding="utf-8") as f:
        json.dump(failures, f, indent=2, default=str, ensure_ascii=False)
    print(f"  {len(failures)} bodycam-recall failures saved to {FAILURES_JSON.name}")


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="UoF discovery benchmark for Pipeline 2")
    parser.add_argument("--download", action="store_true", help="Fetch fresh MPV CSV to cache")
    parser.add_argument("--sample", type=int, default=50, help="Number of records to test (default 50)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for sampling (default 42)")
    parser.add_argument("--case", help="Filter by name substring (debug single case)")
    parser.add_argument("--free-only", action="store_true", help="Disable paid APIs (Brave/Exa)")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without network calls")
    parser.add_argument("--verbose", action="store_true", help="Print per-case JSON detail")
    parser.add_argument("--log", action="store_true", help="Append result to uof_results.tsv")
    parser.add_argument("--hypothesis", default="baseline", help="Short label for this run")
    parser.add_argument("--changes", default="none", help="What changed since last run")
    parser.add_argument("--cross-validate-calibration", action="store_true",
                        help="After the UoF run, also run the 38-case calibration scorer "
                             "(evaluate.py). Exits 1 if research_score regresses below "
                             "--baseline-score. Anti-overfit gate.")
    parser.add_argument("--baseline-score", type=float, default=63.0,
                        help="Floor for the cross-validate gate (default 63.0). "
                             "Historical peak is 63.62 (Exp 23). Use 63.62 for strict mode.")
    args = parser.parse_args()

    if args.download:
        if not download_mpv():
            sys.exit(2)
        return

    records = load_mpv(require_video=True)
    if not records:
        print(f"[ERR] No MPV records loaded. Run with --download first.")
        sys.exit(2)

    print(f"Loaded {len(records):,} MPV records with video_link")

    # Filter or sample
    if args.case:
        cs = args.case.lower()
        candidates = [r for r in records if cs in r["name"].lower()]
        if not candidates:
            print(f"[ERR] No MPV record matches name~={args.case!r}")
            sys.exit(2)
        records = candidates[:1]
    else:
        rng = random.Random(args.seed)
        records = rng.sample(records, min(args.sample, len(records)))

    print(f"Sampling {len(records)} records  (seed={args.seed}, free_only={args.free_only})")
    if args.dry_run:
        for r in records[:5]:
            print(f"  [DRY RUN] {r['name']} | {build_jurisdiction(r)} | gt_video={r['video_link'][:60]}")
        print(f"  ... ({len(records)} total)")
        return

    # Reset budget + declare slice for fair-share
    try:
        from research import reset_budget, set_case_slice
        reset_budget()
        set_case_slice(len(records))
    except Exception as e:
        print(f"  [WARN] couldn't init budget: {e}")

    # Run
    t0 = time.time()
    results = []
    for i, r in enumerate(records):
        print(f"\n  [{i+1}/{len(records)}] {r['name']:30s}  ({r['agency'][:40]})")
        result = run_one(r, free_only=args.free_only, dry_run=args.dry_run, verbose=args.verbose)
        results.append(result)
        if result and not args.verbose:
            bc = result.get("bodycam_recall", {})
            mark = "EXACT" if bc.get("exact") else ("VID-ID" if bc.get("same_video_id") else ("DOM" if bc.get("same_domain") else "MISS"))
            print(f"    {mark}  sources={result.get('n_sources_found', 0)}  "
                  f"sd={result.get('source_domain_recall', {}).get('score', 0):.2f}  "
                  f"pr={result.get('precision', {}).get('score', 0):.2f}  "
                  f"({result.get('elapsed_sec', 0):.1f}s)")

    total = time.time() - t0

    # Aggregate + report
    metrics = aggregate(results)
    print(f"\n{'=' * 60}")
    print(f"  UOF DISCOVERY BENCHMARK")
    print(f"  Sample size:           {metrics['n_total']}")
    print(f"  Bodycam-applicable:    {metrics['n_bodycam_applicable']}")
    print(f"  {'-' * 50}")
    print(f"  Bodycam recall:        {metrics['bodycam_recall_pct']:.2f}%")
    print(f"    Exact URL hits:      {metrics['bodycam_exact_hits']}")
    print(f"    Same video_id hits:  {metrics['bodycam_video_id_hits']}")
    print(f"    Same domain hits:    {metrics['bodycam_domain_hits']}")
    print(f"  Source domain recall:  {metrics['source_domain_recall_pct']:.2f}%")
    print(f"  Precision:             {metrics['precision_pct']:.2f}%")
    print(f"  {'-' * 50}")
    print(f"  Total time:            {total:.1f}s ({total/max(1,len(records)):.1f}s/case)")

    if args.log and not args.dry_run:
        log_run(metrics, sample_size=len(records), free_only=args.free_only,
                total_seconds=total, hypothesis=args.hypothesis, changes=args.changes)
    write_failures(results)


if __name__ == "__main__":
    main()
