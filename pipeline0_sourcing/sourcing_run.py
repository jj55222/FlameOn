"""
sourcing_run.py — P0 orchestrator: news/OSINT -> ranked FOIA queue.

    ingest  ->  cluster  ->  extract  ->  score  ->  draft  ->  report

Autonomous-friendly: keeps a --seen store so repeated runs surface only NEW
incidents (safe to put on a cron / in /loop). Writes a machine queue
(foia_queue.json) and a human-readable queue (foia_queue.md) you review and
submit by hand.

    # offline smoke test (no network, no API key) on the bundled fixtures:
    python sourcing_run.py --mock --signals fixtures/sample_signals.json --out .tmp/p0

    # live, free ingestion + LLM extraction (needs OPENROUTER_API_KEY in .env):
    python sourcing_run.py --since 3 --limit 40 --tier1-only --out .tmp/p0

    # plan only, hit nothing:
    python sourcing_run.py --dry-run
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from ingest import DEFAULT_TERMS, fetch_signals
from cluster import cluster_signals
from extract import extract_incidents, DEFAULT_MODEL
from score import score_incidents, load_state_profiles
from draft import draft_incidents


def _load_seen(path: Optional[Path]) -> set:
    if path and path.exists():
        try:
            return set(json.loads(path.read_text()).get("seen", []))
        except Exception:  # noqa: BLE001
            return set()
    return set()


def _save_seen(path: Optional[Path], seen: set) -> None:
    if path:
        path.write_text(json.dumps({"seen": sorted(seen)}, indent=2))


def _append_run_history(out_dir: Path, record: Dict) -> None:
    """Append one line per run to run_history.jsonl.

    This is the audit trail the WS2 health check reads: it proves the daily job
    actually ran (real, not --mock) across >= 2 runs and that the de-dupe store
    (`seen`) is accumulating. Best-effort — never fail a run over logging.
    """
    try:
        with (out_dir / "run_history.jsonl").open("a") as f:
            f.write(json.dumps(record) + "\n")
    except OSError as e:  # noqa: BLE001
        print(f"[p0] warning: could not append run_history.jsonl: {e}")


def render_markdown(drafts: List[Dict], stats: Dict) -> str:
    lines = ["# FlameOn — FOIA sourcing queue", ""]
    lines.append(f"_{stats['n_file']} FILE · {stats['n_watch']} WATCH · "
                 f"{stats['n_incidents']} incidents from {stats['n_signals']} signals._")
    lines.append("")
    lines.append("> Leads, not findings. Verify agency + jurisdiction, confirm the state-law row "
                 "(`verify_before_send`), fill `[BRACKETS]`, then submit manually.")
    lines.append("")
    for i, d in enumerate(drafts, 1):
        j = d["jurisdiction"]
        flag = "🟢 FILE" if d["verdict"] == "FILE" else "🟡 WATCH"
        lines += [
            f"## {i}. {flag} · worth {d['foia_worth']} · [{j['state']}] {d['headline']}",
            "",
            f"- **Why:** {d['why_it_matters']}",
            f"- **Type / shape:** {d['incident_type']} / {d['ewu_shape']}",
            f"- **Agency:** {d['agency'] or '(verify)'} · **State law:** {d['statute'] or '(unknown)'}"
            + (f" · respond ~{d['response_deadline_days']}d" if d['response_deadline_days'] else ""),
            f"- **Records to request:** " + "; ".join(d["records_requested"]),
        ]
        if d["caveats"]:
            lines.append(f"- **Caveats:** " + " ".join(d["caveats"]))
        if d["verify_before_send"]:
            lines.append("- **⚠ Verify the state-law row against its RCFP link before filing "
                         "(`verified: false`).**")
        if d["sources"]:
            lines.append(f"- **Sources:** " + " · ".join(f"[{k+1}]({u})" for k, u in enumerate(d["sources"][:5])))
        lines += ["", "<details><summary>Draft request letter</summary>", "",
                  "```", d["request_letter"], "```", "</details>", ""]
    return "\n".join(lines)


def run(args) -> int:
    terms = args.terms or DEFAULT_TERMS
    if args.dry_run:
        print("[p0] DRY RUN — would fetch, not fetching.")
        print(f"     terms      : {len(terms)} ({', '.join(terms[:3])}...)")
        print(f"     window     : last {args.since}d, up to {args.limit}/term/source")
        print(f"     extraction : {'MOCK (offline)' if args.mock else 'LLM ' + args.model}")
        print(f"     gate       : tier-1 only = {args.tier1_only}, min worth WATCH")
        print(f"     out        : {args.out}")
        return 0

    # 1. signals
    if args.signals:
        signals = json.loads(Path(args.signals).read_text())
        print(f"[p0] loaded {len(signals)} signals from {args.signals}")
    else:
        signals = fetch_signals(terms, args.since, args.limit)
        print(f"[p0] ingested {len(signals)} signals (last {args.since}d)")

    # 2. cluster
    incidents = cluster_signals(signals)
    print(f"[p0] clustered -> {len(incidents)} incidents")

    # de-dupe against prior autonomous runs
    seen_path = Path(args.seen) if args.seen else None
    seen = _load_seen(seen_path)
    if seen:
        before = len(incidents)
        incidents = [i for i in incidents if i["incident_key"] not in seen]
        print(f"[p0] {before - len(incidents)} already-seen incidents dropped")

    # 3. extract
    incidents = extract_incidents(incidents, mock=args.mock, model=args.model)

    # 4. score
    profiles = load_state_profiles()
    scored = score_incidents(incidents, profiles)
    if args.tier1_only:
        scored = [s for s in scored if str(s["jurisdiction"].get("tier")) == "1" or s["verdict"] == "SKIP"]
    scored = [s for s in scored if s["foia_worth"] >= args.min_worth or s["verdict"] != "SKIP"]

    # 5. draft
    drafts = draft_incidents(scored, profiles, include_watch=args.include_watch)
    stats = {
        "n_signals": len(signals), "n_incidents": len(incidents),
        "n_file": sum(1 for s in scored if s["verdict"] == "FILE"),
        "n_watch": sum(1 for s in scored if s["verdict"] == "WATCH"),
    }
    print(f"[p0] {stats['n_file']} FILE, {stats['n_watch']} WATCH -> {len(drafts)} write-ups")

    # 6. report
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "scored.json").write_text(json.dumps(scored, indent=2))
    (out / "foia_queue.json").write_text(json.dumps(drafts, indent=2))
    (out / "foia_queue.md").write_text(render_markdown(drafts, stats))
    print(f"[p0] wrote {out}/foia_queue.md  (+ .json, scored.json)")

    # remember what we surfaced so the next autonomous run is only-new
    for s in scored:
        if s["verdict"] in ("FILE", "WATCH"):
            seen.add(s["incident_key"])
    _save_seen(seen_path, seen)

    # audit trail for the WS2 health check (proves live daily autonomy over time)
    _append_run_history(out, {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "mock": bool(args.mock),
        "since_days": args.since,
        "n_signals": len(signals),
        "n_incidents_new": len(incidents),
        "n_file": stats["n_file"],
        "n_watch": stats["n_watch"],
        "n_drafts": len(drafts),
        "seen_total": len(seen),
    })
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="P0: news/OSINT -> ranked FOIA queue.")
    ap.add_argument("--terms", nargs="+", default=None, help="search terms (default: EWU-shape set)")
    ap.add_argument("--since", type=int, default=3, help="ingest look-back (days)")
    ap.add_argument("--limit", type=int, default=40, help="max signals per term per source")
    ap.add_argument("--signals", default=None, help="load signals from JSON instead of fetching")
    ap.add_argument("--mock", action="store_true", help="offline heuristic extraction (no API/network)")
    ap.add_argument("--model", default=DEFAULT_MODEL, help="OpenRouter model for extraction")
    ap.add_argument("--tier1-only", action="store_true", help="keep only tier-1 sunshine states (FL/WA/CA)")
    ap.add_argument("--include-watch", action="store_true", help="also draft WATCH incidents")
    ap.add_argument("--min-worth", type=float, default=0.0, help="drop SKIPs below this worth from output")
    ap.add_argument("--seen", default=None, help="path to a seen-incidents store (autonomous de-dupe)")
    ap.add_argument("--out", default=".tmp/p0_sourcing", help="output dir")
    ap.add_argument("--dry-run", action="store_true", help="print the plan; fetch nothing")
    return run(ap.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
