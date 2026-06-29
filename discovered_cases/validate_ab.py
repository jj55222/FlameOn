"""Full file-by-file validation of every tier A + B bundle (no downloads).

Probes EVERY file URL in the A/B bundles with a 1-byte range request (follows
redirects), records live/status/size/content-type, and flags any A/B bundle that
has a dead file — i.e. a bundle the tier calls pipeline-ready but whose URLs don't
all resolve. Free (range probes only, no bytes, no API). RESUMABLE: results cache
keyed by URL; a rerun skips what's done. Checkpoints as it goes.

    python discovered_cases/validate_ab.py            # all A+B
    python discovered_cases/validate_ab.py --tier A   # just A
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

ROOT = Path(__file__).parent
AGG = ROOT / "CASE_BUNDLE_AGG.json"
CACHE = ROOT / "validate_ab_results.json"
REPORT = ROOT / "validate_ab_report.md"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

_session = requests.Session()
_session.headers.update({"User-Agent": UA})


def probe(url: str, timeout: float = 15.0) -> dict:
    try:
        r = _session.get(url, headers={"Range": "bytes=0-0"}, allow_redirects=True,
                         timeout=timeout, stream=True)
        cr = r.headers.get("Content-Range", "")
        ct = r.headers.get("Content-Type", "")[:40]
        size = int(cr.split("/")[-1]) if "/" in cr and cr.split("/")[-1].isdigit() else None
        live = r.status_code in (200, 206)
        r.close()
        return {"live": live, "status": r.status_code,
                "size_mb": round(size / 1e6, 1) if size else None, "ctype": ct}
    except requests.RequestException as e:
        return {"live": False, "status": None, "error": type(e).__name__}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tier", action="append", default=None, help="tiers to validate (default A and B)")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--timeout", type=float, default=15.0)
    args = ap.parse_args()
    tiers = set(args.tier or ["A", "B"])

    bundles = [b for b in json.loads(AGG.read_text())["bundles"] if b.get("tier") in tiers]
    cache = json.loads(CACHE.read_text()) if CACHE.exists() else {}
    todo = sorted({f["url"] for b in bundles for f in b["files"] if f["url"] and f["url"] not in cache})
    print(f"[validate] {len(bundles)} bundles ({sorted(tiers)}); "
          f"{len(todo)} files to probe, {len(cache)} cached")

    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(probe, u, args.timeout): u for u in todo}
        for fut in as_completed(futs):
            cache[futs[fut]] = fut.result()
            done += 1
            if done % 100 == 0:
                CACHE.write_text(json.dumps(cache))
                print(f"  probed {done}/{len(todo)}", flush=True)
    CACHE.write_text(json.dumps(cache))

    # aggregate per source + flag bundles with any dead file
    per_src = defaultdict(lambda: {"bundles": 0, "files": 0, "live": 0, "dead": 0})
    bad_bundles = []
    for b in bundles:
        s = per_src[b["source"]]
        s["bundles"] += 1
        dead_here = []
        for f in b["files"]:
            res = cache.get(f["url"], {})
            s["files"] += 1
            if res.get("live"):
                s["live"] += 1
            else:
                s["dead"] += 1
                dead_here.append((f.get("type"), res.get("status"), f["url"]))
        if dead_here:
            bad_bundles.append((b["case_id"], b["source"], b.get("tier"), len(dead_here), len(b["files"])))

    L = ["# A/B Bundle Validation — file-by-file", "",
         f"Probed every file URL in tier {'/'.join(sorted(tiers))} bundles (1-byte range, no downloads).", "",
         "| source | bundles | files | live | dead | live % |",
         "|--------|--------:|------:|-----:|-----:|-------:|"]
    tl = td = 0
    for s in sorted(per_src):
        v = per_src[s]; tl += v["live"]; td += v["dead"]
        pct = 100 * v["live"] / v["files"] if v["files"] else 0
        L.append(f"| {s} | {v['bundles']} | {v['files']} | {v['live']} | {v['dead']} | {pct:.1f}% |")
    L.append(f"| **TOTAL** | {len(bundles)} | {tl+td} | {tl} | {td} | {100*tl/(tl+td) if tl+td else 0:.1f}% |")
    L += ["", f"## Bundles with ≥1 dead file ({len(bad_bundles)}) — NOT fully valid", ""]
    if not bad_bundles:
        L.append("- none — every A/B bundle's files all resolve ✓")
    for cid, src, tier, nd, nf in sorted(bad_bundles, key=lambda x: -x[3])[:60]:
        L.append(f"- [{tier}] {src} · `{cid}` — {nd}/{nf} dead")
    REPORT.write_text("\n".join(L) + "\n")

    print(f"[validate] live {tl}/{tl+td} ({100*tl/(tl+td) if tl+td else 0:.1f}%); "
          f"{len(bad_bundles)} bundle(s) with a dead file -> {REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
