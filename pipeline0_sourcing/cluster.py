"""
cluster.py — group raw signals about the SAME incident into one candidate.

Multiple outlets cover the same shooting; we want one incident (with all its
source URLs), not ten near-duplicate rows. Pure stdlib (difflib), deterministic,
offline-testable. Heuristic, not ML: normalize the headline, then union signals
whose normalized titles are similar OR share enough rare tokens.
"""
from __future__ import annotations

import argparse
import json
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List

# very common news/crime words carry no disambiguating signal
_STOP = {
    "the", "a", "an", "of", "in", "on", "to", "for", "and", "after", "with", "as",
    "police", "man", "woman", "officer", "say", "says", "said", "dead", "dies",
    "death", "shooting", "shot", "video", "footage", "released", "county", "city",
    "update", "report", "new", "news", "old", "year", "years",
}
SIM_THRESHOLD = 0.62       # normalized-title similarity to merge
SHARED_TOKENS_MIN = 2      # OR: this many shared rare tokens

# lower-cased full state names — used only as a BLOCKING key (different explicit
# states => never the same incident), so structurally-identical headlines about
# different states ("...man dead in Florida" vs "...in Pennsylvania") don't merge.
_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut",
    "delaware", "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa",
    "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan",
    "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada", "ohio",
    "oklahoma", "oregon", "pennsylvania", "tennessee", "texas", "utah", "vermont",
    "virginia", "washington", "wisconsin", "wyoming",
}


def _norm(title: str) -> str:
    t = title.lower()
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _tokens(norm_title: str) -> set:
    return {w for w in norm_title.split() if w not in _STOP and len(w) > 2}


def _state_hint(text: str) -> str:
    hits = {s for s in _STATES if re.search(rf"\b{s}\b", text)}
    return next(iter(hits)) if len(hits) == 1 else ""   # only a clean single-state hit blocks


def _same_incident(a: Dict, b: Dict) -> bool:
    # blocking key: two clean, DIFFERENT state hints => definitely not the same incident
    if a["_state"] and b["_state"] and a["_state"] != b["_state"]:
        return False
    na, nb = a["_norm"], b["_norm"]
    if SequenceMatcher(None, na, nb).ratio() >= SIM_THRESHOLD:
        return True
    return len(a["_tok"] & b["_tok"]) >= SHARED_TOKENS_MIN


def cluster_signals(signals: List[Dict]) -> List[Dict]:
    """Union-find-ish greedy clustering. Returns incident dicts."""
    enriched = []
    for s in signals:
        n = _norm(s.get("title", ""))
        blob = _norm(s.get("title", "") + " " + s.get("snippet", ""))
        enriched.append({**s, "_norm": n, "_tok": _tokens(n), "_state": _state_hint(blob)})

    incidents: List[List[Dict]] = []
    for s in enriched:
        placed = False
        for group in incidents:
            if _same_incident(s, group[0]):
                group.append(s)
                placed = True
                break
        if not placed:
            incidents.append([s])

    out: List[Dict] = []
    for i, group in enumerate(incidents):
        # headline = longest title (usually the most descriptive)
        headline = max((g["title"] for g in group), key=len)
        urls = []
        srcs = []
        for g in group:
            if g.get("url") and g["url"] not in urls:
                urls.append(g["url"])
            if g.get("source") and g["source"] not in srcs:
                srcs.append(g["source"])
        key = re.sub(r"[^a-z0-9]+", "_", group[0]["_norm"])[:60].strip("_") or f"incident_{i}"
        out.append({
            "incident_key": key,
            "headline": headline,
            "n_reports": len(group),
            "urls": urls,
            "sources": srcs,
            "snippets": [g.get("snippet", "") for g in group if g.get("snippet")][:5],
            "queries": sorted({g.get("query", "") for g in group}),
            "first_published": min((g.get("published", "") for g in group), default=""),
        })
    # more corroborating reports first — a signal of newsworthiness
    out.sort(key=lambda x: x["n_reports"], reverse=True)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Cluster signals into incidents.")
    ap.add_argument("--signals", required=True, help="signals JSON from ingest.py")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    signals = json.loads(Path(a.signals).read_text())
    incidents = cluster_signals(signals)
    print(f"[cluster] {len(signals)} signals -> {len(incidents)} incidents")
    if a.out:
        Path(a.out).write_text(json.dumps(incidents, indent=2))
        print(f"[cluster] wrote {a.out}")
    else:
        for inc in incidents[:20]:
            print(f"  - ({inc['n_reports']}x) {inc['headline'][:90]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
