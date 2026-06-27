"""Emit the merged Dutchess golden key as an editable review checklist + a compact console view,
so a human can prune it (demote over-flagged must-finds, cut trivia) before it's frozen as the yardstick."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
G = ROOT / "pipeline4_scoring/golden/sac_so_20-269838_dutchess_way.golden.merged.json"
MD = ROOT / "pipeline4_scoring/golden/golden_review.md"

d = json.load(open(G))
ms = d["moments"]


def loc(m):
    if m["source_layer"] == "audio":
        s = int(m.get("start_sec") or 0)
        return f"{(m.get('artifact_id') or '?')[:20]} @{s//60}:{s%60:02d}"
    return f"p{m.get('page', '?')}"


ms2 = sorted(ms, key=lambda m: (m["source_layer"], -(m.get("salience") or 0)))
na = sum(1 for m in ms if m["source_layer"] == "audio")
nmf = sum(1 for m in ms if m.get("must_find"))

lines = [
    "# Dutchess golden key — REVIEW & PRUNE",
    f"\n{len(ms)} moments ({na} audio + {len(ms)-na} doc). **must_find = {nmf}** — far too many; a real key has ~8-12.",
    "\nHow to prune: keep a moment by leaving `[x]`; cut it → `[ ]`. Demote a false must-find by deleting the `★`.",
    "Then tell me your cuts (or save this file and I'll re-read it).\n",
]
for layer in ["document", "audio"]:
    lines.append(f"\n## {layer} beats\n")
    for m in [x for x in ms2 if x["source_layer"] == layer]:
        star = "★" if m.get("must_find") else " "
        q = (m.get("evidence_quote") or "").replace("\n", " ")[:72]
        lines.append(f"- [x] {star} `{m['moment_id']}` s{m.get('salience')} **{m.get('moment_type')}** ({loc(m)}) — \"{q}\"")
MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"wrote {MD.relative_to(ROOT)}  ({len(ms)} moments, {nmf} must-find)\n")

for layer in ["document", "audio"]:
    grp = [x for x in ms2 if x["source_layer"] == layer]
    print(f"=== {layer} ({len(grp)}) ===")
    for m in grp:
        star = "★" if m.get("must_find") else " "
        q = (m.get("evidence_quote") or "").replace("\n", " ")[:46]
        print(f"  {m['moment_id']:<4} s{m.get('salience')} {star} {(m.get('moment_type') or '')[:16]:<16} {loc(m):<26} {q!r}")
    print()
