"""
Case Bundle Aggregator — ONE registry of every working case bundle's download URLs.

Scans every ``*_candidates.json`` harvester output in this directory (SF DPA,
SDPD, COPA, Long Beach, MuckRock, …), pulls each bundle's media + document file
URLs, and writes a single aggregate the whole project (and other agents) can read:

  discovered_cases/CASE_BUNDLE_AGG.json   <- machine-readable registry (the source of truth)
  discovered_cases/CASE_BUNDLE_AGG.md     <- human-readable index of the same

A "working" bundle = one that resolves to at least one downloadable file URL.

------------------------------------------------------------------------------
FOR CODEX (or any agent) — how to add bundles to this registry:
  Option A (preferred): drop a ``<yourportal>_candidates.json`` into
    discovered_cases/ (same shape as the other harvesters: a list of objects with
    source, case_id, agency, case_url, n_video/n_audio/n_docs, and
    media_files/doc_files lists of {name, url}). Then re-run:
        python discovered_cases/case_bundle_agg.py
  Option B (hand-add a few): append bundles to
    discovered_cases/CASE_BUNDLE_AGG.manual.json  (a JSON list of bundle objects,
    same fields as below). They are merged in on the next run.

Do NOT hand-edit CASE_BUNDLE_AGG.json/.md — they are regenerated. Add a source
file instead (Option A/B) so the registry stays reproducible.
------------------------------------------------------------------------------

Usage:
  python discovered_cases/case_bundle_agg.py                 # all working bundles
  python discovered_cases/case_bundle_agg.py --media-only    # only bundles with video/audio
  python discovered_cases/case_bundle_agg.py --selftest
"""
from __future__ import annotations

import argparse
import glob
import json
import os
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).parent
JSON_OUT = ROOT / "CASE_BUNDLE_AGG.json"
MD_OUT = ROOT / "CASE_BUNDLE_AGG.md"
MANUAL = ROOT / "CASE_BUNDLE_AGG.manual.json"

# Harvester outputs to aggregate. Exclude probe/ranked/derived views and our own
# output so re-runs don't double-count.
EXCLUDE = {"muckrock_candidates_probe.json", "candidates_ranked.json",
           "CASE_BUNDLE_AGG.json"}

_MEDIA_TYPES = {"bodycam", "dashcam", "interrogation", "audio", "video", "media",
                "911", "radio", "surveillance"}


def source_files() -> List[Path]:
    return [Path(p) for p in sorted(glob.glob(str(ROOT / "*_candidates.json")))
            if os.path.basename(p) not in EXCLUDE]


def _files_of(c: Dict[str, Any]) -> List[Dict[str, str]]:
    """Flatten a candidate's media + doc files to ``[{name, type, url}]`` (deduped),
    tolerating the small schema differences between harvesters."""
    out: List[Dict[str, str]] = []
    for f in (c.get("media_files") or []):
        url = f.get("url") or f.get("download_url")
        if url:
            out.append({"name": f.get("name", ""), "url": url,
                        "type": (f.get("evidence_type") or f.get("kind") or "media")})
    for f in (c.get("doc_files") or []):
        url = f.get("url") or f.get("download_url")
        if url:
            out.append({"name": f.get("name", ""), "url": url,
                        "type": (f.get("evidence_type") or "document")})
    if not out:                                  # last resort: a bare url list
        for url in (c.get("urls") or []):
            if url:
                out.append({"name": "", "url": url, "type": "media"})
    seen, ded = set(), []
    for f in out:
        if f["url"] not in seen:
            seen.add(f["url"])
            ded.append(f)
    return ded


def to_bundle(c: Dict[str, Any], origin: str) -> Dict[str, Any]:
    files = _files_of(c)
    nv, na, nd = int(c.get("n_video") or 0), int(c.get("n_audio") or 0), int(c.get("n_docs") or 0)
    return {
        "source": c.get("source") or Path(origin).stem.replace("_candidates", ""),
        "case_id": c.get("case_id") or "",
        "agency": c.get("agency") or "",
        "title": c.get("title") or c.get("folder") or c.get("case_id") or "",
        "case_url": c.get("case_url") or c.get("document_index_url") or "",
        "n_video": nv, "n_audio": na, "n_docs": nd,
        "n_files": len(files),
        "score": c.get("score"),
        "downloads": c.get("total_downloads"),
        "origin_file": Path(origin).name,
        "files": files,
    }


def has_media(b: Dict[str, Any]) -> bool:
    return (b["n_video"] + b["n_audio"]) > 0 or any(
        f["type"] in _MEDIA_TYPES for f in b["files"])


def collect(media_only: bool = False) -> List[Dict[str, Any]]:
    bundles: Dict[tuple, Dict[str, Any]] = {}
    srcs = source_files()
    if MANUAL.exists():
        srcs.append(MANUAL)
    for sf in srcs:
        try:
            data = json.loads(sf.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        items = data if isinstance(data, list) else data.get("bundles", data.get("candidates", []))
        for c in items:
            if not isinstance(c, dict):
                continue
            b = to_bundle(c, str(sf))
            if b["n_files"] == 0:                # not a WORKING bundle (no URLs)
                continue
            if media_only and not has_media(b):
                continue
            bundles[(b["source"], b["case_id"])] = b      # dedup by (source, case_id)
    return sorted(bundles.values(),
                  key=lambda b: (-(b["n_video"] + b["n_audio"]), -(b.get("score") or 0),
                                 b["source"], b["case_id"]))


def write_json(bundles: List[Dict[str, Any]], srcs: List[Path]) -> None:
    JSON_OUT.write_text(json.dumps({
        "what": "Aggregate registry of working case-bundle download URLs across all portal harvests.",
        "regenerate": "python discovered_cases/case_bundle_agg.py  (auto-discovers *_candidates.json)",
        "add_more": "drop a <portal>_candidates.json here, or append to CASE_BUNDLE_AGG.manual.json",
        "sources": [s.name for s in srcs],
        "n_bundles": len(bundles),
        "n_files": sum(b["n_files"] for b in bundles),
        "bundles": bundles,
    }, indent=2, ensure_ascii=False), encoding="utf-8")


def write_md(media_bundles: List[Dict[str, Any]], all_bundles: List[Dict[str, Any]],
             srcs: List[Path]) -> None:
    by_source: Dict[str, List[Dict[str, Any]]] = {}
    for b in media_bundles:
        by_source.setdefault(b["source"], []).append(b)
    doc_only = len(all_bundles) - len(media_bundles)
    L: List[str] = []
    L.append("# Case Bundle Agg — download URLs for every working case bundle")
    L.append("")
    L.append("> Registry of case bundles found across all portal harvests, with their direct download")
    L.append("> URLs. **Generated** by `discovered_cases/case_bundle_agg.py` — do not hand-edit; add a")
    L.append("> `<portal>_candidates.json` (or append to `CASE_BUNDLE_AGG.manual.json`) and re-run.")
    L.append("> This index lists the **media bundles (video/audio)**. The COMPLETE registry — including")
    L.append(f"> {doc_only} document-only bundles and every URL — is in `CASE_BUNDLE_AGG.json`.")
    L.append("")
    L.append(f"**{len(all_bundles)} working bundles total ({len(media_bundles)} with media, "
             f"{doc_only} doc-only) · {sum(b['n_files'] for b in all_bundles)} files** — "
             f"media index below: {sum(b['n_video'] for b in media_bundles)} video · "
             f"{sum(b['n_audio'] for b in media_bundles)} audio.")
    L.append("")
    L.append("| source | media bundles | video | audio | docs | (all working) |")
    L.append("|--------|--------------:|------:|------:|-----:|--------------:|")
    allbysrc: Dict[str, int] = {}
    for b in all_bundles:
        allbysrc[b["source"]] = allbysrc.get(b["source"], 0) + 1
    for s in sorted(set(list(by_source) + list(allbysrc))):
        bs = by_source.get(s, [])
        L.append(f"| {s} | {len(bs)} | {sum(b['n_video'] for b in bs)} | "
                 f"{sum(b['n_audio'] for b in bs)} | {sum(b['n_docs'] for b in bs)} | {allbysrc.get(s,0)} |")
    L.append("")
    for s in sorted(by_source):
        bs = by_source[s]
        agency = next((b["agency"] for b in bs if b["agency"]), "")
        L.append(f"## {s}" + (f" — {agency}" if agency else ""))
        L.append("")
        for b in bs:
            head = (f"### {b['case_id']}  ·  V{b['n_video']} A{b['n_audio']} D{b['n_docs']}"
                    + (f"  ·  score {b['score']}" if b.get("score") is not None else "")
                    + (f"  ·  {b['downloads']} downloads" if b.get("downloads") else ""))
            L.append(head)
            if b["case_url"]:
                L.append(f"case: {b['case_url']}")
            for f in b["files"]:
                nm = (f["name"] or "").replace("|", "/")
                L.append(f"- [{f['type']}] {nm}".rstrip() + f"\n  {f['url']}")
            L.append("")
    MD_OUT.write_text("\n".join(L), encoding="utf-8")


def _selftest() -> int:
    c = {"source": "demo", "case_id": "x-1", "agency": "Demo PD",
         "n_video": 1, "n_audio": 2, "n_docs": 1,
         "media_files": [{"name": "bwc.mp4", "url": "http://h/a/download", "evidence_type": "bodycam"},
                         {"name": "int.mp3", "url": "http://h/b/download", "evidence_type": "interrogation"},
                         {"name": "dup", "url": "http://h/b/download"}],   # dup url
         "doc_files": [{"name": "report.pdf", "url": "http://h/c/download"}]}
    b = to_bundle(c, "demo_candidates.json")
    assert b["n_files"] == 3, f"dedup by url -> 3, got {b['n_files']}"
    assert has_media(b) and {f["type"] for f in b["files"]} >= {"bodycam", "interrogation", "document"}
    empty = to_bundle({"source": "d", "case_id": "y"}, "d.json")
    assert empty["n_files"] == 0 and not has_media(empty)
    print("case_bundle_agg selftest: OK  (dedup, media detect, empty-bundle skip)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--media-only", action="store_true", help="only bundles with video/audio (skip doc-only)")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        return _selftest()

    srcs = source_files() + ([MANUAL] if MANUAL.exists() else [])
    bundles = collect(media_only=args.media_only)
    write_json(bundles, srcs)
    write_md(bundles, srcs)
    print(f"sources: {[s.name for s in srcs]}")
    print(f"working bundles: {len(bundles)}  files: {sum(b['n_files'] for b in bundles)}")
    print(f"  -> {JSON_OUT}")
    print(f"  -> {MD_OUT}")
    from collections import Counter
    per = Counter(b["source"] for b in bundles)
    for s, n in per.most_common():
        print(f"     {s:24s} {n} bundles")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
