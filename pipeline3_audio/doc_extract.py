"""P3.5 / GOAL_D D5 — extract structure from an OCR'd case document.

Turns an OCR'd IA / case-investigation PDF (see doc_ocr.py) into structured
JSON that does two jobs:

  1. SUPPLEMENT THE NARRATIVE  — disposition/outcome, statement-of-problem,
     charges, named people + dates → P6 outcome card, scene-set, lower-thirds.
  2. DIRECT THE CLIPPER         — "evidence pointers": the document's own
     references to footage (Axon video IDs + timestamps, CAD events, citation /
     crime-report #s, "BWC shows X" descriptions, interview page refs). These
     tell the slicer WHICH clip to pull and WHY it matters, instead of guessing
     from audio loudness alone.

Deterministic regex/heuristics — these IA files are highly templated. Pure
stdlib. Generalises across cases (patterns, not case-specific strings).

    python pipeline3_audio/doc_extract.py --pages .tmp/sac_poc/docs/ocr/pages.json \
        --out .tmp/sac_poc/docs/doc_extract.json
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Optional

_IA = re.compile(r"\b(20\d{2}PSB-\d{3,4})\b")
_SUBJECT = re.compile(
    r"Subject\s*(?:Employee)?:?\s*(?:Deputy|Officer|Sergeant|Detective)?\s*"
    r"([A-Z][a-z]+ (?:[A-Z]\.? )?[A-Z][a-z]+)")
_SUBJECT2 = re.compile(r"\b(Deputy|Officer|Sergeant|Detective)\s+([A-Z][a-z]+ [A-Z][a-z]+)")
_DISPO = re.compile(r"\b(SUSTAINED|NOT SUSTAINED|EXONERATED|UNFOUNDED)\b")
_DOC_DATE = re.compile(r"Date:?\s*([A-Z][a-z]+ \d{1,2},? 20\d{2})")
_DISCIPLINE = re.compile(
    r"\b(termination|terminated|dismiss(?:al|ed)|resign|Skelly|"
    r"letter of reprimand|suspension|suspended|discipline affirmed)\b", re.I)

# Evidence pointers (what the document tells the clipper to look at)
_VIDEO = re.compile(r"(Axon\s*Body\s*\d[^.\n]{0,70}?\d{4}[^.\n]{0,45})", re.I)
_CAD = re.compile(r"CAD\s*Event[s]?\s*[#:]?\s*(\d{2}-\d{5,6})", re.I)
_CITATION = re.compile(r"Citation\s*#?\s*(\d{4,})", re.I)
_CRIMEREP = re.compile(r"Crime\s*Report[s]?\s*[#:]?\s*(\d{2}-\d{5,6}(?:[,\s]+\d{2}-\d{5,6})*)", re.I)
_INTERVIEW = re.compile(r"Interview,?\s*pages?\s*\d+(?:\s*-\s*\d+)?", re.I)
_BWC_DESC = re.compile(
    r"(?:BWC|body[\s-]?worn camera)[^.]{0,40}?(?:shows?|depict[s]?|captured?|footage[^.]{0,15}?(?:shows?|depict))"
    r"([^.]{15,240})", re.I)
_DATETIME = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\s+(\d{3,4})\b")  # 2023-10-24 1743


def _norm(t: str) -> str:
    return re.sub(r"[ \t]+", " ", (t or "")).strip()


def extract(pages: List[Dict]) -> Dict:
    full = "\n".join(p.get("text", "") or "" for p in pages)
    out: Dict = {"ia_case_number": None, "subject": None, "doc_date": None,
                 "disposition": {"findings": [], "discipline_signals": [], "summary": None, "pages": []},
                 "narrative": None, "charges": [], "evidence_pointers": [], "clip_directions": [],
                 "people": [], "dates": []}

    m = _IA.search(full)
    if m:
        out["ia_case_number"] = m.group(1)
    m = _SUBJECT.search(full) or _SUBJECT2.search(full)
    if m:
        out["subject"] = m.group(1) if m.re is _SUBJECT else f"{m.group(1)} {m.group(2)}"
    m = _DOC_DATE.search(full)
    if m:
        out["doc_date"] = m.group(1)

    seen_ptr = set()
    for p in pages:
        pg = p.get("page")
        t = _norm(p.get("text", ""))
        if not t:
            continue

        # Disposition findings: a SUSTAINED/etc. with the charge text before it.
        for dm in _DISPO.finditer(t):
            pre = t[max(0, dm.start() - 90): dm.start()].strip()
            # charge name = trailing capitalized word-phrase before the finding
            # (drops the leading "SCDSA MQU 18.5(p)" policy-code noise).
            cm = re.search(r"([A-Z][a-zA-Z]+(?: [A-Za-z]+){1,6})\s*$", pre)
            charge = cm.group(1) if cm else pre[-60:]
            out["disposition"]["findings"].append(
                {"finding": dm.group(1).upper(), "charge": charge, "page": pg})
            out["disposition"]["pages"].append(pg)
        for dm in set(x.group(0) for x in _DISCIPLINE.finditer(t)):
            if dm.lower() not in [s.lower() for s in out["disposition"]["discipline_signals"]]:
                out["disposition"]["discipline_signals"].append(dm)

        # Narrative: statement of problem / synopsis.
        sm = re.search(r"STATEMENT\s*O[FQ]\s*PROBLEM:?(.{60,1200})", t, re.I)
        if sm and not out["narrative"]:
            out["narrative"] = {"text": sm.group(1).strip()[:1000], "page": pg}

        # Evidence pointers.
        def add(kind, ref, **extra):
            key = (kind, ref)
            if key in seen_ptr:
                return
            seen_ptr.add(key)
            out["evidence_pointers"].append({"kind": kind, "ref": ref, "page": pg, **extra})

        for vm in _VIDEO.finditer(t):
            ref = _norm(vm.group(1))
            depicts = t[vm.end(): vm.end() + 200]
            dm = re.search(r"(?:depict|show|engage)[^.]{0,200}", depicts, re.I)
            add("video", ref, depicts=_norm(dm.group(0))[:180] if dm else "", priority=3)
        for cm in _CAD.finditer(t):
            add("cad_event", cm.group(1), priority=2)
        for cm in _CITATION.finditer(t):
            add("citation", cm.group(1), priority=1)
        for cm in _CRIMEREP.finditer(t):
            add("crime_report", _norm(cm.group(1)), priority=2)
        for im in _INTERVIEW.finditer(t):
            add("interview_ref", _norm(im.group(0)), priority=2)
        for bm in _BWC_DESC.finditer(t):
            add("bwc_description", _norm(bm.group(1))[:200], priority=3)

    # Clip directions = the high-value, footage-facing pointers, ranked.
    out["clip_directions"] = sorted(
        [e for e in out["evidence_pointers"] if e["kind"] in ("video", "bwc_description", "cad_event")],
        key=lambda e: -e.get("priority", 0))

    # Dedupe dispositions (charge+finding).
    seen = set(); ded = []
    for f in out["disposition"]["findings"]:
        k = (f["finding"], f["charge"][:30])
        if k not in seen:
            seen.add(k); ded.append(f)
    out["disposition"]["findings"] = ded
    out["disposition"]["pages"] = sorted(set(out["disposition"]["pages"]))
    return out


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="GOAL_D D5 — extract structure from an OCR'd case doc")
    ap.add_argument("--pages", required=True, type=Path, help="doc_ocr pages.json")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args(argv)

    pages = json.loads(args.pages.read_text(encoding="utf-8"))
    data = extract(pages)
    out = args.out or args.pages.parent / "doc_extract.json"
    out.write_text(json.dumps(data, indent=2), encoding="utf-8")

    d = data["disposition"]
    print(f"IA case: {data['ia_case_number']}  subject: {data['subject']}  date: {data['doc_date']}")
    print(f"\nDISPOSITION ({len(d['findings'])} findings; discipline: {d['discipline_signals']}):")
    for f in d["findings"][:8]:
        print(f"   [{f['finding']}] {f['charge']}  (p{f['page']})")
    if data["narrative"]:
        print(f"\nNARRATIVE (p{data['narrative']['page']}): {data['narrative']['text'][:240]}...")
    print(f"\nCLIP DIRECTIONS ({len(data['clip_directions'])}) — doc tells the slicer where to look:")
    for e in data["clip_directions"][:10]:
        extra = e.get("depicts") or ""
        print(f"   [{e['kind']:15s}] {e['ref'][:55]:55s} {('— ' + extra[:60]) if extra else ''} (p{e['page']})")
    print(f"\n[doc_extract] -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
