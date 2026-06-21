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


def extract_ia(pages: List[Dict]) -> Dict:
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


# --- Use-of-Force Blueteam FORM extractor (labeled fields, not narrative) ---
_UOF_DATE = re.compile(r"Date of\s*Occurrence.{0,80}?(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+(\d{1,2}:\d{2})", re.S)
_UOF_REPORT = re.compile(r"Event\s*/?\s*Report\s*#.{0,45}?(\d{2}-\d{5,6})", re.S)
_UOF_DEPUTY = re.compile(r"Deputy Sheriff\s+([A-Z][A-Z'\-]+\s+[A-Z][A-Z'\-]+)")
_UOF_VIDEO = re.compile(r"Camera:\s*(.+?)\s*Time:\s*([\d:][\d:\- ]*?(?:Hours|hrs|hours)?)\s*(?:\n|Additional|$)", re.I)
_UOF_LOC = re.compile(r"Addresses\s*([\dA-Za-z .,'#-]+?CA[,\s]*\d{0,5})", re.S)
_UOF_WEAPON = re.compile(r"(Confirmed Sharp Weapon|Edged Weapon|Sharp Weapon|Firearm|Knife)", re.I)
_UOF_REASON = re.compile(r"Reason For Using Force[^\n]*\n\s*([A-Z][a-z]+(?:\s[A-Za-z]+){0,2})")

# Charges come ONLY from explicitly-labeled charge fields — never a global "PC"
# scan. These bundles are dense with penal-code citations that are NOT charges:
# PC 832.7 (peace-officer personnel-records exemption) and PC 13300-13302 / 13800
# (DOJ criminal-records act) are redaction stamps on nearly every page, and CAD
# logs carry field codes (PC 08/09). A blanket scan swept all of those up as
# "charges". Three labeled contexts cover the DA/booking + dispatch formats:
#   1. "Charge Statute PC <statute>" — structured CHARGE SUMMARY (formal counts)
#   2. "Charges: PC.. PC.."          — an explicit charges label, when present
#   3. "Clear remarks:/REM-" line    — CAD disposition; carries refs the formal
#                                      summary drops (e.g. an unfounded PC 417)
_CHARGE_STATUTE = re.compile(r"Charge\s+Statute\s+PC\s*(\d{2,4})", re.I)
_CHARGES_LABEL = re.compile(r"\bCharges?\b\s*[:\-]?\s*((?:PC\s?\d{2,4}[\w()./]*[ ,]*){1,12})", re.I)
_REMARKS = re.compile(r"(?:Clear\s+remarks|\bREM)\s*[:\-]\s*([^\n]+)", re.I)
_PC_NUM = re.compile(r"(?:PC)?\s?(\d{2,4})", re.I)
_REMARK_STARTS_PC = re.compile(r"PC\s?\d", re.I)
_WORD3 = re.compile(r"[A-Za-z]{3,}")


def _charges_from_remark(value: str) -> List[str]:
    """Statutes from a CAD clear-remark, but only when it leads with the charge
    list (e.g. "PC459, X2 148A(1), 417 UNFOUNDED"). Disposition-only remarks
    ("CHECKED OKAY", "SUBJ LEFT") don't start with PC and are skipped. Reads only
    the leading run — up to the first word like "UNFOUNDED" — so a CAD mega-line
    can't trail its PC 832.7 / PC 13300 redaction citations into the result."""
    value = value.strip()
    if not _REMARK_STARTS_PC.match(value):
        return []
    w = _WORD3.search(value)
    run = value[:w.start()] if w else value
    return [m.group(1) for m in _PC_NUM.finditer(run)]


def _extract_charges(full: str) -> List[str]:
    """Suspect charges from labeled fields only — base PC statutes, deduped."""
    nums = set()
    for m in _CHARGE_STATUTE.finditer(full):
        nums.add(m.group(1))
    for m in _CHARGES_LABEL.finditer(full):
        nums.update(s.group(1) for s in re.finditer(r"PC\s?(\d{2,4})", m.group(1), re.I))
    for m in _REMARKS.finditer(full):
        nums.update(_charges_from_remark(m.group(1)))
    return sorted("PC " + n for n in nums)


def extract_uof(pages: List[Dict], full: str) -> Dict:
    out: Dict = {"doc_type": "uof_form", "ia_case_number": None, "subject": None,
                 "doc_date": None, "incident_time": None, "location": None,
                 "weapon": None, "reason_for_force": None, "citizen_arrested": None,
                 "disposition": {"findings": [], "discipline_signals": [], "summary": None, "pages": []},
                 "narrative": None, "charges": [], "evidence_pointers": [],
                 "clip_directions": [], "people": [], "dates": []}
    m = _UOF_REPORT.search(full)
    if m:
        out["ia_case_number"] = m.group(1)
    m = _UOF_DATE.search(full)
    if m:
        out["doc_date"], out["incident_time"] = m.group(2), m.group(3)
    m = _UOF_DEPUTY.search(full)
    if m:
        out["subject"] = " ".join(w.capitalize() for w in m.group(1).split())
        out["people"].append(out["subject"])
    m = _UOF_LOC.search(full)
    if m:
        out["location"] = _norm(m.group(1))[:70]
    m = _UOF_WEAPON.search(full)
    if m:
        out["weapon"] = m.group(1)
    m = _UOF_REASON.search(full)
    if m:
        out["reason_for_force"] = _norm(m.group(1))
    out["citizen_arrested"] = bool(re.search(r"Citizen Arrested\s*\n?\s*Yes", full, re.I)) \
        or "Suspect/Arrestee" in full
    out["charges"] = _extract_charges(full)

    # CLIP DIRECTIONS straight from the form's "Video Available" field — these
    # point at our actual footage (camera + relevant time window).
    for pg, t in ((p["page"], p.get("text", "") or "") for p in pages):
        for vm in _UOF_VIDEO.finditer(t):
            out["clip_directions"].append(
                {"kind": "bwc_window", "ref": _norm(vm.group(1))[:60],
                 "window": _norm(vm.group(2)), "page": pg, "priority": 3})
    out["evidence_pointers"] = list(out["clip_directions"])

    # Narrative + disposition summary (a UoF report documents force; it carries
    # no SUSTAINED/UNFOUNDED finding like an IA complaint).
    bits = []
    if out["reason_for_force"]:
        bits.append(f"reason: {out['reason_for_force']}")
    if out["weapon"]:
        bits.append(out["weapon"].lower())
    if out["citizen_arrested"]:
        bits.append("suspect arrested")
    out["disposition"]["summary"] = (
        "Use-of-Force report" + (" — " + "; ".join(bits) if bits else "")
        + "; supervisor-reviewed, no further investigation.")
    return out


def _compose_outcome_card(d: Dict) -> Dict:
    """Per-doc-type outcome card text (title + subtitle)."""
    if d.get("doc_type") == "uof_form":
        parts = []
        if d.get("doc_date"):
            parts.append(d["doc_date"])
        if d.get("subject"):
            parts.append(f"Dep. {d['subject'].split()[-1]}")
        if d.get("weapon"):
            parts.append(d["weapon"])
        if d.get("location"):
            parts.append(d["location"])
        sub = " · ".join(parts) + ".  Disposition: supervisor-reviewed, no further investigation."
        return {"title": f"Use of Force · #{d.get('ia_case_number', '')}", "subtitle": sub}
    # IA narrative
    dispo = d.get("disposition", {})
    if dispo.get("findings"):
        findings = "; ".join(f"{f['finding']}: {f['charge'][:45]}" for f in dispo["findings"][:3])
        disc = ", ".join(dispo.get("discipline_signals", [])[:4])
        return {"title": f"IA {d.get('ia_case_number', '')}: SUSTAINED",
                "subtitle": findings + (f".  Discipline: {disc}." if disc else ".")}
    return {"title": "Outcome", "subtitle": dispo.get("summary") or ""}


def extract(pages: List[Dict]) -> Dict:
    """Dispatch on document type: Blueteam UoF form vs IA narrative report."""
    full = "\n".join(p.get("text", "") or "" for p in pages)
    if re.search(r"Use\s*Of\s*Force\s*Report|Blueteam", full, re.I):
        data = extract_uof(pages, full)
    else:
        data = extract_ia(pages)
        data["doc_type"] = "ia_narrative"
    data["outcome_card"] = _compose_outcome_card(data)
    return data


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
    oc = data.get("outcome_card", {})
    print(f"doc_type: {data.get('doc_type')}  case#: {data['ia_case_number']}  "
          f"subject: {data['subject']}  date: {data['doc_date']}")
    print(f"OUTCOME CARD: {oc.get('title')} | {oc.get('subtitle', '')[:130]}")
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
