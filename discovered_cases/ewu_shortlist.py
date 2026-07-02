"""
ewu_shortlist.py — the "P5 selector" (WS3): registry -> "EWU-ready" shortlist.

Finds the cases in the registry (``CASE_BUNDLE_AGG.json``) that carry ENOUGH
EVIDENCE TO PRODUCE a full-package EWU/Dr.Insanity documentary — a case DOC
*and* bodycam *and* ideally interrogation/interview + 911 — and ranks them by
completeness x Tier-1 worth.

Two moving parts, both grounded in existing project machinery:

  1. CLASSIFIER — types every bundle's files into the EWU evidence taxonomy
     {bwc, interrogation, 911, doc, photos}. Reuses muckrock_harvest's artifact
     classifier (RE_BWC / RE_INTERR / RE_SB16 + the extension buckets) layered on
     top of the registry's own per-file ``type`` (which the harvesters, incl.
     MuckRock, already set), and refines the crude ``video``/``audio`` types with
     the same regexes + an officer-POV heuristic (an OIS "..._Video_Officer_..."
     clip IS body-worn, even when the harvester only typed it ``video``).

  2. SCORER — ``completeness_score`` (full-package bonus when BWC+doc+interrogation
     co-occur) x ``worth`` (the Tier-1 model from ``.tmp/_tier1_rubric.md``:
     worth = SEVERITY(gate) x STORY-SHAPE x HUMAN-PROXIMITY, computed here from the
     cheap registry signals — title/case_id severity + evidence-mix shape/proximity).
     The real per-case Tier-1 verdict (doc+911 LLM judge) is attached for any case
     that already has one, and is produced for the top picks in a later cheap step.

Deliberately does NOT use ``pipeline4_score``'s ``moment_density >= 60`` PRODUCE
gate — mis-calibrated for exactly these interview/doc-driven cases (STATE.md
landmine; ``P4_VS_CLAUDE_CONCORDANCE.md``).

Outputs (never hand-edit — regenerate):
  discovered_cases/ewu_shortlist.json   ranked machine-readable shortlist
  discovered_cases/ewu_shortlist.md     human-readable index

Usage:
  python discovered_cases/ewu_shortlist.py                    # classify + rank + emit
  python discovered_cases/ewu_shortlist.py --pull-top 3       # + fetch doc+911 for top 3 baskets
  python discovered_cases/ewu_shortlist.py --audit 10 --seed 7  # eyeball 10 random classifications
  python discovered_cases/ewu_shortlist.py --selftest
"""
from __future__ import annotations

import argparse
import json
import random
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).parent
PROJECT = ROOT.parent
REGISTRY = ROOT / "CASE_BUNDLE_AGG.json"
JSON_OUT = ROOT / "ewu_shortlist.json"
MD_OUT = ROOT / "ewu_shortlist.md"
BASKET_ROOT = PROJECT / ".tmp"

# --- reuse muckrock_harvest's artifact classifier (the spec's requirement) -----
sys.path.insert(0, str(ROOT))
from muckrock_harvest import (  # noqa: E402
    RE_BWC, RE_INTERR, RE_SB16, ext_of, VIDEO_EXTS, AUDIO_EXTS, DOC_EXTS,
)

PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff", ".bmp", ".heic", ".webp"}

# 911/dispatch and doc/photo detectors — extend muckrock's set to the 5-bucket
# EWU taxonomy (muckrock only sniffs bwc/interrogation/sb16).
RE_911 = re.compile(
    r"\b(911|9-1-1|dispatch|radio\s*(transmission|traffic)|call\s*for\s*service"
    r"|cad\b|oemc|calls?\s+of\s+service|comm(?:unication)?s?\s*(audio|recording|log))",
    re.I)
RE_DOC = re.compile(
    r"\b(report|red\s*book|blue\s*team|findings|disposition|memo|letter|summary"
    r"|presentation|ia[\s_\-]?\d|ois[\s_\-]?\d|uof|cid\b|adm\d|icd\d)", re.I)
RE_PHOTO = re.compile(r"\b(photo|photograph|booking\s*photo|scene\s*photo|still\s*image)", re.I)
# An officer/deputy POV clip typed only ``video`` by a crude harvester is really
# body-worn footage — but a *cell-phone*/*surveillance*/*civilian* clip is not.
RE_OFFICER = re.compile(r"\b(officer|sergeant|sgt|ofc|deputy|corporal|cpl|detective|dep\b)", re.I)
RE_NOT_BWC = re.compile(r"\b(cell[\s\-]?phone|surveillance|civilian|bystander|doorbell|ring\b"
                        r"|store|business|residence|home\s*security|cctv)", re.I)

CORE_BUCKETS = ("bwc", "interrogation", "911", "doc", "photos")
# non-core buckets kept for transparency (they don't feed completeness)
EXTRA_BUCKETS = ("surveillance", "video", "audio", "other")
STREAM_RE = re.compile(r"youtube|youtu\.be|vimeo|player\.|jwplayer|/embed/|streaming", re.I)


# ---------------------------------------------------------------------------
# 1. CLASSIFIER
# ---------------------------------------------------------------------------
def classify_file(name: str, ftype: Optional[str], url: str = "") -> str:
    """Map one registry file -> exactly one evidence bucket.

    Priority: the registry's own ``type`` is authoritative for the sources that
    parse folder structure (SDPD/SFDPA); crude/ambiguous types (video/audio/
    media/other/None) fall through to muckrock's regexes + extension + officer
    heuristic. Every file lands in exactly one bucket so counts sum sanely.
    """
    t = (ftype or "").lower()
    name = name or ""
    e = ext_of(name) or ext_of(url)
    # SDPD/portal filenames use '_' and '+' (URL-encoded spaces) as token separators,
    # but '_' is a regex word char so `\bofficer` fails on "_Officer". Normalise both
    # to spaces so every \b anchor below behaves as intended.
    n = re.sub(r"[_+]+", " ", name)

    # 1) authoritative registry types
    if t in ("bodycam", "dashcam"):
        return "bwc"
    if t == "interrogation":
        return "interrogation"
    if t in ("911_audio", "911", "radio"):
        return "911"
    if t in ("documents", "document"):
        return "doc"
    if t == "photos":
        return "photos"
    if t == "surveillance":
        return "surveillance"

    # 2) ambiguous types -> reuse muckrock's regexes, then extension, then heuristics
    if RE_BWC.search(n):
        return "bwc"
    if RE_INTERR.search(n):
        return "interrogation"
    if RE_911.search(n):
        return "911"
    if RE_SB16.search(n) or RE_DOC.search(n) or e in DOC_EXTS:
        return "doc"
    if RE_PHOTO.search(n) or e in PHOTO_EXTS:
        return "photos"
    # crude "video" that is really officer body-worn (OIS "_Video_Officer_...")
    if (t in ("video", "media") or e in VIDEO_EXTS):
        if RE_OFFICER.search(n) and not RE_NOT_BWC.search(n):
            return "bwc"
        return "video"          # uncategorised incident video (cell-phone / civilian)
    if e in AUDIO_EXTS or t == "audio":
        return "audio"          # uncategorised audio (could be a call, an interview)
    return "other"


def classify_bundle(bundle: Dict[str, Any]) -> Dict[str, int]:
    """Return per-bucket counts for a bundle (all core buckets always present)."""
    ev: Dict[str, int] = {k: 0 for k in CORE_BUCKETS + EXTRA_BUCKETS}
    for f in bundle.get("files", []):
        b = classify_file(f.get("name", ""), f.get("type"), f.get("url", ""))
        ev[b] = ev.get(b, 0) + 1
    return ev


# ---------------------------------------------------------------------------
# 2. SCORER — completeness x worth (Tier-1 model, registry proxy)
# ---------------------------------------------------------------------------
def completeness_score(ev: Dict[str, int]) -> Tuple[float, bool]:
    """Evidence completeness (0-100). Full-package = BWC + doc + interrogation."""
    have = {k: ev.get(k, 0) > 0 for k in CORE_BUCKETS}
    presence = (25 * have["bwc"] + 20 * have["doc"] + 20 * have["interrogation"]
                + 15 * have["911"] + 5 * have["photos"])                     # max 85
    full = have["bwc"] and have["doc"] and have["interrogation"]
    bonus = 0
    if full:
        bonus += 15                                                         # the EWU full-package
        if have["911"]:
            bonus += 5                                                      # + inciting call = complete slate
    depth = min(10.0, 1.5 * min(ev.get("bwc", 0), 4) + 1.0 * min(ev.get("interrogation", 0), 4))
    return round(min(100.0, presence + bonus + depth), 1), full


def severity_of(blob: str) -> Tuple[int, str]:
    """Tier-1 SEVERITY gate from the cheap registry text (title + case_id + agency).

    Recall-leaning: an unknown case keeps a mid default rather than being zeroed
    (the rubric never SKIPs a high-severity case on a dry doc)."""
    t = blob.lower()
    if re.search(r"in.?custody death|died in custody|death investigation|custodial death", t):
        return 92, "in-custody-death"
    if re.search(r"homicide|murder|fatal|\bdeath\b|deceased|officer.involved.death", t):
        return 90, "death"
    if re.search(r"officer.involved.shooting|\bois\b|police.shooting|\bshooting\b|shots?.fired", t):
        return 80, "officer-involved-shooting"
    if re.search(r"great bodily injury|\bgbi\b|serious injury|stabb|in.?custody injury|\bk-?9\b|canine bite", t):
        return 70, "serious-injury"
    if re.search(r"use.of.force|\buof\b|excessive force|strike|takedown|taser|less.lethal|restraint", t):
        return 55, "use-of-force"
    if re.search(r"pursuit|arrest|\bdui\b|assault|robbery|burglar", t):
        return 45, "arrest/pursuit"
    if re.search(r"complaint|misconduct|discourtesy|policy|procedure|administrative|neglect of duty|brady", t):
        return 30, "administrative"
    return 45, "unspecified"


def worth_of(ev: Dict[str, int], sev: int) -> Tuple[int, int, int]:
    """Tier-1 worth proxy = 0.5*SEV + 0.3*SHAPE + 0.2*PROX, x small mult.

    SHAPE/PROX come from the evidence MIX (the cheap signal the rubric itself
    calls out: "911-presence is a free severity tell"; an interview => a suspect
    to unfold). Returns (worth, shape, prox)."""
    shape = 45
    if ev["interrogation"] > 0:
        shape += 20                          # a suspect/interview => a story to unfold
    if ev["doc"] > 0:
        shape += 10                          # an investigation/report arc
    if ev["bwc"] > 0 and ev["911"] > 0:
        shape += 10                          # call -> arrival -> incident chronology
    shape = min(100, shape)

    prox = 40
    if ev["911"] > 0:
        prox += 20                           # a caller / a victim in distress
    if ev["interrogation"] > 0:
        prox += 15                           # a central human character
    if ev["photos"] > 0:
        prox += 5
    prox = min(100, prox)

    base = 0.5 * sev + 0.3 * shape + 0.2 * prox
    mult = 1.0
    if (ev["bwc"] + ev.get("surveillance", 0)) >= 3:
        mult += 0.05                         # multi-camera convergence
    if ev["doc"] > 0 and ev["bwc"] > 0:
        mult += 0.05                         # footage-vs-report adjudicable
    return int(round(min(100.0, base * mult))), shape, prox


def is_downloadable(bundle: Dict[str, Any]) -> bool:
    """A/B tier (per CASE_BUNDLE_SPEC both require downloadable direct files) AND
    at least one non-streaming http(s) URL. (The registry's own ``downloadable``
    probe field is null pending WS1's validate_ab.)"""
    if bundle.get("tier") not in ("A", "B"):
        return False
    for f in bundle.get("files", []):
        u = f.get("url") or ""
        if u.startswith("http") and not STREAM_RE.search(u):
            return True
    return False


def eligible(bundle: Dict[str, Any], ev: Dict[str, int]) -> bool:
    """"Enough evidence to produce": downloadable A/B tier with a case DOC plus
    at least one footage/interview spine (bodycam OR interrogation)."""
    return (is_downloadable(bundle)
            and ev["doc"] > 0
            and (ev["bwc"] > 0 or ev["interrogation"] > 0))


# ---------------------------------------------------------------------------
# existing Tier-1 verdicts (attach if a case was already cheaply verified)
# ---------------------------------------------------------------------------
def existing_verdict(case_id: str) -> Optional[Dict[str, Any]]:
    vf = BASKET_ROOT / case_id / "d2" / "tier1_verdict.json"
    if not vf.exists():
        return None
    try:
        return json.loads(vf.read_text())
    except (OSError, ValueError):
        return None


def basket_status(case_id: str) -> Dict[str, Any]:
    """What cheap Tier-1 inputs are already on disk for this case."""
    basket = BASKET_ROOT / case_id
    docs = basket / "docs"
    n_docs = len([p for p in docs.iterdir() if p.is_file()]) if docs.is_dir() else 0
    n_911 = len(list(basket.rglob("*911*"))) if basket.is_dir() else 0
    v = existing_verdict(case_id)
    return {
        "basket": str(basket),
        "exists": basket.is_dir(),
        "n_docs_on_disk": n_docs,
        "n_911_on_disk": n_911,
        "verdict": (v or {}).get("verdict"),
        "verdict_worth": (v or {}).get("worth"),
    }


# ---------------------------------------------------------------------------
# rank + rows
# ---------------------------------------------------------------------------
def build_row(bundle: Dict[str, Any]) -> Dict[str, Any]:
    ev = classify_bundle(bundle)
    comp, full = completeness_score(ev)
    blob = f"{bundle.get('title','')} {bundle.get('case_id','')} {bundle.get('agency','')}"
    sev, sev_label = severity_of(blob)
    worth, shape, prox = worth_of(ev, sev)
    rank_score = round(comp * worth / 100.0, 1)
    v = existing_verdict(bundle.get("case_id", ""))
    return {
        "case_id": bundle.get("case_id", ""),
        "source": bundle.get("source", ""),
        "agency": bundle.get("agency", ""),
        "title": bundle.get("title", ""),
        "case_url": bundle.get("case_url", ""),
        "tier": bundle.get("tier"),
        "downloadable": is_downloadable(bundle),
        "evidence": {k: ev[k] for k in CORE_BUCKETS},
        "evidence_extra": {k: ev[k] for k in EXTRA_BUCKETS},
        "n_files": bundle.get("n_files", 0),
        "full_package": full,
        "completeness_score": comp,
        "severity": sev,
        "severity_label": sev_label,
        "story_shape": shape,
        "human_proximity": prox,
        "worth": worth,
        "rank_score": rank_score,
        # attach the real cheap verdict if this case was already Tier-1 verified
        "tier1_verdict": (v or {}).get("verdict"),
        "tier1_worth": (v or {}).get("worth"),
    }


def rank(bundles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for b in bundles:
        if eligible(b, classify_bundle(b)):
            rows.append(build_row(b))
    rows.sort(key=lambda r: (
        r["rank_score"],
        r["full_package"],
        r["evidence"]["911"],
        r["evidence"]["interrogation"],
        r["evidence"]["bwc"],
        1 if r["tier"] == "A" else 0,
        r["n_files"],
    ), reverse=True)
    return rows


# ---------------------------------------------------------------------------
# emit
# ---------------------------------------------------------------------------
def load_registry(path: Path) -> List[Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("bundles", data if isinstance(data, list) else [])


def write_json(rows: List[Dict[str, Any]], top: List[Dict[str, Any]], n_scanned: int) -> None:
    JSON_OUT.write_text(json.dumps({
        "what": "EWU-ready shortlist: registry cases with enough evidence to produce a "
                "full-package documentary, ranked by completeness x Tier-1 worth.",
        "regenerate": "python discovered_cases/ewu_shortlist.py",
        "selector": "Tier-1 (severity x story-shape x human-proximity); NOT pipeline4 moment-density.",
        "n_scanned": n_scanned,
        "n_eligible": len(rows),
        "top_pick": top[0]["case_id"] if top else None,
        "shortlist": rows,
    }, indent=2, ensure_ascii=False), encoding="utf-8")


def _ev_str(ev: Dict[str, int]) -> str:
    return f"BWC{ev['bwc']} INT{ev['interrogation']} 911×{ev['911']} DOC{ev['doc']} PH{ev['photos']}"


def write_md(rows: List[Dict[str, Any]]) -> None:
    L: List[str] = []
    L.append("# EWU-ready shortlist — cases with enough evidence to produce")
    L.append("")
    L.append("> **Generated** by `discovered_cases/ewu_shortlist.py` — do not hand-edit.")
    L.append("> Ranked by **completeness x Tier-1 worth** (severity x story-shape x human-proximity).")
    L.append("> Every row is a downloadable A/B-tier bundle with a case DOC + a footage/interview spine.")
    L.append("> `verdict` = the cheap Tier-1 doc+911 judgment where a case has already been verified.")
    L.append("")
    L.append(f"**{len(rows)} eligible cases.** Evidence: BWC=bodycam · INT=interrogation/interview "
             "· 911=911/dispatch · DOC=case document · PH=photos.")
    L.append("")
    L.append("| # | case_id | src | tier | evidence | full pkg | comp | sev | worth | rank | verdict |")
    L.append("|--:|---------|-----|:----:|----------|:-------:|-----:|----:|------:|-----:|:-------:|")
    for i, r in enumerate(rows, 1):
        ev = r["evidence"]
        L.append(
            f"| {i} | {r['case_id']} | {r['source']} | {r['tier']} | {_ev_str(ev)} "
            f"| {'✅' if r['full_package'] else ''} | {r['completeness_score']:g} | {r['severity']} "
            f"| {r['worth']} | {r['rank_score']:g} | {r.get('tier1_verdict') or ''} |")
    L.append("")
    L.append("## Top 5 — detail")
    L.append("")
    for i, r in enumerate(rows[:5], 1):
        L.append(f"### {i}. {r['case_id']}  ·  {r['tier']}-tier  ·  worth {r['worth']} "
                 f"·  rank {r['rank_score']:g}")
        L.append(f"- **{r['title']}** — {r['agency']}")
        L.append(f"- evidence: {_ev_str(r['evidence'])}  ·  extra {r['evidence_extra']}")
        L.append(f"- severity {r['severity']} ({r['severity_label']}) · shape {r['story_shape']} "
                 f"· proximity {r['human_proximity']} · completeness {r['completeness_score']:g}"
                 + (f" · **full package**" if r['full_package'] else ""))
        st = basket_status(r["case_id"])
        L.append(f"- basket: exists={st['exists']} docs={st['n_docs_on_disk']} 911={st['n_911_on_disk']} "
                 f"verdict={st['verdict']}")
        if r["case_url"]:
            L.append(f"- case: {r['case_url']}")
        L.append("")
    MD_OUT.write_text("\n".join(L), encoding="utf-8")


# ---------------------------------------------------------------------------
# pull cheap Tier-1 inputs (doc + 911) for the top-N via bundle_to_basket.py
# ---------------------------------------------------------------------------
def pull_top(rows: List[Dict[str, Any]], n: int, max_mb: float = 150.0) -> None:
    for r in rows[:n]:
        cid = r["case_id"]
        basket = BASKET_ROOT / cid
        print(f"\n[pull] {cid} -> {basket}  (doc + 911 only)")
        cmd = [sys.executable, str(ROOT / "bundle_to_basket.py"),
               "--case-id", cid, "--basket", str(basket),
               "--doc-911-only", "--max-mb", str(max_mb)]
        try:
            subprocess.run(cmd, check=False, cwd=str(PROJECT))
        except Exception as e:  # noqa: BLE001
            print(f"  [pull-fail] {cid}: {e}", file=sys.stderr)
        st = basket_status(cid)
        print(f"  [pull] {cid}: docs={st['n_docs_on_disk']} 911={st['n_911_on_disk']} "
              f"verdict={st['verdict']}")


# ---------------------------------------------------------------------------
# audit / selftest
# ---------------------------------------------------------------------------
def audit(bundles: List[Dict[str, Any]], n: int, seed: int) -> None:
    """Print n random per-file classifications for manual quality review."""
    rng = random.Random(seed)
    pool = [b for b in bundles if b.get("files")]
    rng.shuffle(pool)
    shown = 0
    for b in pool:
        ev = classify_bundle(b)
        print(f"\n=== {b['source']} / {b['case_id']}  tier={b.get('tier')}  "
              f"[{_ev_str(ev)} | extra {({k: ev[k] for k in EXTRA_BUCKETS})}]")
        for f in b["files"][:12]:
            bucket = classify_file(f.get("name", ""), f.get("type"), f.get("url", ""))
            print(f"   type={str(f.get('type')):13s} -> {bucket:13s}  {(f.get('name') or '')[:56]}")
        shown += 1
        if shown >= n:
            break


def _selftest() -> int:
    # classifier
    assert classify_file("Officer 1 BWC_Redacted", "bodycam") == "bwc"
    assert classify_file("December 7 ... _Video_Officer Anderson_Redacted", "video") == "bwc"
    assert classify_file("Cell Phone Video 10", "video") == "video"          # civilian, not bwc
    assert classify_file("Surveillance Vid 3", "surveillance") == "surveillance"
    assert classify_file("Interview 12_Redacted", "interrogation") == "interrogation"
    assert classify_file("suspect interview of John", "audio") == "interrogation"
    assert classify_file("911 Call 4_Redacted", "911_audio") == "911"
    assert classify_file("Radio Transmission Of Incident", "radio") == "911"
    assert classify_file("dispatch audio", "audio") == "911"
    assert classify_file("20-10062 OIS Book_Redacted", "documents") == "doc"
    assert classify_file("Use of Force Report.pdf", "other") == "doc"
    assert classify_file("Photo 1_Redacted", "photos") == "photos"
    assert classify_file("scene.jpg", "other") == "photos"

    # completeness: full package (bwc+doc+int) beats a doc-only bundle
    full = {"bwc": 3, "interrogation": 4, "911": 2, "doc": 1, "photos": 0,
            "surveillance": 0, "video": 0, "audio": 0, "other": 0}
    thin = {"bwc": 0, "interrogation": 0, "911": 0, "doc": 1, "photos": 5,
            "surveillance": 0, "video": 0, "audio": 0, "other": 0}
    cf, is_full = completeness_score(full)
    ct, _ = completeness_score(thin)
    assert is_full and cf > ct, (cf, ct)

    # severity gate
    assert severity_of("(Officer Involved Shootings)")[0] == 80
    assert severity_of("In Custody Death investigation")[0] == 92
    assert severity_of("(Use of Force)")[0] == 55
    assert severity_of("longbeach_officer_involved_shootings_06_07_17")[0] == 80  # category in case_id

    # eligibility + downloadable
    okb = {"tier": "A", "files": [{"url": "https://sdpdsb1421.sandiego.gov/x.pdf"}]}
    assert is_downloadable(okb)
    streamb = {"tier": "A", "files": [{"url": "https://player.vimeo.com/video/1"}]}
    assert not is_downloadable(streamb)
    assert not is_downloadable({"tier": "C", "files": [{"url": "https://h/x.pdf"}]})
    assert eligible({"tier": "A", "files": [{"url": "https://h/x.pdf"}]},
                    {"bwc": 1, "doc": 1, "interrogation": 0, "911": 0, "photos": 0})
    assert not eligible({"tier": "A", "files": [{"url": "https://h/x.pdf"}]},
                        {"bwc": 0, "doc": 1, "interrogation": 0, "911": 0, "photos": 0})  # no spine
    print("ewu_shortlist selftest: OK  (classifier, completeness, severity, eligibility)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--registry", type=Path, default=REGISTRY)
    ap.add_argument("--limit", type=int, default=0, help="cap emitted shortlist rows (0 = all eligible)")
    ap.add_argument("--pull-top", type=int, default=0, metavar="N",
                    help="after ranking, fetch doc+911 into baskets for the top N (bundle_to_basket)")
    ap.add_argument("--audit", type=int, default=0, metavar="N", help="print N random classifications and exit")
    ap.add_argument("--seed", type=int, default=1, help="rng seed for --audit")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        return _selftest()

    bundles = load_registry(args.registry)
    if args.audit:
        audit(bundles, args.audit, args.seed)
        return 0

    rows = rank(bundles)
    if args.limit:
        rows = rows[:args.limit]

    write_json(rows, rows, len(bundles))
    write_md(rows)

    print(f"scanned {len(bundles)} bundles -> {len(rows)} EWU-ready (downloadable A/B + doc + spine)")
    print(f"  -> {JSON_OUT}")
    print(f"  -> {MD_OUT}")
    print("\nTop 12:")
    for i, r in enumerate(rows[:12], 1):
        print(f"{i:2}. rank {r['rank_score']:5g}  comp {r['completeness_score']:5g}  worth {r['worth']:3d}  "
              f"{'FULL' if r['full_package'] else '    '}  [{_ev_str(r['evidence'])}]  "
              f"{r['tier']} {r['case_id']}"
              + (f"  <{r['tier1_verdict']}>" if r.get("tier1_verdict") else ""))

    if args.pull_top:
        pull_top(rows, args.pull_top)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
