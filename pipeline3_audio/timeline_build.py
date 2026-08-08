"""P3.5 / GOAL_D D1 — build a phase-bucketed Case Timeline from stamped artifacts.

Consumes ``artifacts.json`` (from timeline_stamp / D0) and produces a
``case_timeline.json``: every artifact placed into a documentary phase
(pre_incident → incident → aftermath → transport → investigation → outcome),
plus an ``unsorted`` list for anything unplaceable.

Placement rules:
  - Anchor the incident to the earliest TRUSTED stamp cluster (AXON OCR /
    filename). Container ``metadata`` is distrusted unless it lands on the
    incident's own day (D0 found it's often the export date, not recording).
  - Precisely-timed artifacts are placed by their offset from the anchor.
  - Un-timed (or distrusted-metadata) artifacts are placed by KIND inference
    (911/radio → pre_incident, dashcam → incident, interrogation →
    investigation, court/doc → outcome), flagged ``inferred`` so D3 / a CAD
    log can refine them later. Nothing is silently guessed a *time*.

Pure stdlib. Run with any Python.
    python pipeline3_audio/timeline_build.py --artifacts .tmp/sac_poc/timeline/artifacts.json
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Dict, List, Optional

TRUSTED_SOURCES = {"axon_ocr", "filename"}
INCIDENT_WINDOW_SEC = 15 * 60      # active scene around the anchor
AFTERMATH_CUTOFF_SEC = 3 * 3600    # same-day, beyond this -> transport/late

PHASE_ORDER = ["pre_incident", "incident", "aftermath", "transport",
               "investigation", "outcome", "unsorted"]
PHASE_ROLE = {
    "pre_incident": "cold_open / scene_set", "incident": "climax",
    "aftermath": "falling_action", "transport": "beat / transition",
    "investigation": "investigation_act", "outcome": "resolution",
    "unsorted": "needs CAD log / D3 audio-xcorr",
}
# Fallback bucket by artifact kind when no usable timestamp.
KIND_PHASE = {
    "911": "pre_incident", "radio": "pre_incident", "dashcam": "incident",
    "interrogation": "investigation", "court": "outcome", "doc": "outcome",
}


def _day(epoch: float) -> dt.date:
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).date()


def build_timeline(arts: List[Dict], anchor_override: Optional[float] = None) -> Dict:
    trusted = sorted(
        [a for a in arts if a.get("start_epoch") and a.get("timestamp_source") in TRUSTED_SOURCES],
        key=lambda a: a["start_epoch"],
    )
    # The auto-anchor is the EARLIEST trusted stamp — correct only when the story's
    # climax is also the earliest event. When the incident is a later beat (e.g. an
    # overdose 3 h after the seizure that opens the day), that mis-buckets the climax
    # as "transport/aftermath" and the whole arc plays backwards. ``anchor_override``
    # lets the operator pin the incident to its real moment (see --incident).
    anchor = anchor_override if anchor_override is not None else (
        trusted[0]["start_epoch"] if trusted else None)
    anchor_day = _day(anchor) if anchor else None

    phases: Dict[str, List[Dict]] = {p: [] for p in PHASE_ORDER}
    for a in arts:
        se = a.get("start_epoch")
        src = a.get("timestamp_source")
        # The incident ANCHOR comes only from TRUSTED_SOURCES (OCR/filename), but
        # D3's sanity-checked audio_xcorr stamps ARE trusted for placement.
        usable = bool(se) and anchor is not None and (
            src in TRUSTED_SOURCES or src == "audio_xcorr"
            or (src == "metadata" and _day(se) == anchor_day)
        )
        rec = {
            "artifact_id": a["artifact_id"], "kind": a["kind"],
            "pov_label": a.get("pov_label"), "start_iso": a.get("start_iso"),
            "duration_sec": a.get("duration_sec"),
        }
        if usable:
            rec["placement"] = "precise"
            rel = se - anchor
            if _day(se) != anchor_day:
                placed = "investigation" if se > anchor else "pre_incident"
            elif rel < -60:
                placed = "pre_incident"
            elif rel <= INCIDENT_WINDOW_SEC:
                placed = "incident"
            elif rel <= AFTERMATH_CUTOFF_SEC:
                placed = "aftermath"
            else:
                placed = "transport"
        else:
            rec["placement"] = "inferred"
            if a.get("start_epoch") and a.get("timestamp_source") == "metadata":
                rec["note"] = "metadata stamp distrusted (off-day / export date)"
            placed = KIND_PHASE.get(a["kind"], "unsorted")
        if a["kind"] == "court":              # kind override regardless of time
            placed = "outcome"
        phases[placed].append(rec)

    return {
        "anchor_epoch": anchor,
        "anchor_iso": (dt.datetime.fromtimestamp(anchor, dt.timezone.utc)
                       .strftime("%Y-%m-%d %H:%M:%S") if anchor else None),
        "incident_window_sec": INCIDENT_WINDOW_SEC,
        "anchor_source": "override" if anchor_override is not None else "auto_earliest_trusted",
        "phases": {p: phases[p] for p in PHASE_ORDER if phases[p]},
        "roles": PHASE_ROLE,
    }


def print_timeline(tl: Dict) -> None:
    print(f"\nincident anchor: {tl['anchor_iso']}  (UTC)\n")
    for phase in PHASE_ORDER:
        arts = tl["phases"].get(phase)
        if not arts:
            continue
        print(f"== {phase.upper()}  [{PHASE_ROLE[phase]}] ==")
        for a in sorted(arts, key=lambda x: (x.get("start_iso") or "zzzz")):
            t = a.get("start_iso") or "  --:--:--  "
            mark = "" if a["placement"] == "precise" else "  (inferred)"
            note = f"  <{a['note']}>" if a.get("note") else ""
            print(f"   {t}  {a['kind']:13s} {a['artifact_id']}{mark}{note}")
        print()


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="GOAL_D D1 — build phase-bucketed case timeline")
    ap.add_argument("--artifacts", required=True, type=Path)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--case-id", default="case")
    ap.add_argument("--incident", default=None,
                    help='pin the incident anchor to a real moment, UTC '
                         '"YYYY-MM-DD HH:MM[:SS]" (overrides the earliest-stamp auto-anchor). '
                         'Use when the climax is NOT the earliest event (e.g. a later overdose).')
    ap.add_argument("--anchor-epoch", type=float, default=None,
                    help="pin the incident anchor to a raw UTC epoch (alternative to --incident)")
    args = ap.parse_args(argv)

    anchor_override = args.anchor_epoch
    if anchor_override is None and args.incident:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
            try:
                anchor_override = (dt.datetime.strptime(args.incident, fmt)
                                   .replace(tzinfo=dt.timezone.utc).timestamp())
                break
            except ValueError:
                continue
        if anchor_override is None:
            ap.error(f"--incident: could not parse {args.incident!r} (want UTC 'YYYY-MM-DD HH:MM[:SS]')")

    arts = json.loads(args.artifacts.read_text(encoding="utf-8"))
    tl = build_timeline(arts, anchor_override=anchor_override)
    tl["case_id"] = args.case_id
    print_timeline(tl)
    out = args.out or (args.artifacts.parent / "case_timeline.json")
    out.write_text(json.dumps(tl, indent=2), encoding="utf-8")
    counts = {p: len(v) for p, v in tl["phases"].items()}
    print(f"[timeline] {counts} -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
