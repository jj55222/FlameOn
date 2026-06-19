"""P3.5 / GOAL_D D2 — bucket-driven targeted transcription.

Transcribe only the artifacts in selected Case-Timeline phases, and only their
hot windows (padded for context). This is the payoff of the timeline: instead
of whispering all 8.4 h, we transcribe the ``incident`` bucket — the actual
confrontation — in a fraction of the time.

    python pipeline3_audio/timeline_transcribe.py \
        --artifacts .tmp/sac_poc/timeline/artifacts.json \
        --timeline  .tmp/sac_poc/timeline/case_timeline.json \
        --phases incident --kinds bodycam --case-id morales_23117201 \
        --out .tmp/sac_poc/d2

Reuses pov_triage (salience scan + hot-window transcription). Global Python.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pov_triage as pt  # noqa: E402

# artifact kind -> p3_to_p4 source_evidence_type
KIND_EVIDENCE = {
    "bodycam": "bodycam", "dashcam": "dash_cam", "911": "911_audio",
    "radio": "other", "interrogation": "interrogation", "court": "court_video",
}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="GOAL_D D2 — transcribe selected timeline phases (hot windows)")
    ap.add_argument("--artifacts", required=True, type=Path)
    ap.add_argument("--timeline", required=True, type=Path)
    ap.add_argument("--phases", required=True, help="comma list, e.g. incident,pre_incident")
    ap.add_argument("--kinds", default=None, help="optional comma filter, e.g. bodycam")
    ap.add_argument("--pad-pre", type=float, default=90.0)
    ap.add_argument("--pad-post", type=float, default=90.0)
    ap.add_argument("--case-id", default="case")
    ap.add_argument("--out", type=Path, default=Path(".tmp/d2"))
    ap.add_argument("--whisper-model", default="small")
    args = ap.parse_args(argv)

    pt.FFMPEG, pt.FFPROBE = pt._ff()
    arts = {a["artifact_id"]: a for a in json.loads(args.artifacts.read_text(encoding="utf-8"))}
    tl = json.loads(args.timeline.read_text(encoding="utf-8"))
    phases = [p.strip() for p in args.phases.split(",") if p.strip()]
    kinds = {k.strip() for k in args.kinds.split(",")} if args.kinds else None

    selected = []
    seen = set()
    for ph in phases:
        for rec in tl.get("phases", {}).get(ph, []):
            if kinds and rec["kind"] not in kinds:
                continue
            a = arts.get(rec["artifact_id"])
            if a and a.get("path") and a["artifact_id"] not in seen and Path(a["path"]).exists():
                seen.add(a["artifact_id"])
                selected.append(a)
    if not selected:
        print(f"[D2] no artifacts matched phases={phases} kinds={kinds}")
        return 1

    tdir = args.out / "transcripts"
    tdir.mkdir(parents=True, exist_ok=True)
    print(f"[D2] {len(selected)} artifact(s) from {phases}; pad -{args.pad_pre:.0f}/+{args.pad_post:.0f}s")
    from faster_whisper import WhisperModel
    model = WhisperModel(args.whisper_model, device="cpu", compute_type="int8")

    total_hot = total_full = 0.0
    for a in selected:
        sc = pt.scan_salience(a["path"])
        win = pt.pad_merge(sc.hot_windows, sc.duration_sec, args.pad_pre, args.pad_post)
        hot = sum(e - s for s, e in win)
        total_hot += hot; total_full += sc.duration_sec
        etype = KIND_EVIDENCE.get(a["kind"], sc.evidence_type)
        print(f"  {a['artifact_id']:12s} {hot/60:5.1f} min hot / {sc.duration_sec/60:5.1f} min ({len(win)} win)")
        tr = pt.transcribe_windows(a["path"], win, model, args.case_id, etype, sc.duration_sec)
        out = tdir / f"{args.case_id}_{a['artifact_id'].lower().replace('-', '')}_transcript.json"
        out.write_text(json.dumps(tr, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[D2] transcribed {total_hot/60:.1f} min of {total_full/60:.1f} min "
          f"({100*total_hot/max(total_full,1):.0f}%) -> {tdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
