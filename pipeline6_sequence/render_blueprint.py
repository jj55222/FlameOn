"""P6 / GOAL_D — render a production blueprint into a long-form rough cut.

``render_rough_cut`` renders from a P4 *verdict*; this adapter renders from a
*blueprint* (skeleton or LLM-shaped). It walks the blueprint's ordered beats and
emits the exact ``timeline`` event shapes ``render_rough_cut.render`` already
consumes — so the segment builders, PIL cards, lower-thirds, and concat are all
reused unchanged. The blueprint already carries everything a cut needs: ordered
beats with clip windows + pinned quotes, act titles/theses, realized narration,
B-roll, and the sourced outcome facts.

Mapping:
  logline            -> title card
  each act (in order)-> a phase header card (title + thesis)
  beat.narration     -> a narration card before the beat
  beat (clip)        -> a video clip (lower-third) or audio-over-card (quote)
  beat.is_broll      -> the same, with no quote (footage/establishing only)
  incident facts     -> the closing outcome card

Deterministic + zero-network for the paper-edit assembly (``--paper-edit-only``);
the final mp4 step needs media + ffmpeg (via render_rough_cut). Global Python.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import render_rough_cut as rc  # noqa: E402  (reuse render + seg builders + constants)

# Kinds that can be played as a clip/over-card. Documents/photos are not (yet)
# primary footage — they ride as inserts or stills in a later pass.
PLAYABLE_KINDS = {"bodycam", "dashcam", "911_audio", "radio", "other"}


def _resolve_media(asset: Dict[str, Any], media_dir: Optional[Path]) -> Optional[str]:
    """Best-effort path for a beat's primary asset. Uses the manifest path; if a
    ``media_dir`` override is given and the manifest path isn't on disk there,
    remaps by filename. Returns None for non-playable kinds (doc/photo)."""
    if asset.get("kind") not in PLAYABLE_KINDS:
        return None
    path = asset.get("path")
    if path:
        p = Path(path)
        if media_dir is not None and not p.exists():
            alt = Path(media_dir) / p.name
            if alt.exists():
                return str(alt)
        return str(p)
    if media_dir is not None and asset.get("pov_label"):
        for ext in (".mp4", ".mov", ".mkv", ".mp3", ".m4a", ".wav"):
            cand = Path(media_dir) / f"{asset['pov_label']}{ext}"
            if cand.exists():
                return str(cand)
    return None


def blueprint_to_paper_edit(bp: Dict[str, Any],
                            media_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Convert a (shaped) blueprint into a render_rough_cut paper_edit."""
    assets = {a["asset_id"]: a for a in bp.get("asset_manifest", [])}
    acts = {a["act_id"]: a for a in bp.get("acts", [])}
    agency = bp.get("agency", "") or ""
    credit = (f"Courtesy {agency}").strip().rstrip(" ·")
    cid = bp.get("case_id", "case")
    inc = bp.get("incident") or {}

    timeline: List[Dict[str, Any]] = []

    # 1. Title card — the logline if the LLM tier wrote one, else the case id.
    timeline.append({"kind": "card", "card_kind": "title",
                     "title": bp.get("logline") or f"Case {cid.split('_')[-1].upper()}",
                     "subtitle": agency, "dur": rc.TITLE_SEC})

    # 2. Walk beats in order; open each new act with a header card.
    current_act = None
    n_clips = n_gaps = 0
    for b in bp.get("beats", []):
        aid = b.get("act_id")
        if aid and aid != current_act:
            current_act = aid
            act = acts.get(aid)
            if act and act.get("title", "").lower() != "cold open":
                timeline.append({"kind": "card", "card_kind": "phase",
                                 "title": act.get("title", aid),
                                 "subtitle": (act.get("thesis") or "")[:140],
                                 "dur": rc.PHASE_CARD_SEC})

        nb = b.get("narration_bridge") or {}
        if nb.get("text"):
            timeline.append({"kind": "narration", "text": nb["text"]})

        pa = b.get("primary_asset")
        asset = assets.get((pa or {}).get("asset_id"), {})
        media = _resolve_media(asset, media_dir) if pa else None
        if not media:
            n_gaps += 1
            timeline.append({"kind": "gap",
                             "reason": f"no playable media for beat {b.get('beat_id')}"
                                       f" ({(pa or {}).get('asset_id', 'none')})",
                             "moment": b.get("description", "")})
            continue

        lt_text = (b.get("lower_third") or {}).get("text", "")
        n_clips += 1
        timeline.append({
            "kind": "clip", "media": media,
            "in_sec": round(float(pa.get("in_sec", 0)), 2),
            "out_sec": round(float(pa.get("out_sec", 0)), 2),
            "lower_third": f"{lt_text}  ·  {credit}".strip().strip("·").strip(),
            # B-roll plays as footage only — no quote card.
            "transcript_excerpt": "" if b.get("is_broll") else (b.get("quote") or {}).get("text", ""),
            "description": b.get("description") or b.get("broll_note") or "",
            "credit_line": credit,
        })

    # 3. Outcome card from the sourced incident facts (the document's disposition).
    sub_bits = [x for x in [", ".join(inc.get("charges", [])) or None,
                            inc.get("disposition")] if x]
    timeline.append({"kind": "card", "card_kind": "outcome",
                     "title": "Outcome",
                     "subtitle": "  ·  ".join(sub_bits) if sub_bits else "",
                     "dur": rc.OUTCOME_SEC, "footer": credit})

    return {
        "case_id": cid, "agency": agency,
        "built_from": bp.get("metadata", {}).get("built_by", "blueprint"),
        "timeline": timeline,
        "_inputs": {"beats": len(bp.get("beats", [])), "clips": n_clips, "gaps": n_gaps},
    }


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="P6 — render a blueprint into a long-form rough cut")
    ap.add_argument("--blueprint", required=True, type=Path, help="<id>_blueprint(_shaped).json")
    ap.add_argument("--media-dir", type=Path, default=None, help="override/remap media location")
    ap.add_argument("--out", type=Path, default=Path(".tmp/p6_long_cuts"))
    ap.add_argument("--paper-edit-only", action="store_true",
                    help="write the paper_edit JSON and stop (no ffmpeg, zero media)")
    args = ap.parse_args(argv)

    bp = json.loads(args.blueprint.read_text(encoding="utf-8"))
    paper_edit = blueprint_to_paper_edit(bp, media_dir=args.media_dir)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    pe_path = out_dir / f"{paper_edit['case_id']}_paper_edit.json"
    pe_path.write_text(json.dumps(paper_edit, indent=2, ensure_ascii=False), encoding="utf-8")
    ic = paper_edit["_inputs"]
    print(f"[render-blueprint] {paper_edit['case_id']} ({paper_edit['built_from']}): "
          f"{len(paper_edit['timeline'])} events, {ic['clips']} clips, {ic['gaps']} gap(s) "
          f"-> {pe_path}")

    if args.paper_edit_only:
        print("[render-blueprint] paper-edit only (no render).")
        return 0

    rc.FFMPEG, rc.FFPROBE = rc._resolve_ffmpeg()
    final = rc.render(paper_edit, out_dir)
    print(f"[render-blueprint] -> {final}  ({rc._duration(final) / 60:.1f} min)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
