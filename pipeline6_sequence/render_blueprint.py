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
  beat.narration_bridge (realized text) -> an on-clip band (narration_top) on playable
                        beats; a standalone narration card only for document/unplayable beats
  beat (clip)        -> a video clip (lower-third) or audio-over-card (quote)
  beat.is_broll      -> the same, with no quote (footage/establishing only)
  incident facts     -> the closing outcome card

Deterministic + zero-network for the paper-edit assembly (``--paper-edit-only``).
That mode now uses the same audio-aware window resolver as a final render, but
stops before encoding; the resulting timeline is the actual pre-render artifact.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_THREAT = re.compile(r"\b(knife|weapon|gun|armed|harass\w*|stab\w*|machete|threat\w*)\b", re.I)
_DISPATCH = re.compile(r"\b(units?|officers?|deput\w*|sending|en route|on (?:the|our) way|respond\w*)\b", re.I)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import render_rough_cut as rc  # noqa: E402  (reuse render + seg builders + constants)

# Kinds that can be played as a clip/over-card. Documents/photos are not (yet)
# primary footage — they ride as inserts or stills in a later pass.
PLAYABLE_KINDS = {"bodycam", "dashcam", "911_audio", "radio", "other"}

_AUDIO_SCAN_SR = 4000        # decode rate for the audible-window search
_AUDIO_SCAN_CAP_SEC = 600    # only scan the first 10 min (huge dashcams stay cheap)


def loudest_window_start(levels: List[float], want: int) -> int:
    """Index of the loudest ``want``-second window in a per-second level list.

    Pure sliding-window max-energy. This is how a B-roll clip avoids landing in
    an AXON muted pre-event buffer or a silent dashcam idle: the loudest stretch
    is, by construction, not silence.
    """
    n = len(levels)
    if n == 0:
        return 0
    want = max(1, min(int(want), n))
    cur = sum(levels[:want])
    best_sum, best_i = cur, 0
    for i in range(1, n - want + 1):
        cur += levels[i + want - 1] - levels[i - 1]
        if cur > best_sum:
            best_sum, best_i = cur, i
    return best_i


def audible_window(media: str, want_sec: float) -> Tuple[float, float]:
    """Find an audible ``want_sec`` window near the start of ``media`` (skips
    muted buffers / idle). Decodes only the first ``_AUDIO_SCAN_CAP_SEC`` so a
    3-hour dashcam stays cheap. Falls back to ``(0, want_sec)`` on any failure."""
    want = max(1.0, float(want_sec))
    try:
        import numpy as np
        raw = subprocess.run(
            [rc.FFMPEG, "-v", "error", "-t", str(_AUDIO_SCAN_CAP_SEC), "-i", str(media),
             "-ac", "1", "-ar", str(_AUDIO_SCAN_SR), "-f", "s16le", "-"],
            capture_output=True).stdout
        a = np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0
        nwin = len(a) // _AUDIO_SCAN_SR
        if nwin <= int(want):
            return 0.0, want
        w = a[: nwin * _AUDIO_SCAN_SR].reshape(nwin, _AUDIO_SCAN_SR)
        rms = np.sqrt((w ** 2).mean(axis=1) + 1e-12)
        db = 20.0 * np.log10(rms + 1e-12)
        start = loudest_window_start([float(x) for x in db], int(want))
        return float(start), float(start) + want
    except Exception:
        return 0.0, want


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


def load_transcripts(transcripts_dir: Optional[Path]) -> Dict[str, List[Dict]]:
    """{media_stem: [segments]} from a p3_to_p4 transcripts dir (keyed by each
    transcript's own source_url stem, so any filename scheme resolves)."""
    out: Dict[str, List[Dict]] = {}
    if not transcripts_dir or not Path(transcripts_dir).exists():
        return out
    for p in Path(transcripts_dir).glob("*.json"):
        try:
            t = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        src = (t.get("source_url") or "").strip()
        segs = [s for s in (t.get("transcript") or []) if s.get("text")]
        if src and segs:
            out[Path(src).stem] = sorted(segs, key=lambda s: s.get("start_sec", 0))
    return out


def resolve_clip_overlaps(timeline: List[Dict], min_dur: float = 1.5) -> List[Dict]:
    """Render-side safety net: the footage never replays itself. A GLOBAL pass (not
    just consecutive clips) — tracks the furthest point already shown on each
    camera and pushes any later clip that dips back into already-seen footage up to
    that point, so each camera only ever moves forward. This catches the
    non-adjacent overlaps that ``--min-clip-sec`` widening can create (two beats on
    the same cam, widened until their windows collide with other clips between
    them) — exactly the replays the judge's craft check flags."""
    shown_until: Dict[str, float] = {}
    for c in (e for e in timeline if e["kind"] == "clip"):
        if c.get("cold_open"):        # a teaser intentionally replays the climax — don't trim it or let it advance the cursor
            continue
        m = c["media"]
        prev = shown_until.get(m)
        if prev is not None and c["in_sec"] < prev - 0.5:
            c["in_sec"] = round(prev, 2)                       # skip past already-shown footage
            if c["out_sec"] - c["in_sec"] < min_dur:           # collapsed → keep a forward sliver
                c["out_sec"] = round(c["in_sec"] + min_dur, 2)
        shown_until[m] = max(prev or 0.0, c["out_sec"])
    return timeline


def snap_to_segments(in_sec: float, out_sec: float, segments: List[Dict]) -> Tuple[float, float]:
    """Snap a window to transcript sentence boundaries — no mid-word cuts."""
    if not segments:
        return in_sec, out_sec
    befores = [s["start_sec"] for s in segments if s["start_sec"] <= in_sec + 0.5]
    afters = [s["end_sec"] for s in segments if s["end_sec"] >= out_sec - 0.5]
    ni = max(befores) if befores else segments[0]["start_sec"]
    no = min(afters) if afters else segments[-1]["end_sec"]
    return (round(ni, 2), round(no, 2)) if no > ni else (in_sec, out_sec)


def threat_window(segments: List[Dict], max_sec: float = 26.0) -> Optional[Tuple[float, float]]:
    """A 911 cold-open window that DISCLOSES the threat and ends on a clean line
    (a dispatch confirmation, or a sentence boundary) — never mid-word."""
    ti = next((i for i, s in enumerate(segments) if _THREAT.search(s.get("text", ""))), None)
    if ti is None:
        return None
    start = segments[max(0, ti - 1)]["start_sec"]      # a beat of lead-in
    end = segments[ti]["end_sec"]
    for s in segments[ti:]:
        if s["start_sec"] - start > max_sec:
            break
        end = s["end_sec"]
        if _DISPATCH.search(s.get("text", "")):         # "units en route" — clean stop
            break
    return round(start, 2), round(end, 2)


def blueprint_to_paper_edit(bp: Dict[str, Any],
                            media_dir: Optional[Path] = None,
                            audio_aware: bool = False,
                            transcripts: Optional[Dict[str, List[Dict]]] = None,
                            min_clip_sec: float = 0.0) -> Dict[str, Any]:
    """Convert a (shaped) blueprint into a render_rough_cut paper_edit.

    ``audio_aware`` (needs ffmpeg) snaps B-roll windows to an audible stretch so
    establishing clips don't open on an AXON muted buffer or silent idle. Quote
    beats are left alone — their timecode already sits on the spoken line.
    """
    assets = {a["asset_id"]: a for a in bp.get("asset_manifest", [])}
    acts = {a["act_id"]: a for a in bp.get("acts", [])}
    agency = bp.get("agency", "") or ""
    credit = (f"Courtesy {agency}").strip().rstrip(" ·")
    cid = bp.get("case_id", "case")
    inc = bp.get("incident") or {}

    timeline: List[Dict[str, Any]] = []

    # PROMISE/PAYOFF anchors (Zenobia critique 2026-08-07): the treatment carries promise+payoff
    # at the ACT level. Tag the act's SETUP beat as the promise and its REVEAL beat as the payoff,
    # so taste_gate's promise_payoff_latency rule can catch "another line of exposition" wedged
    # between a setup and its payoff. Uses beat FUNCTION (real data) — no invented fields.
    _PROMISE_FN = {"tension_shift", "setup", "question", "establish", "audience_question"}
    _PAYOFF_FN = {"reveal", "payoff", "comeuppance", "answer", "detail_noticed"}
    _by_act: Dict[Any, List[Dict[str, Any]]] = {}
    for _b in bp.get("beats", []):
        _by_act.setdefault(_b.get("act_id"), []).append(_b)
    promise_beat: Dict[str, str] = {}   # beat_id -> promise-id (the act_id)
    payoff_beat: Dict[str, str] = {}
    for _act in bp.get("acts", []):
        if not (_act.get("promise") and _act.get("payoff")):
            continue
        _abeats = _by_act.get(_act["act_id"]) or []
        if not _abeats:
            continue
        _pid = _act["act_id"]
        _setup = next((b for b in _abeats if b.get("function") in _PROMISE_FN), _abeats[0])
        _after = _abeats[_abeats.index(_setup) + 1:]
        _payoff = next((b for b in _after if b.get("function") in _PAYOFF_FN),
                       _after[-1] if _after else None)
        if _payoff and _payoff is not _setup:
            promise_beat[_setup.get("beat_id")] = _pid
            payoff_beat[_payoff.get("beat_id")] = _pid

    # BEAT-LEVEL promise/payoff (same critique, finer grain): a standalone SETUP narration naming
    # a forthcoming specific moment ("the wife kept asking one question") must pay off on the very
    # next FOOTAGE beat — exposition wedged between is the defect. Deterministic and high-precision:
    # ordinary VO must never register as a promise (over-tagging only bites when real exposition
    # actually intervenes). B-roll is never the payoff — it isn't a moment, and the act emitter
    # re-sorts it ahead of the setup card anyway.
    _SETUP_RE = re.compile(
        r"\bkept asking\b|\basked\b[^.]{0,24}\bone question\b|\bone question\b|"
        r"\b(his|her|their)\s+(only|final|last|first|dying)\s+words\b|"
        r"\bwhat\s+(he|she|they)\s+(said|asked|told|did)\s+next\b|"
        r"\bwould\s+(later\s+)?(say|ask|tell|admit|reveal)\b|"
        r"\bhad\s+(just\s+)?one\s+(question|thing)\b", re.I)

    def _has_clip(b: Dict[str, Any]) -> bool:
        return bool(b.get("primary_asset") and not b.get("is_document"))

    def _setup_text(b: Dict[str, Any]) -> str:
        return (((b.get("narration_bridge") or {}).get("text", "") + " "
                 + (b.get("lower_third") or {}).get("text", "")).strip())

    beat_promise: Dict[str, str] = {}   # beat_id -> "pp_<setup beat_id>"
    beat_payoff: Dict[str, str] = {}
    for _abeats in _by_act.values():
        for _i, _b in enumerate(_abeats):
            if _has_clip(_b) or not _SETUP_RE.search(_setup_text(_b)):
                continue                  # only standalone setup narrations promise
            _pay = next((c for c in _abeats[_i + 1:]
                         if _has_clip(c) and not c.get("is_broll")), None)
            if _pay is not None:          # payoff = the next footage beat in this act
                _ppid = f"pp_{_b.get('beat_id')}"
                beat_promise[_b.get("beat_id")] = _ppid
                beat_payoff[_pay.get("beat_id")] = _ppid

    def _promise_payoff_tag(b: Dict[str, Any]) -> Optional[Dict[str, str]]:
        bid = b.get("beat_id")
        if bid in beat_promise:              # fine-grain beats act-level
            return {"id": beat_promise[bid], "role": "promise"}
        if bid in beat_payoff:
            return {"payoff_for": beat_payoff[bid]}
        if bid in promise_beat:
            return {"id": promise_beat[bid], "role": "promise"}
        if bid in payoff_beat:
            return {"payoff_for": payoff_beat[bid]}
        return None

    # 1. Title card — the logline if the LLM tier wrote one, else the case id.
    timeline.append({"kind": "card", "card_kind": "title",
                     "title": bp.get("logline") or f"Case {cid.split('_')[-1].upper()}",
                     "subtitle": agency, "dur": rc.TITLE_SEC})

    # 2. Group beats by act and play acts in CANONICAL phase order. Cold-open /
    #    untagged establishing B-roll (act_id None) opens the cut; then the acts
    #    in the blueprint's order (The Call -> The Incident -> Aftermath -> ...).
    #    This keeps 911 calls (which often lack a precise timestamp and would
    #    otherwise sort anywhere) in "The Call" up front, and stops act header
    #    cards from flip-flopping when the LLM interleaves acts.
    def _emit_beat(b: Dict[str, Any]) -> None:
        nb = b.get("narration_bridge") or {}
        nb_text = nb.get("text", "")
        # Record beats are sourced to the IA document, not footage: the narration
        # card IS the beat (the on-screen record). No footage, no gap.
        if b.get("is_document"):
            if nb_text:
                ev = {"kind": "narration", "text": nb_text,
                      "beat_id": b.get("beat_id"), "act_id": b.get("act_id"),
                      "function": b.get("function"), "source_refs": b.get("source_refs", [])}
                pptag = _promise_payoff_tag(b)
                if pptag:
                    ev["promise_payoff"] = pptag
                timeline.append(ev)
            return
        pa = b.get("primary_asset")
        asset = assets.get((pa or {}).get("asset_id"), {})
        media = _resolve_media(asset, media_dir) if pa else None
        if not media:
            if nb_text:
                timeline.append({"kind": "narration", "text": nb_text})
            timeline.append({"kind": "gap",
                             "reason": f"no playable media for beat {b.get('beat_id')}"
                                       f" ({(pa or {}).get('asset_id', 'none')})",
                             "moment": b.get("description", "")})
            return
        in_sec, out_sec = float(pa.get("in_sec", 0)), float(pa.get("out_sec", 0))
        # Snap ONLY against DENSE transcripts (911/radio are fully transcribed).
        # Bodycams are hot-window transcribed — sparse — so snapping a precise
        # vision/quote window to the nearest sparse segment balloons it (it
        # collapsed the whole K9 run to one segment). Bodycam windows are already
        # built on real timecodes; leave them.
        kind = asset.get("kind")
        segs = (transcripts or {}).get(Path(media).stem) if kind in ("911_audio", "radio") else None
        if segs and b.get("is_broll") and kind == "911_audio":
            # 911 cold open: disclose the threat, end on a clean line — not a
            # loudest-energy slice that cuts mid-word ("harassing peo—").
            tw = threat_window(segs)
            in_sec, out_sec = tw if tw else (in_sec, out_sec)
        elif segs and b.get("quote"):
            in_sec, out_sec = snap_to_segments(in_sec, out_sec, segs)
        elif audio_aware and b.get("is_broll"):
            in_sec, out_sec = audible_window(media, max(1.0, out_sec - in_sec))
        # Long-form breathing room: give each key MOMENT (not B-roll) at least
        # min_clip_sec of the real continuous tape, centered on the window and
        # clamped to the asset. resolve_clip_overlaps() below trims any same-asset
        # overlap this creates, so it never replays footage — it just lets a moment
        # play out instead of cutting at ~9s.
        if min_clip_sec and not b.get("is_broll") and (out_sec - in_sec) < min_clip_sec:
            adur = float(asset.get("duration_sec") or 0)
            center = (in_sec + out_sec) / 2.0
            new_in = max(0.0, center - min_clip_sec / 2.0)
            new_out = new_in + min_clip_sec
            if adur:
                new_out = min(new_out, adur)
                new_in = max(0.0, new_out - min_clip_sec)
            in_sec, out_sec = new_in, new_out
        lt_text = (b.get("lower_third") or {}).get("text", "")
        # Clip-synced captions (EWU-style) from the source transcript, rebased to the clip.
        mseg = (transcripts or {}).get(Path(media).stem) or []
        _HALLUC = {"thank you", "thanks for watching", "thanks for watching!", "you", "thanks",
                   "please subscribe", "subscribe", "bye", "bye.", "music", "silence", ".", "thank you."}
        caps = [{"start": round(max(0.0, s.get("start_sec", 0) - in_sec), 2),
                 "end": round(min(out_sec, s.get("end_sec", 0)) - in_sec, 2),
                 "text": s.get("text", "").strip()}
                for s in mseg
                if s.get("end_sec", 0) > in_sec and s.get("start_sec", 0) < out_sec
                and s.get("text", "").strip()
                and s["text"].strip().lower().rstrip(".!?, ") not in _HALLUC]   # drop Whisper silence-hallucinations
        quote_text = "" if b.get("is_broll") else (b.get("quote") or {}).get("text", "")
        # One caption authority per zone: if timed captions exist, do not also
        # render a duplicate pull quote in the center of the frame.
        excerpt = "" if caps else quote_text
        identity = (b.get("speaker_identity") or lt_text or "").strip()
        generic_identity = identity.lower() in {"", "source", "interview", "witness", "officer"}
        timeline.append({
            "kind": "clip", "media": media,
            "in_sec": round(in_sec, 2), "out_sec": round(out_sec, 2),
            "beat_id": b.get("beat_id"), "act_id": b.get("act_id"),
            "function": b.get("function"), "asset_id": (pa or {}).get("asset_id"),
            "asset_kind": kind, "source_refs": b.get("source_refs", []),
            "lower_third": f"{lt_text}  ·  {credit}".strip().strip("·").strip(),
            "captions": caps,
            "caption_authorities": ([{"zone": "center", "kind": "timed_captions"}] if caps else [])
                                   + ([{"zone": "center", "kind": "pull_quote"}] if excerpt else []),
            "cold_open": b.get("cold_open", False),   # teaser preview — exempt from replay-trim
            # Narration rides the footage as a top-third slide (no narrator/TTS yet).
            "narration_top": nb_text,
            # B-roll plays as footage only — no quote card.
            "transcript_excerpt": excerpt,
            "description": b.get("description") or b.get("broll_note") or "",
            "credit_line": credit,
            "speaker_identity": identity or None,
            "speaker_identified": bool(identity and not generic_identity),
            "temporal_relation": b.get("temporal_relation"),
            "visual_progression": b.get("visual_progression"),
            "consequential_atmosphere": bool(quote_text or caps),
            "blur_segments": b.get("blur_segments") or [],
            "content_categories": b.get("content_categories") or [],
            # Redaction state for the redundant_blur rule (Zenobia): pass through whatever the
            # blueprint/redaction pass declared — subject_redacted/body_removed on the beat, and
            # subject_present per blur segment. Absent = the rule's stale_after_sec arm still
            # catches egregiously long blurs; present = it catches blur over an already-hidden body.
            "subject_redacted": bool(b.get("subject_redacted")),
            "body_removed": bool(b.get("body_removed")),
            "promise_payoff": _promise_payoff_tag(b),
        })

    beats_by_act: Dict[Any, List[Dict[str, Any]]] = {}
    for b in bp.get("beats", []):
        beats_by_act.setdefault(b.get("act_id"), []).append(b)
    act_order: List[Any] = [None] + [a["act_id"] for a in bp.get("acts", [])]
    act_order += [k for k in beats_by_act if k not in act_order]   # safety: stragglers
    seen_act = set()
    for aid in act_order:
        if aid in seen_act:
            continue
        seen_act.add(aid)
        bl = beats_by_act.get(aid, [])
        if not bl:
            continue
        act = acts.get(aid)
        if act and act.get("title", "").lower() != "cold open":
            timeline.append({"kind": "card", "card_kind": "phase",
                             "title": act.get("title", aid),
                             "subtitle": (act.get("thesis") or "")[:140],
                             "dur": rc.PHASE_CARD_SEC})
        # Establishing B-roll LEADS its act — it never interrupts a continuous
        # camera run (a "vehicle en route" cut dropped into the middle of the K9
        # chase read as a backtrack). sorted() is stable, so action order holds.
        for b in sorted(bl, key=lambda x: 0 if x.get("is_broll") else 1):
            _emit_beat(b)

    resolve_clip_overlaps(timeline)   # never replay the same footage
    n_clips = sum(1 for e in timeline if e["kind"] == "clip")
    n_gaps = sum(1 for e in timeline if e["kind"] == "gap")

    # 3. Outcome card from the sourced incident facts (the document's disposition).
    sub_bits = [x for x in [", ".join(inc.get("charges", [])) or None,
                            inc.get("disposition")] if x]
    timeline.append({"kind": "card", "card_kind": "outcome",
                     "title": "Outcome",
                     "subtitle": "  ·  ".join(sub_bits) if sub_bits else "",
                     "dur": rc.OUTCOME_SEC, "footer": credit})

    treatment = bp.get("reference_treatment") or {}
    out = {
        "case_id": cid, "agency": agency,
        "built_from": bp.get("metadata", {}).get("built_by", "blueprint"),
        "format": treatment.get("format") or "longform",
        "platform": treatment.get("platform") or "youtube",
        "timeline": timeline,
        "_inputs": {"beats": len(bp.get("beats", [])), "clips": n_clips, "gaps": n_gaps},
    }
    out["runtime_sec"] = project_duration(out)
    return out


def project_duration(paper_edit: Dict[str, Any]) -> float:
    """Total runtime a paper_edit WILL render to — computed, no ffmpeg. Mirrors
    render_rough_cut.render() exactly: card=dur, clip=out-in (≥0.5), narration=3.0,
    gap=0. Lets us solve runtime offline instead of rendering trial cuts."""
    total = 0.0
    for ev in paper_edit.get("timeline", []):
        k = ev["kind"]
        if k == "card":
            total += float(ev.get("dur", 5.0))
        elif k == "clip":
            total += max(0.5, float(ev["out_sec"]) - float(ev["in_sec"]))
        elif k == "narration":
            total += 3.0
    return round(total, 1)


def solve_min_clip_sec(bp: Dict[str, Any], media_dir: Optional[Path],
                       transcripts: Optional[Dict[str, List[Dict]]],
                       target_sec: float, lo: float = 6.0, hi: float = 90.0,
                       iters: int = 20, audio_aware: bool = False) -> Tuple[float, float]:
    """Binary-search the per-moment window (``min_clip_sec``) that lands the cut on
    ``target_sec``. Runtime is monotonic in the window up to the footage caps, so a
    bisection converges; if even the max window can't reach the target (footage-
    bound), return that max — the engine never pads. Projects with the SAME
    ``audio_aware`` setting as the final render (audio-aware shifts B-roll windows,
    which cascades through the no-replay resolver — projecting without it would miss
    the target). Returns ``(min_clip_sec, projected_sec)``."""
    def projected(mcs: float) -> float:
        pe = blueprint_to_paper_edit(bp, media_dir=media_dir, audio_aware=audio_aware,
                                     transcripts=transcripts, min_clip_sec=mcs)
        return project_duration(pe)

    if projected(hi) < target_sec:
        return round(hi, 1), projected(hi)     # footage-capped; honest shortfall
    if projected(lo) >= target_sec:
        return round(lo, 1), projected(lo)     # already over at the floor
    for _ in range(iters):
        mid = (lo + hi) / 2.0
        if projected(mid) < target_sec:
            lo = mid
        else:
            hi = mid
    mcs = round((lo + hi) / 2.0, 1)
    return mcs, projected(mcs)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="P6 — render a blueprint into a long-form rough cut")
    ap.add_argument("--blueprint", required=True, type=Path, help="<id>_blueprint(_shaped).json")
    ap.add_argument("--media-dir", type=Path, default=None, help="override/remap media location")
    ap.add_argument("--out", type=Path, default=Path(".tmp/p6_long_cuts"))
    ap.add_argument("--paper-edit-only", action="store_true",
                    help="write the final-equivalent paper_edit JSON and stop before video encoding; "
                         "audio analysis may still inspect B-roll unless --no-audio-aware is set")
    ap.add_argument("--no-audio-aware", action="store_true",
                    help="don't snap B-roll to audible windows (faster; may open on silence)")
    ap.add_argument("--transcripts", type=Path, default=None,
                    help="p3_to_p4 transcripts dir -> snap clip windows to sentence "
                         "boundaries (no mid-word cuts; 911 cold-open discloses the threat)")
    ap.add_argument("--min-clip-sec", type=float, default=0.0,
                    help="give each key-moment clip at least this many seconds of real "
                         "footage (centered, clamped to the asset) — long-form breathing room")
    ap.add_argument("--target-runtime", type=float, default=None,
                    help="auto-solve --min-clip-sec to land the cut at this runtime (seconds) — "
                         "no manual tuning; the engine still won't pad past the available footage")
    args = ap.parse_args(argv)

    bp = json.loads(args.blueprint.read_text(encoding="utf-8"))
    # Paper edits are a gate artifact, so their windows MUST match the eventual
    # render. Audio-awareness therefore no longer turns off in paper-edit mode.
    audio_aware = not args.no_audio_aware
    if audio_aware:
        rc.FFMPEG, rc.FFPROBE = rc._resolve_ffmpeg()   # analysis only; no encode in paper-only mode
    transcripts = load_transcripts(args.transcripts)

    min_clip_sec = args.min_clip_sec
    if args.target_runtime:
        min_clip_sec, projected = solve_min_clip_sec(
            bp, args.media_dir, transcripts, args.target_runtime, audio_aware=audio_aware)
        verb = "fits" if projected >= args.target_runtime - 1 else "footage-capped at"
        print(f"[render-blueprint] target {args.target_runtime:.0f}s -> solved "
              f"--min-clip-sec={min_clip_sec:g} ({verb} {projected:.0f}s, no padding)")

    paper_edit = blueprint_to_paper_edit(bp, media_dir=args.media_dir, audio_aware=audio_aware,
                                         transcripts=transcripts, min_clip_sec=min_clip_sec)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    pe_path = out_dir / f"{paper_edit['case_id']}_paper_edit.json"
    pe_path.write_text(json.dumps(paper_edit, indent=2, ensure_ascii=False), encoding="utf-8")
    ic = paper_edit["_inputs"]
    print(f"[render-blueprint] {paper_edit['case_id']} ({paper_edit['built_from']}): "
          f"{len(paper_edit['timeline'])} events, {ic['clips']} clips, {ic['gaps']} gap(s) "
          f"-> {pe_path}")

    if args.paper_edit_only:
        print("[render-blueprint] final-equivalent paper edit complete (no video encoding).")
        return 0

    if not audio_aware:
        rc.FFMPEG, rc.FFPROBE = rc._resolve_ffmpeg()
    final = rc.render(paper_edit, out_dir)
    print(f"[render-blueprint] -> {final}  ({rc._duration(final) / 60:.1f} min)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
