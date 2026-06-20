"""P6 v0 — render a real rough cut from a P4 verdict + the real case media.

This is the minimal vertical slice of the documentary sequencer (Goal C):

    P4 verdict (key_moments + transcript_refs)  +  real case media files
        -> map source_idx -> media file
        -> build a light paper_edit.json (clip / insert / narration events)
        -> render styled PIL text cards (title / context / outcome)
        -> ffmpeg: cut real clips, lay audio over cards, concat
        -> {case_id}_rough_cut.mp4  +  {case_id}_paper_edit.json

Deliberately light: ~6-10 ordered events, deterministic, no LLM. Narration
bridges are deterministic sentences from the verdict's own moment
descriptions (locked decision: light deterministic narration). Template mode
is auto-picked from the available evidence types (bodycam-backbone vs
interrogation-led).

Nothing here is invented: every clip's timecode, quote, and description comes
straight from the P4 verdict; credit lines name the real custodian.

Run (global Python has static_ffmpeg + Pillow):

    python pipeline6_sequence/render_rough_cut.py \
        --verdict pipeline4_scoring/verdicts/sfdpa_0409-18_verdict.json \
        --media-dir pipeline3_audio/case_0409-18 \
        --agency "San Francisco Dept. of Police Accountability" \
        --out .tmp/p6_rough_cuts
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# ffmpeg discovery (static_ffmpeg ships a binary; fall back to PATH)
# ---------------------------------------------------------------------------

def _resolve_ffmpeg() -> Tuple[str, str]:
    try:
        import static_ffmpeg  # type: ignore
        static_ffmpeg.add_paths()
    except Exception:
        pass
    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    if not ffmpeg or not ffprobe:
        raise RuntimeError(
            "ffmpeg/ffprobe not found. Install static_ffmpeg "
            "(`pip install static_ffmpeg`) or put ffmpeg on PATH."
        )
    return ffmpeg, ffprobe


FFMPEG, FFPROBE = "", ""

# ---------------------------------------------------------------------------
# Render constants
# ---------------------------------------------------------------------------

W, H = 1280, 720
FPS = 30
ASR = 44100  # audio sample rate
BG = (13, 13, 18)
ACCENT = (220, 80, 60)        # FlameOn red
FG = (238, 238, 240)
MUTED = (150, 150, 160)
CLIP_HEAD_PAD = 5.0           # seconds before the moment (mirrors P5 _add_clip_boundaries)
CLIP_TAIL_PAD = 3.0
COLD_OPEN_SEC = 12.0          # bodycam hook length
TITLE_SEC = 5.0
OUTCOME_SEC = 7.0
PHASE_CARD_SEC = 3.0          # act/phase header card

_FONT_CANDIDATES = {
    "bold": ["C:/Windows/Fonts/arialbd.ttf", "C:/Windows/Fonts/segoeuib.ttf",
             "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"],
    "reg": ["C:/Windows/Fonts/arial.ttf", "C:/Windows/Fonts/segoeui.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"],
    "ital": ["C:/Windows/Fonts/ariali.ttf", "C:/Windows/Fonts/segoeuii.ttf",
             "/usr/share/fonts/truetype/dejavu/DejaVuSans-Oblique.ttf"],
}


def _font(kind: str, size: int):
    from PIL import ImageFont
    for path in _FONT_CANDIDATES[kind]:
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


# ---------------------------------------------------------------------------
# Source mapping: source_idx -> real media file (via verdict.transcript_refs)
# ---------------------------------------------------------------------------

@dataclass
class Source:
    source_idx: int
    media_path: Optional[Path]
    evidence_type: str            # bodycam | interrogation | court_video | ...
    label: str                    # human label, e.g. "DPA Interview — Sgt. Bradford"
    person: Optional[str] = None
    start_epoch: Optional[float] = None   # absolute start time (D1 case_timeline)
    phase: Optional[str] = None           # pre_incident | incident | aftermath | ...


# Order matters: bodycam/dashcam checked before interrogation so a stray
# token can't win. Matched on EXACT alphanumeric TOKENS (split on any
# non-alphanumeric incl. underscore) so the "sfdpa" case-id prefix is its own
# token and never collides with the "dpa" interrogation signal.
_ROLE_TOKENS = (
    ("bodycam", {"bwc", "bodycam", "bodyworn"}),
    ("dash_cam", {"dash", "dashcam"}),
    ("court_video", {"court", "trial", "hearing"}),
    ("dispatch_911", {"911", "dispatch", "cad"}),
    ("interrogation", {"interview", "interrogation", "interrog", "iad", "dpa"}),
)
_KNOWN_PERSON_HINTS = ("bradford", "sherry", "edwards", "hernandez", "pai",
                       "reininger", "dejesus", "rabsatt")


def _tokens(name: str) -> set:
    return {t for t in re.split(r"[^a-z0-9]+", name.lower()) if t}


def _classify_ref(name: str) -> Tuple[str, Optional[str]]:
    toks = _tokens(name)
    etype = "other"
    for et, role_toks in _ROLE_TOKENS:
        if toks & role_toks:
            etype = et
            break
    person = next((p for p in _KNOWN_PERSON_HINTS if p in toks), None)
    return etype, person


def _media_files(media_dir: Path) -> List[Path]:
    exts = {".mp4", ".mov", ".mkv", ".m4v", ".webm", ".mp3", ".m4a", ".wav", ".aac"}
    return [p for p in sorted(media_dir.iterdir())
            if p.is_file() and p.suffix.lower() in exts]


_ROLE_TOK_MAP = dict(_ROLE_TOKENS)


def _match_media(etype: str, person: Optional[str], files: List[Path]) -> Optional[Path]:
    """Find the media file matching an evidence type (+ person when known)."""
    def score(p: Path) -> int:
        toks = _tokens(p.name)
        s = 0
        if toks & _ROLE_TOK_MAP.get(etype, set()):
            s += 2
        if person and person in toks:
            s += 3
        return s
    ranked = sorted(files, key=score, reverse=True)
    return ranked[0] if ranked and score(ranked[0]) > 0 else None


def _media_from_transcript(ref: str) -> Tuple[Optional[Path], Optional[str]]:
    """Resolve a transcript_ref -> (media_path, evidence_type) by reading the
    transcript JSON's own ``source_url`` / ``source_evidence_type``. Robust to
    any filename scheme (the preferred path); returns (None, etype?) when the
    transcript or its media isn't on disk so the caller can fall back."""
    try:
        p = Path(ref)
        if not p.exists():
            return None, None
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None, None
    etype = data.get("source_evidence_type")
    src = (data.get("source_url") or "").strip()
    mp = Path(src) if src else None
    return (mp if (mp and mp.exists()) else None), etype


def _cam_tag(name: str) -> Optional[str]:
    m = re.search(r"\b(bwc|icc|bodycam|dash|dashcam)[\s_\-]?(\d+[a-z]?)\b", name, re.I)
    return f"{m.group(1).upper()}-{m.group(2)}" if m else None


def _load_timeline_index(timeline_path: Path) -> Dict[str, Dict[str, Any]]:
    """{artifact_id: {phase, start_epoch}} from a D1 case_timeline.json (phase)
    joined with its sibling artifacts.json (absolute start_epoch)."""
    tl = json.loads(Path(timeline_path).read_text(encoding="utf-8"))
    phase_of: Dict[str, str] = {}
    for ph, recs in (tl.get("phases") or {}).items():
        for r in recs:
            phase_of[r["artifact_id"]] = ph
    epoch_of: Dict[str, float] = {}
    arts_path = Path(timeline_path).parent / "artifacts.json"
    if arts_path.exists():
        for a in json.loads(arts_path.read_text(encoding="utf-8")):
            if a.get("start_epoch"):
                epoch_of[a["artifact_id"]] = a["start_epoch"]
    return {aid: {"phase": phase_of.get(aid), "start_epoch": epoch_of.get(aid)}
            for aid in set(phase_of) | set(epoch_of)}


def map_sources(verdict: Dict[str, Any], media_dir: Optional[Path],
                timeline_index: Optional[Dict[str, Dict[str, Any]]] = None) -> List[Source]:
    refs = verdict.get("transcript_refs") or []
    files = _media_files(media_dir) if media_dir and Path(media_dir).exists() else []
    sources: List[Source] = []
    for idx, ref in enumerate(refs):
        base = Path(str(ref)).name
        # Preferred: media + type straight from the transcript's source_url.
        media, etype_t = _media_from_transcript(str(ref))
        etype_n, person = _classify_ref(base)
        etype = etype_t or etype_n
        if media is None and files:               # fallback: filename match
            media = _match_media(etype, person, files)
        label_role = {
            "interrogation": "DPA Interview", "bodycam": "Body-Worn Camera",
            "court_video": "Court Video", "dispatch_911": "911 Dispatch",
            "dash_cam": "Dash Camera", "911_audio": "911 Dispatch",
        }.get(etype, "Source")
        cam = _cam_tag(base)
        if person:
            label = f"{label_role} — {person.title()}"
        elif cam:
            label = f"{label_role} ({cam})"
        else:
            label = label_role
        ti = (timeline_index or {}).get(Path(str(media)).stem, {}) if media else {}
        sources.append(Source(
            source_idx=idx, media_path=media, evidence_type=etype,
            label=label, person=person,
            start_epoch=ti.get("start_epoch"), phase=ti.get("phase"),
        ))
    return sources


def _is_video(p: Optional[Path]) -> bool:
    return bool(p) and p.suffix.lower() in {".mp4", ".mov", ".mkv", ".m4v", ".webm"}


def _duration(p: Path) -> float:
    out = subprocess.run(
        [FFPROBE, "-v", "error", "-show_entries", "format=duration",
         "-of", "default=nw=1:nk=1", str(p)],
        capture_output=True, text=True)
    try:
        return float(out.stdout.strip())
    except ValueError:
        return 0.0


# ---------------------------------------------------------------------------
# Template selection + paper-edit assembly
# ---------------------------------------------------------------------------

def pick_template(sources: List[Source], verdict: Dict[str, Any]) -> str:
    """Auto-pick (locked decision): bodycam-backbone if moments live on
    bodycam/dashcam, else interrogation-led."""
    moment_idx = {m.get("source_idx", 0) for m in verdict.get("key_moments", [])}
    moment_types = {sources[i].evidence_type for i in moment_idx
                    if 0 <= i < len(sources)}
    if moment_types & {"bodycam", "dash_cam", "court_video"}:
        return "bodycam_backbone"
    return "interrogation_led"


def build_paper_edit(verdict: Dict[str, Any], sources: List[Source],
                     agency: str, support_docs: List[Path],
                     doc_extract: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    case_id = verdict["case_id"]
    arc = verdict.get("narrative_arc_recommendation") or "chronological"
    template = pick_template(sources, verdict)
    credit = f"Courtesy {agency}"

    # Order moments. With wall-clock from the D1 timeline (D4 act-assembly), play
    # them in TRUE chronological order across POVs and tag each with its phase;
    # otherwise fall back to source_idx + timestamp.
    def _abs(m: Dict[str, Any]) -> Optional[float]:
        i = m.get("source_idx", 0)
        s = sources[i] if 0 <= i < len(sources) else None
        if s and s.start_epoch is not None:
            return s.start_epoch + float(m.get("timestamp_sec") or 0)
        return None
    timeline_mode = any(s.start_epoch is not None for s in sources)
    _raw = verdict.get("key_moments", []) or []
    if timeline_mode:
        moments = sorted(_raw, key=lambda m: (_abs(m) is None, _abs(m) or 0.0))
    else:
        moments = sorted(_raw, key=lambda m: (m.get("source_idx", 0), m.get("timestamp_sec") or 0))
    _PHASE_TITLE = {"pre_incident": "The Call", "incident": "The Incident",
                    "aftermath": "Aftermath", "transport": "Transport",
                    "investigation": "The Investigation", "outcome": "Outcome"}
    current_phase = None

    # Beat assignment: first → hook, last → climax-ish; simple + deterministic.
    timeline: List[Dict[str, Any]] = []

    # 1. Title card.
    timeline.append({"kind": "card", "card_kind": "title",
                     "title": f"Case {case_id.split('_')[-1].upper()}",
                     "subtitle": agency, "dur": TITLE_SEC})

    # 2. Cold-open bodycam B-roll — every real bodycam/dashcam video, so the
    #    cut opens on actual footage (up to 2 to keep the hook tight).
    video_srcs = [s for s in sources if _is_video(s.media_path)]
    # de-dupe by media path (multiple source_idx can map to the same file)
    seen_media: set = set()
    cold = []
    for s in video_srcs:
        key = str(s.media_path)
        if key in seen_media:
            continue
        seen_media.add(key)
        cold.append(s)
    for n, s in enumerate(cold[:2]):
        timeline.append({
            "kind": "clip", "clip_id": f"coldopen{n}", "source_idx": s.source_idx,
            "media": str(s.media_path), "in_sec": 0.0,
            "out_sec": min(COLD_OPEN_SEC, _duration(s.media_path)),
            "beat_role": "hook", "label": s.label, "credit_line": credit,
            "moment_type": "scene_set",
            "lower_third": f"{s.label}  ·  {credit}",
        })

    # 3. One clip per key moment (real timecodes, real quotes).
    for n, m in enumerate(moments):
        idx = m.get("source_idx", 0)
        src = sources[idx] if 0 <= idx < len(sources) else None
        if not src or not src.media_path:
            timeline.append({"kind": "gap", "reason": f"source_idx {idx} has no media",
                             "moment": m.get("description", "")})
            continue
        if timeline_mode and src.phase and src.phase != current_phase:
            current_phase = src.phase
            timeline.append({"kind": "card", "card_kind": "phase",
                             "title": _PHASE_TITLE.get(src.phase, src.phase.replace('_', ' ').title()),
                             "subtitle": "", "dur": PHASE_CARD_SEC})
        ts = float(m.get("timestamp_sec") or 0)
        end = float(m.get("end_timestamp_sec") or ts)
        media_dur = _duration(src.media_path)
        in_sec = max(0.0, ts - CLIP_HEAD_PAD)
        out_sec = min(media_dur, end + CLIP_TAIL_PAD)
        beat = "hook" if n == 0 else ("climax" if n == len(moments) - 1 else "escalation")
        timeline.append({
            "kind": "clip", "clip_id": f"m{n}", "source_idx": idx,
            "media": str(src.media_path), "in_sec": round(in_sec, 2),
            "out_sec": round(out_sec, 2), "beat_role": beat,
            "moment_type": m.get("moment_type"), "importance": m.get("importance"),
            "label": src.label, "credit_line": credit,
            "transcript_excerpt": m.get("transcript_excerpt", ""),
            "description": m.get("description", ""),
            "phase": src.phase,
            "lower_third": f"{src.label}  ·  {credit}",
        })
        # Light deterministic narration bridge before the next clip.
        if n < len(moments) - 1:
            timeline.append({"kind": "narration",
                             "text": _bridge(m, moments[n + 1]),
                             "after_clip_id": f"m{n}",
                             "derived_from": "verdict.key_moments"})

    # 4. Outcome card. Prefer the REAL disposition from the case doc (D5);
    #    else the P4 content_pitch. Nothing invented.
    oc = (doc_extract or {}).get("outcome_card") or {}
    if oc.get("title"):
        out_title, out_sub = oc["title"], oc.get("subtitle", "")
    else:
        out_title = f"Verdict: {verdict.get('verdict', '?')}"
        out_sub = verdict.get("content_pitch") or ""
    timeline.append({"kind": "card", "card_kind": "outcome",
                     "title": out_title, "subtitle": out_sub,
                     "dur": OUTCOME_SEC, "footer": credit})

    # Supporting-doc inserts + clipper directions surfaced from the case doc (D5).
    inserts = [{"role": "back_claim", "doc_type": "court_production",
                "source_path": str(p), "note": "available for on-screen citation"}
               for p in support_docs]
    clip_directions = (doc_extract or {}).get("clip_directions", [])

    return {
        "case_id": case_id,
        "verdict": verdict.get("verdict"),
        "narrative_arc": arc,
        "template_mode": template,
        "agency": agency,
        "sources": [{"source_idx": s.source_idx,
                     "media_path": str(s.media_path) if s.media_path else None,
                     "evidence_type": s.evidence_type, "label": s.label,
                     "credit_line": credit} for s in sources],
        "timeline": timeline,
        "available_inserts": inserts,
        "doc_clip_directions": clip_directions,
        "_inputs": {"verdict_key_moments": len(moments),
                    "sources_mapped": sum(1 for s in sources if s.media_path)},
    }


def _bridge(cur: Dict[str, Any], nxt: Dict[str, Any]) -> str:
    """Deterministic one-line connective narration from moment descriptions."""
    return f"Then: {nxt.get('description', '').rstrip('.')}."


# ---------------------------------------------------------------------------
# PIL card rendering
# ---------------------------------------------------------------------------

def _wrap(draw, text: str, font, max_w: int) -> List[str]:
    words = (text or "").split()
    lines, cur = [], ""
    for w in words:
        trial = (cur + " " + w).strip()
        if draw.textlength(trial, font=font) <= max_w:
            cur = trial
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines or [""]


def render_card(out_png: Path, *, card_kind: str, title: str = "",
                subtitle: str = "", quote: str = "", description: str = "",
                lower_third: str = "", footer: str = "") -> None:
    from PIL import Image, ImageDraw
    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)
    margin = 90

    # accent bar
    d.rectangle([0, 0, W, 6], fill=ACCENT)
    d.rectangle([0, H - 6, W, H], fill=ACCENT)
    d.text((margin, H - 52), "FlameOn · rough cut", font=_font("reg", 22), fill=MUTED)
    if footer:
        f = _font("reg", 22)
        d.text((W - margin - d.textlength(footer, font=f), H - 52), footer,
               font=f, fill=MUTED)

    if card_kind in ("title", "outcome", "phase"):
        tfont = _font("bold", 64 if card_kind == "title" else 52)
        tlines = _wrap(d, title, tfont, W - 2 * margin)
        sfont = _font("reg", 30)
        slines = _wrap(d, subtitle, sfont, W - 2 * margin)
        block_h = len(tlines) * 74 + (24 if slines else 0) + len(slines) * 40
        y = (H - block_h) // 2
        for ln in tlines:
            d.text(((W - d.textlength(ln, font=tfont)) // 2, y), ln, font=tfont, fill=FG)
            y += 74
        y += 24
        for ln in slines:
            d.text(((W - d.textlength(ln, font=sfont)) // 2, y), ln, font=sfont, fill=MUTED)
            y += 40
        return img.save(out_png)

    # context card (audio moment): lower-third label, big quote, description footer
    if lower_third:
        lf = _font("bold", 26)
        d.rectangle([0, 70, 14 + d.textlength(lower_third, font=lf) + 40, 120], fill=(24, 24, 32))
        d.rectangle([0, 70, 8, 120], fill=ACCENT)
        d.text((28, 80), lower_third, font=lf, fill=FG)

    if quote:
        qfont = _font("bold", 44)
        qlines = _wrap(d, f"“{quote}”", qfont, W - 2 * margin)
        block_h = len(qlines) * 58
        y = (H - block_h) // 2 - 20
        for ln in qlines:
            d.text((margin, y), ln, font=qfont, fill=FG)
            y += 58

    if description:
        dfont = _font("ital", 28)
        dlines = _wrap(d, description, dfont, W - 2 * margin)
        y = H - 200
        for ln in dlines[:3]:
            d.text((margin, y), ln, font=dfont, fill=MUTED)
            y += 38
    img.save(out_png)


# ---------------------------------------------------------------------------
# ffmpeg segment builders (all normalized to W x H / FPS / AAC stereo)
# ---------------------------------------------------------------------------

_VF = (f"scale={W}:{H}:force_original_aspect_ratio=decrease,"
       f"pad={W}:{H}:(ow-iw)/2:(oh-ih)/2:color=0d0d12,fps={FPS},format=yuv420p")
_ENC = ["-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-ar", str(ASR), "-ac", "2", "-r", str(FPS)]


def _run(cmd: List[str]) -> None:
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError("ffmpeg failed:\n" + " ".join(cmd) + "\n" + r.stderr[-1500:])


def seg_video(media: Path, in_sec: float, dur: float, out_mp4: Path,
              lower_third_png: Optional[Path]) -> None:
    inputs = ["-ss", f"{in_sec}", "-t", f"{dur}", "-i", str(media)]
    if lower_third_png:
        inputs += ["-i", str(lower_third_png)]
        filt = (f"[0:v]{_VF}[bg];[bg][1:v]overlay=0:0[v]")
        maps = ["-map", "[v]", "-map", "0:a?"]
        _run([FFMPEG, "-y", *inputs, "-filter_complex", filt, *maps,
              "-shortest", *_ENC, str(out_mp4)])
    else:
        _run([FFMPEG, "-y", *inputs, "-vf", _VF, *_ENC, "-shortest", str(out_mp4)])


def seg_audio_over_card(card_png: Path, media: Path, in_sec: float, dur: float,
                        out_mp4: Path) -> None:
    _run([FFMPEG, "-y", "-loop", "1", "-i", str(card_png),
          "-ss", f"{in_sec}", "-t", f"{dur}", "-i", str(media),
          "-vf", f"fps={FPS},format=yuv420p", *_ENC, "-shortest", str(out_mp4)])


def seg_card(card_png: Path, dur: float, out_mp4: Path) -> None:
    _run([FFMPEG, "-y", "-loop", "1", "-i", str(card_png),
          "-f", "lavfi", "-i", f"anullsrc=r={ASR}:cl=stereo", "-t", f"{dur}",
          "-vf", f"fps={FPS},format=yuv420p", *_ENC, "-shortest", str(out_mp4)])


def make_lower_third_png(out_png: Path, text: str) -> None:
    """Transparent overlay strip for video clips (drawn bottom-left)."""
    from PIL import Image, ImageDraw
    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    f = _font("bold", 26)
    tw = d.textlength(text, font=f)
    d.rectangle([0, H - 150, 28 + tw + 40, H - 96], fill=(13, 13, 18, 205))
    d.rectangle([0, H - 150, 8, H - 96], fill=(*ACCENT, 255))
    d.text((28, H - 142), text, font=f, fill=(238, 238, 240, 255))
    img.save(out_png)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def render(paper_edit: Dict[str, Any], out_dir: Path) -> Path:
    case_id = paper_edit["case_id"]
    work = out_dir / case_id
    work.mkdir(parents=True, exist_ok=True)
    seg_dir = work / "segments"
    seg_dir.mkdir(exist_ok=True)
    card_dir = work / "cards"
    card_dir.mkdir(exist_ok=True)

    segments: List[Path] = []
    for i, ev in enumerate(paper_edit["timeline"]):
        seg = seg_dir / f"{i:02d}.mp4"
        kind = ev["kind"]
        if kind == "card":
            png = card_dir / f"{i:02d}.png"
            render_card(png, card_kind=ev["card_kind"], title=ev.get("title", ""),
                        subtitle=ev.get("subtitle", ""), footer=ev.get("footer", ""))
            seg_card(png, float(ev.get("dur", 5.0)), seg)
            segments.append(seg)
        elif kind == "clip":
            media = Path(ev["media"])
            dur = max(0.5, float(ev["out_sec"]) - float(ev["in_sec"]))
            if _is_video(media):
                lt = card_dir / f"{i:02d}_lt.png"
                make_lower_third_png(lt, ev.get("lower_third", ev.get("label", "")))
                seg_video(media, float(ev["in_sec"]), dur, seg, lt)
            else:
                png = card_dir / f"{i:02d}.png"
                render_card(png, card_kind="context",
                            quote=ev.get("transcript_excerpt", ""),
                            description=ev.get("description", ""),
                            lower_third=ev.get("lower_third", ""),
                            footer=ev.get("credit_line", ""))
                seg_audio_over_card(png, media, float(ev["in_sec"]), dur, seg)
            segments.append(seg)
        elif kind == "narration":
            png = card_dir / f"{i:02d}.png"
            render_card(png, card_kind="context", quote="", description=ev["text"],
                        lower_third="Narration")
            seg_card(png, 3.0, seg)
            segments.append(seg)
        # gap events render nothing (flagged in the JSON only)
        print(f"  [seg {i:02d}] {kind:9s} -> {seg.name}")

    # concat (all segments share codec params -> stream copy)
    listfile = work / "concat.txt"
    listfile.write_text("".join(f"file '{s.resolve().as_posix()}'\n" for s in segments),
                        encoding="utf-8")
    final = work / f"{case_id}_rough_cut.mp4"
    try:
        _run([FFMPEG, "-y", "-f", "concat", "-safe", "0", "-i", str(listfile),
              "-c", "copy", str(final)])
    except RuntimeError:
        # fallback: re-encode on concat if stream-copy rejects minor param drift
        _run([FFMPEG, "-y", "-f", "concat", "-safe", "0", "-i", str(listfile),
              *_ENC, str(final)])
    return final


def main(argv: Optional[List[str]] = None) -> int:
    global FFMPEG, FFPROBE
    ap = argparse.ArgumentParser(description="P6 v0: render a real rough cut from a P4 verdict + media")
    ap.add_argument("--verdict", required=True, type=Path)
    ap.add_argument("--media-dir", required=True, type=Path)
    ap.add_argument("--agency", default="Releasing agency")
    ap.add_argument("--out", type=Path, default=Path(".tmp/p6_rough_cuts"))
    ap.add_argument("--timeline", type=Path, default=None,
                    help="D1 case_timeline.json -> chronological act ordering (D4)")
    ap.add_argument("--doc-extract", type=Path, default=None,
                    help="D5 doc_extract.json -> real disposition outcome card + clip directions")
    args = ap.parse_args(argv)

    FFMPEG, FFPROBE = _resolve_ffmpeg()
    verdict = json.loads(args.verdict.read_text(encoding="utf-8"))
    tl_index = _load_timeline_index(args.timeline) if args.timeline else None
    doc_extract = json.loads(args.doc_extract.read_text(encoding="utf-8")) if args.doc_extract else None
    sources = map_sources(verdict, args.media_dir, timeline_index=tl_index)
    support = [p for p in args.media_dir.iterdir()
               if p.suffix.lower() == ".pdf"] if args.media_dir.exists() else []

    print(f"[map] {len(sources)} source(s):")
    for s in sources:
        print(f"   idx {s.source_idx}: {s.evidence_type:13s} -> "
              f"{s.media_path.name if s.media_path else 'UNMATCHED'}")

    paper_edit = build_paper_edit(verdict, sources, args.agency, support, doc_extract=doc_extract)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    pe_path = out_dir / verdict["case_id"] / f"{verdict['case_id']}_paper_edit.json"
    pe_path.parent.mkdir(parents=True, exist_ok=True)
    pe_path.write_text(json.dumps(paper_edit, indent=2), encoding="utf-8")
    print(f"[paper-edit] {pe_path}  (template={paper_edit['template_mode']}, "
          f"{len(paper_edit['timeline'])} events)")

    final = render(paper_edit, out_dir)
    dur = _duration(final)
    print(f"[done] {final}  ({dur/60:.1f} min)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
