#!/usr/bin/env python3
"""Convert a validated paper edit into case-generic Remotion props and clean media.

The adapter only reads upstream artifacts. Clip media is re-extracted from the
original evidence path in the paper edit; rendered rough-cut segments are never
accepted as inputs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

VIDEO_EXT = {".mp4", ".mov", ".mkv", ".webm", ".mb"}
AUDIO_EXT = {".wav", ".mp3", ".m4a", ".aac", ".flac", ".ogg"}
IMAGE_EXT = {".png", ".jpg", ".jpeg", ".webp"}
FORBIDDEN_PARTS = {"segments", "cards"}


def load(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def normalized(value: str) -> str:
    name = Path(value).name
    while Path(name).suffix.lower() in VIDEO_EXT | AUDIO_EXT | IMAGE_EXT | {".json"}:
        name = Path(name).stem
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def run(command: Sequence[str]) -> None:
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"command failed: {' '.join(command)}\n{result.stderr[-1200:]}")


def probe(path: Path, ffprobe: str) -> Dict[str, Any]:
    result = subprocess.run(
        [ffprobe, "-v", "error", "-show_streams", "-show_format", "-of", "json", str(path)],
        capture_output=True, text=True,
    )
    if result.returncode:
        return {}
    return json.loads(result.stdout or "{}")


def image_size(path: Path, ffprobe: str) -> tuple[int, int]:
    data = probe(path, ffprobe)
    stream = next((item for item in data.get("streams", []) if item.get("width")), {})
    return int(stream.get("width") or 1920), int(stream.get("height") or 1080)


def resolve_media(raw: str, repo_root: Path, case_root: Path) -> Path:
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        candidate = repo_root / candidate
    if candidate.exists():
        if any(part in FORBIDDEN_PARTS for part in candidate.parts):
            raise RuntimeError(f"refusing pre-rendered overlay media: {candidate}")
        return candidate.resolve()

    token = normalized(raw)
    roots = [case_root / "video/Video", case_root / "audio/Audio", case_root / "docs"]
    matches: List[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for item in root.rglob("*"):
            if item.is_file() and (normalized(item.name).startswith(token) or token.startswith(normalized(item.name))):
                matches.append(item)
    if not matches:
        raise FileNotFoundError(f"cannot resolve original media for {raw!r} under {case_root}")
    matches.sort(key=lambda item: (abs(len(normalized(item.name)) - len(token)), len(str(item))))
    return matches[0].resolve()


def transcript_index(directory: Path) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {}
    if not directory.exists():
        return result
    for path in directory.glob("*.json"):
        try:
            payload = load(path)
        except (OSError, ValueError):
            continue
        result[normalized(path.name)] = payload.get("transcript", [])
    return result


def find_transcript(index: Dict[str, List[Dict[str, Any]]], source: Path) -> List[Dict[str, Any]]:
    token = normalized(source.name)
    if token in index:
        return index[token]
    key = next((key for key in index if key.startswith(token) or token.startswith(key)), None)
    return index.get(key or "", [])


def fallback_captions(segments: Iterable[Dict[str, Any]], start: float, end: float) -> List[Dict[str, Any]]:
    captions = []
    for segment in segments:
        s = float(segment.get("start_sec", segment.get("start", 0)) or 0)
        e = float(segment.get("end_sec", segment.get("end", s)) or s)
        text = str(segment.get("text", "")).strip()
        if text and e > start and s < end:
            captions.append({"start": round(max(0.0, s - start), 3),
                             "end": round(min(end - start, e - start), 3), "text": text})
    return captions


def contract_beat(event: Dict[str, Any], beats: List[Dict[str, Any]]) -> Dict[str, Any]:
    if event.get("kind") != "clip":
        return {}
    token = normalized(str(event.get("media", "")))
    start = float(event.get("in_sec", 0) or 0)
    candidates = [beat for beat in beats if (
        normalized(str(beat.get("source_file", ""))).startswith(token)
        or token.startswith(normalized(str(beat.get("source_file", ""))))
    )]
    return min(candidates, key=lambda beat: abs(float(beat.get("in_sec", 0)) - start), default={})


def emphasize(text: str, phrases: Iterable[str]) -> str:
    result = text
    for phrase in phrases:
        phrase = str(phrase).strip()
        if phrase and phrase in result and f"**{phrase}**" not in result:
            result = result.replace(phrase, f"**{phrase}**")
    return result


# Operator spec: cards/lower-thirds must read for the AUDIENCE, never as internal file refs.
_FILEREF = re.compile(
    r"\b(?:BWC[\s_-]*\d+|Body[\s_-]*Worn[\s_-]*Camera\s*\d*|Officer\s*\d*\s*BWC|"
    r"Surveillance\s*\d+|Communications?\s*\d+|911\s*Call\s*\d+|Interview\s*\w*\d*|"
    r"Redacted|_mb\b|_SE\b|_KM\b)\b", re.I)


def humanize_label(text: str) -> str:
    """Turn 'BWC_3 · THE VOLLEY' into audience-facing 'Officer's body camera · The volley'."""
    if not text:
        return text
    parts = re.split(r"\s*·\s*|\s*-\s*", text, maxsplit=1)
    head = parts[0]
    if _FILEREF.search(head):
        low = head.lower()
        if "surveillance" in low:
            friendly = "Surveillance camera"
        elif "911" in low:
            friendly = "911 call"
        elif "interview" in low or "interrogat" in low:
            friendly = "Recorded interview"
        elif "communication" in low or "radio" in low:
            friendly = "Police radio"
        else:
            friendly = "Officer's body camera"
        rest = f" · {parts[1].strip()}" if len(parts) > 1 and parts[1].strip() else ""
        return f"{friendly}{rest}"
    return text


def silent_lead_sec(path: Path, ffmpeg: str) -> float:
    """Detect a muted lead-in (Axon pre-event buffer) at the clip start; 0 if audio is live."""
    try:
        out = subprocess.run(
            [ffmpeg, "-i", str(path), "-af", "silencedetect=noise=-45dB:d=1.5", "-f", "null", "-"],
            capture_output=True, text=True, timeout=30).stderr
    except Exception:
        return 0.0
    # only report a lead that starts at (or ~0) the clip beginning
    starts = re.findall(r"silence_start:\s*([\d.]+)", out)
    ends = re.findall(r"silence_end:\s*([\d.]+)", out)
    if starts and float(starts[0]) <= 0.3 and ends:
        lead = float(ends[0])
        return round(lead, 1) if lead >= 2.0 else 0.0
    return 0.0


def extract_media(source: Path, output: Path, start: float, end: float,
                  media_type: str, ffmpeg: str, ffprobe: str) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and output.stat().st_mtime >= source.stat().st_mtime:
        return
    duration = max(0.05, end - start)
    if media_type == "audio":
        run([ffmpeg, "-y", "-v", "error", "-ss", str(start), "-t", str(duration),
             "-i", str(source), "-vn", "-c:a", "aac", "-b:a", "192k", str(output)])
        return
    info = probe(source, ffprobe)
    has_audio = any(stream.get("codec_type") == "audio" for stream in info.get("streams", []))
    command = [ffmpeg, "-y", "-v", "error", "-ss", str(start), "-t", str(duration),
               "-i", str(source), "-map", "0:v:0"]
    if has_audio:
        command += ["-map", "0:a:0?"]
    command += ["-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,fps=30",
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "20", "-pix_fmt", "yuv420p"]
    command += ["-c:a", "aac", "-b:a", "192k"] if has_audio else ["-an"]
    command.append(str(output))
    run(command)


def register_docs(case_root: Path, public_case: Path, public_prefix: str,
                  ffprobe: str, extract: bool, degradations: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    source_dir = case_root / "doc_assets"
    images = sorted(path for path in source_dir.glob("*") if path.suffix.lower() in IMAGE_EXT) if source_dir.exists() else []
    if not images:
        degradations.append({"code": "doc_assets_absent", "detail": f"No document image assets under {source_dir}"})
        return []
    rects_path = source_dir / "rects.json"
    rects_by_id = load(rects_path) if rects_path.exists() else {}
    callouts = []
    for source in images:
        target = public_case / "docs" / source.name
        if extract:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        width, height = image_size(source, ffprobe)
        rects = rects_by_id.get(source.stem, {}).get("rects", []) if isinstance(rects_by_id, dict) else []
        if not rects:
            degradations.append({"code": "doc_highlights_absent", "detail": f"No highlight rectangles for {source.name}"})
        callouts.append({"id": source.stem, "img": f"{public_prefix}/docs/{source.name}",
                         "imgW": width, "imgH": height, "rects": rects})
    return callouts


def build(args: argparse.Namespace) -> Dict[str, Any]:
    paper_path, contract_path = Path(args.paper_edit).resolve(), Path(args.contract).resolve()
    transcript_dir, case_root = Path(args.transcripts).resolve(), Path(args.case_root).resolve()
    repo_root, public_dir = Path(args.repo_root).resolve(), Path(args.public_dir).resolve()
    paper, contract = load(paper_path), load(contract_path)
    case_id = str(paper.get("case_id") or case_root.name)
    public_prefix = f"generated/{case_id}"
    public_case = public_dir / "generated" / case_id
    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    ffprobe = shutil.which("ffprobe") or "/opt/homebrew/bin/ffprobe"
    if not Path(ffmpeg).exists() or not Path(ffprobe).exists():
        raise RuntimeError("ffmpeg/ffprobe unavailable; run eval \"$(/opt/homebrew/bin/brew shellenv)\"")
    transcripts = transcript_index(transcript_dir)
    beats = contract.get("beats", [])
    degradations: List[Dict[str, str]] = []
    doc_callouts = register_docs(case_root, public_case, public_prefix, ffprobe, not args.no_extract, degradations)
    docs_by_id = {item["id"]: item for item in doc_callouts}
    events: List[Dict[str, Any]] = []

    for index, item in enumerate(paper.get("timeline", [])):
        kind = item.get("kind")
        event_id = str(item.get("event_id") or item.get("beat_id") or f"ev{index:03d}")
        if kind == "card":
            title = str(item.get("title") or "")
            if str(item.get("card_kind") or "") == "title":
                thesis = str((contract.get("case_shape_card") or {}).get("chosen_thesis") or "")
                if len(thesis) > len(title):
                    title = thesis
            events.append({"type": "card", "eventId": event_id,
                           "cardKind": str(item.get("card_kind") or "phase"),
                           "title": title, "subtitle": str(item.get("subtitle") or ""),
                           "durSec": float(item.get("dur") or 4.0)})
            continue
        if kind == "narration":
            text = str(item.get("text") or "")
            duration = float(item.get("dur") or max(6.0, min(28.0, len(text.split()) / 3.2)))
            events.append({"type": "narration", "eventId": event_id, "text": text,
                           "footer": str(item.get("footer") or ""), "durSec": duration})
            continue
        if kind == "map":
            events.append({"type": "map", "eventId": event_id, "durSec": float(item.get("dur") or 8.0),
                           "title": str(item.get("title") or "Tactical positions"),
                           "background": item.get("background"), "points": item.get("points") or [],
                           "lines": item.get("lines") or [], "narrationTop": str(item.get("narration_top") or ""),
                           "lowerThird": str(item.get("lower_third") or "")})
            continue
        if kind != "clip":
            degradations.append({"code": "unsupported_event_omitted", "detail": f"Timeline index {index} kind={kind!r}", "eventId": event_id})
            continue

        raw_media = str(item.get("media") or "")
        beat = contract_beat(item, beats)
        event_id = str(item.get("event_id") or item.get("beat_id") or beat.get("beat_id") or event_id)
        source = resolve_media(raw_media, repo_root, case_root)
        start, end = float(item.get("in_sec") or 0), float(item.get("out_sec") or 0)
        if end <= start:
            raise ValueError(f"{event_id}: invalid interval {start}-{end}")
        narration = str(item.get("narration_top") or (beat.get("narration") or {}).get("text") or "")
        phrases = item.get("emphasis_phrases") or beat.get("emphasis_phrases") or []
        narration = emphasize(narration, phrases)
        captions = [{"start": float(cap["start"]), "end": float(cap["end"]), "text": str(cap["text"])}
                    for cap in item.get("captions", []) if cap.get("text")]
        if not captions:
            captions = fallback_captions(find_transcript(transcripts, source), start, end)
            if captions:
                degradations.append({"code": "captions_from_segment_transcript", "detail": source.name, "eventId": event_id})
            else:
                degradations.append({"code": "captions_absent", "detail": f"No timed transcript for {source.name}", "eventId": event_id})
        if not narration:
            degradations.append({"code": "narration_absent", "detail": "NarrationBand omitted", "eventId": event_id})
        if not item.get("lower_third"):
            degradations.append({"code": "lower_third_absent", "detail": "LowerThird omitted", "eventId": event_id})

        suffix = source.suffix.lower()
        if suffix in IMAGE_EXT:
            doc = docs_by_id.get(source.stem)
            if not doc:
                target = public_case / "docs" / source.name
                if not args.no_extract:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, target)
                width, height = image_size(source, ffprobe)
                doc = {"id": source.stem, "img": f"{public_prefix}/docs/{source.name}",
                       "imgW": width, "imgH": height, "rects": item.get("rects") or []}
            events.append({"type": "doc", "eventId": event_id, "durSec": end - start,
                           "img": doc["img"], "imgW": doc["imgW"], "imgH": doc["imgH"],
                           "rects": item.get("rects") or doc.get("rects") or [], "source": source.name,
                           "narrationTop": narration, "lowerThird": str(item.get("lower_third") or "")})
            continue

        media_type = "audio" if suffix in AUDIO_EXT else "clip"
        output_name = f"{event_id}.m4a" if media_type == "audio" else f"{event_id}.mp4"
        target = public_case / "media" / output_name
        if not args.no_extract:
            extract_media(source, target, start, end, media_type, ffmpeg, ffprobe)
        # audience-facing label + muted-lead countdown (operator polish spec)
        lower = humanize_label(str(item.get("lower_third") or ""))
        resumes = 0.0
        if media_type == "clip" and not args.no_extract and target.exists():
            resumes = silent_lead_sec(target, ffmpeg)
            if resumes:
                degradations.append({"code": "muted_lead_countdown", "detail": f"{resumes}s silent lead", "eventId": event_id})
        media_event = {"type": media_type, "eventId": event_id, "file": f"{public_prefix}/media/{output_name}",
                       "durSec": round(end - start, 3), "lowerThird": lower,
                       "narrationTop": narration, "captions": captions,
                       "capOffset": float(item.get("capOffset", item.get("cap_offset", beat.get("capOffset", beat.get("cap_offset", 0)))) or 0),
                       "creditLine": str(item.get("credit_line") or ""), "sourceLabel": source.stem[:100]}
        if resumes:
            media_event["audioResumesInSec"] = resumes
        events.append(media_event)

    total = round(sum(float(event["durSec"]) for event in events), 3)
    props = {
        "schemaVersion": "flameon.remotion.v1", "caseId": case_id,
        "agency": str(paper.get("agency") or "Releasing Agency"), "fps": 30,
        "width": 1280, "height": 720, "totalSec": total,
        "audio": {"voEnabled": False, "duckTo": 0.24},
        "beats": [{"beatId": str(beat.get("beat_id") or ""),
                   "sourceFile": str(beat.get("source_file") or ""),
                   "inSec": float(beat.get("in_sec") or 0), "outSec": float(beat.get("out_sec") or 0),
                   "lowerThird": str(beat.get("lower_third") or ""),
                   "narration": str((beat.get("narration") or {}).get("text") or "")}
                  for beat in beats],
        "events": events,
        "docCallouts": doc_callouts, "degradations": degradations,
        "provenance": {"paperEdit": str(paper_path), "paperEditSha256": sha256(paper_path),
                       "contract": str(contract_path), "contractSha256": sha256(contract_path),
                       "transcripts": str(transcript_dir), "caseRoot": str(case_root),
                       "mediaPolicy": "clean_reextract_from_original_only",
                       "voicePolicy": "disabled_no_network_calls"},
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(props, indent=1, ensure_ascii=False), encoding="utf-8")
    return props


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper-edit", required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--transcripts", required=True)
    parser.add_argument("--case-root", required=True)
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--public-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--no-extract", action="store_true")
    args = parser.parse_args(argv)
    props = build(args)
    counts: Dict[str, int] = {}
    for event in props["events"]:
        counts[event["type"]] = counts.get(event["type"], 0) + 1
    print(json.dumps({"case_id": props["caseId"], "events": counts, "total_sec": props["totalSec"],
                      "degradations": len(props["degradations"]), "vo_enabled": props["audio"]["voEnabled"],
                      "external_calls": 0, "output": str(Path(args.output).resolve())}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
