"""
transcript_loader.py — Load and merge Pipeline 3 transcripts for Pipeline 4.

A single case can have multiple transcripts (e.g., 2 BWC videos + 2 interview
MP3s for the same incident). This module groups them by case_id and produces
a unified view with source_idx tags on every segment, so the LLM can
distinguish overlapping timestamps between sources.
"""

import json
from pathlib import Path
from collections import defaultdict
from typing import Optional


def load_single(path) -> dict:
    """Load a single Pipeline 3 transcript JSON file."""
    path = Path(path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if "case_id" not in data or "transcript" not in data:
        raise ValueError(f"Not a valid Pipeline 3 transcript: {path}")
    data["_source_path"] = str(path)
    return data


def discover_case_transcripts(directory, case_id: Optional[str] = None) -> dict:
    """
    Scan a directory of Pipeline 3 transcripts, group by case_id.
    Returns dict: {case_id: [path, path, ...]}
    If case_id is provided, only returns that group.
    """
    directory = Path(directory)
    groups = defaultdict(list)
    for path in sorted(directory.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            cid = data.get("case_id")
            if cid:
                groups[cid].append(path)
        except (json.JSONDecodeError, OSError):
            continue
    if case_id:
        return {case_id: groups.get(case_id, [])}
    return dict(groups)


def merge_transcripts(paths: list) -> dict:
    """
    Load multiple transcripts that share a case_id and merge them into one
    unified dict with source_idx tags. All sources must share case_id.

    Output shape:
        {
          "case_id": str,
          "sources": [
            {source_idx, source_url, evidence_type, duration_sec, transcript_path, processed_duration_sec}
          ],
          "segments": [
            {source_idx, start_sec, end_sec, text, speaker, confidence}
          ],
          "total_duration_sec": float,
          "transcript_refs": [paths],
          "available_evidence_types": [unique evidence types across sources]
        }
    """
    if not paths:
        raise ValueError("No transcript paths provided")

    loaded = [load_single(p) for p in paths]

    # Validate all share case_id
    case_ids = {d.get("case_id") for d in loaded}
    if len(case_ids) > 1:
        raise ValueError(f"Multiple case_ids found, cannot merge: {case_ids}")
    case_id = next(iter(case_ids))

    # Stable sort by source_url then path — ensures reproducible source_idx assignment
    loaded.sort(key=lambda d: (d.get("source_url", ""), d.get("_source_path", "")))

    sources = []
    segments = []
    evidence_types = []
    total_duration = 0.0

    for idx, data in enumerate(loaded):
        source_url = data.get("source_url", data.get("_source_path", f"source_{idx}"))
        evidence_type = data.get("source_evidence_type", "other")
        duration = float(data.get("original_duration_sec", 0))
        processed = float(data.get("processed_duration_sec", duration))

        sources.append({
            "source_idx": idx,
            "source_url": source_url,
            "evidence_type": evidence_type,
            "duration_sec": duration,
            "processed_duration_sec": processed,
            "transcript_path": data.get("_source_path", ""),
        })
        if evidence_type and evidence_type not in evidence_types:
            evidence_types.append(evidence_type)
        total_duration += duration

        for seg in data.get("transcript", []):
            segments.append({
                "source_idx": idx,
                "start_sec": float(seg.get("start_sec", 0)),
                "end_sec": float(seg.get("end_sec", 0)),
                "text": seg.get("text", "").strip(),
                "speaker": seg.get("speaker"),
                "confidence": seg.get("confidence"),
            })

    return {
        "case_id": case_id,
        "sources": sources,
        "segments": segments,
        "total_duration_sec": round(total_duration, 3),
        "transcript_refs": [s["transcript_path"] for s in sources],
        "available_evidence_types": evidence_types,
    }


def _format_timestamp(sec: float) -> str:
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = int(sec % 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def format_for_llm(merged: dict, max_chars: Optional[int] = None) -> str:
    """
    Render a merged transcript as text for LLM consumption.

    Each line: [HH:MM:SS | S<idx> | SPK<n>] text

    Sources are separated by banner lines so the LLM can tell them apart
    even when timestamps overlap between sources.
    """
    lines = []
    current_source_idx = None

    for seg in merged["segments"]:
        # Banner when switching sources
        if seg["source_idx"] != current_source_idx:
            current_source_idx = seg["source_idx"]
            source = merged["sources"][current_source_idx]
            lines.append("")
            lines.append(
                f"=== SOURCE S{current_source_idx}: {source['source_url']} "
                f"({source['evidence_type']}, {source['duration_sec']:.0f}s) ==="
            )

        ts = _format_timestamp(seg["start_sec"])
        speaker = seg.get("speaker") or "SPK?"
        text = seg["text"]
        lines.append(f"[{ts} | S{seg['source_idx']} | {speaker}] {text}")

    rendered = "\n".join(lines)

    if max_chars is not None and len(rendered) > max_chars:
        # Truncate with an explicit marker for dry-run display
        head = rendered[: max_chars // 2]
        tail = rendered[-max_chars // 2 :]
        total_lines = len(lines)
        shown_head = head.count("\n")
        shown_tail = tail.count("\n")
        omitted = total_lines - shown_head - shown_tail
        rendered = f"{head}\n\n... [{omitted} more lines, {len(rendered) - max_chars} chars omitted] ...\n\n{tail}"

    return rendered


def load_case(
    transcript: Optional[str] = None,
    transcripts: Optional[list] = None,
    transcript_dir: Optional[str] = None,
    case_id: Optional[str] = None,
) -> dict:
    """
    Unified loader. Resolves CLI args into a merged transcript dict.

    Exactly one of (transcript, transcripts, transcript_dir) must be provided.
    If transcript_dir is provided with case_id, filters to that case.
    If transcript_dir is provided without case_id, this returns only the FIRST
    group (caller should use discover_case_transcripts for batch mode).
    """
    if transcript:
        return merge_transcripts([Path(transcript)])
    if transcripts:
        return merge_transcripts([Path(p) for p in transcripts])
    if transcript_dir:
        groups = discover_case_transcripts(transcript_dir, case_id)
        if not groups:
            raise ValueError(f"No transcripts found in {transcript_dir}")
        if case_id:
            paths = groups.get(case_id, [])
            if not paths:
                raise ValueError(f"No transcripts found for case_id={case_id}")
            return merge_transcripts(paths)
        # No case_id — return first group (caller should iterate)
        first_cid = next(iter(groups))
        return merge_transcripts(groups[first_cid])
    raise ValueError("Must provide one of: transcript, transcripts, transcript_dir")
