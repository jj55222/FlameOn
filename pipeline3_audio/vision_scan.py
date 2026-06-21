"""P3.5 / GOAL_D — vision scan: detect the silent visual moments a transcript can't see.

The moment-detection pipeline scores TRANSCRIPTS, so a silent visual action —
an AXON-muted K9 release, a takedown off-mic, a weapon drawn — is invisible to
it. This module has a vision model WATCH the footage: it samples frames from the
hot windows, sends them (downscaled, batched, labelled with their timecode) to a
VLM, and gets back structured visual events — ``{timecode, event_type, actor,
description, confidence}`` — that become candidate beats and feed the rubric.

Two jobs, both things audio can't do:
  1. DETECT a silent force event and time it (so it can be a beat at all).
  2. DESCRIBE who did what (so the clip can be framed from the ACTOR's POV and
     the moment scored for accountability substance, not just dialogue).

Cost is controlled, not incidental: frames are downscaled and sampled at a low
fps only inside the triage hot windows, and batched many-per-call. The pure
helpers (batching, parsing, cross-frame event merge) are zero-network testable;
``VisionBackend`` is the only paid part and ``MockVisionBackend`` stands in for
tests/dry-runs. Reuses pov_triage's ffmpeg discovery. Global Python.
"""
from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

FRAME_W = 640                 # downscale frames before sending (cost/payload)
DEFAULT_FPS = 1.0             # 1 frame/sec inside a hot window
DEFAULT_BATCH = 8             # frames per VLM call
MAX_FRAMES = 160             # hard cap per scan (cost backstop)

# Force / accountability-relevant visual events the model is asked to flag.
EVENT_TYPES = (
    "k9_deployment", "taser", "strike", "takedown", "weapon_drawn",
    "firearm_pointed", "handcuffing", "foot_pursuit", "medical_aid",
    "search", "use_of_force_other", "scene",
)


# ---------------------------------------------------------------------------
# Frame sampling (downscaled, bounded)
# ---------------------------------------------------------------------------

def sample_frames(media: str, windows: List[List[float]], fps: float = DEFAULT_FPS,
                  out_dir: Optional[Path] = None, max_frames: int = MAX_FRAMES
                  ) -> List[Tuple[float, str]]:
    """Extract downscaled frames inside each ``[start, end]`` window. Returns
    ``[(timecode_sec, png_path), ...]`` capped at ``max_frames``."""
    import pov_triage as pt  # lazy (pulls numpy); keeps the pure helpers import-light
    if not pt.FFMPEG:
        pt.FFMPEG, pt.FFPROBE = pt._ff()
    out_dir = Path(out_dir or ".tmp/_vision_frames")
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = Path(media).stem
    frames: List[Tuple[float, str]] = []
    step = 1.0 / max(fps, 1e-6)
    for wi, (s, e) in enumerate(windows):
        t = float(s)
        while t < float(e) and len(frames) < max_frames:
            png = str(out_dir / f"{stem}_{wi}_{t:.1f}.png")
            r = subprocess.run(
                [pt.FFMPEG, "-y", "-ss", str(t), "-i", str(media), "-frames:v", "1",
                 "-vf", f"scale={FRAME_W}:-1", png], capture_output=True)
            if os.path.exists(png):
                frames.append((round(t, 1), png))
            t += step
    return frames


# ---------------------------------------------------------------------------
# Pure helpers (zero-network testable)
# ---------------------------------------------------------------------------

def batch(items: List[Any], n: int) -> List[List[Any]]:
    """Split a list into chunks of size ``n`` (pure)."""
    n = max(1, int(n))
    return [items[i:i + n] for i in range(0, len(items), n)]


def _b64_png(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("ascii")


def parse_events(raw: str, valid_window: Optional[Tuple[float, float]] = None) -> List[Dict]:
    """Parse a VLM JSON reply into validated event dicts. Tolerant of the usual
    LLM shape drift (an object with an ``events`` list, or a bare list). Drops
    anything without a numeric timecode; clamps event_type to the taxonomy; keeps
    only timecodes inside ``valid_window`` when given (no invented timestamps)."""
    try:
        from llm_backends import clean_llm_output  # type: ignore
        raw = clean_llm_output(raw)
    except Exception:
        pass
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return []
    if isinstance(data, dict):
        data = data.get("events") or data.get("moments") or []
    if not isinstance(data, list):
        return []
    out: List[Dict] = []
    for e in data:
        if not isinstance(e, dict):
            continue
        tc = e.get("timecode_sec", e.get("timecode", e.get("t")))
        try:
            tc = float(tc)
        except (TypeError, ValueError):
            continue
        if valid_window and not (valid_window[0] - 0.5 <= tc <= valid_window[1] + 0.5):
            continue
        et = str(e.get("event_type", e.get("type", "scene"))).lower().strip()
        if et not in EVENT_TYPES:
            et = "use_of_force_other" if "force" in et else "scene"
        try:
            conf = float(e.get("confidence", 0.5))
        except (TypeError, ValueError):
            conf = 0.5
        out.append({
            "timecode_sec": round(tc, 1), "event_type": et,
            "actor": (e.get("actor") or e.get("who") or "")[:60],
            "description": (e.get("description") or e.get("desc") or "")[:240],
            "confidence": max(0.0, min(1.0, conf)),
        })
    return out


def merge_events(events: List[Dict], gap_sec: float = 3.0) -> List[Dict]:
    """Collapse same-type events within ``gap_sec`` (a force action spans several
    frames). Keeps the highest-confidence representative and its timecode."""
    if not events:
        return []
    ordered = sorted(events, key=lambda e: (e["event_type"], e["timecode_sec"]))
    merged: List[Dict] = []
    for e in ordered:
        prev = merged[-1] if merged else None
        if (prev and prev["event_type"] == e["event_type"]
                and e["timecode_sec"] - prev["timecode_sec"] <= gap_sec):
            if e["confidence"] > prev["confidence"]:
                merged[-1] = {**e}
        else:
            merged.append({**e})
    merged.sort(key=lambda e: e["timecode_sec"])
    return merged


# ---------------------------------------------------------------------------
# Vision backends
# ---------------------------------------------------------------------------

_SYSTEM = (
    "You are a police-accountability video analyst. You are shown a sequence of "
    "bodycam/dashcam frames, each labelled with its timecode in seconds. Identify "
    "ACTIONS, especially uses of force: " + ", ".join(EVENT_TYPES[:-1]) + ". "
    "For each event, give the timecode of the labelled frame where it is clearest, "
    "the actor (the officer/person performing it, by visible role/appearance — never "
    "invent a name), a one-line description, and a confidence 0-1. Report only what "
    "is visible in these frames. Return JSON: "
    '{"events":[{"timecode_sec":<n>,"event_type":"<type>","actor":"<role>",'
    '"description":"<text>","confidence":<0-1>}]} and nothing else.'
)


class VisionBackend:
    """OpenRouter vision client (OpenAI-compatible). Sends downscaled frames as
    image content interleaved with their timecode labels. PAID — call on bounded
    frame sets only. Needs OPENROUTER_API_KEY (load .env before constructing)."""

    BASE_URL = "https://openrouter.ai/api/v1"

    def __init__(self, model: str = "google/gemini-2.5-flash",
                 api_key: Optional[str] = None, timeout: int = 240):
        self.model = model
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY", "")
        if not self.api_key:
            raise RuntimeError("OPENROUTER_API_KEY not set (load .env)")
        self.timeout = timeout
        self._client = None

    def _get_client(self):
        if self._client is None:
            from openai import OpenAI  # type: ignore
            self._client = OpenAI(api_key=self.api_key, base_url=self.BASE_URL)
        return self._client

    def describe(self, frames: List[Tuple[float, str]],
                 system: str = _SYSTEM, user: str = "") -> str:
        content: List[Dict[str, Any]] = [{"type": "text",
                                          "text": user or "Analyze these frames in order."}]
        for tc, png in frames:
            content.append({"type": "text", "text": f"[t={tc:.1f}s]"})
            content.append({"type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{_b64_png(png)}"}})
        resp = self._get_client().chat.completions.create(
            model=self.model,
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": content}],
            temperature=0.1, max_tokens=1500, timeout=self.timeout,
            extra_headers={"HTTP-Referer": "https://github.com/jj55222/FlameOn",
                           "X-Title": "FlameOn Vision Scan"})
        return resp.choices[0].message.content or ""


class MockVisionBackend:
    """Returns a fixed JSON reply (set at construction) — zero cost, for tests
    and dry runs. Records the frames it was handed."""

    def __init__(self, reply: Any = None):
        self._reply = reply if reply is not None else {"events": []}
        self.calls: List[List[Tuple[float, str]]] = []

    def describe(self, frames, system: str = _SYSTEM, user: str = "") -> str:
        self.calls.append(list(frames))
        return self._reply if isinstance(self._reply, str) else json.dumps(self._reply)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def scan_visual_events(media: str, windows: List[List[float]], backend,
                       fps: float = DEFAULT_FPS, batch_size: int = DEFAULT_BATCH,
                       out_dir: Optional[Path] = None) -> List[Dict]:
    """Sample frames in ``windows``, ask the backend to flag visual events per
    batch, then merge across batches. Returns merged event dicts (sourced to this
    media's local timeline)."""
    frames = sample_frames(media, windows, fps=fps, out_dir=out_dir)
    if not frames:
        return []
    span = (frames[0][0], frames[-1][0])
    events: List[Dict] = []
    for fb in batch(frames, batch_size):
        raw = backend.describe(fb)
        events.extend(parse_events(raw, valid_window=span))
    return merge_events(events)


# ---------------------------------------------------------------------------
# CLI — batch-scan selected timeline artifacts -> a combined events file
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description="GOAL_D — vision-scan timeline artifacts for visual events")
    ap.add_argument("--artifacts", required=True, type=Path)
    ap.add_argument("--timeline", type=Path, default=None, help="optional; restrict to phases")
    ap.add_argument("--phases", default="incident", help="comma list (needs --timeline)")
    ap.add_argument("--kinds", default="bodycam", help="comma artifact-kind filter")
    ap.add_argument("--window-cap", type=float, default=100.0,
                    help="seconds from each artifact's start to scan (the incident action)")
    ap.add_argument("--fps", type=float, default=0.4)
    ap.add_argument("--batch", type=int, default=DEFAULT_BATCH)
    ap.add_argument("--model", default="google/gemini-2.5-flash")
    ap.add_argument("--mock", action="store_true", help="no VLM call (empty events; wiring check)")
    ap.add_argument("--out", type=Path, default=Path(".tmp/vision_events.json"))
    args = ap.parse_args(argv)

    arts = {a["artifact_id"]: a for a in json.loads(args.artifacts.read_text(encoding="utf-8"))}
    kinds = {k.strip() for k in args.kinds.split(",") if k.strip()}
    selected_ids = [aid for aid, a in arts.items() if a.get("kind") in kinds]
    if args.timeline:
        tl = json.loads(args.timeline.read_text(encoding="utf-8"))
        phases = {p.strip() for p in args.phases.split(",") if p.strip()}
        in_phase = {r["artifact_id"] for ph, recs in (tl.get("phases") or {}).items()
                    if ph in phases for r in recs}
        selected_ids = [aid for aid in selected_ids if aid in in_phase]

    if args.mock:
        backend = MockVisionBackend()
        print("[vision] MOCK (no network)")
    else:
        try:
            from dotenv import load_dotenv  # type: ignore
            load_dotenv()
        except Exception:
            pass
        backend = VisionBackend(model=args.model)
        print(f"[vision] live: {backend.model} (paid)")

    all_events: List[Dict] = []
    for aid in selected_ids:
        a = arts[aid]
        if not (a.get("path") and Path(a["path"]).exists()):
            continue
        cap = min(args.window_cap, float(a.get("duration_sec") or args.window_cap))
        ev = scan_visual_events(a["path"], [[0.0, cap]], backend, fps=args.fps, batch_size=args.batch)
        for e in ev:
            e["artifact_id"] = aid
        all_events.extend(ev)
        print(f"  {aid:10s} {len(ev)} event(s): "
              f"{', '.join(sorted({e['event_type'] for e in ev})) or '-'}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(all_events, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[vision] {len(all_events)} event(s) across {len(selected_ids)} artifact(s) -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
