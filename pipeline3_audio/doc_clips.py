"""P3.5 / GOAL_D D5 — turn a document's clip_directions into clip windows.

This closes the doc-directed loop: ``doc_extract.py`` reads a case document's
own pointers to footage (e.g. the UoF form's *Video Available: Camera /
Time* field → ``{"ref": "Deputy Vasquez's BWC", "window": "0943-0948 Hours"}``).
Those are **wall-clock** windows. Every stamped artifact already carries its
absolute recording start (``start_iso`` from AXON OCR / ``audio_xcorr``), so we
can project the document's wall-clock window onto each artifact's *local*
[start, end] seconds — and transcribe exactly the moment the DOCUMENT says
matters, instead of guessing from audio loudness alone.

Pure stdlib, zero-network, same-day arithmetic (seconds-of-day diff — no
timezone juggling, which keeps it consistent with however ``start_iso`` was
stamped). Daytime, single-day incidents; a window that would cross midnight is
rejected rather than mis-projected.
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

# "0943-0948 Hours", "09:43-09:48", "0943 - 0948 hrs", en-dash variants.
_WIN = re.compile(r"\b(\d{1,2}):?(\d{2})\s*[-–—]\s*(\d{1,2}):?(\d{2})\b")
# A single bare time ("0944", "09:44") → expand to a default span.
_SINGLE = re.compile(r"\b(\d{1,2}):?(\d{2})\b")
_DAY = 24 * 3600


def parse_clock_window(window: str, single_span_sec: float = 300.0) -> Optional[Tuple[int, int]]:
    """Parse a wall-clock window string into ``(start, end)`` seconds-of-day.

    Handles ``"0943-0948 Hours"``, ``"09:43-09:48"``, en/em dashes, and a bare
    single time (``"0944"`` → a ``single_span_sec`` window centred forward).
    Returns ``None`` if no time can be read or the values are out of range.
    """
    if not window:
        return None
    m = _WIN.search(window)
    if m:
        h1, m1, h2, m2 = (int(x) for x in m.groups())
        if h1 > 23 or h2 > 23 or m1 > 59 or m2 > 59:
            return None
        return (h1 * 3600 + m1 * 60, h2 * 3600 + m2 * 60)
    m = _SINGLE.search(window)
    if m:
        h, mm = int(m.group(1)), int(m.group(2))
        if h > 23 or mm > 59:
            return None
        s = h * 3600 + mm * 60
        return (s, int(s + single_span_sec))
    return None


def _sod_from_iso(start_iso: Optional[str]) -> Optional[int]:
    """Seconds-of-day from a ``start_iso`` like ``'2023-04-18 09:44:49'``."""
    m = re.search(r"\b(\d{1,2}):(\d{2}):(\d{2})\b", start_iso or "")
    if not m:
        return None
    h, mm, ss = (int(x) for x in m.groups())
    return h * 3600 + mm * 60 + ss


def doc_windows_for_artifact(
    clip_directions: List[Dict],
    start_iso: Optional[str],
    duration_sec: float,
    pad_pre: float = 30.0,
    pad_post: float = 30.0,
) -> List[List[float]]:
    """Project document wall-clock windows onto one artifact's local timeline.

    Each direction's ``window`` (or, failing that, ``ref``) is parsed as
    wall-clock, offset by the artifact's own recording start, padded, clipped to
    ``[0, duration_sec]``, and merged. A direction whose window does not overlap
    this artifact's footage clips to nothing and is dropped — so the SAME set of
    directions can be applied to every POV and each keeps only the part it
    actually recorded (automatic cross-POV overlap, no name matching needed).
    """
    base = _sod_from_iso(start_iso)
    if base is None:
        return []
    out: List[List[float]] = []
    for cd in clip_directions or []:
        w = parse_clock_window(cd.get("window", "") or cd.get("ref", ""))
        if not w:
            continue
        ws, we = w
        # Same-day only: reject a parse that would imply a midnight wrap.
        if we < ws:
            continue
        s = (ws - base) - pad_pre
        e = (we - base) + pad_post
        s = max(0.0, s)
        e = min(float(duration_sec), e)
        if e > s:
            out.append([round(s, 1), round(e, 1)])
    out.sort()
    merged: List[List[float]] = []
    for s, e in out:
        if merged and s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    return merged
