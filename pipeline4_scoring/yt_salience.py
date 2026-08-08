"""GOAL_D / judge — viewer-salience from timestamped YouTube comments.

Timestamped comments ("0:58 when the dog…", "2:14 listen to what he says") are
crowd-sourced moment labels: viewers literally mark the seconds they react to.
Aggregated across a corpus they give a *viewer-salience* curve — the "what reads
as a key moment to an audience" signal the transcript-only detector is missing.

This module is the deterministic core of that pipeline:
  harvest comments -> parse timestamps -> salience curve -> top salient windows
which then (a) STEER P4 moment detection (few-shot exemplars), (b) CALIBRATE
importance against real reactions, and (c) seed the rubric's *viewer-salience*
axis — to be weighed AGAINST the accountability-substance axis P4 already encodes
in moment_type (procedural_violation / contradiction / reveal). YouTube supplies
"interesting"; it must not overwrite "important". See GOAL_D_VISION_AND_SALIENCE.md.

The parse/curve/windows here are pure + zero-network (tested with fixtures). The
harvest is the only network part and is deliberately NOT run at import/test time.
"""
from __future__ import annotations

import math
import re
from typing import Dict, List, Optional, Tuple

# M:SS or H:MM:SS as written in comments. Word-bounded; seconds 00-59 enforced so
# scores ("5-1") and ratios don't match. Minutes up to 2 digits (long videos).
_TS = re.compile(r"(?<![\d:])(?:(\d{1,2}):)?(\d{1,2}):([0-5]\d)(?![\d:])")
# obvious non-reaction noise to discount (engagement spam, not a moment label)
_NOISE = re.compile(r"\b(first|early|who'?s here|like if|sub(scribe)?)\b", re.I)


def parse_timestamp(text: str) -> List[int]:
    """All timestamps in a string, as seconds. '1:02:03' -> 3723, '0:58' -> 58."""
    out: List[int] = []
    for h, m, s in _TS.findall(text or ""):
        secs = (int(h) if h else 0) * 3600 + int(m) * 60 + int(s)
        out.append(secs)
    return out


def comment_weight(like_count: int) -> float:
    """A timestamped mention's weight: base 1 + log1p(likes). Diminishing returns
    so one viral comment can't dominate a curve."""
    return 1.0 + math.log1p(max(0, int(like_count or 0)))


def parse_comment_timestamps(comments: List[Dict],
                             max_per_comment: int = 3) -> List[Dict]:
    """Flatten comments into timestamped reactions ``{sec, weight, text}``.

    ``comments``: dicts with ``text`` (+ optional ``like_count``). A comment with
    several timestamps contributes each (capped at ``max_per_comment`` so a
    chapter-list comment doesn't flood); obvious engagement spam is dropped.
    """
    out: List[Dict] = []
    for c in comments or []:
        text = (c or {}).get("text", "") or ""
        if _NOISE.search(text):
            continue
        secs = parse_timestamp(text)[:max_per_comment]
        if not secs:
            continue
        w = comment_weight((c or {}).get("like_count", 0))
        for s in secs:
            out.append({"sec": s, "weight": round(w, 3), "text": text[:200]})
    return out


def salience_curve(stamps: List[Dict], duration_sec: float,
                   bin_sec: float = 5.0) -> List[Dict]:
    """Bin timestamped reactions into a weighted curve over the video.
    Returns ``[{start, end, weight, n}]`` for every bin (zeros included)."""
    if duration_sec <= 0:
        return []
    nbins = int(math.ceil(duration_sec / bin_sec))
    bins = [{"start": round(i * bin_sec, 1), "end": round(min((i + 1) * bin_sec, duration_sec), 1),
             "weight": 0.0, "n": 0} for i in range(nbins)]
    for st in stamps:
        i = int(st["sec"] // bin_sec)
        if 0 <= i < nbins:
            bins[i]["weight"] += st["weight"]
            bins[i]["n"] += 1
    for b in bins:
        b["weight"] = round(b["weight"], 3)
    return bins


def top_salient_windows(curve: List[Dict], k: int = 8, pad_sec: float = 5.0,
                        min_weight: float = 1.0, merge_gap_sec: float = 10.0
                        ) -> List[Dict]:
    """The ``k`` highest-weight bins as padded, merged windows — the moments an
    audience flagged. Returns ``[{start, end, weight, n}]`` sorted chronologically."""
    hot = sorted((b for b in curve if b["weight"] >= min_weight),
                 key=lambda b: -b["weight"])[:k]
    spans = sorted(([max(0.0, b["start"] - pad_sec), b["end"] + pad_sec, b["weight"], b["n"]]
                    for b in hot), key=lambda x: x[0])
    merged: List[Dict] = []
    for s, e, w, n in spans:
        if merged and s <= merged[-1]["end"] + merge_gap_sec:
            merged[-1]["end"] = max(merged[-1]["end"], e)
            merged[-1]["weight"] = round(merged[-1]["weight"] + w, 3)
            merged[-1]["n"] += n
        else:
            merged.append({"start": round(s, 1), "end": round(e, 1), "weight": round(w, 3), "n": n})
    return merged


def align_to_predicted(salient: List[Dict], predicted_moments: List[Dict],
                       tol_sec: float = 8.0) -> Dict:
    """Calibration: how many viewer-salient windows the detector's predicted
    moments (each with ``timestamp_sec``) actually hit. Returns precision/recall
    style counts — the gap is the steering signal."""
    pred = [float(m.get("timestamp_sec", -1)) for m in predicted_moments or []]
    hit = 0
    for w in salient:
        if any(w["start"] - tol_sec <= t <= w["end"] + tol_sec for t in pred):
            hit += 1
    return {"salient_windows": len(salient), "covered_by_prediction": hit,
            "missed": len(salient) - hit,
            "recall": round(hit / len(salient), 3) if salient else None}


# ---------------------------------------------------------------------------
# Harvest (network — scoped, NOT run in tests/at import)
# ---------------------------------------------------------------------------

def harvest_comments_ytdlp(video_url: str, max_comments: int = 2000) -> List[Dict]:
    """Fetch public comments via yt-dlp (``--write-comments``). Network + rate
    limits; call deliberately, not in a hot loop. Returns ``[{text, like_count}]``.

    Kept thin and isolated so the pure pipeline above stays testable. Requires
    yt-dlp installed; raises if absent so callers fail loudly rather than silently
    harvesting nothing.
    """
    try:
        import yt_dlp  # type: ignore
    except ImportError as e:  # pragma: no cover - network/optional dep
        raise RuntimeError("yt-dlp not installed (pip install yt-dlp)") from e
    opts = {
        "skip_download": True, "getcomments": True, "quiet": True,
        "extractor_args": {"youtube": {"max_comments": [str(max_comments), "all", "0", "0"],
                                       "comment_sort": ["top"]}},
    }
    with yt_dlp.YoutubeDL(opts) as ydl:  # pragma: no cover - network
        info = ydl.extract_info(video_url, download=False)
    return [{"text": c.get("text", ""), "like_count": c.get("like_count", 0)}
            for c in (info.get("comments") or [])]
