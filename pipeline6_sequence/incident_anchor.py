"""P6 — derive the incident anchor automatically (the operator decision removed).

The phase bucketer (``pipeline3_audio/timeline_build``) needs to know WHEN the
incident is. Its default — the earliest trusted stamp — is wrong whenever the
climax is a later beat (an overdose 3 h after the seizure that opens the day),
which plays the whole story backwards. Picking the anchor by hand is the single
biggest "Claude-in-the-loop" step; this module derives it from signals the case
already carries, so the pipeline runs without an operator.

WHY NOT "the most-salient camera": that fails. On 2023PSB-0530 the densest
transcript-salience sits on the 23:09 truck-investigation cam (people talking,
many 'reveal' moments), NOT the 20:45 overdose (chaotic, few clean lines). A
naive salience-argmax anchors on the investigation and still plays it backwards.

What actually marks the incident is **camera CONVERGENCE × salience, clustered by
time**: the incident is where multiple independent cameras fire at once AND the
salience concentrates. The investigation is salient but single-camera. So we
cluster key-moments by absolute time and score each cluster
``sum(importance) × distinct_cameras``, prefer the earliest of the top clusters
(the incident precedes its own investigation), and snap the anchor to the
earliest camera start in that cluster so phases bucket on a real recording edge.
A document cross-check (the IA report cites the incident footage by timestamp)
raises confidence. Pure stdlib; ``--selftest`` covers the 2023PSB shape.
"""
from __future__ import annotations

import datetime as dt
import re
from typing import Any, Dict, List, Optional

_IMPORTANCE_W = {"critical": 3.0, "high": 2.0, "medium": 1.0, "low": 0.5}
DEFAULT_WINDOW_SEC = 30 * 60     # cluster radius: cameras within 30 min are "the same event"


def _w(importance: Optional[str]) -> float:
    return _IMPORTANCE_W.get((importance or "").lower(), 1.0)


def _moment_events(verdict: Dict, sources: List) -> List[Dict]:
    """Map each key_moment to (absolute epoch, weight, camera index). Drops
    moments whose source has no absolute time (can't be placed)."""
    events: List[Dict] = []
    for m in verdict.get("key_moments", []) or []:
        si = m.get("source_idx", 0)
        s = sources[si] if 0 <= si < len(sources) else None
        if not s or s.start_epoch is None:
            continue
        events.append({"epoch": s.start_epoch + float(m.get("timestamp_sec") or 0),
                       "weight": _w(m.get("importance")), "cam": si,
                       "cam_start": s.start_epoch})
    return events


def parse_doc_timestamps(doc_extracts: Optional[List[Dict]]) -> List[float]:
    """Pull candidate incident epochs the IA report names directly — its clip
    directions cite Axon videos by date+time ('...2023-10-24-2046 PC 832' →
    20:46 UTC). Used only to CROSS-CHECK / raise confidence, never alone."""
    out: List[float] = []
    pat = re.compile(r"(\d{4})-(\d{2})-(\d{2})[\sT_-]+(\d{2})(\d{2})\b")
    for de in (doc_extracts or []):
        for cd in (de.get("clip_directions") or []):
            for fld in (cd.get("ref"), cd.get("depicts")):
                for mt in pat.finditer(str(fld or "")):
                    y, mo, d, hh, mm = (int(x) for x in mt.groups())
                    if not (1 <= mo <= 12 and 1 <= d <= 31 and hh < 24 and mm < 60):
                        continue
                    try:
                        out.append(dt.datetime(y, mo, d, hh, mm,
                                               tzinfo=dt.timezone.utc).timestamp())
                    except ValueError:
                        continue
    return out


def derive_anchor(artifacts: List[Dict], sources: List, verdict: Dict,
                  doc_extracts: Optional[List[Dict]] = None,
                  window_sec: float = DEFAULT_WINDOW_SEC) -> Optional[Dict[str, Any]]:
    """Return ``{epoch, iso, score, cameras, weight, confidence, rationale,
    doc_confirmed}`` for the inferred incident, or ``None`` when the signal is too
    weak to overrule the default anchor (faithfulness over cleverness)."""
    events = _moment_events(verdict, sources)
    if len(events) < 2:
        return None

    doc_ts = parse_doc_timestamps(doc_extracts)

    # Score every candidate cluster (seeded on each moment): salience × convergence.
    best: Optional[Dict[str, Any]] = None
    for seed in events:
        members = [e for e in events if abs(e["epoch"] - seed["epoch"]) <= window_sec / 2.0]
        wsum = sum(e["weight"] for e in members)
        cams = sorted({e["cam"] for e in members})
        score = wsum * len(cams)
        # Anchor = the earliest CAMERA START among the cluster's cams: the incident
        # cam begins rolling at the incident, and bucketing on a real edge keeps
        # the climax in 'incident' rather than spilling across a window boundary.
        anchor = min(e["cam_start"] for e in members)
        cand = {"epoch": anchor, "score": round(score, 1), "weight": round(wsum, 1),
                "cameras": len(cams)}
        # Prefer higher score; tie-break EARLIER (incident precedes investigation).
        if best is None or (cand["score"], -cand["epoch"]) > (best["score"], -best["epoch"]):
            best = cand
    assert best is not None

    iso = dt.datetime.fromtimestamp(best["epoch"], dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    doc_confirmed = any(abs(best["epoch"] - t) <= window_sec for t in doc_ts)
    # Confidence: convergence is the strong signal; the document confirms framing.
    if best["cameras"] >= 2 and doc_confirmed:
        conf = "high"
    elif best["cameras"] >= 2 or doc_confirmed:
        conf = "medium"
    else:
        conf = "low"
    best.update({
        "iso": iso, "confidence": conf, "doc_confirmed": doc_confirmed,
        "rationale": (f"top time-cluster: {best['cameras']} converging camera(s), "
                      f"salience {best['weight']}, score {best['score']}"
                      + (" — confirmed by IA-report clip timestamp" if doc_confirmed else "")),
    })
    return best


# ---------------------------------------------------------------------------
# selftest — the 2023PSB-0530 shape: an investigation cam is talkier than the
# incident, so convergence (not raw salience) must win, and pick the OD time.
# ---------------------------------------------------------------------------

class _S:
    def __init__(self, start_epoch): self.start_epoch = start_epoch


def _selftest() -> int:
    base = dt.datetime(2023, 10, 24, tzinfo=dt.timezone.utc)
    def ep(h, m, s=0): return (base + dt.timedelta(hours=h, minutes=m, seconds=s)).timestamp()
    # cams: 0=BWC-2(none) 1=BWC-3a(20:45 OD) 2=BWC-3c(23:09 invest) 3=BWC-4(17:42 seize) 4=BWC-5(20:45 OD)
    sources = [_S(None), _S(ep(20, 45, 9)), _S(ep(23, 9, 7)), _S(ep(17, 42, 32)), _S(ep(20, 45, 51))]
    km = ([{"source_idx": 1, "timestamp_sec": i * 5, "importance": "high"} for i in range(3)]
          + [{"source_idx": 2, "timestamp_sec": i * 5, "importance": "critical"} for i in range(9)]  # talky investigation
          + [{"source_idx": 3, "timestamp_sec": i * 5, "importance": "high"} for i in range(2)]
          + [{"source_idx": 4, "timestamp_sec": i * 5, "importance": "critical"} for i in range(6)])
    verdict = {"key_moments": km}
    docs = [{"clip_directions": [{"kind": "video", "ref": "Axon Body 3 Video 2023-10-24-2046 PC 832"}]}]
    a = derive_anchor([], sources, verdict, docs)
    assert a is not None, "should derive an anchor"
    got = a["iso"]
    assert got == "2023-10-24 20:45:09", f"want OD 20:45:09 (convergence), got {got} (naive salience would give 23:09)"
    assert a["confidence"] == "high", f"2 cams + doc confirm -> high, got {a['confidence']}"
    assert a["doc_confirmed"], "IA report cites 2046 -> should confirm"
    # No-signal guard: too few moments -> None (keep the default anchor).
    assert derive_anchor([], sources, {"key_moments": km[:1]}) is None
    print("incident_anchor selftest: OK  ->", got, a["confidence"], a["rationale"])
    return 0


if __name__ == "__main__":
    import sys
    if "--selftest" in sys.argv:
        raise SystemExit(_selftest())
    print("usage: python incident_anchor.py --selftest")
