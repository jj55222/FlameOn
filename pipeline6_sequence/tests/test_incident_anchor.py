"""Auto incident-anchor: convergence×salience must beat raw salience.

The 2023PSB-0530 trap — the talky 23:09 investigation cam carries MORE
transcript salience than the chaotic 20:45 overdose, so a naive salience-argmax
anchors on the wrong scene and plays the case backwards. The anchor must instead
weight by camera CONVERGENCE (the incident is where independent cameras fire at
once) and prefer the earlier of the top clusters.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import incident_anchor as ia  # noqa: E402


class _S:
    def __init__(self, start_epoch):
        self.start_epoch = start_epoch


def _ep(h, m, s=0):
    return (dt.datetime(2023, 10, 24, tzinfo=dt.timezone.utc)
            + dt.timedelta(hours=h, minutes=m, seconds=s)).timestamp()


def _case():
    # 0=BWC-2(none) 1=OD-a(20:45) 2=invest(23:09, talkiest) 3=seize(17:42) 4=OD-b(20:45)
    sources = [_S(None), _S(_ep(20, 45, 9)), _S(_ep(23, 9, 7)), _S(_ep(17, 42, 32)), _S(_ep(20, 45, 51))]
    km = ([{"source_idx": 1, "timestamp_sec": i * 5, "importance": "high"} for i in range(3)]
          + [{"source_idx": 2, "timestamp_sec": i * 5, "importance": "critical"} for i in range(9)]
          + [{"source_idx": 3, "timestamp_sec": i * 5, "importance": "high"} for i in range(2)]
          + [{"source_idx": 4, "timestamp_sec": i * 5, "importance": "critical"} for i in range(6)])
    return sources, {"key_moments": km}


def test_convergence_beats_raw_salience():
    sources, verdict = _case()
    a = ia.derive_anchor([], sources, verdict)
    # two converging cams at 20:45 win over the single talkier cam at 23:09
    assert a is not None and a["iso"] == "2023-10-24 20:45:09"
    assert a["cameras"] == 2


def test_document_timestamp_raises_confidence():
    sources, verdict = _case()
    docs = [{"clip_directions": [{"kind": "video",
                                  "ref": "Axon Body 3 Video 2023-10-24-2046 PC 832"}]}]
    a = ia.derive_anchor([], sources, verdict, docs)
    assert a["doc_confirmed"] and a["confidence"] == "high"


def test_weak_signal_returns_none():
    sources, verdict = _case()
    assert ia.derive_anchor([], sources, {"key_moments": verdict["key_moments"][:1]}) is None


def test_parse_doc_timestamps():
    ts = ia.parse_doc_timestamps([{"clip_directions": [
        {"ref": "Axon Body 3 Video 2023-10-24 1743 X6033923Q"},
        {"ref": "2023-10-24-2046 PC 832"}]}])
    hhmm = sorted(dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%H%M") for t in ts)
    assert hhmm == ["1743", "2046"]
