"""Zero-network tests for doc_clips — the doc-directed clip projection.

Covers the pure logic that closes the D5 loop: parsing a document's wall-clock
window and projecting it onto each artifact's local [start,end] seconds using
the artifact's stamped start_iso.
"""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import doc_clips as dc  # noqa: E402


# --- parse_clock_window ----------------------------------------------------

def test_parse_hhmm_hours_format():
    assert dc.parse_clock_window("0943-0948 Hours") == (9 * 3600 + 43 * 60, 9 * 3600 + 48 * 60)


def test_parse_colon_and_dash_variants():
    want = (9 * 3600 + 43 * 60, 9 * 3600 + 48 * 60)
    assert dc.parse_clock_window("09:43-09:48") == want
    assert dc.parse_clock_window("0943 – 0948 hrs") == want   # en dash
    assert dc.parse_clock_window("0943 — 0948") == want       # em dash


def test_parse_single_time_expands_to_span():
    s = 9 * 3600 + 44 * 60
    assert dc.parse_clock_window("0944", single_span_sec=300) == (s, s + 300)


def test_parse_rejects_garbage_and_out_of_range():
    assert dc.parse_clock_window("") is None
    assert dc.parse_clock_window("garbage") is None
    assert dc.parse_clock_window("99:99-10:00") is None      # bad hour/min
    # single-digit minutes are malformed for wall-clock OCR -> no match
    assert dc.parse_clock_window("9:5-9:9") is None


# --- doc_windows_for_artifact ----------------------------------------------

def _cd(window):
    return [{"kind": "bwc_window", "ref": "Deputy X's BWC", "window": window}]


def test_window_projected_onto_overlapping_artifact():
    # Recording starts 09:44:49, lasts 497s (ends ~09:53:06).
    # Doc window 09:43-09:48 with no pad -> [0, (09:48 - 09:44:49)] = [0, 191].
    win = dc.doc_windows_for_artifact(_cd("0943-0948 Hours"),
                                      "2023-04-18 09:44:49", 497.0,
                                      pad_pre=0.0, pad_post=0.0)
    assert win == [[0.0, 191.0]]


def test_padding_extends_but_clips_to_bounds():
    win = dc.doc_windows_for_artifact(_cd("0943-0948 Hours"),
                                      "2023-04-18 09:44:49", 497.0,
                                      pad_pre=90.0, pad_post=90.0)
    # start clips to 0 (window begins before the recording); end = 191 + 90.
    assert win == [[0.0, 281.0]]


def test_non_overlapping_artifact_drops_out():
    # A camera that only recorded at 16:16 has nothing in a 09:43-09:48 window.
    assert dc.doc_windows_for_artifact(_cd("0943-0948 Hours"),
                                       "2023-04-18 16:16:05", 600.0) == []


def test_window_end_inside_recording_clips_at_window_not_eof():
    # Recording starts 09:43:24 and runs 321s (to ~09:48:45). The 09:43-09:48
    # window ends at offset (09:48:00 - 09:43:24) = 276s, before end-of-file.
    win = dc.doc_windows_for_artifact(_cd("0943-0948 Hours"),
                                      "2023-04-18 09:43:24", 321.0,
                                      pad_pre=0.0, pad_post=0.0)
    assert win == [[0.0, 276.0]]


def test_missing_start_iso_returns_empty():
    assert dc.doc_windows_for_artifact(_cd("0943-0948 Hours"), None, 500.0) == []
    assert dc.doc_windows_for_artifact(_cd("0943-0948 Hours"), "no-time-here", 500.0) == []


def test_midnight_wrap_window_rejected():
    # 23:50-00:10 parses to end < start -> rejected, not mis-projected.
    assert dc.doc_windows_for_artifact(_cd("2350-0010"),
                                       "2023-04-18 23:55:00", 600.0) == []


def test_multiple_directions_merge_when_overlapping():
    cds = [{"window": "0943-0945"}, {"window": "0944-0948"}]
    win = dc.doc_windows_for_artifact(cds, "2023-04-18 09:43:00", 600.0,
                                      pad_pre=0.0, pad_post=0.0)
    # 09:43-09:45 -> [0,120], 09:44-09:48 -> [60,300]; overlap merges to [0,300].
    assert win == [[0.0, 300.0]]


def test_ref_used_when_window_absent():
    cds = [{"ref": "0943-0948"}]   # window missing, time hides in ref
    win = dc.doc_windows_for_artifact(cds, "2023-04-18 09:43:00", 600.0,
                                      pad_pre=0.0, pad_post=0.0)
    assert win == [[0.0, 300.0]]
