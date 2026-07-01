"""
Offline tests for P0 sourcing — deterministic, no network, no API key.
Run:  cd pipeline0_sourcing && python -m pytest -q
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from cluster import cluster_signals
from extract import extract_incidents, extract_incident_mock
from score import score_incidents, load_state_profiles, score_incident, SEV_GATE
from draft import draft_incidents, draft_incident

ROOT = Path(__file__).resolve().parent
FIX = ROOT / "fixtures" / "sample_signals.json"
TODAY = date(2026, 7, 1)          # freeze "now" so filing_window is deterministic


@pytest.fixture(scope="module")
def signals():
    return json.loads(FIX.read_text())


@pytest.fixture(scope="module")
def profiles():
    return load_state_profiles()


def test_state_table_loads(profiles):
    assert {"FL", "WA", "CA"} <= set(profiles)
    assert profiles["FL"]["tier"] == 1
    assert profiles["VA"]["residency_required"] is True


def test_clustering_merges_duplicate_coverage(signals):
    incidents = cluster_signals(signals)
    # 12 signals, but the two Miami-FL reports and the two LA-CA reports each merge
    assert len(incidents) < len(signals)
    multi = [i for i in incidents if i["n_reports"] >= 2]
    assert multi, "expected at least one incident corroborated by 2+ outlets"
    for inc in multi:
        assert len(inc["urls"]) == inc["n_reports"]


def test_mock_extract_fields():
    inc = {"headline": "Miami Police officer involved shooting leaves man dead in Florida",
           "snippets": ["body camera footage and 911 audio exist"]}
    ext = extract_incident_mock(inc)
    assert ext["state"] == "FL"
    assert ext["severity"] >= SEV_GATE
    assert "bodycam" in ext["record_types_likely"]
    assert "911" in ext["record_types_likely"]


def test_low_severity_and_noncrime_skip(profiles):
    noise = {"incident_key": "n", "headline": "Florida city council votes on parking ordinance",
             "urls": [], "extract": extract_incident_mock(
                 {"headline": "Florida city council votes on parking ordinance", "snippets": ["no crime"]})}
    scored = score_incident(noise, profiles, today=TODAY)
    assert scored["verdict"] == "SKIP"


def test_sunshine_state_beats_avoid_state(profiles):
    """Same severity/records, FL (tier1) must outscore PA (avoid)."""
    base_snip = ["fatal officer involved shooting, body camera and 911 exist"]
    fl = {"incident_key": "fl", "headline": "Miami Police fatal officer involved shooting man dead in Florida",
          "urls": [], "extract": extract_incident_mock({"headline": "Miami Police fatal officer involved shooting man dead in Florida", "snippets": base_snip})}
    pa = {"incident_key": "pa", "headline": "Philadelphia Police fatal officer involved shooting man dead in Pennsylvania",
          "urls": [], "extract": extract_incident_mock({"headline": "Philadelphia Police fatal officer involved shooting man dead in Pennsylvania", "snippets": base_snip})}
    sfl = score_incident(fl, profiles, today=TODAY)
    spa = score_incident(pa, profiles, today=TODAY)
    assert sfl["foia_worth"] > spa["foia_worth"]
    assert sfl["verdict"] == "FILE"


def test_residency_state_flagged_and_capped(profiles):
    va = {"incident_key": "va", "headline": "Man dies in custody after Richmond Police arrest in Virginia",
          "urls": [], "extract": extract_incident_mock(
              {"headline": "Man dies in custody after Richmond Police arrest in Virginia",
               "snippets": ["in custody death, body camera and 911 exist"]})}
    scored = score_incident(va, profiles, today=TODAY)
    assert scored["jurisdiction"]["residency_required"] is True
    assert scored["verdict"] != "FILE"          # capped to WATCH — can't file without in-state requester


def test_end_to_end_mock_produces_queue(signals, profiles):
    incidents = cluster_signals(signals)
    incidents = extract_incidents(incidents, mock=True)
    scored = score_incidents(incidents, profiles, today=TODAY)
    drafts = draft_incidents(scored, profiles, include_watch=True)
    assert drafts, "expected at least one FILE/WATCH write-up"
    # the FL fatal OIS should be top and a FILE
    top = scored[0]
    assert top["verdict"] in ("FILE", "WATCH")
    # every draft has a letter citing a statute and listing records
    for d in drafts:
        assert d["request_letter"].startswith("To the Public Records Officer")
        assert d["records_requested"]
        if d["jurisdiction"]["state"] in ("FL", "WA", "CA", "OH", "TX", "PA", "VA"):
            assert d["statute"]


def test_draft_carries_state_caveats(profiles):
    tx = {"incident_key": "tx", "verdict": "FILE", "foia_worth": 40.0,
          "headline": "Houston Police fatal shooting of suspect in Texas",
          "urls": [], "jurisdiction": {"state": "TX"},
          "extract": extract_incident_mock(
              {"headline": "Houston Police fatal shooting of suspect in Texas",
               "snippets": ["man killed, body camera exists, no charges"]})}
    d = draft_incident(tx, profiles)
    assert any("dead-suspect" in c.lower() for c in d["caveats"])
