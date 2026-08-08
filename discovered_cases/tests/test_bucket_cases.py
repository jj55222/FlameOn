"""Tests for bucket_cases.py — the promoted template-bucketizer.

Covers the pieces bucket_cases ADDS on top of the ewu_shortlist taxonomy: the
sexual-crime / in-custody severity patch, the [_+]->space normalization that lets
underscore-joined case_ids match, the COPA-style richness escalation, and an
end-to-end parity check that the whole registry reproduces the frozen bucket counts.
"""
from __future__ import annotations

import collections
import sys
from pathlib import Path

import pytest

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import bucket_cases as BC  # noqa: E402


def _file(name, type_, url="http://ex/x"):
    return {"name": name, "type": type_, "url": url}


def _bundle(files, **kw):
    b = {"case_id": kw.pop("case_id", "c"), "title": kw.pop("title", ""),
         "agency": kw.pop("agency", ""), "source": kw.pop("source", ""),
         "tier": kw.pop("tier", "B"), "files": files}
    b.update(kw)
    return b


# --- severity patch (sexual-crime / in-custody the ewu rubric under-scores) --

def test_severity_patch_tiers():
    assert BC.severity_ext("child sexual abuse of a minor")[0] == 95
    assert BC.severity_ext("in custody death investigation")[0] == 92
    assert BC.severity_ext("sexual assault by an officer")[0] == 78
    # fall-through to ewu.severity_of untouched
    assert BC.severity_ext("officer involved shooting")[1] == "officer-involved-shooting"
    assert BC.severity_ext("routine records request")[1] == "unspecified"


def test_severity_patch_highest_tier_wins():
    sev, label = BC.severity_ext("child sexual abuse and a shooting")
    assert sev == 95 and label == "child-sexual-abuse"   # CSA(95) beats OIS(80)


# --- [_+] -> space normalization (case_ids join tokens with '_') ------------

def test_normalization_lets_underscore_phrases_match():
    assert BC.severity_ext(BC.normalize_blob({"case_id": "sustained_sexual_assault"}))[0] == 78
    assert BC.severity_ext(BC.normalize_blob({"case_id": "longbeach_icd2020_001"}))[0] == 92
    assert "shots fired" in BC.normalize_blob({"title": "shots+fired+call"}).lower()


def test_icd_digit_suffix_needs_normalization():
    # proves the normalization is load-bearing: the raw underscore blob misses,
    # the normalized one hits (\bicd\s?\d can't anchor against a '_' word-char).
    assert BC.RE_CUSTODY.search("longbeach_icd2020_001") is None
    assert BC.RE_CUSTODY.search("longbeach icd2020 001") is not None


# --- COPA-style richness escalation -----------------------------------------

def test_richness_escalation_promotes_docheavy_unspecified_to_solver():
    files = [_file(f"clip{i}.mp4", "video") for i in range(5)] + [_file("report.pdf", "documents")]
    row = BC.bucket_one(_bundle(files, title="Chicago COPA Log 2020-1234", source="chicago_copa"))
    assert row["stakes"]["category"] == "unspecified"    # no severity word in the title
    assert row["bucket"] == "police_solver"              # escalated by doc + 5-angle richness


def test_richness_escalation_not_fired_without_enough_footage():
    files = [_file("clip1.mp4", "video"), _file("clip2.mp4", "video"), _file("report.pdf", "documents")]
    row = BC.bucket_one(_bundle(files, title="Chicago COPA Log 2020-1234", source="chicago_copa"))
    assert row["bucket"] == "rawwalk"                    # footage < 5 -> not escalated


def test_doconly_unspecified_is_hold_thin():
    row = BC.bucket_one(_bundle([_file("report.pdf", "documents")], title="records log"))
    assert row["bucket"] == "hold_thin"


# --- bucket tree sanity -----------------------------------------------------

def test_flagship_requires_interrogation_doc_and_footage():
    files = [_file("bodycam1.mp4", "bodycam"), _file("interview.mp4", "interrogation"),
             _file("report.pdf", "documents")]
    row = BC.bucket_one(_bundle(files, title="Homicide investigation", source="muckrock"))
    assert row["bucket"] == "flagship_ewu"


def test_imports_ewu_taxonomy_module():
    assert BC.E.__name__ == "ewu_shortlist"
    assert callable(BC.E.severity_of) and callable(BC.E.classify_bundle)


# --- end-to-end parity: reproduce the frozen .tmp bucket counts -------------

def test_reproduces_frozen_bucket_counts():
    if not BC.REGISTRY.exists():
        pytest.skip("CASE_BUNDLE_AGG.json not present")
    rows = BC.bucketize_registry(BC.REGISTRY)
    c = collections.Counter(r["bucket"] for r in rows)
    assert len(rows) == 3413
    assert (c["flagship_ewu"], c["police_solver"], c["rawwalk"], c["hold_thin"]) == (103, 686, 280, 2344)
