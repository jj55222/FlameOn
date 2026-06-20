"""Zero-network tests for doc_extract — the multi-format case-doc miner.

Exercises both dispatch branches with small templated page fixtures (no PDF,
no OCR, no network): the Blueteam Use-of-Force FORM and the IA NARRATIVE
report. Asserts the disposition, clip_directions, and outcome card.
"""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import doc_extract as de  # noqa: E402


# A minimal Blueteam UoF form (labeled fields, not a narrative complaint).
UOF_PAGES = [{"page": 1, "text": (
    "Use Of Force Report   Blueteam\n"
    "Event / Report # 23-117201\n"
    "Date of Occurrence 04/18/2023 04/18/2023 09:44\n"
    "Deputy Sheriff VASQUEZ SONNY\n"
    "Addresses 800 Howe Ave Sacramento CA 95825\n"
    "Confirmed Sharp Weapon\n"
    "Reason For Using Force\n Apprehension Resistance\n"
    "Subject/Arrestee  Citizen Arrested Yes\n"
    "Charges: PC459 PC417 PC148\n"
    "Camera: Deputy Vasquez's BWC Time: 0943-0948 Hours\n"
    "Additional notes follow.\n")}]

# A minimal IA narrative complaint (SUSTAINED finding + discipline + footage).
IA_PAGES = [
    {"page": 1, "text": (
        "Internal Affairs Investigation 2023PSB-0530\n"
        "Subject: Deputy Jane Morales\n"
        "Date: October 24, 2023\n"
        "STATEMENT OF PROBLEM: Deputy Morales is alleged to have removed seized "
        "currency from an evidence locker over several shifts without authorization.\n")},
    {"page": 2, "text": (
        "Allegation of Theft SUSTAINED\n"
        "The Department imposed termination following the Skelly review.\n"
        "Axon Body 3 video 2023-10-24 1743 depicts the deputy at the locker.\n"
        "CAD Event 23-998877 logged the response.\n")},
]


# --- dispatch + UoF form ----------------------------------------------------

def test_dispatch_picks_uof_form():
    d = de.extract(UOF_PAGES)
    assert d["doc_type"] == "uof_form"


def test_uof_core_fields():
    d = de.extract(UOF_PAGES)
    assert d["ia_case_number"] == "23-117201"
    assert d["doc_date"] == "04/18/2023"
    assert d["incident_time"] == "09:44"
    assert d["subject"] == "Vasquez Sonny"
    assert d["weapon"] == "Confirmed Sharp Weapon"
    assert "800 Howe Ave" in (d["location"] or "")
    assert d["citizen_arrested"] is True
    assert "PC 459" in d["charges"]


def test_uof_clip_direction_from_video_available_field():
    d = de.extract(UOF_PAGES)
    cds = d["clip_directions"]
    assert len(cds) == 1
    cd = cds[0]
    assert cd["kind"] == "bwc_window"
    assert "Vasquez" in cd["ref"]
    assert cd["window"] == "0943-0948 Hours"


def test_uof_outcome_card_is_supervisor_reviewed():
    d = de.extract(UOF_PAGES)
    oc = d["outcome_card"]
    assert "Use of Force" in oc["title"]
    assert "23-117201" in oc["title"]
    assert "supervisor-reviewed" in oc["subtitle"]


# --- IA narrative -----------------------------------------------------------

def test_dispatch_picks_ia_narrative():
    d = de.extract(IA_PAGES)
    assert d["doc_type"] == "ia_narrative"


def test_ia_disposition_sustained_with_discipline():
    d = de.extract(IA_PAGES)
    assert d["ia_case_number"] == "2023PSB-0530"
    findings = d["disposition"]["findings"]
    assert any(f["finding"] == "SUSTAINED" for f in findings)
    disc = [s.lower() for s in d["disposition"]["discipline_signals"]]
    assert any("terminat" in s or "skelly" in s for s in disc)


def test_ia_narrative_statement_of_problem_captured():
    d = de.extract(IA_PAGES)
    assert d["narrative"] is not None
    assert "evidence locker" in d["narrative"]["text"]


def test_ia_clip_directions_video_and_cad():
    d = de.extract(IA_PAGES)
    kinds = {c["kind"] for c in d["clip_directions"]}
    # high-value footage-facing pointers surface as clip_directions
    assert "video" in kinds
    assert "cad_event" in kinds


def test_ia_outcome_card_titles_sustained():
    d = de.extract(IA_PAGES)
    oc = d["outcome_card"]
    assert "SUSTAINED" in oc["title"]
    assert "2023PSB-0530" in oc["title"]


# --- robustness -------------------------------------------------------------

def test_empty_pages_do_not_crash():
    d = de.extract([{"page": 1, "text": ""}])
    assert d["clip_directions"] == []
    assert d["outcome_card"]["title"]  # falls back to a generic card
