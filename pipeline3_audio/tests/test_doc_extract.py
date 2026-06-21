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


# A fuller UoF bundle (mirrors the real Sacramento #23-117201 OCR): the suspect's
# charges live in a structured CHARGE SUMMARY and the CAD "Clear remarks"
# disposition; the rest of the bundle is thick with penal-code citations that are
# NOT charges — redaction stamps (PC 832.7 personnel-records, PC 13300-13302/13800
# DOJ records-act), a vandalism count named only in narrative prose (PC 594), CAD
# field codes (PC 08), and a sibling call's disposition-only remark. A global "PC"
# scan swept all of those up; only PC 459 / PC 417 / PC 148 may survive.
UOF_NOISE_PAGES = [
    {"page": 1, "text": (
        "Use Of Force Report   Blueteam\n"
        "Event / Report # 23-117201\n"
        "Date of Occurrence 04/18/2023 04/18/2023 09:44\n"
        "Deputy Sheriff VASQUEZ SONNY\n"
        "This record is exempt under PC 832.7 (b)(7), PC 13300.\n")},
    {"page": 2, "text": (
        "DA/COURT/LAW ENFORCEMENT HARDCOPY\nCHARGE SUMMARY\nCHARGE # 1\n"
        "Offense RESIST/DELAY/OBSTRUCT PEACE OFFICER  - COMPLETED\n"
        "Charge Statute PC  148(A)(1)\nCharge Severity MISDEMEANOR\nCHARGE # 2\n"
        "Offense BURGLARY:SCHOOL  - COMPLETED\n"
        "Charge Statute PC  459\nCharge Severity FELONY\n"
        "PC 832.7 (b)(7),\nPC 13300\nPC 832.7 (b)(6)(B)\n")},
    {"page": 3, "text": (
        "CRIME REPORT NARRATIVE\nThe suspect forced entry into the business "
        "(PC 459// Burglary) and attempted to force the cash registers "
        "(PC 594// Felony Vandalism), then resisted officers (PC 148).\n")},
    {"page": 4, "text": (
        "Reporting officers: -HEROLD, LOGAN C [842D]\n"
        "Clear remarks: PC459, X2 148A(1), 417 UNFOUNDED, S1\n"
        "PC 832.7 (b)(7), PC 13300\nPC 832.7 (b)(6)(B), PC 832.7 (b)(7)\n")},
    {"page": 5, "text": (   # sibling CAD call — disposition-only remark, no charges
        "CALL: SD23-117138 Status: CLEARED\n"
        "Clear remarks: SUBJ LEFT NO THREATS MADE NO CRIME 4841 Pc 13300\n"
        "08 Eba02 OPC 13300 PC 13301 PC 13302 PC 13800 field codes\n")},
    {"page": 6, "text": (   # CAD log mega-line: REM- charges THEN inline redaction noise
        "FINAL-459BUS REM-PC459 X2 148A (1) , 417 UNFOUNDED , S1 APPREHENDED BY "
        "SHERIFF K9 IN CALL 11C1 Pc 832.7 () PC 13300 Pc 13301 PC 13302 PC 13800\n")},
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


def test_uof_charges_read_labeled_fields_not_global_pc_scan():
    """Charges come only from labeled fields (CHARGE SUMMARY 'Charge Statute',
    'Clear remarks'/REM disposition) — never a global PC scan. The bundle's
    redaction stamps, records-act citations, CAD field codes, and a narrative-
    only vandalism count must NOT be mistaken for charges."""
    d = de.extract(UOF_NOISE_PAGES)
    assert d["doc_type"] == "uof_form"
    # PC459 (burglary), PC417 (brandishing), PC148 (resisting) — and nothing else.
    assert set(d["charges"]) == {"PC 148", "PC 417", "PC 459"}
    # explicit guards against the exact noise a global scan produced:
    for noise in ("PC 832", "PC 13300", "PC 13301", "PC 13302", "PC 13800",
                  "PC 594", "PC 08", "PC 09", "PC 4841"):
        assert noise not in d["charges"], f"penal-code noise leaked: {noise}"


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
