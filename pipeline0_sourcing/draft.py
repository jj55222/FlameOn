"""
draft.py — turn a FILE-verdict incident into a ready-to-submit FOIA write-up.

Deterministic templating (no LLM needed, so it's free + reliable): the request
letter is built from the incident's extracted fields + the state's statute cite,
deadline, and caveats pulled straight from state_access_profiles.json. The
operator reviews each write-up and submits it manually (MuckRock or agency portal).

Every letter is grounded ONLY in what the reporting says and asks for records by
category — it asserts no findings. Verify agency + jurisdiction before sending.
"""
from __future__ import annotations

from typing import Dict, List

# human-readable ask per record type
_RECORD_ASK = {
    "bodycam": "All body-worn camera (BWC) video and audio from every responding officer/deputy",
    "dashcam": "All in-car (dashcam) video and audio from responding units",
    "911": "All 911 call audio and recordings associated with the incident",
    "dispatch_cad": "The Computer-Aided Dispatch (CAD) event report / call-for-service log",
    "incident_report": "The incident/offense report, arrest report, and any supplemental reports",
    "internal_affairs": "Any use-of-force reports and, if applicable, internal-affairs / professional-standards records",
    "interrogation": "Any recorded interviews or interrogations of subjects/witnesses",
    "autopsy_me": "The autopsy / medical-examiner report (request from the ME/coroner if separate)",
}


def _records_requested(record_types: List[str]) -> List[str]:
    return [_RECORD_ASK[r] for r in record_types if r in _RECORD_ASK] or [_RECORD_ASK["incident_report"]]


def _deadline_line(prof: Dict) -> str:
    d = prof.get("response_deadline_days")
    note = prof.get("response_deadline_note", "")
    if d:
        return f"Your agency's statutory response window is ~{d} day(s). {note}".strip()
    return f"Please respond promptly as the statute requires. {note}".strip()


def build_request_letter(incident: Dict, prof: Dict) -> str:
    ext = incident["extract"]
    agency = ext.get("agency_name") or "[AGENCY — verify]"
    where = ", ".join(x for x in [ext.get("city", ""), ext.get("county", ""), ext.get("state", "")] if x) or "[location — verify]"
    when = ext.get("incident_date") or "[incident date — verify from reporting]"
    subjects = ", ".join(ext.get("subjects", [])) or "the individuals involved"
    statute = prof.get("statute", "the state public records act")
    records = _records_requested(ext.get("record_types_likely", []))
    record_block = "\n".join(f"  {i+1}. {r}." for i, r in enumerate(records))

    return f"""To the Public Records Officer, {agency}:

Under {statute}, I request copies of the following public records concerning the
incident reported on/around {when} in {where} involving {subjects}:

{record_block}

For each responsive video/audio file, I request the native file (not a screen
recording) and any associated log. If any portion is withheld, please cite the
specific statutory exemption and release all reasonably segregable portions.

{_deadline_line(prof)}

I am willing to pay reasonable duplication fees up to $[LIMIT]; please notify me
before incurring costs beyond that. I request a fee waiver where applicable, as
release serves the public interest in understanding law-enforcement conduct.

Please provide records electronically where possible. Thank you.

[REQUESTER NAME]
[CONTACT — email / mailing address]"""


def draft_incident(incident: Dict, state_profiles: Dict[str, Dict]) -> Dict:
    state = incident["jurisdiction"]["state"]
    prof = state_profiles.get(state, {})
    ext = incident["extract"]

    caveats = []
    if prof.get("residency_required"):
        caveats.append("⚠ RESIDENCY REQUIRED — this state accepts requests only from in-state "
                       "residents/media. File through an in-state requester or skip.")
    notes = prof.get("notes", "")
    if "dead-suspect" in notes.lower():
        caveats.append("⚠ TX dead-suspect loophole: non-conviction cases can be withheld; expect a "
                       "denial and be ready to narrow/appeal.")
    if "act 22" in notes.lower():
        caveats.append("⚠ PA Act 22: bodycam is a separate process — written request within 60 days "
                       "of the recording, by certified mail; court appeal only.")

    return {
        "incident_key": incident["incident_key"],
        "verdict": incident["verdict"],
        "foia_worth": incident["foia_worth"],
        "headline": incident["headline"],
        "why_it_matters": ext.get("reasoning", ""),
        "incident_type": ext.get("incident_type"),
        "ewu_shape": ext.get("ewu_shape"),
        "agency": ext.get("agency_name", ""),
        "jurisdiction": incident["jurisdiction"],
        "records_requested": _records_requested(ext.get("record_types_likely", [])),
        "statute": prof.get("statute", ""),
        "response_deadline_days": prof.get("response_deadline_days"),
        "verify_before_send": prof.get("verified") is False,
        "caveats": caveats,
        "sources": incident.get("urls", []),
        "request_letter": build_request_letter(incident, prof),
    }


def draft_incidents(scored: List[Dict], state_profiles: Dict[str, Dict],
                    include_watch: bool = False) -> List[Dict]:
    want = {"FILE", "WATCH"} if include_watch else {"FILE"}
    return [draft_incident(i, state_profiles) for i in scored if i["verdict"] in want]
