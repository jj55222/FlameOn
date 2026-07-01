"""
extract.py — turn a clustered incident into a structured record.

Given the headline + snippets, extract the fields P0 needs to score and draft:
agency, jurisdiction (state), incident type, severity, likely record types, and
the EWU worth-shape. Two modes:

  * LIVE  — one cheap LLM call (OpenRouter, via pipeline4_scoring/llm_backends).
  * MOCK  — deterministic keyword heuristics, no network/key. Good enough to run
            the whole pipeline offline and to unit-test scoring/drafting.

The LLM is asked ONLY to read what the reporting says + reason about what records
a US LE agency of that type would generate. It never invents facts we assert as
true downstream — scoring/drafting treat these as leads to verify, not findings.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent / "pipeline4_scoring"))

RECORD_TYPES = ["bodycam", "dashcam", "911", "dispatch_cad", "incident_report",
                "internal_affairs", "interrogation", "autopsy_me"]

_SYSTEM = """You are a triage analyst for a true-crime documentary shop that files public-records
(FOIA) requests. You read early news reporting about an incident and extract a STRUCTURED record.

You judge worth by a model learned from what the EWU Bodycam and Dr. Insanity channels publish:
worth = SEVERITY (the gate: did someone die / grave harm / predator?) x STORY-SHAPE (small->big,
reveal, comeuppance) x HUMAN-PROXIMITY. Contradiction (footage vs. official account) is a MULTIPLIER.

Return ONLY a JSON object with these keys:
  is_crime_incident (bool)         - false for opinion/policy/roundup articles
  incident_type (string)           - officer_involved_shooting|in_custody_death|homicide|use_of_force|
                                      domestic_violence|assault|pursuit|sexual_offense|other
  severity (int 0-100)             - death/murder/predator high; property/minor low
  ewu_shape (string)               - death|discovery|domestic|comeuppance|predator|contradiction|none
  agency_name (string)             - the responding LE agency, best guess ("" if unknown)
  agency_type (string)             - municipal_pd|county_sheriff|state_police|other
  city (string) county (string) state (string, 2-letter USPS, "" if not US/unknown)
  incident_date (string)           - YYYY-MM-DD if stated, else ""
  subjects (array of strings)      - named people (suspect/victim/officer) if given
  record_types_likely (array)      - subset of: bodycam,dashcam,911,dispatch_cad,incident_report,
                                      internal_affairs,interrogation,autopsy_me
  contradiction (bool)             - reporting hints footage/witnesses conflict with the official account
  reasoning (string)               - 1-2 sentences: why this severity + which records to chase
Be conservative on severity; do not inflate. Output JSON only, no prose."""


def extract_incident_llm(incident: Dict, backend, max_tokens: int = 900) -> Dict:
    user = (
        f"HEADLINE: {incident.get('headline','')}\n"
        f"REPORTS: {incident.get('n_reports',1)} outlet(s): {', '.join(incident.get('sources',[])[:6])}\n"
        f"SNIPPETS:\n" + "\n".join(f"- {s}" for s in incident.get("snippets", [])[:5]) +
        "\n\nExtract the JSON record."
    )
    raw = backend.complete(system=_SYSTEM, user=user, max_tokens=max_tokens, temperature=0.1)
    try:
        data = json.loads(raw)
    except Exception as e:  # noqa: BLE001
        data = {"is_crime_incident": True, "_parse_error": f"{type(e).__name__}", "reasoning": raw[:200]}
    return _coerce(data)


# ---- MOCK (offline heuristic) -------------------------------------------------

_STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}


def extract_incident_mock(incident: Dict) -> Dict:
    text = " ".join([incident.get("headline", "")] + incident.get("snippets", [])).lower()

    def has(*ws): return any(w in text for w in ws)

    if has("killed", "fatal", "dead", "dies", "died", "homicide", "murder", "in-custody death", "in custody death"):
        severity, itype = 85, "homicide"
    elif has("officer involved shooting", "officer-involved", "deputy involved", "police shooting"):
        severity, itype = 80, "officer_involved_shooting"
    elif has("stabbing", "shooting", "shot"):
        severity, itype = 70, "assault"
    elif has("use of force", "beating", "excessive force", "assault"):
        severity, itype = 55, "use_of_force"
    elif has("pursuit", "chase", "crash", "arrest", "charged"):
        severity, itype = 40, "pursuit"
    else:
        severity, itype = 20, "other"

    if has("in custody", "in-custody", "jail", "detainee"):
        itype = "in_custody_death" if severity >= 70 else itype

    state = ""
    for name, code in _STATE_NAMES.items():
        if re.search(rf"\b{name}\b", text):
            state = code
            break
    if not state:
        m = re.search(r"\b([A-Z]{2})\b", incident.get("headline", ""))
        if m and m.group(1) in _STATE_NAMES.values():
            state = m.group(1)

    am = re.search(r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Police Department|Police|Sheriff'?s? Office|Sheriff'?s? Department|County Sheriff))",
                   incident.get("headline", "") + " " + " ".join(incident.get("snippets", [])))
    agency = am.group(1).strip() if am else ""
    agency_type = ("county_sheriff" if "sheriff" in agency.lower()
                   else "municipal_pd" if agency else "other")

    records = ["incident_report"]
    if has("body camera", "bodycam", "body-worn", "body cam"):
        records.append("bodycam")
    if has("911", "dispatch"):
        records += ["911", "dispatch_cad"]
    if itype in ("officer_involved_shooting", "in_custody_death", "use_of_force"):
        records.append("internal_affairs")
    if severity >= 80:
        records.append("autopsy_me")

    shape = ("death" if severity >= 80 else
             "predator" if has("child", "minor", "sexual", "predator") else
             "domestic" if has("domestic", "wife", "husband", "partner") else "none")
    contradiction = has("contradict", "conflicting", "disputes", "but police say", "family disputes")

    return _coerce({
        "is_crime_incident": severity >= 40 or itype != "other",
        "incident_type": itype, "severity": severity, "ewu_shape": shape,
        "agency_name": agency, "agency_type": agency_type,
        "city": "", "county": "", "state": state, "incident_date": "",
        "subjects": [], "record_types_likely": sorted(set(records)),
        "contradiction": contradiction,
        "reasoning": f"[mock] {itype} sev~{severity}; chase {', '.join(sorted(set(records)))}.",
    })


def _coerce(d: Dict) -> Dict:
    out = {
        "is_crime_incident": bool(d.get("is_crime_incident", True)),
        "incident_type": str(d.get("incident_type", "other")),
        "severity": int(_num(d.get("severity", 0))),
        "ewu_shape": str(d.get("ewu_shape", "none")),
        "agency_name": str(d.get("agency_name", "")),
        "agency_type": str(d.get("agency_type", "other")),
        "city": str(d.get("city", "")), "county": str(d.get("county", "")),
        "state": str(d.get("state", ""))[:2].upper(),
        "incident_date": str(d.get("incident_date", "")),
        "subjects": list(d.get("subjects", []) or []),
        "record_types_likely": [r for r in (d.get("record_types_likely", []) or []) if r in RECORD_TYPES] or ["incident_report"],
        "contradiction": bool(d.get("contradiction", False)),
        "reasoning": str(d.get("reasoning", "")),
    }
    out["severity"] = max(0, min(100, out["severity"]))
    if "_parse_error" in d:
        out["_parse_error"] = d["_parse_error"]
    return out


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        m = re.search(r"\d+", str(v))
        return float(m.group()) if m else 0.0


def extract_incidents(incidents: List[Dict], mock: bool = True,
                      model: Optional[str] = None) -> List[Dict]:
    """Extract every incident. mock=True is offline; else one LLM call each."""
    backend = None
    if not mock:
        from _env import load_env
        load_env()
        from llm_backends import build_backend
        backend = build_backend(model or "google/gemini-3.1-flash-lite-preview")
    out = []
    for inc in incidents:
        ext = extract_incident_mock(inc) if mock else extract_incident_llm(inc, backend)
        out.append({**inc, "extract": ext})
    return out
