"""
score.py — FOIA-worthiness scorer.

    foia_worth = 100 x severity_component
                     x records_likely
                     x jurisdiction_access   (from discovered_cases/foia/state_access_profiles.json)
                     x filing_window         (recency vs. bodycam retention)

Gate on SEVERITY first (don't file on boring cases), then the jurisdiction. A
residency-required state (VA) can't be filed without an in-state requester, so it
is surfaced but flagged and capped to WATCH. All tunables are constants up top;
the function is pure (pass `today` for deterministic tests).
"""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parent
STATE_TABLE = ROOT.parent / "discovered_cases" / "foia" / "state_access_profiles.json"

# --- tunables ---
SEV_GATE = 50            # below this severity -> SKIP regardless (discovery-triage gate)
FILE_THRESHOLD = 35.0    # foia_worth >= -> FILE
WATCH_THRESHOLD = 18.0   # foia_worth in [WATCH, FILE) -> WATCH; below -> SKIP
UNKNOWN_STATE_ACCESS = 0.15   # non-US / unresolved jurisdiction
UNKNOWN_DATE_WINDOW = 0.7     # no incident date -> assume filable-but-verify

# which record types most raise "records likely exist + worth having"
_RECORD_WEIGHT = {
    "bodycam": 1.0, "dashcam": 0.6, "911": 0.7, "dispatch_cad": 0.4,
    "incident_report": 0.3, "internal_affairs": 0.9, "interrogation": 0.8,
    "autopsy_me": 0.5,
}


def load_state_profiles(path: Path = STATE_TABLE) -> Dict[str, Dict]:
    data = json.loads(Path(path).read_text())
    return {s["usps"]: s for s in data.get("states", [])}


def _records_component(record_types: List[str]) -> float:
    if not record_types:
        return 0.2
    # reward the best 2-3 record types (don't linearly punish short lists)
    weights = sorted((_RECORD_WEIGHT.get(r, 0.3) for r in record_types), reverse=True)
    top = weights[:3]
    return min(1.0, sum(top) / 2.2)   # bodycam+IA+911 ~= 1.0


def _filing_window(incident_date: str, today: Optional[date] = None) -> float:
    if not incident_date:
        return UNKNOWN_DATE_WINDOW
    try:
        d = datetime.strptime(incident_date[:10], "%Y-%m-%d").date()
    except ValueError:
        return UNKNOWN_DATE_WINDOW
    today = today or date.today()
    age = (today - d).days
    if age < 0:
        return 1.0
    if age <= 180:
        return 1.0
    if age >= 365:
        return 0.35          # past most non-flagged BWC retention — file fast or lose it
    return 1.0 - 0.65 * (age - 180) / 185.0


def score_incident(incident: Dict, state_profiles: Dict[str, Dict],
                   today: Optional[date] = None) -> Dict:
    ext = incident.get("extract", {})
    sev = int(ext.get("severity", 0))
    state = (ext.get("state") or "").upper()
    prof = state_profiles.get(state)

    sev_c = sev / 100.0
    rec_c = _records_component(ext.get("record_types_likely", []))
    contradiction_mult = 1.15 if ext.get("contradiction") else 1.0
    access = prof["jurisdiction_access"] if prof else UNKNOWN_STATE_ACCESS
    window = _filing_window(ext.get("incident_date", ""), today)

    worth = 100.0 * sev_c * rec_c * access * window * contradiction_mult
    worth = round(min(100.0, worth), 1)

    residency_block = bool(prof and prof.get("residency_required"))
    tier = prof.get("tier") if prof else None

    # verdict
    if not ext.get("is_crime_incident", True) or sev < SEV_GATE:
        verdict = "SKIP"
    elif worth >= FILE_THRESHOLD:
        verdict = "WATCH" if residency_block else "FILE"
    elif worth >= WATCH_THRESHOLD:
        verdict = "WATCH"
    else:
        verdict = "SKIP"

    return {
        **incident,
        "foia_worth": worth,
        "verdict": verdict,
        "score_components": {
            "severity": sev, "severity_component": round(sev_c, 3),
            "records_component": round(rec_c, 3),
            "jurisdiction_access": access, "filing_window": round(window, 3),
            "contradiction_mult": contradiction_mult,
        },
        "jurisdiction": {
            "state": state or "UNKNOWN", "tier": tier,
            "access": access, "residency_required": residency_block,
            "known": prof is not None,
        },
    }


def score_incidents(incidents: List[Dict], state_profiles: Optional[Dict] = None,
                    today: Optional[date] = None) -> List[Dict]:
    state_profiles = state_profiles or load_state_profiles()
    scored = [score_incident(i, state_profiles, today) for i in incidents]
    scored.sort(key=lambda x: x["foia_worth"], reverse=True)
    return scored


def main() -> int:
    ap = argparse.ArgumentParser(description="Score extracted incidents for FOIA-worthiness.")
    ap.add_argument("--incidents", required=True, help="extracted incidents JSON")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    incidents = json.loads(Path(a.incidents).read_text())
    scored = score_incidents(incidents)
    n_file = sum(1 for s in scored if s["verdict"] == "FILE")
    print(f"[score] {len(scored)} incidents -> {n_file} FILE, "
          f"{sum(1 for s in scored if s['verdict']=='WATCH')} WATCH, "
          f"{sum(1 for s in scored if s['verdict']=='SKIP')} SKIP")
    if a.out:
        Path(a.out).write_text(json.dumps(scored, indent=2))
        print(f"[score] wrote {a.out}")
    else:
        for s in scored[:15]:
            j = s["jurisdiction"]
            print(f"  {s['verdict']:5} {s['foia_worth']:5.1f} [{j['state']}] {s['headline'][:70]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
