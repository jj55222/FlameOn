"""OUTCOME3 - mocked outcome corroboration executor.

Consumes OUTCOME2 plans plus caller-provided court/news/document
payloads. It extracts deterministic outcome signals from the mocked
payload text only; ambiguous or weak payloads remain unknown.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .outcome import OUTCOME_PATTERNS, OUTCOME_PRIORITY, STRONG_AUTHORITIES
from .outcome_seed_plan import OutcomeSeedPlan


@dataclass
class OutcomeCorroborationResult:
    case_id: int
    source_type: str
    source_authority: str
    extracted_outcome_status: str
    confidence: float
    supporting_snippet: str = ""
    risk_flags: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)
    execution_status: str = "completed"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def execute_mock_outcome_corroboration(
    plan: OutcomeSeedPlan,
    payload: Mapping[str, Any],
) -> OutcomeCorroborationResult:
    """Extract outcome status from a mocked corroboration payload.

    The executor never fetches the payload. The caller supplies text
    that came from a fixture or test harness.
    """

    source_type = str(payload.get("source_type") or "mock_outcome_source")
    source_authority = str(payload.get("source_authority") or "unknown")
    text = _payload_text(payload)
    matches = _matches(text)
    status = _choose_status(matches)

    risk_flags: List[str] = []
    next_actions: List[str] = []
    if not text:
        risk_flags.append("empty_outcome_payload")
    if status == "unknown":
        risk_flags.append("outcome_unknown")
        next_actions.append("Provide a stronger court, docket, official, or news payload with explicit outcome language.")
    if not _has_identity_anchor(plan, text):
        risk_flags.append("identity_anchor_missing")
        if status != "unknown":
            next_actions.append("Corroborate that the outcome source names the same case identity before locking outcome.")
    if len({match[0] for match in matches}) > 1:
        risk_flags.append("conflicting_outcome_signals")

    return OutcomeCorroborationResult(
        case_id=plan.case_id,
        source_type=source_type,
        source_authority=source_authority,
        extracted_outcome_status=status,
        confidence=_confidence(status, source_authority, text, plan),
        supporting_snippet=_snippet(text, matches[0][1] if matches else None),
        risk_flags=list(dict.fromkeys(risk_flags)),
        next_actions=list(dict.fromkeys(next_actions)),
        execution_status="completed" if text else "empty_payload",
    )


def outcome_corroboration_to_jsonable(result: OutcomeCorroborationResult) -> Dict[str, Any]:
    return result.to_dict()


def _payload_text(payload: Mapping[str, Any]) -> str:
    return " ".join(
        str(payload.get(field) or "")
        for field in ("title", "snippet", "raw_text", "text", "url")
        if payload.get(field)
    )


def _matches(text: str) -> List[Tuple[str, re.Match[str]]]:
    found: List[Tuple[str, re.Match[str]]] = []
    for status, patterns in OUTCOME_PATTERNS.items():
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                found.append((status, match))
                break
    return found


def _choose_status(matches: List[Tuple[str, re.Match[str]]]) -> str:
    if not matches:
        return "unknown"
    statuses = {status for status, _ in matches}
    if "dismissed" in statuses:
        return "dismissed"
    if "acquitted" in statuses:
        return "acquitted"
    return max(statuses, key=lambda status: OUTCOME_PRIORITY[status])


def _confidence(status: str, source_authority: str, text: str, plan: OutcomeSeedPlan) -> float:
    if status == "unknown":
        return 0.0
    confidence = 0.58
    if source_authority in STRONG_AUTHORITIES:
        confidence += 0.18
    if _has_identity_anchor(plan, text):
        confidence += 0.14
    if status in {"sentenced", "dismissed", "acquitted"}:
        confidence += 0.06
    return round(min(confidence, 0.95), 2)


def _has_identity_anchor(plan: OutcomeSeedPlan, text: str) -> bool:
    normalized = _normalize(text)
    names = [piece.strip() for piece in plan.title.split(",") if piece.strip()]
    has_name = any(_normalize(name) in normalized for name in names if name)
    location_values = [plan.state, plan.agency, *plan.jurisdiction.split(",")]
    has_location = any(_normalize(value) in normalized for value in location_values if value)
    return bool(has_name and has_location)


def _normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _snippet(text: str, match: Optional[re.Match[str]]) -> str:
    if not text:
        return ""
    if match is None:
        return re.sub(r"\s+", " ", text[:240]).strip()
    start = max(0, match.start() - 100)
    end = min(len(text), match.end() + 140)
    return re.sub(r"\s+", " ", text[start:end]).strip()
