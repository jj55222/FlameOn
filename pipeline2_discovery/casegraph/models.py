from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


SOURCE_ROLES = [
    "identity_source",
    "outcome_source",
    "claim_source",
    "artifact_source",
    "possible_artifact_source",
]

LEGACY_EVIDENCE_TYPES = [
    "bodycam",
    "interrogation",
    "court_video",
    "docket_docs",
    "dispatch_911",
]


@dataclass
class Jurisdiction:
    city: Optional[str] = None
    county: Optional[str] = None
    state: Optional[str] = None


@dataclass
class CaseInput:
    input_type: str
    raw_input: Dict[str, Any] = field(default_factory=dict)
    known_fields: Dict[str, Any] = field(default_factory=dict)
    missing_fields: List[str] = field(default_factory=list)
    candidate_queries: List[str] = field(default_factory=list)


@dataclass
class CaseIdentity:
    defendant_names: List[str] = field(default_factory=list)
    victim_names: List[str] = field(default_factory=list)
    agency: Optional[str] = None
    jurisdiction: Jurisdiction = field(default_factory=Jurisdiction)
    incident_date: Optional[str] = None
    case_numbers: List[str] = field(default_factory=list)
    charges: List[str] = field(default_factory=list)
    outcome_status: str = "unknown"
    identity_confidence: str = "low"
    identity_anchors: List[str] = field(default_factory=list)


@dataclass
class SourceRecord:
    source_id: str
    url: str
    title: str
    snippet: str = ""
    raw_text: str = ""
    source_type: str = "unknown"
    source_authority: str = "unknown"
    source_roles: List[str] = field(default_factory=list)
    api_name: Optional[str] = None
    discovered_via: str = ""
    retrieved_at: str = field(default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"))
    case_input_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    cost_estimate: float = 0.0
    confidence_signals: Dict[str, Any] = field(default_factory=dict)
    matched_case_fields: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ArtifactClaim:
    claim_id: str
    artifact_type: str
    claim_label: str
    claim_source_id: str
    claim_source_url: str
    supporting_snippet: str
    claim_confidence: float = 0.0
    risk_flags: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)


@dataclass
class VerifiedArtifact:
    artifact_id: str
    artifact_type: str
    artifact_url: str
    source_authority: str
    downloadable: bool
    format: str
    source_url: Optional[str] = None
    matched_case_fields: List[str] = field(default_factory=list)
    confidence: float = 0.0
    claim_source_url: Optional[str] = None
    duration_sec: Optional[float] = None
    requires_manual_download: bool = False
    verification_method: str = "public_url_pattern"
    risk_flags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Scores:
    identity_score: float = 0.0
    outcome_score: float = 0.0
    artifact_score: float = 0.0
    actionability_score: float = 0.0


@dataclass
class CasePacket:
    case_id: str
    input: CaseInput
    case_identity: CaseIdentity
    sources: List[SourceRecord] = field(default_factory=list)
    artifact_claims: List[ArtifactClaim] = field(default_factory=list)
    verified_artifacts: List[VerifiedArtifact] = field(default_factory=list)
    scores: Scores = field(default_factory=Scores)
    verdict: str = "HOLD"
    next_actions: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        for artifact in data["verified_artifacts"]:
            if artifact.get("source_url") is None:
                artifact["source_url"] = artifact.get("artifact_url", "")
            if artifact.get("claim_source_url") is None:
                artifact.pop("claim_source_url", None)
            artifact.pop("duration_sec", None)
            artifact.pop("requires_manual_download", None)
        return data
