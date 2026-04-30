"""CaseGraph rebuild surface for Pipeline 2.

This package is intentionally separate from the deprecated flat-source
`research.py` loop. It starts with deterministic, no-API normalization and
export adapters, then later resolver experiments can plug into the same models.
"""

from .adapters import (
    export_legacy_evaluate_result,
    export_p2_to_p3,
    export_p2_to_p4,
    export_p2_to_p5,
)
from .assembly import (
    StructuredAssemblyResult,
    WeakInputAssemblyResult,
    assemble_structured_case_packet,
    assemble_weak_input_case_packet,
)
from .claim_extraction import ClaimExtractionResult, extract_artifact_claims
from .connectors import ConnectorError, ConnectorUnavailable, CourtListenerConnector, DocumentCloudConnector, MockSourceConnector, MuckRockConnector, SourceConnector, YouTubeConnector, validate_connector_source_record
from .identity import IdentityResolution, resolve_identity
from .inputs import (
    StructuredInputParseResult,
    YouTubeInputParseResult,
    parse_fatal_encounters_case_input,
    parse_mapping_police_violence_case_input,
    parse_wapo_uof_case_input,
    parse_youtube_case_input,
)
from .models import (
    ArtifactClaim,
    CaseIdentity,
    CaseInput,
    CasePacket,
    Jurisdiction,
    Scores,
    SourceRecord,
    VerifiedArtifact,
)
from .ledger import (
    COST_PER_CALL_USD,
    DEFAULT_API_CALLS,
    RunLedgerEntry,
    aggregate_ledger,
    append_ledger_entry,
    build_run_ledger_entry,
    estimate_cost,
    normalize_api_calls,
)
from .outcome import OutcomeResolution, resolve_outcome
from .reporting import build_actionability_report
from .query_planner import (
    ConnectorQueryPlan,
    PlannedQuery,
    QueryPlanResult,
    plan_queries_from_structured_result,
    plan_queries_from_youtube_result,
)
from .resolvers import (
    CourtListenerDocumentResolution,
    DocumentCloudFileResolution,
    MuckRockFileResolution,
    resolve_courtlistener_documents,
    resolve_documentcloud_files,
    resolve_muckrock_released_files,
)
from .routers import route_manual_defendant_jurisdiction
from .scoring import ActionabilityResult, score_case_packet

__all__ = [
    "ActionabilityResult",
    "ArtifactClaim",
    "CaseIdentity",
    "CaseInput",
    "CasePacket",
    "WeakInputAssemblyResult",
    "ClaimExtractionResult",
    "ConnectorQueryPlan",
    "COST_PER_CALL_USD",
    "ConnectorError",
    "ConnectorUnavailable",
    "CourtListenerConnector",
    "CourtListenerDocumentResolution",
    "DEFAULT_API_CALLS",
    "DocumentCloudConnector",
    "DocumentCloudFileResolution",
    "Jurisdiction",
    "IdentityResolution",
    "MockSourceConnector",
    "MuckRockFileResolution",
    "MuckRockConnector",
    "OutcomeResolution",
    "PlannedQuery",
    "QueryPlanResult",
    "RunLedgerEntry",
    "Scores",
    "SourceRecord",
    "StructuredAssemblyResult",
    "StructuredInputParseResult",
    "SourceConnector",
    "VerifiedArtifact",
    "YouTubeConnector",
    "YouTubeInputParseResult",
    "aggregate_ledger",
    "append_ledger_entry",
    "assemble_structured_case_packet",
    "assemble_weak_input_case_packet",
    "build_actionability_report",
    "build_run_ledger_entry",
    "estimate_cost",
    "export_legacy_evaluate_result",
    "export_p2_to_p3",
    "export_p2_to_p4",
    "export_p2_to_p5",
    "extract_artifact_claims",
    "normalize_api_calls",
    "parse_fatal_encounters_case_input",
    "parse_mapping_police_violence_case_input",
    "parse_wapo_uof_case_input",
    "parse_youtube_case_input",
    "plan_queries_from_structured_result",
    "plan_queries_from_youtube_result",
    "resolve_courtlistener_documents",
    "resolve_documentcloud_files",
    "route_manual_defendant_jurisdiction",
    "resolve_identity",
    "resolve_muckrock_released_files",
    "resolve_outcome",
    "score_case_packet",
    "validate_connector_source_record",
]
