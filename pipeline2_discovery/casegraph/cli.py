"""CaseGraph CLI (PIPE1 + PIPE2).

Three modes, all gated by safety policy and explicit env opt-in for
live work:

- **Default (PIPE1)** — load a CasePacket fixture, score it via pure
  ``score_case_packet``, build a single-packet actionability report
  and a RunLedgerEntry. No network.
- **--query-plan** — load a structured-row fixture (WaPo / Fatal
  Encounters / MPV), build a connector query plan, emit it as JSON
  alongside a RunLedgerEntry. No network.
- **--live-dry (PIPE2)** — load a structured-row fixture, run exactly
  one capped live connector smoke under
  ``validate_live_run`` + ``LiveRunBudget``. Refuses to run unless
  ``FLAMEON_RUN_LIVE_CASEGRAPH=1``, ``max_results <= 5``, and the
  connector is on the free-providers allow-list (Brave / Firecrawl /
  LLM / downloads / scraping / transcript-fetching all blocked).

Usage:
    .venv\\Scripts\\python.exe -m pipeline2_discovery.casegraph.cli \\
        --fixture tests/fixtures/casegraph_scenarios/media_rich_produce.json --json
    .venv\\Scripts\\python.exe -m pipeline2_discovery.casegraph.cli \\
        --fixture tests/fixtures/structured_inputs/wapo_uof_complete.json \\
        --query-plan --json
    .venv\\Scripts\\python.exe -m pipeline2_discovery.casegraph.cli \\
        --fixture tests/fixtures/structured_inputs/wapo_uof_complete.json \\
        --live-dry --connector courtlistener --max-results 5 --json

Exit codes:
- ``0`` — success
- ``2`` — bad CLI usage (argparse handles)
- ``3`` — fixture path not found
- ``4`` — fixture is not valid JSON or fails its expected shape
- ``5`` — live-dry blocked by safety policy (env unset, cap exceeded,
  blocked connector, etc.)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, TextIO, Tuple

from .adapters import export_p2_to_p3, export_p2_to_p4, export_p2_to_p5
from .assembly import StructuredAssemblyResult, assemble_structured_case_packet
from .claim_extraction import extract_artifact_claims
from .connectors.agency_ois import AgencyOISConnector
from .identity import resolve_identity
from .inputs import (
    StructuredInputParseResult,
    parse_fatal_encounters_case_input,
    parse_mapping_police_violence_case_input,
    parse_wapo_uof_case_input,
)
from .ledger import RunLedgerEntry, build_run_ledger_entry
from .live_safety import (
    ALLOWED_FREE_CONNECTORS,
    PAID_CONNECTORS,
    LiveRunBlocked,
    LiveRunConfig,
)
from .live_smoke import LiveSmokeResult, run_capped_live_smoke
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
from .outcome import resolve_outcome
from .portal_dry_replay import (
    PortalReplayManifestEntry,
    load_portal_replay_manifest,
)
from .portal_executor import execute_mock_portal_plan
from .portal_fetch_plan import PortalFetchPlan
from .query_planner import QueryPlanResult, plan_queries_from_structured_result
from .reporting import build_actionability_report
from .resolvers import run_metadata_only_resolvers
from .routers import route_manual_defendant_jurisdiction
from .scoring import ActionabilityResult, score_case_packet


EXIT_OK = 0
EXIT_FIXTURE_MISSING = 3
EXIT_FIXTURE_INVALID = 4
EXIT_LIVE_BLOCKED = 5
EXIT_BUNDLE_UNSAFE = 6


# Output paths considered safe for bundle dumps. Everything in this
# list is gitignored in the repo's .gitignore (autoresearch/.runs/ etc.).
# Bundle files outside the repo are also accepted. Anything else
# requires --allow-unsafe-bundle-path to opt in explicitly.
BUNDLE_SAFE_DIRS = (
    "autoresearch/.runs",
    "autoresearch/.tmp",
    "autoresearch/.artifacts",
    "autoresearch/.cache",
    "autoresearch/.logs",
    ".runs",
    ".tmp",
    ".artifacts",
    ".cache",
    ".logs",
)


class BundlePathUnsafe(Exception):
    """Raised when --bundle-out resolves to a path that isn't under one
    of the gitignored artifact directories and --allow-unsafe-bundle-path
    was not passed."""


def _repo_root() -> Path:
    """Resolve the repo root from this module's location."""
    return Path(__file__).resolve().parents[2]


def _is_safe_bundle_path(path: Path) -> bool:
    """A bundle output path is safe iff it resolves outside the repo
    OR resolves inside one of BUNDLE_SAFE_DIRS (gitignored)."""
    abs_path = path.resolve()
    repo = _repo_root()
    try:
        rel = abs_path.relative_to(repo)
    except ValueError:
        return True  # outside repo entirely
    rel_parts = rel.parts
    for safe in BUNDLE_SAFE_DIRS:
        safe_parts = tuple(safe.split("/"))
        if rel_parts[: len(safe_parts)] == safe_parts:
            return True
    return False


def _validate_bundle_path(path: Path, *, allow_unsafe: bool) -> None:
    """Refuse paths that aren't gitignored unless caller opts in."""
    if not allow_unsafe and not _is_safe_bundle_path(path):
        raise BundlePathUnsafe(
            f"refusing to write bundle to {path}; path is not under one of "
            f"the gitignored artifact directories ({', '.join(BUNDLE_SAFE_DIRS)}) "
            "and is not outside the repo. Pass --allow-unsafe-bundle-path to override."
        )


def _strip_expected(data: Dict[str, Any]) -> Dict[str, Any]:
    """Drop the test-only `expected` block (manual scenario labels)
    before schema validation. The CasePacket schema does not allow it,
    and the CLI shouldn't fail on a fixture that carries one."""
    return {key: value for key, value in data.items() if key != "expected"}


def _packet_from_dict(data: Dict[str, Any]) -> CasePacket:
    identity_data = dict(data["case_identity"])
    identity_data["jurisdiction"] = Jurisdiction(**identity_data["jurisdiction"])
    return CasePacket(
        case_id=data["case_id"],
        input=CaseInput(**data["input"]),
        case_identity=CaseIdentity(**identity_data),
        sources=[SourceRecord(**source) for source in data["sources"]],
        artifact_claims=[ArtifactClaim(**claim) for claim in data["artifact_claims"]],
        verified_artifacts=[VerifiedArtifact(**artifact) for artifact in data["verified_artifacts"]],
        scores=Scores(**data["scores"]),
        verdict=data["verdict"],
        next_actions=list(data["next_actions"]),
        risk_flags=list(data["risk_flags"]),
    )


def _validate_against_schema(data: Dict[str, Any]) -> List[str]:
    """Validate against schemas/p2_case_packet.schema.json when the
    schema is present and jsonschema is importable. Returns a list of
    error messages; empty when valid."""
    try:
        from jsonschema import Draft7Validator  # type: ignore
    except ImportError:
        return []
    schema_path = Path(__file__).resolve().parents[2] / "schemas" / "p2_case_packet.schema.json"
    if not schema_path.exists():
        return []
    with schema_path.open("r", encoding="utf-8") as f:
        schema = json.load(f)
    validator = Draft7Validator(schema)
    return [error.message for error in validator.iter_errors(data)]


def _load_fixture(path: Path) -> CasePacket:
    if not path.exists():
        raise FileNotFoundError(f"fixture not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"fixture root is not a JSON object: {path}")
    packet_data = _strip_expected(raw)
    schema_errors = _validate_against_schema(packet_data)
    if schema_errors:
        joined = "; ".join(schema_errors)
        raise ValueError(f"fixture failed schema validation: {joined}")
    return _packet_from_dict(packet_data)


def _input_summary(packet: CasePacket) -> Dict[str, Any]:
    fields = packet.input.known_fields or {}
    jurisdiction = fields.get("jurisdiction") if isinstance(fields.get("jurisdiction"), dict) else {}
    return {
        "input_type": packet.input.input_type,
        "defendant_names": list(fields.get("defendant_names") or []),
        "agency": fields.get("agency"),
        "jurisdiction": {
            "city": jurisdiction.get("city"),
            "county": jurisdiction.get("county"),
            "state": jurisdiction.get("state"),
        }
        if jurisdiction
        else None,
        "incident_date": fields.get("incident_date"),
        "candidate_query_count": len(packet.input.candidate_queries or []),
        "missing_field_count": len(packet.input.missing_fields or []),
    }


def _packet_summary(
    packet: CasePacket,
    *,
    score_result: Optional[ActionabilityResult] = None,
) -> Dict[str, Any]:
    """Summarize the packet for JSON / text CLI output.

    ``packet_verdict`` is the verdict stored on the CasePacket itself.
    For default-mode CasePacket fixtures it carries the fixture's
    hand-set value; for any path through
    ``route_manual_defendant_jurisdiction`` (notably portal-replay) it
    is pinned to the router default ``"HOLD"`` regardless of what the
    scorer concludes — and ``score_case_packet`` is documented as pure,
    so it never writes back to ``packet.verdict``.

    ``score_verdict`` is the verdict from the freshly computed
    ``ActionabilityResult`` when one is threaded through. It is
    ``None`` when the caller has no result on hand. Surfacing both
    side by side lets operator-facing JSON make the stored / fresh
    distinction visible without mutating the packet or changing
    scoring purity.
    """
    return {
        "case_id": packet.case_id,
        "source_count": len(packet.sources),
        "artifact_claim_count": len(packet.artifact_claims),
        "verified_artifact_count": len(packet.verified_artifacts),
        "verified_artifact_types": sorted({a.artifact_type for a in packet.verified_artifacts}),
        "identity_confidence": packet.case_identity.identity_confidence,
        "outcome_status": packet.case_identity.outcome_status,
        "packet_verdict": packet.verdict,
        "score_verdict": score_result.verdict if score_result is not None else None,
    }


def _result_summary(result: ActionabilityResult) -> Dict[str, Any]:
    return {
        "verdict": result.verdict,
        "research_completeness_score": result.research_completeness_score,
        "production_actionability_score": result.production_actionability_score,
        "actionability_score": result.actionability_score,
        "component_scores": dict(result.component_scores),
        "artifact_category_counts": dict(result.artifact_category_counts),
        "reason_codes": list(result.reason_codes),
        "risk_flags": list(result.risk_flags),
        "next_actions": list(result.next_actions),
    }


def _identity_section(packet: CasePacket) -> Dict[str, Any]:
    ident = packet.case_identity
    return {
        "defendant_names": list(ident.defendant_names),
        "victim_names": list(ident.victim_names),
        "agency": ident.agency,
        "jurisdiction": asdict(ident.jurisdiction),
        "incident_date": ident.incident_date,
        "case_numbers": list(ident.case_numbers),
        "charges": list(ident.charges),
        "identity_confidence": ident.identity_confidence,
        "identity_anchors": list(ident.identity_anchors),
    }


def _outcome_section(packet: CasePacket) -> Dict[str, Any]:
    return {
        "outcome_status": packet.case_identity.outcome_status,
        "charges": list(packet.case_identity.charges),
        "case_numbers": list(packet.case_identity.case_numbers),
    }


def _connector_summary_section(packet: CasePacket) -> Dict[str, Any]:
    by_api: Dict[str, int] = {}
    by_role: Dict[str, int] = {}
    for source in packet.sources:
        api = source.api_name or "unknown"
        by_api[api] = by_api.get(api, 0) + 1
        for role in source.source_roles:
            by_role[role] = by_role.get(role, 0) + 1
    return {
        "total_source_records": len(packet.sources),
        "by_api": dict(sorted(by_api.items())),
        "by_role": dict(sorted(by_role.items())),
        "source_ids": [source.source_id for source in packet.sources],
    }


def build_run_bundle(
    *,
    mode: str,
    experiment_id: str,
    wallclock_seconds: float,
    packet: Optional[CasePacket] = None,
    parsed: Optional[StructuredInputParseResult] = None,
    query_plan: Optional[QueryPlanResult] = None,
    multi_source_summary: Optional[Dict[str, Any]] = None,
    smoke_diagnostics: Optional[Dict[str, Any]] = None,
    live_yield_report: Optional[Dict[str, Any]] = None,
    api_calls: Optional[Dict[str, int]] = None,
    notes: Optional[List[str]] = None,
    handoffs: Optional[Dict[str, Any]] = None,
    portal_replay: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Canonical CaseGraph run bundle (PIPE5).

    Always emits the canonical top-level keys; sections without data
    are explicitly ``None`` rather than missing so downstream consumers
    can rely on key presence. Pure: builds from already-computed inputs
    and never makes a network call. The packet, when supplied, is not
    mutated."""
    if packet is None and parsed is None:
        raise ValueError("build_run_bundle requires a packet or a parsed input")

    if packet is not None:
        input_summary: Dict[str, Any] = _input_summary(packet)
        identity_section: Optional[Dict[str, Any]] = _identity_section(packet)
        outcome_section: Optional[Dict[str, Any]] = _outcome_section(packet)
        connector_summary: Optional[Dict[str, Any]] = _connector_summary_section(packet)
        artifact_claims = [asdict(claim) for claim in packet.artifact_claims]
        verified_artifacts = [asdict(artifact) for artifact in packet.verified_artifacts]
        result_obj = score_case_packet(packet)
        result_section: Optional[Dict[str, Any]] = _result_summary(result_obj)
        actionability_report: Optional[Dict[str, Any]] = build_actionability_report([packet])
        next_actions: List[str] = list(result_obj.next_actions)
        risk_flags: List[str] = list(result_obj.risk_flags)
        ledger = build_run_ledger_entry(
            experiment_id=experiment_id,
            packet=packet,
            api_calls=api_calls,
            wallclock_seconds=wallclock_seconds,
            notes=notes,
        )
    else:
        assert parsed is not None  # for type checker
        input_summary = _structured_summary(parsed)
        identity_section = None
        outcome_section = None
        connector_summary = None
        artifact_claims = []
        verified_artifacts = []
        result_section = None
        actionability_report = None
        next_actions = []
        risk_flags = list(parsed.risk_flags)
        ledger = build_run_ledger_entry(
            experiment_id=experiment_id,
            case_id=None,
            api_calls=api_calls,
            wallclock_seconds=wallclock_seconds,
            notes=notes,
        )

    bundle: Dict[str, Any] = {
        "experiment_id": experiment_id,
        "mode": mode,
        "wallclock_seconds": wallclock_seconds,
        "input_summary": input_summary,
        "query_plan": _query_plan_summary(query_plan) if query_plan is not None else None,
        "connector_summary": connector_summary,
        "multi_source_summary": multi_source_summary,
        "smoke_diagnostics": smoke_diagnostics,
        "identity": identity_section,
        "outcome": outcome_section,
        "artifact_claims": artifact_claims,
        "verified_artifacts": verified_artifacts,
        "result": result_section,
        "actionability_report": actionability_report,
        "live_yield_report": live_yield_report,
        "ledger_entry": ledger.to_dict(),
        "next_actions": next_actions,
        "risk_flags": risk_flags,
    }
    if handoffs is not None:
        bundle["handoffs"] = handoffs
    if portal_replay is not None:
        bundle["portal_replay"] = portal_replay
    return bundle


def _write_bundle(path: Path, bundle: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2, sort_keys=False)
        f.write("\n")


def build_handoffs(
    packet: CasePacket,
    *,
    score_result: Optional[ActionabilityResult] = None,
) -> Dict[str, Any]:
    """Build the canonical P2 handoff bundle for a CasePacket.

    Pure: delegates to the schema-validated adapters. ``score_result``
    is threaded into ``export_p2_to_p4`` and ``export_p2_to_p5`` so the
    P4 ``source_quality_notes`` and the P5 ``risk_flags`` /
    ``next_actions`` carry the same freshly computed advisory signals
    the CLI emits in its ``result`` section. When the caller hasn't
    already computed an ``ActionabilityResult``, this helper computes
    one internally via the pure ``score_case_packet`` (no packet
    mutation).
    """
    if score_result is None:
        score_result = score_case_packet(packet)
    return {
        "p2_to_p3": export_p2_to_p3(packet),
        "p2_to_p4": export_p2_to_p4(packet, score_result=score_result),
        "p2_to_p5": export_p2_to_p5(packet, score_result=score_result),
    }


def build_dry_run_payload(
    packet: CasePacket,
    *,
    experiment_id: str = "PIPE1-cli-dry-run",
    wallclock_seconds: float = 0.0,
    emit_handoffs: bool = False,
) -> Dict[str, Any]:
    """Build the structured payload printed by the CLI.

    Pure: scores and reports run via deterministic helpers and never
    mutate the supplied packet. When ``emit_handoffs`` is True, a
    top-level ``handoffs`` object containing the schema-validated
    P2→P3/P4/P5 exports is included; otherwise the payload shape is
    unchanged from prior CLI behavior.
    """
    result = score_case_packet(packet)
    report = build_actionability_report([packet])
    ledger_entry = build_run_ledger_entry(
        experiment_id=experiment_id,
        packet=packet,
        wallclock_seconds=wallclock_seconds,
    )
    payload: Dict[str, Any] = {
        "input_summary": _input_summary(packet),
        "packet_summary": _packet_summary(packet, score_result=result),
        "result": _result_summary(result),
        "report": report,
        "ledger_entry": ledger_entry.to_dict(),
    }
    if emit_handoffs:
        payload["handoffs"] = build_handoffs(packet, score_result=result)
    return payload


def _format_text(payload: Dict[str, Any]) -> str:
    inp = payload["input_summary"]
    pkt = payload["packet_summary"]
    res = payload["result"]
    led = payload["ledger_entry"]
    lines = [
        "=== CaseGraph dry run ===",
        f"case_id: {pkt['case_id']}",
        f"input_type: {inp['input_type']}",
        f"defendant_names: {', '.join(inp['defendant_names']) or '(none)'}",
        f"agency: {inp['agency'] or '(none)'}",
        f"identity_confidence: {pkt['identity_confidence']}",
        f"outcome_status: {pkt['outcome_status']}",
        "",
        f"verdict: {res['verdict']}",
        f"research_completeness_score: {res['research_completeness_score']}",
        f"production_actionability_score: {res['production_actionability_score']}",
        f"actionability_score: {res['actionability_score']}",
        "",
        f"reason_codes ({len(res['reason_codes'])}): {', '.join(res['reason_codes']) or '(none)'}",
        f"risk_flags ({len(res['risk_flags'])}): {', '.join(res['risk_flags']) or '(none)'}",
        "",
        f"next_actions ({len(res['next_actions'])}):",
    ]
    for action in res["next_actions"]:
        lines.append(f"  - {action}")
    if not res["next_actions"]:
        lines.append("  (none)")
    lines.extend(
        [
            "",
            f"verified_artifacts: {pkt['verified_artifact_count']} "
            f"({', '.join(pkt['verified_artifact_types']) or 'none'})",
            f"sources: {pkt['source_count']}    artifact_claims: {pkt['artifact_claim_count']}",
            "",
            f"ledger.experiment_id: {led['experiment_id']}",
            f"ledger.wallclock_seconds: {led['wallclock_seconds']}",
            f"ledger.estimated_cost_usd: {led['estimated_cost_usd']}",
            f"ledger.api_calls: {led['api_calls']}",
        ]
    )
    return "\n".join(lines) + "\n"


def _parse_structured_fixture(path: Path) -> StructuredInputParseResult:
    """Parse a structured-row fixture (WaPo / Fatal Encounters / MPV).

    Dispatches to the right per-dataset parser based on the
    ``dataset`` / ``dataset_name`` field, falling back to WaPo. Used
    by --query-plan and --live-dry modes.
    """
    if not path.exists():
        raise FileNotFoundError(f"fixture not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"fixture root is not a JSON object: {path}")
    dataset = str(raw.get("dataset") or raw.get("dataset_name") or "").lower()
    if "fatal_encounters" in dataset:
        return parse_fatal_encounters_case_input(raw)
    if "mapping_police_violence" in dataset or dataset == "mpv":
        return parse_mapping_police_violence_case_input(raw)
    return parse_wapo_uof_case_input(raw)


def _structured_summary(parsed: StructuredInputParseResult) -> Dict[str, Any]:
    fields = parsed.case_input.known_fields or {}
    jurisdiction = fields.get("jurisdiction") if isinstance(fields.get("jurisdiction"), dict) else {}
    return {
        "input_type": parsed.case_input.input_type,
        "dataset_name": parsed.dataset_name,
        "defendant_names": list(fields.get("defendant_names") or []),
        "agency": fields.get("agency"),
        "jurisdiction": {
            "city": jurisdiction.get("city"),
            "county": jurisdiction.get("county"),
            "state": jurisdiction.get("state"),
        },
        "incident_date": fields.get("incident_date"),
        "candidate_query_count": len(parsed.case_input.candidate_queries or []),
        "missing_field_count": len(parsed.case_input.missing_fields or []),
        "risk_flags": list(parsed.risk_flags),
    }


def _query_plan_summary(plan: QueryPlanResult) -> Dict[str, Any]:
    return {
        "connector_count": len(plan.plans),
        "plans": [
            {
                "connector": connector_plan.connector_name,
                "priority": connector_plan.priority,
                "query_count": len(connector_plan.queries),
                "queries": [
                    {
                        "query": query.query,
                        "reason": query.reason,
                        "candidate_fields_used": list(query.candidate_fields_used),
                    }
                    for query in connector_plan.queries
                ],
                "missing_field_requirements": list(connector_plan.missing_field_requirements),
                "rationale": connector_plan.rationale,
                "risk_flags": list(connector_plan.risk_flags),
            }
            for connector_plan in plan.plans
        ],
        "risk_flags": list(plan.risk_flags),
    }


def build_query_plan_payload(
    parsed: StructuredInputParseResult,
    *,
    experiment_id: str,
    wallclock_seconds: float = 0.0,
) -> Dict[str, Any]:
    """Build the structured payload for the --query-plan mode.

    Pure: parses the fixture, builds the connector query plan, emits a
    RunLedgerEntry. No network."""
    plan = plan_queries_from_structured_result(parsed)
    ledger_entry = build_run_ledger_entry(
        experiment_id=experiment_id,
        case_id=None,
        wallclock_seconds=wallclock_seconds,
    )
    return {
        "input_summary": _structured_summary(parsed),
        "query_plan": _query_plan_summary(plan),
        "ledger_entry": ledger_entry.to_dict(),
    }


def _validate_dry_run_connectors(connectors: List[str]) -> None:
    """Mirror the live-safety connector allow-list for no-live dry runs.

    Rejects the same paid / unknown connectors that ``validate_live_run``
    would reject, but without requiring the live env gate (since no
    network call will be made). Brave / Firecrawl always rejected
    because they're metered providers and the multi-source dry run is
    deliberately scoped to free connectors only.
    """
    for connector in connectors:
        if connector in PAID_CONNECTORS:
            raise LiveRunBlocked(
                f"connector {connector!r} is paid and refused in --multi-source-dry-run"
            )
        if connector not in ALLOWED_FREE_CONNECTORS:
            raise LiveRunBlocked(
                f"connector {connector!r} not in allow-list "
                f"{sorted(ALLOWED_FREE_CONNECTORS)}"
            )


def _per_connector_dry_summary(
    connectors: List[str],
    plan: "QueryPlanResult",
    *,
    max_results: int,
) -> List[Dict[str, Any]]:
    """For each requested connector, surface what WOULD be sent.

    A no-live dry run records zero source records and zero verified
    artifacts — the fields are explicit so callers can assert on them
    without having to introspect missing keys.
    """
    summaries: List[Dict[str, Any]] = []
    for connector in connectors:
        connector_plan = next(
            (cp for cp in plan.plans if cp.connector_name == connector), None
        )
        planned_queries = (
            [
                {"query": q.query, "reason": q.reason}
                for q in connector_plan.queries
            ]
            if connector_plan
            else []
        )
        summaries.append(
            {
                "connector": connector,
                "max_results": max_results,
                "planned_query_count": len(planned_queries),
                "first_planned_query": (
                    planned_queries[0]["query"] if planned_queries else None
                ),
                "planned_queries": planned_queries,
                "missing_field_requirements": (
                    list(connector_plan.missing_field_requirements)
                    if connector_plan
                    else []
                ),
                "rationale": (connector_plan.rationale if connector_plan else "no plan generated"),
                "source_record_count": 0,
                "verified_artifact_count": 0,
                "estimated_cost_usd": 0.0,
            }
        )
    return summaries


def build_multi_source_dry_run_payload(
    parsed: StructuredInputParseResult,
    *,
    connectors: List[str],
    max_results: int,
    experiment_id: str,
    wallclock_seconds: float = 0.0,
) -> Dict[str, Any]:
    """Build the structured payload for --multi-source-dry-run mode.

    Pure: parses the fixture, builds the connector query plan,
    assembles a structured CasePacket with zero sources, scores it,
    and emits a RunLedgerEntry. No network. Verified artifacts stay
    zero — the harness alone never invokes any resolver."""
    plan = plan_queries_from_structured_result(parsed)
    per_connector = _per_connector_dry_summary(
        connectors, plan, max_results=max_results
    )
    assembly = assemble_structured_case_packet(parsed)
    report = build_actionability_report([assembly.packet])
    ledger_entry = build_run_ledger_entry(
        experiment_id=experiment_id,
        packet=assembly.packet,
        api_calls={},
        wallclock_seconds=wallclock_seconds,
        notes=[
            "multi-source-dry-run",
            f"connectors={','.join(connectors)}",
            f"max_results={max_results}",
        ],
    )
    return {
        "input_summary": _structured_summary(parsed),
        "multi_source_dry_run": {
            "connectors": list(connectors),
            "max_results": max_results,
            "per_connector": per_connector,
            "total_planned_queries": sum(
                entry["planned_query_count"] for entry in per_connector
            ),
            "total_source_records": 0,
            "total_verified_artifacts": 0,
            "total_estimated_cost_usd": 0.0,
        },
        "packet_summary": _packet_summary(
            assembly.packet, score_result=assembly.actionability
        ),
        "result": _result_summary(assembly.actionability),
        "report": report,
        "ledger_entry": ledger_entry.to_dict(),
    }


_LIVE_URL_SCHEMES = ("http://", "https://")
_AGENCY_NAME_SUFFIXES = (
    " Police Department",
    " Sheriff's Office",
    " Sheriff Department",
    " Sheriff's Department",
    " Sheriff",
    " Department of Public Safety",
    " PD",
)


def _looks_like_live_url(value: str) -> bool:
    """True when --fixture's value is an http(s) URL rather than a path.

    --portal-replay refuses live URLs by design. This is the explicit
    guard that surfaces a clear error before any I/O happens.
    """
    lowered = (value or "").strip().lower()
    return any(lowered.startswith(scheme) for scheme in _LIVE_URL_SCHEMES)


class _PortalManifestEntryNotFound(Exception):
    """Raised when --portal-manifest-entry references a case_id that
    isn't in the canonical portal replay manifest."""


def _resolve_portal_manifest_entry(case_id: int) -> Tuple[PortalReplayManifestEntry, Path]:
    """Look up a manifest entry by case_id and return (entry,
    manifest_path). Manifest path is the canonical default path used
    by ``load_portal_replay_manifest`` (no override flag in this PR).
    """
    repo_root = _repo_root()
    manifest = load_portal_replay_manifest(repo_root=repo_root)
    for entry in manifest.entries:
        if entry.case_id == case_id:
            manifest_path = (
                repo_root / "tests" / "fixtures" / "portal_replay" / "portal_replay_manifest.json"
            )
            return entry, manifest_path
    raise _PortalManifestEntryNotFound(
        f"manifest has no entry with case_id={case_id}"
    )


def _require_fixture_argument(args, *, mode_label: str, err) -> Optional[int]:
    """Per-mode guard: each non-portal-replay mode requires --fixture.

    Returns ``None`` when the input is fine; otherwise emits a clear
    inline error to ``err`` and returns the exit code the dispatcher
    should propagate. Replaces argparse's prior auto-required error
    so the message matches the rest of our exit-code surface.
    """
    if args.fixture:
        return None
    err.write(
        f"error: --{mode_label} requires --fixture <path>\n"
    )
    return EXIT_FIXTURE_INVALID


def _load_portal_payload(path: Path) -> Dict[str, Any]:
    """Load and minimally validate a portal-replay payload fixture.

    Accepts agency-OIS shaped pages (page_type / subjects / media_links
    / etc.) and manifest-supplied source_records lists. Raises the same
    exceptions the default-mode loader does so the dispatcher can map
    them to consistent exit codes.
    """
    if not path.exists():
        raise FileNotFoundError(f"fixture not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"portal payload root is not a JSON object: {path}")
    if not (
        raw.get("page_type")
        or raw.get("portal_profile_id")
        or isinstance(raw.get("source_records"), list)
    ):
        raise ValueError(
            "portal payload must declare page_type, portal_profile_id, "
            "or source_records"
        )
    return dict(raw)


def _portal_profile_id(payload: Mapping[str, Any]) -> str:
    """Resolve the canonical portal_profile_id for a payload, falling
    back to agency_ois_detail when only ``page_type`` is set."""
    declared = str(payload.get("portal_profile_id") or "")
    if declared:
        return declared
    if payload.get("page_type"):
        return "agency_ois_detail"
    return ""


def _portal_plan_from_payload(payload: Mapping[str, Any], *, fixture_path: Path) -> PortalFetchPlan:
    """Build a minimal PortalFetchPlan suitable for the offline executor.

    No fetcher actually runs; the plan is shaped to mirror the executor's
    expectations and to seed identity/title fields. Derived from the
    payload contents, never from caller-supplied flags."""
    profile_id = _portal_profile_id(payload) or "agency_ois_detail"
    seed_url = str(payload.get("url") or "")
    title = ""
    subjects = payload.get("subjects") or []
    if isinstance(subjects, list) and subjects:
        title = str(subjects[0])
    elif payload.get("title"):
        title = str(payload["title"])
    case_id = int(payload.get("case_id") or 0) or hash(fixture_path.name) % 1_000_000
    return PortalFetchPlan(
        case_id=case_id,
        title=title,
        portal_profile_id=profile_id,
        seed_url=seed_url or None,
        seed_url_exists=bool(seed_url),
        fetcher="firecrawl",
        max_pages=1,
        max_links=10,
        allowed_domain=urlparse_netloc(seed_url),
    )


def urlparse_netloc(url: str) -> Optional[str]:
    """Tiny helper around urlparse to keep the call sites readable."""
    if not url:
        return None
    from urllib.parse import urlparse

    netloc = urlparse(url).netloc.lower()
    return netloc or None


def _source_record_from_dict(raw: Mapping[str, Any]) -> SourceRecord:
    return SourceRecord(
        source_id=str(raw["source_id"]),
        url=str(raw.get("url") or ""),
        title=str(raw.get("title") or ""),
        snippet=str(raw.get("snippet") or ""),
        raw_text=str(raw.get("raw_text") or raw.get("snippet") or ""),
        source_type=str(raw.get("source_type") or "unknown"),
        source_authority=str(raw.get("source_authority") or "unknown"),
        source_roles=list(raw.get("source_roles") or []),
        api_name=raw.get("api_name"),
        discovered_via=str(raw.get("discovered_via") or ""),
        case_input_id=raw.get("case_input_id"),
        metadata=dict(raw.get("metadata") or {}),
        cost_estimate=float(raw.get("cost_estimate") or 0.0),
        confidence_signals=dict(raw.get("confidence_signals") or {}),
        matched_case_fields=list(raw.get("matched_case_fields") or []),
    )


def _portal_payload_source_records(payload: Mapping[str, Any]) -> List[SourceRecord]:
    """Public-API replay of portal_executor's record dispatch.

    Mirrors the executor: manifest-supplied source_records lists load
    directly; agency-shaped pages flow through AgencyOISConnector.
    """
    records_field = payload.get("source_records")
    if isinstance(records_field, list):
        return [_source_record_from_dict(item) for item in records_field]
    profile_id = _portal_profile_id(payload)
    if profile_id.startswith("agency_ois") or payload.get("page_type"):
        subjects = payload.get("subjects") or []
        if isinstance(subjects, str):
            subjects = [subjects]
        case_input = CaseInput(
            input_type="manual",
            raw_input={"defendant_names": ", ".join(str(s) for s in subjects)},
            known_fields={"defendant_names": list(subjects)},
        )
        return list(AgencyOISConnector([dict(payload)]).fetch(case_input))
    return []


def _derive_defendant_string(payload: Mapping[str, Any]) -> str:
    subjects = payload.get("subjects") or []
    if isinstance(subjects, list) and subjects:
        return str(subjects[0])
    return "Generic Subject"


def _derive_jurisdiction_string(payload: Mapping[str, Any]) -> str:
    """Best-effort city derivation from agency name. Identical pattern
    to the PR #8 portal-replay-to-handoffs harness so this CLI mode and
    the integration tests stay in lockstep."""
    agency = str(payload.get("agency") or "")
    if not agency:
        return "Unknown"
    city = agency
    for tag in _AGENCY_NAME_SUFFIXES:
        if tag in city:
            city = city.replace(tag, "").strip()
            break
    return f"{city}, AZ" if city else "Unknown"


def _coerce_identity_string(value: Any) -> Optional[str]:
    """Coerce an arbitrary payload value to a stripped, non-empty
    string, or return None for None / non-stringifiable empties."""
    if value is None:
        return None
    coerced = str(value).strip()
    return coerced or None


def enrich_portal_replay_identity(
    packet: CasePacket, payload: Mapping[str, Any]
) -> None:
    """Populate blank ``case_identity`` fields from a saved portal
    payload (agency_ois page shape).

    The manual router (``route_manual_defendant_jurisdiction``) only
    accepts defendant + jurisdiction strings, so portal-replay packets
    arrive at ``resolve_identity`` with no agency / incident_date /
    case_numbers even though the saved agency_ois payload typically
    surfaces all three. This helper lifts those facts onto
    ``packet.case_identity`` before identity resolution and scoring.

    Rules:
    - Only fills fields the manual router left blank/empty.
    - Never overwrites existing ``case_identity`` values.
    - Coerces values to ``str``.
    - Ignores None / empty / whitespace-only values.

    Does not mutate source records, scoring inputs, resolvers, or the
    artifact graduation chain.
    """
    identity = packet.case_identity

    if not identity.agency:
        agency = _coerce_identity_string(payload.get("agency"))
        if agency:
            identity.agency = agency

    if not identity.incident_date:
        incident_date = _coerce_identity_string(payload.get("incident_date"))
        if incident_date:
            identity.incident_date = incident_date

    if not identity.case_numbers:
        case_number = _coerce_identity_string(payload.get("case_number"))
        if case_number:
            identity.case_numbers = [case_number]


def _build_portal_packet(payload: Mapping[str, Any]) -> CasePacket:
    """Build a CasePacket from a portal payload via the public manual
    router, then attach portal-extracted SourceRecords and lift
    agency_ois identity facts (agency / incident_date / case_number)
    onto ``case_identity`` so ``resolve_identity`` can anchor on them."""
    defendant = _derive_defendant_string(payload)
    jurisdiction = _derive_jurisdiction_string(payload)
    packet = route_manual_defendant_jurisdiction(defendant, jurisdiction)
    packet.sources = list(_portal_payload_source_records(payload))
    enrich_portal_replay_identity(packet, payload)
    return packet


def _portal_input_summary(
    payload: Mapping[str, Any], *, fixture_path: Path
) -> Dict[str, Any]:
    return {
        "input_type": "portal_replay",
        "fixture_path": _relative_fixture_path(fixture_path),
        "portal_profile_id": _portal_profile_id(payload) or None,
        "subjects": list(payload.get("subjects") or []),
        "agency": payload.get("agency"),
        "incident_date": payload.get("incident_date"),
        "case_number": payload.get("case_number"),
        "page_type": payload.get("page_type"),
        "page_url": payload.get("url"),
    }


def _relative_fixture_path(fixture_path: Path) -> str:
    repo = _repo_root()
    try:
        return os.fspath(fixture_path.resolve().relative_to(repo)).replace(os.sep, "/")
    except ValueError:
        return os.fspath(fixture_path.resolve())


def _portal_replay_section(
    payload: Mapping[str, Any],
    exec_result: Any,
    *,
    fixture_path: Path,
    manifest_entry: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    section: Dict[str, Any] = {
        "portal_profile_id": _portal_profile_id(payload) or None,
        "fixture_path": _relative_fixture_path(fixture_path),
        "source_records_count": len(getattr(exec_result, "extracted_source_records", []) or []),
        "artifact_claims_count": len(getattr(exec_result, "artifact_claims", []) or []),
        "candidate_urls_count": len(getattr(exec_result, "candidate_artifact_urls", []) or []),
        "rejected_urls_count": len(getattr(exec_result, "rejected_urls", []) or []),
        "executor_status": getattr(exec_result, "execution_status", "unknown"),
        "executor_risk_flags": list(getattr(exec_result, "risk_flags", []) or []),
        "executor_next_actions": list(getattr(exec_result, "next_actions", []) or []),
    }
    if manifest_entry is not None:
        section["manifest_entry"] = dict(manifest_entry)
    return section


def build_portal_replay_payload(
    payload: Mapping[str, Any],
    *,
    fixture_path: Path,
    experiment_id: str = "PIPE3-cli-portal-replay",
    wallclock_seconds: float = 0.0,
    emit_handoffs: bool = False,
    manifest_entry: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the structured payload printed by --portal-replay mode.

    Pure: runs the executor against the saved payload (no fetch), then
    threads the extracted SourceRecords through the existing public
    assembly pipeline (resolve_identity / resolve_outcome / claim
    extraction / metadata-only resolvers / score). When ``emit_handoffs``
    is True, attaches the schema-validated P2 -> P3/P4/P5 exports under
    a top-level ``handoffs`` key, matching default-mode shape (PR #6).
    """
    plan = _portal_plan_from_payload(payload, fixture_path=fixture_path)
    exec_result = execute_mock_portal_plan(plan, payload)
    packet = _build_portal_packet(payload)
    resolve_identity(packet)
    resolve_outcome(packet)
    extract_artifact_claims(packet)
    run_metadata_only_resolvers(packet)
    result = score_case_packet(packet)
    report = build_actionability_report([packet])
    ledger_entry = build_run_ledger_entry(
        experiment_id=experiment_id,
        packet=packet,
        wallclock_seconds=wallclock_seconds,
        notes=[
            "portal-replay",
            f"profile={_portal_profile_id(payload) or 'unknown'}",
            f"fixture={_relative_fixture_path(fixture_path)}",
        ],
    )
    output: Dict[str, Any] = {
        "input_summary": _portal_input_summary(payload, fixture_path=fixture_path),
        "packet_summary": _packet_summary(packet, score_result=result),
        "result": _result_summary(result),
        "report": report,
        "ledger_entry": ledger_entry.to_dict(),
        "portal_replay": _portal_replay_section(
            payload,
            exec_result,
            fixture_path=fixture_path,
            manifest_entry=manifest_entry,
        ),
    }
    if emit_handoffs:
        output["handoffs"] = build_handoffs(packet, score_result=result)
    return output


def _format_portal_replay_text(payload: Dict[str, Any]) -> str:
    inp = payload["input_summary"]
    pkt = payload["packet_summary"]
    res = payload["result"]
    pr = payload["portal_replay"]
    led = payload["ledger_entry"]
    lines = [
        "=== CaseGraph portal replay (offline) ===",
        f"fixture_path: {pr['fixture_path']}",
        f"portal_profile_id: {pr['portal_profile_id'] or '(none)'}",
        f"page_url: {inp['page_url'] or '(none)'}",
        f"subjects: {', '.join(inp['subjects']) or '(none)'}",
        f"agency: {inp['agency'] or '(none)'}",
        "",
        f"executor_status: {pr['executor_status']}",
        f"source_records: {pr['source_records_count']}    "
        f"artifact_claims: {pr['artifact_claims_count']}",
        f"candidate_urls: {pr['candidate_urls_count']}    "
        f"rejected_urls: {pr['rejected_urls_count']}",
        f"executor_risk_flags: {', '.join(pr['executor_risk_flags']) or '(none)'}",
        "",
        f"case_id: {pkt['case_id']}",
        f"identity_confidence: {pkt['identity_confidence']}    "
        f"outcome_status: {pkt['outcome_status']}",
        f"verified_artifacts: {pkt['verified_artifact_count']} "
        f"({', '.join(pkt['verified_artifact_types']) or 'none'})",
        "",
        f"verdict: {res['verdict']}",
        f"actionability_score: {res['actionability_score']}",
        f"reason_codes: {', '.join(res['reason_codes']) or '(none)'}",
        f"risk_flags: {', '.join(res['risk_flags']) or '(none)'}",
        "",
        f"ledger.experiment_id: {led['experiment_id']}",
        f"ledger.api_calls: {led['api_calls']}",
        f"ledger.estimated_cost_usd: {led['estimated_cost_usd']}",
    ]
    if "handoffs" in payload:
        handoffs = payload["handoffs"]
        lines.extend(
            [
                "",
                f"handoffs.p2_to_p3 rows: {len(handoffs['p2_to_p3'])}",
                f"handoffs.p2_to_p4.case_id: {handoffs['p2_to_p4'].get('case_id')}",
                f"handoffs.p2_to_p5.verdict: {handoffs['p2_to_p5'].get('verdict')}",
            ]
        )
    return "\n".join(lines) + "\n"


def build_live_dry_payload(
    parsed: StructuredInputParseResult,
    *,
    config: LiveRunConfig,
    experiment_id: str,
    smoke_result: LiveSmokeResult,
    wallclock_seconds: float,
) -> Dict[str, Any]:
    """Build the structured payload for --live-dry mode.

    Combines the fixture summary, the live smoke diagnostics, and a
    ledger entry derived from the LiveRunBudget. Verified artifacts
    stay zero — the harness deliberately doesn't run a resolver."""
    diagnostics = smoke_result.to_diagnostics()
    summary = smoke_result.budget.to_ledger_summary()
    ledger_entry = build_run_ledger_entry(
        experiment_id=experiment_id,
        case_id=None,
        api_calls=summary["api_calls"],
        wallclock_seconds=wallclock_seconds,
        notes=[
            f"connector={config.connector}",
            f"max_queries={config.max_queries}",
            f"max_results={config.max_results}",
        ],
    )
    return {
        "input_summary": _structured_summary(parsed),
        "live_dry": {
            "connector": config.connector,
            "max_queries": config.max_queries,
            "max_results": config.max_results,
            "diagnostics": diagnostics,
            "source_record_count": smoke_result.source_count,
            "verified_artifact_count": smoke_result.verified_artifact_count,
        },
        "ledger_entry": ledger_entry.to_dict(),
    }


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipeline2_discovery.casegraph.cli",
        description=(
            "CaseGraph CLI. Three modes: default (CasePacket dry-run, no live), "
            "--query-plan (build a connector query plan from a structured row), "
            "--live-dry (capped live smoke under safety policy)."
        ),
    )
    parser.add_argument(
        "--fixture",
        default=None,
        help=(
            "Path to a JSON fixture. Default mode expects a CasePacket "
            "fixture (e.g. tests/fixtures/casegraph_scenarios/...). "
            "--query-plan and --live-dry expect a structured-row fixture. "
            "--portal-replay expects a portal payload fixture; --portal-replay "
            "may instead use --portal-manifest-entry to resolve a saved "
            "fixture by manifest case_id."
        ),
    )
    parser.add_argument(
        "--portal-manifest-entry",
        dest="portal_manifest_entry",
        type=int,
        default=None,
        help=(
            "Integer case_id from the canonical portal replay manifest "
            "(tests/fixtures/portal_replay/portal_replay_manifest.json). "
            "Only meaningful with --portal-replay. Mutually exclusive with "
            "--fixture in portal-replay mode."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON on stdout instead of human-readable text.",
    )
    parser.add_argument(
        "--experiment-id",
        default=None,
        help=(
            "experiment_id field for the emitted RunLedgerEntry. Defaults: "
            "PIPE1-cli-dry-run / PIPE1-cli-query-plan / PIPE2-cli-live-dry."
        ),
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--query-plan",
        action="store_true",
        help="Build a connector query plan from a structured-row fixture. No network.",
    )
    mode_group.add_argument(
        "--live-dry",
        action="store_true",
        help=(
            "Run exactly one capped live connector smoke under safety policy. "
            "Requires FLAMEON_RUN_LIVE_CASEGRAPH=1."
        ),
    )
    mode_group.add_argument(
        "--multi-source-dry-run",
        dest="multi_source_dry_run",
        action="store_true",
        help=(
            "No-live multi-connector dry run from a structured-row fixture. "
            "Emits the planned per-connector query, an assembled CasePacket "
            "summary, an actionability report, and a ledger entry. "
            "Refuses Brave / Firecrawl / unknown connectors."
        ),
    )
    mode_group.add_argument(
        "--portal-replay",
        dest="portal_replay",
        action="store_true",
        help=(
            "Offline portal-replay mode. --fixture must point at a saved "
            "portal payload JSON (agency_ois page or manifest source_records "
            "list). Runs the executor + assembly + scoring chain on saved "
            "data only. Refuses http:// / https:// fixture values; no "
            "network, no Firecrawl, no browser automation."
        ),
    )
    parser.add_argument(
        "--connector",
        default="courtlistener",
        help="Connector for --live-dry (default: courtlistener).",
    )
    parser.add_argument(
        "--connectors",
        default="",
        help=(
            "Comma-separated connector names for --multi-source-dry-run "
            "(e.g. courtlistener,muckrock,documentcloud)."
        ),
    )
    parser.add_argument(
        "--max-queries",
        type=int,
        default=1,
        help="Hard-capped at validate_live_run's MAX_QUERIES_HARD_CAP (default: 1).",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=5,
        help="Hard-capped at validate_live_run's MAX_RESULTS_HARD_CAP (default: 5).",
    )
    parser.add_argument(
        "--bundle-out",
        dest="bundle_out",
        default=None,
        help=(
            "Optional path to write a single canonical JSON run bundle "
            "(PIPE5). Default-safe: the path must be inside one of the "
            "gitignored artifact directories (autoresearch/.runs, .tmp, "
            ".artifacts, .cache, .logs) or outside the repo entirely. "
            "Use --allow-unsafe-bundle-path to override."
        ),
    )
    parser.add_argument(
        "--allow-unsafe-bundle-path",
        dest="allow_unsafe_bundle_path",
        action="store_true",
        help="Allow --bundle-out to write to a non-gitignored repo path.",
    )
    parser.add_argument(
        "--emit-handoffs",
        dest="emit_handoffs",
        action="store_true",
        help=(
            "Include the schema-validated P2->P3, P2->P4, and P2->P5 "
            "handoff payloads under a top-level 'handoffs' key in the "
            "default-mode JSON output (and in the run bundle when "
            "--bundle-out is also passed). Default mode only."
        ),
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _format_query_plan_text(payload: Dict[str, Any]) -> str:
    inp = payload["input_summary"]
    plan = payload["query_plan"]
    led = payload["ledger_entry"]
    lines = [
        "=== CaseGraph query plan ===",
        f"input_type: {inp['input_type']}",
        f"dataset_name: {inp['dataset_name']}",
        f"defendant_names: {', '.join(inp['defendant_names']) or '(none)'}",
        f"agency: {inp['agency'] or '(none)'}",
        f"incident_date: {inp['incident_date'] or '(none)'}",
        f"missing_fields: {inp['missing_field_count']}",
        f"risk_flags: {', '.join(inp['risk_flags']) or '(none)'}",
        "",
        f"connector_plans: {plan['connector_count']}",
    ]
    for connector_plan in plan["plans"]:
        lines.append(f"  - {connector_plan['connector']}: {connector_plan['query_count']} query/queries")
        for q in connector_plan["queries"]:
            lines.append(f"      * {q['query']} ({q['reason']})")
        if connector_plan["missing_field_requirements"]:
            missing = ", ".join(connector_plan["missing_field_requirements"])
            lines.append(f"      ! missing: {missing}")
    lines.extend(
        [
            "",
            f"ledger.experiment_id: {led['experiment_id']}",
            f"ledger.wallclock_seconds: {led['wallclock_seconds']}",
            f"ledger.estimated_cost_usd: {led['estimated_cost_usd']}",
            f"ledger.api_calls: {led['api_calls']}",
        ]
    )
    return "\n".join(lines) + "\n"


def _format_live_dry_text(payload: Dict[str, Any]) -> str:
    inp = payload["input_summary"]
    live = payload["live_dry"]
    diag = live["diagnostics"]
    led = payload["ledger_entry"]
    lines = [
        "=== CaseGraph live-dry smoke ===",
        f"connector: {live['connector']}",
        f"max_queries: {live['max_queries']}    max_results: {live['max_results']}",
        f"input_type: {inp['input_type']}",
        f"dataset_name: {inp['dataset_name']}",
        f"defendant_names: {', '.join(inp['defendant_names']) or '(none)'}",
        "",
        f"endpoint: {diag['endpoint']}",
        f"query: {diag['query']}",
        f"status_code: {diag['status_code']}",
        f"result_count: {diag['result_count']}",
        f"verified_artifact_count: {diag['verified_artifact_count']}",
        f"wallclock_seconds: {diag['wallclock_seconds']}",
        f"api_calls: {diag['api_calls']}",
        f"estimated_cost_usd: {diag['estimated_cost_usd']}",
        f"error: {diag['error']}",
        "",
        f"ledger.experiment_id: {led['experiment_id']}",
        f"ledger.wallclock_seconds: {led['wallclock_seconds']}",
        f"ledger.estimated_cost_usd: {led['estimated_cost_usd']}",
    ]
    return "\n".join(lines) + "\n"


def _run_default_mode(args, *, out, err) -> int:
    missing = _require_fixture_argument(args, mode_label="fixture", err=err)
    if missing is not None:
        return missing
    fixture_path = Path(args.fixture)
    started = time.perf_counter()
    try:
        packet = _load_fixture(fixture_path)
    except FileNotFoundError as exc:
        err.write(f"error: {exc}\n")
        return EXIT_FIXTURE_MISSING
    except (ValueError, json.JSONDecodeError) as exc:
        err.write(f"error: {exc}\n")
        return EXIT_FIXTURE_INVALID

    experiment_id = args.experiment_id or "PIPE1-cli-dry-run"
    wallclock = round(time.perf_counter() - started, 4)
    emit_handoffs = bool(getattr(args, "emit_handoffs", False))
    payload = build_dry_run_payload(
        packet,
        experiment_id=experiment_id,
        wallclock_seconds=wallclock,
        emit_handoffs=emit_handoffs,
    )

    if args.json:
        out.write(json.dumps(payload, separators=(",", ": "), sort_keys=False))
        out.write("\n")
    else:
        out.write(_format_text(payload))

    if args.bundle_out:
        bundle = build_run_bundle(
            mode="default",
            experiment_id=experiment_id,
            wallclock_seconds=wallclock,
            packet=packet,
            handoffs=build_handoffs(packet) if emit_handoffs else None,
        )
        _write_bundle(Path(args.bundle_out), bundle)
    return EXIT_OK


def _run_query_plan_mode(args, *, out, err) -> int:
    missing = _require_fixture_argument(args, mode_label="query-plan", err=err)
    if missing is not None:
        return missing
    fixture_path = Path(args.fixture)
    started = time.perf_counter()
    try:
        parsed = _parse_structured_fixture(fixture_path)
    except FileNotFoundError as exc:
        err.write(f"error: {exc}\n")
        return EXIT_FIXTURE_MISSING
    except (ValueError, json.JSONDecodeError) as exc:
        err.write(f"error: {exc}\n")
        return EXIT_FIXTURE_INVALID

    experiment_id = args.experiment_id or "PIPE1-cli-query-plan"
    wallclock = round(time.perf_counter() - started, 4)
    payload = build_query_plan_payload(
        parsed,
        experiment_id=experiment_id,
        wallclock_seconds=wallclock,
    )

    if args.json:
        out.write(json.dumps(payload, separators=(",", ": "), sort_keys=False))
        out.write("\n")
    else:
        out.write(_format_query_plan_text(payload))

    if args.bundle_out:
        plan = plan_queries_from_structured_result(parsed)
        bundle = build_run_bundle(
            mode="query_plan",
            experiment_id=experiment_id,
            wallclock_seconds=wallclock,
            parsed=parsed,
            query_plan=plan,
        )
        _write_bundle(Path(args.bundle_out), bundle)
    return EXIT_OK


def _format_multi_source_text(payload: Dict[str, Any]) -> str:
    inp = payload["input_summary"]
    multi = payload["multi_source_dry_run"]
    res = payload["result"]
    led = payload["ledger_entry"]
    lines = [
        "=== CaseGraph multi-source dry run ===",
        f"input_type: {inp['input_type']}",
        f"dataset_name: {inp['dataset_name']}",
        f"defendant_names: {', '.join(inp['defendant_names']) or '(none)'}",
        f"agency: {inp['agency'] or '(none)'}",
        "",
        f"connectors: {', '.join(multi['connectors'])}",
        f"max_results per connector: {multi['max_results']}",
        f"total_planned_queries: {multi['total_planned_queries']}",
        f"total_source_records: {multi['total_source_records']}",
        f"total_verified_artifacts: {multi['total_verified_artifacts']}",
        f"total_estimated_cost_usd: {multi['total_estimated_cost_usd']}",
        "",
    ]
    for entry in multi["per_connector"]:
        lines.append(f"  - {entry['connector']}: {entry['planned_query_count']} planned, max_results={entry['max_results']}")
        if entry["first_planned_query"]:
            lines.append(f"      first query: {entry['first_planned_query']}")
        if entry["missing_field_requirements"]:
            lines.append(f"      missing: {', '.join(entry['missing_field_requirements'])}")
        if entry["rationale"]:
            lines.append(f"      rationale: {entry['rationale']}")
    lines.extend(
        [
            "",
            f"verdict: {res['verdict']}",
            f"actionability_score: {res['actionability_score']}",
            f"reason_codes: {', '.join(res['reason_codes']) or '(none)'}",
            "",
            f"ledger.experiment_id: {led['experiment_id']}",
            f"ledger.api_calls: {led['api_calls']}",
            f"ledger.estimated_cost_usd: {led['estimated_cost_usd']}",
        ]
    )
    return "\n".join(lines) + "\n"


def _run_multi_source_dry_run_mode(args, *, out, err) -> int:
    missing = _require_fixture_argument(args, mode_label="multi-source-dry-run", err=err)
    if missing is not None:
        return missing
    raw_connectors = (args.connectors or "").strip()
    if not raw_connectors:
        err.write(
            "error: --multi-source-dry-run requires --connectors=<comma-separated list>\n"
        )
        return EXIT_FIXTURE_INVALID
    connectors = [c.strip() for c in raw_connectors.split(",") if c.strip()]
    if not connectors:
        err.write(
            "error: --multi-source-dry-run requires at least one valid connector name\n"
        )
        return EXIT_FIXTURE_INVALID

    try:
        _validate_dry_run_connectors(connectors)
    except LiveRunBlocked as exc:
        err.write(f"error: multi-source dry run refused — {exc}\n")
        return EXIT_LIVE_BLOCKED

    fixture_path = Path(args.fixture)
    started = time.perf_counter()
    try:
        parsed = _parse_structured_fixture(fixture_path)
    except FileNotFoundError as exc:
        err.write(f"error: {exc}\n")
        return EXIT_FIXTURE_MISSING
    except (ValueError, json.JSONDecodeError) as exc:
        err.write(f"error: {exc}\n")
        return EXIT_FIXTURE_INVALID

    experiment_id = args.experiment_id or "PIPE3-cli-multisource-dry-run"
    wallclock = round(time.perf_counter() - started, 4)
    payload = build_multi_source_dry_run_payload(
        parsed,
        connectors=connectors,
        max_results=int(args.max_results),
        experiment_id=experiment_id,
        wallclock_seconds=wallclock,
    )

    if args.json:
        out.write(json.dumps(payload, separators=(",", ": "), sort_keys=False))
        out.write("\n")
    else:
        out.write(_format_multi_source_text(payload))

    if args.bundle_out:
        plan = plan_queries_from_structured_result(parsed)
        assembly = assemble_structured_case_packet(parsed)
        bundle = build_run_bundle(
            mode="multi_source_dry_run",
            experiment_id=experiment_id,
            wallclock_seconds=wallclock,
            packet=assembly.packet,
            parsed=parsed,
            query_plan=plan,
            multi_source_summary=payload["multi_source_dry_run"],
            notes=[
                "multi-source-dry-run",
                f"connectors={','.join(connectors)}",
                f"max_results={int(args.max_results)}",
            ],
        )
        _write_bundle(Path(args.bundle_out), bundle)
    return EXIT_OK


def _run_live_dry_mode(args, *, out, err) -> int:
    missing = _require_fixture_argument(args, mode_label="live-dry", err=err)
    if missing is not None:
        return missing
    fixture_path = Path(args.fixture)
    started = time.perf_counter()
    try:
        parsed = _parse_structured_fixture(fixture_path)
    except FileNotFoundError as exc:
        err.write(f"error: {exc}\n")
        return EXIT_FIXTURE_MISSING
    except (ValueError, json.JSONDecodeError) as exc:
        err.write(f"error: {exc}\n")
        return EXIT_FIXTURE_INVALID

    config = LiveRunConfig(
        connector=args.connector,
        max_queries=int(args.max_queries),
        max_results=int(args.max_results),
    )
    try:
        smoke_result = run_capped_live_smoke(parsed.case_input, config=config)
    except LiveRunBlocked as exc:
        err.write(f"error: live-dry blocked — {exc}\n")
        return EXIT_LIVE_BLOCKED

    wallclock_seconds = round(time.perf_counter() - started, 4)
    experiment_id = args.experiment_id or "PIPE2-cli-live-dry"
    payload = build_live_dry_payload(
        parsed,
        config=config,
        experiment_id=experiment_id,
        smoke_result=smoke_result,
        wallclock_seconds=wallclock_seconds,
    )

    if args.json:
        out.write(json.dumps(payload, separators=(",", ": "), sort_keys=False))
        out.write("\n")
    else:
        out.write(_format_live_dry_text(payload))

    if args.bundle_out:
        diagnostics = smoke_result.to_diagnostics()
        api_calls = smoke_result.budget.to_ledger_summary().get("api_calls", {})
        bundle = build_run_bundle(
            mode="live_dry",
            experiment_id=experiment_id,
            wallclock_seconds=wallclock_seconds,
            parsed=parsed,
            smoke_diagnostics=diagnostics,
            api_calls=api_calls,
            notes=[
                f"connector={config.connector}",
                f"max_queries={config.max_queries}",
                f"max_results={config.max_results}",
            ],
        )
        _write_bundle(Path(args.bundle_out), bundle)
    return EXIT_OK


def _run_portal_replay_mode(args, *, out, err) -> int:
    """Offline portal-replay mode handler.

    Accepts either ``--fixture <path>`` (PR #9 direct mode) or
    ``--portal-manifest-entry <case_id>`` (manifest convenience mode);
    exactly one of those must be supplied. Refuses live URLs
    explicitly; loads the saved fixture; runs the deterministic
    executor + assembly + scoring chain via
    ``build_portal_replay_payload``; emits JSON or text. Bundle output
    is intentionally not extended in this PR (deferred follow-up); if
    --bundle-out is also passed, a default-shape bundle is written
    that does NOT carry a portal_replay section.
    """
    fixture_arg = args.fixture
    manifest_case_id = getattr(args, "portal_manifest_entry", None)
    if fixture_arg and manifest_case_id is not None:
        err.write(
            "error: --fixture and --portal-manifest-entry are mutually "
            "exclusive in --portal-replay mode; pass exactly one\n"
        )
        return EXIT_FIXTURE_INVALID
    if not fixture_arg and manifest_case_id is None:
        err.write(
            "error: --portal-replay requires exactly one of --fixture <path> "
            "or --portal-manifest-entry <case_id>\n"
        )
        return EXIT_FIXTURE_INVALID

    manifest_entry_metadata: Optional[Dict[str, Any]] = None
    if manifest_case_id is not None:
        try:
            entry, manifest_path = _resolve_portal_manifest_entry(int(manifest_case_id))
        except _PortalManifestEntryNotFound as exc:
            err.write(f"error: {exc}\n")
            return EXIT_FIXTURE_MISSING
        fixture_arg = str(_repo_root() / entry.mocked_payload_fixture)
        manifest_entry_metadata = {
            "case_id": entry.case_id,
            "manifest_path": _relative_fixture_path(manifest_path),
        }

    if _looks_like_live_url(str(fixture_arg or "")):
        err.write(
            "error: --portal-replay refuses live URLs; pass a saved fixture "
            "path instead\n"
        )
        return EXIT_LIVE_BLOCKED

    fixture_path = Path(fixture_arg)
    started = time.perf_counter()
    try:
        payload = _load_portal_payload(fixture_path)
    except FileNotFoundError as exc:
        err.write(f"error: {exc}\n")
        return EXIT_FIXTURE_MISSING
    except (ValueError, json.JSONDecodeError) as exc:
        err.write(f"error: {exc}\n")
        return EXIT_FIXTURE_INVALID

    experiment_id = args.experiment_id or "PIPE3-cli-portal-replay"
    wallclock = round(time.perf_counter() - started, 4)
    emit_handoffs = bool(getattr(args, "emit_handoffs", False))
    output = build_portal_replay_payload(
        payload,
        fixture_path=fixture_path,
        experiment_id=experiment_id,
        wallclock_seconds=wallclock,
        emit_handoffs=emit_handoffs,
        manifest_entry=manifest_entry_metadata,
    )

    if args.json:
        out.write(json.dumps(output, separators=(",", ": "), sort_keys=False))
        out.write("\n")
    else:
        out.write(_format_portal_replay_text(output))

    if args.bundle_out:
        # The bundle's portal_replay section reuses the JSON output's
        # section verbatim — it depends only on payload + exec_result +
        # fixture_path + manifest_entry, none of which a fresh packet
        # could change. We still build a fresh packet for the bundle's
        # canonical sections so the bundle's view is independent of
        # the JSON write step's packet state.
        packet = _build_portal_packet(payload)
        resolve_identity(packet)
        resolve_outcome(packet)
        extract_artifact_claims(packet)
        run_metadata_only_resolvers(packet)
        bundle = build_run_bundle(
            mode="portal_replay",
            experiment_id=experiment_id,
            wallclock_seconds=wallclock,
            packet=packet,
            handoffs=build_handoffs(packet) if emit_handoffs else None,
            portal_replay=output["portal_replay"],
            notes=[
                "portal-replay",
                f"profile={_portal_profile_id(payload) or 'unknown'}",
                f"fixture={_relative_fixture_path(fixture_path)}",
            ],
        )
        _write_bundle(Path(args.bundle_out), bundle)
    return EXIT_OK


def main(
    argv: Optional[Iterable[str]] = None,
    *,
    stdout: Optional[TextIO] = None,
    stderr: Optional[TextIO] = None,
) -> int:
    out = stdout or sys.stdout
    err = stderr or sys.stderr

    args = parse_args(argv)

    if args.bundle_out:
        try:
            _validate_bundle_path(
                Path(args.bundle_out),
                allow_unsafe=args.allow_unsafe_bundle_path,
            )
        except BundlePathUnsafe as exc:
            err.write(f"error: {exc}\n")
            return EXIT_BUNDLE_UNSAFE

    if args.portal_replay:
        return _run_portal_replay_mode(args, out=out, err=err)
    if args.live_dry:
        return _run_live_dry_mode(args, out=out, err=err)
    if args.query_plan:
        return _run_query_plan_mode(args, out=out, err=err)
    if args.multi_source_dry_run:
        return _run_multi_source_dry_run_mode(args, out=out, err=err)
    return _run_default_mode(args, out=out, err=err)


if __name__ == "__main__":  # pragma: no cover - module entrypoint
    sys.exit(main())
