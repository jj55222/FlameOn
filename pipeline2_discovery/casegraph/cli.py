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
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, TextIO

from .assembly import StructuredAssemblyResult, assemble_structured_case_packet
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
from .query_planner import QueryPlanResult, plan_queries_from_structured_result
from .reporting import build_actionability_report
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


def _packet_summary(packet: CasePacket) -> Dict[str, Any]:
    return {
        "case_id": packet.case_id,
        "source_count": len(packet.sources),
        "artifact_claim_count": len(packet.artifact_claims),
        "verified_artifact_count": len(packet.verified_artifacts),
        "verified_artifact_types": sorted({a.artifact_type for a in packet.verified_artifacts}),
        "identity_confidence": packet.case_identity.identity_confidence,
        "outcome_status": packet.case_identity.outcome_status,
        "packet_verdict": packet.verdict,
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

    return {
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


def _write_bundle(path: Path, bundle: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2, sort_keys=False)
        f.write("\n")


def build_dry_run_payload(
    packet: CasePacket,
    *,
    experiment_id: str = "PIPE1-cli-dry-run",
    wallclock_seconds: float = 0.0,
) -> Dict[str, Any]:
    """Build the structured payload printed by the CLI.

    Pure: scores and reports run via deterministic helpers and never
    mutate the supplied packet."""
    result = score_case_packet(packet)
    report = build_actionability_report([packet])
    ledger_entry = build_run_ledger_entry(
        experiment_id=experiment_id,
        packet=packet,
        wallclock_seconds=wallclock_seconds,
    )
    return {
        "input_summary": _input_summary(packet),
        "packet_summary": _packet_summary(packet),
        "result": _result_summary(result),
        "report": report,
        "ledger_entry": ledger_entry.to_dict(),
    }


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
        "packet_summary": _packet_summary(assembly.packet),
        "result": _result_summary(assembly.actionability),
        "report": report,
        "ledger_entry": ledger_entry.to_dict(),
    }


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
        required=True,
        help=(
            "Path to a JSON fixture. Default mode expects a CasePacket "
            "fixture (e.g. tests/fixtures/casegraph_scenarios/...). "
            "--query-plan and --live-dry expect a structured-row fixture "
            "(e.g. tests/fixtures/structured_inputs/wapo_uof_complete.json)."
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
    payload = build_dry_run_payload(
        packet,
        experiment_id=experiment_id,
        wallclock_seconds=wallclock,
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
        )
        _write_bundle(Path(args.bundle_out), bundle)
    return EXIT_OK


def _run_query_plan_mode(args, *, out, err) -> int:
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

    if args.live_dry:
        return _run_live_dry_mode(args, out=out, err=err)
    if args.query_plan:
        return _run_query_plan_mode(args, out=out, err=err)
    if args.multi_source_dry_run:
        return _run_multi_source_dry_run_mode(args, out=out, err=err)
    return _run_default_mode(args, out=out, err=err)


if __name__ == "__main__":  # pragma: no cover - module entrypoint
    sys.exit(main())
