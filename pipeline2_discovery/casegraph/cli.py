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

from .inputs import (
    StructuredInputParseResult,
    parse_fatal_encounters_case_input,
    parse_mapping_police_violence_case_input,
    parse_wapo_uof_case_input,
)
from .ledger import RunLedgerEntry, build_run_ledger_entry
from .live_safety import LiveRunBlocked, LiveRunConfig
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
    parser.add_argument(
        "--connector",
        default="courtlistener",
        help="Connector for --live-dry (default: courtlistener).",
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

    payload = build_dry_run_payload(
        packet,
        experiment_id=args.experiment_id or "PIPE1-cli-dry-run",
        wallclock_seconds=round(time.perf_counter() - started, 4),
    )

    if args.json:
        out.write(json.dumps(payload, separators=(",", ": "), sort_keys=False))
        out.write("\n")
    else:
        out.write(_format_text(payload))
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

    payload = build_query_plan_payload(
        parsed,
        experiment_id=args.experiment_id or "PIPE1-cli-query-plan",
        wallclock_seconds=round(time.perf_counter() - started, 4),
    )

    if args.json:
        out.write(json.dumps(payload, separators=(",", ": "), sort_keys=False))
        out.write("\n")
    else:
        out.write(_format_query_plan_text(payload))
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
    payload = build_live_dry_payload(
        parsed,
        config=config,
        experiment_id=args.experiment_id or "PIPE2-cli-live-dry",
        smoke_result=smoke_result,
        wallclock_seconds=wallclock_seconds,
    )

    if args.json:
        out.write(json.dumps(payload, separators=(",", ": "), sort_keys=False))
        out.write("\n")
    else:
        out.write(_format_live_dry_text(payload))
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

    if args.live_dry:
        return _run_live_dry_mode(args, out=out, err=err)
    if args.query_plan:
        return _run_query_plan_mode(args, out=out, err=err)
    return _run_default_mode(args, out=out, err=err)


if __name__ == "__main__":  # pragma: no cover - module entrypoint
    sys.exit(main())
