"""PIPE1 — CaseGraph dry-run CLI.

No-live entrypoint that loads a CasePacket fixture, scores it via the
pure `score_case_packet`, optionally builds a single-packet actionability
report, builds a RunLedgerEntry, and emits a structured summary on stdout.

Usage:
    .venv\\Scripts\\python.exe -m pipeline2_discovery.casegraph.cli \\
        --fixture tests/fixtures/casegraph_scenarios/media_rich_produce.json \\
        --json

Or, omit ``--json`` for a human-readable text summary.

The module performs zero network calls. It does not download files,
fetch transcripts, scrape pages, or call any LLM. All inputs are read
from disk and all aggregation is deterministic.

Exit codes:
- ``0`` — success
- ``2`` — bad CLI usage (argparse handles)
- ``3`` — fixture path not found
- ``4`` — fixture is not valid JSON or fails the CasePacket schema
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, TextIO

from .ledger import RunLedgerEntry, build_run_ledger_entry
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
from .reporting import build_actionability_report
from .scoring import ActionabilityResult, score_case_packet


EXIT_OK = 0
EXIT_FIXTURE_MISSING = 3
EXIT_FIXTURE_INVALID = 4


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


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipeline2_discovery.casegraph.cli",
        description=(
            "No-live CaseGraph dry-run. Loads a CasePacket fixture, scores it, "
            "and prints a structured summary. Performs zero network calls."
        ),
    )
    parser.add_argument(
        "--fixture",
        required=True,
        help="Path to a CasePacket JSON fixture (e.g. tests/fixtures/casegraph_scenarios/media_rich_produce.json).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON on stdout instead of human-readable text.",
    )
    parser.add_argument(
        "--experiment-id",
        default="PIPE1-cli-dry-run",
        help="experiment_id field for the emitted RunLedgerEntry (default: PIPE1-cli-dry-run).",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(
    argv: Optional[Iterable[str]] = None,
    *,
    stdout: Optional[TextIO] = None,
    stderr: Optional[TextIO] = None,
) -> int:
    out = stdout or sys.stdout
    err = stderr or sys.stderr

    args = parse_args(argv)
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
        experiment_id=args.experiment_id,
        wallclock_seconds=round(time.perf_counter() - started, 4),
    )

    if args.json:
        out.write(json.dumps(payload, separators=(",", ": "), sort_keys=False))
        out.write("\n")
    else:
        out.write(_format_text(payload))
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover - module entrypoint
    sys.exit(main())
