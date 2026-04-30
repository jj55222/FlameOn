"""DATA2 — Pure no-live runner for the CaseGraph validation manifest.

Loads ``tests/fixtures/validation_manifest.json`` (or any manifest with
the same shape) and runs every entry through deterministic
:func:`score_case_packet` to emit one validation result per entry plus
an aggregate summary.

No network calls, no LLMs, no downloads, no scraping. All work happens
on already-loaded CasePackets (via the same loader the CLI uses) and
the pure scoring helper. Optionally writes a per-entry run bundle into
a caller-supplied ``bundle_dir`` for downstream comparison.

The aggregate summary surfaces the four false-PRODUCE guard counters
(``false_produce_count``, ``document_only_produce_count``,
``claim_only_produce_count``, ``weak_identity_produce_count``) so the
caller can verify that the gates are still holding across the whole
manifest in a single number-glance.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from .cli import _load_fixture, _write_bundle, build_run_bundle
from .models import CasePacket, VerifiedArtifact
from .scoring import MEDIA_ARTIFACT_TYPES, MEDIA_FORMATS, score_case_packet


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _is_media_artifact(artifact: VerifiedArtifact) -> bool:
    return (
        artifact.artifact_type in MEDIA_ARTIFACT_TYPES
        or artifact.format in MEDIA_FORMATS
    )


def _media_document_counts(packet: CasePacket) -> Dict[str, int]:
    media = sum(1 for a in packet.verified_artifacts if _is_media_artifact(a))
    document = sum(1 for a in packet.verified_artifacts if not _is_media_artifact(a))
    return {"media": media, "document": document}


def _check_codes_or_flags(
    actual: List[str],
    must_include: List[str],
    must_not_include: List[str],
) -> Dict[str, List[str]]:
    actual_set = set(actual)
    return {
        "must_include_present": sorted(set(must_include) & actual_set),
        "must_include_missing": sorted(set(must_include) - actual_set),
        "must_not_include_present": sorted(set(must_not_include) & actual_set),
    }


def _missing_fixture_result(
    entry: Mapping[str, Any], *, fixture_path: Path
) -> Dict[str, Any]:
    return {
        "id": entry.get("id", "<unknown>"),
        "fixture_path": entry.get("fixture_path", ""),
        "expected_verdict": entry.get("expected_verdict"),
        "actual_verdict": None,
        "passed": False,
        "fail_reasons": [f"fixture not found: {fixture_path}"],
        "reason_codes": [],
        "risk_flags": [],
        "next_actions": [],
        "reason_code_matches": _check_codes_or_flags(
            [],
            list(entry.get("must_include_reason_codes") or []),
            list(entry.get("must_not_include_reason_codes") or []),
        ),
        "risk_flag_matches": _check_codes_or_flags(
            [],
            list(entry.get("must_include_risk_flags") or []),
            list(entry.get("must_not_include_risk_flags") or []),
        ),
        "research_completeness_score": 0.0,
        "production_actionability_score": 0.0,
        "actionability_score": 0.0,
        "verified_artifact_count": 0,
        "media_artifact_count": 0,
        "document_artifact_count": 0,
        "input_type": entry.get("input_type"),
        "identity_confidence": None,
        "outcome_status": None,
        "bundle_path": None,
    }


def validate_entry(
    entry: Mapping[str, Any],
    *,
    repo_root: Optional[Path] = None,
    bundle_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """Run one manifest entry through scoring and return a structured
    pass/fail validation result."""

    if repo_root is None:
        repo_root = _repo_root()

    fixture_rel = str(entry["fixture_path"])
    fixture_path = repo_root / fixture_rel
    if not fixture_path.exists():
        return _missing_fixture_result(entry, fixture_path=fixture_path)

    packet = _load_fixture(fixture_path)
    result = score_case_packet(packet)
    md = _media_document_counts(packet)

    actual_verdict = result.verdict
    expected_verdict = entry["expected_verdict"]
    fail_reasons: List[str] = []
    if actual_verdict != expected_verdict:
        fail_reasons.append(
            f"verdict mismatch: actual={actual_verdict} expected={expected_verdict}"
        )

    rc_matches = _check_codes_or_flags(
        list(result.reason_codes),
        list(entry.get("must_include_reason_codes") or []),
        list(entry.get("must_not_include_reason_codes") or []),
    )
    rf_matches = _check_codes_or_flags(
        list(result.risk_flags),
        list(entry.get("must_include_risk_flags") or []),
        list(entry.get("must_not_include_risk_flags") or []),
    )
    if rc_matches["must_include_missing"]:
        fail_reasons.append(
            f"missing required reason_codes: {rc_matches['must_include_missing']}"
        )
    if rc_matches["must_not_include_present"]:
        fail_reasons.append(
            f"contains forbidden reason_codes: {rc_matches['must_not_include_present']}"
        )
    if rf_matches["must_include_missing"]:
        fail_reasons.append(
            f"missing required risk_flags: {rf_matches['must_include_missing']}"
        )
    if rf_matches["must_not_include_present"]:
        fail_reasons.append(
            f"contains forbidden risk_flags: {rf_matches['must_not_include_present']}"
        )

    bundle_path: Optional[str] = None
    if bundle_dir is not None:
        bundle_dir.mkdir(parents=True, exist_ok=True)
        bundle = build_run_bundle(
            mode="validation",
            experiment_id=f"DATA2-{entry['id']}",
            wallclock_seconds=0.0,
            packet=packet,
        )
        out_path = bundle_dir / f"{entry['id']}.json"
        _write_bundle(out_path, bundle)
        bundle_path = str(out_path)

    return {
        "id": entry["id"],
        "fixture_path": fixture_rel,
        "expected_verdict": expected_verdict,
        "actual_verdict": actual_verdict,
        "passed": not fail_reasons,
        "fail_reasons": fail_reasons,
        "reason_codes": list(result.reason_codes),
        "risk_flags": list(result.risk_flags),
        "next_actions": list(result.next_actions),
        "reason_code_matches": rc_matches,
        "risk_flag_matches": rf_matches,
        "research_completeness_score": result.research_completeness_score,
        "production_actionability_score": result.production_actionability_score,
        "actionability_score": result.actionability_score,
        "verified_artifact_count": len(packet.verified_artifacts),
        "media_artifact_count": md["media"],
        "document_artifact_count": md["document"],
        "input_type": (entry.get("input_type") or packet.input.input_type),
        "identity_confidence": packet.case_identity.identity_confidence,
        "outcome_status": packet.case_identity.outcome_status,
        "bundle_path": bundle_path,
    }


def _aggregate(results: List[Mapping[str, Any]]) -> Dict[str, Any]:
    total = len(results)
    passed = sum(1 for r in results if r["passed"])
    failed = total - passed

    verdict_counts: Dict[str, int] = {"PRODUCE": 0, "HOLD": 0, "SKIP": 0}
    expected_counts: Dict[str, int] = {"PRODUCE": 0, "HOLD": 0, "SKIP": 0}
    false_produce = 0
    document_only_produce = 0
    claim_only_produce = 0
    weak_identity_produce = 0
    protected_or_pacer_produce = 0

    for r in results:
        actual = r.get("actual_verdict")
        expected = r.get("expected_verdict")
        if actual in verdict_counts:
            verdict_counts[actual] = verdict_counts[actual] + 1
        if expected in expected_counts:
            expected_counts[expected] = expected_counts[expected] + 1
        if actual == "PRODUCE":
            if expected != "PRODUCE":
                false_produce += 1
            if r.get("media_artifact_count", 0) == 0:
                document_only_produce += 1
            if r.get("verified_artifact_count", 0) == 0:
                claim_only_produce += 1
            if r.get("identity_confidence") != "high":
                weak_identity_produce += 1
            forbidden_present = (
                r.get("risk_flag_matches", {}).get("must_not_include_present") or []
            )
            if any(
                flag
                in {"protected_or_nonpublic", "pacer_or_paywalled", "protected_or_nonpublic_only"}
                for flag in forbidden_present
            ):
                protected_or_pacer_produce += 1

    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "verdict_counts": verdict_counts,
        "expected_verdict_counts": expected_counts,
        "false_produce_count": false_produce,
        "document_only_produce_count": document_only_produce,
        "claim_only_produce_count": claim_only_produce,
        "weak_identity_produce_count": weak_identity_produce,
        "protected_or_pacer_produce_count": protected_or_pacer_produce,
    }


def run_validation_manifest(
    manifest_path: Optional[Path] = None,
    *,
    bundle_dir: Optional[Path] = None,
    manifest_dict: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Run every entry in the manifest through the no-live pipeline.

    Pass ``manifest_path`` to load from disk, or ``manifest_dict`` to
    pass a pre-loaded dict (useful in tests). When ``bundle_dir`` is
    given, each entry's run bundle is written there as
    ``<entry_id>.json``.
    """
    resolved_path: Optional[Path] = None
    if manifest_dict is None:
        if manifest_path is None:
            resolved_path = (
                _repo_root() / "tests" / "fixtures" / "validation_manifest.json"
            )
        else:
            resolved_path = Path(manifest_path)
        with resolved_path.open("r", encoding="utf-8") as f:
            manifest_dict = json.load(f)
    elif manifest_path is not None:
        resolved_path = Path(manifest_path)

    repo_root = _repo_root()
    entries = list(manifest_dict.get("entries") or [])
    results = [
        validate_entry(entry, repo_root=repo_root, bundle_dir=bundle_dir)
        for entry in entries
    ]

    return {
        "manifest_path": str(resolved_path) if resolved_path is not None else None,
        "manifest_version": int(manifest_dict.get("manifest_version") or 0),
        "total_entries": len(entries),
        "results": results,
        "summary": _aggregate(results),
    }
