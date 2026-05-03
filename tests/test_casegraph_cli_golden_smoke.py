"""GOLDEN — End-to-end CLI smoke / regression tests.

Subprocess-driven black-box tests that invoke the CaseGraph CLI as
operators do:

    python -m pipeline2_discovery.casegraph.cli \
        --fixture <fixture> --emit-handoffs --json

These guard the operator-facing surface against regressions that
in-process tests can miss (import-order side effects, fresh-interpreter
environment, OS process boundary). Six scenarios cover the canonical
product-level outcomes:

1. media-rich PRODUCE
2. charged-with-media advisory PRODUCE (post-PR5 doctrine)
3. document-only HOLD
4. weak-identity HOLD
5. protected/non-public HOLD
6. claim-only HOLD

Assertions deliberately stay at the stable product level: exit code,
JSON shape, verdict, selected risk/reason/advisory flags, handoff key
presence, schema validity, and artifact-row count lower bounds (or
intentional zero rows). Full-JSON snapshots, timestamps, ledger
timings, exact report prose, and stderr emptiness are NOT asserted.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict

import pytest


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "casegraph_scenarios"
SCHEMA_DIR = ROOT / "schemas"
SUBPROCESS_TIMEOUT_SECONDS = 30


def _run_cli_emit_handoffs(fixture_name: str) -> SimpleNamespace:
    """Invoke the CaseGraph CLI as a subprocess with --emit-handoffs.

    Uses sys.executable so the venv interpreter resolves correctly on
    every platform. cwd is pinned to the repo root so module discovery
    works without relying on the caller's working directory. Returns
    a SimpleNamespace with returncode, stdout, stderr, and an already-
    parsed payload (None when exit code is non-zero).
    """
    fixture_path = FIXTURE_DIR / fixture_name
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pipeline2_discovery.casegraph.cli",
            "--fixture",
            os.fspath(fixture_path),
            "--emit-handoffs",
            "--json",
        ],
        cwd=os.fspath(ROOT),
        capture_output=True,
        text=True,
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
    )
    payload: Dict[str, Any] | None = None
    if proc.returncode == 0:
        try:
            payload = json.loads(proc.stdout)
        except json.JSONDecodeError as exc:  # pragma: no cover
            payload = None
            pytest.fail(
                f"CLI exit 0 but stdout did not parse as JSON for "
                f"{fixture_name}: {exc}\nstdout (head): {proc.stdout[:400]!r}"
            )
    return SimpleNamespace(
        returncode=proc.returncode,
        stdout=proc.stdout,
        stderr=proc.stderr,
        payload=payload,
    )


def _assert_valid(schema_name: str, instance: Any) -> None:
    """Validate a payload against a JSON schema. Skips when jsonschema
    is not importable so the suite stays runnable in minimal envs."""
    try:
        from jsonschema import Draft7Validator  # type: ignore
    except ImportError:  # pragma: no cover
        pytest.skip("jsonschema not installed")
    schema = json.loads((SCHEMA_DIR / schema_name).read_text(encoding="utf-8"))
    errors = sorted(
        Draft7Validator(schema).iter_errors(instance), key=lambda e: list(e.path)
    )
    assert not errors, "; ".join(
        f"{list(e.path)}: {e.message}" for e in errors
    )


def _validate_handoffs(handoffs: Dict[str, Any]) -> None:
    """Per-row P3 + P4 + P5 schema validation, in one helper so each
    scenario test reads cleanly."""
    assert sorted(handoffs.keys()) == ["p2_to_p3", "p2_to_p4", "p2_to_p5"], (
        f"unexpected handoff keys: {sorted(handoffs.keys())}"
    )
    for row in handoffs["p2_to_p3"]:
        _assert_valid("p2_to_p3.schema.json", row)
    _assert_valid("p2_to_p4.schema.json", handoffs["p2_to_p4"])
    _assert_valid("p2_to_p5.schema.json", handoffs["p2_to_p5"])


# ---- 1. media-rich PRODUCE ---------------------------------------------


@pytest.fixture(scope="module")
def media_rich_run() -> SimpleNamespace:
    return _run_cli_emit_handoffs("media_rich_produce.json")


def test_media_rich_cli_exits_zero_and_emits_handoffs(media_rich_run):
    assert media_rich_run.returncode == 0, (
        f"CLI exited {media_rich_run.returncode}; stderr: "
        f"{media_rich_run.stderr[:400]!r}"
    )
    payload = media_rich_run.payload
    assert payload is not None
    assert "handoffs" in payload, "expected top-level handoffs key"
    assert payload["result"]["verdict"] == "PRODUCE"


def test_media_rich_handoffs_validate_and_carry_artifacts(media_rich_run):
    payload = media_rich_run.payload
    assert payload is not None
    handoffs = payload["handoffs"]
    _validate_handoffs(handoffs)
    # media-rich fixture has 3 verified artifacts (bodycam + dispatch +
    # docket_docs) → P3 must surface at least one row.
    assert len(handoffs["p2_to_p3"]) >= 1
    # P4 case_id must match the assembled packet's case_id.
    assert handoffs["p2_to_p4"]["case_id"] == payload["packet_summary"]["case_id"]


# ---- 2. charged-with-media advisory PRODUCE ----------------------------


@pytest.fixture(scope="module")
def charged_advisory_run() -> SimpleNamespace:
    return _run_cli_emit_handoffs("charged_with_media_hold.json")


def test_charged_advisory_cli_produces_by_default(charged_advisory_run):
    assert charged_advisory_run.returncode == 0, charged_advisory_run.stderr
    payload = charged_advisory_run.payload
    assert payload is not None
    # Post-PR5 doctrine: outcome is advisory by default; charged +
    # high identity + verified media → PRODUCE.
    assert payload["result"]["verdict"] == "PRODUCE"


def test_charged_advisory_signals_surface_in_result_section(charged_advisory_run):
    payload = charged_advisory_run.payload
    assert payload is not None
    reasons = set(payload["result"]["reason_codes"])
    risks = set(payload["result"]["risk_flags"])
    expected_advisory = {"outcome_not_concluded_advisory", "produce_with_pending_outcome"}
    assert expected_advisory <= reasons, (
        f"missing advisory reason codes; got {sorted(reasons)}"
    )
    assert expected_advisory <= risks, (
        f"missing advisory risk flags; got {sorted(risks)}"
    )


def test_charged_advisory_p5_handoff_carries_pending_outcome_advisory(charged_advisory_run):
    """The PR #12 adapter consistency fix routes the freshly computed
    ActionabilityResult advisories into the P5 handoff via
    ``export_p2_to_p5(packet, score_result=...)``. The CLI's
    ``build_handoffs`` threads ``score_result`` through, so the P5
    handoff now mirrors the root ``result`` section's advisory
    surface.

    This test was previously a softened "documents the gap" placeholder
    (the gap was tracked from PR #7 onward). Now strengthened to
    assert advisory presence directly. ``score_case_packet`` remains
    pure — the packet itself is not mutated; the merge happens inside
    the exporter.
    """
    payload = charged_advisory_run.payload
    assert payload is not None
    p5 = payload["handoffs"]["p2_to_p5"]
    assert isinstance(p5, dict) and p5, "p2_to_p5 should be a non-empty object"
    _assert_valid("p2_to_p5.schema.json", p5)
    assert p5["case_id"] == payload["packet_summary"]["case_id"]
    p5_risks = set(p5.get("risk_flags") or [])
    p5_next_actions_text = " ".join(p5.get("next_actions") or []).lower()
    assert "outcome_not_concluded_advisory" in p5_risks, (
        f"P5 risk_flags should carry the freshly computed advisory; "
        f"got {sorted(p5_risks)}"
    )
    assert "produce_with_pending_outcome" in p5_risks, (
        f"P5 risk_flags should carry the produce-with-pending-outcome "
        f"advisory; got {sorted(p5_risks)}"
    )
    assert "pending-outcome" in p5_next_actions_text, (
        f"P5 next_actions should describe the pending-outcome caveat; "
        f"got {p5.get('next_actions')}"
    )


def test_charged_advisory_handoffs_validate(charged_advisory_run):
    payload = charged_advisory_run.payload
    assert payload is not None
    _validate_handoffs(payload["handoffs"])


# ---- 3. document-only HOLD ---------------------------------------------


@pytest.fixture(scope="module")
def document_only_run() -> SimpleNamespace:
    return _run_cli_emit_handoffs("document_only_hold.json")


def test_document_only_cli_holds_with_document_gate_signals(document_only_run):
    assert document_only_run.returncode == 0, document_only_run.stderr
    payload = document_only_run.payload
    assert payload is not None
    assert payload["result"]["verdict"] == "HOLD"
    reasons = set(payload["result"]["reason_codes"])
    # Document-only fixture must keep the document/media gate signals.
    assert "document_only_hold" in reasons or "no_verified_media" in reasons, (
        f"document-only gate signal missing; reasons={sorted(reasons)}"
    )


def test_document_only_handoffs_validate(document_only_run):
    payload = document_only_run.payload
    assert payload is not None
    _validate_handoffs(payload["handoffs"])


# ---- 4. weak identity HOLD ---------------------------------------------


@pytest.fixture(scope="module")
def weak_identity_run() -> SimpleNamespace:
    return _run_cli_emit_handoffs("weak_identity_media_blocked.json")


def test_weak_identity_cli_does_not_produce(weak_identity_run):
    assert weak_identity_run.returncode == 0, weak_identity_run.stderr
    payload = weak_identity_run.payload
    assert payload is not None
    assert payload["result"]["verdict"] != "PRODUCE"
    risks = set(payload["result"]["risk_flags"])
    assert "weak_identity" in risks or "identity_unconfirmed" in risks, (
        f"weak-identity gate signal missing; risks={sorted(risks)}"
    )


def test_weak_identity_handoffs_validate(weak_identity_run):
    payload = weak_identity_run.payload
    assert payload is not None
    _validate_handoffs(payload["handoffs"])


# ---- 5. protected / non-public HOLD ------------------------------------


@pytest.fixture(scope="module")
def protected_run() -> SimpleNamespace:
    return _run_cli_emit_handoffs("protected_nonpublic_blocked.json")


def test_protected_cli_does_not_produce(protected_run):
    assert protected_run.returncode == 0, protected_run.stderr
    payload = protected_run.payload
    assert payload is not None
    assert payload["result"]["verdict"] != "PRODUCE"
    risks = set(payload["result"]["risk_flags"])
    assert (
        "protected_or_nonpublic" in risks
        or "protected_or_nonpublic_only" in risks
    ), f"protected-URL gate signal missing; risks={sorted(risks)}"


def test_protected_handoffs_validate_and_p3_intentionally_empty(protected_run):
    """Protected/PACER fixture has no public artifact URLs — P3 rows
    are expected to be zero. P4 and P5 still validate as objects."""
    payload = protected_run.payload
    assert payload is not None
    handoffs = payload["handoffs"]
    _validate_handoffs(handoffs)
    assert handoffs["p2_to_p3"] == [], (
        f"expected zero P3 rows for protected-only fixture; got "
        f"{len(handoffs['p2_to_p3'])} row(s)"
    )


# ---- 6. claim-only HOLD ------------------------------------------------


@pytest.fixture(scope="module")
def claim_only_run() -> SimpleNamespace:
    return _run_cli_emit_handoffs("claim_only_hold.json")


def test_claim_only_cli_holds_with_claim_signals(claim_only_run):
    assert claim_only_run.returncode == 0, claim_only_run.stderr
    payload = claim_only_run.payload
    assert payload is not None
    assert payload["result"]["verdict"] == "HOLD"
    # claim_source != possible_artifact_source doctrine: artifact
    # claims exist; verified artifacts do not.
    assert payload["packet_summary"]["artifact_claim_count"] >= 1
    assert payload["packet_summary"]["verified_artifact_count"] == 0
    reasons = set(payload["result"]["reason_codes"])
    assert "claim_only_hold" in reasons or "artifact_claim_unresolved" in reasons, (
        f"claim-only gate signal missing; reasons={sorted(reasons)}"
    )


def test_claim_only_handoffs_validate_with_zero_p3_rows(claim_only_run):
    """Claim-only doctrine: no verified artifacts → no P3 rows.
    P4 and P5 still validate as schema-conformant objects."""
    payload = claim_only_run.payload
    assert payload is not None
    handoffs = payload["handoffs"]
    _validate_handoffs(handoffs)
    assert handoffs["p2_to_p3"] == [], (
        f"expected zero P3 rows for claim-only fixture; got "
        f"{len(handoffs['p2_to_p3'])} row(s)"
    )
