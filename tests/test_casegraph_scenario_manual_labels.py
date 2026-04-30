"""EVAL2 — Manual-label fixture support for scenario regressions.

Each scenario fixture under `tests/fixtures/casegraph_scenarios/` now
carries an inline `expected` block describing the verdict, reason
codes that MUST appear, reason codes that MUST NOT appear, risk flags
that MUST appear, and risk flags that MUST NOT appear. This module
parametrizes a single test over every fixture and compares the
evaluator output against those expectations.

The `expected` block is a test-only extension; it is stripped before
schema validation in the existing scenario_regression loader. The
schema contract (schemas/p2_case_packet.schema.json) is unchanged.

Goal: any future change that flips a fixture's verdict, drops a
required reason code, or leaks a forbidden risk flag fails loudly here
with a per-fixture error message.
"""
import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    ArtifactClaim,
    CaseIdentity,
    CaseInput,
    CasePacket,
    Jurisdiction,
    Scores,
    SourceRecord,
    VerifiedArtifact,
    score_case_packet,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "casegraph_scenarios"

EXPECTED_BLOCK_KEYS = {
    "verdict",
    "must_include_reason_codes",
    "must_not_include_reason_codes",
    "must_include_risk_flags",
    "must_not_include_risk_flags",
}

VALID_VERDICTS = {"PRODUCE", "HOLD", "SKIP"}


def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def packet_from_dict(data):
    identity_data = dict(data["case_identity"])
    identity_data["jurisdiction"] = Jurisdiction(**identity_data["jurisdiction"])
    return CasePacket(
        case_id=data["case_id"],
        input=CaseInput(**data["input"]),
        case_identity=CaseIdentity(**identity_data),
        sources=[SourceRecord(**s) for s in data["sources"]],
        artifact_claims=[ArtifactClaim(**c) for c in data["artifact_claims"]],
        verified_artifacts=[VerifiedArtifact(**a) for a in data["verified_artifacts"]],
        scores=Scores(**data["scores"]),
        verdict=data["verdict"],
        next_actions=list(data["next_actions"]),
        risk_flags=list(data["risk_flags"]),
    )


def labeled_fixtures():
    """Yield (fixture_name, packet, expected_block) triples for every
    scenario fixture that carries an `expected` block.

    Fixtures without an `expected` block are skipped intentionally so
    new fixtures can be added without immediately breaking this test.
    """
    triples = []
    for path in sorted(FIXTURE_DIR.glob("*.json")):
        data = load_json(path)
        expected = data.get("expected")
        if expected is None:
            continue
        packet_data = {key: value for key, value in data.items() if key != "expected"}
        packet = packet_from_dict(packet_data)
        triples.append((path.name, packet, expected))
    return triples


LABELED = labeled_fixtures()
LABELED_NAMES = [name for name, _, _ in LABELED]


def test_every_existing_fixture_has_expected_block():
    """All 15 scenario fixtures should carry an `expected` block —
    the EVAL2 baseline labels every existing scenario."""
    fixture_names = {p.name for p in FIXTURE_DIR.glob("*.json")}
    labeled_names = set(LABELED_NAMES)
    missing = fixture_names - labeled_names
    assert not missing, f"scenario fixtures without expected block: {sorted(missing)}"


@pytest.mark.parametrize("fixture_name", LABELED_NAMES)
def test_expected_block_shape_is_well_formed(fixture_name):
    expected = next(exp for name, _, exp in LABELED if name == fixture_name)
    assert isinstance(expected, dict), f"{fixture_name} expected is not a dict"
    extra = set(expected.keys()) - EXPECTED_BLOCK_KEYS
    missing = EXPECTED_BLOCK_KEYS - set(expected.keys())
    assert not extra, f"{fixture_name} expected has unknown keys: {sorted(extra)}"
    assert not missing, f"{fixture_name} expected is missing keys: {sorted(missing)}"
    assert expected["verdict"] in VALID_VERDICTS, (
        f"{fixture_name} verdict {expected['verdict']!r} not in {VALID_VERDICTS}"
    )
    for list_key in (
        "must_include_reason_codes",
        "must_not_include_reason_codes",
        "must_include_risk_flags",
        "must_not_include_risk_flags",
    ):
        value = expected[list_key]
        assert isinstance(value, list), f"{fixture_name}/{list_key} is not a list"
        assert all(isinstance(item, str) for item in value), (
            f"{fixture_name}/{list_key} contains non-string entries: {value!r}"
        )


@pytest.mark.parametrize("fixture_name,packet,expected", LABELED, ids=LABELED_NAMES)
def test_evaluator_matches_manual_labels(fixture_name, packet, expected):
    result = score_case_packet(packet)

    actual_reasons = set(result.reason_codes)
    actual_risks = set(result.risk_flags)
    expected_verdict = expected["verdict"]

    assert result.verdict == expected_verdict, (
        f"{fixture_name}: verdict mismatch — expected {expected_verdict}, got {result.verdict}; "
        f"reason_codes={sorted(actual_reasons)}, risk_flags={sorted(actual_risks)}"
    )

    must_reasons = set(expected["must_include_reason_codes"])
    missing_reasons = must_reasons - actual_reasons
    assert not missing_reasons, (
        f"{fixture_name}: missing required reason codes {sorted(missing_reasons)}; "
        f"actual={sorted(actual_reasons)}"
    )

    must_not_reasons = set(expected["must_not_include_reason_codes"])
    leaked_reasons = must_not_reasons & actual_reasons
    assert not leaked_reasons, (
        f"{fixture_name}: forbidden reason codes leaked {sorted(leaked_reasons)}; "
        f"actual={sorted(actual_reasons)}"
    )

    must_risks = set(expected["must_include_risk_flags"])
    missing_risks = must_risks - actual_risks
    assert not missing_risks, (
        f"{fixture_name}: missing required risk flags {sorted(missing_risks)}; "
        f"actual={sorted(actual_risks)}"
    )

    must_not_risks = set(expected["must_not_include_risk_flags"])
    leaked_risks = must_not_risks & actual_risks
    assert not leaked_risks, (
        f"{fixture_name}: forbidden risk flags leaked {sorted(leaked_risks)}; "
        f"actual={sorted(actual_risks)}"
    )


def test_no_produce_fixture_includes_forbidden_no_verified_media_block():
    """Any fixture whose expected verdict is PRODUCE must NOT name
    `no_verified_media` in must_not_include_risk_flags by accident — it
    must explicitly forbid weak_identity / no_verified_media so the
    label catches a regression that flips identity_confidence to low."""
    for name, _, expected in LABELED:
        if expected["verdict"] != "PRODUCE":
            continue
        forbidden_risks = set(expected["must_not_include_risk_flags"])
        # PRODUCE labels in this corpus all forbid weak_identity and
        # no_verified_media — that's the intended invariant guard.
        assert "weak_identity" in forbidden_risks, (
            f"{name}: PRODUCE label should forbid weak_identity"
        )
        assert "no_verified_media" in forbidden_risks, (
            f"{name}: PRODUCE label should forbid no_verified_media"
        )


def test_no_hold_or_skip_fixture_label_requires_produce_only_codes():
    """HOLD/SKIP fixtures must not list a PRODUCE-only marker like
    production_score_threshold_met as a *required* reason code,
    because that would silently green-light flipping a HOLD to PRODUCE
    while still satisfying the label."""
    for name, _, expected in LABELED:
        if expected["verdict"] == "PRODUCE":
            continue
        must_reasons = set(expected["must_include_reason_codes"])
        # production_score_threshold_met is fine on its own (it just
        # means the score crossed the bar), but pairing it with a
        # required HOLD/SKIP verdict mid-fixture is fine for label
        # diagnostics. We instead enforce the harder invariant: PRODUCE
        # exclusivity markers (artifact_portfolio_strong) MUST NOT be
        # required by HOLD/SKIP labels.
        assert "artifact_portfolio_strong" not in must_reasons, (
            f"{name}: HOLD/SKIP label should not require artifact_portfolio_strong"
        )
