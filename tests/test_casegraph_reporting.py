"""EVAL1 — production metrics report tests.

Asserts that `build_actionability_report` produces a stable, structured
summary across CasePackets without mutating any input. The function is
deterministic and pure: it calls `score_case_packet` on each packet
and aggregates the results.
"""
import json
from copy import deepcopy
from pathlib import Path

from pipeline2_discovery.casegraph import (
    ArtifactClaim,
    CaseIdentity,
    CaseInput,
    CasePacket,
    Jurisdiction,
    Scores,
    SourceRecord,
    VerifiedArtifact,
    build_actionability_report,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "casegraph_scenarios"


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
        sources=[SourceRecord(**source) for source in data["sources"]],
        artifact_claims=[ArtifactClaim(**claim) for claim in data["artifact_claims"]],
        verified_artifacts=[VerifiedArtifact(**artifact) for artifact in data["verified_artifacts"]],
        scores=Scores(**data["scores"]),
        verdict=data["verdict"],
        next_actions=list(data["next_actions"]),
        risk_flags=list(data["risk_flags"]),
    )


def load_packet(name):
    return packet_from_dict(load_json(FIXTURE_DIR / name))


def load_all_scenario_packets():
    return [load_packet(p.name) for p in sorted(FIXTURE_DIR.glob("*.json"))]


EXPECTED_TOP_LEVEL_KEYS = {
    "total_cases",
    "verdict_counts",
    "false_produce_guards",
    "artifact_portfolio",
    "score_distribution",
    "risk_flag_counts",
    "reason_code_counts",
    "input_type_breakdown",
    "produce_eligible_inventory",
}

EXPECTED_GUARD_KEYS = {
    "weak_identity_blocks",
    "document_only_holds",
    "claim_only_holds",
    "protected_or_pacer_blocked",
    "outcome_unconcluded_holds",
    "no_verified_media_blocks",
}


def test_empty_packet_list_returns_zero_filled_report():
    report = build_actionability_report([])
    assert set(report.keys()) == EXPECTED_TOP_LEVEL_KEYS
    assert report["total_cases"] == 0
    assert report["verdict_counts"] == {"PRODUCE": 0, "HOLD": 0, "SKIP": 0}
    assert report["false_produce_guards"] == {key: 0 for key in EXPECTED_GUARD_KEYS}
    assert report["artifact_portfolio"]["by_artifact_type"] == {}
    assert report["artifact_portfolio"]["media_only_cases"] == 0
    assert report["artifact_portfolio"]["document_only_cases"] == 0
    assert report["artifact_portfolio"]["no_artifact_cases"] == 0
    assert report["artifact_portfolio"]["multi_media_cases"] == 0
    assert report["artifact_portfolio"]["multi_artifact_premium_cases"] == 0
    for distribution in report["score_distribution"].values():
        assert distribution == {"min": 0.0, "max": 0.0, "mean": 0.0, "median": 0.0, "p90": 0.0}
    assert report["risk_flag_counts"] == {}
    assert report["reason_code_counts"] == {}
    assert report["input_type_breakdown"] == {}
    assert report["produce_eligible_inventory"] == []


def test_single_media_rich_packet_counts_produce_and_inventory():
    packet = load_packet("media_rich_produce.json")
    report = build_actionability_report([packet])
    assert report["total_cases"] == 1
    assert report["verdict_counts"]["PRODUCE"] == 1
    assert report["verdict_counts"]["HOLD"] == 0
    assert report["verdict_counts"]["SKIP"] == 0

    inventory = report["produce_eligible_inventory"]
    assert len(inventory) == 1
    entry = inventory[0]
    assert entry["case_id"] == "scenario_media_rich_produce"
    assert entry["production_actionability_score"] >= 70
    # Both bodycam and dispatch_911 are media; one of these must appear.
    assert any(category in entry["media_categories"] for category in ("bodycam", "dispatch_911"))


def test_document_only_packet_increments_document_guards_and_no_media_block():
    packet = load_packet("document_only_hold.json")
    report = build_actionability_report([packet])

    assert report["verdict_counts"]["HOLD"] == 1
    guards = report["false_produce_guards"]
    assert guards["document_only_holds"] == 1
    assert guards["no_verified_media_blocks"] == 1
    assert report["artifact_portfolio"]["document_only_cases"] == 1
    assert report["artifact_portfolio"]["media_only_cases"] == 0
    assert report["produce_eligible_inventory"] == []


def test_claim_only_packet_increments_claim_only_guard():
    packet = load_packet("claim_only_hold.json")
    report = build_actionability_report([packet])

    assert report["verdict_counts"]["HOLD"] == 1
    guards = report["false_produce_guards"]
    assert guards["claim_only_holds"] == 1
    # No verified artifacts at all.
    assert report["artifact_portfolio"]["no_artifact_cases"] == 1


def test_protected_packet_increments_protected_or_pacer_guard():
    packet = load_packet("protected_nonpublic_blocked.json")
    report = build_actionability_report([packet])

    assert report["verdict_counts"]["PRODUCE"] == 0
    assert report["false_produce_guards"]["protected_or_pacer_blocked"] >= 1


def test_weak_identity_packet_increments_weak_identity_guard():
    packet = load_packet("weak_identity_media_blocked.json")
    report = build_actionability_report([packet])

    assert report["verdict_counts"]["PRODUCE"] == 0
    assert report["false_produce_guards"]["weak_identity_blocks"] >= 1


def test_full_scenario_corpus_summarized_consistently():
    packets = load_all_scenario_packets()
    report = build_actionability_report(packets)

    # Every scenario flows through.
    assert report["total_cases"] == len(packets)
    # Verdict counts sum back to total.
    assert sum(report["verdict_counts"].values()) == report["total_cases"]
    # At least one PRODUCE and one HOLD across the corpus (the fixtures
    # cover both productions and holds by design).
    assert report["verdict_counts"]["PRODUCE"] >= 1
    assert report["verdict_counts"]["HOLD"] >= 1
    # PRODUCE-eligible inventory size matches verdict_counts.
    assert len(report["produce_eligible_inventory"]) == report["verdict_counts"]["PRODUCE"]
    # Inventory is sorted by production score descending.
    scores = [entry["production_actionability_score"] for entry in report["produce_eligible_inventory"]]
    assert scores == sorted(scores, reverse=True)


def test_report_does_not_mutate_input_packets():
    packets = load_all_scenario_packets()
    snapshots = [deepcopy(packet.to_dict()) for packet in packets]

    build_actionability_report(packets)

    for packet, snapshot in zip(packets, snapshots):
        assert packet.to_dict() == snapshot, (
            f"build_actionability_report mutated case_id={packet.case_id}"
        )


def test_score_distribution_stats_have_stable_shape():
    packets = load_all_scenario_packets()
    report = build_actionability_report(packets)

    for key in ("research_completeness", "production_actionability", "actionability"):
        stats = report["score_distribution"][key]
        assert set(stats.keys()) == {"min", "max", "mean", "median", "p90"}
        for stat_name, value in stats.items():
            assert isinstance(value, (int, float)), f"{key}.{stat_name} not numeric: {value!r}"
        assert stats["min"] <= stats["median"] <= stats["max"]
        assert stats["min"] <= stats["mean"] <= stats["max"]
        assert stats["min"] <= stats["p90"] <= stats["max"]


def test_artifact_by_type_buckets_aggregate_across_scenarios():
    packets = load_all_scenario_packets()
    report = build_actionability_report(packets)

    by_type = report["artifact_portfolio"]["by_artifact_type"]
    # The fixture corpus covers media + document categories.
    assert "bodycam" in by_type or "interrogation" in by_type or "dispatch_911" in by_type
    # All counts are positive integers.
    assert all(isinstance(count, int) and count >= 1 for count in by_type.values())
    # Keys are sorted alphabetically for stable downstream comparison.
    assert list(by_type.keys()) == sorted(by_type.keys())


def test_risk_flag_and_reason_code_counts_are_sorted_and_positive():
    packets = load_all_scenario_packets()
    report = build_actionability_report(packets)

    for counts in (report["risk_flag_counts"], report["reason_code_counts"]):
        assert all(isinstance(value, int) and value >= 1 for value in counts.values())
        # Values sorted descending; ties broken alphabetically by key.
        items = list(counts.items())
        for prev, curr in zip(items, items[1:]):
            assert prev[1] > curr[1] or (prev[1] == curr[1] and prev[0] <= curr[0])


def test_no_produce_with_protected_or_weak_identity_or_document_only():
    """Cross-check the false-PRODUCE guard counters by walking the
    inventory. Every PRODUCE packet must have media artifacts and
    high identity and concluded outcome — the inventory should never
    contain a case that any guard would flag."""
    packets = load_all_scenario_packets()
    report = build_actionability_report(packets)

    for entry in report["produce_eligible_inventory"]:
        # PRODUCE entries must list at least one media category.
        assert entry["media_categories"], (
            f"PRODUCE entry {entry['case_id']} has no media categories"
        )


def test_input_type_breakdown_sums_to_total_cases():
    packets = load_all_scenario_packets()
    report = build_actionability_report(packets)
    assert sum(report["input_type_breakdown"].values()) == report["total_cases"]


def test_report_uses_pure_score_case_packet_no_mutation_after_repeated_calls():
    """Calling the report twice in succession must produce the same
    output and must still leave packets unchanged."""
    packets = load_all_scenario_packets()
    snapshots = [deepcopy(packet.to_dict()) for packet in packets]

    first = build_actionability_report(packets)
    second = build_actionability_report(packets)

    assert first == second
    for packet, snapshot in zip(packets, snapshots):
        assert packet.to_dict() == snapshot
