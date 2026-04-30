"""EVAL3 — Cost/runtime ledger tests.

Asserts that:
- a `RunLedgerEntry` carries every field the experiment JSONL row
  needs, with stable defaults
- `build_run_ledger_entry` derives source/artifact/media/document
  counts and verdict/scores from a CasePacket via pure
  `score_case_packet` (no mutation)
- `estimate_cost` applies per-provider rates correctly and respects
  caller overrides
- `aggregate_ledger` sums multiple entries into a batch summary
- `append_ledger_entry` writes a valid JSONL row to disk
- the ledger never makes live API calls and never mutates packets
"""
import json
from copy import deepcopy
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    ArtifactClaim,
    CaseIdentity,
    CaseInput,
    CasePacket,
    Jurisdiction,
    RunLedgerEntry,
    Scores,
    SourceRecord,
    VerifiedArtifact,
    aggregate_ledger,
    append_ledger_entry,
    build_run_ledger_entry,
    estimate_cost,
    normalize_api_calls,
)
from pipeline2_discovery.casegraph.ledger import (
    COST_PER_CALL_USD,
    DEFAULT_API_CALLS,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "casegraph_scenarios"


def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def packet_from_fixture(name):
    data = load_json(FIXTURE_DIR / name)
    packet_data = {k: v for k, v in data.items() if k != "expected"}
    identity_data = dict(packet_data["case_identity"])
    identity_data["jurisdiction"] = Jurisdiction(**identity_data["jurisdiction"])
    return CasePacket(
        case_id=packet_data["case_id"],
        input=CaseInput(**packet_data["input"]),
        case_identity=CaseIdentity(**identity_data),
        sources=[SourceRecord(**s) for s in packet_data["sources"]],
        artifact_claims=[ArtifactClaim(**c) for c in packet_data["artifact_claims"]],
        verified_artifacts=[VerifiedArtifact(**a) for a in packet_data["verified_artifacts"]],
        scores=Scores(**packet_data["scores"]),
        verdict=packet_data["verdict"],
        next_actions=list(packet_data["next_actions"]),
        risk_flags=list(packet_data["risk_flags"]),
    )


# ---- estimate_cost ----------------------------------------------------------


def test_estimate_cost_free_providers_only_returns_zero():
    cost = estimate_cost({"courtlistener": 5, "muckrock": 3, "documentcloud": 2, "youtube": 4})
    assert cost == 0.0


def test_estimate_cost_brave_at_005_per_call():
    cost = estimate_cost({"brave": 10})
    assert cost == 0.05


def test_estimate_cost_firecrawl_at_001_per_call():
    cost = estimate_cost({"firecrawl": 7})
    assert cost == 0.007


def test_estimate_cost_overrides_apply_per_provider():
    # Caller models a $0.0035/request LLM rate.
    cost = estimate_cost({"llm": 100}, per_call_overrides={"llm": 0.0035})
    assert cost == 0.35


def test_estimate_cost_unknown_provider_costs_zero_unless_overridden():
    base = estimate_cost({"future_provider": 100})
    assert base == 0.0
    overridden = estimate_cost({"future_provider": 100}, per_call_overrides={"future_provider": 0.01})
    assert overridden == 1.0


def test_estimate_cost_combines_multiple_providers():
    cost = estimate_cost(
        {"brave": 10, "firecrawl": 5, "llm": 0, "courtlistener": 20},
        per_call_overrides={"llm": 0.001},
    )
    # 10 * 0.005 + 5 * 0.001 + 0 * 0.001 + 20 * 0 = 0.05 + 0.005 = 0.055
    assert cost == 0.055


# ---- normalize_api_calls ----------------------------------------------------


def test_normalize_api_calls_fills_default_zeros_for_missing_providers():
    normalized = normalize_api_calls({"brave": 3})
    for provider in DEFAULT_API_CALLS:
        assert provider in normalized
    assert normalized["brave"] == 3
    assert normalized["courtlistener"] == 0


def test_normalize_api_calls_handles_none_input():
    normalized = normalize_api_calls(None)
    assert normalized == DEFAULT_API_CALLS
    # Returns a copy — caller mutations must not leak into module state.
    normalized["brave"] = 99
    assert DEFAULT_API_CALLS["brave"] == 0


def test_normalize_api_calls_preserves_unknown_providers():
    normalized = normalize_api_calls({"new_provider": 4})
    assert normalized["new_provider"] == 4


# ---- RunLedgerEntry shape ---------------------------------------------------


def test_run_ledger_entry_default_values_are_zero_filled():
    entry = RunLedgerEntry(experiment_id="EVAL3-test")
    assert entry.experiment_id == "EVAL3-test"
    assert entry.case_id is None
    assert entry.api_calls == DEFAULT_API_CALLS
    assert entry.wallclock_seconds == 0.0
    assert entry.source_record_count == 0
    assert entry.verified_artifact_count == 0
    assert entry.media_artifact_count == 0
    assert entry.document_artifact_count == 0
    assert entry.estimated_cost_usd == 0.0
    assert entry.verdict is None
    assert entry.research_completeness_score is None
    assert entry.production_actionability_score is None
    assert entry.actionability_score is None
    assert entry.notes == []
    assert isinstance(entry.timestamp, str) and "T" in entry.timestamp


def test_run_ledger_entry_to_dict_has_canonical_api_call_keys():
    entry = RunLedgerEntry(experiment_id="x", api_calls={"brave": 2})
    payload = entry.to_dict()
    for provider in DEFAULT_API_CALLS:
        assert provider in payload["api_calls"]
    assert payload["api_calls"]["brave"] == 2


def test_run_ledger_entry_to_jsonl_row_round_trips_via_json_loads():
    entry = RunLedgerEntry(
        experiment_id="EVAL3-roundtrip",
        api_calls={"brave": 4, "courtlistener": 2},
        wallclock_seconds=1.234,
        notes=["smoke test"],
    )
    parsed = json.loads(entry.to_jsonl_row())
    assert parsed["experiment_id"] == "EVAL3-roundtrip"
    assert parsed["api_calls"]["brave"] == 4
    assert parsed["api_calls"]["courtlistener"] == 2
    assert parsed["wallclock_seconds"] == 1.234
    assert parsed["notes"] == ["smoke test"]


# ---- build_run_ledger_entry -------------------------------------------------


def test_build_run_ledger_entry_without_packet_carries_supplied_fields_only():
    entry = build_run_ledger_entry(
        experiment_id="LIVE-preflight",
        api_calls={"brave": 3},
        wallclock_seconds=0.5,
        notes=["preflight smoke"],
    )
    assert entry.experiment_id == "LIVE-preflight"
    assert entry.case_id is None
    assert entry.api_calls["brave"] == 3
    assert entry.wallclock_seconds == 0.5
    assert entry.estimated_cost_usd == 0.015  # 3 * $0.005
    assert entry.notes == ["preflight smoke"]
    # No packet ⇒ source/artifact counts stay zero, verdict stays None.
    assert entry.source_record_count == 0
    assert entry.verified_artifact_count == 0
    assert entry.verdict is None


def test_build_run_ledger_entry_with_media_rich_packet_records_yields_and_verdict():
    packet = packet_from_fixture("media_rich_produce.json")
    entry = build_run_ledger_entry(
        experiment_id="EVAL3-media-rich",
        packet=packet,
        api_calls={"courtlistener": 1, "muckrock": 1},
        wallclock_seconds=0.42,
    )
    assert entry.case_id == packet.case_id
    assert entry.verdict == "PRODUCE"
    assert entry.verified_artifact_count == len(packet.verified_artifacts)
    assert entry.media_artifact_count >= 1
    assert entry.production_actionability_score is not None
    assert entry.production_actionability_score >= 70
    # Free providers ⇒ zero cost.
    assert entry.estimated_cost_usd == 0.0


def test_build_run_ledger_entry_does_not_mutate_packet():
    packet = packet_from_fixture("media_rich_produce.json")
    snapshot = deepcopy(packet.to_dict())

    build_run_ledger_entry(experiment_id="EVAL3-mutation-check", packet=packet)

    assert packet.to_dict() == snapshot


def test_build_run_ledger_entry_uses_supplied_cost_when_provided():
    entry = build_run_ledger_entry(
        experiment_id="EVAL3-supplied-cost",
        api_calls={"brave": 10},
        estimated_cost_usd=0.999,  # caller overrode
    )
    assert entry.estimated_cost_usd == 0.999


def test_build_run_ledger_entry_separates_media_and_document_counts():
    packet = packet_from_fixture("multi_artifact_premium_produce.json")
    entry = build_run_ledger_entry(experiment_id="EVAL3-counts", packet=packet)
    assert entry.media_artifact_count >= 1
    # The media-rich and multi-artifact-premium fixtures both ship at
    # least one document among their verified artifacts (foia exhibits).
    # If a fixture changes that, this should adjust.
    assert entry.media_artifact_count + entry.document_artifact_count == entry.verified_artifact_count


def test_build_run_ledger_entry_carries_zero_counts_for_no_artifact_packet():
    packet = packet_from_fixture("claim_only_hold.json")
    entry = build_run_ledger_entry(experiment_id="EVAL3-claim-only", packet=packet)
    assert entry.verified_artifact_count == 0
    assert entry.media_artifact_count == 0
    assert entry.document_artifact_count == 0
    assert entry.verdict == "HOLD"


# ---- aggregate_ledger -------------------------------------------------------


def test_aggregate_ledger_empty_returns_zero_summary():
    summary = aggregate_ledger([])
    assert summary["run_count"] == 0
    assert summary["api_calls_total"] == DEFAULT_API_CALLS
    assert summary["wallclock_seconds_total"] == 0.0
    assert summary["estimated_cost_usd_total"] == 0.0
    assert summary["verdict_counts"] == {"PRODUCE": 0, "HOLD": 0, "SKIP": 0, "unknown": 0}


def test_aggregate_ledger_sums_api_calls_and_costs_across_runs():
    e1 = build_run_ledger_entry(
        experiment_id="r1",
        api_calls={"brave": 5, "courtlistener": 1},
        wallclock_seconds=1.0,
    )
    e2 = build_run_ledger_entry(
        experiment_id="r2",
        api_calls={"brave": 3, "muckrock": 2},
        wallclock_seconds=2.0,
    )
    summary = aggregate_ledger([e1, e2])
    assert summary["run_count"] == 2
    assert summary["api_calls_total"]["brave"] == 8
    assert summary["api_calls_total"]["courtlistener"] == 1
    assert summary["api_calls_total"]["muckrock"] == 2
    assert summary["wallclock_seconds_total"] == 3.0
    # 8 brave at $0.005 = $0.04 total.
    assert summary["estimated_cost_usd_total"] == 0.04
    assert summary["experiment_ids"] == ["r1", "r2"]


def test_aggregate_ledger_counts_verdicts_with_unknown_bucket_for_no_packet_runs():
    no_packet = build_run_ledger_entry(experiment_id="no-packet", api_calls={})
    media_rich = build_run_ledger_entry(
        experiment_id="media-rich", packet=packet_from_fixture("media_rich_produce.json")
    )
    document_only = build_run_ledger_entry(
        experiment_id="document-only",
        packet=packet_from_fixture("document_only_hold.json"),
    )
    summary = aggregate_ledger([no_packet, media_rich, document_only])
    assert summary["verdict_counts"]["PRODUCE"] == 1
    assert summary["verdict_counts"]["HOLD"] == 1
    assert summary["verdict_counts"]["unknown"] == 1


# ---- append_ledger_entry ----------------------------------------------------


def test_append_ledger_entry_writes_valid_jsonl_row(tmp_path):
    path = tmp_path / "test_experiments.jsonl"
    entry = build_run_ledger_entry(
        experiment_id="EVAL3-disk",
        api_calls={"brave": 1},
        wallclock_seconds=0.1,
        notes=["disk write"],
    )
    append_ledger_entry(entry, str(path))
    append_ledger_entry(entry, str(path))  # appended again, not overwritten

    lines = path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    for line in lines:
        parsed = json.loads(line)
        assert parsed["experiment_id"] == "EVAL3-disk"
        assert parsed["api_calls"]["brave"] == 1
        assert parsed["estimated_cost_usd"] == 0.005


def test_append_ledger_entry_creates_file_if_missing(tmp_path):
    path = tmp_path / "new_experiments.jsonl"
    assert not path.exists()
    entry = RunLedgerEntry(experiment_id="EVAL3-create")
    append_ledger_entry(entry, str(path))
    assert path.exists()
    parsed = json.loads(path.read_text(encoding="utf-8").strip())
    assert parsed["experiment_id"] == "EVAL3-create"


# ---- Live-call invariant ----------------------------------------------------


def test_ledger_module_does_not_import_or_call_live_apis(monkeypatch):
    """The ledger module is read-only: it must not perform HTTP, must
    not import requests at module import time, and must not call any
    LIVE smoke. Build entries against several packets and confirm the
    process never touches the network.

    We assert this by monkey-patching the requests.Session.get method
    on the requests module BEFORE running build_run_ledger_entry, then
    confirming no calls were made through it.
    """
    import requests

    calls = []
    original_get = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original_get(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)

    for fixture in (
        "media_rich_produce.json",
        "document_only_hold.json",
        "claim_only_hold.json",
        "weak_identity_media_blocked.json",
    ):
        packet = packet_from_fixture(fixture)
        entry = build_run_ledger_entry(experiment_id="EVAL3-no-live", packet=packet)
        assert entry.case_id == packet.case_id

    assert calls == [], (
        f"ledger build path made {len(calls)} live HTTP call(s); ledger must be pure"
    )
