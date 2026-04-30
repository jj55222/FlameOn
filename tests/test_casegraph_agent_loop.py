"""LOOP1 — agent-loop controller scaffold tests.

Asserts that the agent loop controller:

- exposes a stable BACKLOG and BACKLOG_VERSION
- backlog entries carry every required field (experiment_id,
  milestone, hypothesis, files_to_touch, tests_to_run,
  stop_conditions, expected_metrics, requires_live, depends_on)
- plan_next_experiment returns LOOP1 first when nothing is completed
  and live is disabled
- plan_next_experiment returns PILOT2 once LOOP1 is in completed_ids
- plan_next_experiment respects within-batch dependencies (EVAL7
  only emerges after PILOT2 is marked completed)
- plan_next_experiment skips live-flagged experiments unless
  live_enabled=True (LIVE6 should never appear without live_enabled
  even when its deps are met)
- plan_next_experiment surfaces LIVE6 only when live_enabled=True
  AND its prerequisites are completed
- plan_next_experiment returns None when every backlog entry is
  completed
- plan_next_batch returns at most max_experiments plans, in priority
  order, honoring within-batch dependencies (PILOT2 then EVAL7 in
  the same batch)
- plan_next_batch refuses non-positive max_experiments (ValueError)
- plan_next_batch output is JSON-serializable
- plan_next_batch carries current_state with validation accuracy and
  guard counter snapshot
- read_completed_ids_from_ledger parses the local jsonl ledger and
  filters by decision='keep'
- assess_current_state returns the canonical snapshot shape
- write_plan refuses non-ignored repo paths unless allow_unsafe=True
- write_plan accepts paths under autoresearch/.runs (gitignored)
- module makes zero network calls during planning
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline2_discovery.casegraph import (
    BACKLOG,
    BACKLOG_VERSION,
    BacklogEntry,
    assess_current_state,
    plan_next_batch,
    plan_next_experiment,
    read_completed_ids_from_ledger,
)
from pipeline2_discovery.casegraph.agent_loop import write_plan


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "tests" / "fixtures" / "validation_manifest.json"


REQUIRED_PLAN_KEYS = (
    "experiment_id",
    "milestone",
    "hypothesis",
    "files_to_touch",
    "tests_to_run",
    "stop_conditions",
    "expected_metrics",
    "requires_live",
    "depends_on",
)


def test_backlog_version_is_set():
    assert isinstance(BACKLOG_VERSION, int)
    assert BACKLOG_VERSION >= 1


def test_backlog_is_non_empty_and_priority_ordered():
    assert len(BACKLOG) >= 6
    ids = [entry.experiment_id for entry in BACKLOG]
    # First entry should be LOOP1 (the controller itself).
    assert ids[0].startswith("LOOP1-")
    # PILOT2 should come before EVAL7 (EVAL7 depends on PILOT2).
    pilot2_idx = ids.index("PILOT2-pilot-manifest-no-live-runner")
    eval7_idx = ids.index("EVAL7-pilot-validation-scoreboard")
    assert pilot2_idx < eval7_idx


def test_every_backlog_entry_has_canonical_shape():
    for entry in BACKLOG:
        assert isinstance(entry, BacklogEntry)
        plan = entry.to_plan_dict()
        for key in REQUIRED_PLAN_KEYS:
            assert key in plan, f"{entry.experiment_id} missing {key!r}"


def test_plan_next_experiment_returns_loop1_first_when_nothing_completed():
    plan = plan_next_experiment(completed_ids=[], live_enabled=False)
    assert plan is not None
    assert plan["experiment_id"].startswith("LOOP1-")
    assert plan["requires_live"] is False


def test_plan_next_experiment_returns_pilot2_after_loop1_completed():
    plan = plan_next_experiment(
        completed_ids=["LOOP1-agent-loop-controller-scaffold"],
        live_enabled=False,
    )
    assert plan is not None
    assert plan["experiment_id"] == "PILOT2-pilot-manifest-no-live-runner"


def test_plan_next_experiment_returns_eval7_only_after_pilot2_completed():
    plan = plan_next_experiment(
        completed_ids=[
            "LOOP1-agent-loop-controller-scaffold",
            "PILOT2-pilot-manifest-no-live-runner",
        ],
        live_enabled=False,
    )
    assert plan is not None
    assert plan["experiment_id"] == "EVAL7-pilot-validation-scoreboard"


def test_plan_next_experiment_skips_live_experiments_when_live_disabled():
    """Even when LIVE6's prerequisites are completed, plan_next_experiment
    must NOT propose it without live_enabled=True."""
    completed = [
        "LOOP1-agent-loop-controller-scaffold",
        "PILOT2-pilot-manifest-no-live-runner",
        "EVAL7-pilot-validation-scoreboard",
        "SOURCE1-agency-ois-connector-mocked",
        "SOURCE2-agency-ois-media-document-resolver",
        "MEDIA1-media-url-policy",
    ]
    plan = plan_next_experiment(completed_ids=completed, live_enabled=False)
    # All non-live entries done; only LIVE6 remains, but live disabled -> None.
    assert plan is None


def test_plan_next_experiment_surfaces_live6_only_when_live_enabled():
    completed = [
        "LOOP1-agent-loop-controller-scaffold",
        "PILOT2-pilot-manifest-no-live-runner",
        "EVAL7-pilot-validation-scoreboard",
    ]
    plan = plan_next_experiment(completed_ids=completed, live_enabled=True)
    assert plan is not None
    # First eligible non-live entry will still come ahead of LIVE6 in priority
    # order. LIVE6 is the LAST entry in the backlog.
    assert plan["experiment_id"] != "LIVE6-known-case-pilot-smoke"


def test_plan_next_experiment_returns_live6_when_only_live_remains():
    """When every non-live entry is completed AND live_enabled=True,
    LIVE6 should be the next plan."""
    completed = [
        "LOOP1-agent-loop-controller-scaffold",
        "PILOT2-pilot-manifest-no-live-runner",
        "EVAL7-pilot-validation-scoreboard",
        "SOURCE1-agency-ois-connector-mocked",
        "SOURCE2-agency-ois-media-document-resolver",
        "MEDIA1-media-url-policy",
    ]
    plan = plan_next_experiment(completed_ids=completed, live_enabled=True)
    assert plan is not None
    assert plan["experiment_id"] == "LIVE6-known-case-pilot-smoke"


def test_plan_next_experiment_returns_none_when_all_completed():
    completed = [entry.experiment_id for entry in BACKLOG]
    plan = plan_next_experiment(completed_ids=completed, live_enabled=True)
    assert plan is None


def test_plan_next_batch_returns_at_most_max_experiments(tmp_path):
    out = plan_next_batch(
        completed_ids=[],
        live_enabled=False,
        max_experiments=3,
        manifest_path=MANIFEST_PATH,
        ledger_path=tmp_path / "no_ledger.jsonl",
    )
    assert out["max_experiments"] == 3
    assert len(out["experiments"]) <= 3
    # Within the same batch, EVAL7 must follow PILOT2 (dependency
    # honored across the batch boundary).
    ids = [exp["experiment_id"] for exp in out["experiments"]]
    if "EVAL7-pilot-validation-scoreboard" in ids and "PILOT2-pilot-manifest-no-live-runner" in ids:
        assert ids.index("PILOT2-pilot-manifest-no-live-runner") < ids.index(
            "EVAL7-pilot-validation-scoreboard"
        )


def test_plan_next_batch_first_entry_is_loop1_when_nothing_completed(tmp_path):
    out = plan_next_batch(
        completed_ids=[],
        live_enabled=False,
        manifest_path=MANIFEST_PATH,
        ledger_path=tmp_path / "no_ledger.jsonl",
    )
    assert out["experiments"][0]["experiment_id"].startswith("LOOP1-")


def test_plan_next_batch_refuses_non_positive_max_experiments(tmp_path):
    with pytest.raises(ValueError):
        plan_next_batch(
            completed_ids=[],
            max_experiments=0,
            manifest_path=MANIFEST_PATH,
            ledger_path=tmp_path / "no_ledger.jsonl",
        )
    with pytest.raises(ValueError):
        plan_next_batch(
            completed_ids=[],
            max_experiments=-1,
            manifest_path=MANIFEST_PATH,
            ledger_path=tmp_path / "no_ledger.jsonl",
        )


def test_plan_next_batch_output_is_json_serializable(tmp_path):
    out = plan_next_batch(
        completed_ids=[],
        live_enabled=False,
        manifest_path=MANIFEST_PATH,
        ledger_path=tmp_path / "no_ledger.jsonl",
    )
    encoded = json.dumps(out)
    decoded = json.loads(encoded)
    assert decoded == out


def test_plan_next_batch_includes_current_state_snapshot(tmp_path):
    out = plan_next_batch(
        completed_ids=[],
        live_enabled=False,
        manifest_path=MANIFEST_PATH,
        ledger_path=tmp_path / "no_ledger.jsonl",
    )
    snap = out["current_state"]
    assert "validation_accuracy_pct" in snap
    assert "guard_counters" in snap
    assert "guard_counters_all_zero" in snap
    assert isinstance(snap["validation_accuracy_pct"], (int, float))


def test_plan_next_batch_marks_no_more_experiments_when_all_complete(tmp_path):
    completed = [entry.experiment_id for entry in BACKLOG]
    out = plan_next_batch(
        completed_ids=completed,
        live_enabled=True,
        manifest_path=MANIFEST_PATH,
        ledger_path=tmp_path / "no_ledger.jsonl",
    )
    assert out["no_more_experiments"] is True
    assert out["experiments"] == []


def test_read_completed_ids_from_ledger_filters_to_keep_decisions(tmp_path):
    ledger = tmp_path / "experiments.jsonl"
    rows = [
        {"experiment_id": "EXP-A", "decision": "keep"},
        {"experiment_id": "EXP-B", "decision": "revert"},
        {"experiment_id": "EXP-C", "decision": "keep"},
        {"experiment_id": "EXP-D", "decision": "park"},
        {"experiment_id": "EXP-E"},
    ]
    with ledger.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
        # Add a malformed line and an empty line to ensure they're skipped.
        f.write("not json at all\n")
        f.write("\n")
    completed = read_completed_ids_from_ledger(ledger)
    assert completed == ["EXP-A", "EXP-C"]


def test_read_completed_ids_from_ledger_handles_missing_file(tmp_path):
    completed = read_completed_ids_from_ledger(tmp_path / "does_not_exist.jsonl")
    assert completed == []


def test_assess_current_state_returns_canonical_snapshot(tmp_path):
    snap = assess_current_state(
        manifest_path=MANIFEST_PATH,
        ledger_path=tmp_path / "no_ledger.jsonl",
    )
    for key in (
        "validation_manifest_entries",
        "validation_passed",
        "validation_failed",
        "validation_accuracy_pct",
        "false_produce_count",
        "guard_counters_all_zero",
        "guard_counters",
        "completed_experiment_ids",
    ):
        assert key in snap, f"missing snapshot key {key!r}"
    # The committed validation manifest is fully passing today.
    assert snap["validation_failed"] == 0
    assert snap["validation_accuracy_pct"] == 100.0
    assert snap["guard_counters_all_zero"] is True


def test_write_plan_refuses_unsafe_repo_path():
    target = ROOT / "tests" / "_loop1_unsafe_plan.json"
    if target.exists():
        target.unlink()
    plan = {"experiments": []}
    try:
        with pytest.raises(ValueError):
            write_plan(target, plan)
        assert not target.exists()
    finally:
        if target.exists():
            target.unlink()


def test_write_plan_writes_to_safe_runs_path():
    safe_dir = ROOT / "autoresearch" / ".runs" / "loop1_test"
    safe_dir.mkdir(parents=True, exist_ok=True)
    target = safe_dir / "plan.json"
    if target.exists():
        target.unlink()
    plan = {
        "backlog_version": BACKLOG_VERSION,
        "experiments": [],
        "no_more_experiments": True,
    }
    try:
        path = write_plan(target, plan)
        assert path.exists()
        assert json.loads(path.read_text(encoding="utf-8"))["backlog_version"] == BACKLOG_VERSION
    finally:
        if target.exists():
            target.unlink()
        try:
            safe_dir.rmdir()
        except OSError:
            pass


def test_write_plan_accepts_unsafe_path_with_override(tmp_path):
    target = tmp_path / "plan.json"
    plan = {"experiments": []}
    path = write_plan(target, plan, allow_unsafe=True)
    assert path.exists()


def test_planning_makes_zero_network_calls(monkeypatch, tmp_path):
    import requests

    calls = []
    original = requests.Session.get

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    plan_next_experiment(completed_ids=[], live_enabled=False)
    plan_next_batch(
        completed_ids=[],
        live_enabled=False,
        manifest_path=MANIFEST_PATH,
        ledger_path=tmp_path / "no_ledger.jsonl",
    )
    assess_current_state(
        manifest_path=MANIFEST_PATH,
        ledger_path=tmp_path / "no_ledger.jsonl",
    )
    assert calls == [], f"agent loop made {len(calls)} live HTTP call(s)"
