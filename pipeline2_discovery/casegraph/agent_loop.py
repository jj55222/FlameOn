"""LOOP1 — Pure no-live CaseGraph agent-loop controller.

Formalizes the controlled OBSERVE -> DECIDE -> ACT -> VALIDATE -> RECORD
-> STOP loop by reading current state and the approved experiment
backlog, then emitting a deterministic JSON plan for the next batch.

The controller does NOT autonomously edit code. It only reads
existing artifacts (validation manifest, pilot manifest, experiments
ledger) and proposes the next experiment(s) to run, with each plan
including the experiment_id, hypothesis, files_to_touch, tests_to_run,
stop_conditions, and expected_metrics drawn from the approved backlog.

Pure: no network, no LLM, no file writes except via the optional
``write_plan`` helper (which refuses non-ignored repo paths through
the same default-safe policy as PIPE5's ``--bundle-out``).

Live experiments are skipped unless the caller passes
``live_enabled=True``. Within-batch dependencies are honored so
``plan_next_batch`` will not propose, e.g., EVAL7 before PILOT2 even
in the same batch.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .reporting import build_validation_metrics_report
from .validation import run_validation_manifest


BACKLOG_VERSION = 1
MAX_EXPERIMENTS_PER_BATCH_DEFAULT = 3


@dataclass(frozen=True)
class BacklogEntry:
    experiment_id: str
    milestone: str
    hypothesis: str
    files_to_touch: Tuple[str, ...]
    tests_to_run: Tuple[str, ...]
    stop_conditions: Tuple[str, ...]
    expected_metrics: Tuple[str, ...]
    requires_live: bool = False
    depends_on: Tuple[str, ...] = field(default_factory=tuple)

    def to_plan_dict(self) -> Dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "milestone": self.milestone,
            "hypothesis": self.hypothesis,
            "files_to_touch": list(self.files_to_touch),
            "tests_to_run": list(self.tests_to_run),
            "stop_conditions": list(self.stop_conditions),
            "expected_metrics": list(self.expected_metrics),
            "requires_live": self.requires_live,
            "depends_on": list(self.depends_on),
        }


BACKLOG: Tuple[BacklogEntry, ...] = (
    BacklogEntry(
        experiment_id="LOOP1-agent-loop-controller-scaffold",
        milestone="agent_loop_controller",
        hypothesis=(
            "A deterministic, no-live controller can read validation metrics, the "
            "pilot manifest, and the approved backlog, and emit a single JSON plan "
            "for the next experiment(s) without itself making any code changes."
        ),
        files_to_touch=(
            "pipeline2_discovery/casegraph/agent_loop.py",
            "tests/test_casegraph_agent_loop.py",
        ),
        tests_to_run=(
            "tests/test_casegraph_agent_loop.py",
        ),
        stop_conditions=(
            "live calls",
            "non-deterministic selection",
            "module imports network or LLM client",
        ),
        expected_metrics=(
            "tests_passed >= 12",
            "live_calls == 0",
            "estimated_cost_usd == 0.0",
        ),
        requires_live=False,
        depends_on=(),
    ),
    BacklogEntry(
        experiment_id="PILOT2-pilot-manifest-no-live-runner",
        milestone="pilot_manifest_runner",
        hypothesis=(
            "A pure pilot runner can iterate every entry in pilot_manifest.json, "
            "score the seed fixture in dry mode, and emit per-pilot readiness "
            "(ready_for_live_smoke / blocked_*) plus next_actions, without any "
            "network call."
        ),
        files_to_touch=(
            "pipeline2_discovery/casegraph/pilots.py",
            "tests/test_casegraph_pilot_runner.py",
        ),
        tests_to_run=(
            "tests/test_casegraph_pilot_runner.py",
        ),
        stop_conditions=(
            "live call",
            "budget bypass",
            "PRODUCE on a pilot whose seed alone shouldn't graduate",
            "paid connector reaches the runner",
        ),
        expected_metrics=(
            "tests_passed >= 12",
            "live_calls == 0",
            "estimated_cost_usd == 0.0",
        ),
        requires_live=False,
        depends_on=(),
    ),
    BacklogEntry(
        experiment_id="EVAL7-pilot-validation-scoreboard",
        milestone="pilot_validation_scoreboard",
        hypothesis=(
            "A merged scoreboard over DATA2 validation metrics + PILOT2 readiness "
            "can flag over-budget pilots, paid-connector usage, and "
            "media_required_for_produce=false in a single deterministic JSON output, "
            "without any network call."
        ),
        files_to_touch=(
            "pipeline2_discovery/casegraph/reporting.py",
            "tests/test_casegraph_pilot_scoreboard.py",
        ),
        tests_to_run=(
            "tests/test_casegraph_pilot_scoreboard.py",
        ),
        stop_conditions=(
            "scoreboard misses an over-budget / paid / missing-media-gate flag",
            "live call",
        ),
        expected_metrics=(
            "tests_passed >= 8",
            "live_calls == 0",
            "estimated_cost_usd == 0.0",
        ),
        requires_live=False,
        depends_on=("PILOT2-pilot-manifest-no-live-runner",),
    ),
    BacklogEntry(
        experiment_id="SOURCE1-agency-ois-connector-mocked",
        milestone="agency_ois_connector",
        hypothesis=(
            "A fixture-based agency OIS / critical-incident connector can emit "
            "SourceRecords from official agency listings without any live scrape, "
            "respecting source_authority=official, claim_source for release "
            "language, possible_artifact_source only when a concrete public URL "
            "exists, and creating no VerifiedArtifact at the connector layer."
        ),
        files_to_touch=(
            "pipeline2_discovery/casegraph/connectors/agency_ois.py",
            "tests/test_casegraph_agency_ois_connector.py",
            "tests/fixtures/agency_ois/",
        ),
        tests_to_run=(
            "tests/test_casegraph_agency_ois_connector.py",
        ),
        stop_conditions=(
            "live network call",
            "scraping",
            "VerifiedArtifact created at connector layer",
            "claim text without URL graduates to artifact_source role",
        ),
        expected_metrics=(
            "tests_passed >= 6",
            "live_calls == 0",
            "estimated_cost_usd == 0.0",
        ),
        requires_live=False,
        depends_on=(),
    ),
    BacklogEntry(
        experiment_id="SOURCE2-agency-ois-media-document-resolver",
        milestone="agency_ois_resolver",
        hypothesis=(
            "A metadata-only agency OIS resolver can graduate concrete public "
            ".mp4/.mov/.webm/.pdf URLs from agency OIS SourceRecords into "
            "VerifiedArtifacts (media or document), reject login/private/protected "
            "links, never graduate claim text without a URL, and never produce "
            "PRODUCE on its own (identity/outcome gates still apply)."
        ),
        files_to_touch=(
            "pipeline2_discovery/casegraph/resolvers/agency_ois_files.py",
            "tests/test_casegraph_agency_ois_file_resolver.py",
        ),
        tests_to_run=(
            "tests/test_casegraph_agency_ois_file_resolver.py",
        ),
        stop_conditions=(
            "claim-only graduates to VerifiedArtifact",
            "protected/login URL graduates",
            "live network call",
            "download",
        ),
        expected_metrics=(
            "tests_passed >= 8",
            "live_calls == 0",
            "estimated_cost_usd == 0.0",
        ),
        requires_live=False,
        depends_on=("SOURCE1-agency-ois-connector-mocked",),
    ),
    BacklogEntry(
        experiment_id="MEDIA1-media-url-policy",
        milestone="media_url_policy",
        hypothesis=(
            "A central media URL classification policy can deterministically "
            "classify .mp4/.mov/.webm/.m3u8/.mp3/.wav/.m4a, YouTube and Vimeo "
            "watch/embed URLs, and official-hosted media as media artifact "
            "candidates, while rejecting protected/login/auth/token URLs, "
            "non-public portals, thumbnails, and generic pages without media "
            "indicators - returning artifact_type, format, risk_flags, and "
            "verification_method, with no network call and no download."
        ),
        files_to_touch=(
            "pipeline2_discovery/casegraph/media_policy.py",
            "tests/test_casegraph_media_policy.py",
        ),
        tests_to_run=(
            "tests/test_casegraph_media_policy.py",
        ),
        stop_conditions=(
            "live call",
            "download",
            "non-deterministic classification",
            "private/protected URL classified as media",
        ),
        expected_metrics=(
            "tests_passed >= 12",
            "live_calls == 0",
            "estimated_cost_usd == 0.0",
        ),
        requires_live=False,
        depends_on=(),
    ),
    BacklogEntry(
        experiment_id="LIVE6-known-case-pilot-smoke",
        milestone="pilot_live_smoke",
        hypothesis=(
            "Exactly one pilot from pilot_manifest.json can be exercised live "
            "under its own declared budget (allowed_connectors, max_live_calls, "
            "max_results_per_connector), with metadata-only resolvers, no paid "
            "providers, no downloads, no scraping, no LLM. Run bundle written to "
            "ignored .runs; ledger entry created; comparison report and "
            "pilot/validation scoreboard updated."
        ),
        files_to_touch=(
            "tests/test_casegraph_live_known_case_pilot_smoke.py",
        ),
        tests_to_run=(
            "tests/test_casegraph_live_known_case_pilot_smoke.py",
        ),
        stop_conditions=(
            "exceed manifest's max_live_calls",
            "paid provider call",
            "download",
            "scraping",
            "transcript fetching",
            "LLM call",
            "PACER/login URL graduates to artifact",
        ),
        expected_metrics=(
            "tests_passed >= 1",
            "live_calls <= manifest.max_live_calls",
            "estimated_cost_usd == 0.0",
            "verified_artifact_count >= 0",
        ),
        requires_live=True,
        depends_on=(
            "PILOT2-pilot-manifest-no-live-runner",
            "EVAL7-pilot-validation-scoreboard",
        ),
    ),
)


# Default ignored output directories - same as PIPE5's BUNDLE_SAFE_DIRS.
PLAN_SAFE_DIRS: Tuple[str, ...] = (
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


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _is_safe_plan_path(path: Path) -> bool:
    abs_path = path.resolve()
    repo = _repo_root()
    try:
        rel = abs_path.relative_to(repo)
    except ValueError:
        return True
    rel_parts = rel.parts
    for safe in PLAN_SAFE_DIRS:
        safe_parts = tuple(safe.split("/"))
        if rel_parts[: len(safe_parts)] == safe_parts:
            return True
    return False


def read_completed_ids_from_ledger(ledger_path: Path) -> List[str]:
    """Parse autoresearch/.runs/experiments.jsonl and return all
    ``experiment_id`` values whose row has ``decision == 'keep'``.

    Pure read; does not touch the network. Returns an empty list when
    the file does not exist."""
    completed: List[str] = []
    path = Path(ledger_path)
    if not path.exists():
        return completed
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            if row.get("decision") != "keep":
                continue
            exp_id = row.get("experiment_id")
            if isinstance(exp_id, str) and exp_id:
                completed.append(exp_id)
    return completed


def assess_current_state(
    *,
    manifest_path: Optional[Path] = None,
    ledger_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Return a brief read-only snapshot of current state used to
    inform the controller's selection. Pure: no network, no LLM."""
    completed_ids: List[str] = []
    if ledger_path is not None:
        completed_ids = read_completed_ids_from_ledger(ledger_path)

    if manifest_path is None:
        manifest_path = _repo_root() / "tests" / "fixtures" / "validation_manifest.json"

    validation_output = run_validation_manifest(manifest_path)
    metrics = build_validation_metrics_report(validation_output)
    summary = validation_output["summary"]

    guards_zero = (
        metrics["guard_counters"]["document_only_produce_count"] == 0
        and metrics["guard_counters"]["claim_only_produce_count"] == 0
        and metrics["guard_counters"]["weak_identity_produce_count"] == 0
        and metrics["guard_counters"]["protected_or_pacer_produce_count"] == 0
    )

    return {
        "validation_manifest_entries": validation_output["total_entries"],
        "validation_passed": summary["passed"],
        "validation_failed": summary["failed"],
        "validation_accuracy_pct": metrics["verdict_accuracy"]["accuracy_pct"],
        "false_produce_count": metrics["false_verdicts"]["false_produce_count"],
        "guard_counters_all_zero": guards_zero,
        "guard_counters": dict(metrics["guard_counters"]),
        "completed_experiment_ids": list(completed_ids),
    }


def _eligible_entries(
    *,
    completed_ids: Sequence[str],
    live_enabled: bool,
) -> List[BacklogEntry]:
    completed = set(completed_ids)
    eligible: List[BacklogEntry] = []
    for entry in BACKLOG:
        if entry.experiment_id in completed:
            continue
        if entry.requires_live and not live_enabled:
            continue
        if not all(dep in completed for dep in entry.depends_on):
            continue
        eligible.append(entry)
    return eligible


def plan_next_experiment(
    *,
    completed_ids: Optional[Sequence[str]] = None,
    live_enabled: bool = False,
) -> Optional[Dict[str, Any]]:
    """Pick the next experiment in priority order, respecting the
    completed set, dependencies, and the live-enabled flag.

    Returns ``None`` when no eligible experiment remains. The returned
    dict is JSON-serializable and shaped per ``BacklogEntry.to_plan_dict``.
    """
    completed_ids = list(completed_ids or [])
    eligible = _eligible_entries(completed_ids=completed_ids, live_enabled=live_enabled)
    if not eligible:
        return None
    return eligible[0].to_plan_dict()


def plan_next_batch(
    *,
    completed_ids: Optional[Sequence[str]] = None,
    live_enabled: bool = False,
    max_experiments: int = MAX_EXPERIMENTS_PER_BATCH_DEFAULT,
    manifest_path: Optional[Path] = None,
    ledger_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Plan the next bounded batch of experiments.

    Returns a JSON-serializable dict with ``experiments`` (list of
    plan dicts, length up to ``max_experiments``), ``current_state``
    (snapshot from :func:`assess_current_state`), and metadata
    (``backlog_version``, ``selected_at``, ``max_experiments``,
    ``live_enabled``, ``no_more_experiments``). Within-batch
    dependencies are honored: a successor will only appear after its
    prerequisite is already chosen earlier in the same batch.

    Refuses ``max_experiments`` <= 0 (raises ``ValueError``) so callers
    cannot accidentally short-circuit the loop.
    """
    if max_experiments <= 0:
        raise ValueError("max_experiments must be a positive integer")

    if ledger_path is None:
        ledger_path = _repo_root() / "autoresearch" / ".runs" / "experiments.jsonl"
    if completed_ids is None:
        completed_ids = read_completed_ids_from_ledger(ledger_path)

    snapshot = assess_current_state(manifest_path=manifest_path, ledger_path=ledger_path)
    pretend_completed = list(completed_ids)
    plans: List[Dict[str, Any]] = []

    for _ in range(max_experiments):
        eligible = _eligible_entries(
            completed_ids=pretend_completed, live_enabled=live_enabled
        )
        if not eligible:
            break
        chosen = eligible[0]
        plans.append(chosen.to_plan_dict())
        pretend_completed.append(chosen.experiment_id)

    no_more = len(plans) == 0

    return {
        "backlog_version": BACKLOG_VERSION,
        "selected_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "max_experiments": max_experiments,
        "live_enabled": live_enabled,
        "completed_experiment_ids": list(completed_ids),
        "remaining_backlog_count": (
            len(BACKLOG) - len(set(completed_ids) & {e.experiment_id for e in BACKLOG})
        ),
        "no_more_experiments": no_more,
        "current_state": snapshot,
        "experiments": plans,
    }


def write_plan(path: Path, plan: Mapping[str, Any], *, allow_unsafe: bool = False) -> Path:
    """Write a plan dict as JSON. Refuses non-ignored repo paths
    unless ``allow_unsafe=True`` - same default-safe policy as the
    PIPE5 bundle output, so plan files do not pollute the repo."""
    target = Path(path)
    if not allow_unsafe and not _is_safe_plan_path(target):
        raise ValueError(
            f"refusing to write plan to {target}; not under one of "
            f"{', '.join(PLAN_SAFE_DIRS)} and not outside the repo. "
            "Pass allow_unsafe=True to override."
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as f:
        json.dump(dict(plan), f, indent=2, sort_keys=False)
        f.write("\n")
    return target.resolve()
