"""Operator script: enrichment task runner over a search_tasks.json.

Reads the document produced by ``tools/run_dataset_intake.py``,
filters by task_type / candidate_id / grade, sorts deterministically,
caps by --max-tasks, and either reports the selection (dry-run) or
dispatches each task to a provider.

This PR ships only the harness + MockProvider. Live providers
(YouTube, MuckRock, Brave/Exa/Tavily) are deferred to follow-up
PRs and explicitly raise NotImplementedError if requested.

Pure: no network, no portal-live, no MuckRock API, no media
downloads.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional, Sequence

# Repo-root sys.path bootstrap (PR #28 pattern).
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline2_discovery.casegraph.cli import (  # noqa: E402
    BUNDLE_SAFE_DIRS,
    _is_safe_bundle_path,
)
from pipeline2_discovery.enrichment import (  # noqa: E402
    DEFERRED_PROVIDERS,
    KNOWN_PROVIDERS,
    EnrichmentTask,
    TaskType,
    filter_tasks,
    get_provider,
    load_search_tasks_file,
    run_enrichment_batch,
)


DEFAULT_MAX_TASKS = 10
DEFAULT_PROVIDER = "mock"


# ---- CLI ------------------------------------------------------------


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tools.run_enrichment_tasks",
        description=(
            "Enrichment task runner. Reads a search_tasks.json document "
            "produced by tools/run_dataset_intake.py and dispatches each "
            "selected task to a provider. Default mode is dry-run; pass "
            "--run to actually invoke the provider. This PR ships only "
            "the harness + MockProvider; live providers (YouTube, "
            "MuckRock API, Brave/Exa/Tavily) are deferred."
        ),
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to a search_tasks.json document.",
    )
    parser.add_argument(
        "--output-json",
        dest="output_json",
        default=None,
        help=(
            "Optional path to write the per-task results JSON. Subject "
            "to the safe-path policy (must resolve under one of "
            f"{', '.join(BUNDLE_SAFE_DIRS)} or outside the repo)."
        ),
    )
    parser.add_argument(
        "--summary-out",
        dest="summary_out",
        default=None,
        help=(
            "Optional path to write the aggregate summary JSON. Subject "
            "to the same safe-path policy as --output-json."
        ),
    )
    parser.add_argument(
        "--task-type",
        dest="task_types",
        action="append",
        default=[],
        choices=list(TaskType.ALL),
        help=(
            "Restrict to one or more task types. Repeatable. Empty "
            "list = no filter."
        ),
    )
    parser.add_argument(
        "--candidate-id",
        dest="candidate_ids",
        action="append",
        default=[],
        help="Restrict to one or more candidate_ids. Repeatable.",
    )
    parser.add_argument(
        "--grade",
        dest="grades",
        action="append",
        default=[],
        choices=["A", "B", "C", "D"],
        help="Restrict to one or more candidate grades. Repeatable.",
    )
    parser.add_argument(
        "--max-tasks",
        dest="max_tasks",
        type=int,
        default=DEFAULT_MAX_TASKS,
        help=f"Hard cap on selected tasks per run (default: {DEFAULT_MAX_TASKS}).",
    )
    parser.add_argument(
        "--max-candidates",
        dest="max_candidates",
        type=int,
        default=None,
        help=(
            "Optional cap on the number of distinct candidate_id values "
            "selected. Combined with --tasks-per-candidate this fans the "
            "task budget out across more candidates instead of stacking it "
            "on the few lowest candidate_ids. If omitted, behaviour is "
            "unchanged from earlier versions of the runner."
        ),
    )
    parser.add_argument(
        "--tasks-per-candidate",
        dest="tasks_per_candidate",
        type=int,
        default=None,
        help=(
            "Optional cap on tasks selected per candidate. Default: "
            "unlimited per candidate. Used together with --max-candidates "
            "to broaden coverage of grade-A pools that have multiple "
            "tasks per candidate."
        ),
    )
    parser.add_argument(
        "--run",
        dest="run",
        action="store_true",
        help=(
            "Required to actually invoke the provider. Without --run, "
            "the script is dry-run only."
        ),
    )
    parser.add_argument(
        "--provider",
        default=DEFAULT_PROVIDER,
        help=(
            f"Provider name. Only {KNOWN_PROVIDERS!r} is implemented in "
            f"this PR; {DEFERRED_PROVIDERS!r} raise NotImplementedError."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a single JSON document on stdout instead of a human report.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _format_human_summary(summary: dict) -> str:
    lines = [
        "=== enrichment task runner ===",
        f"run_id:                   {summary['run_id']}",
        f"dry_run:                  {summary['dry_run']}",
        f"provider:                 {summary['provider']}",
        f"max_tasks:                {summary['max_tasks']}",
        f"max_candidates:           {summary.get('max_candidates')}",
        f"tasks_per_candidate:      {summary.get('tasks_per_candidate')}",
        f"selected_count:           {summary['selected_count']}",
        f"selected_candidate_count: {summary.get('selected_candidate_count', '-')}",
        f"attempted_count:          {summary['attempted_count']}",
        f"completed_count:          {summary['completed_count']}",
        f"failed_count:             {summary['failed_count']}",
        f"skipped_count:            {summary['skipped_count']}",
        f"elapsed_seconds:          {summary['elapsed_seconds']:.3f}",
        "",
    ]
    if summary["results"]:
        lines.append("results:")
        for r in summary["results"]:
            tail = ""
            if r.get("result_urls"):
                tail = f" -> {r['result_urls'][0]}"
            elif r.get("error"):
                tail = f" error={r['error']!r}"
            lines.append(
                f"  [{r['status']}] {r['candidate_id']} "
                f"{r['task_type']} {r['query']!r}{tail}"
            )
    return "\n".join(lines).rstrip() + "\n"


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    stdout=None,
    stderr=None,
) -> int:
    out = stdout if stdout is not None else sys.stdout
    err = stderr if stderr is not None else sys.stderr
    args = _parse_args(argv)

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"error: input not found: {input_path}", file=err)
        return 2

    if args.max_tasks < 1:
        print(
            f"error: --max-tasks must be >= 1; got {args.max_tasks}",
            file=err,
        )
        return 2

    if args.max_candidates is not None and args.max_candidates < 1:
        print(
            f"error: --max-candidates must be >= 1 if provided; got "
            f"{args.max_candidates}",
            file=err,
        )
        return 2

    if args.tasks_per_candidate is not None and args.tasks_per_candidate < 1:
        print(
            f"error: --tasks-per-candidate must be >= 1 if provided; got "
            f"{args.tasks_per_candidate}",
            file=err,
        )
        return 2

    output_json: Optional[Path] = None
    if args.output_json:
        output_json = Path(args.output_json)
        if not _is_safe_bundle_path(output_json):
            print(
                f"error: --output-json must resolve under one of the safe "
                f"artifact dirs ({', '.join(BUNDLE_SAFE_DIRS)}) or outside "
                f"the repo entirely; got: {output_json}",
                file=err,
            )
            return 2

    summary_out: Optional[Path] = None
    if args.summary_out:
        summary_out = Path(args.summary_out)
        if not _is_safe_bundle_path(summary_out):
            print(
                f"error: --summary-out must resolve under one of the safe "
                f"artifact dirs ({', '.join(BUNDLE_SAFE_DIRS)}) or outside "
                f"the repo entirely; got: {summary_out}",
                file=err,
            )
            return 2

    try:
        all_tasks = load_search_tasks_file(input_path)
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"error: could not load search tasks: {exc}", file=err)
        return 2

    selected = filter_tasks(
        all_tasks,
        task_types=args.task_types,
        candidate_ids=args.candidate_ids,
        grades=args.grades,
    )

    dry_run = not args.run

    # Provider acquisition: dry-run never instantiates a deferred
    # provider, but we still want to fail loudly if the operator
    # passed a bogus name.
    try:
        provider = get_provider(args.provider) if not dry_run else (
            get_provider(args.provider) if args.provider == "mock"
            else _check_provider_name_only(args.provider)
        )
    except (NotImplementedError, ValueError) as exc:
        print(f"error: {exc}", file=err)
        return 2

    summary = run_enrichment_batch(
        selected,
        provider=provider,
        dry_run=dry_run,
        max_tasks=args.max_tasks,
        max_candidates=args.max_candidates,
        tasks_per_candidate=args.tasks_per_candidate,
    )

    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(
            json.dumps(summary["results"], indent=2) + "\n",
            encoding="utf-8",
        )

    if summary_out is not None:
        summary_out.parent.mkdir(parents=True, exist_ok=True)
        summary_out.write_text(
            json.dumps(summary, indent=2) + "\n",
            encoding="utf-8",
        )

    if args.json:
        print(json.dumps(summary, indent=2), file=out)
    else:
        print(_format_human_summary(summary), file=out)

    if summary["failed_count"] > 0:
        return 1
    return 0


def _check_provider_name_only(name: str):
    """Validate provider name without instantiating; used in dry-run
    so a bogus --provider still rejects up front, but a deferred name
    doesn't raise (the runner skips dispatch entirely)."""
    if name in KNOWN_PROVIDERS or name in DEFERRED_PROVIDERS:
        # Return a stub object with a name; the runner won't invoke
        # execute() in dry-run mode.
        class _Stub:
            pass
        stub = _Stub()
        stub.name = name
        return stub
    raise ValueError(
        f"unknown provider {name!r}; known: {KNOWN_PROVIDERS}, "
        f"deferred: {DEFERRED_PROVIDERS}"
    )


if __name__ == "__main__":
    sys.exit(main())
