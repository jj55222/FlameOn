"""Operator script: generate portal-live target fixtures from a curated URL list.

Reads a JSON file of curated official Phoenix Newsroom CIB URLs, lints
each against the portal-live target allowlist, and emits zero or more
target-fixture JSON files into an output directory. The script itself
never touches the network.

Usage:
    python tools/generate_portal_live_targets.py \\
        --input <path/to/curated.json> \\
        --output-dir <path/to/output_dir> \\
        --mode {fetch_only,extract_required,both} \\
        [--max-targets N] \\
        [--dry-run] \\
        [--json]

Input shape: a JSON array of objects, each with at minimum a ``url``
key. Optional keys: ``target_id`` (custom override), ``agency``,
``jurisdiction``, ``notes`` (free-form metadata; not read by the
script). A live smoke against a generated fixture is a separate
operator action — see PORTAL_LIVE_OPERATOR.md.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional, Sequence

# Make the repo root importable regardless of how the script is
# invoked. ``python tools/generate_portal_live_targets.py`` puts
# only ``tools/`` on sys.path; ``python -m tools.generate_portal_live_targets``
# puts the cwd on sys.path. This bootstrap normalises both forms so
# the operator-facing direct invocation documented in
# PORTAL_LIVE_OPERATOR.md works without surprises.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline2_discovery.casegraph.portal_live_target_generator import (  # noqa: E402
    DEFAULT_GENERATION_MODE,
    DEFAULT_MAX_TARGETS_PER_RUN,
    GENERATION_MODES,
    GeneratorResult,
    generate_targets,
)


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tools.generate_portal_live_targets",
        description=(
            "Generate portal-live target fixtures from a curated URL list. "
            "Never fetches the web; runs lint + serialize only."
        ),
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to a JSON file containing an array of curated row objects.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write generated fixture JSON files into.",
    )
    parser.add_argument(
        "--mode",
        default=DEFAULT_GENERATION_MODE,
        choices=GENERATION_MODES,
        help=(
            f"Which fixture variants to emit (default: {DEFAULT_GENERATION_MODE})."
        ),
    )
    parser.add_argument(
        "--max-targets",
        type=int,
        default=DEFAULT_MAX_TARGETS_PER_RUN,
        help=(
            f"Per-run cap on accepted targets "
            f"(default: {DEFAULT_MAX_TARGETS_PER_RUN})."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Report what would be written without creating any files. "
            "Output directory is not required to exist in dry-run mode."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a single JSON document on stdout instead of a human report.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _load_rows(input_path: Path) -> List[dict]:
    if not input_path.exists():
        raise FileNotFoundError(f"input not found: {input_path}")
    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("input must be a JSON array of row objects")
    if not all(isinstance(r, dict) for r in data):
        raise ValueError("every row in the input must be a JSON object")
    return data


def _format_human_report(result: GeneratorResult) -> str:
    lines = [
        "=== portal-live target generator ===",
        f"dry_run:         {result.dry_run}",
        f"accepted:        {result.accepted_count}",
        f"rejected:        {result.rejected_count}",
        f"deduped:         {result.deduped_count}",
        f"max_targets_cap: {result.capped_count}",
        f"files written:   {len(result.written_paths)}",
        "",
    ]
    if result.written_paths:
        lines.append("written fixtures:")
        for p in result.written_paths:
            lines.append(f"  {p}")
        lines.append("")
    rejected_outcomes = [o for o in result.outcomes if not o.lint.accepted]
    if rejected_outcomes:
        lines.append("rejected rows:")
        for o in rejected_outcomes:
            lines.append(f"  reason={o.lint.reason}: {o.input_url}")
        lines.append("")
    skipped_outcomes = [o for o in result.outcomes if o.skipped_reason]
    if skipped_outcomes:
        lines.append("skipped (post-lint):")
        for o in skipped_outcomes:
            lines.append(f"  reason={o.skipped_reason}: {o.input_url}")
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
    output_dir = Path(args.output_dir)

    try:
        rows = _load_rows(input_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=err)
        return 2

    if not args.dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    try:
        result = generate_targets(
            rows,
            output_dir=output_dir,
            mode=args.mode,
            max_targets=args.max_targets,
            dry_run=args.dry_run,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=err)
        return 2

    if args.json:
        print(json.dumps(result.to_dict(), indent=2), file=out)
    else:
        print(_format_human_report(result), file=out)

    return 0


if __name__ == "__main__":
    sys.exit(main())
