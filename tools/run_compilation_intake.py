"""Operator script: compilation-channel case-lead intake.

Reads a JSON or JSONL document of compilation-video metadata
records, extracts case leads from titles / descriptions / chapter
markers, and writes normalized DatasetCandidate rows + search
tasks + lightweight packet stubs.

This script is **pure-parsing**. It never fetches yt-dlp metadata
live; the operator is expected to supply a JSON / JSONL file of
already-fetched metadata records (one per compilation video).
Live yt-dlp metadata fetch is a deferred follow-up.

Output paths are gated by the same safe-bundle policy used by
``tools/run_enrichment_tasks.py`` / ``tools/run_dataset_intake.py``.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import List, Optional, Sequence

# Repo-root sys.path bootstrap (matches PR #28 pattern).
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline2_discovery.casegraph.cli import (  # noqa: E402
    BUNDLE_SAFE_DIRS,
    _is_safe_bundle_path,
)
from pipeline2_discovery.dataset_sources.compilation_leads import (  # noqa: E402
    parse_compilation_input_file,
)
from pipeline2_discovery.dataset_sources.models import (  # noqa: E402
    DatasetCandidate,
)


# ---- CLI ------------------------------------------------------------


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tools.run_compilation_intake",
        description=(
            "Compilation-channel case-lead intake. Reads a JSON or "
            "JSONL document of compilation-video metadata records and "
            "writes normalized DatasetCandidate rows. Pure parsing — "
            "no yt-dlp / network / portal-live."
        ),
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to a JSON or JSONL document of compilation-video "
             "metadata records.",
    )
    parser.add_argument(
        "--output-csv",
        dest="output_csv",
        default=None,
        help="Optional path to write candidates as CSV.",
    )
    parser.add_argument(
        "--output-json",
        dest="output_json",
        default=None,
        help="Optional path to write candidates as JSON list.",
    )
    parser.add_argument(
        "--search-tasks-out",
        dest="search_tasks_out",
        default=None,
        help="Optional path to write per-candidate search tasks JSON.",
    )
    parser.add_argument(
        "--packet-stubs-dir",
        dest="packet_stubs_dir",
        default=None,
        help="Optional directory for lightweight packet-stub markdown files.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a single JSON document on stdout instead of a human report.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _validate_safe_path(value: Optional[str], label: str, err) -> Optional[Path]:
    if value is None:
        return None
    p = Path(value)
    if not _is_safe_bundle_path(p):
        print(
            f"error: {label} must resolve under one of the safe artifact "
            f"dirs ({', '.join(BUNDLE_SAFE_DIRS)}) or outside the repo "
            f"entirely; got: {p}",
            file=err,
        )
        return ...  # type: ignore  # sentinel for "rejected"
    return p


# ---- writers --------------------------------------------------------


_CSV_FIELDS: List[str] = [
    "candidate_id", "source_lane", "source_dataset", "source_row_id",
    "source_url", "case_title", "agency_name",
    "jurisdiction_city", "jurisdiction_state",
    "incident_date", "subject_name",
    "bodycam_likelihood", "footage_likelihood",
    "evidence_strength", "packet_priority_score", "grade",
]


def _write_csv(path: Path, candidates: List[DatasetCandidate]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_CSV_FIELDS)
        w.writeheader()
        for c in candidates:
            row = {k: getattr(c, k, "") for k in _CSV_FIELDS}
            w.writerow(row)


def _write_json(path: Path, candidates: List[DatasetCandidate]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(c) for c in candidates]
    path.write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )


def _write_search_tasks(path: Path, candidates: List[DatasetCandidate]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tasks: List[dict] = []
    for c in candidates:
        ctx = {
            "agency": (c.agency_name or "").lower(),
            "subject_name": (c.subject_name or "").lower(),
            "city": (c.jurisdiction_city or "").lower(),
            "state": c.jurisdiction_state or "",
        }
        for q in c.youtube_queries:
            tasks.append({
                "candidate_id": c.candidate_id, "grade": c.grade,
                "task_type": "youtube_query", "query": q, "context": ctx,
            })
        for q in c.muckrock_queries:
            tasks.append({
                "candidate_id": c.candidate_id, "grade": c.grade,
                "task_type": "muckrock_query", "query": q, "context": ctx,
            })
        for q in c.outcome_queries:
            tasks.append({
                "candidate_id": c.candidate_id, "grade": c.grade,
                "task_type": "outcome_query", "query": q, "context": ctx,
            })
    payload = {
        "generated_at": _utc_now(),
        "source_lane": "compilation_leads",
        "candidate_count": len(candidates),
        "task_count": len(tasks),
        "tasks": tasks,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _bullet_list(items: List[str]) -> List[str]:
    """Render a list of items as ``- item`` bullets, or ``_(none)_``
    when empty. Returned lines are joined by the caller."""
    if not items:
        return ["_(none)_"]
    return [f"- {item}" for item in items]


def _write_packet_stubs(directory: Path, candidates: List[DatasetCandidate]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for c in candidates:
        slug = c.candidate_id.replace("/", "_").replace(":", "__")
        path = directory / f"{slug}.md"
        lines: List[str] = [
            f"# Packet stub: {c.subject_name or '(no subject)'} — "
            f"{c.incident_date or '(no date)'} — "
            f"{c.jurisdiction_city or c.jurisdiction_state or '(no jurisdiction)'}",
            "",
            f"- **Grade:** {c.grade}  packet_priority_score = {c.packet_priority_score}",
            f"- **Source lane:** `{c.source_lane}`",
            f"- **Source video:** {c.source_url or '(none)'}",
            "",
            "## Core facts",
            "",
            f"- Subject: {c.subject_name or '(none)'}",
            f"- Agency: {c.agency_name or '(none)'}",
            (
                f"- Jurisdiction: "
                f"{c.jurisdiction_city or '(no city)'}, "
                f"{c.jurisdiction_state or '(no state)'}"
            ),
            f"- Incident date: {c.incident_date or '(none)'}",
            f"- Artifacts: {', '.join(c.artifact_types_detected) or '(none)'}",
            "",
            "## Source URLs",
            "",
            "**News:**",
        ]
        lines.extend(_bullet_list(c.news_urls or []))
        lines.append("")
        lines.append("**Official:**")
        lines.extend(_bullet_list(c.official_urls or []))
        lines.append("")
        lines.append("**MuckRock:**")
        lines.extend(_bullet_list(c.muckrock_urls or []))
        lines.append("")
        lines.append("## Suggested search tasks")
        lines.append("")
        lines.append("**YouTube queries:**")
        lines.extend(_bullet_list(c.youtube_queries or []))
        lines.append("")
        lines.append("**MuckRock queries:**")
        lines.extend(_bullet_list(c.muckrock_queries or []))
        lines.append("")
        lines.append("**Outcome queries:**")
        lines.extend(_bullet_list(c.outcome_queries or []))
        lines.append("")
        lines.append("## Notes")
        lines.append("")
        lines.extend(_bullet_list(c.notes or []))
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _utc_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _format_human_summary(
    candidates: List[DatasetCandidate],
) -> str:
    from collections import Counter
    grades = Counter(c.grade for c in candidates)
    lines = [
        "=== compilation case-lead intake ===",
        f"candidate_count: {len(candidates)}",
        f"grades:          {dict(grades)}",
        "",
    ]
    for c in candidates[:25]:
        lines.append(
            f"  [{c.grade}] {c.candidate_id}  "
            f"subject={c.subject_name or '-'}  "
            f"agency={c.agency_name or '-'}  "
            f"loc={c.jurisdiction_city or '-'}/"
            f"{c.jurisdiction_state or '-'}  "
            f"date={c.incident_date or '-'}"
        )
    if len(candidates) > 25:
        lines.append(f"  ... ({len(candidates) - 25} more)")
    return "\n".join(lines).rstrip() + "\n"


# ---- main -----------------------------------------------------------


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

    output_csv = _validate_safe_path(args.output_csv, "--output-csv", err)
    if output_csv is ...:
        return 2
    output_json = _validate_safe_path(args.output_json, "--output-json", err)
    if output_json is ...:
        return 2
    search_tasks_out = _validate_safe_path(
        args.search_tasks_out, "--search-tasks-out", err,
    )
    if search_tasks_out is ...:
        return 2
    packet_stubs_dir = _validate_safe_path(
        args.packet_stubs_dir, "--packet-stubs-dir", err,
    )
    if packet_stubs_dir is ...:
        return 2

    try:
        candidates = parse_compilation_input_file(input_path)
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"error: could not load input: {exc}", file=err)
        return 2

    # Sort by score desc for human readability
    candidates.sort(
        key=lambda c: (-c.packet_priority_score, c.candidate_id),
    )

    if output_csv is not None:
        _write_csv(output_csv, candidates)
    if output_json is not None:
        _write_json(output_json, candidates)
    if search_tasks_out is not None:
        _write_search_tasks(search_tasks_out, candidates)
    if packet_stubs_dir is not None:
        _write_packet_stubs(packet_stubs_dir, candidates)

    if args.json:
        from collections import Counter
        grades = Counter(c.grade for c in candidates)
        summary = {
            "candidate_count": len(candidates),
            "grades": dict(grades),
            "candidates": [asdict(c) for c in candidates],
        }
        print(json.dumps(summary, indent=2), file=out)
    else:
        print(_format_human_summary(candidates), file=out)

    return 0


if __name__ == "__main__":
    sys.exit(main())
