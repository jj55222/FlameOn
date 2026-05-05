"""Operator script: run a dataset-source intake lane.

Reads a curated input file, calls the appropriate per-lane parser,
sorts the resulting DatasetCandidate rows by score, writes
candidates.csv / candidates.json / search_tasks.json / packet stubs
to operator-supplied paths under .tmp/.

Pure: no network, no portal-live, no MuckRock API, no media downloads.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

# Repo-root sys.path bootstrap (same pattern as the other tools/).
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline2_discovery.dataset_sources import (  # noqa: E402
    DatasetCandidate,
    is_federal_agency,
    rank_key,
)
from pipeline2_discovery.dataset_sources import (  # noqa: E402
    sfchronicle_pursuits,
    muckrock_leads,
)


SUPPORTED_SOURCES: Tuple[str, ...] = (
    "sfchronicle_pursuits",
    "muckrock_curated",
)
DEFAULT_TOP_N = 25
DEFAULT_TARGET_STATES = "AZ,FL,TX,OH,IL,CA"


# ---- per-source dispatch --------------------------------------------


def _load_candidates(
    *,
    source: str,
    input_path: Path,
    target_states: Sequence[str],
) -> List[DatasetCandidate]:
    if source == "sfchronicle_pursuits":
        return sfchronicle_pursuits.parse_csv_file(
            input_path, target_states=target_states
        )
    if source == "muckrock_curated":
        # Auto-detect by extension: .json → records, anything else → URL list.
        if input_path.suffix.lower() == ".json":
            return muckrock_leads.parse_records_json_file(input_path)
        return muckrock_leads.parse_url_list_file(input_path)
    raise ValueError(f"unsupported source: {source!r}")


# ---- output writers -------------------------------------------------


# CSV columns. Lists are joined with "; ". Order is stable so
# downstream consumers (Sheets imports, grep/awk) can rely on it.
_CSV_COLUMNS: Tuple[str, ...] = (
    "candidate_id",
    "grade",
    "packet_priority_score",
    "source_lane",
    "source_dataset",
    "source_row_id",
    "case_title",
    "agency_name",
    "subject_name",
    "person_role",
    "initial_reason",
    "incident_date",
    "jurisdiction_city",
    "jurisdiction_county",
    "jurisdiction_state",
    "fatality_count",
    "injury_count",
    "bodycam_likelihood",
    "footage_likelihood",
    "evidence_strength",
    "outcome_validation_needed",
    "news_urls",
    "official_urls",
    "muckrock_urls",
    "youtube_queries",
    "muckrock_queries",
    "official_source_queries",
    "outcome_queries",
    "artifact_types_detected",
    "next_actions_hint",
    "notes",
    "source_url",
)

_LIST_COLUMNS = frozenset({
    "news_urls",
    "official_urls",
    "muckrock_urls",
    "youtube_queries",
    "muckrock_queries",
    "official_source_queries",
    "outcome_queries",
    "artifact_types_detected",
    "next_actions_hint",
    "notes",
})


def _candidate_to_csv_row(c: DatasetCandidate) -> dict:
    d = asdict(c)
    out = {}
    for col in _CSV_COLUMNS:
        val = d.get(col)
        if col in _LIST_COLUMNS:
            out[col] = "; ".join(val or [])
        elif val is None:
            out[col] = ""
        elif isinstance(val, bool):
            out[col] = "true" if val else "false"
        else:
            out[col] = val
    return out


def _write_csv(path: Path, candidates: Iterable[DatasetCandidate]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(_CSV_COLUMNS))
        writer.writeheader()
        for c in candidates:
            writer.writerow(_candidate_to_csv_row(c))


def _write_json(path: Path, candidates: Iterable[DatasetCandidate]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [c.to_dict() for c in candidates]
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


# ---- search-task lane -----------------------------------------------


def _search_tasks(
    candidates: Iterable[DatasetCandidate],
) -> List[dict]:
    """Flatten per-candidate query lists into one task feed.

    Each task carries the originating candidate_id + grade so the
    downstream router can prioritise A-grade tasks first."""
    out: List[dict] = []
    for c in candidates:
        ctx = {
            "agency": c.agency_name,
            "subject_name": c.subject_name,
            "city": c.jurisdiction_city,
            "state": c.jurisdiction_state,
        }
        for q in c.youtube_queries:
            out.append(
                {
                    "candidate_id": c.candidate_id,
                    "grade": c.grade,
                    "task_type": "youtube_query",
                    "query": q,
                    "context": ctx,
                }
            )
        for q in c.muckrock_queries:
            out.append(
                {
                    "candidate_id": c.candidate_id,
                    "grade": c.grade,
                    "task_type": "muckrock_query",
                    "query": q,
                    "context": ctx,
                }
            )
        for q in c.official_source_queries:
            out.append(
                {
                    "candidate_id": c.candidate_id,
                    "grade": c.grade,
                    "task_type": "official_source_query",
                    "query": q,
                    "context": ctx,
                }
            )
        for q in c.outcome_queries:
            out.append(
                {
                    "candidate_id": c.candidate_id,
                    "grade": c.grade,
                    "task_type": "outcome_query",
                    "query": q,
                    "context": ctx,
                }
            )
    return out


def _write_search_tasks(
    path: Path,
    *,
    source_lane: str,
    candidates: Iterable[DatasetCandidate],
) -> int:
    candidates_list = list(candidates)
    tasks = _search_tasks(candidates_list)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_lane": source_lane,
        "candidate_count": len(candidates_list),
        "task_count": len(tasks),
        "tasks": tasks,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return len(tasks)


# ---- packet stubs ---------------------------------------------------


def _format_packet_stub(c: DatasetCandidate) -> str:
    """Human-readable markdown stub. Operators read these by eye to
    decide which candidates to advance to full packet authoring."""

    def _bullets(items: List[str], empty: str = "_(none)_") -> str:
        if not items:
            return empty
        return "\n".join(f"- {item}" for item in items)

    sections = [
        f"# Packet stub: {c.case_title or c.candidate_id}",
        "",
        f"- **Grade:** {c.grade}  \\\n  packet_priority_score = {c.packet_priority_score}",
        f"- **Source lane:** `{c.source_lane}` ({c.source_dataset})",
        f"- **Source row id:** `{c.source_row_id}`",
        f"- **Source URL:** {c.source_url or '_(n/a)_'}",
        "",
        "## Core facts",
        "",
        f"- Subject: {c.subject_name or '_(unknown)_'}",
        f"- Person role: {c.person_role or '_(unknown)_'}",
        f"- Agency: {c.agency_name or '_(unknown)_'}",
        f"- Jurisdiction: {', '.join(filter(None, [c.jurisdiction_city, c.jurisdiction_county, c.jurisdiction_state])) or '_(unknown)_'}",
        f"- Incident date: {c.incident_date or '_(unknown)_'}",
        f"- Initial reason: {c.initial_reason or '_(unknown)_'}",
        f"- Fatalities: {c.fatality_count if c.fatality_count is not None else '_(unknown)_'}",
        f"- Injuries: {c.injury_count if c.injury_count is not None else '_(unknown)_'}",
        "",
        "## Source URLs",
        "",
        "**News:**",
        _bullets(c.news_urls),
        "",
        "**Official:**",
        _bullets(c.official_urls),
        "",
        "**MuckRock:**",
        _bullets(c.muckrock_urls),
        "",
        "## Suggested search tasks",
        "",
        "**YouTube queries:**",
        _bullets(c.youtube_queries),
        "",
        "**MuckRock queries:**",
        _bullets(c.muckrock_queries),
        "",
        "**Official-source queries:**",
        _bullets(c.official_source_queries),
        "",
        "**Outcome queries:**",
        _bullets(c.outcome_queries),
        "",
        "## Artifact gaps",
        "",
        f"- Bodycam likelihood: **{c.bodycam_likelihood}**",
        f"- Footage likelihood: **{c.footage_likelihood}**",
        f"- Evidence strength: **{c.evidence_strength}**",
        f"- Outcome validation needed: {'yes' if c.outcome_validation_needed else 'no'}",
        f"- Artifact types detected so far: {', '.join(c.artifact_types_detected) or '_(none)_'}",
        "",
        "## Next actions",
        "",
        _bullets(c.next_actions_hint),
        "",
        "## Notes",
        "",
        _bullets(c.notes),
        "",
    ]
    return "\n".join(sections)


def _write_packet_stubs(
    dir_path: Path,
    *,
    candidates: Iterable[DatasetCandidate],
    top_n: int,
) -> List[Path]:
    dir_path.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []
    for c in list(candidates)[:top_n]:
        # Filename-safe: candidate_id contains ":" which Windows can't
        # use; replace with "__".
        safe_id = c.candidate_id.replace(":", "__").replace("/", "_")
        out = dir_path / f"{safe_id}.md"
        out.write_text(_format_packet_stub(c), encoding="utf-8")
        written.append(out)
    return written


# ---- CLI ------------------------------------------------------------


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tools.run_dataset_intake",
        description=(
            "Run a Pipeline 1 dataset-source intake lane. Reads a "
            "local curated input file, emits normalized candidates "
            "+ search tasks + top-N packet stubs. No network calls."
        ),
    )
    parser.add_argument(
        "--source",
        required=True,
        choices=SUPPORTED_SOURCES,
        help="Dataset-source lane to run.",
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the curated input file (CSV for sfchronicle, "
             "TXT or JSON for muckrock_curated).",
    )
    parser.add_argument(
        "--output-csv",
        dest="output_csv",
        default=None,
        help="Path to write the candidates CSV. Optional.",
    )
    parser.add_argument(
        "--output-json",
        dest="output_json",
        default=None,
        help="Path to write the candidates JSON. Optional.",
    )
    parser.add_argument(
        "--search-tasks-out",
        dest="search_tasks_out",
        default=None,
        help="Path to write the aggregated search-tasks JSON. Optional.",
    )
    parser.add_argument(
        "--packet-stubs-dir",
        dest="packet_stubs_dir",
        default=None,
        help="Directory to write top-N packet stub markdown files. Optional.",
    )
    parser.add_argument(
        "--top-n",
        dest="top_n",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"Number of top-graded candidates to write packet stubs for "
             f"(default: {DEFAULT_TOP_N}).",
    )
    parser.add_argument(
        "--target-states",
        dest="target_states",
        default=DEFAULT_TARGET_STATES,
        help=f"Comma-separated state codes for the SF Chronicle target-state "
             f"score bonus AND for the operational rank-key tie-break "
             f"(default: {DEFAULT_TARGET_STATES}).",
    )
    parser.add_argument(
        "--exclude-federal-agencies",
        dest="exclude_federal",
        action="store_true",
        help=(
            "Drop candidates whose agency_name matches a known federal "
            "agency (U.S. Border Patrol / CBP / FBI / DEA / ATF / "
            "Homeland Security / U.S. Marshals / Secret Service). "
            "Default off; municipal/sheriff/state agencies always pass. "
            "Use this for portal-live target lead generation since the "
            "current extractor architecture targets municipal newsrooms."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a single JSON document on stdout instead of a human report.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _format_human_report(
    *,
    source: str,
    input_path: Path,
    candidates: List[DatasetCandidate],
    csv_path: Optional[Path],
    json_path: Optional[Path],
    tasks_path: Optional[Path],
    stubs_dir: Optional[Path],
    stub_count: int,
    task_count: int,
    top_n: int,
    excluded_federal_count: int = 0,
) -> str:
    grade_counts = {"A": 0, "B": 0, "C": 0, "D": 0}
    for c in candidates:
        grade_counts[c.grade] = grade_counts.get(c.grade, 0) + 1
    lines = [
        "=== dataset intake ===",
        f"source:           {source}",
        f"input:            {input_path}",
        f"candidates:       {len(candidates)}",
        f"  A: {grade_counts['A']}  B: {grade_counts['B']}  "
        f"C: {grade_counts['C']}  D: {grade_counts['D']}",
        f"top-N (stubs):    {top_n}",
        f"task_count:       {task_count}",
    ]
    if excluded_federal_count:
        lines.append(f"federal excluded: {excluded_federal_count}")
    lines.append("")
    if csv_path is not None:
        lines.append(f"wrote CSV:        {csv_path}")
    if json_path is not None:
        lines.append(f"wrote JSON:       {json_path}")
    if tasks_path is not None:
        lines.append(f"wrote tasks:      {tasks_path}")
    if stubs_dir is not None:
        lines.append(f"wrote stubs:      {stub_count} files in {stubs_dir}")
    lines.append("")
    if candidates:
        lines.append("top 10:")
        for c in candidates[:10]:
            tail = c.case_title or c.candidate_id
            lines.append(
                f"  [{c.grade} score={c.packet_priority_score:>3}] {tail}"
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

    if args.top_n < 0:
        print(f"error: --top-n must be >= 0; got {args.top_n}", file=err)
        return 2

    target_states = tuple(
        s.strip().upper() for s in (args.target_states or "").split(",") if s.strip()
    )

    try:
        candidates = _load_candidates(
            source=args.source,
            input_path=input_path,
            target_states=target_states,
        )
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=err)
        return 2

    excluded_federal_count = 0
    if args.exclude_federal:
        before = len(candidates)
        candidates = [c for c in candidates if not is_federal_agency(c.agency_name)]
        excluded_federal_count = before - len(candidates)

    candidates.sort(key=lambda c: rank_key(c, target_states=target_states))

    csv_path = Path(args.output_csv) if args.output_csv else None
    json_path = Path(args.output_json) if args.output_json else None
    tasks_path = Path(args.search_tasks_out) if args.search_tasks_out else None
    stubs_dir = Path(args.packet_stubs_dir) if args.packet_stubs_dir else None

    if csv_path is not None:
        _write_csv(csv_path, candidates)
    if json_path is not None:
        _write_json(json_path, candidates)
    task_count = 0
    if tasks_path is not None:
        task_count = _write_search_tasks(
            tasks_path, source_lane=args.source, candidates=candidates,
        )
    stub_count = 0
    if stubs_dir is not None:
        written = _write_packet_stubs(
            stubs_dir, candidates=candidates, top_n=args.top_n,
        )
        stub_count = len(written)

    if args.json:
        payload = {
            "source": args.source,
            "input": str(input_path),
            "candidate_count": len(candidates),
            "grade_counts": {
                "A": sum(1 for c in candidates if c.grade == "A"),
                "B": sum(1 for c in candidates if c.grade == "B"),
                "C": sum(1 for c in candidates if c.grade == "C"),
                "D": sum(1 for c in candidates if c.grade == "D"),
            },
            "top_n": args.top_n,
            "task_count": task_count,
            "stub_count": stub_count,
            "exclude_federal_agencies": bool(args.exclude_federal),
            "excluded_federal_count": excluded_federal_count,
            "csv_path": str(csv_path) if csv_path else None,
            "json_path": str(json_path) if json_path else None,
            "search_tasks_path": str(tasks_path) if tasks_path else None,
            "packet_stubs_dir": str(stubs_dir) if stubs_dir else None,
            "candidates": [c.to_dict() for c in candidates],
        }
        print(json.dumps(payload, indent=2), file=out)
    else:
        print(
            _format_human_report(
                source=args.source,
                input_path=input_path,
                candidates=candidates,
                csv_path=csv_path,
                json_path=json_path,
                tasks_path=tasks_path,
                stubs_dir=stubs_dir,
                stub_count=stub_count,
                task_count=task_count,
                top_n=args.top_n,
                excluded_federal_count=excluded_federal_count,
            ),
            file=out,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
