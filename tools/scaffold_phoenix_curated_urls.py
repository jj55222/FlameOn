"""Operator helper: scaffold a curated Phoenix URL list into the
shape expected by ``tools/generate_portal_live_targets.py``.

Workflow this script enables (also documented in
``pipeline2_discovery/casegraph/PORTAL_LIVE_OPERATOR.md``):

  1. Operator runs a manual site-search query against
     ``site:phoenix.gov/newsroom/police-department-news``.
  2. Operator copy-pastes the URLs they want to consider into a
     local file (one URL per line, CSV/TSV with a ``url`` column,
     or a JSON array of strings or objects).
  3. This script reads that file, runs ``lint_candidate`` from the
     same module the generator uses, and emits two reports:
       - an "accepted" curated JSON scaffold (the input shape the
         generator expects) for any URL that passes lint
       - a "rejected" report listing each rejected URL with the
         lint reason code
  4. By default the script is **dry-run** — it prints the reports
     but writes no files. Pass ``--write-reviewed`` to actually
     write the curated JSON (and, optionally, the rejected report
     when ``--rejected-output`` is also passed).
  5. Operator inspects the dry-run output, removes anything that
     shouldn't be there, and only then re-runs with
     ``--write-reviewed`` to commit a reviewed file to disk for
     the generator to consume.

The script is **pure**: no network calls, no crawling, no browser
automation. Discovery (the act of finding which URLs to feed in)
remains a manual operator action.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

# Make the repo root importable when the script is invoked as
# ``python tools/scaffold_phoenix_curated_urls.py`` from the repo
# root (Python only puts ``tools/`` on sys.path in that form).
# Mirrors the bootstrap in ``tools/generate_portal_live_targets.py``
# (PR #28). Idempotent.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline2_discovery.casegraph.portal_live_target_lint import (  # noqa: E402
    LintDecision,
    lint_candidate,
)


SUPPORTED_FORMATS: Tuple[str, ...] = ("auto", "txt", "csv", "tsv", "json")
DEFAULT_FORMAT = "auto"

# Default metadata fields injected into each accepted entry. The
# generator only reads ``url`` and ``target_id``, so these are
# free-form metadata for the operator's benefit. Phoenix-specific.
_DEFAULT_AGENCY = "Phoenix Police Department"
_DEFAULT_JURISDICTION = "Phoenix, Maricopa County, Arizona"


# ---- input parsers --------------------------------------------------


def _detect_format(input_path: Path) -> str:
    """Map a file extension to one of SUPPORTED_FORMATS minus 'auto'.
    Anything unrecognised falls back to txt (one URL per line)."""
    suffix = input_path.suffix.lower()
    if suffix == ".json":
        return "json"
    if suffix == ".csv":
        return "csv"
    if suffix == ".tsv":
        return "tsv"
    return "txt"


def _parse_text_lines(text: str) -> List[str]:
    """One URL per line. Blank lines and lines starting with ``#`` are
    skipped so operators can paste with comments."""
    urls: List[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        urls.append(line)
    return urls


def _parse_delimited(text: str, *, delimiter: str) -> List[str]:
    """CSV/TSV with at minimum a ``url`` column. Other columns are
    ignored at this layer; the generator can carry them forward via
    its own row-level metadata if the caller adds them later."""
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    if reader.fieldnames is None or "url" not in [
        (f or "").strip().lower() for f in reader.fieldnames
    ]:
        raise ValueError(
            "input must contain a 'url' column "
            f"(found columns: {reader.fieldnames!r})"
        )
    # Match the actual column name case-insensitively.
    url_col = next(
        f for f in reader.fieldnames if (f or "").strip().lower() == "url"
    )
    urls: List[str] = []
    for row in reader:
        val = (row.get(url_col) or "").strip()
        if val:
            urls.append(val)
    return urls


def _parse_json_array(text: str) -> List[str]:
    """Accept either an array of strings or an array of objects with
    a ``url`` key."""
    data = json.loads(text)
    if not isinstance(data, list):
        raise ValueError("JSON input must be an array")
    urls: List[str] = []
    for i, item in enumerate(data):
        if isinstance(item, str):
            stripped = item.strip()
            if stripped:
                urls.append(stripped)
        elif isinstance(item, dict):
            val = (item.get("url") or "").strip() if isinstance(item.get("url"), str) else ""
            if not val:
                raise ValueError(
                    f"JSON array entry [{i}] is an object with no 'url' key"
                )
            urls.append(val)
        else:
            raise ValueError(
                f"JSON array entry [{i}] must be a string or "
                f"object with a 'url' key, got {type(item).__name__}"
            )
    return urls


def load_input(input_path: Path, *, fmt: str) -> List[str]:
    """Read ``input_path`` and return a list of URL strings,
    pre-lint. Whitespace-stripped, empty entries removed."""
    if fmt == "auto":
        fmt = _detect_format(input_path)
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(
            f"unsupported format {fmt!r}; must be one of {SUPPORTED_FORMATS}"
        )

    text = input_path.read_text(encoding="utf-8")

    if fmt == "txt":
        return _parse_text_lines(text)
    if fmt == "csv":
        return _parse_delimited(text, delimiter=",")
    if fmt == "tsv":
        return _parse_delimited(text, delimiter="\t")
    if fmt == "json":
        return _parse_json_array(text)
    raise ValueError(f"unhandled format {fmt!r}")


# ---- scaffolding ----------------------------------------------------


def _accepted_entry(decision: LintDecision) -> dict:
    """Shape that ``tools/generate_portal_live_targets.py`` consumes.
    Default agency / jurisdiction are Phoenix-specific (current
    allowlist). ``notes`` is left blank for the operator to fill in."""
    return {
        "url": decision.normalized_url,
        "agency": _DEFAULT_AGENCY,
        "jurisdiction": _DEFAULT_JURISDICTION,
        "notes": "",
    }


def scaffold(
    urls: Iterable[str],
) -> Tuple[List[dict], List[dict], List[dict]]:
    """Run lint over each URL and bucket the outcomes.

    Returns three lists in stable input order:
      - accepted: dicts ready to be serialised as the generator's
        input JSON
      - rejected: ``{input_url, reason, host, path}`` per rejected
        candidate
      - duplicates: ``{input_url, normalized_url, reason: "duplicate"}``
        for inputs whose normalized URL collides with an already-accepted
        candidate

    Dedup uses the lint's ``normalized_url`` so two inputs that differ
    only by a fragment collapse correctly.
    """
    accepted: List[dict] = []
    rejected: List[dict] = []
    duplicates: List[dict] = []
    seen: set = set()

    for url in urls:
        decision = lint_candidate(url)
        if not decision.accepted:
            rejected.append(
                {
                    "input_url": url,
                    "reason": decision.reason,
                    "host": decision.host,
                    "path": decision.path,
                }
            )
            continue
        if decision.normalized_url in seen:
            duplicates.append(
                {
                    "input_url": url,
                    "normalized_url": decision.normalized_url,
                    "reason": "duplicate",
                }
            )
            continue
        seen.add(decision.normalized_url)
        accepted.append(_accepted_entry(decision))

    return accepted, rejected, duplicates


# ---- CLI ------------------------------------------------------------


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tools.scaffold_phoenix_curated_urls",
        description=(
            "Transform a manually reviewed URL list into the curated "
            "input JSON shape consumed by tools/generate_portal_live_targets.py. "
            "Pure: no network, no crawling, no browser. Default mode is "
            "dry-run; pass --write-reviewed to actually write files."
        ),
    )
    parser.add_argument(
        "--input",
        required=True,
        help=(
            "Path to a local file with candidate URLs. Format is "
            "auto-detected from the file extension; override with --format."
        ),
    )
    parser.add_argument(
        "--output",
        required=True,
        help=(
            "Path where the curated JSON scaffold would be written. "
            "Without --write-reviewed, no file is created — the path "
            "is reported in the dry-run output only."
        ),
    )
    parser.add_argument(
        "--rejected-output",
        dest="rejected_output",
        default=None,
        help=(
            "Optional path for a JSON report of rejected candidates. "
            "Only written when --write-reviewed is also passed."
        ),
    )
    parser.add_argument(
        "--format",
        default=DEFAULT_FORMAT,
        choices=SUPPORTED_FORMATS,
        help=(
            f"Input format (default: {DEFAULT_FORMAT}; auto-detects from "
            "file extension)."
        ),
    )
    parser.add_argument(
        "--write-reviewed",
        dest="write_reviewed",
        action="store_true",
        help=(
            "Required to actually write files. Without it, the script "
            "is dry-run-only — it reports what would happen but creates "
            "no output. The flag's name is deliberately verbose to keep "
            "operators in the loop: only pass it after reviewing the "
            "dry-run output."
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
    input_path: Path,
    output_path: Path,
    rejected_output: Optional[Path],
    fmt: str,
    accepted: List[dict],
    rejected: List[dict],
    duplicates: List[dict],
    wrote_files: bool,
) -> str:
    lines = [
        "=== phoenix curated-URL scaffold ===",
        f"input:           {input_path}",
        f"format:          {fmt}",
        f"output:          {output_path}",
    ]
    if rejected_output is not None:
        lines.append(f"rejected_output: {rejected_output}")
    lines += [
        f"accepted:        {len(accepted)}",
        f"rejected:        {len(rejected)}",
        f"duplicates:      {len(duplicates)}",
        f"wrote_files:     {wrote_files}",
        "",
    ]
    if accepted:
        lines.append("accepted urls:")
        for entry in accepted:
            lines.append(f"  {entry['url']}")
        lines.append("")
    if rejected:
        lines.append("rejected urls:")
        for entry in rejected:
            lines.append(f"  reason={entry['reason']}: {entry['input_url']}")
        lines.append("")
    if duplicates:
        lines.append("duplicates (post-lint):")
        for entry in duplicates:
            lines.append(f"  {entry['input_url']}")
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
    output_path = Path(args.output)
    rejected_output = (
        Path(args.rejected_output) if args.rejected_output else None
    )

    if not input_path.exists():
        print(f"error: input not found: {input_path}", file=err)
        return 2

    try:
        urls = load_input(input_path, fmt=args.format)
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=err)
        return 2

    accepted, rejected, duplicates = scaffold(urls)
    wrote_files = False

    if args.write_reviewed:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(accepted, indent=2) + "\n",
            encoding="utf-8",
        )
        wrote_files = True
        if rejected_output is not None:
            rejected_output.parent.mkdir(parents=True, exist_ok=True)
            rejected_output.write_text(
                json.dumps(
                    {
                        "rejected_count": len(rejected),
                        "duplicate_count": len(duplicates),
                        "rejected": rejected,
                        "duplicates": duplicates,
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

    if args.json:
        payload = {
            "input": str(input_path),
            "format": args.format,
            "output": str(output_path),
            "rejected_output": (
                str(rejected_output) if rejected_output else None
            ),
            "accepted_count": len(accepted),
            "rejected_count": len(rejected),
            "duplicate_count": len(duplicates),
            "wrote_files": wrote_files,
            "accepted": accepted,
            "rejected": rejected,
            "duplicates": duplicates,
        }
        print(json.dumps(payload, indent=2), file=out)
    else:
        print(
            _format_human_report(
                input_path=input_path,
                output_path=output_path,
                rejected_output=rejected_output,
                fmt=args.format,
                accepted=accepted,
                rejected=rejected,
                duplicates=duplicates,
                wrote_files=wrote_files,
            ),
            file=out,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
