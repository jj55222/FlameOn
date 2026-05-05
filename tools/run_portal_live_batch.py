"""Operator script: sequential portal-live batch runner over local fixtures.

Pure orchestrator. Takes already-generated portal-live target fixtures
(typically the output of ``tools/generate_portal_live_targets.py``)
and runs each through the existing CaseGraph CLI's ``--portal-live``
mode, one at a time, with courtesy delays between targets. Aggregates
the per-target results into one summary JSON.

What this is NOT:
- not a crawler — only consumes pre-existing local fixture JSON files
- not a discovery layer — never picks URLs, never reads search results
- not parallel — sequential by design so live HTTP load stays courteous
- not a retry mechanism — one attempt per fixture; failures stop or
  continue based on --continue-on-error

The batch runner shells out to ``python -m pipeline2_discovery.casegraph.cli``
once per fixture rather than re-implementing the portal-live
orchestration. This keeps all safety checks, env-gate handling,
bundle assembly, and KnownUrlLiveSmokeTarget preflight authoritative
in one place. Tests monkeypatch the subprocess wrapper to stay
zero-network.

Default mode is dry-run; ``--run`` is required to actually fetch.
The CLI's existing env gates (``FLAMEON_RUN_LIVE_CASEGRAPH=1`` AND
``FLAMEON_RUN_LIVE_PORTAL_FETCH=1``) are inherited from the parent
environment — the batch runner never bypasses them.

Output paths must resolve to one of the gitignored artifact dirs
(repo-root ``.tmp/``, ``.runs/``, ``.artifacts/``, ``.cache/``,
``.logs/``, or any of the same names under ``autoresearch/``); the
script reuses the CLI's own ``_is_safe_bundle_path`` check.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Sequence

# Repo-root sys.path bootstrap (PR #28 pattern; mirrored in the
# sister scripts so direct script invocation works from the repo root
# without PYTHONPATH=.).
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline2_discovery.casegraph.cli import (  # noqa: E402
    BUNDLE_SAFE_DIRS,
    _is_safe_bundle_path,
)


DEFAULT_PATTERN = "*_real_extract_required.json"
DEFAULT_MAX_TARGETS = 5
DEFAULT_DELAY_SECONDS = 2.0
DEFAULT_PER_FIXTURE_TIMEOUT_SECONDS = 120.0


# ---- per-target result row ------------------------------------------


@dataclass
class FixtureResult:
    """One row in ``summary["results"]``. Status is one of
    ``"dry_run"``, ``"completed"``, ``"blocked"``, ``"failed"``,
    or ``"skipped"``."""
    target_id: Optional[str]
    fixture_path: str
    url: Optional[str]
    fetcher: Optional[str]
    require_extraction: Optional[bool]
    bundle_path: Optional[str]
    status: str
    blocked_reason: Optional[str] = None
    status_code: Optional[int] = None
    api_calls: dict = field(default_factory=dict)
    raw_payload_path: Optional[str] = None
    extracted_payload_path: Optional[str] = None
    replayed: Optional[bool] = None
    verdict: Optional[str] = None
    reason_codes: List[str] = field(default_factory=list)
    verified_artifact_types: List[str] = field(default_factory=list)
    wallclock_seconds: Optional[float] = None
    error: Optional[str] = None


# ---- discovery / loading --------------------------------------------


def _load_fixture_metadata(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def select_fixtures(
    *,
    fixtures_dir: Optional[Path],
    explicit_fixtures: Sequence[Path],
    pattern: str,
    max_targets: int,
) -> List[Path]:
    """Discover fixtures from a single directory glob OR from an
    explicit list of paths. Mutually exclusive. Sorts paths for
    deterministic ordering. Caps at ``max_targets``.
    """
    if max_targets < 1:
        raise ValueError(f"--max-targets must be >= 1; got {max_targets}")
    if explicit_fixtures and fixtures_dir is not None:
        raise ValueError(
            "pass either --fixtures-dir or --fixture (one or more), not both"
        )
    if not explicit_fixtures and fixtures_dir is None:
        raise ValueError("must pass either --fixtures-dir or --fixture")

    if explicit_fixtures:
        paths = [Path(p) for p in explicit_fixtures]
        for p in paths:
            if not p.exists():
                raise ValueError(f"fixture not found: {p}")
        paths = sorted(paths)
    else:
        if not fixtures_dir.exists():
            raise ValueError(f"--fixtures-dir not found: {fixtures_dir}")
        if not fixtures_dir.is_dir():
            raise ValueError(f"--fixtures-dir is not a directory: {fixtures_dir}")
        paths = sorted(fixtures_dir.glob(pattern))

    if not paths:
        raise ValueError(
            f"no fixtures matched (pattern={pattern!r}, "
            f"fixtures_dir={fixtures_dir})"
        )

    return paths[:max_targets]


# ---- CLI subprocess wrapper (isolated for tests to monkeypatch) -----


def _run_cli_subprocess(
    *,
    fixture_path: Path,
    bundle_path: Path,
    env: dict,
    timeout: float = DEFAULT_PER_FIXTURE_TIMEOUT_SECONDS,
) -> subprocess.CompletedProcess:
    """Invoke the CaseGraph CLI as a subprocess for a single fixture."""
    return subprocess.run(
        [
            sys.executable, "-m", "pipeline2_discovery.casegraph.cli",
            "--portal-live",
            "--target-fixture", str(fixture_path),
            "--bundle-out", str(bundle_path),
            "--json",
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
        cwd=str(_REPO_ROOT),
    )


def _sleep(seconds: float) -> None:
    """Courtesy delay between targets. Isolated for tests to monkeypatch."""
    if seconds > 0:
        time.sleep(seconds)


# ---- result-row construction ----------------------------------------


def _result_from_cli_output(
    *,
    fixture_path: Path,
    bundle_path: Path,
    fixture_meta: dict,
    cli_proc: subprocess.CompletedProcess,
) -> FixtureResult:
    """Map subprocess output to a FixtureResult row.

    Maps the CLI's exit code + JSON output into the per-target row.
    The CLI emits ``live_fetch.status`` of ``"completed"`` /
    ``"blocked"``; anything else (including non-zero exit) is mapped
    to ``"failed"`` here.
    """
    target_id = fixture_meta.get("target_id")
    url = fixture_meta.get("url")
    fetcher = fixture_meta.get("fetcher")
    require_extraction = fixture_meta.get("require_extraction")

    if cli_proc.returncode != 0:
        return FixtureResult(
            target_id=target_id,
            fixture_path=str(fixture_path),
            url=url,
            fetcher=fetcher,
            require_extraction=require_extraction,
            bundle_path=str(bundle_path),
            status="failed",
            error=(
                (cli_proc.stderr or "").strip()
                or f"CLI exited with code {cli_proc.returncode}"
            ),
        )

    try:
        payload = json.loads(cli_proc.stdout)
    except json.JSONDecodeError as exc:
        return FixtureResult(
            target_id=target_id,
            fixture_path=str(fixture_path),
            url=url,
            fetcher=fetcher,
            require_extraction=require_extraction,
            bundle_path=str(bundle_path),
            status="failed",
            error=f"could not parse CLI JSON: {exc}",
        )

    lf = payload.get("live_fetch") or {}
    result = payload.get("result") or {}
    packet = payload.get("packet_summary") or {}

    raw_status = lf.get("status") or "unknown"
    if raw_status == "completed":
        status = "completed"
    elif raw_status == "blocked":
        status = "blocked"
    else:
        status = "failed"

    return FixtureResult(
        target_id=lf.get("target_id") or target_id,
        fixture_path=str(fixture_path),
        url=lf.get("url") or url,
        fetcher=lf.get("fetcher") or fetcher,
        require_extraction=lf.get("require_extraction"),
        bundle_path=str(bundle_path),
        status=status,
        blocked_reason=lf.get("blocked_reason"),
        status_code=lf.get("status_code"),
        api_calls=dict(lf.get("api_calls") or {}),
        raw_payload_path=lf.get("raw_payload_path"),
        extracted_payload_path=lf.get("extracted_payload_path"),
        replayed=lf.get("replayed"),
        verdict=result.get("verdict"),
        reason_codes=list(result.get("reason_codes") or []),
        verified_artifact_types=list(packet.get("verified_artifact_types") or []),
        wallclock_seconds=lf.get("wallclock_seconds"),
    )


# ---- main batch loop ------------------------------------------------


def run_batch(
    *,
    fixtures: List[Path],
    output_dir: Path,
    delay_seconds: float,
    dry_run: bool,
    continue_on_error: bool,
    env: Optional[dict] = None,
) -> dict:
    """Top-level batch runner. Returns the summary dict."""
    started = datetime.now(timezone.utc)
    results: List[FixtureResult] = []
    completed = 0
    failed = 0
    blocked = 0
    skipped = 0
    api_call_totals: dict = {}

    cli_env = env if env is not None else dict(os.environ)
    last_idx = len(fixtures) - 1

    for i, fixture_path in enumerate(fixtures):
        is_last = (i == last_idx)

        try:
            fixture_meta = _load_fixture_metadata(fixture_path)
        except (json.JSONDecodeError, OSError) as exc:
            results.append(FixtureResult(
                target_id=None,
                fixture_path=str(fixture_path),
                url=None,
                fetcher=None,
                require_extraction=None,
                bundle_path=None,
                status="failed",
                error=f"could not load fixture: {exc}",
            ))
            failed += 1
            if not dry_run and not continue_on_error:
                break
            continue

        target_id = fixture_meta.get("target_id") or fixture_path.stem
        bundle_path = output_dir / target_id / "bundle.json"

        if dry_run:
            results.append(FixtureResult(
                target_id=target_id,
                fixture_path=str(fixture_path),
                url=fixture_meta.get("url"),
                fetcher=fixture_meta.get("fetcher"),
                require_extraction=fixture_meta.get("require_extraction"),
                bundle_path=str(bundle_path),
                status="dry_run",
            ))
            continue

        # --- run mode ---
        bundle_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            cli_proc = _run_cli_subprocess(
                fixture_path=fixture_path,
                bundle_path=bundle_path,
                env=cli_env,
            )
        except subprocess.TimeoutExpired as exc:
            row = FixtureResult(
                target_id=target_id,
                fixture_path=str(fixture_path),
                url=fixture_meta.get("url"),
                fetcher=fixture_meta.get("fetcher"),
                require_extraction=fixture_meta.get("require_extraction"),
                bundle_path=str(bundle_path),
                status="failed",
                error=f"CLI subprocess timed out: {exc}",
            )
            results.append(row)
            failed += 1
            if not continue_on_error:
                break
            if not is_last:
                _sleep(delay_seconds)
            continue
        except FileNotFoundError as exc:
            # python executable not on PATH, etc. Rare; report cleanly.
            row = FixtureResult(
                target_id=target_id,
                fixture_path=str(fixture_path),
                url=fixture_meta.get("url"),
                fetcher=fixture_meta.get("fetcher"),
                require_extraction=fixture_meta.get("require_extraction"),
                bundle_path=str(bundle_path),
                status="failed",
                error=f"failed to spawn CLI subprocess: {exc}",
            )
            results.append(row)
            failed += 1
            if not continue_on_error:
                break
            if not is_last:
                _sleep(delay_seconds)
            continue

        row = _result_from_cli_output(
            fixture_path=fixture_path,
            bundle_path=bundle_path,
            fixture_meta=fixture_meta,
            cli_proc=cli_proc,
        )
        results.append(row)

        if row.status == "completed":
            completed += 1
            for provider, count in (row.api_calls or {}).items():
                try:
                    api_call_totals[provider] = (
                        api_call_totals.get(provider, 0) + int(count)
                    )
                except (TypeError, ValueError):
                    pass
        elif row.status == "blocked":
            blocked += 1
        elif row.status == "failed":
            failed += 1
        else:
            skipped += 1

        is_failure = row.status in ("failed", "blocked")
        if is_failure and not continue_on_error:
            break
        if not is_last:
            _sleep(delay_seconds)

    finished = datetime.now(timezone.utc)
    elapsed = (finished - started).total_seconds()

    return {
        "run_id": started.strftime("%Y%m%dT%H%M%SZ"),
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "elapsed_seconds": elapsed,
        "dry_run": dry_run,
        "selected_count": len(fixtures),
        "attempted_count": len(results),
        "completed_count": completed,
        "failed_count": failed,
        "blocked_count": blocked,
        "skipped_count": skipped,
        "total_api_calls": api_call_totals,
        "fixtures_selected": [str(p) for p in fixtures],
        "output_dir": str(output_dir),
        "delay_seconds": delay_seconds,
        "continue_on_error": continue_on_error,
        "results": [asdict(r) for r in results],
    }


# ---- CLI ------------------------------------------------------------


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tools.run_portal_live_batch",
        description=(
            "Sequential portal-live batch runner over local generated "
            "fixture JSON files. Default mode is dry-run; pass --run to "
            "actually invoke the CaseGraph CLI per fixture. Live HTTP "
            "requires the same env gates the CLI requires "
            "(FLAMEON_RUN_LIVE_CASEGRAPH=1 AND "
            "FLAMEON_RUN_LIVE_PORTAL_FETCH=1)."
        ),
    )
    parser.add_argument(
        "--fixtures-dir",
        dest="fixtures_dir",
        default=None,
        help=(
            "Directory to glob (non-recursively) for fixture JSON files. "
            "Mutually exclusive with --fixture."
        ),
    )
    parser.add_argument(
        "--fixture",
        dest="fixtures",
        action="append",
        default=[],
        help=(
            "Path to a single fixture JSON. Repeat for multiple. "
            "Mutually exclusive with --fixtures-dir."
        ),
    )
    parser.add_argument(
        "--pattern",
        default=DEFAULT_PATTERN,
        help=(
            f"Glob pattern under --fixtures-dir (default: "
            f"{DEFAULT_PATTERN!r}; targets extract-required fixtures only)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        required=True,
        help=(
            "Directory to write per-target run bundles into. Must "
            "resolve to one of the CLI's safe artifact dirs "
            f"({', '.join(BUNDLE_SAFE_DIRS)}) or outside the repo."
        ),
    )
    parser.add_argument(
        "--summary-out",
        dest="summary_out",
        default=None,
        help=(
            "Optional path to write the aggregate summary JSON. Subject "
            "to the same safe-path policy as --output-dir. Without "
            "this, the summary is only printed to stdout."
        ),
    )
    parser.add_argument(
        "--max-targets",
        dest="max_targets",
        type=int,
        default=DEFAULT_MAX_TARGETS,
        help=(
            f"Hard cap on selected targets per run (default: "
            f"{DEFAULT_MAX_TARGETS})."
        ),
    )
    parser.add_argument(
        "--delay-seconds",
        dest="delay_seconds",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help=(
            f"Courtesy delay between targets in seconds (default: "
            f"{DEFAULT_DELAY_SECONDS}). Applied between successive "
            "fixtures only — not after the last."
        ),
    )
    parser.add_argument(
        "--run",
        dest="run",
        action="store_true",
        help=(
            "Required to actually invoke the CLI for each fixture. "
            "Without --run, the script is dry-run only — it reports "
            "which fixtures would be run and to where."
        ),
    )
    parser.add_argument(
        "--continue-on-error",
        dest="continue_on_error",
        action="store_true",
        help=(
            "Continue running subsequent fixtures even if a fixture "
            "fails or is blocked. Default behavior is stop-on-first-failure."
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
        "=== portal-live batch runner ===",
        f"run_id:           {summary['run_id']}",
        f"dry_run:          {summary['dry_run']}",
        f"selected_count:   {summary['selected_count']}",
        f"attempted_count:  {summary['attempted_count']}",
        f"completed_count:  {summary['completed_count']}",
        f"failed_count:     {summary['failed_count']}",
        f"blocked_count:    {summary['blocked_count']}",
        f"skipped_count:    {summary['skipped_count']}",
        f"total_api_calls:  {summary['total_api_calls']}",
        f"elapsed_seconds:  {summary['elapsed_seconds']:.2f}",
        f"output_dir:       {summary['output_dir']}",
        "",
    ]
    if summary["results"]:
        lines.append("results:")
        for r in summary["results"]:
            tid = r.get("target_id") or "(unknown)"
            tail = ""
            if r.get("verdict"):
                tail += f" verdict={r['verdict']}"
            if r.get("blocked_reason"):
                tail += f" blocked_reason={r['blocked_reason']}"
            if r.get("error"):
                tail += f" error={r['error']!r}"
            lines.append(f"  [{r['status']}] {tid}{tail}")
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

    output_dir = Path(args.output_dir)
    if not _is_safe_bundle_path(output_dir):
        print(
            f"error: --output-dir must resolve under one of the safe "
            f"artifact dirs ({', '.join(BUNDLE_SAFE_DIRS)}) or outside "
            f"the repo entirely; got: {output_dir}",
            file=err,
        )
        return 2

    summary_out: Optional[Path] = None
    if args.summary_out:
        summary_out = Path(args.summary_out)
        if not _is_safe_bundle_path(summary_out):
            print(
                f"error: --summary-out must resolve under one of the "
                f"safe artifact dirs ({', '.join(BUNDLE_SAFE_DIRS)}) or "
                f"outside the repo entirely; got: {summary_out}",
                file=err,
            )
            return 2

    fixtures_dir = (
        Path(args.fixtures_dir) if args.fixtures_dir else None
    )
    explicit_fixtures = [Path(f) for f in (args.fixtures or [])]

    try:
        fixtures = select_fixtures(
            fixtures_dir=fixtures_dir,
            explicit_fixtures=explicit_fixtures,
            pattern=args.pattern,
            max_targets=args.max_targets,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=err)
        return 2

    summary = run_batch(
        fixtures=fixtures,
        output_dir=output_dir,
        delay_seconds=args.delay_seconds,
        dry_run=not args.run,
        continue_on_error=args.continue_on_error,
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

    if summary["failed_count"] > 0 and not args.continue_on_error:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
