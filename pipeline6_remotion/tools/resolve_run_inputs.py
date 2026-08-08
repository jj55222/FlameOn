#!/usr/bin/env python3
"""Resolve generic bakeoff run inputs for render_cut.sh."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Optional, Sequence


def first_existing(paths):
    return next((path for path in paths if path.exists()), None)


def resolve(case_id: str, run_dir: Path) -> dict:
    run_dir = run_dir.expanduser().resolve()
    if not run_dir.is_dir():
        raise FileNotFoundError(f"run directory not found: {run_dir}")
    paper_candidates = sorted(run_dir.rglob("*_paper_edit.json"), key=lambda path: ("/render/" not in str(path), len(path.parts)))
    if not paper_candidates:
        raise FileNotFoundError(f"no paper-edit manifest under {run_dir}")
    contract = first_existing([run_dir / "repair/contract.json", run_dir / "contract.json"])
    if contract is None:
        contract = next(iter(sorted(run_dir.rglob("contract.json"))), None)
    if contract is None:
        raise FileNotFoundError(f"no contract.json under {run_dir}")

    roots = []
    explicit = os.environ.get("FLAMEON_DATA_ROOT")
    if explicit:
        roots.append(Path(explicit).expanduser().resolve())
    roots.extend(run_dir.parents)
    repo_root = next((root for root in roots if (root / ".tmp" / case_id).is_dir()), None)
    if repo_root is None:
        raise FileNotFoundError(f"cannot locate .tmp/{case_id}; set FLAMEON_DATA_ROOT")
    case_root = repo_root / ".tmp" / case_id
    transcripts = case_root / "d2/transcripts"
    return {"case_id": case_id, "run_dir": str(run_dir), "paper_edit": str(paper_candidates[0]),
            "contract": str(contract), "repo_root": str(repo_root), "case_root": str(case_root),
            "transcripts": str(transcripts)}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("case_id")
    parser.add_argument("run_dir")
    parser.add_argument("--field")
    args = parser.parse_args(argv)
    result = resolve(args.case_id, Path(args.run_dir))
    if args.field:
        if args.field not in result:
            raise KeyError(args.field)
        print(result[args.field])
    else:
        print(json.dumps(result, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
