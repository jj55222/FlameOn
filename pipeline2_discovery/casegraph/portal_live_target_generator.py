"""Portal-live target fixture generator.

Pure module that turns a curated list of candidate rows into
target-fixture JSON files. Never fetches the web; never crawls. The
input is a small operator-curated list; the output is a directory of
agency_ois-shaped fixtures the existing ``--portal-live`` mode can
drive.

Generation flow:

  list of rows
      -> lint each row via portal_live_target_lint.lint_candidate
      -> deduplicate by normalized URL and target_id
      -> cap at max_targets_per_run
      -> serialize one or both fixture variants per accepted row:
            <target_id>_real_fetch_only.json
            <target_id>_real_extract_required.json

The two variants are byte-equivalent except for three boolean fields
the orchestrator reads to gate extraction and replay
(``save_extracted_payload``, ``replay_through_portal_replay``,
``require_extraction``) and the trailing ``_extract_required`` suffix
on the extract-required fixture's ``target_id``.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Set, Tuple

from .portal_live_target_lint import (
    LintDecision,
    lint_candidate,
)


DEFAULT_MAX_TARGETS_PER_RUN = 5
GENERATION_MODES: Tuple[str, ...] = ("fetch_only", "extract_required", "both")
DEFAULT_GENERATION_MODE = "fetch_only"

# Static fixture fields shared by both variants.
_BASE_FIXTURE = {
    "profile_id": "agency_ois_detail",
    "fetcher": "requests",
    "max_pages": 1,
    "max_links": 5,
    "expected_response_status": 200,
    "save_raw_payload": True,
}


@dataclass
class GeneratorRowOutcome:
    """Per-row report. ``written_paths`` is empty when the row was
    rejected by lint or skipped post-lint (duplicate / cap)."""
    input_url: str
    custom_target_id: Optional[str]
    lint: LintDecision
    written_paths: List[Path] = field(default_factory=list)
    skipped_reason: Optional[str] = None  # e.g. "duplicate", "max_targets_cap"


@dataclass
class GeneratorResult:
    """Top-level run report."""
    outcomes: List[GeneratorRowOutcome]
    written_paths: List[Path]
    accepted_count: int
    rejected_count: int
    deduped_count: int
    capped_count: int
    dry_run: bool

    def to_dict(self) -> dict:
        return {
            "accepted_count": self.accepted_count,
            "rejected_count": self.rejected_count,
            "deduped_count": self.deduped_count,
            "capped_count": self.capped_count,
            "dry_run": self.dry_run,
            "written_paths": [str(p) for p in self.written_paths],
            "outcomes": [
                {
                    "input_url": o.input_url,
                    "custom_target_id": o.custom_target_id,
                    "lint": o.lint.to_dict(),
                    "written_paths": [str(p) for p in o.written_paths],
                    "skipped_reason": o.skipped_reason,
                }
                for o in self.outcomes
            ],
        }


def generate_targets(
    rows: Iterable[Mapping[str, object]],
    *,
    output_dir: Path,
    mode: str = DEFAULT_GENERATION_MODE,
    max_targets: int = DEFAULT_MAX_TARGETS_PER_RUN,
    dry_run: bool = False,
) -> GeneratorResult:
    """Generate portal-live target fixtures from curated rows.

    ``rows`` is an iterable of mappings with at minimum a ``url`` key.
    Optional keys: ``target_id`` (custom override), ``agency``,
    ``jurisdiction``, ``notes``. Rows with extra keys are tolerated;
    only ``url`` and ``target_id`` are read.

    ``mode`` selects which fixture variants to emit per accepted row:
      - ``"fetch_only"`` (default) — fetch_only fixture only
      - ``"extract_required"`` — extract_required fixture only
      - ``"both"`` — both variants

    ``output_dir`` must already exist when ``dry_run=False``. Filenames
    written are ``<target_id>_real_fetch_only.json`` and/or
    ``<target_id>_real_extract_required.json``.

    ``dry_run=True`` reports what would be written but creates no
    files. ``GeneratorResult.written_paths`` is still populated so the
    operator can see the planned output.
    """
    if mode not in GENERATION_MODES:
        raise ValueError(
            f"unsupported generation mode {mode!r}; "
            f"must be one of {GENERATION_MODES}"
        )
    if max_targets < 1:
        raise ValueError(f"max_targets must be >= 1; got {max_targets}")

    outcomes: List[GeneratorRowOutcome] = []
    written_paths: List[Path] = []
    seen_urls: Set[str] = set()
    seen_target_ids: Set[str] = set()
    accepted = 0
    rejected = 0
    deduped = 0
    capped = 0

    for row in rows:
        url = str(row.get("url") or "").strip()
        custom_id_raw = row.get("target_id")
        custom_id = (
            str(custom_id_raw).strip() if custom_id_raw is not None else None
        )
        custom_id = custom_id or None

        decision = lint_candidate(url, custom_target_id=custom_id)
        outcome = GeneratorRowOutcome(
            input_url=url,
            custom_target_id=custom_id,
            lint=decision,
        )

        if not decision.accepted:
            rejected += 1
            outcomes.append(outcome)
            continue

        # Dedupe by normalized URL AND target_id (either collision skips).
        if (
            decision.normalized_url in seen_urls
            or decision.target_id_suggestion in seen_target_ids
        ):
            deduped += 1
            outcome.skipped_reason = "duplicate"
            outcomes.append(outcome)
            continue

        # Cap.
        if accepted >= max_targets:
            capped += 1
            outcome.skipped_reason = "max_targets_cap"
            outcomes.append(outcome)
            continue

        seen_urls.add(decision.normalized_url)  # type: ignore[arg-type]
        seen_target_ids.add(decision.target_id_suggestion)  # type: ignore[arg-type]

        for variant in _variants_for_mode(mode):
            fixture = _fixture_for(decision, variant=variant)
            filename = (
                f"{decision.target_id_suggestion}_real_{variant}.json"
            )
            out_path = Path(output_dir) / filename
            if not dry_run:
                out_path.write_text(
                    json.dumps(fixture, indent=2) + "\n",
                    encoding="utf-8",
                )
            written_paths.append(out_path)
            outcome.written_paths.append(out_path)

        accepted += 1
        outcomes.append(outcome)

    return GeneratorResult(
        outcomes=outcomes,
        written_paths=written_paths,
        accepted_count=accepted,
        rejected_count=rejected,
        deduped_count=deduped,
        capped_count=capped,
        dry_run=dry_run,
    )


def _variants_for_mode(mode: str) -> Tuple[str, ...]:
    if mode == "fetch_only":
        return ("fetch_only",)
    if mode == "extract_required":
        return ("extract_required",)
    return ("fetch_only", "extract_required")


def _fixture_for(decision: LintDecision, *, variant: str) -> dict:
    """Build the agency_ois fixture dict for one accepted candidate.

    The fetch-only variant uses the lint's target_id_suggestion
    verbatim. The extract-required variant appends
    ``_extract_required`` to disambiguate the two saved-payload
    streams produced by the same URL on disk (see
    ``autoresearch/.runs/live_payloads/`` filename convention)."""
    if variant == "fetch_only":
        target_id = decision.target_id_suggestion
        return {
            "target_id": target_id,
            "url": decision.normalized_url,
            **dict(_BASE_FIXTURE),
            "allowed_domains": [decision.host],
            "save_extracted_payload": False,
            "replay_through_portal_replay": False,
            "require_extraction": False,
        }
    if variant == "extract_required":
        target_id = f"{decision.target_id_suggestion}_extract_required"
        return {
            "target_id": target_id,
            "url": decision.normalized_url,
            **dict(_BASE_FIXTURE),
            "allowed_domains": [decision.host],
            "save_extracted_payload": True,
            "replay_through_portal_replay": True,
            "require_extraction": True,
        }
    raise ValueError(f"unknown variant {variant!r}")


__all__ = [
    "DEFAULT_GENERATION_MODE",
    "DEFAULT_MAX_TARGETS_PER_RUN",
    "GENERATION_MODES",
    "GeneratorResult",
    "GeneratorRowOutcome",
    "generate_targets",
]
