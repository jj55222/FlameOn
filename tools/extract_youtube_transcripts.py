"""Operator script: YouTube caption extractor for high-confidence
enrichment results.

Reads an ``EnrichmentResult`` JSON document (typically the output
of ``tools/run_enrichment_tasks.py --provider youtube``), selects
URLs at or above a confidence threshold whose
``next_actions_hint`` includes ``youtube_metadata``, and fetches
their captions only via yt-dlp.

Captions only — no video / audio download. No Whisper. No new
YouTube searches. The script operates strictly on URLs already
present in the input JSON.

Defaults are dry-run; ``--run`` is required to actually invoke
yt-dlp.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Sequence

# Repo-root sys.path bootstrap (PR #28 pattern).
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline2_discovery.casegraph.cli import (  # noqa: E402
    BUNDLE_SAFE_DIRS,
    _is_safe_bundle_path,
)
from pipeline2_discovery.enrichment.youtube_transcripts import (  # noqa: E402
    DEFAULT_MAX_VIDEOS,
    DEFAULT_MIN_CONFIDENCE,
    SelectedVideo,
    TranscriptResult,
    YouTubeCaptionExtractor,
    load_context_lookup,
    score_transcript,
    select_urls_for_extraction,
    vtt_to_plaintext,
    write_artifacts_for_video,
)


CONFIDENCE_CHOICES = ("low", "medium", "high")


# ---- CLI ------------------------------------------------------------


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tools.extract_youtube_transcripts",
        description=(
            "YouTube caption-only extractor. Reads an enrichment-runner "
            "result JSON (provider=youtube), selects URLs at or above "
            "--min-confidence with a youtube_metadata next-action hint, "
            "and fetches their captions via yt-dlp (no video, no audio, "
            "no Whisper). Dry-run by default; pass --run to invoke yt-dlp."
        ),
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to an EnrichmentResult JSON list (the result file emitted "
             "by tools/run_enrichment_tasks.py --output-json ...).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help=(
            "Per-video output directory. Captions land at "
            "<output-dir>/<video_id>/<video_id>.<lang>.vtt; parsed plaintext "
            "lands at <output-dir>/<video_id>/<video_id>.txt; per-video "
            "metadata at <output-dir>/<video_id>/metadata.json. Subject to "
            "the safe-path policy (must resolve under one of "
            f"{', '.join(BUNDLE_SAFE_DIRS)} or outside the repo)."
        ),
    )
    parser.add_argument(
        "--summary-out",
        default=None,
        help=(
            "Optional path to write the aggregate summary JSON. Subject to "
            "the same safe-path policy as --output-dir."
        ),
    )
    parser.add_argument(
        "--tasks-file",
        default=None,
        help=(
            "Optional search_tasks.json path used to look up each "
            "candidate's identity context (agency / subject_name / city / "
            "state) for transcript relevance scoring. If omitted, scoring "
            "falls back to query-token matching."
        ),
    )
    parser.add_argument(
        "--min-confidence",
        choices=CONFIDENCE_CHOICES,
        default=DEFAULT_MIN_CONFIDENCE,
        help=(
            f"Minimum source-row confidence to consider for caption "
            f"extraction (default: {DEFAULT_MIN_CONFIDENCE})."
        ),
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        default=DEFAULT_MAX_VIDEOS,
        help=f"Cap on total videos to extract (default: {DEFAULT_MAX_VIDEOS}).",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help=(
            "Required to actually invoke yt-dlp. Without --run the script "
            "is dry-run only — selects URLs, plans output paths, and writes "
            "a summary, but never imports yt-dlp or hits the network."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a single JSON document on stdout instead of a human report.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


# ---- formatting -----------------------------------------------------


def _format_human_summary(summary: dict) -> str:
    lines = [
        "=== youtube transcript extractor ===",
        f"run_id:                {summary['run_id']}",
        f"dry_run:               {summary['dry_run']}",
        f"min_confidence:        {summary['min_confidence']}",
        f"max_videos:            {summary['max_videos']}",
        f"selected_count:        {summary['selected_count']}",
        f"downloaded_count:      {summary['downloaded_count']}",
        f"no_captions_count:     {summary['no_captions_count']}",
        f"failed_count:          {summary['failed_count']}",
        f"relevance_high:        {summary['relevance_high_count']}",
        f"relevance_medium:      {summary['relevance_medium_count']}",
        f"relevance_low:         {summary['relevance_low_count']}",
        "",
    ]
    if summary["results"]:
        lines.append("results:")
        for r in summary["results"]:
            tail = ""
            if r.get("transcript_path"):
                tail = f" -> {r['transcript_path']}"
            elif r.get("error"):
                tail = f" error={r['error']!r}"
            lines.append(
                f"  [{r['caption_status']:18s}] [{r['transcript_relevance']:7s}] "
                f"{r['candidate_id']} {r['video_id']}{tail}"
            )
    return "\n".join(lines).rstrip() + "\n"


# ---- main -----------------------------------------------------------


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    stdout=None,
    stderr=None,
    extractor: Optional[YouTubeCaptionExtractor] = None,
) -> int:
    """``extractor`` lets tests inject a fake YouTubeCaptionExtractor
    that bypasses yt-dlp entirely. Production passes ``None`` so the
    extractor is constructed lazily inside the run-mode branch only."""
    out = stdout if stdout is not None else sys.stdout
    err = stderr if stderr is not None else sys.stderr
    args = _parse_args(argv)

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"error: input not found: {input_path}", file=err)
        return 2

    if args.max_videos < 1:
        print(
            f"error: --max-videos must be >= 1; got {args.max_videos}",
            file=err,
        )
        return 2

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
                f"error: --summary-out must resolve under one of the safe "
                f"artifact dirs ({', '.join(BUNDLE_SAFE_DIRS)}) or outside "
                f"the repo entirely; got: {summary_out}",
                file=err,
            )
            return 2

    try:
        rows = json.loads(input_path.read_text(encoding="utf-8"))
    except (ValueError, OSError) as exc:
        print(f"error: could not load input: {exc}", file=err)
        return 2
    if not isinstance(rows, list):
        print(
            f"error: input must be a JSON list of EnrichmentResult rows; "
            f"got {type(rows).__name__}",
            file=err,
        )
        return 2

    selected = select_urls_for_extraction(
        rows,
        min_confidence=args.min_confidence,
        max_videos=args.max_videos,
    )

    contexts = load_context_lookup(
        Path(args.tasks_file) if args.tasks_file else None
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    dry_run = not args.run
    results = []

    if dry_run:
        for v in selected:
            results.append(_dry_run_row(v, output_dir))
    else:
        if extractor is None:
            extractor = YouTubeCaptionExtractor()
        for v in selected:
            results.append(_run_one(v, extractor, output_dir, contexts))

    summary = _aggregate(args, results, dry_run)

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

    return 0


# ---- per-video helpers ---------------------------------------------


def _dry_run_row(v: SelectedVideo, output_dir: Path) -> dict:
    from pipeline2_discovery.enrichment.youtube_transcripts import (
        _video_id_from_url,
    )
    vid = _video_id_from_url(v.url)
    planned = output_dir / vid
    return TranscriptResult(
        video_url=v.url,
        video_id=vid,
        candidate_id=v.candidate_id,
        source_query=v.source_query,
        source_confidence=v.source_confidence,
        caption_status="dry_run",
        caption_path=None,
        transcript_path=None,
        metadata_path=str(planned / "metadata.json"),
        transcript_relevance="unknown",
        notes=[
            "dry-run — yt-dlp was not invoked",
            f"planned_dir={planned}",
        ],
    ).to_dict()


def _run_one(
    v: SelectedVideo,
    extractor: YouTubeCaptionExtractor,
    output_dir: Path,
    contexts: dict,
) -> dict:
    extraction = extractor.extract(v.url, output_dir=output_dir)

    transcript_text = ""
    transcript_relevance = "unknown"
    matched_terms: list = []

    if extraction.status == "downloaded" and extraction.vtt_path:
        try:
            vtt = Path(extraction.vtt_path).read_text(encoding="utf-8")
            transcript_text = vtt_to_plaintext(vtt)
        except Exception as exc:
            extraction.notes.append(f"vtt_read_error={type(exc).__name__}: {exc}")

        if transcript_text:
            ctx = contexts.get(v.candidate_id, {}) if contexts else {}
            scored = score_transcript(
                transcript_text,
                source_query=v.source_query,
                context=ctx or None,
            )
            transcript_relevance = scored.relevance
            matched_terms = scored.matched_terms

    metadata = {
        "video_url": v.url,
        "video_id": extraction.video_id,
        "candidate_id": v.candidate_id,
        "source_query": v.source_query,
        "source_confidence": v.source_confidence,
        "caption_status": extraction.status,
        "matched_terms": matched_terms,
        "transcript_relevance": transcript_relevance,
    }

    transcript_path, metadata_path = write_artifacts_for_video(
        output_dir=output_dir,
        video_id=extraction.video_id,
        extraction=extraction,
        transcript_text=transcript_text,
        metadata=metadata,
    )

    return TranscriptResult(
        video_url=v.url,
        video_id=extraction.video_id,
        candidate_id=v.candidate_id,
        source_query=v.source_query,
        source_confidence=v.source_confidence,
        caption_status=extraction.status,
        caption_path=extraction.vtt_path,
        transcript_path=transcript_path,
        metadata_path=metadata_path,
        transcript_relevance=transcript_relevance,
        matched_terms=matched_terms,
        error=extraction.error,
        notes=list(extraction.notes),
    ).to_dict()


def _aggregate(args: argparse.Namespace, results: list, dry_run: bool) -> dict:
    from datetime import datetime, timezone
    started = datetime.now(timezone.utc)

    downloaded = sum(1 for r in results if r["caption_status"] == "downloaded")
    no_caps = sum(1 for r in results if r["caption_status"] == "no_captions_found")
    failed = sum(1 for r in results if r["caption_status"] == "failed")
    rel = {"high": 0, "medium": 0, "low": 0, "unknown": 0}
    for r in results:
        rel[r.get("transcript_relevance", "unknown")] = (
            rel.get(r.get("transcript_relevance", "unknown"), 0) + 1
        )

    return {
        "run_id": started.strftime("%Y%m%dT%H%M%SZ"),
        "started_at": started.isoformat(),
        "dry_run": dry_run,
        "input": str(args.input),
        "output_dir": str(args.output_dir),
        "min_confidence": args.min_confidence,
        "max_videos": args.max_videos,
        "selected_count": len(results),
        "downloaded_count": downloaded,
        "no_captions_count": no_caps,
        "failed_count": failed,
        "relevance_high_count": rel.get("high", 0),
        "relevance_medium_count": rel.get("medium", 0),
        "relevance_low_count": rel.get("low", 0),
        "relevance_unknown_count": rel.get("unknown", 0),
        "results": results,
    }


if __name__ == "__main__":
    sys.exit(main())
