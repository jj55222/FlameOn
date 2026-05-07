"""Operator script: regenerate P5 packet-mode briefs from a packets JSONL.

Reads any packets JSONL (raw `packets_master.jsonl` or the enriched
artifact written by ``tools/enrich_packets_from_transcripts.py``),
filters by confidence_grade, resolves each packet's cached caption
transcripts, and runs ``pipeline5_assembly.pipeline5_assemble.assemble_packet``
on each one.

Outputs:
  - ``{packet_id}_packet_brief.md``    per packet (P5 adapter format)
  - ``{packet_id}_packet_brief.json``  per packet
  - ``REGENERATION_REPORT.md``         summary with size delta vs a baseline dir

The input JSONL is **never** mutated. No LLM, no network, no transcript
re-extraction.

CLI:

  # Regenerate all A briefs from the enriched master, comparing against
  # the previous full-corpus generation directory:
  python tools/regenerate_packet_briefs.py \\
      --packets-master .tmp/packet_enrichment_smoke/packets_master_enriched.jsonl \\
      --transcript-root <repo-root-with-cached-transcripts> \\
      --output-dir .tmp/p5_regenerated_from_enriched_master \\
      --baseline-dir .tmp/p5_generated_full

  # Filter by grade
  python tools/regenerate_packet_briefs.py \\
      --packets-master <path> --grade A --grade B
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Iterable, Optional, Sequence

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
if str(_REPO_ROOT / "pipeline5_assembly") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "pipeline5_assembly"))

from pipeline5_assemble import assemble_packet  # noqa: E402


DEFAULT_OUTPUT_DIR = _REPO_ROOT / ".tmp" / "p5_regenerated_from_enriched_master"


# ---------------------------------------------------------------------------
# Transcript resolution
# ---------------------------------------------------------------------------


def _resolve_transcript_dir(packet: dict, transcript_root: Path) -> Optional[Path]:
    """Return the directory containing this packet's cached caption
    ``.txt`` files, or ``None`` if it cannot be resolved.

    P5's ``assemble_packet`` recursively scans a single dir for ``.txt``
    files. Each cached transcript path on the packet looks like
    ``<smoke_root>/transcripts/<candidate_id>/<video_id>/<video_id>.txt``;
    the natural per-packet root is the ``<candidate_id>`` segment so
    that all per-video transcripts under it are picked up.
    """
    rels = packet.get("transcript_paths") or []
    if not rels:
        return None
    candidates = [transcript_root, _REPO_ROOT, _REPO_ROOT.parent]
    first_rel = Path(rels[0])
    for root in candidates:
        if first_rel.is_absolute():
            full = first_rel
        else:
            full = root / first_rel
        if full.exists():
            # transcripts/<candidate_id>/<video_id>/<video_id>.txt
            # walk up two levels to <candidate_id>/
            return full.parent.parent
    return None


# ---------------------------------------------------------------------------
# JSONL I/O
# ---------------------------------------------------------------------------


def _read_jsonl(path: Path) -> list[dict]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


# ---------------------------------------------------------------------------
# Per-packet regeneration
# ---------------------------------------------------------------------------


def _regenerate_one(packet: dict, transcript_root: Path, out_dir: Path) -> dict:
    """Regenerate one packet brief. Returns a result dict with size /
    transcript-resolution metadata for the report.

    ``assemble_packet`` writes to ``out_dir`` directly; we don't move
    or rename. Each packet's brief lands as
    ``{packet_id}_packet_brief.{md,json}``.
    """
    pid = packet.get("packet_id") or "(unknown)"
    transcript_dir = _resolve_transcript_dir(packet, transcript_root)
    transcript_dir_str = str(transcript_dir) if transcript_dir is not None else None

    # We need an on-disk packet JSON for assemble_packet's contract.
    # Use a NamedTemporaryFile under the output dir so it's cleaned up
    # even if assemble_packet raises.
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", dir=str(out_dir), delete=False, encoding="utf-8",
    )
    try:
        json.dump(packet, tmp, ensure_ascii=False)
        tmp.close()
        brief = assemble_packet(
            packet_path=Path(tmp.name),
            transcript_dir=transcript_dir_str,
            dry_run=False,
            output_dir=str(out_dir),
        )
    finally:
        try:
            Path(tmp.name).unlink()
        except OSError:
            pass

    md_path = out_dir / f"{pid}_packet_brief.md"
    json_path = out_dir / f"{pid}_packet_brief.json"
    md_size = md_path.stat().st_size if md_path.exists() else 0
    json_size = json_path.stat().st_size if json_path.exists() else 0

    return {
        "packet_id": pid,
        "transcript_dir": transcript_dir_str,
        "md_path": str(md_path),
        "json_path": str(json_path),
        "md_size": md_size,
        "json_size": json_size,
        "brief_built": brief is not None,
        "had_optional_fields": {
            "involved_officers": bool(packet.get("involved_officers")),
            "family_decedent": bool(packet.get("family_decedent")),
            "case_outcome": bool(packet.get("case_outcome")),
        },
    }


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def _baseline_md_size(baseline_dir: Optional[Path], packet_id: str) -> Optional[int]:
    if baseline_dir is None:
        return None
    p = baseline_dir / f"{packet_id}_packet_brief.md"
    if not p.exists():
        return None
    return p.stat().st_size


def _build_report(
    results: Sequence[dict],
    input_path: Path,
    grades: Sequence[str],
    baseline_dir: Optional[Path],
) -> str:
    lines = []
    lines.append("# P5 brief regeneration — report")
    lines.append("")
    lines.append(f"Input:           `{input_path}`")
    lines.append(f"Grade filter:    `{','.join(grades) if grades else '(all)'}`")
    if baseline_dir is not None:
        lines.append(f"Baseline dir:    `{baseline_dir}`")
    lines.append(f"Packets built:   **{sum(1 for r in results if r['brief_built'])}**")
    lines.append("")

    have_baseline = baseline_dir is not None
    headers = ["packet_id", "officers?", "family?", "outcome?", "md size"]
    if have_baseline:
        headers += ["baseline size", "delta"]
    headers += ["transcript dir resolved?"]
    lines.append("| " + " | ".join(headers) + " |")
    sep = ["---"] * 4 + ["---:"] + (["---:", "---:"] if have_baseline else []) + [":---:"]
    lines.append("| " + " | ".join(sep) + " |")
    for r in results:
        row = [
            f"`{r['packet_id']}`",
            "✓" if r["had_optional_fields"]["involved_officers"] else "·",
            "✓" if r["had_optional_fields"]["family_decedent"] else "·",
            "✓" if r["had_optional_fields"]["case_outcome"] else "·",
            f"{r['md_size']:,}",
        ]
        if have_baseline:
            base_size = _baseline_md_size(baseline_dir, r["packet_id"])
            if base_size is None:
                row += ["—", "—"]
            else:
                delta = r["md_size"] - base_size
                pct = (100.0 * delta / base_size) if base_size else 0.0
                row += [f"{base_size:,}", f"{delta:+,} ({pct:+.1f}%)"]
        row += ["✓" if r["transcript_dir"] else "·"]
        lines.append("| " + " | ".join(row) + " |")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append(
        "- `officers?` / `family?` / `outcome?` columns reflect whether the "
        "INPUT packet carried each optional field. The renderer picks up "
        "those fields automatically — the brief size delta vs baseline is "
        "the operational signal that the schema fields actually surfaced "
        "in sections 3 / 4 / 5 / 8 / 10."
    )
    lines.append(
        "- `transcript dir resolved?` indicates whether the cached caption "
        "directory was found for this packet. When `·`, the brief still "
        "renders all 11 sections but section 7 falls back to the "
        "no-captions placeholder."
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tools.regenerate_packet_briefs",
        description=(
            "Regenerate P5 packet-mode briefs from a packets JSONL. "
            "Reads either a raw or enriched packets master; never "
            "mutates the input. Use --baseline-dir to compare against "
            "a previous brief generation directory."
        ),
    )
    parser.add_argument(
        "--packets-master",
        required=True,
        help="Path to a packets JSONL (input; never mutated).",
    )
    parser.add_argument(
        "--grade",
        action="append",
        default=None,
        help=(
            "Filter packets by confidence_grade. May be passed multiple "
            "times. Default: A only."
        ),
    )
    parser.add_argument(
        "--transcript-root",
        default=None,
        help="Override root for resolving packet.transcript_paths (default: repo root).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--baseline-dir",
        default=None,
        help=(
            "Optional directory containing a prior set of "
            "{packet_id}_packet_brief.md files for size-delta comparison."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)

    input_path = Path(args.packets_master)
    if not input_path.exists():
        print(f"[ERR] packets-master not found: {input_path}", file=sys.stderr)
        return 2

    import hashlib
    input_hash_before = hashlib.sha256(input_path.read_bytes()).hexdigest()

    grades = args.grade or ["A"]
    transcript_root = Path(args.transcript_root) if args.transcript_root else _REPO_ROOT
    out_dir = Path(args.output_dir)
    baseline_dir = Path(args.baseline_dir) if args.baseline_dir else None

    rows = _read_jsonl(input_path)
    print(f"Read {len(rows)} packet rows from {input_path}")

    targets = [p for p in rows if not grades or p.get("confidence_grade") in grades]
    print(f"Regenerating: {len(targets)} brief(s) (grade={','.join(grades)})")

    out_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for packet in targets:
        results.append(_regenerate_one(packet, transcript_root, out_dir))

    report_text = _build_report(results, input_path, grades, baseline_dir)
    (out_dir / "REGENERATION_REPORT.md").write_text(report_text, encoding="utf-8")

    # Confirm raw master byte-stability.
    input_hash_after = hashlib.sha256(input_path.read_bytes()).hexdigest()
    if input_hash_before != input_hash_after:
        print(
            f"[ERR] input packets file was mutated during regeneration "
            f"(sha256 {input_hash_before} -> {input_hash_after})",
            file=sys.stderr,
        )
        return 3

    built = sum(1 for r in results if r["brief_built"])
    print(f"\nBuilt: {built} | Total targets: {len(targets)}")
    print(f"Report: {out_dir / 'REGENERATION_REPORT.md'}")
    print(f"  [verified] input file unchanged (sha256 {input_hash_before[:12]}...)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
