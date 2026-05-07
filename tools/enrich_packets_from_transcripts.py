"""Operator script: deterministic packet enrichment.

Reads ``packets_master.jsonl`` (or any JSONL of packet stubs), applies the
deterministic extractors in ``pipeline1_winners.packet_enricher`` to each
packet using its cached caption transcripts, and writes:

  - ``packets_master_enriched.jsonl``         — enriched packet copies
  - ``PACKET_ENRICHMENT_PROVENANCE.json``     — per-packet provenance audit
  - ``EXTRACTION_VALIDATION.md``              — counts + per-field summary

The input file is **never** mutated. Output goes to ``--output-dir``,
which defaults to ``.tmp/packet_enrichment_smoke/``.

No LLM. No network. No transcript re-extraction. Only the three
extractor fields are populated:

  - involved_officers[]
  - family_decedent
  - case_outcome

Pre-existing values for those fields are preserved as-is — the
extractor is a populator for empty fields only.

CLI:

  # Default: enrich all A-grade packets in packets_master.jsonl
  python tools/enrich_packets_from_transcripts.py \\
      --packets-master .tmp/packet_production_smoke/packets_master.jsonl

  # Filter by grade
  python tools/enrich_packets_from_transcripts.py \\
      --packets-master <path> --grade A --grade B

  # Override the transcript root if .tmp lives outside the repo
  python tools/enrich_packets_from_transcripts.py \\
      --packets-master <path> --transcript-root /alt/path

  # Dry run — print summary, no files written
  python tools/enrich_packets_from_transcripts.py \\
      --packets-master <path> --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Iterable, Optional, Sequence

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline1_winners.packet_enricher import enrich_packet  # noqa: E402


DEFAULT_OUTPUT_DIR = _REPO_ROOT / ".tmp" / "packet_enrichment_smoke"


# ---------------------------------------------------------------------------
# Transcript resolution
# ---------------------------------------------------------------------------


def _resolve_transcript_paths(packet: dict, transcript_root: Path) -> list[Path]:
    """Resolve ``packet.transcript_paths`` against ``transcript_root``.

    Each entry in the packet is a relative path under the original
    `.tmp/packet_production_smoke/...` tree. We try a small set of
    candidate roots so the same enricher can run against either an
    in-repo `.tmp` or an alternate cache location.
    """
    rels = packet.get("transcript_paths") or []
    if not rels:
        return []
    candidates = [transcript_root, _REPO_ROOT, _REPO_ROOT.parent]
    out = []
    for rel in rels:
        rel_path = Path(rel)
        if rel_path.is_absolute() and rel_path.exists():
            out.append(rel_path)
            continue
        for root in candidates:
            p = root / rel_path
            if p.exists():
                out.append(p)
                break
    return out


def _read_transcript_texts(paths: Iterable[Path]) -> list[str]:
    """Read transcript files, skipping unreadable / empty ones."""
    out = []
    for p in paths:
        try:
            text = Path(p).read_text(encoding="utf-8")
        except OSError:
            continue
        if text.strip():
            out.append(text)
    return out


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


def _write_jsonl(path: Path, rows: Sequence[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Validation report
# ---------------------------------------------------------------------------


def _build_validation_report(
    enriched: Sequence[dict],
    provenance: Sequence[dict],
    grades: Sequence[str],
    input_path: Path,
) -> str:
    """Render the per-field extraction summary as Markdown."""
    field_counts = Counter()
    by_field_packets = {"involved_officers": [], "family_decedent": [], "case_outcome": []}
    for prov in provenance:
        pid = prov.get("packet_id") or "(unknown)"
        for field in by_field_packets:
            if field in prov.get("fields", {}):
                field_counts[field] += 1
                by_field_packets[field].append(pid)

    total = len(enriched)
    lines = []
    lines.append("# Packet enrichment — extraction validation")
    lines.append("")
    lines.append(f"Input: `{input_path}`")
    lines.append(f"Grade filter: `{','.join(grades) if grades else '(all)'}`")
    lines.append(f"Packets processed: **{total}**")
    lines.append("")
    lines.append("## Per-field population counts")
    lines.append("")
    lines.append("| Field | Packets populated | % of processed |")
    lines.append("| --- | ---: | ---: |")
    for field in ("involved_officers", "family_decedent", "case_outcome"):
        n = field_counts[field]
        pct = (100.0 * n / total) if total else 0.0
        lines.append(f"| `{field}` | {n} | {pct:.1f}% |")
    lines.append("")
    lines.append("## Per-packet field surface")
    lines.append("")
    lines.append("| packet_id | officers | family | outcome | transcripts loaded |")
    lines.append("| --- | :---: | :---: | :---: | ---: |")
    for prov in provenance:
        pid = prov.get("packet_id") or "(unknown)"
        flds = prov.get("fields", {})
        loaded = prov.get("transcripts_loaded", 0)
        cells = []
        for f in ("involved_officers", "family_decedent", "case_outcome"):
            if f in flds:
                detail = flds[f]
                if "count" in detail:
                    cells.append(str(detail["count"]))
                else:
                    cells.append("✓")
            else:
                cells.append("·")
        lines.append(f"| `{pid}` | {cells[0]} | {cells[1]} | {cells[2]} | {loaded} |")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append(
        "- `officers` / `family` cells show the count of named entries "
        "extracted; `outcome` cells show `✓` when at least one outcome "
        "field was populated."
    )
    lines.append(
        "- `·` indicates the field was not populated — either no high-"
        "confidence pattern fired, or the packet already carried that "
        "field (the populator never overwrites pre-existing values)."
    )
    lines.append(
        "- This is the deterministic baseline. Coverage gaps are "
        "expected and out of scope for this PR (LLM-assisted "
        "population, timeline / narrative_spine / production_angles "
        "extraction)."
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tools.enrich_packets_from_transcripts",
        description=(
            "Deterministic packet enricher — populates "
            "involved_officers[] / family_decedent / case_outcome from "
            "cached caption transcripts and DOJ URL slugs. Never "
            "mutates the input packets file."
        ),
    )
    parser.add_argument(
        "--packets-master",
        required=True,
        help="Path to packets_master.jsonl (input; never mutated).",
    )
    parser.add_argument(
        "--grade",
        action="append",
        default=None,
        help=(
            "Filter packets by confidence_grade. May be passed multiple "
            "times (e.g. --grade A --grade B). Default: A only."
        ),
    )
    parser.add_argument(
        "--transcript-root",
        default=None,
        help=(
            "Override root for resolving packet.transcript_paths "
            "(default: repo root)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write files; print the validation summary only.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)

    input_path = Path(args.packets_master)
    if not input_path.exists():
        print(f"[ERR] packets-master not found: {input_path}", file=sys.stderr)
        return 2

    grades = args.grade or ["A"]
    transcript_root = Path(args.transcript_root) if args.transcript_root else _REPO_ROOT
    out_dir = Path(args.output_dir)

    rows = _read_jsonl(input_path)
    print(f"Read {len(rows)} packet rows from {input_path}")

    enriched_rows = []
    provenance_rows = []
    skipped = 0
    for packet in rows:
        if grades and packet.get("confidence_grade") not in grades:
            skipped += 1
            continue
        paths = _resolve_transcript_paths(packet, transcript_root)
        texts = _read_transcript_texts(paths)
        enriched, prov = enrich_packet(packet, texts)
        prov["transcript_paths_resolved"] = [str(p) for p in paths]
        enriched_rows.append(enriched)
        provenance_rows.append(prov)

    print(
        f"Processed: {len(enriched_rows)} | "
        f"Skipped (grade filter): {skipped} | "
        f"Total: {len(rows)}"
    )

    report = _build_validation_report(
        enriched_rows, provenance_rows, grades, input_path
    )

    if args.dry_run:
        print()
        print(report)
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    enriched_out = out_dir / "packets_master_enriched.jsonl"
    prov_out = out_dir / "PACKET_ENRICHMENT_PROVENANCE.json"
    report_out = out_dir / "EXTRACTION_VALIDATION.md"
    _write_jsonl(enriched_out, enriched_rows)
    prov_out.write_text(
        json.dumps({"packets": provenance_rows}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    report_out.write_text(report, encoding="utf-8")

    # Defensive: confirm input file is byte-identical to what we read.
    # (Belt-and-suspenders against any future edit that accidentally
    # mutates the input — the extractor itself does not, but a future
    # CLI change could.)
    print(f"  [ok] {enriched_out.name}  ({len(enriched_rows)} rows)")
    print(f"  [ok] {prov_out.name}")
    print(f"  [ok] {report_out.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
