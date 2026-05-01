"""
pipeline4_score.py — Pipeline 4: Transcript Analysis + Narrative Scoring
=========================================================================

Decides PRODUCE / HOLD / SKIP for each case by running a two-pass LLM
analysis over Pipeline 3 transcripts combined with Pipeline 1 scoring weights.

Pass 1: Gemini 3.1 Flash Lite (long context, cheap) — structural extraction
Pass 2: Qwen 3.6 Plus (free) — narrative judgment from Pass 1 output

Transcripts sharing a case_id are merged into one unified narrative with
source_idx tags, then scored as a single case.

CLI:
    # Single transcript
    python pipeline4_score.py --transcript T.json --weights W.json --output verdicts/

    # Multiple transcripts (explicit)
    python pipeline4_score.py --transcripts T1.json T2.json --weights W.json --output verdicts/

    # Case-id filter against a directory
    python pipeline4_score.py --case-id sfdpa_0409-18 \\
        --transcript-dir ../pipeline3_audio/transcripts --weights W.json --output verdicts/

    # Batch: auto-group all transcripts by case_id
    python pipeline4_score.py --transcript-dir ../pipeline3_audio/transcripts \\
        --weights W.json --output verdicts/

    # Dry run
    python pipeline4_score.py --transcript T.json --dry-run
"""

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

# Load pipeline-local .env
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

from transcript_loader import (
    merge_transcripts,
    discover_case_transcripts,
    format_for_llm,
)
from llm_backends import LLMBackend, LLMError, repair_truncated_json
from prompts import render_pass1, render_pass2, DRY_RUN_PASS1_STUB, VALID_MOMENT_TYPES, VALID_ARC_TYPES
from scoring_math import compute_all, equal_weight_fallback


DEFAULT_PASS1_MODEL = os.environ.get(
    "P4_PASS1_MODEL", "google/gemini-3.1-flash-lite-preview"
)
DEFAULT_PASS2_MODEL = os.environ.get("P4_PASS2_MODEL", "qwen/qwen3.6-plus")

# Output token caps. Pass 1 (structural extraction) generates the
# largest JSON — at the prompt-imposed caps (60 moments, 100 timeline,
# 20 emotional_arc) a max-extraction Pass 1 can run ~9k tokens, which
# means the previous 8000 default was clipping. Bumping to 16000;
# Gemini Flash Lite supports up to 65535 output tokens via OpenRouter.
DEFAULT_PASS1_MAX_TOKENS = int(os.environ.get("P4_PASS1_MAX_TOKENS", "16000"))
DEFAULT_PASS2_MAX_TOKENS = int(os.environ.get("P4_PASS2_MAX_TOKENS", "3000"))

# Threshold (fraction of cap) at which we warn the operator that the
# response is approaching the output-token ceiling — a hint that the
# next-larger transcript may truncate.
NEAR_CAP_WARN_FRAC = float(os.environ.get("P4_NEAR_CAP_WARN_FRAC", "0.9"))


# ─────────────────────────────────────────────────────────────
# Input loading helpers
# ─────────────────────────────────────────────────────────────

def load_weights(path):
    """Load Pipeline 1 scoring weights JSON. Returns None on failure."""
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"  [WARN] Failed to load weights from {path}: {e}")
        return None


def load_case_research(path):
    """Load Pipeline 2 case research JSON. Returns None on failure."""
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"  [WARN] Failed to load case research from {path}: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# Pass 1 / Pass 2 execution
# ─────────────────────────────────────────────────────────────

def _log_response_size(label, raw, max_tokens, elapsed):
    """Log response size with a near-cap warning so the operator can
    spot output-token-ceiling hits before they manifest as JSON parse
    failures."""
    response_tokens_est = len(raw) // 4
    cap_pct = (response_tokens_est / max_tokens * 100) if max_tokens > 0 else 0
    if cap_pct >= NEAR_CAP_WARN_FRAC * 100:
        print(
            f"    [WARN] {label} returned in {elapsed:.1f}s — response ~{response_tokens_est} tokens "
            f"({cap_pct:.0f}% of {max_tokens} cap; near limit, output may be truncated)"
        )
    else:
        print(
            f"    Returned in {elapsed:.1f}s — response ~{response_tokens_est} tokens "
            f"({cap_pct:.0f}% of {max_tokens} cap)"
        )


def _parse_with_repair(raw, label):
    """Parse JSON from an LLM response. If the initial parse fails,
    attempt a brace-balance repair (handles output-cap truncation)
    before giving up. Returns the parsed dict or raises ``LLMError``."""
    try:
        return json.loads(raw)
    except json.JSONDecodeError as first_err:
        repaired = repair_truncated_json(raw)
        if repaired is None or repaired == raw:
            raise LLMError(
                f"{label} JSON parse failed: {first_err}\nFirst 500 chars: {raw[:500]}"
            )
        try:
            parsed = json.loads(repaired)
        except json.JSONDecodeError as second_err:
            raise LLMError(
                f"{label} JSON parse failed even after repair: "
                f"first={first_err}; repaired={second_err}\n"
                f"First 500 chars of raw: {raw[:500]}"
            )
        # Repair worked — log it so the operator knows the response
        # was truncated and can bump P4_PASS1_MAX_TOKENS for next run.
        print(
            f"    [WARN] {label} JSON was truncated by output-token cap — "
            f"repaired by closing {len(repaired) - len(raw.rstrip())} trailing chars. "
            f"Consider raising P4_PASS1_MAX_TOKENS / P4_PASS2_MAX_TOKENS."
        )
        return parsed


def run_pass1(merged, backend):
    """Execute Pass 1 structural extraction. Returns parsed dict or raises."""
    transcript_text = format_for_llm(merged)
    system, user = render_pass1(merged, transcript_text)

    print(f"  [Pass 1] Calling {backend.model}...")
    print(f"    Prompt size: ~{len(user) // 4} tokens ({len(user):,} chars)")

    t0 = time.time()
    raw = backend.complete(
        system=system,
        user=user,
        max_tokens=DEFAULT_PASS1_MAX_TOKENS,
        temperature=0.1,
    )
    elapsed = time.time() - t0
    _log_response_size("Pass 1", raw, DEFAULT_PASS1_MAX_TOKENS, elapsed)

    parsed = _parse_with_repair(raw, "Pass 1")

    # Light validation
    parsed = validate_pass1(parsed, merged)
    return parsed


def validate_pass1(pass1, merged):
    """
    Light validation of Pass 1 output. Drops invalid moments in place, coerces
    importance, sanity-checks timestamps against source durations.
    """
    pass1.setdefault("timeline", [])
    pass1.setdefault("moments", [])
    pass1.setdefault("contradictions", [])
    pass1.setdefault("speaker_dynamics", [])
    pass1.setdefault("emotional_arc", [])
    pass1.setdefault("factual_anchors", [])
    pass1.setdefault("detected_structure_hint", None)

    # Map source_idx → max_duration for timestamp sanity
    max_durations = {s["source_idx"]: s["duration_sec"] for s in merged["sources"]}

    valid_types_set = set(VALID_MOMENT_TYPES)
    valid_importance = {"critical", "high", "medium", "low"}

    cleaned_moments = []
    for m in pass1.get("moments", []):
        mtype = m.get("type")
        if mtype not in valid_types_set:
            continue
        sidx = m.get("source_idx", 0)
        if sidx not in max_durations:
            continue
        ts = m.get("timestamp_sec")
        if ts is None or ts < 0 or ts > max_durations[sidx] + 5:
            continue
        # Coerce importance
        imp = m.get("provisional_importance", "medium")
        if imp not in valid_importance:
            imp = "medium"
        m["provisional_importance"] = imp
        cleaned_moments.append(m)

    pass1["moments"] = cleaned_moments[:60]  # cap
    return pass1


def run_pass2(merged, pass1, weights, backend, scoring_breakdown, narrative_score):
    """Execute Pass 2 narrative judgment. Returns parsed dict or raises."""
    system, user = render_pass2(
        merged_transcript=merged,
        pass1_output=pass1,
        weights=weights,
        scoring_breakdown=scoring_breakdown,
        combined_score=narrative_score,
    )

    print(f"  [Pass 2] Calling {backend.model}...")
    print(f"    Prompt size: ~{len(user) // 4} tokens ({len(user):,} chars)")

    t0 = time.time()
    raw = backend.complete(
        system=system,
        user=user,
        max_tokens=DEFAULT_PASS2_MAX_TOKENS,
        temperature=0.2,
    )
    elapsed = time.time() - t0
    _log_response_size("Pass 2", raw, DEFAULT_PASS2_MAX_TOKENS, elapsed)

    parsed = _parse_with_repair(raw, "Pass 2")

    parsed = validate_pass2(parsed, pass1)
    return parsed


def validate_pass2(pass2, pass1):
    """
    Validate Pass 2 output. Drop hallucinated moments (timestamps not in Pass 1).
    Coerce verdict and arc_recommendation.
    """
    valid_verdicts = {"PRODUCE", "HOLD", "SKIP"}
    valid_types_set = set(VALID_MOMENT_TYPES)
    valid_importance = {"critical", "high", "medium", "low"}

    pass2["verdict"] = pass2.get("verdict", "HOLD")
    if pass2["verdict"] not in valid_verdicts:
        pass2["verdict"] = "HOLD"

    arc = pass2.get("narrative_arc_recommendation")
    if arc not in VALID_ARC_TYPES:
        pass2["narrative_arc_recommendation"] = pass1.get("detected_structure_hint") or "chronological"

    try:
        conf = float(pass2.get("confidence", 0.5))
        pass2["confidence"] = max(0.0, min(1.0, conf))
    except (TypeError, ValueError):
        pass2["confidence"] = 0.5

    # Build a lookup of valid (source_idx, timestamp_sec) pairs from Pass 1
    pass1_keys = {
        (m.get("source_idx", 0), round(float(m.get("timestamp_sec", 0)), 1))
        for m in pass1.get("moments", [])
    }

    cleaned_final = []
    for m in pass2.get("final_moments", []) or []:
        mtype = m.get("moment_type")
        if mtype not in valid_types_set:
            continue
        try:
            sidx = int(m.get("source_idx", 0))
            ts = float(m.get("timestamp_sec", 0))
        except (TypeError, ValueError):
            continue
        # Must exist in Pass 1 (within 1s tolerance)
        found = any(
            k[0] == sidx and abs(k[1] - round(ts, 1)) < 2.0 for k in pass1_keys
        )
        if not found:
            continue
        imp = m.get("importance", "medium")
        if imp not in valid_importance:
            imp = "medium"
        m["importance"] = imp
        cleaned_final.append(m)

    pass2["final_moments"] = cleaned_final
    pass2.setdefault("content_pitch", "")
    pass2.setdefault("reasoning_summary", "")
    return pass2


# ─────────────────────────────────────────────────────────────
# Score case — main orchestration
# ─────────────────────────────────────────────────────────────

def score_case(
    merged,
    weights,
    case_research,
    pass1_backend,
    pass2_backend,
    dry_run=False,
):
    """
    Score a single (possibly multi-transcript) case.
    Returns a verdict dict matching p4_to_p5_verdict schema.
    """
    case_id = merged["case_id"]
    print(f"\n{'=' * 70}")
    print(f"Scoring case: {case_id}")
    print(f"  Sources: {len(merged['sources'])}")
    print(f"  Total duration: {merged['total_duration_sec']:.0f}s ({merged['total_duration_sec'] / 60:.1f} min)")
    print(f"  Segments: {len(merged['segments'])}")
    print(f"  Evidence types: {merged.get('available_evidence_types', [])}")

    if weights is None:
        print("  [WARN] No Pipeline 1 weights provided, using equal-weight fallback (1/7 per moment type)")
        weights = equal_weight_fallback()

    if dry_run:
        return _dry_run_output(merged, weights, pass1_backend, pass2_backend)

    # Pass 1: structural extraction
    try:
        pass1 = run_pass1(merged, pass1_backend)
    except Exception as e:
        print(f"  [ERR] Pass 1 failed: {e}")
        return _error_verdict(merged, weights, error_str=f"pass1_failed: {e}")

    print(f"  [Pass 1] Extracted: {len(pass1.get('moments', []))} moments, "
          f"{len(pass1.get('contradictions', []))} contradictions, "
          f"{len(pass1.get('factual_anchors', []))} anchors, "
          f"hint={pass1.get('detected_structure_hint')}")

    # Deterministic pre-scoring
    available_set = set(merged.get("available_evidence_types", []))
    # Also include artifacts from case research if available
    if case_research:
        for src in case_research.get("sources", []):
            et = src.get("evidence_type")
            if et:
                available_set.add(et)

    scoring = compute_all(
        moments=pass1.get("moments", []),
        weights=weights,
        runtime_sec=merged["total_duration_sec"],
        available_artifacts=available_set,
        detected_structure=pass1.get("detected_structure_hint"),
        factual_anchors=pass1.get("factual_anchors", []),
    )

    from scoring_math import SUBSCORE_MISSING_FLOOR

    def _fmt(name, v):
        # Make missing-critical penalties VISIBLE in the run log.
        # `None` means "we lack reference data to measure this dimension"
        # — combine() substitutes a floor. Show that floor explicitly so
        # the operator can spot a degraded-data run, rather than hiding
        # it as a generic 'n/a'.
        if isinstance(v, (int, float)):
            return f"{v:.1f}"
        floor = SUBSCORE_MISSING_FLOOR.get(name, 0.0)
        # ASCII-only — Windows cp1252 consoles can't encode →.
        return f"MISS:floor={floor:.0f}"

    breakdown = scoring["scoring_breakdown"]
    print(f"  [Scoring] narrative_score={scoring['narrative_score']:.1f} "
          f"(density={_fmt('moment_density_score', breakdown['moment_density_score'])}, "
          f"arc={_fmt('arc_similarity_score', breakdown['arc_similarity_score'])}, "
          f"artifact={_fmt('artifact_completeness_score', breakdown['artifact_completeness_score'])}, "
          f"unique={_fmt('uniqueness_score', breakdown['uniqueness_score'])})")
    print(f"  [Scoring] Deterministic verdict: {scoring['verdict']} (conf={scoring['confidence']})")

    # Pass 2: LLM judgment
    degraded = False
    try:
        pass2 = run_pass2(
            merged, pass1, weights, pass2_backend,
            scoring_breakdown=scoring["scoring_breakdown"],
            narrative_score=scoring["narrative_score"],
        )
    except Exception as e:
        print(f"  [WARN] Pass 2 failed: {e}")
        print(f"  [WARN] Falling back to deterministic-only verdict")
        degraded = True
        # Build fallback pass2 from top 10 Pass 1 moments
        top_moments = sorted(
            pass1.get("moments", []),
            key=lambda m: {"critical": 4, "high": 3, "medium": 2, "low": 1}.get(
                m.get("provisional_importance", "medium"), 0
            ),
            reverse=True,
        )[:10]
        pass2 = {
            "verdict": scoring["verdict"],
            "confidence": min(scoring["confidence"], 0.5),
            "narrative_arc_recommendation": pass1.get("detected_structure_hint") or "chronological",
            "final_moments": [
                {
                    "moment_type": m.get("type"),
                    "source_idx": m.get("source_idx", 0),
                    "timestamp_sec": m.get("timestamp_sec"),
                    "end_timestamp_sec": m.get("end_timestamp_sec"),
                    "description": m.get("description", ""),
                    "importance": m.get("provisional_importance", "medium"),
                    "transcript_excerpt": m.get("transcript_excerpt", ""),
                }
                for m in top_moments
            ],
            "content_pitch": "Pass 2 failed — deterministic-only verdict based on Pass 1 moments.",
            "reasoning_summary": f"Fallback verdict. Error: {str(e)[:200]}",
        }

    # Reconcile: LLM verdict vs deterministic verdict
    # Trust the deterministic narrative_score, but let Pass 2 modulate.
    # If they disagree, the deterministic one wins on hard gates, Pass 2 wins on reasoning.
    llm_verdict = pass2.get("verdict", "HOLD")
    det_verdict = scoring["verdict"]

    # Reconciliation rules (env-tunable for experiments):
    #   P4_TRUST_DETERMINISTIC_PRODUCE=1 — if math says PRODUCE, accept it even
    #   when Pass 2 demotes to HOLD. Pass 2 has been conservative on winners
    #   that score 60+ (V6 calibration showed 5/9 winners landing wrong-HOLD).
    #   When deterministic = SKIP, still never let Pass 2 upgrade to PRODUCE.
    trust_det_produce = os.environ.get("P4_TRUST_DETERMINISTIC_PRODUCE", "0") == "1"

    if det_verdict == "SKIP" and llm_verdict == "PRODUCE":
        final_verdict = "HOLD"  # compromise — never SKIP→PRODUCE swing
    elif det_verdict == "PRODUCE" and llm_verdict == "SKIP":
        final_verdict = "HOLD"  # compromise — never PRODUCE→SKIP swing
    elif det_verdict == "PRODUCE" and llm_verdict == "HOLD" and trust_det_produce:
        final_verdict = "PRODUCE"  # math says PRODUCE; trust it over Pass 2's HOLD
    else:
        final_verdict = llm_verdict

    # Source references for p4_to_p5_verdict schema
    transcript_refs = merged.get("transcript_refs", [])
    source_refs = [case_research.get("case_id", "")] if case_research else []

    # Build artifact_completeness block
    top_combo_key = None
    if weights.get("artifact_value"):
        top_combo_key = max(weights["artifact_value"].items(), key=lambda x: x[1])[0]
    missing_recommended = scoring.get("missing_recommended_artifacts", [])

    artifact_completeness = {
        "available": sorted(available_set),
        "missing_recommended": missing_recommended,
    }

    verdict = {
        "case_id": case_id,
        "verdict": final_verdict,
        "narrative_score": scoring["narrative_score"],
        "confidence": pass2.get("confidence", scoring["confidence"]),
        "key_moments": [
            {
                "moment_type": m.get("moment_type"),
                "source_idx": m.get("source_idx", 0),
                "timestamp_sec": m.get("timestamp_sec"),
                "end_timestamp_sec": m.get("end_timestamp_sec"),
                "description": m.get("description", ""),
                "importance": m.get("importance", "medium"),
                "transcript_excerpt": m.get("transcript_excerpt", ""),
            }
            for m in pass2.get("final_moments", [])
        ],
        "content_pitch": pass2.get("content_pitch", ""),
        "narrative_arc_recommendation": pass2.get("narrative_arc_recommendation", "chronological"),
        "estimated_runtime_min": scoring["estimated_runtime_min"],
        "artifact_completeness": artifact_completeness,
        "scoring_breakdown": scoring["scoring_breakdown"],
        "transcript_refs": transcript_refs,
        "source_refs": source_refs,
        "_pipeline4_metadata": {
            "pass1_model": pass1_backend.model,
            "pass2_model": pass2_backend.model,
            "deterministic_verdict": det_verdict,
            "llm_verdict": llm_verdict,
            "reasoning_summary": pass2.get("reasoning_summary", ""),
            "degraded": degraded,
            "n_sources": len(merged["sources"]),
            "n_pass1_moments": len(pass1.get("moments", [])),
            "n_final_moments": len(pass2.get("final_moments", [])),
            "scored_at": datetime.utcnow().isoformat() + "Z",
        },
    }

    return verdict


def _dry_run_output(merged, weights, pass1_backend, pass2_backend):
    """Render both prompts without calling APIs."""
    print(f"\n[DRY RUN] Rendering Pass 1 + Pass 2 prompts without calling APIs...\n")

    transcript_text = format_for_llm(merged, max_chars=2000)
    system1, user1 = render_pass1(merged, transcript_text)

    print("=" * 70)
    print(f"PASS 1 — {pass1_backend.model}")
    print("=" * 70)
    print(f"[SYSTEM]\n{system1}\n")
    print(f"[USER]\n{user1}\n")

    # Use the stub Pass 1 result for Pass 2 rendering
    from scoring_math import compute_all as _compute
    scoring = _compute(
        moments=DRY_RUN_PASS1_STUB.get("moments", []),
        weights=weights,
        runtime_sec=merged["total_duration_sec"],
        available_artifacts=set(merged.get("available_evidence_types", [])),
        detected_structure=DRY_RUN_PASS1_STUB.get("detected_structure_hint"),
        factual_anchors=DRY_RUN_PASS1_STUB.get("factual_anchors", []),
    )

    system2, user2 = render_pass2(
        merged_transcript=merged,
        pass1_output=DRY_RUN_PASS1_STUB,
        weights=weights,
        scoring_breakdown=scoring["scoring_breakdown"],
        combined_score=scoring["narrative_score"],
    )

    print("=" * 70)
    print(f"PASS 2 — {pass2_backend.model} (rendered against STUB Pass 1)")
    print("=" * 70)
    print(f"[SYSTEM]\n{system2}\n")
    print(f"[USER]\n{user2}\n")

    print("=" * 70)
    print(f"[DRY RUN] Deterministic stub scoring: {scoring}")
    print(f"[DRY RUN] No API calls made. No file written.")
    print("=" * 70)
    return None


def _error_verdict(merged, weights, error_str):
    """Build a minimal verdict when Pass 1 completely fails."""
    return {
        "case_id": merged["case_id"],
        "verdict": "HOLD",
        "narrative_score": 0,
        "confidence": 0.1,
        "key_moments": [],
        "content_pitch": f"Pipeline 4 failed: {error_str}",
        "narrative_arc_recommendation": "chronological",
        "estimated_runtime_min": 5.0,
        "artifact_completeness": {
            "available": sorted(set(merged.get("available_evidence_types", []))),
            "missing_recommended": [],
        },
        "scoring_breakdown": {
            "moment_density_score": 0,
            "arc_similarity_score": 0,
            "artifact_completeness_score": 0,
            "uniqueness_score": 0,
        },
        "transcript_refs": merged.get("transcript_refs", []),
        "source_refs": [],
        "_pipeline4_metadata": {
            "_error": error_str,
            "scored_at": datetime.utcnow().isoformat() + "Z",
        },
    }


# ─────────────────────────────────────────────────────────────
# File I/O helpers
# ─────────────────────────────────────────────────────────────

def write_verdict(verdict, output_dir):
    """Save a verdict JSON to disk."""
    if verdict is None:
        return None
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{verdict['case_id']}_verdict.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(verdict, f, indent=2, ensure_ascii=False)
    print(f"  ✓ Saved: {out_path}")
    return out_path


def append_batch_summary(verdict, output_dir):
    """Append a row to batch_summary.tsv."""
    if verdict is None:
        return
    output_dir = Path(output_dir)
    summary_path = output_dir / "batch_summary.tsv"
    from scoring_math import SUBSCORE_MISSING_FLOOR

    def _fmt_subscore(v, name=None):
        # Subscores can be None when we lack reference data. For TSV
        # stability we render the per-subscore missing floor (the value
        # `combine` actually used) rather than a generic 0.0 — that way
        # the TSV reflects what actually went into narrative_score.
        if isinstance(v, (int, float)):
            return f"{v:.1f}"
        floor = SUBSCORE_MISSING_FLOOR.get(name, 0.0)
        return f"{floor:.1f}"

    is_new = not summary_path.exists()
    with open(summary_path, "a", encoding="utf-8") as f:
        if is_new:
            f.write(
                "case_id\tverdict\tnarrative_score\tconfidence\tn_sources\t"
                "n_moments\tdensity\tarc\tartifact\tunique\tscored_at\n"
            )
        bd = verdict.get("scoring_breakdown", {})
        meta = verdict.get("_pipeline4_metadata", {})
        f.write(
            f"{verdict['case_id']}\t{verdict['verdict']}\t"
            f"{verdict.get('narrative_score', 0):.1f}\t{verdict.get('confidence', 0):.2f}\t"
            f"{meta.get('n_sources', 0)}\t{meta.get('n_final_moments', 0)}\t"
            f"{_fmt_subscore(bd.get('moment_density_score'), 'moment_density_score')}\t"
            f"{_fmt_subscore(bd.get('arc_similarity_score'), 'arc_similarity_score')}\t"
            f"{_fmt_subscore(bd.get('artifact_completeness_score'), 'artifact_completeness_score')}\t"
            f"{_fmt_subscore(bd.get('uniqueness_score'), 'uniqueness_score')}\t"
            f"{meta.get('scored_at', '')}\n"
        )


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline 4: Transcript Analysis + Narrative Scoring"
    )

    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--transcript", help="Single Pipeline 3 transcript JSON")
    src.add_argument("--transcripts", nargs="+", help="Multiple transcripts to merge")
    src.add_argument("--transcript-dir", help="Directory of transcripts (auto-grouped by case_id)")

    parser.add_argument("--case-id", help="Filter --transcript-dir to one case_id")
    parser.add_argument("--weights", help="Pipeline 1 scoring_weights.json (optional, uses equal-weight fallback if omitted)")
    parser.add_argument("--case-research", help="Pipeline 2 case research JSON (optional)")
    parser.add_argument("--output", default="verdicts", help="Output directory (default: verdicts/)")
    parser.add_argument("--pass1-model", default=DEFAULT_PASS1_MODEL)
    parser.add_argument("--pass2-model", default=DEFAULT_PASS2_MODEL)
    parser.add_argument("--dry-run", action="store_true", help="Render prompts without calling APIs")
    parser.add_argument("--force", action="store_true", help="Re-score even if verdict file exists")
    args = parser.parse_args()

    # Load weights + case research
    weights = load_weights(args.weights)
    case_research = load_case_research(args.case_research)

    # Build backends (lazy — no API calls until .complete())
    try:
        pass1_backend = LLMBackend(model=args.pass1_model)
        pass2_backend = LLMBackend(model=args.pass2_model)
    except LLMError as e:
        print(f"[ERR] {e}")
        sys.exit(2)

    # Resolve which cases to score
    cases = []  # list of (case_id, merged_transcript)

    if args.transcript:
        merged = merge_transcripts([Path(args.transcript)])
        cases.append((merged["case_id"], merged))
    elif args.transcripts:
        merged = merge_transcripts([Path(p) for p in args.transcripts])
        cases.append((merged["case_id"], merged))
    elif args.transcript_dir:
        groups = discover_case_transcripts(args.transcript_dir, args.case_id)
        if not groups:
            print(f"[ERR] No transcripts found in {args.transcript_dir}")
            sys.exit(1)
        for cid, paths in groups.items():
            if not paths:
                continue
            merged = merge_transcripts(paths)
            cases.append((cid, merged))

    print(f"\nFound {len(cases)} case(s) to score")

    # Score each case
    results = []
    for cid, merged in cases:
        # Skip if verdict exists and not --force
        out_path = Path(args.output) / f"{cid}_verdict.json"
        if out_path.exists() and not args.force and not args.dry_run:
            print(f"\n[SKIP] {cid} already scored (use --force to re-run)")
            continue

        try:
            verdict = score_case(
                merged=merged,
                weights=weights,
                case_research=case_research,
                pass1_backend=pass1_backend,
                pass2_backend=pass2_backend,
                dry_run=args.dry_run,
            )
        except Exception as e:
            print(f"[ERR] Case {cid} failed: {e}")
            traceback.print_exc()
            continue

        if verdict is not None and not args.dry_run:
            write_verdict(verdict, args.output)
            append_batch_summary(verdict, args.output)
            results.append(verdict)

    # Final summary
    if not args.dry_run and results:
        print(f"\n{'=' * 70}")
        print(f"BATCH COMPLETE — {len(results)} case(s) scored")
        print(f"{'=' * 70}")
        counts = {"PRODUCE": 0, "HOLD": 0, "SKIP": 0}
        for v in results:
            counts[v.get("verdict", "HOLD")] = counts.get(v.get("verdict", "HOLD"), 0) + 1
        print(f"  PRODUCE: {counts['PRODUCE']}")
        print(f"  HOLD:    {counts['HOLD']}")
        print(f"  SKIP:    {counts['SKIP']}")
        if len(results) > 0:
            produce_rate = counts["PRODUCE"] / len(results) * 100
            print(f"  PRODUCE rate: {produce_rate:.0f}% (target: <30%)")


if __name__ == "__main__":
    main()
