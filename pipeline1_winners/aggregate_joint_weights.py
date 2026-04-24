"""
aggregate_joint_weights.py — moment × artifact joint weights from winners
==========================================================================

Builds a joint distribution P(moment_type | artifact_type) across the
winner corpus so Pipeline 4 can reward a case for having, e.g., a
contradiction inside interrogation footage differently than a
contradiction inside narration.

Two data sources per profile:
  1. `moments_by_artifact` — exact attribution (if the profile was analyzed
      with the extended prompt).
  2. Fallback: distribute `moment_types[type]` across the profile's
      `artifact_combination` weighted by `segment_stats.{artifact}_pct`.
      This gives a plausible estimate for the 29 legacy profiles.

Output schema (extends p1_scoring_weights):
{
  "moment_weights": {...},                  # unchanged from scoring_weights.json
  "arc_patterns": [...],                    # unchanged
  "artifact_value": {...},                  # unchanged
  "moment_artifact_weights": {              # NEW — per artifact × moment type
      "bodycam":       {contradiction: 0.14, ...},
      "interrogation": {reveal: 0.22, ...},
      ...
  },
  "moment_artifact_lift": {                 # NEW — how much a moment-type
      "bodycam":       {contradiction: 1.8, ...}  # over-indexes inside
      ...                                        # that artifact vs baseline
  },
  "channel_breakdown": {                    # NEW — per-channel joint dist
      "EXPLORE WITH US": {...},
      "Dr Insanity":     {...}
  },
  "_joint_metadata": { ... }
}

CLI:
    # Aggregate all profiles, blend with existing scoring_weights.json
    python aggregate_joint_weights.py --winners winners/ \\
        --base-weights scoring_weights.json \\
        --output scoring_weights_joint.json

    # Dry run — print to stdout, no write
    python aggregate_joint_weights.py --winners winners/ --dry-run
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path


HERE = Path(__file__).parent.resolve()
DEFAULT_WINNERS_DIR = HERE / "winners"
DEFAULT_BASE_WEIGHTS = HERE / "scoring_weights.json"
DEFAULT_OUTPUT = HERE / "scoring_weights_joint.json"

VALID_MOMENT_TYPES = [
    "contradiction", "emotional_peak", "procedural_violation",
    "reveal", "detail_noticed", "callback", "tension_shift",
]
VALID_ARTIFACTS = [
    "bodycam", "interrogation", "911_audio", "court_video",
    "news_clips", "documents", "narration",
]


# ─────────────────────────────────────────────────────────────
# Per-profile extraction
# ─────────────────────────────────────────────────────────────

def _seg_stat_pct(profile, artifact):
    """Pull an artifact's segment_stats share. Missing = 0."""
    ss = profile.get("segment_stats") or {}
    # segment_stats uses {bodycam_pct, narration_pct, interrogation_pct, other_pct}
    # Only 3 artifacts have direct keys — rest go in `other_pct`. For aggregation we
    # approximate by giving each non-tracked artifact an equal share of other_pct.
    direct_key = f"{artifact}_pct"
    if direct_key in ss:
        return float(ss.get(direct_key, 0) or 0)
    return 0.0


def _distribute_legacy(profile):
    """
    Fallback for profiles without `moments_by_artifact`: distribute each
    moment_type count across the profile's `artifact_combination`, weighted
    by segment_stats share where known.
    """
    moment_types = profile.get("moment_types") or {}
    combo = [a for a in (profile.get("artifact_combination") or []) if a in VALID_ARTIFACTS]
    if not combo:
        return {}

    # Build weights per artifact in combo
    weights = {}
    direct_sum = 0.0
    for a in combo:
        w = _seg_stat_pct(profile, a)
        weights[a] = w
        direct_sum += w

    # Artifacts without a direct segment_stats key share the `other_pct` bucket
    ss = profile.get("segment_stats") or {}
    other_pct = float(ss.get("other_pct", 0) or 0)
    untracked = [a for a, w in weights.items() if w == 0]
    if untracked and other_pct > 0:
        share = other_pct / len(untracked)
        for a in untracked:
            weights[a] = share

    # If every weight is still 0, fall back to uniform
    total_w = sum(weights.values())
    if total_w <= 0:
        weights = {a: 1.0 for a in combo}
        total_w = float(len(combo))

    # Normalize
    weights = {a: w / total_w for a, w in weights.items()}

    # Distribute moment counts
    distribution = defaultdict(lambda: defaultdict(float))
    for mt, count in moment_types.items():
        if mt not in VALID_MOMENT_TYPES or not isinstance(count, (int, float)):
            continue
        for a, w in weights.items():
            distribution[a][mt] += float(count) * w
    return distribution


def extract_joint_counts(profile):
    """
    Return (source, distribution_dict).
    source ∈ {"explicit", "distributed"}.
    distribution_dict[artifact][moment_type] = float count (fractional allowed).
    """
    mba = profile.get("moments_by_artifact")
    if isinstance(mba, dict) and mba:
        dist = defaultdict(lambda: defaultdict(float))
        for artifact, counts in mba.items():
            if artifact not in VALID_ARTIFACTS or not isinstance(counts, dict):
                continue
            for mt, v in counts.items():
                if mt not in VALID_MOMENT_TYPES:
                    continue
                try:
                    dist[artifact][mt] += float(v)
                except (TypeError, ValueError):
                    pass
        return "explicit", dist

    return "distributed", _distribute_legacy(profile)


# ─────────────────────────────────────────────────────────────
# Aggregation
# ─────────────────────────────────────────────────────────────

def aggregate(profiles):
    """
    Build joint weights from a list of profile dicts.
    Returns a dict ready to merge into scoring_weights.json.
    """
    total = defaultdict(lambda: defaultdict(float))   # artifact → moment → count
    explicit_count = 0
    distributed_count = 0

    # Per-channel aggregation
    per_channel = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    channel_video_counts = defaultdict(int)

    for p in profiles:
        source, dist = extract_joint_counts(p)
        if source == "explicit":
            explicit_count += 1
        else:
            distributed_count += 1

        channel = p.get("channel", "unknown")
        channel_video_counts[channel] += 1

        for artifact, counts in dist.items():
            for mt, v in counts.items():
                total[artifact][mt] += v
                per_channel[channel][artifact][mt] += v

    # Normalize per artifact so each row sums to 1.0 (conditional distribution)
    moment_artifact_weights = {}
    artifact_totals = {}  # total moments per artifact (for lift calc)
    for artifact in VALID_ARTIFACTS:
        counts = total.get(artifact, {})
        s = sum(counts.values())
        artifact_totals[artifact] = s
        if s <= 0:
            moment_artifact_weights[artifact] = {mt: 0.0 for mt in VALID_MOMENT_TYPES}
        else:
            moment_artifact_weights[artifact] = {mt: round(counts.get(mt, 0) / s, 4) for mt in VALID_MOMENT_TYPES}

    # Lift = P(moment | artifact) / P(moment)  — how much this moment-type over/under-indexes in this artifact
    grand_total = sum(artifact_totals.values())
    baseline = {}
    if grand_total > 0:
        for mt in VALID_MOMENT_TYPES:
            baseline[mt] = sum(total.get(a, {}).get(mt, 0) for a in VALID_ARTIFACTS) / grand_total
    else:
        baseline = {mt: 0.0 for mt in VALID_MOMENT_TYPES}

    moment_artifact_lift = {}
    for artifact in VALID_ARTIFACTS:
        lift = {}
        row = moment_artifact_weights[artifact]
        for mt in VALID_MOMENT_TYPES:
            base = baseline.get(mt, 0)
            if base > 0 and row[mt] > 0:
                lift[mt] = round(row[mt] / base, 3)
            else:
                lift[mt] = 0.0
        moment_artifact_lift[artifact] = lift

    # Per-channel normalization
    channel_breakdown = {}
    for channel, art_map in per_channel.items():
        channel_weights = {}
        for artifact in VALID_ARTIFACTS:
            counts = art_map.get(artifact, {})
            s = sum(counts.values())
            if s <= 0:
                channel_weights[artifact] = {mt: 0.0 for mt in VALID_MOMENT_TYPES}
            else:
                channel_weights[artifact] = {mt: round(counts.get(mt, 0) / s, 4) for mt in VALID_MOMENT_TYPES}
        channel_breakdown[channel] = {
            "n_videos": channel_video_counts[channel],
            "moment_artifact_weights": channel_weights,
        }

    return {
        "moment_artifact_weights": moment_artifact_weights,
        "moment_artifact_lift": moment_artifact_lift,
        "moment_artifact_baseline": {mt: round(v, 4) for mt, v in baseline.items()},
        "artifact_observation_totals": {a: round(v, 2) for a, v in artifact_totals.items()},
        "channel_breakdown": channel_breakdown,
        "_joint_metadata": {
            "profiles_total": len(profiles),
            "profiles_explicit": explicit_count,
            "profiles_distributed": distributed_count,
            "channels": list(channel_video_counts.keys()),
        },
    }


# ─────────────────────────────────────────────────────────────
# I/O
# ─────────────────────────────────────────────────────────────

def load_profiles(winners_dir):
    winners_dir = Path(winners_dir)
    if not winners_dir.exists():
        return []
    out = []
    for p in sorted(winners_dir.glob("*.json")):
        if p.name.endswith("_transcript.json"):
            continue
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
            if d.get("video_id"):
                out.append(d)
        except (json.JSONDecodeError, OSError):
            continue
    return out


def merge_with_base(base, joint):
    """Merge joint fields into base scoring_weights.json (base left unchanged if None)."""
    if base is None:
        return joint
    out = dict(base)
    for k, v in joint.items():
        out[k] = v
    return out


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Aggregate moment × artifact joint weights from winner profiles")
    parser.add_argument("--winners", default=str(DEFAULT_WINNERS_DIR),
                        help=f"Directory of winner profile JSONs (default: {DEFAULT_WINNERS_DIR})")
    parser.add_argument("--base-weights", default=str(DEFAULT_BASE_WEIGHTS),
                        help=f"Existing scoring_weights.json to blend with (default: {DEFAULT_BASE_WEIGHTS})")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT),
                        help=f"Output path (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--dry-run", action="store_true", help="Print to stdout; don't write")
    args = parser.parse_args()

    profiles = load_profiles(args.winners)
    if not profiles:
        print(f"[ERR] no winner profiles in {args.winners}")
        sys.exit(2)

    print(f"Loaded {len(profiles)} winner profiles")

    joint = aggregate(profiles)
    meta = joint["_joint_metadata"]
    print(f"  Explicit moments_by_artifact: {meta['profiles_explicit']}/{meta['profiles_total']}")
    print(f"  Distributed (legacy fallback): {meta['profiles_distributed']}/{meta['profiles_total']}")
    print(f"  Channels: {meta['channels']}")

    # Load base weights (optional)
    base = None
    if args.base_weights and Path(args.base_weights).exists():
        try:
            with open(args.base_weights, "r", encoding="utf-8") as f:
                base = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"  [WARN] could not load {args.base_weights}: {e}", file=sys.stderr)

    merged = merge_with_base(base, joint)

    if args.dry_run:
        print("\n[DRY RUN] merged weights preview:")
        preview = {k: merged[k] for k in (
            "moment_artifact_weights",
            "moment_artifact_lift",
            "moment_artifact_baseline",
            "artifact_observation_totals",
            "_joint_metadata",
        ) if k in merged}
        print(json.dumps(preview, indent=2, ensure_ascii=False))
        # Also print the lift highlights (what's most interesting)
        print("\n[DRY RUN] LIFT HIGHLIGHTS (moment-types that over-index per artifact):")
        for artifact, lifts in merged["moment_artifact_lift"].items():
            top = sorted(lifts.items(), key=lambda kv: kv[1], reverse=True)
            best = [(mt, l) for mt, l in top if l > 1.0][:3]
            if best:
                s = ", ".join(f"{mt}: {l}x" for mt, l in best)
                print(f"  {artifact:14s} → {s}")
        return

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print(f"\n✓ Wrote {out_path}")


if __name__ == "__main__":
    main()
