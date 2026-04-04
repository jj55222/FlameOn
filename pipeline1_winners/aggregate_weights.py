"""
aggregate_weights.py — Pipeline 1: Scoring Weight Generator
============================================================
Takes winner profiles + comment calibration data and produces
scoring_weights.json matching p1_scoring_weights in contracts.json.

CLI:
    python aggregate_weights.py --winners winners/ --comments calibration/ --output scoring_weights.json
    python aggregate_weights.py --winners winners/ --dry-run
"""

import argparse
import json
import os
import sys
from pathlib import Path
from collections import Counter


MOMENT_TYPES = [
    "contradiction", "emotional_peak", "procedural_violation",
    "reveal", "detail_noticed", "callback", "tension_shift",
]

COMMENT_WEIGHT = 0.70  # How much comment calibration influences final weights
PROFILE_WEIGHT = 0.30  # How much profile frequency influences final weights


# ──────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────

def load_profiles(winners_dir):
    """Load all winner profile JSONs from directory."""
    profiles = []
    winners_path = Path(winners_dir)
    if not winners_path.exists():
        print(f"[WARN] Winners directory not found: {winners_dir}")
        return profiles
    for f in sorted(winners_path.glob("*.json")):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if "video_id" in data and "narrative_arc" in data:
                profiles.append(data)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"  [WARN] Skipping {f.name}: {e}")
    return profiles


def load_calibrations(comments_dir):
    """Load all comment calibration JSONs from directory."""
    calibrations = []
    comments_path = Path(comments_dir)
    if not comments_path.exists():
        return calibrations
    for f in sorted(comments_path.glob("*_comments.json")):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if "moment_distribution" in data:
                calibrations.append(data)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"  [WARN] Skipping {f.name}: {e}")
    return calibrations


# ──────────────────────────────────────────────────────────────
# Weight computation
# ──────────────────────────────────────────────────────────────

def compute_moment_weights(profiles, calibrations):
    """
    Compute moment type weights from profile frequency + comment calibration.
    70% comment signal, 30% profile signal. Normalized to sum to 1.0.
    """
    # Profile signal: how often each moment type appears across winners
    profile_counts = Counter()
    for p in profiles:
        mt = p.get("moment_types", {})
        for k in MOMENT_TYPES:
            profile_counts[k] += mt.get(k, 0)
    profile_total = sum(profile_counts.values()) or 1
    profile_dist = {k: profile_counts[k] / profile_total for k in MOMENT_TYPES}

    # Comment signal: averaged moment distribution across calibration files
    if calibrations:
        comment_dist = {k: 0.0 for k in MOMENT_TYPES}
        for cal in calibrations:
            md = cal.get("moment_distribution", {})
            for k in MOMENT_TYPES:
                comment_dist[k] += md.get(k, 0)
        n_cal = len(calibrations)
        comment_dist = {k: v / n_cal for k, v in comment_dist.items()}

        # Blended weights
        blended = {}
        for k in MOMENT_TYPES:
            blended[k] = COMMENT_WEIGHT * comment_dist.get(k, 0) + PROFILE_WEIGHT * profile_dist.get(k, 0)
    else:
        # No calibration data — 100% profile signal
        blended = profile_dist.copy()

    # Normalize to sum to 1.0
    total = sum(blended.values()) or 1
    weights = {k: round(v / total, 4) for k, v in blended.items()}

    # Ensure exact sum to 1.0 by adjusting the largest weight
    diff = round(1.0 - sum(weights.values()), 4)
    if diff != 0:
        max_key = max(weights, key=weights.get)
        weights[max_key] = round(weights[max_key] + diff, 4)

    return weights


def compute_arc_patterns(profiles):
    """Compute narrative arc pattern frequencies and avg view counts."""
    arc_groups = {}
    for p in profiles:
        st = p.get("narrative_arc", {}).get("structure_type", "unknown")
        if st not in arc_groups:
            arc_groups[st] = {"count": 0, "total_views": 0}
        arc_groups[st]["count"] += 1
        arc_groups[st]["total_views"] += p.get("view_count", 0)

    total = len(profiles) or 1
    patterns = []
    for st, data in sorted(arc_groups.items(), key=lambda x: -x[1]["count"]):
        patterns.append({
            "structure_type": st,
            "frequency": round(data["count"] / total, 4),
            "avg_view_count": int(data["total_views"] / data["count"]) if data["count"] else 0,
        })
    return patterns


def compute_artifact_value(profiles):
    """Compute artifact combination values relative to avg performance."""
    combo_groups = {}
    global_views = []

    for p in profiles:
        artifacts = sorted(p.get("artifact_combination", []))
        combo_key = "+".join(artifacts) if artifacts else "none"
        views = p.get("view_count", 0)
        global_views.append(views)

        if combo_key not in combo_groups:
            combo_groups[combo_key] = {"count": 0, "total_views": 0}
        combo_groups[combo_key]["count"] += 1
        combo_groups[combo_key]["total_views"] += views

    global_avg = sum(global_views) / len(global_views) if global_views else 1

    artifact_value = {}
    for combo, data in combo_groups.items():
        combo_avg = data["total_views"] / data["count"] if data["count"] else 0
        artifact_value[combo] = round(combo_avg / global_avg, 4) if global_avg else 0

    # Normalize so max = 1.0
    max_val = max(artifact_value.values()) if artifact_value else 1
    if max_val > 0:
        artifact_value = {k: round(v / max_val, 4) for k, v in artifact_value.items()}

    return artifact_value


def compute_comment_calibration_summary(calibrations):
    """Aggregate comment calibration stats across all files."""
    if not calibrations:
        return None

    total_analyzed = sum(c.get("total_comments_analyzed", 0) for c in calibrations)
    total_timestamps = sum(c.get("timestamp_comment_count", 0) for c in calibrations)

    # Average moment distributions
    moment_dist = {k: 0.0 for k in MOMENT_TYPES}
    for cal in calibrations:
        md = cal.get("moment_distribution", {})
        for k in MOMENT_TYPES:
            moment_dist[k] += md.get(k, 0)
    n = len(calibrations)
    moment_dist = {k: round(v / n, 4) for k, v in moment_dist.items()}

    return {
        "total_comments_analyzed": total_analyzed,
        "moment_distribution": moment_dist,
        "timestamp_comment_count": total_timestamps,
    }


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pipeline 1: Aggregate scoring weights")
    parser.add_argument("--winners", required=True, help="Directory with winner profile JSONs")
    parser.add_argument("--comments", default=None, help="Directory with comment calibration JSONs")
    parser.add_argument("--output", default="scoring_weights.json", help="Output file path")
    parser.add_argument("--dry-run", action="store_true", help="Show weights preview, don't write file")
    args = parser.parse_args()

    print("Pipeline 1: Scoring Weight Aggregation")
    print("=" * 50)

    # Load data
    profiles = load_profiles(args.winners)
    print(f"Loaded {len(profiles)} winner profiles from {args.winners}")

    calibrations = []
    if args.comments:
        calibrations = load_calibrations(args.comments)
        print(f"Loaded {len(calibrations)} comment calibrations from {args.comments}")
    else:
        print("No comment calibration directory specified — using profile-only weights")

    if not profiles:
        print("[ERROR] No profiles found. Run analyze_winner.py first.")
        sys.exit(1)

    # Compute weights
    print("\nComputing weights...")
    moment_weights = compute_moment_weights(profiles, calibrations)
    arc_patterns = compute_arc_patterns(profiles)
    artifact_value = compute_artifact_value(profiles)
    comment_cal = compute_comment_calibration_summary(calibrations)

    scoring_weights = {
        "moment_weights": moment_weights,
        "arc_patterns": arc_patterns,
        "artifact_value": artifact_value,
    }
    if comment_cal:
        scoring_weights["comment_calibration"] = comment_cal

    # Display
    print("\n--- Moment Weights (sum to 1.0) ---")
    for k, v in sorted(moment_weights.items(), key=lambda x: -x[1]):
        bar = "#" * int(v * 50)
        print(f"  {k:25s}: {v:.4f}  {bar}")
    print(f"  {'SUM':25s}: {sum(moment_weights.values()):.4f}")

    print("\n--- Arc Patterns ---")
    for ap in arc_patterns:
        print(f"  {ap['structure_type']:25s}: {ap['frequency']:.0%} ({ap['avg_view_count']:,} avg views)")

    print("\n--- Artifact Value ---")
    for combo, val in sorted(artifact_value.items(), key=lambda x: -x[1]):
        print(f"  {combo:40s}: {val:.4f}")

    if comment_cal:
        print(f"\n--- Comment Calibration ---")
        print(f"  Total comments analyzed: {comment_cal['total_comments_analyzed']:,}")
        print(f"  Timestamp comments: {comment_cal['timestamp_comment_count']}")

    if args.dry_run:
        print("\n[DRY RUN] Would write to:", args.output)
        return

    # Save
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(scoring_weights, f, indent=2, ensure_ascii=False)
    print(f"\nSaved: {args.output}")

    # Validate sum
    ws = sum(scoring_weights["moment_weights"].values())
    if abs(ws - 1.0) > 0.001:
        print(f"[WARN] moment_weights sum to {ws}, expected 1.0")


if __name__ == "__main__":
    main()
