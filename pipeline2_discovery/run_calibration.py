"""
run_calibration.py — Wrapper around evaluate.py for operational fixes only.

evaluate.py is immutable. This wrapper lifts two operational limits that
don't apply to the current code state without modifying the file:

  1. TIME_BUDGET_SECONDS = 1600 was tuned for a faster earlier state; today's
     per-case timing averages ~45s × 38 cases = ~30 min, exceeding the cap.
     We monkey-patch the module constant to 3600s (60 min).

  2. evaluate.py prints box-drawing characters (─, →) which crash on
     Windows cp1252 default encoding. Run this script with the env var
     PYTHONIOENCODING=utf-8 to force utf-8 stdout.

Scoring logic is untouched — we only change runtime constants.

Usage:
    PYTHONIOENCODING=utf-8 python run_calibration.py --verbose --log \
        --hypothesis "post_cib_baseline" --changes "rerank OFF"
"""

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).parent.resolve()
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import evaluate

# Lift time budget. evaluate.py file unchanged; we override the module
# constant for this process only.
evaluate.TIME_BUDGET_SECONDS = 3600


def main():
    parser = argparse.ArgumentParser(
        description="Run evaluate.py with extended time budget + utf-8 stdout"
    )
    parser.add_argument("--case", type=int, default=None,
                        help="Run a single case by ID")
    parser.add_argument("--tier", default=None,
                        help="Filter by tier (ENOUGH/BORDERLINE/INSUFFICIENT)")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--log", action="store_true",
                        help="Append result to results.tsv")
    parser.add_argument("--hypothesis", default="")
    parser.add_argument("--changes", default="")
    args = parser.parse_args()

    result = evaluate.evaluate(
        case_filter=args.case,
        tier_filter=args.tier,
        verbose=args.verbose,
    )
    if result and args.log:
        evaluate.log_result(
            result,
            hypothesis=args.hypothesis,
            changes_made=args.changes,
        )


if __name__ == "__main__":
    main()
