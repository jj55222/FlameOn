"""Shared pytest fixtures + sys.path setup for pipeline4_scoring/tests/.

The P4 modules (`pipeline4_score`, `scoring_math`, `prompts`,
`transcript_loader`, `llm_backends`) are top-level files inside
`pipeline4_scoring/`. The CLI relies on `cwd == pipeline4_scoring/`
to import them, so this conftest adds that dir to sys.path before
any test collection happens.
"""
import sys
from pathlib import Path

P4_DIR = Path(__file__).resolve().parent.parent
if str(P4_DIR) not in sys.path:
    sys.path.insert(0, str(P4_DIR))
