"""Pipeline 1 dataset-source discovery lanes.

Pure module family. Ingests curated public datasets / saved API
results and emits normalized DatasetCandidate rows + downstream
search tasks + rough packet stubs. Never crawls. Never fetches the
web. Never calls portal-live, Firecrawl, or any other live API in
this package.

Outputs feed into:
  - Pipeline 2 portal-live (when a candidate carries an official URL)
  - existing MuckRock file resolver (when a candidate carries a
    MuckRock URL)
  - YouTube/transcript extraction (when a candidate carries a YouTube
    URL or a search query the operator can run)
  - identity / outcome validation (via emitted search tasks)

Source lane priorities (PORTAL_LIVE_OPERATOR.md mirrors this list):

  1. MuckRock released records
  2. SF Chronicle fatal pursuits (this PR)
  3. Official YouTube
  4. Official portals
  5. FARS validation
  6. Illinois / PA reports later
"""
from __future__ import annotations

from .models import (
    DATASET_GRADES,
    DatasetCandidate,
    NextActionHint,
    SourceLane,
)
from .scoring import assign_grade


__all__ = [
    "DATASET_GRADES",
    "DatasetCandidate",
    "NextActionHint",
    "SourceLane",
    "assign_grade",
]
