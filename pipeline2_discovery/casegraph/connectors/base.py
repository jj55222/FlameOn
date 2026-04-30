from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import Dict, Iterable, List, Set

from ..models import CaseInput, SourceRecord


FORBIDDEN_SOURCE_FIELDS: Set[str] = {
    "artifact_verified",
    "confidence",
    "final_confidence",
    "final_verdict",
    "verdict",
    "verified_artifact",
    "verified_artifacts",
}


def validate_connector_source_record(source: SourceRecord) -> None:
    """Reject source records that try to make final CaseGraph decisions."""
    data = asdict(source)
    forbidden = sorted(FORBIDDEN_SOURCE_FIELDS & set(data))
    if forbidden:
        raise ValueError(f"SourceRecord contains final-decision fields: {', '.join(forbidden)}")

    metadata_keys = set((source.metadata or {}).keys())
    signal_keys = set((source.confidence_signals or {}).keys())
    nested_forbidden = sorted(FORBIDDEN_SOURCE_FIELDS & (metadata_keys | signal_keys))
    if nested_forbidden:
        raise ValueError(f"SourceRecord signals contain final-decision fields: {', '.join(nested_forbidden)}")


class SourceConnector(ABC):
    """No-API/non-final interface for CaseGraph source discovery connectors."""

    name: str = "base"

    def collect(self, case_input: CaseInput) -> List[SourceRecord]:
        sources = list(self.fetch(case_input))
        for source in sources:
            validate_connector_source_record(source)
        return sources

    @abstractmethod
    def fetch(self, case_input: CaseInput) -> Iterable[SourceRecord]:
        """Return source records only; never final confidence, verdict, or artifacts."""
        raise NotImplementedError
