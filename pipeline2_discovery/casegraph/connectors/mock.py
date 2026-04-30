from __future__ import annotations

from typing import Iterable

from ..models import CaseInput, SourceRecord
from .base import SourceConnector


class MockSourceConnector(SourceConnector):
    """Static no-network connector used to exercise CaseGraph source plumbing."""

    name = "mock"

    def fetch(self, case_input: CaseInput) -> Iterable[SourceRecord]:
        case_input_id = case_input.raw_input.get("defendant_names")
        yield SourceRecord(
            source_id="mock_identity_001",
            url="https://example.org/news/min-jian-guan-case",
            title="Example report names Min Jian Guan",
            snippet="Min Jian Guan was identified in a San Francisco case.",
            raw_text="Min Jian Guan was identified in a San Francisco case.",
            source_type="news",
            source_roles=["identity_source"],
            source_authority="news",
            api_name=None,
            discovered_via="mock_fixture",
            retrieved_at="2026-04-29T23:40:00Z",
            case_input_id=case_input_id,
            metadata={"fixture_kind": "identity"},
            cost_estimate=0.0,
            confidence_signals={"matched_full_name": True, "matched_city": True},
            matched_case_fields=["defendant_full_name", "city"],
        )
        yield SourceRecord(
            source_id="mock_claim_001",
            url="https://example.org/news/min-jian-guan-bodycam-release",
            title="Example report says bodycam was released",
            snippet="The report says police released bodycam footage, but does not link the file.",
            raw_text="The report says police released bodycam footage, but does not link the file.",
            source_type="news",
            source_roles=["claim_source"],
            source_authority="news",
            api_name=None,
            discovered_via="mock_fixture",
            retrieved_at="2026-04-29T23:40:00Z",
            case_input_id=case_input_id,
            metadata={"fixture_kind": "claim", "claim_language": "released"},
            cost_estimate=0.0,
            confidence_signals={"mentions_bodycam": True, "release_language": True},
            matched_case_fields=["defendant_full_name"],
        )
        yield SourceRecord(
            source_id="mock_artifact_like_001",
            url="https://www.youtube.com/watch?v=ytap7WQnLK4",
            title="Example public video result",
            snippet="A public video result that looks like a possible artifact source.",
            raw_text="A public video result that looks like a possible artifact source.",
            source_type="video",
            source_roles=["artifact_source"],
            source_authority="third_party",
            api_name=None,
            discovered_via="mock_fixture",
            retrieved_at="2026-04-29T23:40:00Z",
            case_input_id=case_input_id,
            metadata={"fixture_kind": "artifact_like", "host": "youtube.com"},
            cost_estimate=0.0,
            confidence_signals={"public_video_url": True},
            matched_case_fields=["defendant_full_name"],
        )
