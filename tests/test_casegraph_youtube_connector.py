import json
import os
import time
from pathlib import Path

import pytest
from jsonschema import Draft7Validator

from pipeline2_discovery.casegraph import (
    ConnectorUnavailable,
    YouTubeConnector,
    route_manual_defendant_jurisdiction,
)


ROOT = Path(__file__).resolve().parents[1]
CASE_PACKET_SCHEMA = ROOT / "schemas" / "p2_case_packet.schema.json"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def assert_valid_packet(packet):
    validator = Draft7Validator(load_json(CASE_PACKET_SCHEMA))
    errors = sorted(validator.iter_errors(packet.to_dict()), key=lambda e: list(e.path))
    assert errors == [], "\n".join(error.message for error in errors)


class FakeYoutubeDL:
    def __init__(self, opts):
        self.opts = opts

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def extract_info(self, query, download=False):
        assert download is False
        assert query.startswith("ytsearch")
        return {
            "entries": [
                {
                    "id": "abc123",
                    "title": "Police released bodycam footage in Min Jian Guan case",
                    "channel": "Example Police Department",
                    "description": "Body camera video released in San Francisco.",
                    "duration": 123,
                    "upload_date": "20260429",
                },
                {
                    "id": "def456",
                    "title": "Second bodycam result",
                    "channel": "Example News",
                    "description": "Bodycam report.",
                },
                {
                    "id": "ghi789",
                    "title": "Third bodycam result",
                    "channel": "Example Channel",
                    "description": "Bodycam report.",
                },
                {
                    "id": "jkl012",
                    "title": "Fourth bodycam result",
                    "channel": "Example Channel",
                    "description": "Bodycam report.",
                },
                {
                    "id": "mno345",
                    "title": "Fifth bodycam result",
                    "channel": "Example Channel",
                    "description": "Bodycam report.",
                },
            ]
        }


def test_mocked_ytdlp_response_creates_source_records():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    connector = YouTubeConnector(ydl_cls=FakeYoutubeDL)

    sources = connector.search(packet.input, max_results=3, max_queries=1)

    assert len(sources) == 3
    first = sources[0]
    assert first.source_id == "youtube_abc123"
    assert first.url == "https://www.youtube.com/watch?v=abc123"
    assert first.source_type == "video"
    assert "claim_source" in first.source_roles
    assert "possible_artifact_source" in first.source_roles
    assert "identity_source" in first.source_roles
    assert first.source_authority == "official"
    assert first.api_name == "youtube_yt_dlp"
    assert first.discovered_via == "Min Jian Guan bodycam"
    assert first.metadata["video_id"] == "abc123"
    assert first.metadata["channel"] == "Example Police Department"
    assert packet.verified_artifacts == []


def test_youtube_connector_hard_caps_results():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    connector = YouTubeConnector(ydl_cls=FakeYoutubeDL)

    sources = connector.search(packet.input, max_results=2, max_queries=1)

    assert len(sources) == 2


def test_artifact_looking_video_does_not_verify_artifact_or_change_packet_decisions():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    connector = YouTubeConnector(ydl_cls=FakeYoutubeDL)
    packet.sources.extend(connector.search(packet.input, max_results=1, max_queries=1))

    assert packet.verified_artifacts == []
    assert packet.verdict == "HOLD"
    assert packet.case_identity.identity_confidence == "low"
    assert packet.scores.artifact_score == 0.0
    assert_valid_packet(packet)


def test_ytdlp_missing_is_controlled(monkeypatch):
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    connector = YouTubeConnector()

    def missing():
        raise ConnectorUnavailable("yt-dlp is not installed")

    monkeypatch.setattr(connector, "_load_yt_dlp", missing)

    with pytest.raises(ConnectorUnavailable, match="yt-dlp is not installed"):
        connector.search(packet.input, max_results=1, max_queries=1)


@pytest.mark.skipif(os.environ.get("FLAMEON_RUN_LIVE_YOUTUBE") != "1", reason="live YouTube smoke is opt-in")
def test_live_youtube_smoke_metadata_only():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    connector = YouTubeConnector()
    started = time.perf_counter()
    try:
        sources = connector.search(packet.input, max_results=3, max_queries=1)
    except ConnectorUnavailable as exc:
        pytest.skip(str(exc))
    runtime = time.perf_counter() - started
    packet.sources.extend(sources)

    print(f"LIVE_YOUTUBE_QUERY={connector.last_query}")
    print(f"LIVE_YOUTUBE_RESULT_COUNT={len(sources)}")
    print(f"LIVE_YOUTUBE_RUNTIME_SECONDS={runtime:.2f}")
    print(f"LIVE_YOUTUBE_ERROR={connector.last_error or ''}")

    assert connector.last_query == "Min Jian Guan bodycam"
    assert len(sources) <= 3
    assert packet.verified_artifacts == []
    assert packet.verdict == "HOLD"
    assert packet.case_identity.identity_confidence == "low"
    assert_valid_packet(packet)
