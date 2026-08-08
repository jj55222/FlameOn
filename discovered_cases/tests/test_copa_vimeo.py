from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import copa_harvest as copa  # noqa: E402


def test_find_vimeo_urls_dedupes_and_canonicalizes():
    html = """
    <iframe src="//player.vimeo.com/video/12345?h=abc"></iframe>
    <iframe src="https://player.vimeo.com/video/12345"></iframe>
    <iframe src="https://player.vimeo.com/video/67890?badge=0"></iframe>
    """

    assert copa.find_vimeo_urls(html) == [
        "https://player.vimeo.com/video/12345",
        "https://player.vimeo.com/video/67890",
    ]


def test_vimeo_file_record_is_streaming_video_lead():
    rec = copa.vimeo_file_record(
        "https://player.vimeo.com/video/12345",
        "2025-0003972",
        1,
        metadata={"live": True, "title": "Log #2025-0003972 BWC 1", "duration": 82.0},
    )

    assert rec["name"] == "Log #2025-0003972 BWC 1"
    assert rec["evidence_type"] == "bodycam"
    assert rec["kind"] == "video"
    assert rec["streaming"] is True
    assert rec["downloadable"] is False
