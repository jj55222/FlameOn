from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import vet_bundles as vb  # noqa: E402


def test_longbeach_mediahandler_prefers_download_url_and_normalizes_ext():
    candidate = {
        "source": "longbeach_laserfiche",
        "case_id": "lb_1",
        "media_files": [
            {
                "name": "BWC 1",
                "ext": "mp4",
                "url": "https://citydocs.longbeach.gov/LBPDPublicDocs/mediahandler.ashx?id=123",
                "download_url": "https://citydocs.longbeach.gov/LBPDPublicDocs/ElectronicFile.aspx?docid=123",
            }
        ],
        "doc_files": [{"name": "Report", "ext": "pdf", "url": "https://example.test/report.pdf"}],
    }

    clean = vb.vet_candidate(candidate)

    assert clean["media_files"][0]["url"].endswith("ElectronicFile.aspx?docid=123")
    assert clean["media_files"][0]["vet_original_url"].endswith("mediahandler.ashx?id=123")
    assert clean["media_files"][0]["ext"] == ".mp4"
    assert clean["n_video"] == 1
    assert clean["n_docs"] == 1
    assert clean["tier"] == "C"


def test_copa_press_release_pdf_stays_document_not_bodycam():
    candidate = {
        "source": "chicago_copa",
        "case_id": "copa_1",
        "doc_files": [
            {
                "name": "COPA RELEASES VIDEO MATERIALS",
                "url": "https://example.test/release.pdf",
                "ext": "pdf",
                "evidence_type": "bodycam",
            }
        ],
    }

    clean = vb.vet_candidate(candidate)

    assert clean["doc_files"][0]["evidence_type"] == "documents"
    assert clean["n_video"] == 0
    assert clean["n_docs"] == 1
    assert clean["tier"] == "D"


def test_interrogation_audio_counts_as_audio_not_video():
    candidate = {
        "source": "muckrock",
        "case_id": "m_1",
        "media_files": [
            {
                "name": "Interview.wav",
                "url": "https://example.test/interview.wav",
                "ext": "wav",
                "evidence_type": "interrogation",
            }
        ],
        "doc_files": [{"name": "Report.pdf", "url": "https://example.test/report.pdf"}],
    }

    clean = vb.vet_candidate(candidate)

    assert clean["media_files"][0]["kind"] == "audio"
    assert clean["n_audio"] == 1
    assert clean["n_video"] == 0
    assert clean["tier"] == "B"
