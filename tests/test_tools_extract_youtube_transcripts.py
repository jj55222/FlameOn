"""Zero-network tests for ``tools/extract_youtube_transcripts.py``.

The script wraps the youtube_transcripts module with argparse +
safe-path gating + per-video output writing. These tests pin the
operator-facing surface so a refactor cannot silently change
behavior.
"""
from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

from tools import extract_youtube_transcripts as script
from pipeline2_discovery.enrichment.youtube_transcripts import (
    CaptionExtraction,
    YouTubeCaptionExtractor,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "tools" / "extract_youtube_transcripts.py"


# ---- helpers --------------------------------------------------------


def _make_safe_output_dir(tmp_path: Path) -> Path:
    target = REPO_ROOT / ".tmp" / "test_youtube_transcripts" / tmp_path.name
    target.mkdir(parents=True, exist_ok=True)
    return target


def _run(argv, *, extractor=None):
    out = io.StringIO()
    err = io.StringIO()
    code = script.main(argv, stdout=out, stderr=err, extractor=extractor)
    return code, out.getvalue(), err.getvalue()


def _write_input(path: Path, rows: List[Dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows), encoding="utf-8")
    return path


def _row(*, provider="youtube", confidence="high",
         hints=("youtube_metadata",),
         urls=("https://www.youtube.com/watch?v=okcvideo01",),
         titles=("OKCPD Community Incident Briefing",),
         cid="x:1004", query="ryan keyon williams oklahoma city police bodycam"):
    return {
        "candidate_id": cid,
        "task_type": "youtube_query",
        "query": query,
        "status": "completed",
        "provider": provider,
        "result_urls": list(urls),
        "result_titles": list(titles),
        "confidence": confidence,
        "next_actions_hint": list(hints),
    }


# ---- dry-run -------------------------------------------------------


def test_dry_run_does_not_invoke_extractor(tmp_path):
    inp = _write_input(tmp_path / "results.json", [_row()])
    output_dir = _make_safe_output_dir(tmp_path)

    class _BoomExtractor:
        def extract(self, url, *, output_dir):
            raise AssertionError("extractor must not be invoked in dry-run")

    code, out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(output_dir),
        "--json",
    ], extractor=_BoomExtractor())
    assert code == 0
    payload = json.loads(out)
    assert payload["dry_run"] is True
    assert payload["selected_count"] == 1
    assert payload["downloaded_count"] == 0
    assert payload["results"][0]["caption_status"] == "dry_run"


def test_dry_run_writes_summary_when_requested(tmp_path):
    inp = _write_input(tmp_path / "results.json", [_row()])
    output_dir = _make_safe_output_dir(tmp_path)
    summary_out = output_dir / "summary.json"
    code, _out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(output_dir),
        "--summary-out", str(summary_out),
    ])
    assert code == 0
    assert summary_out.exists()
    payload = json.loads(summary_out.read_text(encoding="utf-8"))
    assert payload["dry_run"] is True
    assert payload["selected_count"] == 1


# ---- run-mode (monkeypatched extractor) ----------------------------


class _FakeExtractor:
    """Test-only extractor that simulates a successful caption fetch
    by writing a canned VTT to the per-video output dir."""

    def __init__(self, vtt_text="WEBVTT\n\n00:00:00.000 --> 00:00:02.000\n"
                 "ryan keyon williams oklahoma city police bodycam release\n"):
        self.vtt_text = vtt_text
        self.calls = []

    def extract(self, url, *, output_dir):
        self.calls.append({"url": url, "output_dir": str(output_dir)})
        from pipeline2_discovery.enrichment.youtube_transcripts import (
            _video_id_from_url,
        )
        vid = _video_id_from_url(url)
        video_dir = Path(output_dir) / vid
        video_dir.mkdir(parents=True, exist_ok=True)
        vtt_path = video_dir / f"{vid}.en.vtt"
        vtt_path.write_text(self.vtt_text, encoding="utf-8")
        return CaptionExtraction(
            status="downloaded", video_id=vid,
            vtt_path=str(vtt_path),
            notes=[f"selected_vtt={vtt_path.name}"],
        )


class _NoCaptionsExtractor:
    def extract(self, url, *, output_dir):
        from pipeline2_discovery.enrichment.youtube_transcripts import (
            _video_id_from_url,
        )
        return CaptionExtraction(
            status="no_captions_found",
            video_id=_video_id_from_url(url),
            notes=["no vtt files found"],
        )


class _FailingExtractor:
    def extract(self, url, *, output_dir):
        from pipeline2_discovery.enrichment.youtube_transcripts import (
            _video_id_from_url,
        )
        return CaptionExtraction(
            status="failed",
            video_id=_video_id_from_url(url),
            error="OSError: boom",
        )


def test_run_with_fake_extractor_writes_vtt_txt_metadata(tmp_path):
    inp = _write_input(tmp_path / "results.json", [_row(
        urls=("https://www.youtube.com/watch?v=runvid12345",),
    )])
    output_dir = _make_safe_output_dir(tmp_path)

    code, out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(output_dir),
        "--run",
        "--json",
    ], extractor=_FakeExtractor())
    assert code == 0
    payload = json.loads(out)
    assert payload["dry_run"] is False
    assert payload["selected_count"] == 1
    assert payload["downloaded_count"] == 1

    r = payload["results"][0]
    assert r["caption_status"] == "downloaded"
    # vtt + txt + metadata.json all under <output_dir>/<video_id>/
    vtt = Path(r["caption_path"])
    txt = Path(r["transcript_path"])
    md = Path(r["metadata_path"])
    assert vtt.exists() and vtt.suffix == ".vtt"
    assert txt.exists() and txt.suffix == ".txt"
    assert md.exists() and md.name == "metadata.json"
    assert "ryan keyon williams" in txt.read_text(encoding="utf-8")
    md_data = json.loads(md.read_text(encoding="utf-8"))
    assert md_data["candidate_id"] == "x:1004"


def test_run_high_relevance_when_subject_in_transcript(tmp_path):
    """End-to-end: subject name in transcript + a --tasks-file
    providing context → relevance=high. Without --tasks-file the
    scorer can only see query tokens (no subject_name field) and
    falls back to medium — so a high-confidence relevance assertion
    must come with the tasks file. This mirrors the recommended
    operational path."""
    inp = _write_input(tmp_path / "results.json", [_row(
        urls=("https://www.youtube.com/watch?v=highvid12345",),
        cid="x:1004",
        query="ryan keyon williams oklahoma city police bodycam",
    )])
    tasks_file = tmp_path / "tasks.json"
    tasks_file.write_text(json.dumps({
        "tasks": [{
            "candidate_id": "x:1004",
            "context": {
                "subject_name": "ryan keyon williams",
                "agency": "oklahoma city police department",
                "city": "oklahoma city",
                "state": "OK",
            },
        }],
    }), encoding="utf-8")
    output_dir = _make_safe_output_dir(tmp_path)
    code, out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(output_dir),
        "--tasks-file", str(tasks_file),
        "--run",
        "--json",
    ], extractor=_FakeExtractor())
    payload = json.loads(out)
    r = payload["results"][0]
    assert r["transcript_relevance"] == "high"
    assert payload["relevance_high_count"] == 1


def test_run_medium_relevance_without_tasks_file(tmp_path):
    """Without --tasks-file, even a transcript containing the
    subject name gets medium — the scorer can't distinguish
    subject from agency tokens in the query string alone."""
    inp = _write_input(tmp_path / "results.json", [_row(
        urls=("https://www.youtube.com/watch?v=medvid123456",),
        cid="x:1004",
        query="ryan keyon williams oklahoma city police bodycam",
    )])
    output_dir = _make_safe_output_dir(tmp_path)
    code, out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(output_dir),
        "--run",
        "--json",
    ], extractor=_FakeExtractor())
    payload = json.loads(out)
    r = payload["results"][0]
    # Query-token fallback path lands at medium for matches with artifact
    assert r["transcript_relevance"] == "medium"


def test_run_low_relevance_when_unrelated_transcript(tmp_path):
    """Transcript that doesn't ground the candidate → low."""
    inp = _write_input(tmp_path / "results.json", [_row(
        urls=("https://www.youtube.com/watch?v=lowvid123456",),
        cid="x:9", query="alice smith generic case",
    )])
    output_dir = _make_safe_output_dir(tmp_path)
    code, out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(output_dir),
        "--run",
        "--json",
    ], extractor=_FakeExtractor(
        vtt_text="WEBVTT\n\n00:00:00.000 --> 00:00:02.000\n"
                 "Today's recipe is for a savory pumpkin pie.\n",
    ))
    payload = json.loads(out)
    assert payload["results"][0]["transcript_relevance"] == "low"


def test_run_no_captions_status_propagates(tmp_path):
    inp = _write_input(tmp_path / "results.json", [_row(
        urls=("https://www.youtube.com/watch?v=nocap1234567",),
    )])
    output_dir = _make_safe_output_dir(tmp_path)
    code, out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(output_dir),
        "--run",
        "--json",
    ], extractor=_NoCaptionsExtractor())
    payload = json.loads(out)
    assert payload["no_captions_count"] == 1
    assert payload["results"][0]["caption_status"] == "no_captions_found"
    assert payload["results"][0]["transcript_relevance"] == "unknown"


def test_run_failed_extraction_propagates(tmp_path):
    inp = _write_input(tmp_path / "results.json", [_row(
        urls=("https://www.youtube.com/watch?v=failvid12345",),
    )])
    output_dir = _make_safe_output_dir(tmp_path)
    code, out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(output_dir),
        "--run",
        "--json",
    ], extractor=_FailingExtractor())
    payload = json.loads(out)
    assert payload["failed_count"] == 1
    r = payload["results"][0]
    assert r["caption_status"] == "failed"
    assert r["error"] is not None


# ---- selection through CLI -----------------------------------------


def test_min_confidence_high_skips_lower_rows(tmp_path):
    rows = [
        _row(confidence="low", urls=("https://www.youtube.com/watch?v=lowvid1",)),
        _row(confidence="medium", urls=("https://www.youtube.com/watch?v=medvid2",)),
        _row(confidence="high", urls=("https://www.youtube.com/watch?v=highvid3",)),
    ]
    inp = _write_input(tmp_path / "results.json", rows)
    output_dir = _make_safe_output_dir(tmp_path)
    code, out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(output_dir),
        "--min-confidence", "high",
        "--json",
    ])
    payload = json.loads(out)
    assert payload["selected_count"] == 1


def test_max_videos_caps_selection(tmp_path):
    inp = _write_input(tmp_path / "results.json", [_row(
        urls=tuple(f"https://www.youtube.com/watch?v=cap{i:08d}_x"
                   for i in range(20)),
        titles=tuple(f"t{i}" for i in range(20)),
    )])
    output_dir = _make_safe_output_dir(tmp_path)
    code, out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(output_dir),
        "--max-videos", "3",
        "--json",
    ])
    payload = json.loads(out)
    assert payload["selected_count"] == 3


# ---- input + safe-path errors --------------------------------------


def test_missing_input_returns_exit_2(tmp_path):
    output_dir = _make_safe_output_dir(tmp_path)
    code, _out, err = _run([
        "--input", str(tmp_path / "missing.json"),
        "--output-dir", str(output_dir),
    ])
    assert code == 2
    assert "input not found" in err


def test_malformed_input_returns_exit_2(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("not valid json", encoding="utf-8")
    output_dir = _make_safe_output_dir(tmp_path)
    code, _out, err = _run([
        "--input", str(bad),
        "--output-dir", str(output_dir),
    ])
    assert code == 2
    assert "could not load input" in err


def test_input_must_be_a_list(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"not": "a list"}), encoding="utf-8")
    output_dir = _make_safe_output_dir(tmp_path)
    code, _out, err = _run([
        "--input", str(bad),
        "--output-dir", str(output_dir),
    ])
    assert code == 2
    assert "must be a JSON list" in err


def test_unsafe_output_dir_rejected(tmp_path):
    inp = _write_input(tmp_path / "results.json", [_row()])
    unsafe = REPO_ROOT / "pipeline2_discovery" / "_should_be_rejected_dir"
    code, _out, err = _run([
        "--input", str(inp),
        "--output-dir", str(unsafe),
    ])
    assert code == 2
    assert "safe artifact dirs" in err


def test_unsafe_summary_out_rejected(tmp_path):
    inp = _write_input(tmp_path / "results.json", [_row()])
    output_dir = _make_safe_output_dir(tmp_path)
    unsafe = REPO_ROOT / "pipeline2_discovery" / "_should_be_rejected_summary.json"
    code, _out, err = _run([
        "--input", str(inp),
        "--output-dir", str(output_dir),
        "--summary-out", str(unsafe),
    ])
    assert code == 2
    assert "safe artifact dirs" in err


def test_negative_max_videos_rejected(tmp_path):
    inp = _write_input(tmp_path / "results.json", [_row()])
    output_dir = _make_safe_output_dir(tmp_path)
    code, _out, err = _run([
        "--input", str(inp),
        "--output-dir", str(output_dir),
        "--max-videos", "0",
    ])
    assert code == 2
    assert "max-videos" in err


# ---- subprocess invocation regression guards -----------------------


def _run_subprocess(argv, cwd=REPO_ROOT, env=None):
    return subprocess.run(
        argv, cwd=str(cwd), capture_output=True, text=True, env=env, timeout=30,
    )


def test_direct_script_invocation_help_exits_zero():
    result = _run_subprocess([sys.executable, str(SCRIPT_PATH), "--help"])
    assert result.returncode == 0, (
        f"stderr={result.stderr!r}\nstdout={result.stdout!r}"
    )
    assert "--input" in result.stdout
    assert "--output-dir" in result.stdout
    assert "--min-confidence" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


def test_module_invocation_help_exits_zero():
    result = _run_subprocess(
        [sys.executable, "-m", "tools.extract_youtube_transcripts", "--help"],
    )
    assert result.returncode == 0
    assert "--input" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


# ---- zero-network --------------------------------------------------


def test_default_run_does_not_load_yt_dlp(monkeypatch, tmp_path):
    """Dry-run must never import yt_dlp. Pinned by monkeypatching
    YouTubeCaptionExtractor.extract to raise if accidentally called."""
    from pipeline2_discovery.enrichment import youtube_transcripts as mod

    def boom(self, *args, **kwargs):
        raise AssertionError("extractor must not be invoked in dry-run")

    monkeypatch.setattr(mod.YouTubeCaptionExtractor, "extract", boom)

    inp = _write_input(tmp_path / "results.json", [_row()])
    output_dir = _make_safe_output_dir(tmp_path)
    code, _out, _err = _run([
        "--input", str(inp),
        "--output-dir", str(output_dir),
    ])
    assert code == 0
