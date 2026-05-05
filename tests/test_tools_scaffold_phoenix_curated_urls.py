"""Zero-network tests for ``tools/scaffold_phoenix_curated_urls.py``.

Two layers of tests:
  - In-process tests call ``script.main(...)`` directly to exercise
    every code path quickly.
  - Subprocess tests invoke the real Python interpreter to pin both
    invocation forms (``python tools/...py`` and
    ``python -m tools....``) and to catch any future
    ModuleNotFoundError-style regression in the sys.path bootstrap.
"""
from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

import pytest

from tools import scaffold_phoenix_curated_urls as script


PHOENIX_3286 = "https://www.phoenix.gov/newsroom/police-department-news/3286.html"
PHOENIX_3369 = "https://www.phoenix.gov/newsroom/police-department-news/3369.html"
PHOENIX_SLUG = (
    "https://www.phoenix.gov/newsroom/police-department-news/"
    "critical-incident-briefing---may-25--2025---2500-e-cactus-road.html"
)
MUCKROCK_URL = "https://www.muckrock.com/foi/phoenix/12345/"
HTTP_URL = "http://www.phoenix.gov/newsroom/police-department-news/3286.html"
WAPO_URL = "https://www.washingtonpost.com/some-article"

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "tools" / "scaffold_phoenix_curated_urls.py"


def _run(argv):
    out = io.StringIO()
    err = io.StringIO()
    code = script.main(argv, stdout=out, stderr=err)
    return code, out.getvalue(), err.getvalue()


def _write_text(tmp_path, name, body):
    p = tmp_path / name
    p.write_text(body, encoding="utf-8")
    return p


# ---- input-format coverage ------------------------------------------


def test_plain_text_input_one_url_per_line(tmp_path):
    inp = _write_text(
        tmp_path,
        "urls.txt",
        f"# header comment\n{PHOENIX_3286}\n\n{PHOENIX_3369}\n",
    )
    code, out, _err = _run([
        "--input", str(inp),
        "--output", str(tmp_path / "curated.json"),
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["accepted_count"] == 2
    assert payload["rejected_count"] == 0
    accepted_urls = [a["url"] for a in payload["accepted"]]
    assert PHOENIX_3286 in accepted_urls
    assert PHOENIX_3369 in accepted_urls


def test_csv_input_with_url_column(tmp_path):
    inp = _write_text(
        tmp_path,
        "urls.csv",
        f"url,note\n{PHOENIX_3286},baseline\n{PHOENIX_3369},second\n",
    )
    code, out, _err = _run([
        "--input", str(inp),
        "--output", str(tmp_path / "curated.json"),
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["accepted_count"] == 2
    assert payload["format"] == "auto"


def test_tsv_input_with_url_column(tmp_path):
    inp = _write_text(
        tmp_path,
        "urls.tsv",
        f"url\tnote\n{PHOENIX_3286}\tbaseline\n{PHOENIX_3369}\tsecond\n",
    )
    code, out, _err = _run([
        "--input", str(inp),
        "--output", str(tmp_path / "curated.json"),
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["accepted_count"] == 2


def test_json_array_of_strings(tmp_path):
    inp = _write_text(
        tmp_path,
        "urls.json",
        json.dumps([PHOENIX_3286, PHOENIX_3369, PHOENIX_SLUG]),
    )
    code, out, _err = _run([
        "--input", str(inp),
        "--output", str(tmp_path / "curated.json"),
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["accepted_count"] == 3


def test_json_array_of_objects_with_url_field(tmp_path):
    inp = _write_text(
        tmp_path,
        "urls.json",
        json.dumps([{"url": PHOENIX_3286}, {"url": PHOENIX_3369}]),
    )
    code, out, _err = _run([
        "--input", str(inp),
        "--output", str(tmp_path / "curated.json"),
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["accepted_count"] == 2


def test_explicit_format_overrides_extension(tmp_path):
    """A .txt file containing JSON should still parse as JSON when
    --format json is passed explicitly."""
    inp = _write_text(
        tmp_path,
        "urls.txt",
        json.dumps([PHOENIX_3286]),
    )
    code, out, _err = _run([
        "--input", str(inp),
        "--output", str(tmp_path / "curated.json"),
        "--format", "json",
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["accepted_count"] == 1
    assert payload["format"] == "json"


# ---- rejection behavior ---------------------------------------------


def test_rejects_muckrock_news_social_and_non_https(tmp_path):
    inp = _write_text(
        tmp_path,
        "urls.txt",
        "\n".join([
            PHOENIX_3286,
            MUCKROCK_URL,
            WAPO_URL,
            HTTP_URL,
            "https://twitter.com/phoenixpolice",
        ]),
    )
    code, out, _err = _run([
        "--input", str(inp),
        "--output", str(tmp_path / "curated.json"),
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["accepted_count"] == 1
    assert payload["rejected_count"] == 4
    reasons = {r["reason"] for r in payload["rejected"]}
    assert "host_not_in_allowlist" in reasons
    assert "non_https_scheme" in reasons


def test_dedupes_by_normalized_url(tmp_path):
    inp = _write_text(
        tmp_path,
        "urls.txt",
        "\n".join([
            PHOENIX_3369,
            PHOENIX_3369 + "#video",  # normalizes to the same URL
            PHOENIX_3369,
        ]),
    )
    code, out, _err = _run([
        "--input", str(inp),
        "--output", str(tmp_path / "curated.json"),
        "--json",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["accepted_count"] == 1
    assert payload["duplicate_count"] == 2
    assert payload["rejected_count"] == 0


# ---- write-reviewed gating ------------------------------------------


def test_default_dry_run_writes_no_files(tmp_path):
    inp = _write_text(tmp_path, "urls.txt", PHOENIX_3286)
    out_path = tmp_path / "curated.json"
    rej_path = tmp_path / "rejected.json"
    code, out, _err = _run([
        "--input", str(inp),
        "--output", str(out_path),
        "--rejected-output", str(rej_path),
        "--json",
    ])
    assert code == 0
    assert not out_path.exists()
    assert not rej_path.exists()
    payload = json.loads(out)
    assert payload["wrote_files"] is False
    assert payload["accepted_count"] == 1


def test_write_reviewed_flag_writes_curated_json(tmp_path):
    inp = _write_text(
        tmp_path,
        "urls.txt",
        f"{PHOENIX_3286}\n{PHOENIX_3369}",
    )
    out_path = tmp_path / "out" / "curated.json"
    code, _out, _err = _run([
        "--input", str(inp),
        "--output", str(out_path),
        "--write-reviewed",
        "--json",
    ])
    assert code == 0
    assert out_path.exists()
    written = json.loads(out_path.read_text(encoding="utf-8"))
    assert isinstance(written, list)
    assert len(written) == 2
    assert written[0] == {
        "url": PHOENIX_3286,
        "agency": "Phoenix Police Department",
        "jurisdiction": "Phoenix, Maricopa County, Arizona",
        "notes": "",
    }


def test_write_reviewed_flag_writes_rejected_report_when_requested(tmp_path):
    inp = _write_text(
        tmp_path,
        "urls.txt",
        f"{PHOENIX_3286}\n{MUCKROCK_URL}\n{HTTP_URL}",
    )
    out_path = tmp_path / "curated.json"
    rej_path = tmp_path / "rejected.json"
    code, _out, _err = _run([
        "--input", str(inp),
        "--output", str(out_path),
        "--rejected-output", str(rej_path),
        "--write-reviewed",
        "--json",
    ])
    assert code == 0
    assert out_path.exists()
    assert rej_path.exists()
    rej = json.loads(rej_path.read_text(encoding="utf-8"))
    assert rej["rejected_count"] == 2
    assert rej["duplicate_count"] == 0
    reasons = {r["reason"] for r in rej["rejected"]}
    assert "host_not_in_allowlist" in reasons
    assert "non_https_scheme" in reasons


def test_write_reviewed_without_rejected_output_skips_rejected_file(tmp_path):
    inp = _write_text(
        tmp_path,
        "urls.txt",
        f"{PHOENIX_3286}\n{MUCKROCK_URL}",
    )
    out_path = tmp_path / "curated.json"
    rej_path = tmp_path / "rejected.json"
    code, _out, _err = _run([
        "--input", str(inp),
        "--output", str(out_path),
        "--write-reviewed",
    ])
    assert code == 0
    assert out_path.exists()
    assert not rej_path.exists()  # not requested


# ---- error handling -------------------------------------------------


def test_missing_input_returns_clean_error(tmp_path):
    code, _out, err = _run([
        "--input", str(tmp_path / "nope.txt"),
        "--output", str(tmp_path / "out.json"),
    ])
    assert code == 2
    assert "input not found" in err


def test_csv_without_url_column_returns_clean_error(tmp_path):
    inp = _write_text(
        tmp_path,
        "urls.csv",
        "name,description\nfoo,bar\n",
    )
    code, _out, err = _run([
        "--input", str(inp),
        "--output", str(tmp_path / "out.json"),
    ])
    assert code == 2
    assert "url" in err.lower()


def test_json_input_not_array_returns_clean_error(tmp_path):
    inp = _write_text(tmp_path, "urls.json", json.dumps({"not": "array"}))
    code, _out, err = _run([
        "--input", str(inp),
        "--output", str(tmp_path / "out.json"),
    ])
    assert code == 2
    assert "array" in err.lower()


def test_invalid_format_choice_rejected_by_argparse(tmp_path):
    inp = _write_text(tmp_path, "urls.txt", PHOENIX_3286)
    with pytest.raises(SystemExit) as exc_info:
        script.main([
            "--input", str(inp),
            "--output", str(tmp_path / "out.json"),
            "--format", "yaml",
        ])
    assert exc_info.value.code == 2


# ---- subprocess invocation forms ------------------------------------


def _run_subprocess(argv, cwd=REPO_ROOT, env=None):
    return subprocess.run(
        argv,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )


def test_direct_script_invocation_help_exits_zero():
    """``python tools/scaffold_phoenix_curated_urls.py --help`` must
    succeed from the repo root. Mirrors the regression guard from
    PR #28 for the sister generator script."""
    result = _run_subprocess(
        [sys.executable, str(SCRIPT_PATH), "--help"],
    )
    assert result.returncode == 0, (
        f"direct --help failed:\nstdout={result.stdout!r}\n"
        f"stderr={result.stderr!r}"
    )
    assert "--input" in result.stdout
    assert "--write-reviewed" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


def test_module_invocation_help_exits_zero():
    """``python -m tools.scaffold_phoenix_curated_urls --help``."""
    result = _run_subprocess(
        [sys.executable, "-m", "tools.scaffold_phoenix_curated_urls", "--help"],
    )
    assert result.returncode == 0, (
        f"module --help failed:\nstdout={result.stdout!r}\n"
        f"stderr={result.stderr!r}"
    )
    assert "--input" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


def test_direct_script_invocation_dry_run_exits_zero(tmp_path):
    """End-to-end: direct script invocation runs lint, exits 0 in
    dry-run mode, writes no files."""
    inp = _write_text(tmp_path, "urls.txt", PHOENIX_3369)
    out_path = tmp_path / "curated.json"
    result = _run_subprocess([
        sys.executable, str(SCRIPT_PATH),
        "--input", str(inp),
        "--output", str(out_path),
        "--json",
    ])
    assert result.returncode == 0, (
        f"direct dry-run failed:\nstdout={result.stdout!r}\n"
        f"stderr={result.stderr!r}"
    )
    payload = json.loads(result.stdout)
    assert payload["accepted_count"] == 1
    assert payload["wrote_files"] is False
    assert not out_path.exists()


# ---- zero-network ---------------------------------------------------


def test_scaffold_makes_zero_network_calls(monkeypatch, tmp_path):
    import requests

    def fail_get(self, *args, **kwargs):
        raise AssertionError("scaffold must never touch the network")

    monkeypatch.setattr(requests.Session, "get", fail_get)
    inp = _write_text(
        tmp_path,
        "urls.txt",
        "\n".join([PHOENIX_3286, PHOENIX_3369, MUCKROCK_URL, HTTP_URL]),
    )
    code, _out, _err = _run([
        "--input", str(inp),
        "--output", str(tmp_path / "curated.json"),
        "--write-reviewed",
        "--json",
    ])
    assert code == 0


def test_subprocess_invocations_make_zero_real_network_calls(tmp_path):
    """Negative assertions on stderr for network-related errors
    across both invocation forms. Mirrors the analogous guard from
    PR #28."""
    inp = _write_text(
        tmp_path,
        "urls.txt",
        f"{PHOENIX_3286}\n{MUCKROCK_URL}",
    )
    out_path = tmp_path / "curated.json"
    for argv in (
        [sys.executable, str(SCRIPT_PATH)],
        [sys.executable, "-m", "tools.scaffold_phoenix_curated_urls"],
    ):
        result = _run_subprocess(argv + [
            "--input", str(inp),
            "--output", str(out_path),
        ])
        assert result.returncode == 0
        for needle in (
            "ConnectionError", "TimeoutError", "TLS",
            "SSL", "Connection refused", "Name or service",
        ):
            assert needle not in result.stderr
