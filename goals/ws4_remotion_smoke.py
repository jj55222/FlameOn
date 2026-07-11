#!/usr/bin/env python3
"""WS4 gate: two-case props generation, one local render, zero VO calls."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LANE = ROOT / "pipeline6_remotion"
CASES = {
    "sdpd_12_07_2023_10500_4s_commons_drive": "4s_closed_loop",
    "sdpd_01_05_2025_4400_fanuel_street": "fanuel_w1",
}


def run(command, env):
    result = subprocess.run(command, cwd=ROOT, env=env, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(f"failed ({result.returncode}): {' '.join(map(str, command))}\n{result.stdout}\n{result.stderr}")
    return result


def find_data_root() -> Path:
    if os.environ.get("FLAMEON_DATA_ROOT"):
        return Path(os.environ["FLAMEON_DATA_ROOT"]).expanduser().resolve()
    sibling = ROOT.parent / "FlameOn-main"
    if sibling.is_dir():
        return sibling.resolve()
    if (ROOT / ".tmp/bakeoff/runs").is_dir():
        return ROOT
    raise FileNotFoundError("set FLAMEON_DATA_ROOT to the checkout containing .tmp case data")


def main() -> int:
    data_root = find_data_root()
    env = os.environ.copy()
    env["FLAMEON_DATA_ROOT"] = str(data_root)
    env.pop("ELEVENLABS_API_KEY", None)
    env.pop("ENABLE_ELEVENLABS", None)
    proof_modules = data_root / ".tmp/remotion_iona/node_modules"
    if "REMOTION_NODE_MODULES" not in env and proof_modules.is_dir():
        env["REMOTION_NODE_MODULES"] = str(proof_modules)

    reports = {}
    for case_id, run_name in CASES.items():
        run_dir = data_root / ".tmp/bakeoff/runs" / run_name
        run([str(LANE / "render_cut.sh"), case_id, str(run_dir), "--props-only"], env)
        props_path = run_dir / "remotion" / f"{case_id}_props.json"
        props = json.loads(props_path.read_text(encoding="utf-8"))
        assert props["caseId"] == case_id
        assert props["events"] and props["totalSec"] > 60
        assert props["beats"]
        assert props["audio"]["voEnabled"] is False
        assert isinstance(props.get("degradations"), list)
        media_events = [event for event in props["events"] if event["type"] in {"clip", "audio"}]
        assert media_events
        assert any(event.get("captions") for event in media_events)
        assert any(event.get("lowerThird") for event in media_events)
        assert all("segments/" not in event.get("file", "") for event in media_events)
        reports[case_id] = {"props": str(props_path), "events": len(props["events"]),
                            "media_events": len(media_events), "degradations": len(props["degradations"])}

    render_case = next(iter(CASES))
    render_dir = data_root / ".tmp/bakeoff/runs" / CASES[render_case]
    run([str(LANE / "render_cut.sh"), render_case, str(render_dir),
         "--smoke-seconds", "60", "--low-res"], env)
    output = render_dir / "remotion" / f"{render_case}_remotion_smoke.mp4"
    assert output.exists() and output.stat().st_size > 1_000_000
    probe = run(["/opt/homebrew/bin/ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=nw=1:nk=1", str(output)], env)
    duration = float(probe.stdout.strip())
    assert 59 <= duration <= 61
    status = json.loads((render_dir / "remotion/vo_stage_status.json").read_text(encoding="utf-8"))
    assert status["enabled"] is False and status["network_calls"] == 0
    assert "elevenlabs" not in (render_dir / "remotion/render.log").read_text(encoding="utf-8").lower()

    report = {"schema_version": "ws4.remotion-smoke.v1", "passed": True,
              "props_cases": reports, "render": str(output), "duration_sec": duration,
              "elevenlabs_calls": 0}
    report_path = render_dir / "remotion/ws4_remotion_smoke.json"
    report_path.write_text(json.dumps(report, indent=1), encoding="utf-8")
    print(json.dumps(report, indent=1))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"WS4 REMOTION SMOKE FAIL: {exc}", file=sys.stderr)
        raise
