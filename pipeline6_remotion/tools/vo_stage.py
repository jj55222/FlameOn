#!/usr/bin/env python3
"""Voice-over gate and local attachment stage.

This lane never contacts ElevenLabs. It records whether a future remote adapter
could be enabled and attaches only pre-generated local files listed in props.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Optional, Sequence


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--props", required=True)
    parser.add_argument("--status", required=True)
    parser.add_argument("--enable", action="store_true")
    args = parser.parse_args(argv)
    props_path = Path(args.props)
    props = json.loads(props_path.read_text(encoding="utf-8"))
    key_present = bool(os.environ.get("ELEVENLABS_API_KEY"))
    explicitly_enabled = args.enable and os.environ.get("ENABLE_ELEVENLABS") == "1"
    enabled = bool(key_present and explicitly_enabled)
    attached = sum(1 for event in props.get("events", []) if event.get("voFile")) if enabled else 0
    props.setdefault("audio", {})["voEnabled"] = bool(enabled and attached)
    props_path.write_text(json.dumps(props, indent=1, ensure_ascii=False), encoding="utf-8")
    status = {
        "schema_version": "flameon.vo-stage.v1", "enabled": props["audio"]["voEnabled"],
        "key_present": key_present, "explicit_enable": explicitly_enabled,
        "local_vo_attached": attached, "network_calls": 0,
        "reason": "local pre-generated VO attached" if attached else (
            "no local VO files; remote generation is intentionally not implemented in the zero-dollar lane"
            if enabled else "disabled by default; requires key plus ENABLE_ELEVENLABS=1 plus --enable"
        ),
    }
    output = Path(args.status)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(status, indent=1), encoding="utf-8")
    print(json.dumps(status, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
