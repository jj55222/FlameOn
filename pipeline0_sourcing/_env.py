"""Minimal .env loader (house pattern, mirrors muckrock_harvest._read_env_file).

Reads repo-root .env, strips inline `# comments` (unless the value is quoted) so
the em-dash trailing comments in this project's .env don't poison HTTP headers,
and populates os.environ WITHOUT overwriting anything already set.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict

REPO_ROOT = Path(__file__).resolve().parent.parent


def read_env_file() -> Dict[str, str]:
    out: Dict[str, str] = {}
    env = REPO_ROOT / ".env"
    if not env.exists():
        return out
    for line in env.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key, val = key.strip(), val.strip()
        if val[:1] not in ("'", '"') and "#" in val:
            val = val.split("#", 1)[0].strip()
        out[key] = val.strip('"').strip("'").strip()
    return out


def load_env() -> Dict[str, str]:
    """Populate os.environ from .env (non-destructive) and return the parsed dict."""
    parsed = read_env_file()
    for k, v in parsed.items():
        os.environ.setdefault(k, v)
    return parsed
