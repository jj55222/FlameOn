#!/usr/bin/env bash
# Run the CaseGraph no-live test suite from a stable Python interpreter.
# Bootstraps .venv if missing, installs requirements-dev.txt, then invokes pytest.
#
# Usage:
#   ./scripts/run_casegraph_tests.sh
#   ./scripts/run_casegraph_tests.sh tests/test_casegraph_scoring.py -v
#
# Extra arguments are forwarded to pytest verbatim.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$REPO_ROOT/.venv"

# Pick the platform-appropriate venv python path.
if [[ -x "$VENV_DIR/Scripts/python.exe" ]]; then
    VENV_PY="$VENV_DIR/Scripts/python.exe"
elif [[ -x "$VENV_DIR/bin/python" ]]; then
    VENV_PY="$VENV_DIR/bin/python"
else
    echo "Bootstrapping .venv at $VENV_DIR" >&2
    if command -v py >/dev/null 2>&1; then
        py -3.12 -m venv "$VENV_DIR"
    elif command -v python3.12 >/dev/null 2>&1; then
        python3.12 -m venv "$VENV_DIR"
    elif command -v python3 >/dev/null 2>&1; then
        python3 -m venv "$VENV_DIR"
    else
        echo "No usable python interpreter found (need py, python3.12, or python3)." >&2
        exit 1
    fi
    if [[ -x "$VENV_DIR/Scripts/python.exe" ]]; then
        VENV_PY="$VENV_DIR/Scripts/python.exe"
    else
        VENV_PY="$VENV_DIR/bin/python"
    fi
fi

echo "Syncing requirements-dev.txt" >&2
"$VENV_PY" -m pip install --quiet --upgrade pip
"$VENV_PY" -m pip install --quiet -r "$REPO_ROOT/requirements-dev.txt"

echo "Running tests/ from $REPO_ROOT" >&2
cd "$REPO_ROOT"
exec "$VENV_PY" -m pytest tests/ "$@"
