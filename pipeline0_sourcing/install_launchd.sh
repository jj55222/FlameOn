#!/bin/bash
# install_launchd.sh — (re)install the com.flameon.p0 daily launchd agent. Idempotent.
#
#   ./pipeline0_sourcing/install_launchd.sh          # install / reload
#   ./pipeline0_sourcing/install_launchd.sh --uninstall
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LABEL="com.flameon.p0"
SRC="$HERE/$LABEL.plist"
DEST="$HOME/Library/LaunchAgents/$LABEL.plist"

# Always start from a clean slate so re-runs don't error on an already-loaded agent.
launchctl unload "$DEST" 2>/dev/null || true

if [ "${1:-}" = "--uninstall" ]; then
  rm -f "$DEST"
  echo "[p0] uninstalled $LABEL"
  exit 0
fi

mkdir -p "$HOME/Library/LaunchAgents"
cp "$SRC" "$DEST"
launchctl load "$DEST"

echo "[p0] installed and loaded $LABEL (07:30 daily)"
launchctl list | grep flameon.p0 || { echo "[p0] ERROR: agent not listed after load"; exit 1; }
