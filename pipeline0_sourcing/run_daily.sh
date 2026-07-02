#!/bin/bash
# run_daily.sh — one autonomous daily P0 FOIA-sourcing pass. Safe under launchd.
#
# Pure Python, no Claude in the loop. Ingests free news/OSINT, extracts + scores,
# and appends new file-worthy leads to .tmp/p0/foia_queue.md. DRAFTS ONLY — it
# never submits a request; the operator reviews the queue weekly and files by hand.
#
# Invoked by ~/Library/LaunchAgents/com.flameon.p0.plist at 07:30 daily.
# Always exits 0 so a bad news day / dead source never marks the launchd job failed.
set -uo pipefail

# Resolve this script's own dir so launchd's unknown working directory doesn't matter.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE" || exit 0

OUT=".tmp/p0"
mkdir -p "$OUT"
LOG="$OUT/run.log"

# House convention: brew tools on PATH (harmless for P0; future-proofs media steps).
if [ -x /opt/homebrew/bin/brew ]; then
  eval "$(/opt/homebrew/bin/brew shellenv)"
fi

# Repo venv (has requests + the openai SDK for extraction). API keys auto-load from
# ../.env inside extract.py (_env.load_env), so we do not need to source .env here.
if [ -f ../.venv/bin/activate ]; then
  # shellcheck disable=SC1091
  source ../.venv/bin/activate
fi

{
  echo "===== $(date '+%Y-%m-%d %H:%M:%S %z') p0 daily run start ====="
  python sourcing_run.py \
      --since 3 \
      --tier1-only \
      --seen "$OUT/seen.json" \
      --out "$OUT" \
    || echo "[run_daily] sourcing_run.py exited non-zero ($?) — see above"
  echo "===== $(date '+%Y-%m-%d %H:%M:%S %z') p0 daily run end ====="
  echo ""
} >>"$LOG" 2>&1 || true

exit 0
