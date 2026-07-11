#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <case_id> <run_dir> [--smoke-seconds N] [--low-res] [--props-only]" >&2
  exit 2
fi

CASE_ID=$1
RUN_DIR=$2
shift 2
SMOKE_SECONDS=""
LOW_RES=0
PROPS_ONLY=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --smoke-seconds) SMOKE_SECONDS=${2:?missing smoke duration}; shift 2 ;;
    --low-res) LOW_RES=1; shift ;;
    --props-only) PROPS_ONLY=1; shift ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

eval "$(/opt/homebrew/bin/brew shellenv)"
HERE=$(cd "$(dirname "$0")" && pwd)
PYTHON=${FLAMEON_PYTHON:-python3}
RESOLVER="$HERE/tools/resolve_run_inputs.py"
FIELD() { "$PYTHON" "$RESOLVER" "$CASE_ID" "$RUN_DIR" --field "$1"; }

RUN_DIR=$(FIELD run_dir)
PAPER_EDIT=$(FIELD paper_edit)
CONTRACT=$(FIELD contract)
REPO_ROOT=$(FIELD repo_root)
CASE_ROOT=$(FIELD case_root)
TRANSCRIPTS=$(FIELD transcripts)
OUT_DIR="$RUN_DIR/remotion"
PROPS="$OUT_DIR/${CASE_ID}_props.json"
STATUS="$OUT_DIR/vo_stage_status.json"
mkdir -p "$OUT_DIR"

GEN_ARGS=(
  --paper-edit "$PAPER_EDIT"
  --contract "$CONTRACT"
  --transcripts "$TRANSCRIPTS"
  --case-root "$CASE_ROOT"
  --repo-root "$REPO_ROOT"
  --public-dir "$HERE/public"
  --output "$PROPS"
)
if [[ $PROPS_ONLY -eq 1 ]]; then GEN_ARGS+=(--no-extract); fi
"$PYTHON" "$HERE/props_from_paper_edit.py" "${GEN_ARGS[@]}"
"$PYTHON" "$HERE/tools/vo_stage.py" --props "$PROPS" --status "$STATUS"

if [[ $PROPS_ONLY -eq 1 ]]; then
  echo "props-only -> $PROPS"
  exit 0
fi

CREATED_LINK=0
if [[ ! -e "$HERE/node_modules" && -n "${REMOTION_NODE_MODULES:-}" ]]; then
  ln -s "$REMOTION_NODE_MODULES" "$HERE/node_modules"
  CREATED_LINK=1
  trap 'if [[ $CREATED_LINK -eq 1 ]]; then rm -f "$HERE/node_modules"; fi' EXIT
fi
if [[ -x "$HERE/node_modules/.bin/remotion" ]]; then
  CLI="$HERE/node_modules/.bin/remotion"
else
  echo "node_modules missing; run npm install in $HERE or set REMOTION_NODE_MODULES" >&2
  exit 3
fi
if [[ ! -x "$CLI" ]]; then
  echo "Remotion CLI not executable: $CLI" >&2
  exit 3
fi

OUTPUT="$OUT_DIR/${CASE_ID}_remotion.mp4"
RENDER_ARGS=(render "$HERE/src/index.ts" Cut "$OUTPUT" --props="$PROPS" --codec=h264 --crf=18 --concurrency=2)
if [[ $LOW_RES -eq 1 ]]; then RENDER_ARGS+=(--scale=0.5); fi
if [[ -n "$SMOKE_SECONDS" ]]; then
  END_FRAME=$((SMOKE_SECONDS * 30 - 1))
  RENDER_ARGS+=(--frames="0-${END_FRAME}")
  OUTPUT="$OUT_DIR/${CASE_ID}_remotion_smoke.mp4"
  RENDER_ARGS[3]="$OUTPUT"
fi

(cd "$HERE" && "$CLI" "${RENDER_ARGS[@]}") 2>&1 | tee "$OUT_DIR/render.log"
echo "render -> $OUTPUT"
