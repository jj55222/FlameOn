#!/usr/bin/env bash
set -e
export PYTHONIOENCODING=utf-8
cd "$(dirname "$0")/.."

OUT=pipeline3_audio/transcripts
CACHE=pipeline3_audio/foia_cache

run() {
    local label="$1"; local file="$2"; local etype="$3"
    if [[ -s "$OUT/${label}_transcript.json" ]]; then
        local segs=$(python -c "import json; d=json.load(open('$OUT/${label}_transcript.json',encoding='utf-8')); print(len(d.get('segments',[])))")
        if [[ "$segs" -gt "5" ]]; then
            echo "[SKIP] $label (already has $segs segments)"
            return 0
        fi
    fi
    echo "[RUN ] $label ($etype)"
    python pipeline3_audio/pipeline3_transcribe.py \
        --audio-file "$file" \
        --case-id "$label" \
        --evidence-type "$etype" \
        --backend groq \
        --output "$OUT/" 2>&1 | tail -8
}

# SF DPA interrogations
run sfdpa_0040-15_edwards  "$CACHE/0040-15 DPA Interview of Sgt. Scott Edwards #541.mp3" interrogation
run sfdpa_0045-19_pai      "$CACHE/sfdpa_0045-19_dpa_pai.mp3"       interrogation
run sfdpa_0045-19_hernandez "$CACHE/sfdpa_0045-19_dpa_hernandez.mp3" interrogation
run sfdpa_0204-18_reininger "$CACHE/sfdpa_0204-18_iad_reininger.mp3" interrogation
run sfdpa_44321-20_rabsatt "$CACHE/sfdpa_44321-20_dpa_rabsatt.mp3"  interrogation

# Santa Ana CIBs (bodycam narration)
run sapd_25-01082 "$CACHE/sapd_25-01082.webm" bodycam
run sapd_26-02155 "$CACHE/sapd_26-02155.webm" bodycam
run sapd_26-00037 "$CACHE/sapd_26-00037.m4a"  bodycam

echo "--- DONE ---"
