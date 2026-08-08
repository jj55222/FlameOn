---
name: rawwalk-shortform
description: Produce a short chronological raw-bodycam-replay cut (Path A, "Midwest-Safety raw walk") from a case basket — mostly-raw lightly-edited bodycam in a few long continuous runs, ~7-10 min, no interviews. Use when the case is bodycam-driven (an incident that reads chronologically on tape) rather than doc/investigation-driven. For doc-driven longform narrated cuts, use flagship-longform instead.
---

# Raw-walk short cut (Path A)

The operator's locked short-form style: mostly-raw, lightly-edited, CHRONOLOGICAL bodycam played in
a few LONG continuous runs (stop/contact → escalation+action → aftermath), top-third text narration
(no TTS), burned captions, **no interviews**. Locked reference: `sdpd_01_20_2023_logan_avenue`.

## Prereqs
```
source .venv/bin/activate
eval "$(/opt/homebrew/bin/brew shellenv)"      # brew ffmpeg/ffprobe MUST be on PATH
```

## Recipe
```
# 1. AXON clock OCR → real chronology (matches SDPD one-token cam names like "Officer1BodyCameraVideo")
python pipeline3_audio/timeline_stamp.py --basket .tmp/<cid>/video

# 2. transcribe the full slate (mlx-whisper, local GPU)
python .tmp/_p4_transcribe.py --basket .tmp/<cid> --case-id <cid> --include-video --clean

# 3. build the raw-walk blueprint (auto primary-cam, action peak, 3 continuous windows, cold-open)
python pipeline6_sequence/rawwalk.py \
  --basket .tmp/<cid> --case-id <cid> --agency "San Diego Police Dept." \
  --out .tmp/<cid>/blueprint --narrate

# 4. render — --no-audio-aware IS REQUIRED (see below)
python pipeline6_sequence/render_blueprint.py \
  --blueprint .tmp/<cid>/blueprint/<cid>_rawwalk.json \
  --media-dir .tmp/<cid>/video/Video \
  --transcripts .tmp/<cid>/d2/transcripts \
  --no-audio-aware --out .tmp/<cid>/cuts
# open .tmp/<cid>/cuts/<cid>/<cid>_rough_cut.mp4
```

## What rawwalk.py does automatically
- **Primary camera** = EARLIEST-on-scene bodycam with contact/action cues (the contact officer, NOT
  the later supervisor who just relays "shots fired" on radio).
- **Action peak** from force-ONSET cues ("shots fired", "drop the knife/gun", "taser") — NOT bare
  "shot"/"shooting" (those recur in the medical aftermath and mis-place the peak).
- **Cold-open**: a short CLIMAX TEASER (last command → shots), then the chronological walk replays
  it in full. Operator preference: PREFER opening on the most emotionally intense CITIZEN 911 call
  when one exists; else fall back to the climax teaser.
- **Narration** grounded on the documented OUTCOME (doc_extract → else tier1 verdict pitch), not the
  transcript alone — with a guard that flags compliance-language when the outcome is a shooting/death.

## Non-negotiables
- **`--no-audio-aware` is REQUIRED.** The audio-aware snap drags establishing clips to the LOUDEST
  audio (the gunfire), repeatedly putting the shooting at the front. Off = clips play at their real
  AXON timestamps, in order.
- **Only trust `axon_ocr`-stamped cams for chronology** (distrust metadata-stamped BWCs).
- Captions/narration are PIL→PNG overlays (this ffmpeg has NO libass/drawtext). A hallucination
  filter drops Whisper silence-artifacts ("thank you", "you", "music") so silent tape isn't captioned.

## Known tuning (open)
Post-teaser runs a touch long; narration top-slide lingers the whole clip (wants a timed ~6-8s
overlay); caption font ~30px reads small (bump ~40px); silent drive-up stretches have no fill when
there's no 911 call. See [STATE.md](../../../STATE.md).

Reference: memory `rawwalk-template`; [HANDOFF_LONGFORM_EWU_STYLE.md](../../../HANDOFF_LONGFORM_EWU_STYLE.md).
