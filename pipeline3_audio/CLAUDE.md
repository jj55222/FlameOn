# CLAUDE.md — Pipeline 3: Audio Preprocessing + Transcription

## What this pipeline does

Takes raw audio/video files (from MuckRock CDN, YouTube, agency portals) and
produces clean, timestamped, speaker-labeled transcripts ready for Pipeline 4
narrative analysis.

## What to build

### `pipeline3_transcribe.py` — Main Processing Script

**Input:** A P2 case JSON (matching `p2_to_p3_case` schema) OR a direct audio/video file path.

**Processing chain:**

1. **Source acquisition**
   - If YouTube URL → `yt-dlp` extract audio (best quality, wav preferred)
   - If direct URL → `requests` download (or `wget` for large files)
   - If local file → use directly
   - Extract to working directory with case_id prefix

2. **Audio analysis** (pre-processing diagnostics)
   - `ffprobe` to get duration, sample rate, channels, codec
   - Log original file stats for metadata output

3. **Silence detection + trimming**
   - `ffmpeg silencedetect` with configurable threshold (default: -40dB, 2sec min)
   - Build silence map with ORIGINAL timestamps (critical for Pipeline 4)
   - Trim silence segments, producing cleaned audio
   - Maintain **timestamp offset mapping**: for every position in trimmed audio,
     calculate the corresponding position in original. This is a list of
     (trimmed_sec, original_sec) pairs that Pipeline 4 uses to map moments
     back to original footage.

4. **Dynamic range compression**
   - `ffmpeg compand` to normalize loud/quiet sections
   - Bodycam audio is notoriously inconsistent — yelling vs. whispers
   - Conservative settings: don't destroy dynamics, just make whispers audible

5. **Loudness normalization**
   - `ffmpeg loudnorm` to target -16 LUFS (standard for speech processing)
   - Two-pass: measure first, then normalize

6. **Transcription (WhisperX)**
   - Model: `large-v3` on Colab T4 GPU (~1hr audio in ~10min)
   - WhisperX provides word-level alignment + speaker diarization via pyannote
   - Requires `HF_TOKEN` for pyannote speaker diarization model
   - Output: word-level timestamps aligned to TRIMMED audio

7. **Timestamp remapping**
   - Map all WhisperX timestamps from trimmed → original using the offset mapping
   - This is the critical step. Pipeline 4 moments reference original video
     timestamps so the content creator can find the exact moment in raw footage.

8. **Output**
   - JSON matching `p3_to_p4_transcript` schema in `../schemas/contracts.json`
   - Includes: transcript array, silence map, processing metadata, speaker count

**CLI:**
```bash
# Process from P2 case JSON
python pipeline3_transcribe.py --case-json cases/smith_harris_tx_2023.json --output transcripts/

# Process a direct file
python pipeline3_transcribe.py --audio-file bodycam_raw.wav --case-id test_case --output transcripts/

# Dry run (shows what would be processed, estimates duration)
python pipeline3_transcribe.py --case-json cases/smith.json --dry-run

# Skip diarization (faster, no HF_TOKEN needed)
python pipeline3_transcribe.py --audio-file raw.wav --case-id test --no-diarize --output transcripts/
```

### `batch_transcribe.py` — Batch Processing

Processes all sources from a P2 case (a case may have multiple audio files:
bodycam + interrogation + 911 call).

```bash
python batch_transcribe.py --case-dir cases/ --output transcripts/ --dry-run
```

## Output contract

Must match `p3_to_p4_transcript` in `../schemas/contracts.json`:
- `transcript[]` with `start_sec`, `end_sec`, `text`, `speaker` (original timestamps)
- `silence_map[]` with original start/end
- `original_duration_sec`, `processed_duration_sec`
- `speaker_count`
- `processing_metadata`

## Dependencies

```
yt-dlp
ffmpeg  # system install, not pip
whisperx  # pip install git+https://github.com/m-bain/whisperX.git
torch  # CUDA version for Colab GPU
pyannote.audio  # via whisperx, needs HF_TOKEN
```

## Environment variables

```
HF_TOKEN=...  # Hugging Face token for pyannote diarization model
               # Get from: huggingface.co/settings/tokens
               # Accept license at: huggingface.co/pyannote/speaker-diarization-3.1
```

## Technical notes

- **Timestamp mapping is the hardest part.** Every trimmed silence segment
  shifts all subsequent timestamps. Build the offset map during trimming and
  apply it after transcription. Test this thoroughly — wrong timestamps make
  Pipeline 4 output useless.
- **WhisperX vs base Whisper:** WhisperX adds word-level alignment (via wav2vec2)
  and diarization. Both are critical. Don't use base Whisper.
- **Memory on Colab T4:** large-v3 + diarization can hit 15GB VRAM. If OOM,
  fall back to `medium` model or process in chunks.
- **Silence threshold tuning:** -40dB works for most bodycam. Interrogation
  rooms are quieter — may need -50dB. Make this configurable.

## What success looks like

- Process a real bodycam audio file end-to-end
- Timestamps in output match original video when spot-checked
- Speaker labels distinguish at least officer vs. civilian
- `--dry-run` shows processing plan without executing
- Batch mode handles a multi-source case (bodycam + interrogation)
