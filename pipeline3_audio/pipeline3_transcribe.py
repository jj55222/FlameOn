"""
pipeline3_transcribe.py — Pipeline 3: Audio Preprocessing + Transcription
==========================================================================
Takes raw audio/video (from a P2 case JSON, URL, or local file) and produces
a clean timestamped transcript matching the p3_to_p4_transcript schema.

Processing chain:
  1. Source acquisition (yt-dlp / requests / local)
  2. Audio analysis (ffprobe diagnostics)
  3. Silence detection (ffmpeg silencedetect)
  4. Silence trimming + offset map (ORIGINAL timestamps preserved)
  5. Dynamic range compression (ffmpeg compand)
  6. Loudness normalization (ffmpeg loudnorm, two-pass)
  7. Transcription (faster-whisper, medium model, CPU int8)
  8. Timestamp remapping (trimmed → original)
  9. Output JSON matching p3_to_p4_transcript schema

CLI:
    python pipeline3_transcribe.py --url "https://vimeo.com/..." --case-id test_case --output transcripts/
    python pipeline3_transcribe.py --audio-file raw.wav --case-id test --output transcripts/
    python pipeline3_transcribe.py --case-json cases/smith.json --output transcripts/
    python pipeline3_transcribe.py --url "..." --dry-run
    python pipeline3_transcribe.py --url "..." --whisper-model small --output transcripts/
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# Register static-ffmpeg binaries on PATH so ffmpeg/ffprobe are callable
try:
    import static_ffmpeg
    static_ffmpeg.add_paths()
except ImportError:
    pass

SCRIPT_DIR = Path(__file__).parent
DEFAULT_WHISPER_MODEL = "medium"
DEFAULT_SILENCE_THRESHOLD_DB = -40
DEFAULT_MIN_SILENCE_SEC = 2.0
DEFAULT_LOUDNESS_TARGET = -16.0
TARGET_SAMPLE_RATE = 16000   # Whisper's native rate

FFMPEG = shutil.which("ffmpeg") or "ffmpeg"
FFPROBE = shutil.which("ffprobe") or "ffprobe"


# ──────────────────────────────────────────────────────────────
# 1. Source acquisition
# ──────────────────────────────────────────────────────────────

def acquire_audio(source, work_dir, case_id):
    """
    Download / copy source to a WAV file at 16kHz mono in work_dir.
    Returns path to the WAV file.
    """
    out_path = os.path.join(work_dir, f"{case_id}_original.wav")

    # Local file
    if os.path.isfile(source):
        print(f"  [acquire] Converting local file to 16kHz mono WAV...")
        _ffmpeg_convert_to_wav(source, out_path)
        return out_path

    # URL — use yt-dlp for streaming sites, requests for direct
    if source.startswith(("http://", "https://")):
        if any(domain in source for domain in ("youtube.com", "youtu.be", "vimeo.com",
                                                "dailymotion.com", "twitch.tv", "facebook.com")):
            print(f"  [acquire] Downloading via yt-dlp...")
            return _acquire_via_ytdlp(source, out_path, work_dir)
        else:
            print(f"  [acquire] Downloading direct file...")
            return _acquire_via_requests(source, out_path, work_dir)

    raise ValueError(f"Unrecognized source: {source}")


def _acquire_via_ytdlp(url, out_path, work_dir):
    """Use yt-dlp to download best audio, then convert to 16kHz mono WAV."""
    import yt_dlp
    temp_template = os.path.join(work_dir, "yt_%(id)s.%(ext)s")
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": temp_template,
        "quiet": True,
        "no_warnings": True,
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "wav",
            "preferredquality": "0",
        }],
        "postprocessor_args": {
            "FFmpegExtractAudio": ["-ar", str(TARGET_SAMPLE_RATE), "-ac", "1"],
        },
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        vid_id = info.get("id", "unknown")
    # yt-dlp outputs <work_dir>/yt_<id>.wav
    downloaded = os.path.join(work_dir, f"yt_{vid_id}.wav")
    if os.path.exists(downloaded):
        shutil.move(downloaded, out_path)
    return out_path


def _acquire_via_requests(url, out_path, work_dir):
    """Download via requests, then convert to 16kHz mono WAV."""
    import requests
    tmp = os.path.join(work_dir, "download.bin")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    _ffmpeg_convert_to_wav(tmp, out_path)
    os.remove(tmp)
    return out_path


def _ffmpeg_convert_to_wav(src, dst):
    """Convert any audio/video file to 16kHz mono WAV."""
    cmd = [
        FFMPEG, "-y", "-i", src,
        "-ar", str(TARGET_SAMPLE_RATE),
        "-ac", "1",
        "-vn",  # no video
        dst,
    ]
    subprocess.run(cmd, capture_output=True, check=True)


# ──────────────────────────────────────────────────────────────
# 2. Audio analysis (ffprobe)
# ──────────────────────────────────────────────────────────────

def analyze_audio(wav_path):
    """Return dict with duration, sample_rate, channels, bitrate."""
    cmd = [
        FFPROBE, "-v", "error",
        "-show_entries", "stream=sample_rate,channels,codec_name,bit_rate",
        "-show_entries", "format=duration,size",
        "-of", "json",
        wav_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(result.stdout)
    stream = data.get("streams", [{}])[0]
    fmt = data.get("format", {})
    return {
        "duration_sec": float(fmt.get("duration", 0)),
        "sample_rate": int(stream.get("sample_rate", 0)),
        "channels": int(stream.get("channels", 0)),
        "codec": stream.get("codec_name", ""),
        "size_bytes": int(fmt.get("size", 0)),
    }


# ──────────────────────────────────────────────────────────────
# 3. Silence detection
# ──────────────────────────────────────────────────────────────

def detect_silence(wav_path, threshold_db=DEFAULT_SILENCE_THRESHOLD_DB,
                   min_silence_sec=DEFAULT_MIN_SILENCE_SEC):
    """
    Run ffmpeg silencedetect and parse stderr to extract silence spans.
    Returns list of (start_sec, end_sec) tuples in ORIGINAL audio timeline.
    """
    cmd = [
        FFMPEG, "-i", wav_path,
        "-af", f"silencedetect=noise={threshold_db}dB:d={min_silence_sec}",
        "-f", "null", "-",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    stderr = result.stderr

    silence_starts = [float(m.group(1)) for m in re.finditer(r'silence_start:\s*([\d.]+)', stderr)]
    silence_ends = [float(m.group(1)) for m in re.finditer(r'silence_end:\s*([\d.]+)', stderr)]

    spans = []
    for start, end in zip(silence_starts, silence_ends):
        spans.append({
            "original_start_sec": round(start, 3),
            "original_end_sec": round(end, 3),
            "duration_sec": round(end - start, 3),
        })
    return spans


# ──────────────────────────────────────────────────────────────
# 4. Silence trimming + offset map
# ──────────────────────────────────────────────────────────────

def build_offset_map(silence_spans, original_duration_sec):
    """
    Build a list of (trimmed_sec, original_sec) pairs representing the mapping
    from trimmed audio timeline back to original timeline. Also returns the
    list of KEEP segments (non-silent spans) as (start, end) in original time.

    Example: if silence from 10-15 is trimmed, then:
      trimmed sec 10 == original sec 10 (before trim)
      trimmed sec 10.01 == original sec 15.01 (after trim, skipped silence)
    """
    silence_spans = sorted(silence_spans, key=lambda s: s["original_start_sec"])
    keep_segments = []
    cursor = 0.0
    for sp in silence_spans:
        start = sp["original_start_sec"]
        end = sp["original_end_sec"]
        if start > cursor:
            keep_segments.append((cursor, start))
        cursor = max(cursor, end)
    if cursor < original_duration_sec:
        keep_segments.append((cursor, original_duration_sec))

    # Build (trimmed, original) map: trimmed_start_of_segment → original_start_of_segment
    offset_map = []
    trimmed_cursor = 0.0
    for orig_start, orig_end in keep_segments:
        offset_map.append((round(trimmed_cursor, 3), round(orig_start, 3)))
        trimmed_cursor += (orig_end - orig_start)

    return offset_map, keep_segments, round(trimmed_cursor, 3)


def trimmed_to_original(trimmed_sec, offset_map, keep_segments):
    """
    Convert a trimmed-timeline timestamp back to original-timeline timestamp.
    Walks the offset_map to find which kept segment contains this trimmed time.
    """
    # Find the last offset entry where trimmed_cursor <= trimmed_sec
    result_orig = 0.0
    for i, (trimmed_cursor, orig_cursor) in enumerate(offset_map):
        if trimmed_sec >= trimmed_cursor:
            # Offset within this kept segment
            delta = trimmed_sec - trimmed_cursor
            result_orig = orig_cursor + delta
            # Check we don't exceed the segment's end
            if i < len(keep_segments):
                seg_start, seg_end = keep_segments[i]
                if result_orig > seg_end:
                    # Spill over — this shouldn't happen for correct inputs
                    # but clamp to segment end
                    result_orig = seg_end
        else:
            break
    return round(result_orig, 3)


def trim_silence(wav_path, keep_segments, output_path):
    """Create a new WAV containing only the keep segments concatenated."""
    if not keep_segments:
        shutil.copy(wav_path, output_path)
        return

    # Build ffmpeg filter: trim each segment, concatenate
    filters = []
    for i, (start, end) in enumerate(keep_segments):
        filters.append(f"[0:a]atrim=start={start}:end={end},asetpts=PTS-STARTPTS[a{i}]")
    concat_inputs = "".join(f"[a{i}]" for i in range(len(keep_segments)))
    filter_complex = ";".join(filters) + f";{concat_inputs}concat=n={len(keep_segments)}:v=0:a=1[out]"

    cmd = [
        FFMPEG, "-y", "-i", wav_path,
        "-filter_complex", filter_complex,
        "-map", "[out]",
        "-ar", str(TARGET_SAMPLE_RATE), "-ac", "1",
        output_path,
    ]
    subprocess.run(cmd, capture_output=True, check=True)


# ──────────────────────────────────────────────────────────────
# 5. Dynamic range compression
# ──────────────────────────────────────────────────────────────

def compress_audio(wav_path, output_path):
    """Apply conservative dynamic range compression to normalize loud/quiet."""
    # compand: attack,release | transfer-fn | gain | initial-volume | delay
    # Conservative setting — don't crush dynamics, just tame extremes
    compand_filter = "compand=attacks=0.3:decays=0.8:points=-80/-80|-45/-15|-27/-9|0/-7|20/-7:soft-knee=6:gain=0"
    cmd = [
        FFMPEG, "-y", "-i", wav_path,
        "-af", compand_filter,
        "-ar", str(TARGET_SAMPLE_RATE), "-ac", "1",
        output_path,
    ]
    subprocess.run(cmd, capture_output=True, check=True)


# ──────────────────────────────────────────────────────────────
# 6. Loudness normalization (two-pass loudnorm)
# ──────────────────────────────────────────────────────────────

def normalize_loudness(wav_path, output_path, target_lufs=DEFAULT_LOUDNESS_TARGET):
    """Two-pass ffmpeg loudnorm. Target -16 LUFS for speech."""
    # Pass 1: measure
    cmd1 = [
        FFMPEG, "-i", wav_path,
        "-af", f"loudnorm=I={target_lufs}:TP=-1.5:LRA=11:print_format=json",
        "-f", "null", "-",
    ]
    result = subprocess.run(cmd1, capture_output=True, text=True)
    # Parse JSON from stderr (last {...} block)
    match = re.search(r'\{[^{}]*"input_i"[^{}]*\}', result.stderr, re.DOTALL)
    measured = {}
    if match:
        try:
            measured = json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    # Pass 2: normalize with measured values
    if measured:
        af = (f"loudnorm=I={target_lufs}:TP=-1.5:LRA=11"
              f":measured_I={measured.get('input_i', '-23.0')}"
              f":measured_LRA={measured.get('input_lra', '7.0')}"
              f":measured_TP={measured.get('input_tp', '-2.0')}"
              f":measured_thresh={measured.get('input_thresh', '-34.0')}"
              f":offset={measured.get('target_offset', '0.0')}"
              f":linear=true")
    else:
        af = f"loudnorm=I={target_lufs}:TP=-1.5:LRA=11"

    cmd2 = [
        FFMPEG, "-y", "-i", wav_path,
        "-af", af,
        "-ar", str(TARGET_SAMPLE_RATE), "-ac", "1",
        output_path,
    ]
    subprocess.run(cmd2, capture_output=True, check=True)


# ──────────────────────────────────────────────────────────────
# 7. Transcription (faster-whisper)
# ──────────────────────────────────────────────────────────────

def transcribe(wav_path, model_name=DEFAULT_WHISPER_MODEL):
    """
    Transcribe with faster-whisper on CPU (int8). Returns list of segments
    with start/end/text/confidence on the TRIMMED timeline.
    """
    from faster_whisper import WhisperModel
    print(f"  [whisper] Loading model: {model_name} (int8 on CPU)...")
    model = WhisperModel(model_name, device="cpu", compute_type="int8")

    print(f"  [whisper] Transcribing...")
    segments, info = model.transcribe(
        wav_path,
        language="en",
        beam_size=5,
        word_timestamps=True,
        vad_filter=False,  # we already trimmed silence
    )
    segments = list(segments)  # materialize the generator

    results = []
    for seg in segments:
        # Average word-level confidence
        words = getattr(seg, "words", None) or []
        if words:
            confs = [getattr(w, "probability", None) for w in words]
            confs = [c for c in confs if c is not None]
            confidence = sum(confs) / len(confs) if confs else None
        else:
            confidence = None
        results.append({
            "start_sec": round(seg.start, 3),
            "end_sec": round(seg.end, 3),
            "text": seg.text.strip(),
            "confidence": round(confidence, 3) if confidence is not None else None,
        })

    return results, info


# ──────────────────────────────────────────────────────────────
# 8. Main pipeline
# ──────────────────────────────────────────────────────────────

def process_audio(
    source,
    case_id,
    output_dir,
    source_evidence_type="bodycam",
    source_url=None,
    whisper_model=DEFAULT_WHISPER_MODEL,
    silence_threshold_db=DEFAULT_SILENCE_THRESHOLD_DB,
    min_silence_sec=DEFAULT_MIN_SILENCE_SEC,
    loudness_target=DEFAULT_LOUDNESS_TARGET,
    keep_intermediate=False,
    dry_run=False,
):
    """Full Pipeline 3 processing. Returns the output transcript dict."""
    print(f"\n{'='*60}")
    print(f"Pipeline 3: {case_id}")
    print(f"  Source: {source}")
    print(f"  Evidence type: {source_evidence_type}")
    print(f"{'='*60}")

    if dry_run:
        print("  [DRY RUN] Would:")
        print(f"    1. Acquire audio from {source}")
        print(f"    2. Run ffprobe diagnostics")
        print(f"    3. Detect silence ({silence_threshold_db}dB, {min_silence_sec}s min)")
        print(f"    4. Trim silence + build offset map")
        print(f"    5. Dynamic range compression")
        print(f"    6. Loudness normalize to {loudness_target} LUFS")
        print(f"    7. Transcribe with Whisper {whisper_model}")
        print(f"    8. Remap timestamps trimmed→original")
        print(f"    9. Save JSON to {output_dir}/{case_id}_transcript.json")
        return None

    os.makedirs(output_dir, exist_ok=True)
    work_dir = tempfile.mkdtemp(prefix=f"p3_{case_id}_")
    t_start = time.time()

    try:
        # Step 1: acquire
        print("\n  [1/8] Acquiring audio...")
        original_wav = acquire_audio(source, work_dir, case_id)
        print(f"    → {original_wav}")

        # Step 2: analyze
        print("\n  [2/8] Analyzing audio...")
        stats = analyze_audio(original_wav)
        original_duration = stats["duration_sec"]
        print(f"    Duration: {original_duration:.1f}s ({original_duration/60:.1f} min)")
        print(f"    Sample rate: {stats['sample_rate']} Hz, channels: {stats['channels']}")
        print(f"    Size: {stats['size_bytes']/1024/1024:.1f} MB")

        # Step 3: silence detection
        print("\n  [3/8] Detecting silence...")
        silence_spans = detect_silence(original_wav, silence_threshold_db, min_silence_sec)
        total_silence = sum(s["duration_sec"] for s in silence_spans)
        print(f"    Found {len(silence_spans)} silence segments totaling {total_silence:.1f}s")

        # Step 4: build offset map + trim
        print("\n  [4/8] Trimming silence + building offset map...")
        offset_map, keep_segments, trimmed_duration = build_offset_map(silence_spans, original_duration)
        print(f"    Kept {len(keep_segments)} audio segments, new duration: {trimmed_duration:.1f}s")
        trimmed_wav = os.path.join(work_dir, f"{case_id}_trimmed.wav")
        trim_silence(original_wav, keep_segments, trimmed_wav)

        # Step 5: compression
        print("\n  [5/8] Dynamic range compression...")
        compressed_wav = os.path.join(work_dir, f"{case_id}_compressed.wav")
        compress_audio(trimmed_wav, compressed_wav)

        # Step 6: loudness normalization
        print(f"\n  [6/8] Loudness normalization to {loudness_target} LUFS...")
        normalized_wav = os.path.join(work_dir, f"{case_id}_normalized.wav")
        normalize_loudness(compressed_wav, normalized_wav, loudness_target)

        # Step 7: transcribe
        print(f"\n  [7/8] Transcribing with Whisper {whisper_model}...")
        t_whisper = time.time()
        segments_trimmed, info = transcribe(normalized_wav, whisper_model)
        print(f"    Got {len(segments_trimmed)} segments in {time.time()-t_whisper:.1f}s")
        print(f"    Language: {info.language} (probability: {info.language_probability:.2f})")

        # Step 8: remap timestamps to ORIGINAL timeline
        print("\n  [8/8] Remapping timestamps (trimmed → original)...")
        transcript = []
        for seg in segments_trimmed:
            orig_start = trimmed_to_original(seg["start_sec"], offset_map, keep_segments)
            orig_end = trimmed_to_original(seg["end_sec"], offset_map, keep_segments)
            transcript.append({
                "start_sec": orig_start,
                "end_sec": orig_end,
                "text": seg["text"],
                "confidence": seg["confidence"],
            })

        # Build output matching p3_to_p4_transcript schema
        output = {
            "case_id": case_id,
            "source_evidence_type": source_evidence_type,
            "source_url": source_url or source,
            "transcript": transcript,
            "silence_map": silence_spans,
            "original_duration_sec": round(original_duration, 3),
            "processed_duration_sec": trimmed_duration,
            "speaker_count": None,  # Diarization not implemented in v1
            "processing_metadata": {
                "whisper_model": whisper_model,
                "silence_threshold_db": silence_threshold_db,
                "min_silence_duration_sec": min_silence_sec,
                "compression_applied": True,
                "loudness_target_lufs": loudness_target,
                "total_processing_sec": round(time.time() - t_start, 1),
            },
        }

        # Save
        out_path = os.path.join(output_dir, f"{case_id}_transcript.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f"\n  ✓ Saved: {out_path}")
        print(f"  Total time: {time.time()-t_start:.1f}s")

        return output

    finally:
        if not keep_intermediate:
            shutil.rmtree(work_dir, ignore_errors=True)


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pipeline 3: Audio preprocessing + transcription")
    src_group = parser.add_mutually_exclusive_group(required=True)
    src_group.add_argument("--url", help="YouTube/Vimeo/direct URL")
    src_group.add_argument("--audio-file", help="Local audio/video file path")
    src_group.add_argument("--case-json", help="P2 case JSON file")

    parser.add_argument("--case-id", help="Case ID (required for --url and --audio-file)")
    parser.add_argument("--output", default="transcripts", help="Output directory")
    parser.add_argument("--evidence-type", default="bodycam",
                        choices=["bodycam", "interrogation", "court_video", "911_audio", "dash_cam", "news_report", "other"])
    parser.add_argument("--whisper-model", default=DEFAULT_WHISPER_MODEL,
                        choices=["tiny", "base", "small", "medium", "large-v3"])
    parser.add_argument("--silence-threshold", type=float, default=DEFAULT_SILENCE_THRESHOLD_DB,
                        help="Silence detection threshold in dB (default: -40)")
    parser.add_argument("--min-silence", type=float, default=DEFAULT_MIN_SILENCE_SEC,
                        help="Minimum silence duration in seconds (default: 2.0)")
    parser.add_argument("--loudness-target", type=float, default=DEFAULT_LOUDNESS_TARGET,
                        help="Loudness target in LUFS (default: -16)")
    parser.add_argument("--keep-intermediate", action="store_true",
                        help="Keep intermediate WAV files in temp dir")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show processing plan without executing")
    args = parser.parse_args()

    # Resolve source
    if args.case_json:
        with open(args.case_json, "r") as f:
            case = json.load(f)
        case_id = case.get("case_id", "unknown")
        # Pick first downloadable source from the case
        sources = case.get("sources", [])
        videos = [s for s in sources if s.get("format") == "video" and s.get("requires_download")]
        if not videos:
            videos = [s for s in sources if s.get("format") in ("video", "audio")]
        if not videos:
            print("[ERROR] No downloadable video/audio sources in case JSON")
            sys.exit(1)
        source = videos[0]["url"]
        evidence_type = videos[0].get("evidence_type", "other")
        if evidence_type not in ("bodycam", "interrogation", "court_video", "911_audio", "dash_cam", "news_report", "other"):
            evidence_type = "other"
        source_url = source
    else:
        source = args.url or args.audio_file
        case_id = args.case_id
        if not case_id:
            print("[ERROR] --case-id is required with --url or --audio-file")
            sys.exit(1)
        evidence_type = args.evidence_type
        source_url = args.url if args.url else None

    process_audio(
        source=source,
        case_id=case_id,
        output_dir=args.output,
        source_evidence_type=evidence_type,
        source_url=source_url,
        whisper_model=args.whisper_model,
        silence_threshold_db=args.silence_threshold,
        min_silence_sec=args.min_silence,
        loudness_target=args.loudness_target,
        keep_intermediate=args.keep_intermediate,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
