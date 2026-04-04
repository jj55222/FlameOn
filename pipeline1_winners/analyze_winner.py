"""
analyze_winner.py — Pipeline 1: Single Video Analyzer
=====================================================
Analyzes a YouTube video's narrative structure by pulling its transcript
(via youtube-transcript-api) and sending it to an LLM for structural
extraction. Outputs a winner profile JSON matching p1_winner_profile
in schemas/contracts.json.

No audio download or Whisper needed — uses YouTube's existing captions.

CLI:
    python analyze_winner.py --url "https://youtube.com/watch?v=XXX" --output winners/
    python analyze_winner.py --batch video_urls.txt --output winners/
    python analyze_winner.py --url "..." --dry-run
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────────────────────
# LLM Configuration — OpenRouter via OpenAI-compatible SDK
# ──────────────────────────────────────────────────────────────

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
LLM_MODEL = os.environ.get("P1_LLM_MODEL", "qwen/qwen3-235b-a22b:free")
LLM_BASE_URL = "https://openrouter.ai/api/v1"
LLM_TIMEOUT = 120  # seconds — long transcripts take time

SCRIPT_DIR = Path(__file__).parent
SCHEMA_FILE = SCRIPT_DIR.parent / "schemas" / "contracts.json"


# ──────────────────────────────────────────────────────────────
# YouTube helpers
# ──────────────────────────────────────────────────────────────

def extract_video_id(url):
    """Extract video ID from various YouTube URL formats."""
    if not url:
        return None
    # Handle youtu.be short URLs
    parsed = urlparse(url)
    if parsed.hostname in ("youtu.be",):
        return parsed.path.lstrip("/")
    # Handle youtube.com URLs
    if parsed.hostname in ("www.youtube.com", "youtube.com", "m.youtube.com"):
        if parsed.path == "/watch":
            return parse_qs(parsed.query).get("v", [None])[0]
        if parsed.path.startswith("/embed/") or parsed.path.startswith("/v/"):
            return parsed.path.split("/")[2]
        if parsed.path.startswith("/shorts/"):
            return parsed.path.split("/")[2]
    # Maybe it's already just an ID
    if re.match(r'^[a-zA-Z0-9_-]{11}$', url):
        return url
    return None


def extract_metadata(video_id):
    """Extract video metadata using yt-dlp (no download)."""
    try:
        import yt_dlp
    except ImportError:
        print("[ERROR] yt-dlp not installed. Run: pip install yt-dlp")
        return None

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": False,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            return {
                "video_id": video_id,
                "channel": info.get("channel", "") or info.get("uploader", ""),
                "title": info.get("title", ""),
                "view_count": info.get("view_count", 0) or 0,
                "duration_sec": info.get("duration", 0) or 0,
                "upload_date": info.get("upload_date", ""),
            }
    except Exception as e:
        print(f"[ERROR] Failed to extract metadata for {video_id}: {e}")
        return None


def fetch_transcript(video_id):
    """Fetch YouTube transcript using youtube-transcript-api."""
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
    except ImportError:
        print("[ERROR] youtube-transcript-api not installed. Run: pip install youtube-transcript-api")
        return None

    try:
        api = YouTubeTranscriptApi()
        result = api.fetch(video_id)
        segments = []
        for s in result:
            # Support both dict-like and attribute access (API versions vary)
            start = s["start"] if isinstance(s, dict) else s.start
            dur = s["duration"] if isinstance(s, dict) else s.duration
            text = s["text"] if isinstance(s, dict) else s.text
            segments.append({
                "start_sec": round(start, 2),
                "end_sec": round(start + dur, 2),
                "text": text,
            })
        return segments
    except Exception as e:
        print(f"[ERROR] Failed to fetch transcript for {video_id}: {e}")
        return None


def format_transcript_for_llm(segments, duration_sec):
    """Format transcript segments as timestamped text for LLM analysis."""
    lines = []
    for s in segments:
        start_m, start_s = divmod(int(s["start_sec"]), 60)
        start_h, start_m = divmod(start_m, 60)
        ts = f"{start_h:02d}:{start_m:02d}:{start_s:02d}"
        lines.append(f"[{ts}] {s['text']}")

    header = f"VIDEO DURATION: {duration_sec} seconds ({duration_sec/60:.1f} minutes)\n"
    header += f"TRANSCRIPT SEGMENTS: {len(segments)}\n\n"
    return header + "\n".join(lines)


# ──────────────────────────────────────────────────────────────
# LLM structural analysis
# ──────────────────────────────────────────────────────────────

STRUCTURAL_ANALYSIS_PROMPT = """You are a narrative structure analyst specializing in true crime documentary content on YouTube.

Analyze this video transcript and extract its structural elements as JSON.

VIDEO METADATA:
- Title: {title}
- Channel: {channel}
- Duration: {duration_sec} seconds ({duration_min:.1f} minutes)
- Views: {view_count:,}

TRANSCRIPT:
{transcript_text}

Return a JSON object with EXACTLY this structure:

{{
  "narrative_arc": {{
    "structure_type": "<one of: chronological, cold_open, parallel_timeline, reveal_structure, escalation>",
    "beats": [
      {{
        "beat_type": "<one of: hook, setup, escalation, climax, aftermath, reveal, context, transition>",
        "start_pct": <float 0.0-1.0, position in video as fraction>,
        "end_pct": <float 0.0-1.0>,
        "description": "<brief description of what happens in this beat>"
      }}
    ]
  }},
  "moment_types": {{
    "contradiction": <integer count>,
    "emotional_peak": <integer count>,
    "procedural_violation": <integer count>,
    "reveal": <integer count>,
    "detail_noticed": <integer count>,
    "callback": <integer count>,
    "tension_shift": <integer count>
  }},
  "segment_stats": {{
    "avg_segment_length_sec": <number>,
    "total_segments": <integer>,
    "bodycam_pct": <float 0.0-1.0, fraction of runtime that is bodycam footage>,
    "narration_pct": <float 0.0-1.0, fraction that is narrator voiceover>,
    "interrogation_pct": <float 0.0-1.0, fraction that is interrogation footage>,
    "other_pct": <float 0.0-1.0, fraction that is other content>
  }},
  "artifact_combination": ["<list of artifact types used, from: bodycam, interrogation, 911_audio, court_video, news_clips, documents, narration>"]
}}

DEFINITIONS:
- structure_type: "chronological" = events in order; "cold_open" = starts with dramatic moment then backtracks; "parallel_timeline" = alternates between timelines; "reveal_structure" = builds to a reveal; "escalation" = tension builds throughout
- beat_type: "hook" = opening attention grab; "setup" = context/background; "escalation" = rising tension; "climax" = peak moment; "aftermath" = consequences; "reveal" = new information changes understanding; "context" = factual background; "transition" = connecting segment
- moment_types: "contradiction" = someone says something that conflicts with evidence or another statement; "emotional_peak" = intense emotional moment; "procedural_violation" = law enforcement error or rights issue; "reveal" = new information revealed; "detail_noticed" = narrator/video highlights a small but important detail; "callback" = reference to earlier moment; "tension_shift" = sudden change in tension level

RULES:
- Beats must cover the full video (start_pct of first beat near 0.0, end_pct of last beat near 1.0)
- Beats should not overlap significantly
- segment_stats percentages should sum to approximately 1.0
- Count moments conservatively — only clear, distinct instances
- Be precise with beat positions based on transcript timestamps
- artifact_combination should list ALL artifact types woven into the video

Return ONLY the JSON object, no other text."""


def analyze_with_llm(transcript_text, metadata):
    """Send transcript to LLM for structural analysis."""
    try:
        from openai import OpenAI
    except ImportError:
        print("[ERROR] openai SDK not installed. Run: pip install openai")
        return None

    if not OPENROUTER_API_KEY:
        print("[ERROR] OPENROUTER_API_KEY not set in environment")
        return None

    client = OpenAI(
        api_key=OPENROUTER_API_KEY,
        base_url=LLM_BASE_URL,
    )

    prompt = STRUCTURAL_ANALYSIS_PROMPT.format(
        title=metadata["title"],
        channel=metadata["channel"],
        duration_sec=metadata["duration_sec"],
        duration_min=metadata["duration_sec"] / 60,
        view_count=metadata["view_count"],
        transcript_text=transcript_text,
    )

    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                timeout=LLM_TIMEOUT,
                extra_headers={
                    "HTTP-Referer": "https://github.com/jj55222/FlameOn",
                    "X-Title": "FlameOn Pipeline 1",
                },
            )
            content = response.choices[0].message.content.strip()

            # Strip markdown code fences if present
            content = re.sub(r'^```(?:json)?\s*\n?', '', content)
            content = re.sub(r'\n?```\s*$', '', content)

            # Strip <think>...</think> blocks if present (Qwen thinking)
            content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()

            return json.loads(content)
        except json.JSONDecodeError as e:
            print(f"  [WARN] JSON parse failed (attempt {attempt+1}/3): {e}")
            if attempt < 2:
                time.sleep(5)
        except Exception as e:
            print(f"  [WARN] LLM call failed (attempt {attempt+1}/3): {e}")
            if attempt < 2:
                time.sleep(10)

    return None


# ──────────────────────────────────────────────────────────────
# Validation
# ──────────────────────────────────────────────────────────────

VALID_STRUCTURE_TYPES = {"chronological", "cold_open", "parallel_timeline", "reveal_structure", "escalation"}
VALID_BEAT_TYPES = {"hook", "setup", "escalation", "climax", "aftermath", "reveal", "context", "transition"}
VALID_MOMENT_TYPES = {"contradiction", "emotional_peak", "procedural_violation", "reveal", "detail_noticed", "callback", "tension_shift"}
VALID_ARTIFACTS = {"bodycam", "interrogation", "911_audio", "court_video", "news_clips", "documents", "narration"}


def validate_profile(profile):
    """Validate a winner profile against the p1_winner_profile schema. Returns (ok, errors)."""
    errors = []

    # Required top-level fields
    for field in ("video_id", "channel", "title", "view_count", "duration_sec",
                  "narrative_arc", "moment_types", "segment_stats", "artifact_combination"):
        if field not in profile:
            errors.append(f"Missing required field: {field}")

    # narrative_arc
    arc = profile.get("narrative_arc", {})
    st = arc.get("structure_type", "")
    if st not in VALID_STRUCTURE_TYPES:
        errors.append(f"Invalid structure_type: {st}")
    for i, beat in enumerate(arc.get("beats", [])):
        if beat.get("beat_type", "") not in VALID_BEAT_TYPES:
            errors.append(f"Beat {i}: invalid beat_type '{beat.get('beat_type')}'")
        sp = beat.get("start_pct", -1)
        ep = beat.get("end_pct", -1)
        if not (0 <= sp <= 1) or not (0 <= ep <= 1):
            errors.append(f"Beat {i}: start_pct/end_pct out of range")

    # moment_types
    mt = profile.get("moment_types", {})
    for k, v in mt.items():
        if k not in VALID_MOMENT_TYPES:
            errors.append(f"Invalid moment_type key: {k}")
        if not isinstance(v, int) or v < 0:
            errors.append(f"moment_types.{k} must be non-negative integer, got {v}")

    # segment_stats
    ss = profile.get("segment_stats", {})
    pct_sum = sum(ss.get(k, 0) for k in ("bodycam_pct", "narration_pct", "interrogation_pct", "other_pct"))
    if pct_sum > 0 and abs(pct_sum - 1.0) > 0.15:
        errors.append(f"segment_stats percentages sum to {pct_sum:.2f}, expected ~1.0")

    # artifact_combination
    for a in profile.get("artifact_combination", []):
        if a not in VALID_ARTIFACTS:
            errors.append(f"Invalid artifact: {a}")

    return (len(errors) == 0, errors)


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def analyze_video(url, output_dir, dry_run=False):
    """Analyze a single video and save winner profile."""
    video_id = extract_video_id(url)
    if not video_id:
        print(f"[ERROR] Could not extract video ID from: {url}")
        return None

    print(f"\n{'='*60}")
    print(f"Analyzing: {video_id}")

    # Step 1: Metadata
    print("  [1/4] Extracting metadata...")
    metadata = extract_metadata(video_id)
    if not metadata:
        return None
    print(f"    Title:    {metadata['title']}")
    print(f"    Channel:  {metadata['channel']}")
    print(f"    Views:    {metadata['view_count']:,}")
    print(f"    Duration: {metadata['duration_sec']}s ({metadata['duration_sec']/60:.1f}min)")

    if dry_run:
        print("  [DRY RUN] Would fetch transcript, send to LLM, save profile. Stopping.")
        return metadata

    # Step 2: Transcript
    print("  [2/4] Fetching transcript...")
    segments = fetch_transcript(video_id)
    if not segments:
        print("    [WARN] No transcript available. Skipping.")
        return None
    print(f"    Got {len(segments)} transcript segments")

    # Step 3: LLM Analysis
    print(f"  [3/4] Analyzing structure with {LLM_MODEL}...")
    transcript_text = format_transcript_for_llm(segments, metadata["duration_sec"])
    analysis = analyze_with_llm(transcript_text, metadata)
    if not analysis:
        print("    [ERROR] LLM analysis failed after retries")
        return None

    # Merge metadata + analysis into full profile
    profile = {
        "video_id": metadata["video_id"],
        "channel": metadata["channel"],
        "title": metadata["title"],
        "view_count": metadata["view_count"],
        "duration_sec": metadata["duration_sec"],
        **analysis,
    }

    # Step 4: Validate and save
    print("  [4/4] Validating profile...")
    ok, errors = validate_profile(profile)
    if not ok:
        print(f"    [WARN] Validation errors ({len(errors)}):")
        for e in errors[:5]:
            print(f"      - {e}")
        # Save anyway with a flag
        profile["_validation_errors"] = errors

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, f"{video_id}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(profile, f, indent=2, ensure_ascii=False)
    print(f"  Saved: {out_path}")

    return profile


def main():
    parser = argparse.ArgumentParser(description="Pipeline 1: Analyze winner YouTube videos")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--url", help="YouTube video URL to analyze")
    group.add_argument("--batch", help="Text file with one YouTube URL per line")
    parser.add_argument("--output", default="winners", help="Output directory (default: winners/)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without LLM calls")
    args = parser.parse_args()

    if args.url:
        urls = [args.url]
    else:
        urls = []
        with open(args.batch, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    urls.append(line)
        print(f"Loaded {len(urls)} URLs from {args.batch}")

    results = []
    for url in urls:
        result = analyze_video(url, args.output, dry_run=args.dry_run)
        if result:
            results.append(result)

    print(f"\n{'='*60}")
    print(f"Done. {len(results)}/{len(urls)} videos processed.")
    if not args.dry_run:
        print(f"Profiles saved to: {args.output}/")


if __name__ == "__main__":
    main()
