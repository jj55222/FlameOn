"""
comment_calibration.py — Pipeline 1: Comment Signal Extractor
=============================================================
Extracts audience engagement signals from YouTube comments to
empirically calibrate Pipeline 4 scoring weights.

Two-pass architecture:
  Pass 1: Rules-based noise gate (Python, zero API cost)
  Pass 2: LLM classification (batched, via OpenRouter)

CLI:
    python comment_calibration.py --video-ids video_ids.txt --output calibration/
    python comment_calibration.py --video-id "dQw4w9WgXcQ" --output calibration/
    python comment_calibration.py --video-ids video_ids.txt --dry-run
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
LLM_MODEL = os.environ.get("P1_LLM_MODEL", "qwen/qwen3-235b-a22b:free")
LLM_BASE_URL = "https://openrouter.ai/api/v1"

MAX_COMMENTS_DEFAULT = 500


# ──────────────────────────────────────────────────────────────
# Comment extraction
# ──────────────────────────────────────────────────────────────

def fetch_comments_yt_dlp(video_id, max_comments=500):
    """Fetch comments using yt-dlp (reliable fallback)."""
    try:
        import yt_dlp
    except ImportError:
        return []

    ydl_opts = {
        "getcomments": True,
        "skip_download": True,
        "quiet": True,
        "no_warnings": True,
        "extractor_args": {"youtube": {"max_comments": [str(max_comments)]}},
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            raw = info.get("comments", []) or []
            return [
                {
                    "text": c.get("text", ""),
                    "likes": c.get("like_count", 0) or 0,
                    "author": c.get("author", ""),
                    "is_reply": c.get("parent", "root") != "root",
                }
                for c in raw[:max_comments]
                if c.get("text")
            ]
    except Exception as e:
        print(f"  [WARN] yt-dlp comment fetch failed: {e}")
        return []


def fetch_comments(video_id, max_comments=500):
    """Fetch comments — try youtube-search-python first, fall back to yt-dlp."""
    try:
        from youtubesearchpython import Comments, CommentsSort
        comments_obj = Comments(video_id, CommentsSort.BY_TOP)
        raw = []
        while len(raw) < max_comments and comments_obj.hasMoreComments:
            comments_obj.getNextComments()
            for c in comments_obj.comments.get("result", []):
                raw.append({
                    "text": c.get("content", "") or c.get("text", ""),
                    "likes": c.get("votes", {}).get("simpleText", "0") if isinstance(c.get("votes"), dict) else 0,
                    "author": c.get("author", {}).get("name", "") if isinstance(c.get("author"), dict) else "",
                    "is_reply": False,
                })
                if len(raw) >= max_comments:
                    break
        if raw:
            return raw
    except Exception as e:
        print(f"  [INFO] youtube-search-python failed ({e}), trying yt-dlp...")

    return fetch_comments_yt_dlp(video_id, max_comments)


# ──────────────────────────────────────────────────────────────
# Pass 1: Noise gate (zero API cost)
# ──────────────────────────────────────────────────────────────

POLITICAL_KEYWORDS = {
    "trump", "biden", "liberal", "conservative", "democrat", "republican",
    "election", "maga", "woke", "defund", "blm", "acab", "antifa",
    "leftist", "right wing", "far right", "far left", "socialism",
    "communist", "fascist", "libtard",
}

AGGRESSIVE_KEYWORDS = {
    "idiot", "stupid", "stfu", "shut up", "moron", "clown", "dumb",
    "trash", "loser", "braindead", "brain dead", "retard",
}

SPAM_PHRASES = {
    "check out my", "subscribe to my", "follow me", "link in bio",
    "free v-bucks", "free robux", "giveaway", "promo code",
}

# Emoji-only pattern
EMOJI_PATTERN = re.compile(
    r'^[\U0001F000-\U0001FFFF\U00002600-\U000027BF\U0000FE00-\U0000FE0F'
    r'\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF'
    r'\U00002702-\U000027B0\U0000200D\s]+$'
)

TIMESTAMP_PATTERN = re.compile(r'\b(\d{1,2}:\d{2}(?::\d{2})?)\b')


def noise_gate(comments):
    """
    Apply rules-based filtering. Returns (filtered_comments, stats).
    Target: 60-70% removal.
    """
    stats = {
        "total_input": len(comments),
        "removed_short": 0,
        "removed_emoji": 0,
        "removed_political": 0,
        "removed_aggressive_reply": 0,
        "removed_spam": 0,
        "removed_allcaps": 0,
        "survived": 0,
    }

    filtered = []
    for c in comments:
        text = c.get("text", "")

        # 1. Too short
        if len(text.strip()) < 15:
            stats["removed_short"] += 1
            continue

        # 2. Emoji-only
        if EMOJI_PATTERN.match(text.strip()):
            stats["removed_emoji"] += 1
            continue

        text_lower = text.lower()
        words = set(re.findall(r'\b\w+\b', text_lower))

        # 3. Political soapboxing
        if len(words & POLITICAL_KEYWORDS) >= 2:
            stats["removed_political"] += 1
            continue

        # 4. Aggressive reply chains
        if "@" in text and words & AGGRESSIVE_KEYWORDS:
            stats["removed_aggressive_reply"] += 1
            continue

        # 5. Spam
        if any(sp in text_lower for sp in SPAM_PHRASES) or "http" in text_lower:
            stats["removed_spam"] += 1
            continue

        # 6. All-caps shouting
        alpha_words = [w for w in text.split() if w.isalpha()]
        if len(alpha_words) > 3:
            caps_ratio = sum(1 for w in alpha_words if w.isupper()) / len(alpha_words)
            if caps_ratio > 0.8:
                stats["removed_allcaps"] += 1
                continue

        # Extract timestamps from comment
        timestamps = TIMESTAMP_PATTERN.findall(text)
        c["_timestamps"] = timestamps

        filtered.append(c)

    stats["survived"] = len(filtered)
    stats["removal_pct"] = round((1 - len(filtered) / max(len(comments), 1)) * 100, 1)
    return filtered, stats


# ──────────────────────────────────────────────────────────────
# Pass 2: LLM classification
# ──────────────────────────────────────────────────────────────

COMMENT_CLASSIFY_PROMPT = """You are classifying YouTube comments from true crime documentary videos by what narrative moment the viewer is reacting to.

For each comment, identify the moment type the viewer is responding to. Most comments should be classified as "none" — only classify when there's a clear signal.

MOMENT TYPES:
- "contradiction": viewer noticed something that conflicts with what was said/shown (e.g., "Wait, he said he wasn't there but his car was on camera")
- "emotional_peak": viewer expressing strong emotion about a moment (e.g., "This part had me in tears", "I got chills")
- "procedural_violation": viewer noting law enforcement error or rights issue (e.g., "They didn't read him his rights", "That search was illegal")
- "reveal": viewer reacting to new information (e.g., "I did NOT see that coming", "Plot twist!")
- "detail_noticed": viewer pointing out a small but important detail (e.g., "Did anyone else notice the blood on his shoe?")
- "pacing_note": viewer commenting on the video structure itself (e.g., "This part drags", "The editing here is perfect")
- "none": no clear moment-type signal (general praise, off-topic, etc.)

COMMENTS TO CLASSIFY:
{comments_json}

Return a JSON array with one object per comment, in the same order:
[
  {{"index": 0, "moment_type": "contradiction", "confidence": 0.8}},
  {{"index": 1, "moment_type": "none", "confidence": 0.9}},
  ...
]

RULES:
- Use "none" liberally — most comments are general reactions
- confidence should be 0.0-1.0
- Only classify with confidence >= 0.5; otherwise use "none"
- Return ONLY the JSON array"""


def classify_comments_batch(comments, batch_size=50):
    """Classify comments using LLM in batches. Returns list of classifications."""
    try:
        from openai import OpenAI
    except ImportError:
        print("[ERROR] openai SDK not installed")
        return []

    if not OPENROUTER_API_KEY:
        print("[ERROR] OPENROUTER_API_KEY not set")
        return []

    client = OpenAI(api_key=OPENROUTER_API_KEY, base_url=LLM_BASE_URL)
    all_classifications = []

    for i in range(0, len(comments), batch_size):
        batch = comments[i:i + batch_size]
        batch_texts = [{"index": j, "text": c["text"][:500]} for j, c in enumerate(batch)]

        prompt = COMMENT_CLASSIFY_PROMPT.format(comments_json=json.dumps(batch_texts, ensure_ascii=False))

        for attempt in range(3):
            try:
                response = client.chat.completions.create(
                    model=LLM_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.3,
                    timeout=60,
                    extra_headers={
                        "HTTP-Referer": "https://github.com/jj55222/FlameOn",
                        "X-Title": "FlameOn Pipeline 1 Comments",
                    },
                )
                content = response.choices[0].message.content.strip()
                content = re.sub(r'^```(?:json)?\s*\n?', '', content)
                content = re.sub(r'\n?```\s*$', '', content)
                content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()

                classifications = json.loads(content)
                all_classifications.extend(classifications)
                break
            except json.JSONDecodeError:
                if attempt < 2:
                    time.sleep(5)
            except Exception as e:
                print(f"  [WARN] Batch {i//batch_size} attempt {attempt+1}: {e}")
                if attempt < 2:
                    time.sleep(10)

        # Rate limit: ~4 seconds between batches
        if i + batch_size < len(comments):
            time.sleep(4)
            print(f"    Classified {min(i + batch_size, len(comments))}/{len(comments)} comments...", end="\r")

    return all_classifications


# ──────────────────────────────────────────────────────────────
# Aggregation
# ──────────────────────────────────────────────────────────────

def aggregate_calibration(comments, classifications):
    """Aggregate classifications into calibration summary."""
    moment_counts = {}
    timestamp_comment_count = 0

    for cls in classifications:
        mt = cls.get("moment_type", "none")
        conf = cls.get("confidence", 0)
        if mt != "none" and conf >= 0.5:
            moment_counts[mt] = moment_counts.get(mt, 0) + 1

    # Count comments with timestamps
    for c in comments:
        if c.get("_timestamps"):
            timestamp_comment_count += 1

    total = sum(moment_counts.values()) or 1
    moment_distribution = {k: round(v / total, 4) for k, v in sorted(moment_counts.items(), key=lambda x: -x[1])}

    return {
        "total_comments_analyzed": len(comments),
        "total_classified": len(classifications),
        "moment_counts": moment_counts,
        "moment_distribution": moment_distribution,
        "timestamp_comment_count": timestamp_comment_count,
    }


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def process_video(video_id, output_dir, max_comments=500, dry_run=False):
    """Process comments for a single video."""
    print(f"\n{'='*60}")
    print(f"Comment calibration: {video_id}")

    # Fetch comments
    print(f"  [1/3] Fetching comments (max {max_comments})...")
    comments = fetch_comments(video_id, max_comments)
    if not comments:
        print("    No comments found. Skipping.")
        return None
    print(f"    Fetched {len(comments)} comments")

    # Pass 1: Noise gate
    print("  [2/3] Running noise gate...")
    filtered, stats = noise_gate(comments)
    print(f"    Input: {stats['total_input']}")
    print(f"    Removed: short={stats['removed_short']}, emoji={stats['removed_emoji']}, "
          f"political={stats['removed_political']}, aggressive={stats['removed_aggressive_reply']}, "
          f"spam={stats['removed_spam']}, allcaps={stats['removed_allcaps']}")
    print(f"    Survived: {stats['survived']} ({100 - stats['removal_pct']:.0f}%)")

    if dry_run:
        print("  [DRY RUN] Would classify with LLM. Stopping.")
        return {"video_id": video_id, "noise_gate_stats": stats, "dry_run": True}

    # Pass 2: LLM classification
    print(f"  [3/3] Classifying with {LLM_MODEL}...")
    classifications = classify_comments_batch(filtered)
    print(f"    Classified {len(classifications)} comments")

    # Aggregate
    calibration = aggregate_calibration(filtered, classifications)
    calibration["video_id"] = video_id
    calibration["noise_gate_stats"] = stats

    # Save
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, f"{video_id}_comments.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(calibration, f, indent=2, ensure_ascii=False)
    print(f"  Saved: {out_path}")

    # Print summary
    print(f"  Moment distribution:")
    for mt, pct in calibration["moment_distribution"].items():
        print(f"    {mt:25s}: {pct:.1%}")

    return calibration


def main():
    parser = argparse.ArgumentParser(description="Pipeline 1: Comment calibration")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--video-id", help="Single YouTube video ID")
    group.add_argument("--video-ids", help="Text file with one video ID per line")
    parser.add_argument("--output", default="calibration", help="Output directory")
    parser.add_argument("--max-comments", type=int, default=MAX_COMMENTS_DEFAULT, help="Max comments per video")
    parser.add_argument("--dry-run", action="store_true", help="Run noise gate only, no LLM calls")
    args = parser.parse_args()

    if args.video_id:
        ids = [args.video_id]
    else:
        ids = []
        with open(args.video_ids, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    # Accept full URLs or bare IDs
                    if "youtube.com" in line or "youtu.be" in line:
                        from analyze_winner import extract_video_id
                        vid = extract_video_id(line)
                        if vid:
                            ids.append(vid)
                    else:
                        ids.append(line)
        print(f"Loaded {len(ids)} video IDs from {args.video_ids}")

    results = []
    for vid_id in ids:
        result = process_video(vid_id, args.output, args.max_comments, args.dry_run)
        if result:
            results.append(result)

    print(f"\n{'='*60}")
    print(f"Done. {len(results)}/{len(ids)} videos processed.")


if __name__ == "__main__":
    main()
