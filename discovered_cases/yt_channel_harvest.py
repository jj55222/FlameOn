"""
Agency critical-incident VIDEO-channel harvester (YouTube) — a VIDEO case source.

Many US agencies publish their AB748 / SB1421 "critical incident" releases as
individual videos on a YouTube channel (LAPD, San José PD, …), each an edited
body-worn-camera / critical-incident-video release for one incident. This harvester
enumerates a channel with ``yt-dlp --flat-playlist`` (bulk + gentle — a handful of
metadata requests, no per-video calls, no downloads), keeps the critical-incident
videos, and emits one candidate per incident in the shared registry schema (same
shape as ``sfdpa_harvest.py`` / ``sdpd_harvest.py``) so they flow straight into
``case_bundle_agg.py``.

VIDEO-URL PRECEDENT: ``copa_harvest.py`` already stores hosted-player URLs
(``player.vimeo.com/video/<id>``) as ``media_files``. We store the YouTube watch
URL the same way (``ext .youtube``, ``mime video/youtube``); downstream fetch is
via yt-dlp, exactly like the Vimeo entries. n_video=1 per bundle (single release).

PRESETS (agency channels, reverse-engineered 2026-07-02):
  lapd_civ  @LAPDHQ         Los Angeles Police Department  (strict "OIS/LERI (NRF##-##)" naming)
  sjpd_civ  @SanJosePolice  San José Police Department

RESUMABLE: the raw channel enumeration is cached under
``.tmp/yt_channel/<source>_flat.json`` (re-runs reuse it unless ``--refresh``); the
output ``<source>_candidates.json`` is merged by ``case_id`` so re-runs never
duplicate and only add new incidents.

Usage:
  python discovered_cases/yt_channel_harvest.py --preset lapd_civ
  python discovered_cases/yt_channel_harvest.py --preset sjpd_civ --max 60
  python discovered_cases/yt_channel_harvest.py --channel https://www.youtube.com/@Foo/videos \
      --source foo_civ --agency "Foo PD" --match "OIS|critical incident"
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent))
from muckrock_harvest import score_text, slugify  # noqa: E402

ROOT = Path(__file__).parent
CACHE_DIR = ROOT.parent / ".tmp" / "yt_channel"

# Agency channel presets. `match` is a case-insensitive title regex selecting the
# critical-incident releases; `case_id_re` (optional) pulls a stable case number
# from the title, else we fall back to the YouTube id.
PRESETS: Dict[str, Dict[str, str]] = {
    "lapd_civ": {
        "channel": "https://www.youtube.com/@LAPDHQ/videos",
        "agency": "Los Angeles Police Department",
        # LAPD names every critical-incident release "[Area] OIS/LERI m/d/yyyy (NRF###-##)".
        "match": r"\bOIS\b|\bLERI\b|NRF\s?\d{2,3}-\d{2}|critical incident",
        "case_id_re": r"(NRF\s?\d{2,3}-\d{2})",
    },
    "sjpd_civ": {
        "channel": "https://www.youtube.com/@SanJosePolice/videos",
        "agency": "San José Police Department",
        "match": r"officer[- ]involved shooting|\bOIS\b|critical incident|in[- ]custody death|use of force",
        "case_id_re": r"\b(\d{2}-\d{3,})\b",
    },
}


def enumerate_channel(url: str, cache: Path, *, refresh: bool = False,
                      verbose: bool = True) -> List[Dict[str, Any]]:
    """Return [{id, title, url, duration}] for every video on the channel, via
    ``yt-dlp --flat-playlist --dump-json`` (one bulk metadata pass). Cached."""
    if cache.exists() and not refresh:
        rows = json.loads(cache.read_text(encoding="utf-8"))
        if verbose:
            print(f"[enum] {len(rows)} videos from cache {cache.name} (--refresh to re-fetch)")
        return rows
    cmd = [sys.executable, "-m", "yt_dlp", "--flat-playlist", "--dump-json",
           "--no-warnings", "--ignore-errors", url]
    if verbose:
        print(f"[enum] yt-dlp flat-playlist {url} …")
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=1200)
    rows: List[Dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
        except ValueError:
            continue
        vid = d.get("id")
        if not vid:
            continue
        rows.append({
            "id": vid,
            "title": (d.get("title") or "").strip(),
            "url": d.get("webpage_url") or d.get("url") or f"https://www.youtube.com/watch?v={vid}",
            "duration": d.get("duration"),
        })
    if not rows and proc.returncode != 0:
        sys.stderr.write((proc.stderr or "")[-800:] + "\n")
        raise SystemExit(f"[enum] yt-dlp returned no videos (exit {proc.returncode}) for {url}")
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    if verbose:
        print(f"[enum] {len(rows)} videos -> cached {cache.name}")
    return rows


def make_candidate(row: Dict[str, Any], *, source: str, agency: str,
                   case_id_re: Optional[str]) -> Dict[str, Any]:
    title = row["title"]
    yt_id = row["id"]
    watch_url = row["url"]
    case_key = ""
    if case_id_re:
        m = re.search(case_id_re, title, re.I)
        if m:
            case_key = slugify(m.group(1), 24)
    case_id = f"{source}_{case_key or yt_id}"

    dur = row.get("duration")
    media = [{
        "name": title,
        "url": watch_url,
        "browse_url": watch_url,
        "ext": ".youtube",
        "mime_type": "video/youtube",
        "kind": "video",
        "evidence_type": "bodycam",   # these releases are edited BWC / critical-incident video
        "youtube_id": yt_id,
        "duration": dur,
    }]
    keyword_score, hits = score_text(title)
    # video bonus mirrors the other harvesters (a lone hosted video is a tier-C bundle)
    score = keyword_score + 6
    return {
        "source": source,
        "case_id": case_id,
        "agency": agency,
        "title": title,
        "case_url": watch_url,
        "n_video": 1, "n_audio": 0, "n_docs": 0, "n_photos": 0, "n_other": 0,
        "has_video": True, "has_mp3": False, "has_pdf": False,
        "is_bwc": True, "is_interrogation": False,
        "artifact_kinds": 1,
        "tier": "C",                  # single hosted video, no docs/audio -> C (see vet_bundles.tier_for)
        "downloadable": None,
        "pivot": {"agency": agency, "case_title": title},
        "titles": [title],
        "media_files": media,
        "doc_files": [],
        "urls": [watch_url],
        "duration_sec": dur,
        "keyword_score": keyword_score,
        "score": score,
        "hits": hits,
        "platform": "youtube",
    }


def load_existing(out: Path) -> Dict[str, Dict[str, Any]]:
    if not out.exists():
        return {}
    try:
        data = json.loads(out.read_text(encoding="utf-8"))
    except ValueError:
        return {}
    items = data if isinstance(data, list) else data.get("candidates", data.get("bundles", []))
    return {c["case_id"]: c for c in items if isinstance(c, dict) and c.get("case_id")}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--preset", choices=sorted(PRESETS), help="agency channel preset")
    ap.add_argument("--channel", help="channel /videos URL (overrides preset)")
    ap.add_argument("--source", help="short stable source id, e.g. foo_civ (overrides preset)")
    ap.add_argument("--agency", help="agency display name (overrides preset)")
    ap.add_argument("--match", help="case-insensitive title regex for critical-incident videos")
    ap.add_argument("--case-id-re", help="regex capturing a stable case number from the title")
    ap.add_argument("--max", type=int, default=0, help="cap kept candidates (0 = all matches)")
    ap.add_argument("--min-duration", type=int, default=60,
                    help="drop videos shorter than this many seconds (skips Shorts/teasers)")
    ap.add_argument("--refresh", action="store_true", help="re-fetch the channel (ignore cache)")
    ap.add_argument("--out", help="output path (default discovered_cases/<source>_candidates.json)")
    args = ap.parse_args()

    preset = PRESETS.get(args.preset or "", {})
    channel = args.channel or preset.get("channel")
    source = args.source or args.preset
    agency = args.agency or preset.get("agency", "")
    match = args.match or preset.get("match")
    case_id_re = args.case_id_re or preset.get("case_id_re")
    if not (channel and source and match):
        ap.error("need --preset, or --channel + --source + --match")

    out = Path(args.out) if args.out else ROOT / f"{source}_candidates.json"
    cache = CACHE_DIR / f"{source}_flat.json"
    match_re = re.compile(match, re.I)

    rows = enumerate_channel(channel, cache, refresh=args.refresh)
    kept_rows, seen_ids = [], set()
    for r in rows:
        if r["id"] in seen_ids:
            continue
        if not match_re.search(r["title"]):
            continue
        dur = r.get("duration")
        if args.min_duration and isinstance(dur, (int, float)) and dur < args.min_duration:
            continue
        seen_ids.add(r["id"])
        kept_rows.append(r)
    if args.max:
        kept_rows = kept_rows[:args.max]

    existing = load_existing(out)
    for r in kept_rows:
        c = make_candidate(r, source=source, agency=agency, case_id_re=case_id_re)
        existing[c["case_id"]] = c            # merge/overwrite by stable case_id (resumable)
    candidates = sorted(existing.values(), key=lambda c: (-(c.get("score") or 0), c["case_id"]))

    out.write_text(json.dumps(candidates, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[{source}] channel={channel}")
    print(f"[{source}] {len(rows)} videos on channel -> {len(kept_rows)} matched "
          f"(min-dur {args.min_duration}s) -> {len(candidates)} total candidates")
    print(f"[{source}] wrote {out.relative_to(ROOT.parent)}")
    for c in candidates[:8]:
        mm = int((c.get('duration_sec') or 0) // 60)
        print(f"   [{c['score']:>3}] {c['case_id']:<28} {mm:>3}min  {c['title'][:56]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
