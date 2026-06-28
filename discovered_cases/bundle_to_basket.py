"""Download a registry bundle into a pipeline BASKET — the glue from
CASE_BUNDLE_AGG.json to make_documentary / timeline_stamp.

Lays files out the way the doc engine expects:
  <basket>/video/Video/<media files>   (BWC / dashcam / interview / audio)
  <basket>/docs/<doc files>            (PDFs)
so ``timeline_stamp --basket <basket>/video`` finds the media and the doc steps
find the PDFs. Browser UA + follow redirects (portals 302 to signed S3), skip
already-downloaded files (resumable), optional per-file size cap so a huge
"Production" PDF doesn't block a first staged run.

    python discovered_cases/bundle_to_basket.py --case-id sfdpa_0409_18 \
        --basket .tmp/sfdpa_0409_18 --media-only
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import requests

ROOT = Path(__file__).parent
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
MEDIA_TYPES = {"bodycam", "dashcam", "surveillance", "interrogation", "audio", "video", "media", "911", "radio"}


def _safe(name: str) -> str:
    name = re.sub(r"\s+", "_", (name or "file").strip())
    return re.sub(r"[^A-Za-z0-9._-]+", "", name) or "file"


def _bundle(registry: Path, case_id: str) -> Dict[str, Any]:
    data = json.loads(registry.read_text(encoding="utf-8"))
    for b in data.get("bundles", []):
        if b.get("case_id") == case_id:
            return b
    sys.exit(f"case_id {case_id!r} not found in {registry}")


def _download(s: requests.Session, url: str, dest: Path, cap_mb: float) -> str:
    if dest.exists() and dest.stat().st_size > 0:
        return f"cached ({dest.stat().st_size/1e6:.0f} MB)"
    # size pre-check (range probe) so a cap can skip giant files
    try:
        h = s.get(url, headers={"Range": "bytes=0-0"}, stream=True, timeout=30, allow_redirects=True)
        cr = h.headers.get("Content-Range", "")
        size = int(cr.split("/")[-1]) if "/" in cr else 0
        h.close()
        if cap_mb and size and size / 1e6 > cap_mb:
            return f"SKIP — {size/1e6:.0f} MB over --max-mb {cap_mb:g}"
    except requests.RequestException:
        size = 0
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        with s.get(url, stream=True, timeout=600, allow_redirects=True) as r:
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(1 << 20):
                    if chunk:
                        f.write(chunk)
        tmp.rename(dest)
        return f"downloaded ({dest.stat().st_size/1e6:.0f} MB)"
    except (requests.RequestException, OSError) as e:
        tmp.unlink(missing_ok=True)
        return f"ERROR {type(e).__name__}: {e}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--registry", type=Path, default=ROOT / "CASE_BUNDLE_AGG.json")
    ap.add_argument("--case-id", required=True)
    ap.add_argument("--basket", required=True, type=Path)
    ap.add_argument("--media-only", action="store_true", help="skip doc/PDF files (defer the big production PDF)")
    ap.add_argument("--max-mb", type=float, default=0.0, help="skip any single file larger than this (MB)")
    args = ap.parse_args()

    b = _bundle(args.registry, args.case_id)
    vdir = args.basket / "video" / "Video"
    ddir = args.basket / "docs"
    s = requests.Session(); s.headers.update({"User-Agent": UA})

    print(f"[basket] {args.case_id} -> {args.basket}  (V{b['n_video']} A{b['n_audio']} D{b['n_docs']})")
    got: List[str] = []
    for f in b.get("files", []):
        is_media = f.get("type") in MEDIA_TYPES
        if not is_media and args.media_only:
            print(f"  - skip (doc, --media-only): {f.get('name','')[:54]}")
            continue
        dest = (vdir if is_media else ddir) / _safe(f.get("name") or "file")
        status = _download(s, f["url"], dest, args.max_mb)
        print(f"  - [{f.get('type'):13s}] {status:28s} {dest.name[:48]}")
        if status.startswith(("downloaded", "cached")):
            got.append(str(dest))
    print(f"[basket] {len(got)} file(s) ready under {vdir} (+ {ddir} for docs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
