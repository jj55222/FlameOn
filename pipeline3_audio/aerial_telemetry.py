"""GOAL_D — the aerial unit is the clock + GPS we thought was dead.

Clock-OCR "failed" on the dashcams because they carry no overlay. But the police
helicopter burns a full telemetry HUD into every frame: a UTC-offset timestamp
(top-left) and, along the bottom, the GEOPOINT / TGT street address the camera is
slewed onto. That gives two things the ground cameras can't:

  * a real WALL-CLOCK to anchor the whole case timeline, and
  * a frame-by-frame GPS TRACK of exactly what the air unit is looking at — so the
    instant its geopoint locks onto the incident location (or the downed person) is
    the moment, located by geography instead of sound.

Pure parsers (HUD signature / address / timestamp) are unit-tested; the OCR scan
(crop the HUD corners, upscale, easyocr) is best-effort and exercised live.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# HUD regions as normalized (x0, y0, x1, y1) — from the 1280x720 air-unit frame.
TS_BOX = (0.0, 0.02, 0.26, 0.14)      # top-left: date / time / UTC offset
GEO_BOX = (0.16, 0.84, 0.84, 1.0)     # bottom band: GEOPOINT / TGT address

# Distinctive HUD tokens a ground camera never shows.
_HUD_SIGNATURE = ("searchlight", "geopoint", "nearest parcels", "los", "offset",
                  "dflt", "tgt", "ete", "geopoimt", "geopoint")

_ADDR_RE = re.compile(
    r"\b(\d{2,5})\s+([A-Za-z]+(?:\s+[A-Za-z]+){0,3}?)\s+"
    r"(AVE|AVENUE|ST|STREET|BLVD|WAY|WY|DR|DRIVE|RD|ROAD|LN|LANE|CT|COURT|PL|PKWY)\b",
    re.IGNORECASE)
# HH:MM[:SS] — OCR mangles the separator to :/./; and may insert spaces.
_TIME_RE = re.compile(r"\b(\d{1,2})\s*[:.;]\s*(\d{2})(?:\s*[:.;]\s*(\d{2}))?\b")
_DATE_RE = re.compile(r"\b(\d{1,2})[-/ .](\d{1,2})[-/ .](20\d{2})\b")
_DATE8_RE = re.compile(r"\b(\d{2})(\d{2})(20\d{2})\b")          # DDMMYYYY mashed by OCR


# ---------------------------------------------------------------------------
# Pure parsers (unit-tested)
# ---------------------------------------------------------------------------

def is_aerial_hud(tokens: List[str], min_hits: int = 2) -> bool:
    """True if the OCR'd tokens carry the air-unit HUD signature (pure)."""
    blob = " ".join(tokens).lower()
    return sum(1 for s in set(_HUD_SIGNATURE) if s in blob) >= min_hits


def parse_address(text: str) -> Optional[str]:
    """Normalize a '<num> <STREET> <SUFFIX>' geopoint, else None (pure)."""
    m = _ADDR_RE.search(text or "")
    if not m:
        return None
    num, street, suf = m.group(1), m.group(2).upper().strip(), m.group(3).upper()
    return f"{num} {street} {suf}"


def extract_addresses(tokens: List[str]) -> List[str]:
    """All distinct addresses across a frame's tokens, order-preserving (pure)."""
    seen, out = set(), []
    for t in tokens:
        a = parse_address(t)
        if a and a not in seen:
            seen.add(a)
            out.append(a)
    return out


def parse_timestamp(text: str) -> Dict[str, Optional[str]]:
    """Best-effort wall-clock from garbled HUD text (pure). Returns
    ``{time, date, utc_offset, raw}`` with whatever could be recovered. Accepts the
    first VALID HH:MM[:SS] (the air unit shows minute resolution), so a random
    number pair like 54:43 is rejected."""
    text = text or ""
    time = None
    for m in _TIME_RE.finditer(text):
        hh, mm = int(m.group(1)), int(m.group(2))
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            time = f"{hh:02d}:{mm:02d}" + (f":{m.group(3)}" if m.group(3) else "")
            break
    d = _DATE_RE.search(text) or _DATE8_RE.search(text)
    utc = re.search(r"utc\s*([+-]\d{1,2})", text, re.IGNORECASE)
    return {
        "time": time,
        "date": "-".join(d.groups()) if d else None,
        "utc_offset": utc.group(1) if utc else None,
        "raw": text.strip()[:80] or None,
    }


# ---------------------------------------------------------------------------
# OCR scan (best-effort, easyocr)
# ---------------------------------------------------------------------------

def _frame(media: str, t: float, out_png: str, ffmpeg: str) -> bool:
    import subprocess
    subprocess.run([ffmpeg, "-y", "-hide_banner", "-loglevel", "error",
                    "-ss", str(t), "-i", media, "-frames:v", "1", out_png],
                   capture_output=True)
    return os.path.exists(out_png)


def _ocr_region(img, reader, box: Tuple[float, float, float, float], upscale: int = 4):
    import numpy as np
    from PIL import Image
    H, W = img.shape[:2]
    x0, y0, x1, y1 = box
    crop = img[int(y0 * H):int(y1 * H), int(x0 * W):int(x1 * W)]
    if crop.size == 0:
        return []
    if upscale > 1:
        crop = np.array(Image.fromarray(crop).resize(
            (crop.shape[1] * upscale, crop.shape[0] * upscale)))
    return reader.readtext(crop, detail=0)


def scan_aerial(media: str, reader=None, samples: int = 6,
                ffmpeg: Optional[str] = None) -> Dict:
    """Detect the air-unit HUD and pull its timestamp + GPS target track.
    Returns ``{is_aerial, frames:[{t,timestamp,targets}], track:[address,...]}``."""
    import subprocess
    import numpy as np
    from PIL import Image
    import pov_triage as pt
    if not pt.FFMPEG:
        pt.FFMPEG, pt.FFPROBE = pt._ff()
    ffmpeg = ffmpeg or pt.FFMPEG
    ffprobe = pt.FFPROBE
    if reader is None:
        import easyocr
        reader = easyocr.Reader(["en"], gpu=False, verbose=False)

    out = subprocess.run([ffprobe, "-v", "error",
                          "-show_entries", "format=duration", "-of",
                          "default=nw=1:nk=1", media], capture_output=True, text=True)
    try:
        dur = float(out.stdout.strip())
    except ValueError:
        dur = 0.0
    tmp = Path(".tmp/_aerial_frames"); tmp.mkdir(parents=True, exist_ok=True)
    frames, track, aerial_votes = [], [], 0
    for k in range(samples):
        t = dur * (k + 0.5) / samples if dur else float(k)
        png = str(tmp / f"a_{abs(hash(media)) % 9999}_{k}.png")
        if not _frame(media, t, png, ffmpeg):
            continue
        try:
            img = np.array(Image.open(png).convert("RGB"))
            full = reader.readtext(img, detail=0)
            if is_aerial_hud(full):
                aerial_votes += 1
            ts_tokens = _ocr_region(img, reader, TS_BOX, upscale=5)
            geo_tokens = _ocr_region(img, reader, GEO_BOX, upscale=3)
            addrs = extract_addresses(geo_tokens + full)
            ts = parse_timestamp(" ".join(ts_tokens))
            frames.append({"t": round(t, 1), "timestamp": ts, "targets": addrs})
            for a in addrs:
                if a not in track:
                    track.append(a)
        except Exception as e:   # noqa: BLE001
            sys.stderr.write(f"  ocr skip ({type(e).__name__}: {str(e)[:60]})\n")
        finally:
            if os.path.exists(png):
                os.remove(png)
    return {"is_aerial": aerial_votes >= max(1, samples // 3),
            "aerial_votes": aerial_votes, "n_samples": samples,
            "frames": frames, "track": track}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="GOAL_D — extract aerial HUD clock + GPS track")
    ap.add_argument("--zip", required=True)
    ap.add_argument("--members", nargs="*", required=True, help="basenames to scan")
    ap.add_argument("--samples", type=int, default=8)
    ap.add_argument("--out", default=".tmp/ois_289964/aerial_telemetry.json")
    args = ap.parse_args(argv)

    import pov_triage as pt
    pt.FFMPEG, pt.FFPROBE = pt._ff()
    import easyocr
    reader = easyocr.Reader(["en"], gpu=False, verbose=False)
    z = zipfile.ZipFile(args.zip)
    by_base = {os.path.basename(i.filename): i.filename for i in z.infolist()}
    tmp = Path(".tmp/_aerial"); tmp.mkdir(parents=True, exist_ok=True)
    results = []
    for base in args.members:
        member = by_base.get(base, base)
        local = tmp / base
        print(f"[aerial] {base} ...")
        with z.open(member) as s, open(local, "wb") as d:
            shutil.copyfileobj(s, d, 1 << 20)
        try:
            r = scan_aerial(str(local), reader=reader, samples=args.samples, ffmpeg=pt.FFMPEG)
            r["member"] = base
            results.append(r)
            print(f"   aerial={r['is_aerial']} ({r['aerial_votes']}/{r['n_samples']})  "
                  f"track={r['track'][:6]}")
            for f in r["frames"]:
                if f["timestamp"]["time"] or f["timestamp"]["raw"]:
                    print(f"     t={f['t']}s  clock={f['timestamp']['time']} "
                          f"raw={f['timestamp']['raw']!r}  tgt={f['targets']}")
        finally:
            if local.exists():
                local.unlink()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(results, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
