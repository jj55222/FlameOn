#!/usr/bin/env python3
"""
doc_zoom.py — highlight a phrase in a case PDF and Ken-Burns zoom into it.

The recipe proven on Iona (WS4): PyMuPDF ``search_for`` locates the phrase → draw a
yellow highlight (fill + border) on the page → composite a full-width band of the
page, centered on the highlight, onto a dark 16:9 canvas (the still) → a
SUPERSAMPLED ffmpeg zoompan slowly pushes into the highlight → 1280x720 h264 with a
silent aac track (so it drops straight into the video timeline as an x_doc_* asset).

    python doc_zoom.py --pdf case.pdf --page 18 \
        --highlight "SDPD Sniper fired one round at the" \
        --highlight "suspect causing the suspect to fall" --sec 8 \
        --out doc_p18_sniper.mp4

The crop/rect/zoom MATH is pure and unit-tested (no ffmpeg needed); ffmpeg is only
shelled out for the final encode.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

Rect = Tuple[float, float, float, float]   # (x0, y0, x1, y1)

# Canvas the still is laid out in (also the zoom coordinate space) and the encode.
CANVAS_W, CANVAS_H = 1920, 1080
OUT_W, OUT_H = 1280, 720
FPS = 30
SUPERSAMPLE = 4               # pre-scale before zoompan so the push stays crisp
Z_END = 1.5                   # final zoom factor (1.0 = no zoom)
MARGIN_FRAC = 0.04            # black border kept around the page band
CONTEXT_FRAC = 0.16           # page-height padding above/below the highlight band
HIGHLIGHT_FILL = (255, 226, 88, 90)     # translucent yellow
HIGHLIGHT_LINE = (214, 158, 0, 255)     # solid amber border


# ---------------------------------------------------------------------------
# Geometry (pure — unit-tested with a generated PDF, no ffmpeg)
# ---------------------------------------------------------------------------

def find_highlight_rects(page, phrases: Sequence[str]) -> List[Rect]:
    """search_for every phrase on the page → list of (x0,y0,x1,y1) rects (a phrase
    spanning a line break yields more than one rect). Empty if nothing matches."""
    rects: List[Rect] = []
    for ph in phrases:
        for r in page.search_for(ph):
            rects.append((float(r[0]), float(r[1]), float(r[2]), float(r[3])))
    return rects


def union_rect(rects: Sequence[Rect]) -> Rect:
    if not rects:
        raise ValueError("no highlight rects to union")
    return (min(r[0] for r in rects), min(r[1] for r in rects),
            max(r[2] for r in rects), max(r[3] for r in rects))


def compute_crop(page_rect: Rect, hl: Rect, context_frac: float = CONTEXT_FRAC) -> Rect:
    """A FULL-WIDTH horizontal band of the page, centered on the highlight, padded
    by ``context_frac`` of page height above/below (clamped to the page)."""
    px0, py0, px1, py1 = page_rect
    pad = context_frac * (py1 - py0)
    cy0 = max(py0, hl[1] - pad)
    cy1 = min(py1, hl[3] + pad)
    return (px0, cy0, px1, cy1)


def canvas_layout(crop: Rect, canvas_w: int = CANVAS_W, canvas_h: int = CANVAS_H,
                  margin_frac: float = MARGIN_FRAC) -> Dict[str, float]:
    """Where/how the cropped band sits on the dark canvas: fit to the content width,
    shrink to fit height if needed, then center. Returns scale + destination box."""
    cw, ch = crop[2] - crop[0], crop[3] - crop[1]
    content_w = canvas_w * (1 - 2 * margin_frac)
    scale = content_w / cw
    if ch * scale > canvas_h * (1 - 2 * margin_frac):    # too tall → fit height instead
        scale = canvas_h * (1 - 2 * margin_frac) / ch
    dst_w, dst_h = cw * scale, ch * scale
    return {"scale": scale, "dst_x": (canvas_w - dst_w) / 2,
            "dst_y": (canvas_h - dst_h) / 2, "dst_w": dst_w, "dst_h": dst_h}


def highlight_center_canvas(hl: Rect, crop: Rect, layout: Dict[str, float]) -> Tuple[float, float]:
    """Highlight center in canvas pixels (the zoom target)."""
    hcx, hcy = (hl[0] + hl[2]) / 2, (hl[1] + hl[3]) / 2
    return (layout["dst_x"] + (hcx - crop[0]) * layout["scale"],
            layout["dst_y"] + (hcy - crop[1]) * layout["scale"])


def zoompan_params(cx: float, cy: float, sec: float, *, fps: int = FPS,
                   canvas_w: int = CANVAS_W, canvas_h: int = CANVAS_H,
                   out_w: int = OUT_W, out_h: int = OUT_H, z_end: float = Z_END) -> Dict:
    """ffmpeg zoompan expressions that push from z=1 to ``z_end`` while keeping the
    highlight centered. Uses FRACTIONAL positions (fx,fy) so the expressions are
    independent of the supersample factor; the window is clamped in-bounds."""
    frames = max(1, int(round(sec * fps)))
    fx, fy = cx / canvas_w, cy / canvas_h
    z = f"min(1+{z_end - 1:.4f}*on/{max(1, frames - 1)},{z_end})"
    x = f"max(0,min(iw*{fx:.5f}-iw/zoom/2,iw-iw/zoom))"
    y = f"max(0,min(ih*{fy:.5f}-ih/zoom/2,ih-ih/zoom))"
    return {"fx": fx, "fy": fy, "frames": frames, "z": z, "x": x, "y": y,
            "s": f"{out_w}x{out_h}", "fps": fps}


# ---------------------------------------------------------------------------
# Render (PIL — needs the PDF, no ffmpeg) + encode (ffmpeg)
# ---------------------------------------------------------------------------

def render_still(page, crop: Rect, layout: Dict[str, float], hl_rects: Sequence[Rect],
                 canvas_w: int = CANVAS_W, canvas_h: int = CANVAS_H):
    """Render the page band with the highlight drawn, composited on a black canvas.
    Returns a PIL.Image (canvas_w x canvas_h). fitz + PIL only."""
    import fitz  # noqa: PLC0415  (imported lazily so geometry tests don't need it here)
    from PIL import Image, ImageDraw

    scale = layout["scale"]
    pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
    page_img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples).convert("RGBA")

    overlay = Image.new("RGBA", page_img.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    for r in hl_rects:
        box = [r[0] * scale, r[1] * scale, r[2] * scale, r[3] * scale]
        draw.rectangle(box, fill=HIGHLIGHT_FILL, outline=HIGHLIGHT_LINE, width=max(2, int(scale)))
    page_img = Image.alpha_composite(page_img, overlay)

    band = page_img.crop((int(crop[0] * scale), int(crop[1] * scale),
                          int(crop[2] * scale), int(crop[3] * scale)))
    canvas = Image.new("RGB", (canvas_w, canvas_h), (0, 0, 0))
    canvas.paste(band.convert("RGB"), (int(layout["dst_x"]), int(layout["dst_y"])))
    return canvas


def build_ffmpeg_cmd(still_path: str, zp: Dict, sec: float, out_path: str,
                     supersample: int = SUPERSAMPLE) -> List[str]:
    """Assemble the supersampled-zoompan + silent-aac encode command."""
    vf = (f"[0:v]scale=iw*{supersample}:ih*{supersample},"
          f"zoompan=z='{zp['z']}':x='{zp['x']}':y='{zp['y']}':d={zp['frames']}:"
          f"s={zp['s']}:fps={zp['fps']},format=yuv420p[v]")
    return ["ffmpeg", "-y", "-loop", "1", "-i", str(still_path),
            "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000",
            "-filter_complex", vf, "-map", "[v]", "-map", "1:a",
            "-t", f"{sec:.3f}", "-c:v", "libx264", "-preset", "medium", "-crf", "18",
            "-c:a", "aac", "-shortest", str(out_path)]


def make_doc_zoom(pdf: str, page_no: int, phrases: Sequence[str], out_path: str,
                  sec: float = 8.0, still_path: Optional[str] = None) -> Dict:
    """Full pipeline: locate → still → zoompan encode. Returns a summary dict.
    ``page_no`` is 1-indexed (human page number)."""
    import fitz  # noqa: PLC0415
    doc = fitz.open(pdf)
    if not (1 <= page_no <= doc.page_count):
        raise ValueError(f"page {page_no} out of range (1..{doc.page_count})")
    page = doc[page_no - 1]
    hl = find_highlight_rects(page, phrases)
    if not hl:
        raise ValueError(f"none of {list(phrases)!r} found on page {page_no} of {pdf}")
    page_rect = (float(page.rect.x0), float(page.rect.y0), float(page.rect.x1), float(page.rect.y1))
    union = union_rect(hl)
    crop = compute_crop(page_rect, union)
    layout = canvas_layout(crop)
    cx, cy = highlight_center_canvas(union, crop, layout)
    zp = zoompan_params(cx, cy, sec)

    still = render_still(page, crop, layout, hl)
    still_out = still_path or str(Path(out_path).with_suffix(".png"))
    still.save(still_out)

    cmd = build_ffmpeg_cmd(still_out, zp, sec, out_path)
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {r.stderr[-800:]}")
    return {"pdf": pdf, "page": page_no, "highlights": len(hl), "still": still_out,
            "out": out_path, "sec": sec, "zoom_center": (round(cx, 1), round(cy, 1))}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Highlight a phrase in a PDF and Ken-Burns zoom into it.")
    ap.add_argument("--pdf", required=True, help="case PDF")
    ap.add_argument("--page", required=True, type=int, help="page number (1-indexed)")
    ap.add_argument("--highlight", required=True, action="append",
                    help="verbatim phrase to highlight (repeatable; each is search_for'd)")
    ap.add_argument("--sec", type=float, default=8.0, help="clip length in seconds (default 8)")
    ap.add_argument("--out", required=True, help="output .mp4")
    ap.add_argument("--still", default=None, help="also write the still PNG here (default: <out>.png)")
    args = ap.parse_args(argv)

    info = make_doc_zoom(args.pdf, args.page, args.highlight, args.out, sec=args.sec,
                         still_path=args.still)
    print(f"[doc_zoom] p{info['page']} · {info['highlights']} highlight rect(s) · "
          f"zoom→{info['zoom_center']} · {info['sec']}s")
    print(f"[doc_zoom] still → {info['still']}")
    print(f"[doc_zoom] clip  → {info['out']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
