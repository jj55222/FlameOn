"""Tests for doc_zoom.py — the highlight-crop-zoom geometry (FIX 6).

The crop / rect / zoom math (and the still compositing) are exercised against a
GENERATED PDF via PyMuPDF; ffmpeg is never invoked here (only build_ffmpeg_cmd is
checked as a string, not run).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import doc_zoom as DZ  # noqa: E402

fitz = pytest.importorskip("fitz")   # PyMuPDF; skip cleanly if unavailable


@pytest.fixture
def pdf_page(tmp_path):
    """A US-Letter page with a known phrase to highlight."""
    doc = fitz.open()
    page = doc.new_page(width=612, height=792)
    page.insert_text((72, 120), "Preamble line one of the report narrative.", fontsize=13)
    page.insert_text((72, 400), "SDPD Sniper fired one round at the suspect.", fontsize=13)
    page.insert_text((72, 700), "Footer line near the bottom of the page.", fontsize=13)
    p = tmp_path / "case.pdf"
    doc.save(str(p))
    return fitz.open(str(p))[0]


# --- search + union ---------------------------------------------------------

def test_find_and_union_highlight(pdf_page):
    rects = DZ.find_highlight_rects(pdf_page, ["Sniper fired one round"])
    assert rects, "phrase not located"
    u = DZ.union_rect(rects)
    assert u[0] < u[2] and u[1] < u[3]
    assert 380 < u[1] < 405            # glyph bbox top sits just above the y=400 baseline


def test_missing_phrase_returns_empty(pdf_page):
    assert DZ.find_highlight_rects(pdf_page, ["this text is not on the page"]) == []


def test_union_rect_empty_raises():
    with pytest.raises(ValueError):
        DZ.union_rect([])


# --- crop band --------------------------------------------------------------

def test_compute_crop_is_full_width_and_contains_highlight(pdf_page):
    page_rect = (0.0, 0.0, 612.0, 792.0)
    hl = DZ.union_rect(DZ.find_highlight_rects(pdf_page, ["Sniper fired one round"]))
    crop = DZ.compute_crop(page_rect, hl)
    assert crop[0] == page_rect[0] and crop[2] == page_rect[2]     # full page width
    assert crop[1] <= hl[1] and crop[3] >= hl[3]                   # contains the highlight
    assert crop[1] >= page_rect[1] and crop[3] <= page_rect[3]     # clamped to the page


def test_crop_clamps_at_page_top(pdf_page):
    page_rect = (0.0, 0.0, 612.0, 792.0)
    hl = DZ.union_rect(DZ.find_highlight_rects(pdf_page, ["Preamble line one"]))
    crop = DZ.compute_crop(page_rect, hl)
    assert crop[1] == 0.0        # highlight near the top → band clamps to y=0


# --- canvas layout ----------------------------------------------------------

def test_canvas_layout_fits_and_centers():
    crop = (0.0, 300.0, 612.0, 520.0)          # 612 x 220 band
    lay = DZ.canvas_layout(crop)
    assert lay["scale"] > 0
    assert lay["dst_x"] >= 0 and lay["dst_x"] + lay["dst_w"] <= DZ.CANVAS_W + 0.5
    assert lay["dst_y"] >= 0 and lay["dst_y"] + lay["dst_h"] <= DZ.CANVAS_H + 0.5
    assert abs(lay["dst_x"] - (DZ.CANVAS_W - lay["dst_w"]) / 2) < 0.5   # centered
    assert abs(lay["dst_y"] - (DZ.CANVAS_H - lay["dst_h"]) / 2) < 0.5


def test_tall_band_is_height_fit_not_overflow():
    crop = (0.0, 0.0, 612.0, 792.0)            # whole page — would overflow on width-fit
    lay = DZ.canvas_layout(crop)
    assert lay["dst_h"] <= DZ.CANVAS_H * (1 - 2 * DZ.MARGIN_FRAC) + 0.5


# --- zoom target ------------------------------------------------------------

def test_highlight_center_inside_canvas(pdf_page):
    page_rect = (0.0, 0.0, 612.0, 792.0)
    hl = DZ.union_rect(DZ.find_highlight_rects(pdf_page, ["Sniper fired one round"]))
    crop = DZ.compute_crop(page_rect, hl)
    lay = DZ.canvas_layout(crop)
    cx, cy = DZ.highlight_center_canvas(hl, crop, lay)
    assert 0 < cx < DZ.CANVAS_W and 0 < cy < DZ.CANVAS_H


def test_zoompan_params_target_and_frames():
    zp = DZ.zoompan_params(960.0, 540.0, sec=8.0)
    assert zp["frames"] == 240                     # 8s * 30fps
    assert abs(zp["fx"] - 0.5) < 1e-6 and abs(zp["fy"] - 0.5) < 1e-6
    assert "1.5" in zp["z"] and "0.50000" in zp["x"]     # fx baked into the x expr
    # an off-center target shifts the fraction
    zp2 = DZ.zoompan_params(480.0, 270.0, sec=4.0)
    assert zp2["frames"] == 120 and abs(zp2["fx"] - 0.25) < 1e-6


def test_build_ffmpeg_cmd_is_silent_aac_and_720p():
    zp = DZ.zoompan_params(960.0, 540.0, sec=8.0)
    cmd = DZ.build_ffmpeg_cmd("still.png", zp, 8.0, "out.mp4")
    joined = " ".join(cmd)
    assert "anullsrc" in joined and "aac" in joined          # silent audio track
    assert f"scale=iw*{DZ.SUPERSAMPLE}" in joined            # supersampled
    assert "1280x720" in joined and "libx264" in joined


# --- still compositing (PIL, still no ffmpeg) -------------------------------

def test_render_still_is_dark_canvas_with_yellow_highlight(pdf_page):
    page_rect = (0.0, 0.0, 612.0, 792.0)
    hl_rects = DZ.find_highlight_rects(pdf_page, ["Sniper fired one round"])
    hl = DZ.union_rect(hl_rects)
    crop = DZ.compute_crop(page_rect, hl)
    lay = DZ.canvas_layout(crop)
    img = DZ.render_still(pdf_page, crop, lay, hl_rects)
    assert img.size == (DZ.CANVAS_W, DZ.CANVAS_H)
    assert img.getpixel((5, 5)) == (0, 0, 0)               # letterboxed dark canvas
    # mean colour over the highlight's canvas region is yellowish (R > B)
    cx, cy = DZ.highlight_center_canvas(hl, crop, lay)
    half_w = (hl[2] - hl[0]) * lay["scale"] / 2
    box = (int(cx - half_w), int(cy - 8), int(cx + half_w), int(cy + 8))
    region = img.crop(box)
    w, h = region.size
    px = [region.getpixel((x, y)) for y in range(h) for x in range(w)]
    mean_r = sum(p[0] for p in px) / len(px)
    mean_b = sum(p[2] for p in px) / len(px)
    assert mean_r > 180 and mean_r - mean_b > 20           # highlight tint present
