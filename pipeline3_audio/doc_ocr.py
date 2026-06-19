"""Extensive-mine step (GOAL_D D5 prep): OCR a scanned case PDF page-by-page.

Sacramento (and most agency) document releases are scanned images with no text
layer. This dumps every page to text via easyocr so we can SEE what's in the
file (disposition, CAD/dispatch log, officer-interview transcripts, narrative)
before building the structured extractor. Saves incrementally so partial
results are usable while it runs.

    python pipeline3_audio/doc_ocr.py --pdf ".tmp/sac_poc/docs/DOCS/2023PSB-0530.pdf" \
        --out .tmp/sac_poc/docs/ocr

Output: <out>/pages.json (per-page text) + <out>/full_text.txt (combined).
Run with the global Python (easyocr + pypdf + PIL). Slow on big files (~5-20s/page).
"""
from __future__ import annotations

import argparse
import io
import json
import sys
from pathlib import Path
from typing import List, Optional

import numpy as np


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="OCR a scanned case PDF page-by-page")
    ap.add_argument("--pdf", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--start", type=int, default=0, help="first page index (0-based)")
    ap.add_argument("--end", type=int, default=None, help="last page index (exclusive)")
    ap.add_argument("--save-every", type=int, default=5)
    args = ap.parse_args(argv)

    import pypdf
    import easyocr
    from PIL import Image

    args.out.mkdir(parents=True, exist_ok=True)
    reader = easyocr.Reader(["en"], gpu=False, verbose=False)
    pdf = pypdf.PdfReader(str(args.pdf))
    n = len(pdf.pages)
    end = args.end if args.end is not None else n
    pages: List[dict] = []

    def _flush():
        (args.out / "pages.json").write_text(json.dumps(pages, indent=1), encoding="utf-8")
        (args.out / "full_text.txt").write_text(
            "\n\n".join(f"===== PAGE {p['page']} =====\n{p['text']}" for p in pages),
            encoding="utf-8")

    for i in range(args.start, end):
        text = ""
        try:
            imgs = list(pdf.pages[i].images)
            if imgs:
                arr = np.array(Image.open(io.BytesIO(imgs[0].data)).convert("RGB"))
                lines = reader.readtext(arr, detail=0, paragraph=True)
                text = "\n".join(lines)
        except Exception as exc:  # noqa: BLE001 — keep going, log the page
            text = f"[OCR_ERROR: {type(exc).__name__}: {exc}]"
        pages.append({"page": i + 1, "chars": len(text), "text": text})
        if (i + 1) % args.save_every == 0 or i == end - 1:
            sys.stderr.write(f"  OCR {i + 1}/{end}  ({len(text)} chars)\n"); sys.stderr.flush()
            _flush()
    _flush()
    total = sum(p["chars"] for p in pages)
    print(f"[doc_ocr] {len(pages)} pages, {total} chars -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
