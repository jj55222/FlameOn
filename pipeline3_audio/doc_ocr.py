"""Mine a case PDF to per-page text — works on EITHER file style, via a router.

Two PDF styles in the wild: (1) born-digital with a real text layer (fast — just
extract it), and (2) scanned images with no text (need OCR). This routes per page:

  page has a text layer  -> extract it (pypdf), no OCR
  page is scanned        -> rasterize it (PyMuPDF/fitz, which decodes ANYTHING —
                            incl. CCITT-fax scans pypdf can't) -> OCR

OCR backend is itself routed by what's available:
  Apple Vision (ocrmac, macOS)  -> fast, on the Neural Engine (~1-2 pg/s)   [preferred]
  easyocr (cross-platform CPU)   -> slow fallback (~5-20 s/page)

Force a path with --ocr {auto,vision,easyocr,text-only}. Saves incrementally.

    python pipeline3_audio/doc_ocr.py --pdf case.pdf --out .tmp/docs/ocr
    # -> <out>/pages.json (per-page {page,chars,ocr,text}) + <out>/full_text.txt
"""
from __future__ import annotations

import argparse
import io
import json
import platform
import sys
from collections import Counter
from pathlib import Path
from typing import List, Optional

MIN_TEXT_CHARS = 20   # a page with fewer embedded-text chars is treated as scanned


def vision_available() -> bool:
    """Apple Vision OCR usable here? (macOS + ocrmac + fitz installed)."""
    if platform.system() != "Darwin":
        return False
    try:
        import fitz  # noqa: F401  (PyMuPDF)
        from ocrmac import ocrmac  # noqa: F401
        return True
    except Exception:
        return False


def _ocr_vision(img) -> str:
    """Apple Vision OCR on a PIL image -> text, in rough reading order."""
    from ocrmac import ocrmac
    res = ocrmac.OCR(img).recognize()          # [(text, confidence, bbox[x,y,w,h] norm, bottom-left origin)]
    def _key(r):
        bb = r[2] if len(r) > 2 and r[2] else (0, 0, 0, 0)
        return (-(bb[1] or 0), bb[0] or 0)     # top-to-bottom (y is bottom-up), then left-to-right
    return "\n".join(t for t, *_ in sorted(res, key=_key) if t)


class _EasyOCR:
    """Lazy cross-platform fallback — only builds the reader if actually used."""
    def __init__(self) -> None:
        self._r = None

    def __call__(self, img) -> str:
        import numpy as np
        if self._r is None:
            import easyocr
            self._r = easyocr.Reader(["en"], gpu=False, verbose=False)
        return "\n".join(self._r.readtext(np.array(img.convert("RGB")), detail=0, paragraph=True))


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Mine a case PDF to per-page text (router: text-layer vs OCR)")
    ap.add_argument("--pdf", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path, help="OUTPUT DIRECTORY (pages.json is written inside)")
    ap.add_argument("--start", type=int, default=0, help="first page index (0-based)")
    ap.add_argument("--end", type=int, default=None, help="last page index (exclusive)")
    ap.add_argument("--ocr", choices=["auto", "vision", "easyocr", "text-only"], default="auto",
                    help="auto = Vision if available else easyocr; text-only = never OCR")
    ap.add_argument("--dpi", type=int, default=200, help="rasterization DPI for OCR")
    ap.add_argument("--save-every", type=int, default=5)
    args = ap.parse_args(argv)

    import pypdf
    from PIL import Image

    # route the OCR backend
    if args.ocr == "vision":
        if not vision_available():
            print("[doc_ocr] --ocr vision requested but Apple Vision/ocrmac unavailable", file=sys.stderr)
            return 2
        use_vision, ocr_on = True, True
    elif args.ocr == "easyocr":
        use_vision, ocr_on = False, True
    elif args.ocr == "text-only":
        use_vision, ocr_on = False, False
    else:  # auto
        use_vision = vision_available()
        ocr_on = True
    backend = "vision" if (ocr_on and use_vision) else ("easyocr" if ocr_on else "off")
    print(f"[doc_ocr] OCR backend: {backend}  (text layer used where present)")

    args.out.mkdir(parents=True, exist_ok=True)
    pdf = pypdf.PdfReader(str(args.pdf))
    end = args.end if args.end is not None else len(pdf.pages)
    easy = _EasyOCR()
    _fitz_doc: list = []

    def _render(i: int):
        import fitz
        if not _fitz_doc:
            _fitz_doc.append(fitz.open(str(args.pdf)))
        pix = _fitz_doc[0][i].get_pixmap(dpi=args.dpi)
        return Image.open(io.BytesIO(pix.tobytes("png")))

    pages: List[dict] = []
    used: Counter = Counter()

    def _flush() -> None:
        (args.out / "pages.json").write_text(json.dumps(pages, indent=1), encoding="utf-8")
        (args.out / "full_text.txt").write_text(
            "\n\n".join(f"===== PAGE {p['page']} ({p['ocr']}) =====\n{p['text']}" for p in pages),
            encoding="utf-8")

    for i in range(args.start, end):
        src = "text"
        try:
            text = (pdf.pages[i].extract_text() or "").strip()       # 1) text layer?
            if len(text) < MIN_TEXT_CHARS and ocr_on:                # 2) scanned -> rasterize + OCR
                img = _render(i)
                text = _ocr_vision(img) if use_vision else easy(img)
                src = backend
        except Exception as exc:  # noqa: BLE001 — never let one bad page stop the run
            text, src = f"[OCR_ERROR: {type(exc).__name__}: {exc}]", "error"
        used[src] += 1
        pages.append({"page": i + 1, "chars": len(text), "ocr": src, "text": text})
        if (i + 1) % args.save_every == 0 or i == end - 1:
            sys.stderr.write(f"  [{src:7s}] page {i + 1}/{end}  ({len(text)} chars)\n"); sys.stderr.flush()
            _flush()
    _flush()
    total = sum(p["chars"] for p in pages)
    print(f"[doc_ocr] {len(pages)} pages, {total} chars  ({dict(used)}) -> {args.out / 'pages.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
