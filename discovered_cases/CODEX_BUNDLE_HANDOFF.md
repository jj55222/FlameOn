# Codex Handoff — adding case bundles to the shared registry

You (Codex) are contributing **case bundles** — a case's downloadable evidence files
(BWC/dashcam/surveillance video, interview/911/radio audio, IA/UoF/DA PDFs) and the
**direct download URLs** for each — to one shared, aggregated registry the rest of the
FlameOn pipeline reads.

**The registry (generated — do NOT hand-edit):**
- `discovered_cases/CASE_BUNDLE_AGG.json` — complete: every working bundle + every file URL (source of truth)
- `discovered_cases/CASE_BUNDLE_AGG.md` — human index of the media (video/audio) bundles
- Built by `discovered_cases/case_bundle_agg.py`, which **auto-discovers every `*_candidates.json`** in this dir.

A bundle "works" (gets into the registry) iff it resolves to **≥1 downloadable file URL**.

---

## How to add your bundles (pick one)

### Option A — preferred: write a `<portal>_candidates.json`
Drop a file named `discovered_cases/<yourportal>_candidates.json` (a JSON **list** of bundle
objects, schema below), then regenerate:
```bash
python discovered_cases/case_bundle_agg.py          # picks up your new file automatically
python discovered_cases/case_bundle_agg.py --selftest
```
Naming: end it in `_candidates.json`. Avoid `*probe*` and `candidates_ranked.json` — those are
excluded on purpose. One file per portal.

### Option B — a handful by hand
Append bundle objects to `discovered_cases/CASE_BUNDLE_AGG.manual.json` (a JSON list, same schema).
They're merged on the next run. Use this only for one-offs.

---

## Bundle schema (minimum the registry needs)

```json
{
  "source": "sf_pd",                         // short stable portal id (snake_case)
  "case_id": "sf_pd_2024-001",               // UNIQUE per bundle; dedup key is (source, case_id)
  "agency": "San Francisco Police Department",
  "title": "2024-001 Officer-Involved Shooting",
  "case_url": "https://portal.example/case/2024-001",   // landing page for the case
  "n_video": 2, "n_audio": 1, "n_docs": 1,   // counts (used for ranking; set them)
  "media_files": [
    {"name": "BWC Officer Lee.mp4", "url": "https://portal.example/doc/991/download", "evidence_type": "bodycam"},
    {"name": "Store CCTV.mp4",      "url": "https://portal.example/doc/992/download", "evidence_type": "surveillance"},
    {"name": "Interview Lee.mp3",   "url": "https://portal.example/doc/993/download", "evidence_type": "interrogation"}
  ],
  "doc_files": [
    {"name": "IA Report 2024-001.pdf", "url": "https://portal.example/doc/994/download", "evidence_type": "documents"}
  ]
}
```

Rules the aggregator enforces:
- Each file is `{name, url, evidence_type}`. The **`url` must be a direct download** (a redirect to
  S3/CDN is fine — downloaders follow redirects). If you only have a bare list, set `"urls": [...]`
  instead of `media_files`, but `media_files`/`doc_files` is strongly preferred.
- URLs are **deduped within a bundle**; bundles are **deduped by `(source, case_id)`** — so make
  `case_id` unique and stable (re-running with the same id overwrites, doesn't duplicate).
- `n_video/n_audio/n_docs` default to 0 if omitted; set them so ranking + the media index work.

`evidence_type` vocabulary (use these — they map to pipeline kinds):
`bodycam · dashcam · surveillance · interrogation · 911_audio · radio · audio · video · documents · other`

---

## Match the existing harvesters (recommended for full pipeline ingest)

The richer candidate schema (so a bundle also flows cleanly into pipeline3 transcribe / pipeline4
score) is produced by the shared helpers in `discovered_cases/muckrock_harvest.py`. Reuse them:

```python
from muckrock_harvest import (
    score_text, detect_artifacts, slugify, ext_of,
    VIDEO_EXTS, AUDIO_EXTS, DOC_EXTS,   # NOTE: these are DOTTED ('.mp4', '.pdf', ...)
)
```

Look at these as templates (simplest → most complete):
- `sdpd_harvest.py` — open static-directory portal (scrape case pages → S3 file URLs).
- `sfdpa_harvest.py` — NextRequest JSON API (live paged crawl → per-folder bundles).
- `copa_harvest.py`, `longbeach_harvest`/`muckrock_harvest.py` — more portal shapes.

---

## Gotchas already paid for (don't re-learn these)

- **Dotted extensions.** `VIDEO_EXTS/AUDIO_EXTS/DOC_EXTS` are dotted (`.pdf`). If your portal's API
  hands you a bare `"pdf"`, normalize to `".pdf"` before classifying — otherwise **every PDF silently
  falls into `other` and the legal-doc half of the bundle disappears** (this bit SF DPA).
- **Browser User-Agent.** Most of these portals 403 a plain fetcher. Send a real `User-Agent`.
- **NextRequest paging** (if you touch one): the param is `page_number` (NOT `page`/`offset`, which are
  silently ignored → infinite first-page loop), and `page_size` caps at 50.
- **Verify a sample.** HEAD a few of your `url`s and confirm a 200/302→file before publishing the file.

---

## Done checklist
1. `discovered_cases/<portal>_candidates.json` written (or `CASE_BUNDLE_AGG.manual.json` appended).
2. `python discovered_cases/case_bundle_agg.py` re-run; your `source` shows in the printed per-source counts.
3. `python discovered_cases/case_bundle_agg.py --selftest` passes.
4. Spot-checked a couple of your URLs resolve to real files.

Questions about the documentary pipeline these feed (timeline → blueprint → shaped cut → judge gate):
see the top-level `HANDOFF.md`.
