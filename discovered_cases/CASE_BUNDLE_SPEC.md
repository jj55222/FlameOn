# Case Bundle Spec — what makes a bundle PIPELINE-READY

Defines the ideal case bundle and a quality rubric, so the registry
(`CASE_BUNDLE_AGG.json`) can be cleaned toward the **SDPD gold shape**. This is the
"what we're looking for" doc; `CODEX_BUNDLE_HANDOFF.md` is the "how to add the JSON"
doc. Read both. The criteria below are grounded in real pipeline runs, not theory —
each one is something we watched break (or hold) on a live bundle.

---

## The gold standard: SDPD

A SDPD case is one downloadable, per-case folder of raw evidence:

```
<case>/  Video/   (BWC + dashcam + CCTV, raw .MOV/.mp4 — DOWNLOADABLE)
         Audio/   (911 / radio / interviews — DOWNLOADABLE)
         Documents/ (IA / OIS / UoF report PDFs)
         Photos/  (scene / booking stills)
```

Example: `sdpd_08_11_2023_3400_lebon_drive` — **70 video + 41 audio + 1 doc**, every
file a direct download from `sdpdsb1421.sandiego.gov`. **The ideal is to get every
source into this shape.**

---

## What the PIPELINE actually needs (the criteria that matter)

Each is tagged with the run that proved it matters.

1. **DOWNLOADABLE files, not streaming.** Every media URL must resolve to a file
   (direct, or a 302 → signed S3/CDN). Streaming embeds (YouTube/Vimeo/JW Player)
   are NOT pipeline-ready without stream extraction. → *COPA fails this: 0 of 2128
   bundles have a downloadable video; its BWC is embeds.*
2. **AUDIBLE incident footage.** The bodycam audio must carry speech — mean loudness
   above ~−40 dB. Redaction sometimes MUTES the whole clip. → *SF DPA `0409_18`
   failed: 2-min BWC at −54 dB mean → ~0 transcript. The case's substance was all in
   the interviews + doc, so it could not drive a bodycam cut.*
3. **REAL timestamps on the incident footage.** Chronology needs an absolute time
   per clip — from AXON on-screen clock (OCR), a date in the filename, or trustworthy
   metadata. → *Validated: clock-OCR read the AXON stamp even off REDACTED SF DPA BWC,
   so redaction alone doesn't kill timing. Non-AXON cams (e.g. exacqVision) usually
   carry only an export date — distrust it.*
4. **A PARSEABLE incident DOCUMENT.** An IA / OIS / UoF report PDF the doc-extractor
   can read into CASE_FACTS (subject, findings, disposition) — the accountability
   spine and the grounding for narration. Image-only scans need OCR first; a 400 MB
   image PDF is slow.
5. **Enough SALIENT incident footage.** Not just 2 minutes of near-silence. A real
   cut needs several minutes of footage where something narratable happens. Long
   interview audio + a doc can carry an *analysis-style* cut even when footage is
   thin — but a *bodycam* cut needs the footage.
6. **Multiple POVs at the incident (nice-to-have).** ≥2 cameras converging at one
   time sharpens the auto-anchor (salience × camera-convergence).

---

## Quality tiers (score every bundle)

| Tier | Meaning | Checklist |
|---|---|---|
| **A — pipeline-ready** | runs end-to-end as-is | downloadable ✓ · audible BWC ✓ · timestamps ✓ · doc ✓ · ≥3 min salient footage ✓ |
| **B — analysis-only** | no usable footage, but rich interviews + doc | downloadable ✓ · doc ✓ · interviews ✓ · (BWC muted/absent) |
| **C — needs work** | fixable with effort | streaming video (extractable) OR doc-only OR uncategorized files |
| **D — drop/defer** | not worth it now | streaming-only with no extractable media, or doc-only with no media |

A bundle is **media-pipeline-ready (A)** only if it clears criteria 1–5. B is still
valuable (it's the EWU/interview style). C is the cleanup queue. D is noise.

---

## Per-source status (current)

| Source | Tier | Note |
|---|---|---|
| **SDPD** | **A** | gold — downloadable per-case Video/Audio/Documents/Photos. Verify audio isn't muted per case. |
| **SF DPA** | **B** | interview + doc rich, downloadable; BWC often redaction-MUTED → analysis-style cut, not bodycam. |
| **Long Beach** | **C** | Laserfiche — registry URLs need to resolve to direct files (JS/cookie-gated). Vet URLs first. |
| **MuckRock** | **B/C** | mixed; per-bundle. |
| **COPA** | **D→C** | needs the most work — see below. |

---

## COPA cleanup — the concrete tasks (for Codex)

Measured from the registry: **2128 COPA bundles, 0 with downloadable video**, file
types = `documents: 3482`, `other: 1373`, `911_audio: 9`, `bodycam: 21` (and even
those 21 "bodycam" files register `n_video=0` — they're streaming refs or
misclassified). So COPA today is a pile of doc + audio + uncategorized links.

Goal: turn each COPA *case page* (`chicagocopa.org/case/<id>/`) into an SDPD-shape
bundle. Tasks, in order:

1. **Extract the streaming video.** COPA's BWC / in-car / 3rd-party surveillance are
   embedded players, not files. For each case page, resolve the embed to a
   downloadable stream (yt-dlp / direct mp4 in the page source). If a case has NO
   extractable video → it's Tier B (audio+doc) or D, mark it, don't pretend it has video.
2. **Reclassify the 1373 `other` files.** Inspect extensions/titles: PDFs →
   `documents`; mp3/wav → `audio`/`911_audio`; mp4/mov → `video`; drop dead links.
   `other` should end near zero.
3. **Fix the `bodycam`-but-`n_video=0` mismatch.** Those 21 are typed bodycam but not
   counted as video — they're streaming URLs. Either extract them (task 1) or retype.
4. **Collapse doc-only bundles.** Most of the 2128 are doc-only (no media) → they're
   not media bundles. Either enrich with extracted video (task 1) or flag
   `tier: D` so they stop diluting the media counts.
5. **Re-emit** as a clean `chicago_copa_candidates.json` (same schema as the others)
   and re-run `case_bundle_agg.py`. Each surviving bundle should carry a `tier` and,
   ideally, downloadable Video + Audio + Documents.

Target end state: COPA bundles that reach **Tier A/B** look like SDPD — a case id
with downloadable `media_files` (video/audio) + `doc_files`, every URL resolving.

---

## Machine-checkable rubric (so a vetter can score)

Per bundle, compute and store:
- `downloadable`: fraction of file URLs that return 200/206 (HEAD/range), following redirects.
- `has_video`, `has_audio`, `has_docs`: real, downloadable.
- `bwc_audible`: for ≥1 video, ffmpeg `volumedetect` mean_volume > −40 dB.  *(needs a probe download)*
- `stamped`: ≥1 video yields a timestamp (AXON OCR / filename / metadata).  *(needs the file)*
- `tier`: A/B/C/D from the table above.

`downloadable` + `has_*` are cheap (HEAD only). `bwc_audible` + `stamped` need a small
probe download — run them only on bundles that already pass the cheap checks.
