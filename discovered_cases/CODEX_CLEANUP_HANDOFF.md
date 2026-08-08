# Codex Handoff — vet & clean the case-bundle registry (VERIFY, don't download)

## Mission
Turn `CASE_BUNDLE_AGG.json` into a **vetted, tiered** registry where every listed URL
is **confirmed to exist right now**, every file is correctly classified, and every
bundle carries a quality `tier`. **Verify existence only — do NOT download media.**
We just need to know the file is there, its size, and its type.

Read first: **`CASE_BUNDLE_SPEC.md`** (the quality rubric + A/B/C/D tiers + the COPA
recipe) and **`CODEX_BUNDLE_HANDOFF.md`** (the bundle JSON schema). This doc is the
task list that ties them together with the *current, verified* state of each source.

---

## How to "verify it exists" without downloading

**Direct files** (SDPD / SF DPA / MuckRock / Long Beach): a 1-byte range request,
following redirects, with a browser User-Agent — never a full GET.
```python
r = session.get(url, headers={"Range": "bytes=0-0"}, allow_redirects=True,
                timeout=20, stream=True)   # UA = a real Chrome UA
live = r.status_code in (200, 206)
cr = r.headers.get("Content-Range", "")
size_mb = int(cr.split("/")[-1]) / 1e6 if "/" in cr else None
ctype = r.headers.get("Content-Type", "")
r.close()
```
**Streaming (COPA Vimeo):** resolve metadata, don't pull the stream.
```bash
yt-dlp --skip-download --print "%(title)s|%(duration)s" "https://player.vimeo.com/video/<id>"
```
If it prints a title/duration, the video **exists** (no download). Record
`live`, `size_mb`, `content_type`, `checked_at` on each file.

---

## Current verified state per source (measured this session — trust but re-check)

| source | URLs live? | what they are | action |
|---|---|---|---|
| **SDPD** | ✅ live | direct `.MOV` on `sdpdsb1421.sandiego.gov` (needs UA), 100s of MB each | verify all; it's the **gold** shape; just confirm + tier A |
| **MuckRock** | ✅ live | direct `.mp4` on `cdn.muckrock.com` | verify; tier per content |
| **SF DPA** | ✅ live | `/documents/<id>/download` → 302 → signed S3 | verify; **BWC is often redaction-MUTED** → tier **B** (analysis-only), not A. (Audible check needs a probe download — defer; assume B unless known otherwise.) |
| **Long Beach** | ❌ **broken** | registry "video" URLs are Laserfiche `citydocs.longbeach.gov/.../mediahandler.ashx?...` → **HTTP 500 text/html**, not files | the 320 "media" bundles have **dead** video URLs. Re-extract the real per-file URLs from Laserfiche (headless browser / the repo's file API), or mark these bundles **dead/Tier D**. Do NOT leave 500-ing URLs in the registry. |
| **COPA** | ⚠ video missing | real BWC = **Vimeo embeds** on the case page (NOT in the registry). Registry "bodycam" files are **misclassified press-release PDFs**. 1373 `other` files. | follow the COPA recipe in `CASE_BUNDLE_SPEC.md` §"COPA cleanup" — capture Vimeo, reclassify, tier |

---

## The cleanup tasks (in order)

1. **Verify every file URL** (method above). Add `live`/`size_mb`/`content_type`/`checked_at`.
   Drop or flag (`live:false`) anything that 404s/500s — Long Beach especially.
2. **Fix classifications.**
   - Normalize extensions to **dotted** before classifying (`'pdf'` → `'.pdf'`) — the
     bare-vs-dotted bug silently dumps PDFs into `other`/`media`.
   - COPA: the press-release PDFs ("COPA RELEASES VIDEO…") mis-typed `bodycam` → `documents`.
   - COPA: triage the 1373 `other` by extension/title → `documents`/`audio`/`video`; drop dead links.
   - SDPD/MuckRock: retype bare `media`/`other` to real `video`/`audio` by extension.
3. **Capture COPA video** — scrape each `chicagocopa.org/case/<id>/` page for
   `player.vimeo.com/video/<id>` iframe srcs, record each as a `video` media_file
   (name = the yt-dlp `%(title)s`, e.g. "Log #… BWC 1"). Verify with `yt-dlp --skip-download`.
4. **Fix or retire Long Beach** — extract real downloadable URLs from Laserfiche, or
   tier the bundles **D** and stop counting them as media.
5. **Tier every bundle** A/B/C/D per `CASE_BUNDLE_SPEC.md`. Store `tier` on the bundle.
6. **Re-emit** a clean `<source>_candidates.json` per source (same schema as the
   existing harvester outputs), then run `python discovered_cases/case_bundle_agg.py`
   to regenerate `CASE_BUNDLE_AGG.json` + `.md`.

---

## Rules
- **Never download media** — verify-only (range probe / yt-dlp resolve). Sizes come
  from headers, not bytes on disk.
- **Never hand-edit** `CASE_BUNDLE_AGG.json` / `.md` — they're generated. Fix the
  per-source `*_candidates.json` and re-run `case_bundle_agg.py` (it auto-discovers them).
- **Don't fabricate URLs.** A bundle with no live media is Tier B (docs/audio only) or D.
- Send a real browser **User-Agent**; most of these hosts 403 a bare fetcher.

## Suggested deliverables
- `discovered_cases/vet_bundles.py` — reads the registry, verifies URLs (no download),
  reclassifies + tiers, writes back per-source candidate files + a `vet_report.md`
  (counts live/dead/tier per source). Add a `--selftest` like the other tools.
- COPA Vimeo capture added to `copa_harvest.py` (scrape case pages → Vimeo srcs).

## Reference files (don't reinvent)
- `CASE_BUNDLE_SPEC.md` — rubric, tiers, COPA Vimeo recipe.
- `CODEX_BUNDLE_HANDOFF.md` — the bundle/candidate JSON schema + how the aggregator reads it.
- `case_bundle_agg.py` — the aggregator (auto-discovers `*_candidates.json`; has `--selftest`).
- `*_harvest.py` — the per-source harvesters (sfdpa/sdpd/copa/longbeach/muckrock) to mimic/extend.
- `muckrock_harvest.py` — exports the shared classifiers (`score_text`, `detect_artifacts`,
  `ext_of`, dotted `VIDEO_EXTS`/`AUDIO_EXTS`/`DOC_EXTS`).
