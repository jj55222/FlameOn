# Handoff — MuckRock Case Intake → FlameOn Pipeline

**Date:** 2026-06-27
**Author:** prior session (Claude)
**Goal:** Use the MuckRock API to discover/build cases (BWC video + interrogation + SB1421/16
accountability artifacts) and feed them through the existing FlameOn pipeline to find "winners."

This doc is self-contained so a fresh agent (Codex) can continue. Read it top to bottom once.

---

## 0. TL;DR of what exists now

- **`discovered_cases/muckrock_harvest.py`** — the new harvester. Queries MuckRock api_v2, walks
  requests → communications → files, scores each case by the **artifact bundle** it carries
  (BWC/dash video, interrogation, SB1421/16 docs), and writes a ranked candidate list.
- **`discovered_cases/muckrock_candidates.json`** — 53 ranked candidates from the last full run.
- Two cases already pushed end-to-end (see §7).
- Calibration scripts to (re)tune the search terms: `discovered_cases/muckrock_calibrate_terms.py`,
  `discovered_cases/muckrock_calibrate_v2.py`.

---

## 1. Environment setup

```bash
cd /Users/jmoney/FlameOn-main
source .venv/bin/activate          # Python 3.12 venv already created
# deps already installed: requests, faster-whisper, mlx-whisper, groq/openai, etc.
```

Requirements:
- **ffmpeg + ffprobe on PATH** (brew). Confirm: `which ffmpeg ffprobe` → `/opt/homebrew/bin/...`.
  The `imageio-ffmpeg` shim lacks `ffprobe`, so brew's is required by the finders/transcriber.
- macOS Apple-Silicon. `mlx-whisper` (GPU) is installed but **not wired** into the transcriber;
  the transcriber's `--backend local` uses `faster-whisper` (CPU) instead — fine for short audio.

### 1a. `.env` — CRITICAL GOTCHAS (these cost the prior session hours)

`.env` lives at repo root and is git-ignored. **Several keys have INLINE COMMENTS after the value,
and the value is often EMPTY.** Naive `grep | cut` extraction grabs the comment text (which contains
an em-dash `—`), producing a garbage "key" → `UnicodeEncodeError` on the HTTP header, or a silent
empty value. Always strip the inline `#...` comment and trim.

| Key | State in `.env` | Use it how |
|-----|-----------------|------------|
| `MUCKROCK_USERNAME` / `MUCKROCK_PASSWORD` | **SET** (muckrock.com login) | harvester reads via its own clean parser |
| `OPENROUTER_API_KEY` | **SET** (`sk-or-v1...`, has trailing comment) | needed by pipeline4 + find_shooting_pov |
| `GROQ_API_KEY` | **EMPTY** (only a comment) | do NOT use; use `--backend local` for whisper |
| `MUCKROCK_API_TOKEN` | dead (empty + comment) | ignore; auth is JWT, not a token |

There is a **robust `.env` parser already written** — reuse it instead of shell hacks:

```python
import sys; sys.path.insert(0, "discovered_cases")
from muckrock_harvest import _read_env_file      # strips inline comments + quotes
key = _read_env_file().get("OPENROUTER_API_KEY", "")
```

**`pipeline4_score.py` reads keys from `os.environ` ONLY** (it does not auto-load `.env`). Export first:
```bash
export OPENROUTER_API_KEY=$(python3 -c "import sys; sys.path.insert(0,'discovered_cases'); \
  from muckrock_harvest import _read_env_file; print(_read_env_file().get('OPENROUTER_API_KEY',''))")
```

### 1b. Shell gotcha
Default shell is **zsh**, which does **NOT word-split unquoted `$VARS`**. To pass many files to a
`nargs="+"` arg, pass a **glob directly** (`--transcripts dir/kolb_*.json`), not `$FILES`.

---

## 2. MuckRock api_v2 — auth + the filter gotchas

**Auth = Squarelet JWT (NOT a static token).** POST username/password →
`https://accounts.muckrock.com/api/token/` → `{access, refresh}`; then call api_v2 with
`Authorization: Bearer <access>`. The harvester does this automatically (re-auths on 401).

**Endpoints:** `requests/`, `communications/`, `files/`, `agencies/` (base `https://www.muckrock.com/api_v2/`).

**Three traps baked into the harvester (do not "fix" them back):**
1. The `requests/` serializer returns `agency` as a bare **int id** and **no `absolute_url`**.
2. `communications/` filters on **`foia=<request_id>`**. The intuitive `request=` is **silently
   ignored** and returns the global ~1.4M-comm feed. (Same for `files/?request=`.)
3. A communication's `files` field is a list of **integer file IDs**, not objects. Resolve them with
   **`files/?communication=<comm_id>`** — the only working files filter (`id__in`/`request` are
   ignored and dump all ~1M files). `ffile` = the CDN download URL; `title` = filename.

The old `autoresearch/research.py:search_muckrock()` hits `api_v2/foia/` which **404s** — it's dead.
`muckrock_harvest.py` supersedes it.

---

## 3. The artifact-bundle model (how cases are scored)

A "winner" is no longer "has video." It's the **mix** of artifacts:
- BWC / dash **video** (+6)
- interrogation / interview **audio or video** (+5/+4)
- **SB1421/16** accountability docs (+5)
- plus a **bundle bonus** when several co-occur in one case (the ideal).

Artifact detection (`detect_artifacts()` in the harvester) sniffs request + file titles:
`is_bwc`, `is_interrogation`, `is_sb16`. Each candidate carries these flags + a `pivot` block
(agency name + case title + date) for the cross-source step (§6).

Keep filter: `--keep artifact` (default; video/audio/SB16-doc/interrogation) | `media` | `any`.

---

## 4. How to run each stage

### 4a. Harvest a candidate list (~6–10 min for 16 terms)
```bash
source .venv/bin/activate
python discovered_cases/muckrock_harvest.py --per-term 40 --keep artifact \
  --out discovered_cases/muckrock_candidates.json
# smaller test:  --terms "body camera footage" "SB 1421" --per-term 8 --min-score 0
```
Output: ranked JSON, each candidate has `media_files` (download URLs), `doc_files`, artifact
flags, `pivot`, and `score`. Console prints a `[keep NN VASI]` line per case (V/A/S/I tags).

### 4b. Re-tune the search net (optional; if yield drops)
```bash
python discovered_cases/muckrock_calibrate_v2.py   # prints per-term video/audio/doc + SB16/interr/BWC yield
```
Then edit `DEFAULT_TERMS` in `muckrock_harvest.py`. Lesson learned: "footage"/"camera"/"dash"
phrasings yield video; "body worn camera"/"use of force"/"in custody death" return only docs.

### 4c. Download a case's media
The harvester `--download` flag fetches `media_files` into `pipeline3_audio/foia_cache/`. Or pull
specific URLs from the candidate JSON with curl. **Heads-up: OIS bodycam files can be huge** (the
Mark Johnson case was 3.3 GB for 2 files); interrogation audio is small (~62 MB for Kolb's 10).

### 4d. Pipeline path depends on artifact type

**VIDEO cases (BWC/OIS)** → zip the files, then the audio/vision finders:
```bash
zip -0 -j case.zip file1.mp4 file2.mp4          # store mode (no recompress)
python pipeline3_audio/zip_triage.py --zip case.zip --out .tmp/<case>/zip_triage.json
python pipeline3_audio/find_shooting_pov.py --zip case.zip \
  --triage .tmp/<case>/zip_triage.json --out .tmp/<case>/pov_vision.json \
  --top-n 12 --budget-usd 0.50           # vision step uses OpenRouter (needs key loaded)
```

**INTERROGATION / AUDIO cases** → transcribe, then salience-score:
```bash
export PYTHONIOENCODING=utf-8
# 1) transcribe each file (LOCAL backend — Groq key is empty!)
python pipeline3_audio/pipeline3_transcribe.py --audio-file <f> --case-id <label> \
  --evidence-type interrogation --backend local --whisper-model small \
  --output pipeline3_audio/transcripts/
# 2) all files for one case must share case_id BEFORE merge — stamp it:
python3 -c "import json,glob; [ (lambda d: (d.update(case_id='<CASE>'), \
  json.dump(d, open(f,'w'), ensure_ascii=False)))(json.load(open(f))) \
  for f in glob.glob('pipeline3_audio/transcripts/<label>_*_transcript.json') ]"
# 3) two-pass salience scoring (export OPENROUTER_API_KEY first, see §1a)
python pipeline4_scoring/pipeline4_score.py \
  --transcripts pipeline3_audio/transcripts/<label>_*_transcript.json \
  --case-id <CASE> --output verdicts/
```
Transcript segments are under the **`transcript`** key (not `segments`). Verdict → `verdicts/<CASE>_verdict.json`
with `verdict` (PRODUCE/HOLD/SKIP), `narrative_score`, `key_moments`, `content_pitch`.

---

## 5. Key file locations

| Path | What |
|------|------|
| `discovered_cases/muckrock_harvest.py` | the harvester (auth, fetch, classify, score, download) |
| `discovered_cases/muckrock_candidates.json` | 53 ranked candidates (last full run) |
| `discovered_cases/muckrock_candidates_probe.json` | smaller probe run |
| `discovered_cases/muckrock_calibrate_terms.py` / `_v2.py` | term-yield calibration |
| `discovered_cases/sdpd_harvest.py` | **San Diego PD video crawler** (50 cases, 253 videos) — same schema |
| `discovered_cases/sdpd_candidates.json` | SD PD crawl output (ranked, with download URLs) |
| `discovered_cases/rank_candidates.py` | shared narrative keyword scorer (`score_text`, reused) |
| `pipeline3_audio/pipeline3_transcribe.py` | transcriber (`--backend local|groq`) |
| `pipeline4_scoring/pipeline4_score.py` | two-pass salience scorer → verdict |
| `pipeline3_audio/foia_cache/muckrock_78826/` | Mark Johnson OIS, 2 bodycams (~6.6 GB) |
| `pipeline3_audio/foia_cache/muckrock_181169_kolb/` | Kolb interrogation, 10 files (~62 MB) |
| `pipeline3_audio/transcripts/kolb_*_transcript.json` | Kolb transcripts (10) |
| `verdicts/muckrock_181169_kolb_verdict.json` | Kolb salience verdict (HOLD) |
| `.tmp/muckrock_78826/` | Mark Johnson triage + pov_vision output |
| `.env` | creds (see §1a gotchas) |

---

## 6. Cross-source pivot — where the VIDEO actually is

**Important finding:** SB1421/16 record portals serve **PDFs** (personnel/IA records). The **video**
is a different law — **AB748** ("critical incident video") — and is NOT on those portals.
- The **Berkeley California Police Records Access Project** explicitly **excludes audio/video**
  (docs/transcripts/photos only) — not a video source.
- AB748 video has **no statewide portal**; agencies post it on their own **"Critical Incident
  Video" pages, usually YouTube/Vimeo** (e.g., San Diego PD). This is the SAME channel as the
  existing `discovered_cases/` CIB YouTube source (yt-dlp).

**So, for VIDEO, in priority order:**
1. **MuckRock "body camera footage" / "dash camera" requests** — proven real `.mp4`s (Joliet 20-video
   case, Camden 9, Miami Beach 14, Washington State Patrol 4). Already in the candidate list.
2. **Agency Critical Incident Video pages / YouTube** (AB748) — extend the existing CIB-YouTube intake.
3. **NextRequest portals** (SFDPA-style) — can host video/audio, unlike SB1421 PDF portals.

SB1421 hits are best treated as **case documents** to enrich a video case, not as a video source.
The harvester emits each candidate's `pivot` block to support looking up the agency's fuller
release — but expect docs there, not footage.

---

## 6b. VIDEO PORTALS (AB748 critical-incident video) — ranked ingest targets

Since SB1421 portals are PDFs, here are the actual **video** sources. A prior session already ran a
verified 24-agency portal hunt (see the `transparency-portals` memory + the Sacramento SO gold
standard `sacsheriff.com/pages/released_cases.php`). Reconciled ranking below. Two ingest patterns
already exist in this repo: **yt-dlp** (CIB YouTube playlists, `discovered_cases/download_all.py`)
and **NextRequest scrape** (SFDPA, `rank_candidates.py`).

**Tier 1 — clear the bar (downloadable video bundled with case docs):**

| # | Portal | URL | Ingest reality |
|---|--------|-----|----------------|
| 1 | **San Diego PD** ⭐ **CRAWLER BUILT** | front-end: `sandiego.gov/.../mandated-disclosures/sb16-sb1421-ab748`; files: `https://sdpdsb1421.sandiego.gov` | `discovered_cases/sdpd_harvest.py` — DONE & verified. Crawls index → 50 case pages → S3 file URLs → `sdpd_candidates.json` (50 cases, 253 videos). NB: the S3 bucket **denies listing** (scrape the case pages, can't enumerate the bucket); whole site **403s without a browser UA**; case hrefs have a **spurious mid-URL newline** + literal spaces (clean = strip `\n\r\t`, encode spaces→`%20`). All handled. |
| 2 | **Chicago COPA** richest media | `https://www.chicagocopa.org/data-cases/case-portal/` | ~101 pages, filter by log#/district/type/date. BWC + in-car + 3rd-party **surveillance** + 911/radio + PDFs. **Video = streaming embeds → needs stream extraction** (inspect case-page network requests). Docs = direct PDFs. Start case `2025-0003972`. |
| 3 | **Long Beach PD** deepest archive | `https://citydocs.longbeach.gov/LBPDPublicDocs/` | Laserfiche, OIS back to 1968 (200+). BWC + surveillance + reports per case. **JS/cookie-gated → headless browser** to enumerate. |
| 0 | **Sacramento SO** gold standard | `https://www.sacsheriff.com/pages/released_cases.php` | BWC + dashcam + CCTV + 911/radio **AND** IA/OIS reports + DA letters, bundled per case. The template the others are measured against. |

**Tier 2 — video-only / streaming (no doc bundle), still usable for footage:**
- **LAPD** — CIV page `lapdonline.org/.../critical-incident-videos/` + YouTube "Critical Incident
  Community Briefings" playlist `PLW5iqZEagvjMvmXRnBaYqozLYwmzUO2B9` (yt-dlp). Raw BWC inside briefings.
- **San Diego County Sheriff CIB** — YouTube playlists, **already wired** in `discovered_cases/`.
- **SF DPA** — `sfdpa.nextrequest.com/documents` — BWC + interrogation audio + docs (already a source).
- **LASD, LVMPD, Maryland OAG IID** — deep DA-letter docs but **video is YouTube-streamed**.

**Docs-only (NOT video):** CA DOJ `oag.ca.gov/ois-incidents` (AB1506 decision PDFs, zero media);
SB1421/16 record portals (Oakland, etc.); the Berkeley Police Records Access DB (excludes audio/video).

**Avoid:** EDITED "briefing" narratives (Phoenix) — not raw footage; third-party aggregator channels
(PoliceActivity, EWU Bodycam) — re-uploads, no case metadata. Stick to official agency / oversight sources.

**Mechanisms (not portals):** Axon `evidence.com`, NextRequest/GovQA (request-and-fulfill), NACOLE (index).

## 7. Cases already processed (results)

**Mark Johnson OIS (req 78826, Montpelier PD)** — 2 bodycams (~66 min each).
`zip_triage` found an impulse volley @7:42; `find_shooting_pov` scored **0.32 `foot_pursuit`** ($0.0016).
Pipeline works, but not a strong shooting-POV winner. Worth a human spot-check at 7:42, low priority.

**Sarah Kolb & Cory Gregory Interrogation (req 181169)** — the standout bundle: 1 mp4 + 9 mp3
interrogation calls (Adrianne Reynolds murder, 2005). Transcribed (local whisper) → `pipeline4_score`.
**Verdict: HOLD** (narrative 30, conf 0.7). Pass1 found 6 moments incl. CRITICAL *"subject admits
coaching Corey on what to tell police"* + an account contradiction. HOLD (not PRODUCE) because
**single artifact type + low moment density** — the verdict itself recommends adding "footage or case
files." → Good candidate to complete via the pivot (transcribe its BWC mp4, add case files, re-score).

---

## 8. Open work / next steps

1. **Complete the Kolb case** → transcribe `CALL_1_2.mp4` into the same case_id + pull Adrianne
   Reynolds case files; re-run `pipeline4_score` → likely flips HOLD→PRODUCE.
2. **Process the video keepers** → download + `zip_triage` the Joliet / Miami Beach / Camden cases.
3. **Build the pivot as a real stage** → for SB1421 hits, auto-query agency portals (NextRequest /
   GovQA) for the fuller doc release; for video, extend the CIB-YouTube/AB748 intake.
4. **Wire mlx-whisper** into `pipeline3_transcribe.py` as a backend (GPU, ~10x faster than the
   faster-whisper CPU path) for larger audio batches.
5. **Optional:** add negative keywords (`cia`, `declassification`, `kubark`) to filter the
   CIA/historical noise the "interrogation"/"interview" terms drag in.

---

## 9. Quick smoke test (verify the env works)

```bash
source .venv/bin/activate
# A) MuckRock auth + a 1-term harvest
python discovered_cases/muckrock_harvest.py --terms "body camera footage" \
  --per-term 5 --min-score 0 --out /tmp/smoke.json
# B) confirm OpenRouter key loads
python3 -c "import sys; sys.path.insert(0,'discovered_cases'); \
  from muckrock_harvest import _read_env_file; k=_read_env_file().get('OPENROUTER_API_KEY',''); \
  print('openrouter key ok:', k.startswith('sk-or-'))"
```
If (A) prints `[keep ...]` lines and writes JSON, and (B) prints `True`, the environment is good.
