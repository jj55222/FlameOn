# Handoff for Codex — Find the Shooting Across Hundreds of Files, Cost-Effectively

You're being asked to help solve one problem: **locate the officer-involved-shooting
moment inside a ~40 GB FOIA evidence bundle of 266 videos — without brute-forcing
expensive AI over hundreds of multi-hour files.** This doc is the goal, the measures
already taken, what was found, and where to push next. Deeper detail lives in
`.tmp/HANDOFF_OIS_2026-06-25.md`; environment setup is in `MIGRATION.md`.

---

## 1. The goal (and the constraint)

- **Find the moment of the shooting** (which file + timecode) across **266 videos**
  (dashcams, bodycams, aerial, fixed surveillance, evidence clips, bystander phone
  video) packed in one **40 GB zip**: `C:\Users\Diner\Downloads\2017-289964.zip`.
- **The hard constraint is COST.** You cannot transcribe/vision-scan every second of
  hundreds of files — that's hours of GPU and real API dollars. The design goal is a
  **funnel**: free local signals decide *where* to look, and a *cheap, capped, paid*
  step confirms *what* is there. A prior project overspent **$57** on Brave by running
  an uncapped loop — never again. Every paid call is metered and hard-capped.
- The case (per docs): a **fatal OIS** — rifle from a 2nd-floor balcony, an officer
  shot, civilian CPR, ~16–30 rounds, Auburn Blvd / Ramada, Sacramento, ~noon 2017.

---

## 2. The cost-effective architecture (the measures taken)

**The funnel — keep the expensive signal LAST and BOUNDED:**

1. **FREE local triage decides where to look.** Disk-safe streaming from the zip
   (never unzip the 40 GB whole; extract one file → scan → delete → next, so disk
   never holds two POVs). Two free rankers:
   - *Audio impulse / gunshot-volley* (`zip_triage.py`) — full audio of each file.
   - *Visual motion-salience* (`motion_triage.py`) — the violent moment by frame
     motion, decoded tiny+gray, no audio.
2. **CHEAP, capped paid vision confirms what it is.** `find_shooting_pov.py` points
   **Gemini 2.5 Flash** (via OpenRouter, OpenAI-compatible) at only the pre-filtered
   candidates' loud/violent windows — **~$0.001 per POV, measured live**. Bounded
   three ways: a per-candidate frame cap, a candidate cap, and a **hard USD budget
   that aborts before the next call**. Real per-call cost is read back from OpenRouter
   (`extra_body={"usage":{"include":True}}`) and logged — spend is observed, not
   estimated.
3. **Everything checkpoints + resumes.** A crash or a budget-stop never re-pays for
   work already done (per-file JSON checkpoint, reload-and-skip on restart).

**Cost outcome:** the entire 266-file search cost **≈ $0.13** of paid vision. Audio
and motion passes are $0 (local).

---

## 3. The tools (all in `pipeline3_audio/`, 76 tests green)

| File | Role | Cost |
|---|---|---|
| `pov_triage.py` | primitives: ffmpeg resolution, audio salience/impulse scan, AXON clock OCR | free |
| `zip_triage.py` | disk-safe audio triage from the zip (gunshot-volley rank), clock-OCR, checkpoint/resume | free |
| `find_shooting_pov.py` | **audio-rank → cheap Gemini-Flash vision**, cost-metered, **USD-budget-capped**, resumable | ~$0.001/POV |
| `motion_triage.py` | **sound-free** motion-salience (violent moment), full timeline, checkpoint/resume | free |
| `motion_sync.py` | **clock-free** cross-camera alignment (the shared "flinch") → shared moment + each cam's local timecode | free |
| `aerial_telemetry.py` | the air-unit HUD = the clock the dashcams lack: OCR timestamp + GPS target track | ~free |
| `vision_scan.py` | the Gemini-Flash vision backend + event taxonomy (firearm_discharge, officer_down, …) | paid |

Run any with `--help`. Example (the main finder):
```bash
python pipeline3_audio/find_shooting_pov.py \
  --zip "C:/Users/Diner/Downloads/2017-289964.zip" \
  --triage .tmp/ois_289964/zip_triage_full.json \
  --top-n 136 --budget-usd 0.50 --out .tmp/ois_289964/pov_vision.json
```
Tests: `cd pipeline3_audio && python -m pytest -q` (≈76 pass).
Env: see `MIGRATION.md` — `.env` needs `OPENROUTER_API_KEY` (+ others); `pip install -r requirements.txt`; torch installed separately.

---

## 4. Why this problem is hard (what defeats the obvious approaches)

- **No synchronized clock.** AXON stripped/normalized timestamps; the dashcam overlay
  is gone. So you cannot line cameras up by time directly. (The *aerial* footage is the
  exception — its HUD burns in a timestamp + GPS; see `aerial_telemetry.py`.)
- **The AXON muted pre-event buffer.** Each bodycam has a ~30–60 s MUTED buffer before
  activation, so the firing officer's own camera is often **silent at the trigger-pull**
  and ranks LOW on audio. → audio alone is insufficient; motion-salience exists to catch
  exactly this (the cam is silent but visually violent).
- **The audio impulse proxy is confounded.** A sharp transient ≠ gunshot: car doors,
  a garage door, and **magazine-loading clicks** (evidence handling) all spike it. The
  top audio "volley" was a parking-garage false positive.
- **Vision says "shots_fired" for bullet HOLES.** The VLM tags evidence (bullet holes
  in a door, a spent casing, a shattered car window) as `shots_fired`. You must
  distinguish *live discharge* from *aftermath/evidence documentation* — the prompt in
  `find_shooting_pov.py` was sharpened to return `scene` for evidence handling.

---

## 5. What was already run, and the finding (coverage is now complete)

| Modality | Coverage | Result |
|---|---|---|
| Audio impulse | **266 / 266 videos, full audio** | **no gunshot volley anywhere** |
| Motion-salience | **136 / 136 large, full timeline** + 130 small | every top moment = a dashcam *driving* / an arrest |
| Vision (Gemini Flash) | all real small clips + the 136 at their loud window | response / pursuit / **aftermath / evidence** only |
| Aerial (5 files: 138, 166, 17-1067×2, 17-1068) | HUD + frames | air unit over the scene = **aftermath** (person down, crashed car) |

**Conclusion (now properly supported, not premature): no camera in the 266 captured
the live gunfire.** The footage is entirely police *responding* (dashcams driving in),
foot pursuit, K9, arrests, ambulance, aerial scene-management, **bystander phone video
of the aftermath**, and crime-scene evidence. The clincher: **bystander phones have no
muted buffer** — if anyone had filmed the shooting, the audio would carry a clear
volley. None does. The shots happened *before* any of these cameras were capturing them.

---

## 6. The open threads (where Codex can add value)

1. **⭐ Resolve incident IDENTITY (highest value, non-video).** The footage repeatedly
   shows **"El Camino Ave"** (aerial HUD) + a crashed muscle car + a downed person,
   while the docs say **"Auburn Blvd / Ramada"** + balcony rifle. The video bundle may
   be a **different or mixed incident**. Resolve by **OCR-ing the 1793-page packet**
   (`17-289964 .pdf`, scanned) — it holds the **CAD/radio "shots fired" timestamp** and
   the **camera→officer map**, which would (a) tell you which gunshots and (b) possibly
   name the exact camera. This turns a video *search* into a *lookup*. (`pypdf` +
   `pytesseract`/`easyocr`; or an LLM doc-extractor — see `doc_extract_llm.py`.)
2. **Nail the aerial wall-clock.** `aerial_telemetry.parse_timestamp` recovers minute
   resolution but the **hour digit is OCR-split** (`17 18:09` → caught `18:09`). Tighten
   the top-left HUD crop + upscale, get absolute HH:MM:SS → anchor the whole timeline
   (the aerial advances 1:1 with video, verified).
3. **Free acoustic gunshot CLASSIFIER.** Replace the impulse proxy with **PANNs**
   (`panns_inference`, installed) or YAMNet — a real "Gunshot, gunfire" probability,
   not a sharp-transient heuristic. Decisive confirmation that there's truly no live
   gunfire audio (or finds it if there is). Local, free.
4. **Multi-window vision on the 136.** Vision saw only ONE window per large file;
   motion covered the rest for *violent* moments. A distant, low-motion discharge is the
   only thing that could slip both — cheap to close (a few $ at Gemini-Flash rates).
5. **Weigh the negative result.** It is entirely plausible the live shooting **is not in
   the video bundle** (cameras arrived after). Don't assume it must be there; the packet
   (#1) is the way to confirm.

---

## 7. Cost discipline — non-negotiable rules

- **Free local first; paid last and capped.** Never run paid vision/transcription over
  hundreds of files unfiltered.
- **Always meter real spend.** OpenRouter returns true cost via
  `extra_body={"usage":{"include":True}}` → accumulate, and **abort before exceeding a
  hard USD budget**. See `find_shooting_pov.CostMeter`.
- **Gemini 2.5 Flash** is the cheap vision model (~$0.001/POV downscaled to 640px). Don't
  reach for a pricier model without reason.
- **Brave is the only other paid API** and has a persistent `$`/quota guard
  (`brave_quota.json`) after a prior $57 overspend. Respect it.
- **Disk-safe + resume always** when touching the 40 GB zip: extract one → scan →
  delete; checkpoint after each file.
