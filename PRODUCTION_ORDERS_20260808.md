# PRODUCTION ORDERS — 2026-08-08  (rev 2, post-audit)

> **AUDIT STATUS (2026-08-08): CONDITIONAL GREENLIGHT — build a corrected EDP before rendering.**
> **Do not render directly from this order.** Pre-render editorial audit (operator-endorsed) is
> the controlling document for ORDER 1's editorial plan; this file is SELECTION + basket state.
> The audit's 10 pre-render gates + the BWC-spine standard
> (`EDP_CONTRACT_ADDENDUM_20260808.md`) must be satisfied first. rev 1's
> "institutional contradiction" framing is CORRECTED below.

Binding: BWC of the central event is the hard gate (secondary evidence strengthens, never
replaces). **Revised central-event rule (audit):** clear VISUAL coverage is preferred but not a
hard gate — an occluded event still qualifies when captured on an original camera track and the
surrounding footage makes approach, commands, gunfire, reactions, custody, and outcome
narratively understandable; the editor must first search every plausible camera for a clearer
angle. Never select outside this file or NEXT_CUTS.md. Vet sheets: durable `.tmp/vet_sheets/`.

## ORDER 1 — `copa_2021_0001076`  (OIS near 2500 W. 46th St, Chicago — 2021-03-25 ~17:03 CDT)

**CONDITIONAL — EDP required** (selection gates closed 2026-08-08; editorial plan = the audit)

| gate | status | evidence |
|---|---|---|
| 1. BWC exists | YES — **32 in registry, 12 on disk at vet** | `Log #2021-0001076 Vimeo BWC 1-32` (registry, Vimeo player URLs). BWC 13-32 hydration launched 2026-08-08 (`hydrate_1076_extra.log`); some "BWC"-numbered exhibits may actually be ICV/surveillance/news video under COPA's generic numbering |
| 2. central event on BWC | YES | Officer shot on BWC audio ("I'm shot") + urgent location/ambulance calls; subject keeps firing during mobile alley containment ("shots fired and shot returned by the police" BWC 10 @219-227s, BWC 11 @220-228s; "He's just shooting right now" BWC 3 @165s, BWC 9 @277s); command cluster t0=324.1s BWC 8 ("Put your hands up, dude" / "We don't have to shoot you" / "He's shooting", 12 hits/120s); weapon recovery + restraint ("Don't move" BWC 7 @745s, BWC 5 @845s) |
| 3. central event visually understandable | YES | DAYTIME alley, Axon Body 3 burn-ins every frame (17:03-17:05 -0500): officers at cover along garages + behind SUV, subject visible mid-alley (COPA pre-blurred face), geometry followable. Sheets: `.tmp/vet_sheets/copa_2021_0001076_bwc8_event.jpg`, `_bwc7_restraint.jpg` |
| 4. secondary footage | **HYDRATED + IDENTIFIED 2026-08-08** | All 32 registry files on disk and classified by frame-ID (sheets `.tmp/vet_sheets/copa_2021_0001076_{short_exhibits,mid_exhibits_a,mid_exhibits_b,long_runs}.jpg`): **ICV = BWC 16** (22.5min, "L M1M2" overlay, cruiser POV straight down the alley); **cellphone = BWC 17** (portrait, elevated over the alley); **residential surveillance = BWC 18-32 group** (multi-DVR: street1, Camera1-8, CAM6, Garage, CAMERA02) — several with ACTION in frame: Camera8/BWC 23 has the restraint-aid area in view (8min neutral elevated angle), CAM6/BWC 26 catches a person walking the alley (subject pre-contact?), Camera7/BWC 30 shows an officer moving with weapon drawn, Garage/BWC 27-29 cover the SUV; **BWC 13-15** = three more Axon runs from ~17:09 (aid close-up, perimeter, driveway — post-exchange wave, Houston-candidate cams). **NOT RELEASED by COPA (recorded per audit): WGN footage of the final exchange, Home Depot surveillance, 911/OEMC as separate files, interview recordings.** Proceed BWC-led; never substitute report screenshots. DVR clocks disagree across cams (16:5x / 15:3x / 4:47PM) — reconcile with the Vision DVR stamper (`.tmp/autocut/_dvr_vision_stamp.py`) at EDP time |
| 5. support | STRONG — **corrected framing** | 11 PDFs: FSR + Original Case Incident Report + TRR 1-7 + Nonconcurrence. **The nonconcurrence is a PENALTY dispute after agreed sustained violations, NOT a dispute about the shooting**: COPA found the deadly force within policy; sustained 2 allegations vs Officer Houston (incl. failure to timely activate BWC) + 1 vs Officer Kwa (discharge notification); CPD AGREED all three should be sustained and disputed only the penalty (short suspensions vs "Violation Noted"). A ~45-75s coda, not the thesis |

**Working thesis (audit):** after a loss-prevention agent and a police officer were shot,
responding officers entered a daylight alley where the gunman kept firing; their cameras captured
the containment, repeated attempts to make him surrender, and the final exchange. The shooting
was found within policy — and the investigation separately sustained three reporting/camera
violations, with the later dispute only over punishment.

- basket: `.tmp/copa_2021_0001076` — ALL 32 video files on disk (~5.9 GB): 15 BWC runs + 1 ICV +
  1 cellphone + 15 surveillance clips; 11 PDFs; transcripts: BWC 1-12 done, 13-32 transcribing
  2026-08-08 (triage tier; surveillance will mostly VAD-skip as no-speech — expected)
- **KNOWN POV GAP (audit Risk 3):** Officer Houston's BWC was activated late — his direct POV of
  the shooting is absent. Camera map must state which officer fired, whether the discharge is
  visible from another camera, and what portion of Houston's POV is missing. A documented
  limitation and possible narrative beat — not permission to over-use documents.
- **ASR NOT CAPTION-APPROVED (audit Risk 5):** 12/12 transcribed is discovery status. Every
  excerpt used must be audibly verified; `[unclear audio]` where uncertain; raw ASR never
  reaches audience captions.
- **GRAPHIC (audit Risk 6):** aid phase (BWC 7/5, 17:10-17:12) has extensive visible blood.
  Standing rule: NO visible blood; blur the subject whenever dead or visibly bleeding; remove
  blur when subject leaves frame or source already redacted (`no_blur_after_redaction`).
- **RUNTIME (audit Risk 7):** provisional 9-11 min, subject to BWC spine audit. Ratios: 75-85%
  direct footage / 10-20% narration-over-footage / ≤10% documents. Penalty-dispute coda 45-75s.
- **Pre-render gates:** the audit's 10-item list (hydration result, camera map, BWC spine,
  action anchor verified on ≥2 cams against burn-ins, audibly-reviewed dialogue anchors,
  act/runtime map, document ledger, redaction map, music map, proof frames). If not supplied,
  RETURN the order rather than rendering.
- ledger: known=False longform_done=False shorts_done=False blocked=False
- **before cutting:** `python .tmp/preprocess_queue/produced_ledger.py --check copa_2021_0001076`
- **after mastering:** `--mark copa_2021_0001076 --scope longform|shorts --evidence <path>`
- publication gates (human, always open): redaction review · independent status check · no fault
  claim without cited disposition · plate `MP 15885` visible in BWC 8 frames — blur

## BENCH — `copa_2020_0003466`  (OIS at/near 25th District — 2020-07-30 ~14:33Z)

**Revised after audit camera scan: occlusion veto LIFTED; held for a STORY-STRENGTH audit.**

- The audit scanned the low-dialogue cams: BWC 5/6 begin useful coverage after the firing window
  (vehicle/interior/response); BWC 11/12 are fixed roadway/vehicle views; no camera offers a
  clearly superior view of the shooting. BWC 4 carries the consequential audio ("shots fired at
  the police", "drop your weapon", officer-down call, ambulance request) behind an arm-blocked
  lens; BWC 1 cleanly captures the arrest/cuffing.
- Under the revised central-event rule this case is NOT disqualified: the event is captured and
  narratively understandable. Promote to ORDER if the complete chronology, original audio,
  officer-down stakes, arrest arc, and documented outcome (FSR + Concurrence + Arrest Report +
  Court Orders — subject survived, prosecution lane) sustain a distinct BWC-led longform.
  Route to shorts/segment feedstock if they do not.
- On disk: 12 BWC (1.3 GB, transcribed 12/12 — BWC 9 = `no_speech` bug44 marker), 17 docs.
  Sheets: `.tmp/vet_sheets/copa_2020_0003466_bwc4_event.jpg`, `_bwc1_event.jpg`.

## RETURNS

(record one-line reasons here — they tune the selector)
