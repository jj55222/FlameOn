# PRODUCTION ORDERS — 2026-08-08

> Fresh batch. The 2026-08-06 batch is CONSUMED (see PRODUCTION_ORDERS_20260806.md). NEXT_CUTS
> READY and BENCH are empty — this file is today's feed. New work flows through the EDP contract
> (`FlameOn-remotion/EDP_CONTRACT_2026-08-06.md`).

Every ORDER below has **all five gates closed**, including a frame-level look at the central
event. If an ORDER still doesn't work at the timeline, say WHY in one line in `## RETURNS` —
that reason tunes the selector; silent substitution does not.

Binding: BWC of the central event is the hard gate (secondary evidence strengthens, never
replaces). Never select outside this file or NEXT_CUTS.md. Vet sheets now live at the DURABLE
path `.tmp/vet_sheets/` (FlameOn-main), not session scratchpads — the 08-06 sheet links are dead,
these are not.

## ORDER 1 — `copa_2021_0001076`  (OIS near 2500 W. 46th St, Chicago — 2021-03-25 ~17:03 CDT)

**PRODUCTION-READY** (all gates closed 2026-08-08 16:00)

| gate | status | evidence |
|---|---|---|
| 1. BWC exists | YES | 12 files: `Log #2021-0001076 Vimeo BWC 1-12.mp4`, 3.7 GB, all transcribed (12/12 in `d2/transcripts/`) |
| 2. central event on BWC | YES | Sustained exchange of gunfire WITH police, on tape across cams: radio via BWC "shots fired and shot returned by the police" (BWC 10 @219-227s, BWC 11 @220-228s); live engagement "He's just shooting right now" (BWC 3 @165s, BWC 9 @277s); command cluster t0=324.1s in `BWC 8` — "Put your hands up, dude." @324.1s, "We don't have to shoot you." @325.7s/@327.7s, "He's shooting." @336.2s (12 hits/120s); takedown "Don't move" cluster BWC 7 @745s, BWC 5 @845s |
| 3. central event visually understandable | **YES** | DAYTIME alley engagement, Axon Body 3 burn-in clocks on every frame (2021-03-25 17:03-17:05 -0500): officers at cover along garage lines and behind a Ford SUV, subject VISIBLE mid-alley (face pre-blurred by COPA in release), geometry followable frame to frame. Restraint/aid phase confirmed on BWC 7 (17:10-17:12). Sheets: `.tmp/vet_sheets/copa_2021_0001076_bwc8_event.jpg`, `.tmp/vet_sheets/copa_2021_0001076_bwc7_restraint.jpg` |
| 4. secondary footage | — | 0 dashcam, 0 interview recordings (registry); radio traffic is embedded in BWC audio |
| 5. support | STRONG | 11 PDFs on disk: **FSR + `Nonconcur.pdf` (Superintendent non-concurrence with COPA's finding — institutional contradiction, Lane-B2 multiplier)** + Original Case Incident Report + Tactical Response Reports 1-7 (multi-officer force). Separate 911/radio files: no |

- basket: `.tmp/copa_2021_0001076` — 12 BWC (3.7 GB) in `video/Video/`, 11 PDFs in `docs/`, transcripts in `d2/transcripts/`
- **GRAPHIC-CONTENT FLAG:** the BWC 7/5 aid phase (17:10-17:12) shows large blood pools —
  publication requires aggressive redaction/blur of that window; observe `no_blur_after_redaction`
  taste rule (blur must not persist once the subject is redacted/framed out).
- **CLOCK ANCHOR:** legible Axon burn-ins on every frame — anchor EDP on burn-in clocks
  (17:00-17:05 exchange window, 17:10+ aid). Exact muzzle-flash frames not pinned per-cam in this
  vet; EDP authoring should locate them inside the anchored window (BWC 8/9 primary).
- ledger at generation: known=False longform_done=False shorts_done=False blocked=False
- **before cutting:** `python .tmp/preprocess_queue/produced_ledger.py --check copa_2021_0001076`
- **after mastering:** `--mark copa_2021_0001076 --scope longform|shorts --evidence <path>` (refreshes NEXT_CUTS immediately)
- publication gates (human, always open): redaction review of any doc image · independent status check · no fault claim without cited disposition · plate `MP 15885` visible in BWC 8 frames — blur

## BENCH — `copa_2020_0003466`  (OIS at/near 25th District, Chicago — 2020-07-30 ~14:33Z)

**NOT ORDERED — one vet step short.** Do not cut without closing gate 3 on the firing moment.

- On disk NOW: 12 BWC (1.3 GB, transcribed 12/12 — one `no_speech` bug44 marker on BWC 9),
  17 docs incl. FSR + **Concurrence** + **Arrest Report + Court Orders** (subject survived —
  arrest + prosecution lane; strong outcome-card material).
- What's confirmed: audio anchor `BWC 4` @12.1-38.3s ("Back to the station, shots fired at the
  police!" / "Shots fired, drop your weapon!") but that cam is OBSTRUCTED (officer's arm across
  lens) through the window — sheet: `.tmp/vet_sheets/copa_2020_0003466_bwc4_event.jpg`. Arrest
  arc IS clearly visible daytime on `BWC 1` @14:34-36Z (cuffing on pavement → walk to cruiser) —
  sheet: `.tmp/vet_sheets/copa_2020_0003466_bwc1_event.jpg`. "Shot fired by the police" @150s on
  BWC 8; "Stop reaching" during cuffing (BWC 1 @149s, BWC 2 @51s).
- **Next step to close gate 3:** frame-scan the low-dialogue cams (BWC 5, 6, 11, 12) around
  T14:33Z for the firing moment:
  `ffmpeg -ss 0 -i "Log #2020-0003466 Vimeo BWC 5.mp4" -vf "fps=1/8,scale=480:-2,tile=4x4" -frames:v 1 .tmp/vet_sheets/copa_2020_0003466_bwc5.jpg`
  (repeat per cam). If the shooting is visible on any: promote to ORDER. If not: route as
  short/segment feedstock only (Bounsom precedent, see 08-06 ORDER 2 format constraint).

## RETURNS

(record one-line reasons here — they tune the selector)
