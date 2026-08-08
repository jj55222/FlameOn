# EDP contract — addendum 2026-08-08 (BWC-spine editorial standard)

Source: `~/Documents/Codex/2026-06-26/v/outputs/kyle_gray_production_20260808/`
`KYLE_GRAY_V3_CODEX_EDITORIAL_REBUILD_HANDOFF.md` — operator-endorsed. The Kyle Gray v3 act
structure is case-specific; everything below GENERALIZES to every longform production and
extends `EDP_CONTRACT_2026-08-06.md`. Machine-checked pieces are marked [GATE] — taste_gate now
enforces them pre-render (`event_footage_carries_runtime`, `document_dwell_max`,
`no_vo_over_commands`, all on_fail=REVISE).

## Central-event rule (revised by the COPA-1076 pre-render audit, 2026-08-08)
Clear visual coverage of the central event is PREFERRED, not a hard gate. An occluded event
still qualifies when it is captured on an original camera track and the surrounding footage
makes the approach, commands, gunfire, reactions, custody, and outcome narratively
understandable. The editor must search every plausible camera for a clearer angle before
accepting the occlusion — and the camera map must document the search. Selection verdicts are
story-strength audits, not visibility vetoes.

## Source hierarchy (binding order)
1. Full, continuous BWC moments — primary narrative and emotional material.
2. Other synced BWC/ICV — only when they add geography, causality, clearer action, a reaction,
   or a contradiction.
3. Original radio/911 audio — stakes, coordination, what officers knew at the time.
4. Concise narration — bridges facts, setup/payoff, explains what footage cannot.
5. Documents — substantiate a precise assertion, display an exact finding, or introduce a
   contradiction that immediately pays off in footage/audio.
6. Family statements / non-footage records — only if they materially change understanding of an
   on-camera sequence. Written statements are DOCUMENTS, not interviews; never structure an act
   around "interviews" without actual interview video in the inventory.

## Runtime is earned [GATE partly]
- Runtime comes from compelling audiovisual material, never packet size. If the BWC inventory
  cannot carry the target length without repetition: shorten, or reclassify as short/segment.
  Do not compensate with documents.
- Targets: 70-85% direct event footage; 10-20% narration over relevant moving footage;
  <=10-15% documents/maps/static combined [GATE runtime_ratios]. No uninterrupted static run
  beyond ~8-12s without a strong reason [GATE max_static_dwell].
- Repetition only when the second use reveals a NEW detail or answers a planted question.

## Document-use gate (answer all four BEFORE any document beat)
1. What exact assertion does this document substantiate?
2. What question from the BWC does it answer?
3. What on-camera or audible payoff follows it?
4. Can narration deliver the same fact faster over relevant footage?
No concrete answers to 1-3 → cut the section. Yes to 4 → narrate it; keep the document as a
short highlighted citation, not the visual centerpiece.

## Multicamera grammar
- Begin each event with ONE intelligible hero POV; the encounter must be understandable in real
  time from a continuous primary POV before any analytical replay.
- A camera switch must reveal new information — never switch because another angle exists.
- Crop court-prepared side-by-side exhibits to the intended native POV before recomposing.
  NEVER nest a split-screen source inside another split-screen.
- Two-up split only when simultaneous comparison matters; 3/4-up grids are a brief analytical
  beat (~3-6s), never the lead. Exclude dark/obstructed/duplicate feeds from grids.
- Sync all cameras by shot waveform; verify at least two impulses for drift.

## Narration jobs + protected zones [GATE partly]
Narration does exactly one of: ORIENT (fact the camera can't show), CONNECT (earlier line →
later consequence), COMPLICATE (documented contradiction/uncertainty), RESOLVE (official
finding + its limits). It never describes visible action, reads documents at length, or
manufactures suspense. Every factual line carries a page/paragraph or source-timecode citation
in the production trace. Protected no-VO zones around clearest commands, credited warnings,
gunfire, immediate reactions [GATE no_vo_over_commands].

## Music contract
- Original BWC, radio, commands, gunfire, immediate aftermath: NO music (rare transition
  exceptions only). Music lives in narration-heavy connective passages.
- Restrained investigative bed; never heroic/sentimental/trailer cues.
- Mix ~15-20 dB below narration; duck further under any original dialogue; fade OUT before
  consequential original audio. A music bed must never bridge across the central encounter.
- Silence and environmental sound beat constant scoring.

## On-screen design
- Approved word-for-word subtitle style; no process language (ASR/exhibit/workflow terms) in
  audience-facing renders (already gated: `no_trace_language`).
- Evidence labels identify source and date, not editing process.
- DR spin-and-unfurl CTA in longform: 2-3 placements (after the hook, near midpoint, near
  close) — never on a fixed short interval.

## Pre-render acceptance package (required before any heavy longform render)
1. BWC inventory — every camera: duration, POV role, visual quality, unique contribution, sync status.
2. BWC spine — exact source-relative windows forming the continuous narrative, search → aftermath.
3. Support map — every narration claim → page/paragraph or timestamp.
4. Document ledger — each document beat: its assertion, the BWC question it answers, its immediate payoff.
5. Cut plan — act-by-act runtime with projected percentages (BWC / narration-over-footage / documents / graphics).
6. Music map — exact ranges where music appears and why.
7. Exclusion list — supplied evidence intentionally omitted for lacking an on-camera payoff.
8. Proof stills — hero POV, any split screen, document treatment, captions, CTA placement.
9. Hydration result — every video exhibit CITED in the investigative record (surveillance,
   cellphone, news, ICV, 911/OEMC, interviews): located, downloaded, or explicitly recorded as
   unavailable. Never substitute report screenshots for missing footage.
10. Action anchor — first/last shot impulses verified against burn-in clocks on at least two
    cameras.
11. Dialogue anchors — every used excerpt audibly reviewed with verbatim captions;
    `[unclear audio]` where uncertain. Raw ASR NEVER reaches audience-facing captions
    ("12/12 transcribed" is discovery status, not caption approval).
12. Redaction map — exact frames/windows for plates, blood, the subject, and civilians.
    Standing visual rule: NO visible blood; blur the subject whenever dead or visibly bleeding;
    lift the blur the moment the subject leaves frame or the source is already redacted.

## Definition of done (longform)
Viewer sees meaningful BWC within the opening seconds and stays primarily in event footage; the
final encounter is understandable from a continuous primary POV before multicam analysis;
documents clarify footage rather than replace it; every evidentiary section has an on-camera or
audible payoff; no nested/split-source artifacts; music never competes with narration or
original audio; official disposition stated without implying unsupported fault; runtime earned
by distinct story material.
