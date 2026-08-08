# Pipeline 6 Remotion - Merge Ready

Branch: `codex/p6-remotion`

Target: `p6-documentary-assembly`

Status: ready for review; intentionally not merged.

## What is included

- `pipeline6_remotion/props_from_paper_edit.py`: generic validated-paper-edit,
  contract, transcript, document-asset, and original-media adapter.
- `pipeline6_remotion/src/Cut.tsx`: props-only Remotion composition with timed
  narration bands, `**bold-red**` emphasis, karaoke captions, per-event
  `capOffset`, lower thirds, real-waveform audio, document callouts, optional
  tactical maps, and disabled-by-default local VO/duck plumbing.
- `pipeline6_remotion/render_cut.sh`: one-command clean extraction and render to
  `<run_dir>/remotion/<case_id>_remotion.mp4`.
- `goals/ws4_remotion_smoke.py`: two-case props gate plus local short-render and
  zero-ElevenLabs verification.

## Verified locally

- TypeScript: `tsc --noEmit` passes.
- Python modules compile; `render_cut.sh` passes `bash -n`.
- `goals/ws4_remotion_smoke.py` exits 0.
- Props generated unchanged for 4S closed-loop (20 events) and Fanuel W1 (15
  events).
- The same render command produced a 30-second Fanuel proof.
- Full 4S Remotion render completed at 1280x720, 30 fps, H.264 + AAC:
  445.29 seconds, 247,832,330 bytes.
- Full-render spot check covers title, clean BWC, narration bands, karaoke
  captions, lower thirds, audio waveform, and outcome states without overlap or
  double-burned overlays.
- VO status and smoke report both record zero network/ElevenLabs calls.

## Local outputs

- 4S full cut:
  `.tmp/bakeoff/runs/4s_closed_loop/remotion/sdpd_12_07_2023_10500_4s_commons_drive_remotion.mp4`
- 4S smoke report:
  `.tmp/bakeoff/runs/4s_closed_loop/remotion/ws4_remotion_smoke.json`
- Fanuel proof:
  `.tmp/bakeoff/runs/fanuel_w1/remotion/sdpd_01_05_2025_4400_fanuel_street_remotion_smoke.mp4`

Install dependencies with `npm install` in `pipeline6_remotion/`. Exact runtime
versions are pinned in `package.json`; generated media and render outputs remain
ignored.
