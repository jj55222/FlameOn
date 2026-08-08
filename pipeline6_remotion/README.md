# Pipeline 6 Remotion Lane

Case-generic polished rendering for validated FlameOn paper edits. The lane is a
read-only consumer of bakeoff contracts, paper-edit manifests, transcripts, and
original case media.

```bash
pipeline6_remotion/render_cut.sh <case_id> <run_dir>
```

The default output is `<run_dir>/remotion/<case_id>_remotion.mp4`. The command
re-extracts clean clips from original source paths; it refuses media inside
rough-render `segments/` and `cards/` folders.

For local smoke tests:

```bash
pipeline6_remotion/render_cut.sh <case_id> <run_dir> --smoke-seconds 60 --low-res
```

`props_from_paper_edit.py` preserves paper-edit captions and per-event
`capOffset`, or derives segment-level captions from the case transcripts when
needed. Missing narration, captions, lower thirds, document assets, and document
highlight metadata degrade to absent and are recorded in the props JSON.

## Voice-over policy

The VO attachment stage exists but is disabled by default. It never makes an
ElevenLabs or other network call. A future adapter must require all three: an
explicit key, `ENABLE_ELEVENLABS=1`, and `vo_stage.py --enable`. Existing local
VO files can then be attached and source audio will duck while they play.
