
## GPU priority protocol (operator rule 2026-07-16: RENDERING > TRANSCRIPTION)
Video production owns the GPU/Metal device. If your session will run ANY Metal/MLX work
(mlx-whisper, Vision OCR) or a long render, claim priority first:

    touch /Users/jmoney/FlameOn-main/.tmp/RENDER_PRIORITY.lock     # at session start
    rm -f /Users/jmoney/FlameOn-main/.tmp/RENDER_PRIORITY.lock     # when done

Background preprocessing lanes (transcription queues) check this lock and any live
`remotion render` process, and yield at file boundaries (≤ ~6 min). Locks older than 6h are
treated as stale (crashed session) and ignored — refresh with `touch` on long sessions.
If you hit "Metal device unavailable": the background lane is mid-file; wait ≤6 min and retry.

## Taste layer (P6 pre-render gates) — maintenance contract
`pipeline6_sequence/TASTE_LAYER.md` is the contract for this layer: component map, rule-adding
workflow, promise/payoff annotation semantics, invariants. Short form: rules live in canonical
`pipeline6_sequence/TASTE_RULES.json` (two `discovered_cases/` mirrors — `cp` after editing, never
edit a mirror); check handlers in `taste_gate.py` `_CHECKS`; the thesis gate reads the BLUEPRINT,
the taste gate reads the PAPER EDIT; gates are craft-only, never worth (WORKFLOW_FREEZE holds).
Any change: full `pipeline6_sequence/tests/` green, then commit on this branch — never leave the
layer as uncommitted working-tree state.
