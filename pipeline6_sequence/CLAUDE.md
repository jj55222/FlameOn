# Pipeline 6 — Documentary sequencing & rendering

**This is where a scored case becomes a watchable video cut.** The editorial heart of FlameOn and
the pipeline you'll spend the most time in for EWU-level work. Deterministic assembly — no LLM does
the cutting; LLMs only author/shape narration. Root context: [/CLAUDE.md](../CLAUDE.md),
[/STATE.md](../STATE.md). Full runbooks: `.claude/skills/{flagship-longform,rawwalk-shortform}`.
Pre-render taste layer (gates, rules, maintenance contract): [TASTE_LAYER.md](TASTE_LAYER.md).

## Prereqs (always)
```
source ../.venv/bin/activate
eval "$(/opt/homebrew/bin/brew shellenv)"   # brew ffmpeg/ffprobe on PATH — silent failures otherwise
```
This ffmpeg has **no libass/drawtext** — all captions/narration are rendered PIL→PNG→overlay.

## Tools (what each does)
| Script | Role |
|--------|------|
| `make_documentary.py` | **Orchestrator.** Chains the whole flagship (or rough-cut) recipe. Dry-run by default; `--run` executes. Run it with no `--run` to print the exact argv of every sub-step. |
| `rawwalk.py` | Path A: builds the chronological raw-bodycam-walk blueprint (auto primary-cam, action peak, cold-open teaser, phase narration). |
| `blueprint.py` | Path B: authors the longform production blueprint (acts, theses, beats). `--auto-anchor` = salience × camera-convergence; `--target-runtime SEC` auto-fits. |
| `blueprint_shape.py` | LLM narration pass over a blueprint. `--analysis --grammar <profile>` = EWU/Dr.Insanity analytical voice. Injects `CASE_FACTS` + `audit_narration()` fact-check rail. Paid. |
| `bridge_verdict.py` | Unions `beat_miner` moments (audio) + doc moments into a P4-shaped verdict. `bridge_verdict.py <moments.json> <out_verdict.json> [transcript_dir]`. |
| `incident_anchor.py` | `derive_anchor()` — auto-derives the incident time (used by `blueprint --auto-anchor`). Has `--selftest`. |
| `render_blueprint.py` | Renders a (shaped) blueprint → mp4 + paper-edit. `--no-audio-aware` for chronological cuts. `--target-runtime` / `--min-clip-sec` control length. |
| `render_rough_cut.py` | Minimal vertical-slice rough cut straight from a P4 verdict + media dir (MVP path). |
| `judge.py` | Release gate. `--gate` exits 3 (HOLD) on REWORK/unsourced footage, else 0. `--mock` = free deterministic gate. |

## The two paths (pick by case shape)
- **Path B — flagship longform** (doc-driven, narrated, EWU target): `make_documentary.py --flagship`.
  → skill `flagship-longform`.
- **Path A — rawwalk short** (bodycam-driven, chronological replay): `rawwalk.py` → `render_blueprint`.
  → skill `rawwalk-shortform`.

## Load-bearing fixes — DON'T regress these
- **`render_blueprint --no-audio-aware` is REQUIRED for chronological cuts.** The audio-aware snap
  drags establishing clips to the loudest audio (gunfire) → puts the shooting at the front.
- **Incident anchor.** Timeline phases anchor on the earliest *trusted* stamp; wrong anchor plays the
  story backwards. Auto-anchor uses convergence (not raw salience argmax — the talky investigation
  cam outscores the actual incident). Override: `timeline_build --incident "YYYY-MM-DD HH:MM:SS"` UTC.
- **Narration grounding is defamation-critical.** Ground on the documented OUTCOME (`doc_extract` →
  else tier1 verdict), never the transcript alone. `audit_narration()` quarantines invented
  civilian-detainee framing + ungrounded accusations. Never disable the rail.
- **PEAK = force-ONSET cues** ("shots fired", "drop the knife", "taser"), NOT bare "shot"/"shooting"
  (recurs in medical aftermath, mis-places the peak).
- **Only trust `axon_ocr`-stamped cams for chronology** (metadata stamps can be export dates).
- **Clip audio:** BWC audio is AAC — extracting with `-c copy` to `.mp3` = silent/invalid; re-encode
  `-vn -c:a libmp3lame`.
- **`source_url` per transcript** must be the absolute media path, or same-type-no-person refs
  collapse onto the first file.
- **`resolve_clip_overlaps` is a global forward-only pass**; `cold_open` teaser clips are EXEMPT
  (else the replay-trim shoves the whole walk past the teaser).
- **`blueprint_shape --analysis` needs `--max-tokens 12000`** — verbose; under ~7k it truncates →
  JSON parse fails → silently degrades to skeleton (surfaced as `edit_report.parse_error`).
- **`validate_edit` refuses cuts >50% of beats** (a grammar steered "cut aggressively" once emptied
  the cut). Omission never deletes everything.

## Tests
`python -m pytest -q` in this dir (~107 P6 tests pass). Keep them green.

## Open editorial tuning (see /STATE.md)
Narration top-slide lingers whole clip (wants timed ~6-8s overlay); post-teaser runs long; caption
font ~30px small (→~40); silent drive-up stretches unfilled without a 911 call; **TTS still needed**
to actually hear the EWU vs Dr.Insanity voice difference (3s cards too short for analytical lines).
