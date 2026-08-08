# P6 editorial-taste layer

This layer shapes and gates a cut. It does **not** decide whether a case is
worth producing; selection and rerouting remain upstream.

## Inputs

- A persisted W1 contract (`d6_blueprint/<case_id>_contract.json`).
- One v1 reference treatment from `reference_cards/treatments/`.
- `TASTE_RULES.json`.
- The existing case artifacts, timeline, verdict, transcripts and documents.

`narration_grammar` and `reference_treatment` are intentionally separate:
grammar controls cadence/voice, while the treatment controls act order,
proportions, required evidence, promise/payoff and forbidden failures.

## Review-only flagship run

This produces a deterministic blueprint, thesis report, shaped blueprint,
final-equivalent paper edit, deterministic validation, and enum-only critic
report. It does not encode a video.

```bash
python pipeline6_sequence/make_documentary.py \
  --basket <case-basket> \
  --case-id <case-id> \
  --agency "<agency>" \
  --doc <case-document.pdf> \
  --flagship \
  --contract <case-basket>/d6_blueprint/<case-id>_contract.json \
  --reference-treatment standoff_tactical_longform_v1 \
  --judge-mock \
  --stop-at-paper-edit \
  --run
```

Omit `--judge-mock` to use the single live editorial critic. Add
`--thesis-model <model>` only when a live thesis-survivability read is needed;
the thesis gate is deterministic by default.

## Artifacts

- `d6_blueprint/<case-id>_blueprint.json`
- `d6_blueprint/<case-id>_thesis_validation.json`
- `d6_blueprint/THESIS_VALIDATION_<case-id>.md`
- `d6_blueprint/<case-id>_blueprint_shaped.json`
- `d6_cuts/<case-id>_paper_edit.json`
- `d6_cuts/VALIDATION_<case-id>.json`
- `d6_cuts/VALIDATION_<case-id>.md`
- `d6_cuts/<case-id>/<case-id>_judge.json`

Only when all pre-render gates pass and `--stop-at-paper-edit` is absent does
the final `render-blueprint` encoding step run.

## Gate semantics

- `REJECT` and `ABSTAIN` stop encoding.
- `REVISE` is emitted as a visible warning so the Winding Oak / Jason Norris
  usable floor remains reviewable rather than being discarded.
- Missing treatment evidence, an unsourced accountability disposition, or a
  contradiction that is merely topical cannot be disguised by narration.
- For shorts, `taste_gate.py --format shortform` enforces a visual story and a
  hook → context → payoff sequence; audio-only and blur-only configurations fail.

## Component map (maintenance — Codex + Claude)

| Piece | File | Role |
|---|---|---|
| Rules | `TASTE_RULES.json` (**CANONICAL, here**) | Machine-checkable taste rules; `check.type` selects the handler |
| Rule mirrors | `FlameOn-main/discovered_cases/`, `FlameOn-remotion/discovered_cases/` | Copies only — edit canonical then `cp` to both; editing a mirror silently does nothing |
| Craft gate | `taste_gate.py` | Pre-render checks over the paper edit; handler registry `_CHECKS` keyed by `check.type` |
| Thesis gate | `thesis_gate.py` | Deterministic: can the promised payoff be SEEN in bound evidence — reads the BLUEPRINT, pre-narration |
| Editorial references | `reference_cards/` (treatments / routes / moments) | Structure-only cards compiled into blueprints; no case content |
| Adapter tags | `render_blueprint.blueprint_to_paper_edit` | Emits the hooks the gate checks: `promise_payoff` `{id, role}` / `{payoff_for}`, `subject_redacted` / `body_removed` passthrough |
| Wiring | `make_documentary.py` | thesis gate → shaping → paper edit → taste gate, all before any encode |
| Upstream signal | `pipeline4_scoring/pipeline4_score.py` | `contradiction_present` / `contradiction_count` preserved for router + thesis gate |
| Contract fields | `schemas/contracts.json` | `disposition` + `disposition_source` — accountability conclusions need a source of record |

## Promise/payoff annotation (two grains)

**Act level:** an act carrying both `promise` and `payoff` tags its setup-function beat as the
promise and its reveal-function beat as the payoff (promise id = the act_id).

**Beat level (2026-08-08):** a standalone SETUP narration (`is_document` / no clip) whose text
matches `_SETUP_RE` ("kept asking", "one question", "his dying/final words", "what he said next",
"would later admit/reveal", "had one thing") promises `pp_<beat_id>`; its payoff is the **next
footage beat in the act**. `is_broll` is never the payoff — the act emitter re-sorts B-roll to the
front of the act, so a B-roll payoff could land before its promise, and B-roll isn't a moment.
Beat-level wins over act-level in `_promise_payoff_tag`.

Widening `_SETUP_RE`: add phrasings conservatively — a false promise only bites when real
exposition follows, but ordinary two-line VO must never trip the gate. Log additions in
`docs/plans/EDITORIAL_TASTE_PLAN.md`.

Known edges (accepted): the no-media narration fallback carries no beat_id/tag, so a promise whose
payoff clip fails to resolve reads as delayed — that timeline already contains a `gap` event, i.e.
the cut is broken anyway. Two stacked setups before one clip both fail — intended: a setup must
cut straight to its payoff.

## Adding a rule (the only supported path)

1. Route the critique note first: generalizes to every case → taste rule (here); tooling must do
   something new → `docs/plans/EDITORIAL_CAPABILITY_BACKLOG.md`; this cut only → the cut's
   revision notes, never the rule base.
2. Add the rule object to canonical `TASTE_RULES.json`; reuse an existing `check.type` when one fits.
3. New check type → new `_check_*` handler in `taste_gate.py`, registered in `_CHECKS`.
4. Tests in `tests/test_taste_layer.py`: one arrangement that FAILs the rule, one that PASSes.
5. `cp` canonical over both mirrors.
6. Full suite green, then **commit on this branch**. This layer lived a full day as uncommitted
   working-tree state in no git ref — one `git checkout -- .` from oblivion. Never leave it that way.

## Invariants (both agents)

- Gates are **craft-only**. Worth/selection lives upstream (WORKFLOW_FREEZE 2026-07-11). A gate may
  report a *direction* is uncuttable; it must never reject a case on worth or pick a new angle.
- Grep every consumer before changing an artifact's shape (adapter fields, rule schema, contract
  fields); never add a flag without its consumer.
- Tests: `python -m pytest tests/ -q` from `pipeline6_sequence/` on the repo venv
  (`FlameOn-main/.venv`). 160 passed as of 2026-08-08 — keep it green.
