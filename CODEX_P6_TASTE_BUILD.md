# TCDR — Pipeline-6 editorial-taste build (Codex work prompt)
Plan reconciled + verified 2026-07-26. Full plan: FlameOn-main/docs/plans/EDITORIAL_TASTE_PLAN.md

## The framing (why this work exists)
The system learned a visual grammar but not editorial taste — we optimized for successful **renders**
instead of successful **edits**. The root cause is structural:

**The blueprint is currently a generic chronological skeleton, not an editorial spine.**
`blueprint.py:320-337` builds acts from hardcoded constants (`_PHASE_ACT`/`_PHASE_FUNC`:
pre_incident → incident → aftermath) sized by beat-count proportionality. So a standoff, an
accountability story and a pursuit all receive the SAME three-act chronological shape. That is why
Winding Oak was a strong pick told flatly: nothing in the chain ever said "a standoff is told as
crisis hook → catalyst → threat picture → containment → tactical planning → attempt/regroup →
resolution." Events were laid end-to-end in time order and taste had nowhere to live except narration —
the layer least able to fix a story that is built wrong.

**Goal: promote the blueprint from structural spine to EDITORIAL spine — while keeping it deterministic.**
It must remain a *compile* of (contract intent + reference treatment + bound evidence), never a
free-form LLM output. Determinism is what makes it judgeable and auditable: every beat still points at
a real file, a real timestamp, a real citation.

Layer split we are building toward:
| Layer | Owns | Nature |
|---|---|---|
| Contract | Intent — thesis, which moments | LLM, upstream-guided |
| **Blueprint** | **Editorial spine — act architecture + evidence bound to beats** | **Deterministic compile** |
| Shaping | Voice — narration wording/cadence | LLM, conditioned by `narration_grammar` |
| Paper edit | Timeline — exact play order/windows | Deterministic |
| Render | Pixels | Mechanical |

Gate placement follows from this: the **thesis gate reads the BLUEPRINT** (can the promised payoff be
seen in the bound evidence, before narration dresses it up?); the **craft judge reads the PAPER EDIT**
(replays, dead space, runtime).

## Binding constraints (do not violate)
1. **WORKFLOW_FREEZE 2026-07-11 HOLDS.** P6 does NOT decide whether a case deserves production.
   Worth/selection/reroute lives upstream (Tier-1, Claude-Code). P6 may report that a *direction* is
   uncuttable; it must never silently pick a new angle or reject a case on worth.
2. **The P6 gate is CRAFT-only** — unsourced beats, replays, uncovered required_subevents,
   audio-only/blur-only, dead space. Never worth-based rejection.
3. **Blueprint stays deterministic.** Treatment-driven ≠ LLM-generated.
4. **No hallucinated confidence.** Verdicts are enums (`SHIP` / `REVISE` / `ABSTAIN` / `REJECT`) with a
   cited reason. No 0-100 quality scores — we have no calibration set, so a "78" would be noise
   dressed as precision.
5. **No accountability/prosecution conclusion without a cited disposition source** (defamation rail).

## Verified code facts — use these, don't re-derive
- `render_blueprint.py:401` — `audio_aware = not args.paper_edit_only and not args.no_audio_aware`.
  Audio-aware is DISABLED under `--paper-edit-only`, so that artifact does NOT match the render.
- Audio-aware shifts B-roll windows (`:247-248`, `audible_window`) which cascade through
  `resolve_clip_overlaps()` (`:131`) → different windows AND runtime vs. the real cut.
- `make_documentary.py:261-266` — the orchestrator prints gate results and CONTINUES regardless of
  exit code ("the cut is already rendered, so we never abort"; see also `:52`). Reordering the judge
  alone would NOT stop a render.
- `judge.py:322-339` — already exits 3 on unsourced footage / REWORK verdict. The exit code exists;
  the orchestrator ignores it.
- `blueprint.py:320-337` + `_PHASE_ACT`/`_PHASE_FUNC` (`:47-56`) — acts generated deterministically;
  template `span_pct` / `required_beats` / `vo_moves` / `notes` are **never loaded**.
- `contracts_to_blueprint.py:338-340` — hardcodes 3 acts (act_pre_incident/incident/aftermath).
- `blueprint_shape.py:594-620` `_grammar_directive()` — `--grammar` is cadence-only (phase→move
  distributions, `vo_word_share`, open_moves). Correct as-is; it is the WRONG channel for a full
  reference treatment.
- `blueprint_shape.py:628-629, 650-652` — per-act prompt payload carries only
  `{act_id, title, function, target_sec}`.

## Work items (ordered by what is unblocked)

### 1. FIRST — final-equivalent paper edit + real gate abort  [no upstream dependency]
The "paper edits, not renders" deliverable. Two parts:
- **(a)** A mode that produces a paper edit **identical in timing to the eventual render**
  (`audio_aware=True`) but **stops before ffmpeg**. Requires decoupling audio-aware from
  paper-edit-only at `render_blueprint.py:401`. (`--no-audio-aware` goes the wrong direction; there is
  no existing flag that does this.)
- **(b)** Orchestrator change so a gate step returning non-zero **aborts before render**
  (`make_documentary.py:261-266`). Keep the existing "release gate" semantics available for
  post-render reporting if useful, but a pre-render gate must be able to stop the chain.
- **Acceptance:** running the flagship chain on a case yields paper_edit.json + VALIDATION_*.md +
  judge report, with **zero ffmpeg invocations**, and the clip windows/runtime match what a full render
  would have produced. Exit 3 demonstrably prevents encoding.

### 2. Thesis-survivability gate  [needs the persisted contract — Claude-Code is delivering it]
A **separate step** between blueprint build and shaping. NOT inside `blueprint_shape` — shaping is
incentivized to make the thesis sound convincing, so it is the wrong layer to test whether it *is*
supported. Reads: blueprint + MOMENT_OBJECTS + persisted contract.
Asks: does the chosen thesis survive the available moments? Is the promised payoff actually present?
Are the required acts supported? Is the strongest evidence inconsistent with the chosen direction?
Does the reference treatment require material this case lacks?
Returns exactly one of: `REVISE_THESIS` · `MISSING_EVIDENCE` · `SUGGEST_UPSTREAM_REROUTE` · `ABSTAIN`
(plus PASS). P6 identifies uncuttable direction; **upstream owns the actual reroute.**
- **Acceptance:** on Winding Oak it should flag the weak contradiction (two statements about the same
  subject ≠ mutually exclusive claims) rather than passing it through.

### 3. Wire reference_treatment into the BLUEPRINT  [needs treatment cards — Claude-Code delivering]
This is the promote-to-editorial-spine change, and it is a **blueprint-build change, not a prompt
tweak** — templates are not consumed anywhere today. The treatment must drive act order, act
proportions, required beats and beat-function quotas, replacing/overriding the `_PHASE_ACT` +
beat-count-proportionality defaults when a treatment is supplied. Fall back to current deterministic
behavior when none is.
- **Open question for you to decide, with rationale:** does this land in `blueprint.py` or in
  `contracts_to_blueprint.py` (which currently hardcodes the 3 acts)? You own the frozen chain — tell
  me which and I will author the treatment cards to that consumption point.

### 4. Split the two conditioning objects  [with #3]
- `narration_grammar` — existing `--grammar`; cadence, narration density, rhetorical moves. Unchanged.
- `reference_treatment` — NEW; story architecture, required beats, promise/payoff, modality rotation,
  forbidden failures. Must reach shaping as its own object (the per-act payload at
  `blueprint_shape.py:650-652` currently strips everything but title/function/target_sec).

### 5. Deterministic veto + one editorial critic  [needs TASTE_RULES.json — Claude-Code delivering]
- **Veto layer:** deterministic, zero-LLM, reads `TASTE_RULES.json`. Rules include: no trace/workflow
  language on screen · no audio-only short · no blur-only short · no full-frame blur for extended
  YouTube sequences · no unidentified interview subject · no 911 call without its temporal/factual
  relation to the incident · no duplicate caption authority · no silent footage without visual
  progression or narration · no required reference beat replaced by a label claiming it occurred ·
  no accountability conclusion without a cited disposition.
- **One editorial critic** (not a cascade — six sequential LLM gates is cost/latency theater at our
  volume). Enum verdict + cited reason, with an explicit **ABSTAIN** for off-distribution cases.

### 6. Shorts craft floor
At config instantiation: reject audio-only and blur-only; require hook → context → payoff. A 911 call,
radio transmission or witness interview may provide context but normally cannot BE the entire short.

## What Claude-Code (upstream) is delivering to you
- **Persisted W1 contract** → `d6_blueprint/{case_id}_contract.json` (currently thrown away).
- `contradiction_present` preserved through `pipeline4_score.py`; legal `disposition` populated in P2.
- **Route cards** (upstream angle selection), **treatment cards** (your `reference_treatment`),
  **moment cards** (strong-vs-weak per moment type — where cut-level taste lives), **finish/veto cards**
  → `TASTE_RULES.json`.
- Each approved case arrives with `{chosen_route, candidate_thesis, reference_ids}` attached.
- **Tell me the exact JSON shape you want to consume for `reference_treatment` and `TASTE_RULES`** and
  I will author to that spec rather than guessing.

## Explicitly OUT of scope
- Any case-worth / produce-reject decision (freeze — upstream owns it).
- Preference-training, fixed benchmark, distillation to a cheaper model — deferred until ≥40-50
  approved contracts across ≥3 archetypes. Corpus today is ~18 shorts, 100% SDPD/sacso; training on it
  would bake in "SDPD-OIS = good" and reject valid off-distribution cases.
- Embedding/similarity reference retrieval — deterministic lookup only at this corpus size.
- **Renderer bugs are a SEPARATE lane in a DIFFERENT tree**
  (`Documents/Codex/2026-06-26/.../remotion`): real-FFT waveform (Components.tsx:279), single caption
  authority (:315), selective/tracked blur instead of full-frame (:196). The pre-render gate does not
  cover these — fix them independently, don't fold them into this work.

## Validation targets
Winding Oak and Jason Norris are the **usable floor**, not exemplars — each record carries a positive
and a negative delta (Winding Oak: viable structure / contradiction doesn't deliver its payoff.
Jason Norris: strong case + primary evidence / thin pre-event coverage, over-aggressive blur, dead
space). The gates should reproduce those judgments: pass them as shippable while flagging exactly
those deltas. If a gate rejects them outright it is mis-calibrated; if it passes them clean it is not
discriminating.

## Deliverable for this round
Paper edits, not renders. No re-rendering of anything.

---
# UPSTREAM DELIVERED (Claude-Code, 2026-07-26) — ready for you to consume

All paths below are now IN YOUR TREE at `FlameOn-remotion/discovered_cases/` (copied as plain
additions; your in-flight `blueprint.py` / `Root.tsx` work was not touched, and I did not merge or
cherry-pick into `codex/p6-remotion`).

## Reference cards (authored to your v1 schema)
- `reference_cards/treatments/` — 5 cards. Longform: `standoff_tactical_longform_v1`,
  `accountability_prosecution_longform_v1`, `interrogation_contradiction_longform_v1`.
  Shortform: `contradiction_short_v1`, `direct_event_short_v1`.
  Every act carries `id` (so the CURRENT `build_acts_from_template()` reads them as-is) plus the
  richer `evidence_slots` / `promise` / `payoff` / `forbidden_failures` for your extension.
  INVARIANT ENFORCED: every `required_beats` id has a matching `evidence_slots.slot_id` in the same
  act (validated at generation; 2 authoring bugs were caught and fixed by it).
- `reference_cards/routes/` — 4 route cards (upstream angle selection).
- `reference_cards/moments/moment_cards_v1.json` — 7 moment types, strong-vs-weak + `machine_checks`
  (e.g. contradiction: `two_claims_present`, `speaker_identified_both`, `verbatim_quotes_present`,
  `mutual_exclusivity_asserted_with_evidence`). This is the cut-level taste layer.
- `TASTE_RULES.json` — 12 deterministic rules, closed `check.type` enum, `on_fail` ∈ your verdict enum.
  Seeded with provenance from real operator decisions in `.tmp/analytics/LEDGER.json` flags
  (`held_self_harm_method`, `quote_paraphrase_pending_v2`, `attribution_corrected`, …).

## Upstream signals now available
- `contradiction_present` + `contradiction_count` now survive into the P4 verdict (were dropped).
- `disposition` is now a closed ENUM (GUILTY / ACQUITTED / DISMISSED / DECLINED_TO_CHARGE / PENDING /
  NO_CHARGES_FILED / UNKNOWN, default UNKNOWN) + `disposition_source`, on BOTH `p2_to_p3_case` and
  `p6_blueprint.incident`. NOTE: `pipeline3_audio/doc_extract.py` ALREADY emits
  `disposition{findings, discipline_signals, pages}` — `pages` is your source-of-record for the
  `accountability_requires_disposition_source` rule. You map it to the enum at blueprint build.
- W1 contract persistence: `.tmp/bakeoff/run_contender.py` now takes `--persist-contract <path>` and
  writes `{contract, provenance{model, pack_sha256, cost_usd, tokens, raw_ref}}`. Wire
  `make_documentary` to pass `d6_blueprint/{case_id}_contract.json`. (`.tmp/` is gitignored, so that
  harness edit lives only in FlameOn-main — copy it over or re-apply the 2-line change.)

## The routing handoff object (step 4 — attaches route + thesis + reference IDs)
`discovered_cases/case_fingerprint.py` (read-only synthesis) → `discovered_cases/route_select.py`
(deterministic lookup, NO LLM). Ran across all 117 carded cases:
  ROUTE 85 · ROUTE_NEEDS_VERIFICATION 27 · ABSTAIN 5
Per-case output at `.tmp/routing/{case_id}.json`:
```
{ "decision": "ROUTE" | "ROUTE_NEEDS_VERIFICATION" | "ABSTAIN",
  "chosen_route", "candidate_thesis", "reference_ids": {longform, shortform},
  "route_card", "taste_rules", "moment_cards",
  "verify_before_build": [unproven required features],
  "alternate_routes": [...], "fingerprint_features": [...], "fingerprint_unknowns": [...] }
```
Semantics you can rely on:
- A required feature that cannot be PROVEN deterministically is never counted as satisfied — it
  degrades to `ROUTE_NEEDS_VERIFICATION` and is listed in `verify_before_build`.
- `ABSTAIN` = no treatment fits (off-distribution). It is **not** a case-worth verdict — the freeze
  holds; selection stays upstream.
- `candidate_thesis` is a hedged SKELETON, not a claim. Your thesis-survivability gate is expected to
  test it and may return `REVISE_THESIS`.
- `alternate_routes[]` surfaces reframes, including near-misses with `evidence_gap_request`. Example
  (real): `sdpd_06_22_2023_ia_2023_0345` routes standoff, but flags "REFRAME POSSIBLE — documented
  outcome (termination/suspended/resign, pages 79/87/88) but missing officer_account_or_witness_exists."
  7 such accountability-reframe candidates exist in the current pool.

## Calibration anchors — verified behavior
- Winding Oak (`sacso_16_318045`) → `ROUTE` / standoff_tactical, cleanly satisfied. ✅
- Jason Norris (`ohbci_jason_norris`) → `ROUTE_NEEDS_VERIFICATION` / interrogation_contradiction with
  `contradiction_available` UNPROVEN. This is the Winding-Oak failure mode (assuming a contradiction
  that turns out merely topical) caught structurally BEFORE a build. ✅
