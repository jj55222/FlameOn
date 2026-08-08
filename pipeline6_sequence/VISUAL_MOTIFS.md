# Visual Motif Registry — for beat_miner generation

The catalog of visual treatments the documentary engine can render (Remotion), each
mapped to the **beat_miner `moment_type`** that should trigger it and the **props** the
pipeline must supply. Goal: when beat_miner mines a moment, it (or a post-pass) also
assigns a `motif` + the data that motif needs, so the renderer picks a template instead
of hand-designing each beat.

**Grounding rule (non-negotiable, same rail as narration):** every motif must cite a
real source on screen, and may only show what the record/footage contains. A motif never
invents evidence. The `phone_artifact` is the one to watch — never fabricate a message.

beat_miner moment types (current `VALID_TYPES`): `reveal · detail_noticed · tension_shift
· emotional_peak · callback · procedural_violation · contradiction`. Plus phase/grammar
context (pre_incident/incident/aftermath/investigation/outcome).

---

## Motifs

| id | what it shows | fires on (moment_type / phase) | needs (asset + props) | grounding | status |
|---|---|---|---|---|---|
| **lower_third** | footage + speaker lower-third + quote caption + vignette | any spoken moment — `emotional_peak`, `tension_shift`, dialogue | a CLIP + `speaker`, `role`, `caption` (real line), `source` | caption = real transcript line @ its timecode | ✅ built |
| **doc_highlight** | a report page/excerpt, zoom to a line, red box, margin note | `contradiction`, `procedural_violation`, `reveal` (a record), `outcome` | a DOC page (text/image) + `highlight` phrase, `note`, `source` (p.N) | points at the real source paragraph | ✅ built |
| **evidence_board** | assets scattered on a wall, red-string chain, drift | `callback`, the case **chain/summary** (cross-moment) | 3–6 ASSETS (frames/labels) + `items`, `chain`, `source` | only connect what the record connects (no manufactured links) | ✅ built |
| **map_zoom** | location locator, push-in, pin drop, route, date stamp | `scene_set` / phase transitions, the incident **location** | `place`, `coords`, `date`, `route`, `source` (+ satellite tile if API key) | real place + time | ✅ built (stylized; real tiles need a maps key) |
| **phone_artifact** | a phone frame: text/social/record, blurred context | a **real** message/record `reveal` only | `messages`/`rows` (REAL, sourced) + `source` | ⚠ NEVER fabricate — real evidence only, else label TEMPLATE | ✅ built |
| **timeline_strip** | horizontal timeline of the key timestamps | `tension_shift` / phase transition; chronology | the stamped events (`17:42 → 20:45 → 23:09`) | real timestamps from the timeline | candidate |
| **freeze_callout** | pause footage, arrow/circle on a detail, label | `detail_noticed`, `reveal` (a visual object) | a CLIP frame + detail bbox/region + `label` | the object is really on screen at that frame | candidate (needs vision_scan for the bbox) |
| **quote_card** | bold typographic card of a key line (no footage) | `emotional_peak`, a powerful single line | `quote` + `attribution` | verbatim, attributed | candidate |
| **verdict_stamp** | "SUSTAINED" stamp sequence over the findings | `outcome` / the IA verdict | the sustained `findings` + `disposition` | from doc_extract findings | candidate (can fold into doc_highlight) |
| **split_screen_pov** | the same moment on 2 cameras side-by-side | convergence moments (one event, ≥2 cams) | 2 synced CLIPS at the same abs-time | both are real footage of the same moment | candidate (uses camera-convergence) |
| **ken_burns_still** | slow pan/zoom on a still (booking photo, scene) | `context` — establish a person/place | a PHOTO/still + caption + `source` | real case still | candidate |
| **legal_disclaimer** | "based on official records / presumed innocent" | the OPEN (hook), once per film | fixed text + agency/case | the EWU faithfulness beat | candidate |
| **person_card** | name + role + photo of a key person | `context` — introduce someone | `name`, `role`, optional photo, `source` | from the record | candidate |

**Treatments (applied to footage, not standalone beats):** `redaction_blur` (faces/PII —
compliance), `vignette/grain/grade` (the channel look, always-on).

---

## beat_miner integration (how a moment gets a motif)

Add a `motif` (+ `motif_props`) field to each mined moment. Assignment is a **deterministic
floor, LLM-refined ceiling**:

1. **Floor — `moment_type` + available asset → default motif** (a mechanical map, predictable
   and grounded):
   ```
   contradiction        -> doc_highlight (if a doc backs it) else lower_third
   procedural_violation -> doc_highlight  (cite the policy/finding)
   detail_noticed       -> freeze_callout (needs vision bbox) else lower_third
   reveal (record)      -> doc_highlight ;  reveal (spoken) -> lower_third
   emotional_peak       -> lower_third  (or quote_card if no usable clip)
   tension_shift        -> timeline_strip / map_zoom (phase change)
   callback             -> evidence_board
   outcome/verdict      -> verdict_stamp / doc_highlight
   scene_set            -> map_zoom / ken_burns_still
   ```
2. **Ceiling — the LLM proposer suggests the motif** in the same call that proposes the
   moment (add to the beat_miner prompt: *"for each beat, pick the best motif from this
   list given what's on screen"*), constrained to the available assets.
3. **Asset gate:** a motif is only assigned if its required asset exists (a `doc_highlight`
   needs a doc; `split_screen_pov` needs 2 synced cams). Else fall back down the floor map.

The renderer (`render_blueprint` + the Remotion sidecar) reads `motif` per beat and fills
the template from `motif_props` — the props each motif needs are the columns above.

**Build order (by leverage):** the 5 built motifs already cover most beats. Next highest
value = `freeze_callout` (Dr-Insanity's signature; needs `vision_scan` for the detail bbox)
and `timeline_strip` (cheap, grounds chronology). `verdict_stamp` folds into `doc_highlight`.
