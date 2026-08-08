# FOIA sourcing — state access profiles

Data for **P0 proactive FOIA sourcing** (plan: [/docs/plans/P0_foia_sourcing.md](../../docs/plans/P0_foia_sourcing.md)).
This dir is the natural home for FOIA tooling — `../muckrock_harvest.py` (the completed-request
harvester) lives one level up.

## `state_access_profiles.json`

A **data table, not logic**: per-state public-records access facts, so (a) the future P0
FOIA-worthiness scorer can read a `jurisdiction_access` multiplier, and (b) a human can pick where to
file *this week*. 12 actionable states now (tier-1 file-first → tier-2 caveated → avoid); extend to 50
as needed.

### Schema (per state)
| field | meaning |
|-------|---------|
| `state` / `usps` | name + 2-letter code |
| `tier` | `1` (file first), `2` (good, caveats), `"avoid"` |
| `jurisdiction_access` | scorer multiplier (tier-1 = 1.0, tier-2 = 0.6, avoid = 0.2) — **placeholders, tune later** |
| `police_video_access` | `high` / `moderate` / `low` — bodycam obtainability specifically (not general law) |
| `statute` | governing act + citation |
| `response_deadline_days` | int, or `null` when the law says only "promptly / reasonable time" |
| `response_deadline_note` | the real nuance (acknowledge vs. produce, BWC-specific windows) |
| `residency_required` | `true` = requester must be a state resident / in-state media |
| `bodycam_rule` / `audio_911_rule` | how BWC and 911 are actually treated |
| `fee_regime` | copy/redaction/search-fee posture |
| `key_exemptions` | the exemptions most likely to block us |
| `rcfp` | the state's RCFP Open Government Guide page — **the authority to verify against** |
| `verified` | `false` until a human confirms the row against `rcfp` |
| `notes` | filing strategy / gotchas |

### How to use it NOW (manual filing)
1. Filter to `tier == 1` (FL, WA, CA) for your first real filings.
2. Check `residency_required` — skip VA-style rows unless you have an in-state requester.
3. Read `bodycam_rule` + `response_deadline_note` to frame the request and set follow-up dates.
4. Cite the `statute` in the request; diary the `response_deadline_days`.

### How P0 will use it (later)
`foia_worth = EWU_shape_severity × records_likely_exist × jurisdiction_access × filing_window`
— the scorer reads `jurisdiction_access` here and hard-gates on `tier`/`residency_required`.

## ⚠️ Provenance & trust
Seeded 2026-07-01 from RCFP + Ballotpedia + MuckRock (`meta.sources`). **Every row is `verified:
false`** — good enough to prioritize, NOT legal advice. Before filing, confirm the row against its
`rcfp` page; laws change and per-agency policy varies (broad general law ≠ easy police video). Flip
`verified: true` once a human has checked a row.
