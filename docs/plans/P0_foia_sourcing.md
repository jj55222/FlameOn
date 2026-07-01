# PLAN — P0: Proactive FOIA sourcing (top-of-funnel)

**Status:** proposed, not built (2026-07-01). Recorded as a gap in [/STATE.md](../../STATE.md).
**Owner decision needed:** whether to build now or after the first flagship cut lands.

---

## 1. The gap this fills

Every current intake path sources from records that are **already public**:

- `discovered_cases/muckrock_harvest.py` — reads only **`status=done`** (fulfilled) MuckRock requests.
- agency-portal harvesters (`sdpd_harvest.py`, `sfdpa_harvest.py`, `portal_harnesses.py`, COPA) —
  scrape **already-released** SB1421/SB16 packages.
- P2 / AutoResearch — finds sources for a case **you already name** (defendant + jurisdiction).

So the funnel is capped by what other people already requested and what agencies already posted.
**P0 opens the top of the funnel:** scan public *signals* → find serious incidents whose records
likely exist but aren't released yet → file our own FOIA/public-records requests → feed the fulfilled
releases into the existing intake (muckrock_harvest / P2 → P3…P6).

```
[news/blotter/docket signals] → P0 (score + gate + draft + file + track) → [released records]
                                                                              → muckrock_harvest / P2 intake → P3 → … → P6 cut
```

## 2. Core idea — a FOIA-worthiness score

Reuse the Tier-1 worth model (`.tmp/_tier1_rubric.md`, learned from the 428 EWU/Dr.Insanity titles)
and multiply it by two NEW factors — will the records exist, and can we actually get them:

```
foia_worth = EWU_shape_severity      # reuse tier1: SEVERITY(gate) × story-shape × human-proximity
           × records_likely_exist    # bodycam-equipped agency? 911? in-custody/UoF/death → IA file?
           × jurisdiction_access     # sunshine-state multiplier (see §3) — permissive law = higher
           × filing_window           # fresh enough that retention hasn't purged; not so old it's stale
```

- **EWU_shape_severity** is the existing gate — don't file on boring cases. Contradiction is still a
  multiplier, not the gate.
- **records_likely_exist** — the whole point of a FOIA is that footage isn't posted yet, so infer
  likelihood: agency uses BWC, incident type generates records (OIS, in-custody death, UoF, pursuit,
  high-profile arrest), 911 was called, an IA/PSB investigation is probable.
- **jurisdiction_access** — the sunshine-state model. This is what makes a request *worth filing*
  vs. doomed. See §3.
- **filing_window** — bodycam retention is often 90 days–1 year for non-flagged footage; file BEFORE
  it's purged. Recency is an asset here (opposite of the produce pipeline, which likes settled cases).

## 3. Sunshine states — the jurisdiction model

"Sunshine laws" = open-records/open-meetings laws. But **broad general law ≠ easy police video** —
several states with strong records laws carve out big law-enforcement or bodycam exemptions. Rank by
what THIS project needs: broad record definition + presumption of disclosure + **bodycam/911/incident
reports actually releasable** + workable response deadline + no residency requirement + low fees.

**Working tier list (a HYPOTHESIS — validate each against RCFP before filing; laws change):**

| Tier | States | Why |
|------|--------|-----|
| **1 — file here first** | **Florida**, **Washington**, **California** | FL: Ch. 119, broadest in the nation, strong presumption, criminal/BWC/911 broadly public, no residency. WA: Public Records Act, narrow exemptions, BWC accessible (some agencies self-publish). CA: CPRA + **SB1421/SB16** statutory **45-day** BWC release for critical incidents + unsealed UoF/misconduct — already our backbone (SDPD/SFDPA). |
| **2 — good, with caveats** | Ohio, Georgia, Arizona, Wisconsin, Colorado, Texas | OH/GA/AZ/WI: strong presumption, BWC generally releasable (redaction rules). CO: audio-disclosure mandate + courts ordering BWC release, BUT criminal-justice records are custodian-**discretionary**. TX: TPIA solid but the "**dead-suspect loophole**" lets agencies withhold non-conviction records. |
| **avoid / hard** | Pennsylvania, Virginia, New York | PA Act 22 routes BWC OUT of the normal Right-to-Know Law into a harder, denial-prone process. VA: **residency requirement** + discretionary release of LE records. NY: FOIL slow on police records. |

**Don't hard-code this in code.** Keep it as a **per-state access profile** table (data, not logic)
the scorer reads. ✅ **Built:** [`discovered_cases/foia/state_access_profiles.json`](../../discovered_cases/foia/state_access_profiles.json)
(12 states seeded, schema in [that dir's README](../../discovered_cases/foia/README.md); every row
`verified: false` pending human check against RCFP). Correct it against the authoritative live
references:
- **RCFP** — Reporters Committee: [Open Government Guide](https://www.rcfp.org/open-government-guide/)
  (state-by-state) + [police BWC access map](https://www.rcfp.org/resources/bodycams/).
- **MuckRock** state FOIA guides + agency contacts.

## 4. Pipeline stages

1. **Ingest signals** — pull serious-incident candidates from public feeds: local-news RSS/APIs,
   police blotters + agency press releases, DA/PD news pages, CourtListener/PACER new filings,
   Reddit (e.g. r/news local subs), scanner-summary sites. Store raw signal + source URL + timestamp.
2. **Cluster → incident** — dedupe multiple stories about the same event into one incident record
   (agency, location, date, subjects, incident type).
3. **Enrich** — resolve the responding agency, its BWC policy (does it even wear cameras?), likely
   record types (BWC, 911, dash, IA/PSB, arrest/incident report), and the state.
4. **Score** — compute `foia_worth` (§2). Gate on EWU_shape_severity first (cheap reject of noise).
5. **Jurisdiction gate** — keep only sunshine-state incidents (Tier 1 first), using the per-state
   access profile (§3). Attach the correct statute cite + deadline for the request.
6. **Draft request package** — per surviving incident, generate the FOIA text: agency FOIA contact,
   record-type-specific asks (BWC for date/time/location/officers, 911 audio + CAD, IA findings,
   incident report), templated with the state's statute citation and deadline language.
7. **File** — submit via MuckRock (preferred — it tracks comms + hosts released files in the schema
   `muckrock_harvest` already reads) OR emit a manual filing queue. ⚠️ **Verify MuckRock exposes a
   programmatic request-CREATE endpoint** for our account tier; if not, produce ready-to-paste
   packets + a tracking sheet and file by hand. (Current `muckrock_harvest` only READS.)
8. **Track + hand off** — poll request status; when a request flips to fulfilled, it's exactly what
   `muckrock_harvest --status done` already ingests → the case drops into the normal P2→P6 funnel.

## 5. Reuse vs. build-new

**Reuse:** Tier-1 worth model + `.tmp/creator_catalog/` (the EWU/DrInsanity shape corpus);
`muckrock_harvest.py` auth (Squarelet JWT via `MUCKROCK_USERNAME/PASSWORD`) + its candidate/download
schema + the `pivot` block; the `.env` robust reader (`_read_env_file`).

**Build new:** signal ingestion + incident clustering; the `records_likely_exist` estimator; the
per-state access-profile table; request drafting; the filing + status-tracking loop.

## 6. MVP → full

- **MVP (days, low risk):** a hand-curated incident list (10–20 fresh serious cases) → Tier-1 worth
  score → sunshine-state gate (FL/WA/CA only) → auto-**drafted** request text you review and file by
  hand → a simple tracking sheet. Proves the worth model + drafting on real filings without building
  ingestion or auto-filing.
- **Full:** automated signal ingestion + clustering + auto-file via MuckRock API + status loop that
  feeds fulfilled releases straight into intake.

## 7. Risks & caveats (read before building)

- **Latency.** Agency responses take weeks–months. P0 is a *pipeline-filling* investment, **not** an
  EOW deliverable. Prioritize the first flagship cut from already-held footage first.
- **Laws change / vary by department.** Keep jurisdiction rules as DATA validated against RCFP, never
  hard-coded; per-agency policy can differ from state law.
- **Cost & volume.** MuckRock filings and video fees add up. Be TARGETED — the worth gate exists so we
  don't spam agencies (which also burns goodwill and invites blanket denials).
- **Ethics/legal.** Public records only; handle subject PII responsibly; respect sealed-record and
  victim-privacy exemptions. This supports fact-checked accountability journalism — keep it that.
- **Retention race.** File within the retention window or the footage is gone — recency-gate hard.

## 8. Concrete next steps

1. Decide build-now vs. after-first-cut (recommend: after).
2. ✅ **Done** — per-state access-profile table stood up at `discovered_cases/foia/state_access_profiles.json`
   (12 states). Remaining: a human verifies each row against its `rcfp` link and flips `verified: true`.
3. Confirm whether MuckRock supports programmatic request creation for our account (determines §4.7).
4. Run the **MVP**: 10–20 hand-picked fresh FL/WA/CA incidents → score → draft → file by hand → track.
5. If MVP filings yield usable releases, automate ingestion (§4.1–4.3).
