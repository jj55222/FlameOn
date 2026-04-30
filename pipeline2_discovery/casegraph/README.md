# CaseGraph (P2 rebuild)

Deterministic, no-API replacement for the legacy `pipeline2_discovery/research.py` flat-source loop.

The legacy loop optimized `(defendant, jurisdiction) → flat source list with evidence keywords`. CaseGraph optimizes `partial input → locked identity → outcome → artifact claims → verified public artifact URLs → CasePacket → PRODUCE/HOLD/SKIP verdict`.

The package is **pure**: no live HTTP, no LLM calls, no downloads. Connectors and resolvers that wrap external APIs only run when explicitly called with credentials; tests skip them by default. Rules below are enforced by code, not convention.

---

## Pipeline flow

```
CaseInput                       (inputs/youtube.py, inputs/structured.py, routers.py)
    │
    ├─ query_planner.plan_queries_from_*   →  ConnectorQueryPlan[]
    │
    ├─ connector.collect(case_input)       →  SourceRecord[]      (connectors/)
    │       (mock | youtube | muckrock | courtlistener)
    │
    ├─ resolver(packet)                    →  VerifiedArtifact[]  (resolvers/)
    │       (muckrock_files; future: documentcloud, courtlistener_recap)
    │
    ▼
CasePacket  (models.py)
    ├─ resolve_identity(packet)            →  IdentityResolution      (identity.py)
    ├─ resolve_outcome(packet)             →  OutcomeResolution       (outcome.py)
    ├─ extract_artifact_claims(packet)     →  ClaimExtractionResult   (claim_extraction.py)
    └─ score_case_packet(packet)           →  ActionabilityResult     (scoring.py)
                                                  └─ verdict: PRODUCE | HOLD | SKIP
```

Weak-input shortcut: [`assemble_weak_input_case_packet`](assembly.py) chains plan → packet → identity → outcome → claims → resolver → score in one call for fixture/test use.

---

## Core types ([models.py](models.py))

| Type | Purpose |
|---|---|
| `CaseInput` | Normalized partial input. `input_type`, `known_fields`, `missing_fields`, `candidate_queries`. |
| `CaseIdentity` | Candidate facts about the case: names, jurisdiction, agency, date, case numbers, charges. Holds `identity_confidence` (`high`/`medium`/`low`) and `outcome_status`. |
| `SourceRecord` | One discovered URL. `source_type`, `source_authority` (`court`/`official`/`foia`/`news`/`unknown`), `source_roles`, `matched_case_fields`. **Never carries a verdict.** |
| `ArtifactClaim` | "X video was released/requested/withheld" — a textual claim, not a file. Has `claim_source_url`, never an `artifact_url`. |
| `VerifiedArtifact` | A concrete public artifact URL (PDF, MP4, MP3, MuckRock/DocumentCloud doc, etc.) confirmed by a resolver. Carries `format`, `downloadable`, `source_authority`, `confidence`. |
| `CasePacket` | Container for `input + case_identity + sources + artifact_claims + verified_artifacts + scores + verdict + risk_flags + next_actions`. |
| `ActionabilityResult` | Output of `score_case_packet`. Pure — does not mutate the packet. |

### Source roles

A `SourceRecord` carries `source_roles: List[str]` from this set:

- `identity_source` — anchors who/where/when (used by identity gate)
- `outcome_source` — confirms charged/convicted/sentenced/closed/dismissed/acquitted
- `claim_source` — contains language about an artifact (used by claim extraction)
- `artifact_source` — points at the artifact itself (still doesn't *prove* verification — only the resolver creates a `VerifiedArtifact`)
- `possible_artifact_source` — looks artifact-shaped but unconfirmed

**Hard rule:** `claim_source ≠ artifact_source`. A page saying "bodycam was released" is a claim. It becomes a `VerifiedArtifact` only when a resolver finds the actual public URL.

---

## Gates

### Identity ([identity.py](identity.py))

`resolve_identity(packet)` scores anchors detected in `identity_source` / strong-authority source text:

| Anchor | Weight |
|---|---|
| full name match | +35 |
| last name only | +15 (sets `weak_identity`, `common_name_risk`) |
| jurisdiction (city/county/state, capped) | +10 each, up to +25 |
| agency | +15 |
| incident_date | +15 |
| case_number | +20 |
| victim_name | +12 |
| strong source authority (court/official/foia) | +15 |

Confidence rules:
- **high**: full_name + jurisdiction + ≥1 disambiguator (agency/date/case#/victim/authority) + score ≥ 80 + no `conflicting_jurisdiction`
- **medium**: score ≥ 45 and no `conflicting_jurisdiction`
- **low**: everything else

Risk flags emitted: `weak_identity`, `name_city_only`, `missing_disambiguator`, `common_name_risk`, `conflicting_jurisdiction`, `insufficient_identity_anchors`.

### Outcome ([outcome.py](outcome.py))

Regex-detects `sentenced | convicted | charged | dismissed | acquitted | closed` over source text. Outcome confidence is **clamped by identity confidence** — `low` identity caps outcome score at 55, blocks outcome=`high`. Strong authority (court/official/foia) and source-side identity anchor (name+location or matched case fields) are required for outcome=`high`.

Conflicting statuses across sources → `conflicting_outcome_signals` risk.

### Claim extraction ([claim_extraction.py](claim_extraction.py))

Scans every source for artifact-type patterns (bodycam, interrogation, court_video, dispatch_911, docket_docs, surveillance_video, audio) and label patterns (`artifact_released`, `artifact_requested`, `artifact_withheld`, `artifact_mentioned_only`).

**Never** creates a `VerifiedArtifact`. **Never** mutates `identity_confidence`, `outcome_status`, `verdict`, or scores. `artifact_withheld` adds a `next_action` to follow up; that's the only side-effect class beyond appending claims and risk flags.

### Artifact verification ([resolvers/muckrock_files.py](resolvers/muckrock_files.py))

Only resolvers create `VerifiedArtifact`s. The MuckRock resolver looks for concrete public file URLs (`.pdf`, `.mp4`, `.mp3`, `.wav`, `.m4a`, `.doc`, `.docx`, MuckRock public-file paths, DocumentCloud links). Rejects URLs with login/auth/private markers. Request-only pages and "records produced" prose remain claims.

---

## Scores ([scoring.py](scoring.py))

`score_case_packet(packet)` is **pure**. It returns an `ActionabilityResult` and does not mutate the packet.

It computes two scores plus an aggregate:

### `research_completeness_score` (0–100)

How well-researched is this case? Measures discovery completeness regardless of whether it's video-ready.

Components: identity (≤25), outcome (≤20), document research (≤20, includes case#/charges/victim/agency/date/document artifacts/strong authority), source quality & diversity (≤15), artifact-claim signals (≤10), gap clarity (≤10).

### `production_actionability_score` (0–100)

Can this case be turned into a video right now from verified public artifacts?

Components: identity (≤20), outcome (≤15), verified media artifacts (≤40, weighted by type — bodycam/interrogation 26, court_video 20, dispatch_911/surveillance/dashcam 18, other_video 16, audio 14, plus +2 each for `downloadable` and strong authority), portfolio bonus (single category 6, multi 12, ≥3 categories or premium pair 15), downstream readiness (≤10).

### `actionability_score` (aggregate)

`round(production_score * 0.65 + research_score * 0.35, 2)`. Used for ranking; verdict comes from the rules below, not this number.

---

## Verdict rules ([scoring.py:_verdict](scoring.py))

```
PRODUCE  ↔  identity_confidence == "high"
            AND outcome_status ∈ {sentenced, closed, convicted}
            AND len(media_artifacts) ≥ 1            # verified, not claim
            AND production_score ≥ 70
            AND no severe risk in {
                conflicting_jurisdiction, weak_identity,
                protected_or_nonpublic_only, identity_unconfirmed,
                artifact_unverified
            }

SKIP     ↔  conflicting_jurisdiction in risk_flags
            OR not promising  (no verified media, no claims, no docs+identity, no concluded high-id)

HOLD     ↔  everything else that is at least promising
```

### Defaults this enforces

- **Document-only → HOLD** (verified docs but zero verified media). Adds reason `document_only_hold`.
- **Claim-only → HOLD** (claims exist but zero verified artifacts). Adds reason `claim_only_hold`, risk `artifact_unverified`.
- **Weak identity → never PRODUCE.** Identity gate is upstream of every other gate.
- **Weak input alone → never PRODUCE.** Weak input cannot lock identity, cannot create `VerifiedArtifact`, and `assemble_weak_input_case_packet` always tags the packet with `weak_input_preliminary_packet` and `candidate_fields_not_identity_lock` risk flags.
- **Protected/private artifacts → not eligible.** Resolver rejects login/auth URLs; `protected_or_nonpublic_only` risk blocks PRODUCE.

---

## Connector contract ([connectors/base.py](connectors/base.py))

`SourceConnector.collect()` calls `fetch()` and validates every returned record with `validate_connector_source_record`. The validator **rejects** any `SourceRecord` (or its `metadata` / `confidence_signals`) carrying any of: `artifact_verified`, `confidence`, `final_confidence`, `final_verdict`, `verdict`, `verified_artifact`, `verified_artifacts`. Connectors discover; they never decide.

Live connectors (CourtListener, MuckRock, YouTube/yt-dlp) are metadata-only. No downloads. Tests for live calls skip when env vars are unset.

---

## Adapters ([adapters.py](adapters.py))

- `export_p2_to_p3(packet)` — shape for Pipeline 3 (audio enrichment).
- `export_p2_to_p4(packet)` — shape for Pipeline 4 (scoring).
- `export_p2_to_p5(packet)` — shape for Pipeline 5 (assembly/publish).
- `export_legacy_evaluate_result(packet)` — backward-compatible `{evidence_found, sources_found, confidence}` for the old `evaluate.py` scorer.

---

## Where things live

```
pipeline2_discovery/casegraph/
├── models.py              CasePacket, SourceRecord, ArtifactClaim, VerifiedArtifact, ...
├── identity.py            resolve_identity (anchors → confidence)
├── outcome.py             resolve_outcome (regex + identity-clamp)
├── claim_extraction.py    extract_artifact_claims (claim_source ≠ artifact_source)
├── scoring.py             score_case_packet, verdict rules
├── assembly.py            assemble_weak_input_case_packet (chained pipeline)
├── query_planner.py       CaseInput → ConnectorQueryPlan[]
├── adapters.py            P3/P4/P5/legacy export shapes
├── routers.py             route_manual_defendant_jurisdiction (legacy entry)
├── connectors/            base + mock + youtube + muckrock + courtlistener
├── resolvers/             muckrock_files (only path that creates VerifiedArtifact today)
└── inputs/                youtube (weak title/desc/transcript), structured (WaPo UoF rows)
```

Tests live at repo root under [`tests/`](../../tests/). Fixtures under [`tests/fixtures/casegraph_scenarios/`](../../tests/fixtures/casegraph_scenarios/), [`tests/fixtures/youtube_inputs/`](../../tests/fixtures/youtube_inputs/), [`tests/fixtures/structured_inputs/`](../../tests/fixtures/structured_inputs/).
