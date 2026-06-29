# P4 (selector) vs. Claude (strong LLM) — case-worth-pursuing concordance

**Question:** does Pipeline-4's PRODUCE/HOLD/SKIP gauge of "is this case worth pursuing"
agree with a strong LLM's independent judgment, on the same material?
**Set:** the 10 most-interesting A-tier SDPD cases (all gold: downloadable video+audio+docs).
**Method:** each case downloaded → narrative-core audio transcribed (mlx-whisper) → scored
by `pipeline4_score.py` (Gemini Pass-1 extract → deterministic `scoring_math` → Qwen Pass-2);
in parallel a **blind** Claude subagent (same rubric, transcripts+manifest only, never sees P4's
output) rendered an independent PRODUCE/HOLD/SKIP + 0–100.

## Results (n=10)

| case | type | P4 det/qwen/final | P4 score | Claude | Claude score | agree |
|---|---|---|---|---|---|---|
| 09_2023 IA-009 (Instagram/EEO meme) | Sustained | SKIP/SKIP/**SKIP** | 20.9 | **HOLD** | 34 | adj |
| fanuel St (force) | Use of Force | HOLD/HOLD/**HOLD** | 54.2 | **PRODUCE** | 82 | adj |
| 10_2023 IA-016 (sexual harassment/retaliation) | Sustained | SKIP/SKIP/**SKIP** | 18.7 | **PRODUCE** | 82 | **FAR** |
| 08_2023 IA-0388 (prisoner sexual assault) | Sustained | HOLD/HOLD/**HOLD** | 51.7 | **PRODUCE** | 88 | adj |
| 06_2024 IA-013 (K-9 slur/scuffle) | Sustained | SKIP/HOLD/**HOLD** | 19.9 | **HOLD** | 48 | **EXACT** |
| logan Ave | OIS (fatal) | HOLD/HOLD/**HOLD** | 54.4 | **PRODUCE** | 86 | adj |
| la cresta Blvd | OIS (fatal) | HOLD/HOLD/**HOLD** | 54.2 | **PRODUCE** | 82 | adj |
| la jolla Blvd | OIS (fatal) | HOLD/HOLD/**HOLD** | 53.8 | **PRODUCE** | 78 | adj |
| san ysidro Blvd | OIS (fatal) | HOLD/HOLD/**HOLD** | 51.2 | **PRODUCE** | 82 | adj |
| 4s commons Dr | OIS (fatal) | HOLD/HOLD/**HOLD** | 49.2 | **PRODUCE** | 84 | adj |

**Verdict distribution:** P4 = 0 PRODUCE / 8 HOLD / 2 SKIP.  Claude = 8 PRODUCE / 2 HOLD / 0 SKIP.
Near mirror images: P4's modal verdict is HOLD, Claude's is PRODUCE.

**Agreement:** exact 10% (1/10) · within-one-notch 90% (9/10) · Spearman(score rank) 0.41.
Every disagreement is **Claude-higher** — P4 never out-rates Claude on any case.

## The mechanism — P4 *cannot* PRODUCE this class of case

`narrative_score = 0.40·density + 0.30·arc + 0.20·artifact + 0.10·uniqueness`, and the
PRODUCE gate hard-requires **moment_density_score ≥ 60** AND narrative_score ≥ 72.

Observed `moment_density_score` across all 10 cases: **1.3 – 5.8** (reference density 0.6
weighted-moments/min maps to 60). Interview/911/witness transcripts are long and talky —
they don't pack ~0.6 weighted moments per minute, so density scores ~1–6 here, an order of
magnitude under the gate. With density pinned near zero, the score ceiling for this material is:

`0.30·100(arc) + 0.20·75(artifact) + 0.10·~70(uniqueness) + 0.40·~5(density) ≈ 54`

— which is exactly where the 8 OIS/force/criminal cases landed (49–54). **It is mathematically
impossible for P4 to clear 72 (PRODUCE) on interview-driven evidence, regardless of how serious
the case is.** That's why a fatal shooting and an alleged sexual assault of a prisoner both cap at HOLD.

Note: the Qwen Pass-2 LLM rubber-stamped the deterministic verdict in **9 of 10** cases (it only
nudged 013 SKIP→HOLD). So this is effectively the **deterministic core vs. Claude** — and they diverge hard.

## Where they agree, and where they break

- **Agreement is at the low-stakes floor.** The only exact match (013, both HOLD) and the closest
  call (009, SKIP vs HOLD) are the two genuinely middling cases — a workplace meme and a K-9-yard
  slur/scuffle. When the content really is thin, Claude's precision bias also pulls back, converging with P4.
- **Divergence scales with stakes.** Every high-stakes case — 4 fatal shootings, an alleged
  in-custody sexual assault, a multi-year harassment/retaliation case, a force case — is a Claude
  PRODUCE and a P4 HOLD/SKIP. Claude weighs **stakes + a documented critical contradiction +
  accountability arc**; P4 weighs **moments-per-minute**. Those are different axes.
- **The sharpest single conflict is IA-016** (sexual harassment/retaliation): P4's *lowest* score
  (18.7, SKIP) is one of Claude's *highest* (82, PRODUCE). It's the case where density most badly
  mis-measures substance — a documented self-contradiction in the subject's own texts + a second
  corroborating victim + a sustained-discipline payoff, all delivered in long interviews.

## Implication

P4 is calibrated for **discovery triage** — finding the rare gem among thousands of raw candidate
cases, precision-biased to a <30% PRODUCE rate so production time isn't wasted on false positives.
Those thresholds (density ≥ 60, score ≥ 72) are right for needle-in-a-haystack intake.

They are the **wrong calibration for ranking among already-good cases.** The A-tier set is, by
construction, the pipeline-ready gold population; a selector pointed at it should *discriminate among*
good cases, not reject all of them. P4 says "none of these 10 gold cases are worth producing," which
is plainly miscalibrated for this job. To rank good cases it needs either:
1. a separate **"rank good cases" mode** with relaxed thresholds and a density metric that doesn't
   punish long investigative testimony, or
2. an added **stakes / critical-contradiction / accountability axis** (what Claude actually weighs)
   that P4's density-dominated math currently ignores.

The strong LLM tracks documentary worth here; P4's deterministic spine tracks moment-density, which
for interview-driven accountability cases is close to orthogonal to worth.

## Caveats
- **Transcription scope:** audio-only narrative core (interviews / 911 / statements / incident clips),
  capped at 14 files/case; the big BWC/CCTV *video* was downloaded but not transcribed (tractability).
  Both P4 and Claude saw the *identical* audio set, so the comparison is fair; absolute "worth" of the
  OIS cases is understated for both (the on-camera shooting itself wasn't in the transcript).
- **mlx-whisper hallucination:** several long quiet interviews showed localized repetition loops /
  stray characters. Substance was recoverable and the verdicts are sound, but any case taken to an
  actual cut should be re-transcribed (`condition_on_previous_text=False` / VAD).
- **Claude side:** judged holistically on P4's own rubric (narrative tension, contradictions, stakes,
  artifact completeness, precision-bias), blind to P4's number, one independent subagent per case.

Artifacts: per-case P4 verdicts in `.tmp/<cid>/d2/verdicts_p4/`, Claude verdicts in
`.tmp/<cid>/d2/claude_verdict.json`, comparator `.tmp/_compare.py`, frozen rubric `.tmp/_verdict_prompt.md`.
