"""
prompts.py — Pass 1 and Pass 2 prompt templates for Pipeline 4.

Pass 1 (long context, cheap): structural extraction from raw transcript.
Pass 2 (short context, better judgment): narrative scoring from Pass 1 output.

Keeping prompts here (separate from orchestration) makes dry-run diffs
reviewable and lets future summarization experiments import them.
"""

import json
from typing import Optional


VALID_MOMENT_TYPES = [
    "contradiction",
    "emotional_peak",
    "procedural_violation",
    "reveal",
    "detail_noticed",
    "callback",
    "tension_shift",
]

VALID_ARC_TYPES = [
    "chronological",
    "cold_open",
    "parallel_timeline",
    "reveal_structure",
    "escalation",
]

VALID_IMPORTANCE = ["critical", "high", "medium", "low"]

VALID_EVIDENCE_TYPES = [
    "bodycam",
    "interrogation",
    "court_video",
    "911_audio",
    "dash_cam",
    "news_report",
    "other",
]


# ─────────────────────────────────────────────────────────────
# PASS 1 — Structural extraction
# ─────────────────────────────────────────────────────────────

PASS1_SYSTEM = """You are a structural analyst for law enforcement transcripts (bodycam, interrogation, DPA/IA interviews, 911 calls, and critical incident briefings).

Your job is to extract STRUCTURE from a transcript. You identify events, moments, contradictions, and factual anchors.

You do NOT judge narrative value — a separate model does that. Your output must be valid JSON, nothing else: no prose, no markdown fences, no <think> blocks."""


PASS1_USER_TEMPLATE = """CASE ID: {case_id}
TOTAL RUNTIME: {total_sec:.0f}s ({total_min:.1f} min) across {source_count} source(s).
TRANSCRIPT SEGMENT COUNT: {segment_count}

================================================================
MANDATORY EXTRACTION QUOTA: {target_moments_low} to {target_moments_high} moments.
Returning fewer than {target_moments_low} moments is a FAILED extraction.
Successful winning true-crime documentaries pack {target_moments_low}-{target_moments_high} narrative
moments per hour. You must find them. Be thorough, not conservative.
================================================================

CONTENT TYPE GUIDANCE:
This transcript may be raw single-source footage (one bodycam, one
interrogation) OR a compiled documentary (narrator voiceover + clips
of bodycam + clips of interrogation + court audio + 911). Treat both
as valid: in compiled content, the narrator's reveals/contradictions/
callbacks count as moments just like the on-scene speech. Each segment
is potential signal — scan all of it.

SOURCES:
{source_list}

TRANSCRIPT (each line: [HH:MM:SS | S<idx> | SPK<n>] text):
{transcript_text}

Extract the following and return a single JSON object with EXACTLY this structure:

{{
  "timeline": [
    {{
      "source_idx": 0,
      "timestamp_sec": 12.4,
      "event": "short description",
      "speakers_involved": ["SPK0", "SPK1"],
      "emotional_intensity": 3
    }}
  ],
  "moments": [
    {{
      "source_idx": 0,
      "timestamp_sec": 412.8,
      "end_timestamp_sec": 431.0,
      "type": "contradiction|emotional_peak|procedural_violation|reveal|detail_noticed|callback|tension_shift",
      "description": "what happens",
      "transcript_excerpt": "verbatim quote from the transcript",
      "provisional_importance": "critical|high|medium|low"
    }}
  ],
  "contradictions": [
    {{
      "statement_a": {{"source_idx": 0, "timestamp_sec": 120.0, "speaker": "SPK0", "text": "..."}},
      "statement_b": {{"source_idx": 1, "timestamp_sec": 48.0, "speaker": "SPK0", "text": "..."}},
      "nature_of_contradiction": "why these conflict"
    }}
  ],
  "speaker_dynamics": [
    {{
      "speaker_pair": ["SPK0", "SPK1"],
      "interaction_type": "cooperative|adversarial|neutral",
      "power_dynamic": "description"
    }}
  ],
  "emotional_arc": [
    {{
      "source_idx": 0,
      "segment_start_sec": 0,
      "segment_end_sec": 300,
      "avg_intensity": 2.1,
      "trend": "rising|falling|stable"
    }}
  ],
  "factual_anchors": [
    {{
      "type": "name|date|location|badge_number|charge|statute",
      "value": "the fact",
      "source_idx": 0,
      "timestamp_sec": 45.2
    }}
  ],
  "detected_structure_hint": "chronological|cold_open|parallel_timeline|reveal_structure|escalation"
}}

RULES:
1. Use ONLY these seven moment types: {moment_types_list}
   - "contradiction" — a statement that conflicts with another statement, evidence, or known fact (within or across sources, including narrator vs. on-scene speech)
   - "reveal" — new information that changes understanding (a discovery, a confession, a piece of evidence surfaced, a name dropped)
   - "emotional_peak" — visible/audible distress, shock, anger, breakdown, dramatic shift in tone
   - "procedural_violation" — Miranda issue, use-of-force concern, evidence handling failure, rights ignored, policy breach
   - "detail_noticed" — narrator or speaker draws attention to a small but loaded detail (a poison search, a missing object, an out-of-place statement)
   - "callback" — explicit reference to an earlier moment that recontextualizes it
   - "tension_shift" — interaction pivots from cooperative→adversarial or vice versa, or stakes escalate suddenly
2. timestamp_sec and end_timestamp_sec must exist within the transcript — NEVER invent timestamps.
3. Every moment MUST include source_idx matching the source it came from (use 0 for single-source compiled videos).
4. Cross-source signal is gold: if the same speaker appears in bodycam AND a later interrogation, scan for contradictions between them.
5. EXTRACTION DENSITY IS THE PRIMARY METRIC. Do NOT be conservative about how many moments to extract. The quota above is a floor, not a ceiling. A 60-minute compiled true-crime documentary regularly contains 20-40 distinct narrative moments — find them. Hard cap moments at 60, timeline at 100, emotional_arc at 20.
6. Mark provisional_importance based on narrative weight (NOT extraction stinginess):
   - "critical" (~10-20% of moments): confession, cross-source contradiction, Miranda issue, fatal force, named suspect first revealed, motive uncovered
   - "high" (~30-40% of moments): strong emotional peak, key reveal, tension shift, procedural concern
   - "medium" (~30-40% of moments): default; most moments fall here
   - "low" (~10-20% of moments): minor callback, supporting detail, transitional beat
7. emotional_intensity scale: 1 (calm) to 5 (crisis).
8. detected_structure_hint criteria:
   - "cold_open" — opens with the climactic/dramatic moment (arrest, body discovery, key quote), then backtracks to fill in setup. THIS IS THE MOST COMMON in true-crime documentaries (~80% of winners) — pick this if the first 60 seconds contains a payoff that's later contextualized.
   - "chronological" — strict forward time order from setup to climax to aftermath
   - "escalation" — opens calm and steadily escalates without re-ordering
   - "parallel_timeline" — alternates between two timelines (e.g. investigation thread vs. suspect-life thread)
   - "reveal_structure" — only when the structure is explicitly built around delaying ONE specific revelation (rare; do not over-pick this)
9. Return ONLY the JSON object. No prose before or after. No markdown. No thinking blocks.
"""


def render_pass1(merged_transcript: dict, transcript_text: str) -> tuple:
    """Render Pass 1 system + user messages from a merged transcript."""
    sources = merged_transcript["sources"]
    source_list = "\n".join(
        f"  S{s['source_idx']}: {s['source_url'][:80]} ({s['evidence_type']}, {s['duration_sec']:.0f}s)"
        for s in sources
    )
    segment_count = len(merged_transcript.get("segments", []))
    # Scale extraction target to transcript length so Gemini isn't over-conservative.
    # Rough guidance: ~1 moment per 50 segments, ~1 timeline event per 20 segments.
    target_moments_low = max(8, min(25, round(segment_count / 70)))
    target_moments_high = max(target_moments_low + 4, min(45, round(segment_count / 35)))
    target_timeline = max(15, min(80, round(segment_count / 20)))
    user = PASS1_USER_TEMPLATE.format(
        case_id=merged_transcript["case_id"],
        total_sec=merged_transcript["total_duration_sec"],
        total_min=merged_transcript["total_duration_sec"] / 60,
        source_count=len(sources),
        segment_count=segment_count,
        target_moments_low=target_moments_low,
        target_moments_high=target_moments_high,
        target_timeline=target_timeline,
        source_list=source_list,
        transcript_text=transcript_text,
        moment_types_list=", ".join(VALID_MOMENT_TYPES),
    )
    return PASS1_SYSTEM, user


# ─────────────────────────────────────────────────────────────
# PASS 2 — Narrative scoring / judgment
# ─────────────────────────────────────────────────────────────

PASS2_SYSTEM = """You are a true crime content editor for the FlameOn YouTube channel. Your job is to decide whether a case is worth producing a full video about.

You receive structured Pass 1 extraction PLUS a deterministic scoring breakdown that already accounts for moment density, arc fit, and artifact diversity. Trust those numbers when they are unambiguous.

DECISIVE SIGNAL = PRODUCE: when narrative_score is high AND density is solid AND artifacts cover a winning combination, agree with PRODUCE. The deterministic math has already filtered for precision.

DEFAULT TO HOLD on borderline cases (narrative_score in the marginal band, mixed signals, or one strong but one weak component). Use SKIP only for clear non-narrative content, single-source admin material, or political/protest cases without criminal arc.

False PRODUCE wastes a production sprint, but missing a clear winner is also expensive — the math is calibrated against winners and you should NOT systematically demote them.

Your output must be valid JSON, nothing else: no prose, no markdown fences, no <think> blocks."""


PASS2_USER_TEMPLATE = """CASE ID: {case_id}
TOTAL RUNTIME: {total_sec:.0f}s ({total_min:.1f} min)
AVAILABLE ARTIFACTS: {available_artifacts}

PIPELINE 1 SCORING WEIGHTS (value of each moment type, derived from {winner_count} high-performing winner videos):
{moment_weights_json}

WINNER ARC PATTERNS (structure types that correlate with high view counts):
{arc_patterns_json}

WINNER ARTIFACT VALUE (engagement multiplier for each combination):
{artifact_value_json}

PASS 1 STRUCTURAL EXTRACTION:
{pass1_json}

DETERMINISTIC SCORING BREAKDOWN (pre-computed, for your reference):
  moment_density_score:         {md:.1f} / 100
  arc_similarity_score:         {asim:.1f} / 100
  artifact_completeness_score:  {ac:.1f} / 100
  uniqueness_score:             {un:.1f} / 100
  combined_narrative_score:     {combined:.1f} / 100

YOUR TASK:

1. Re-rank the Pass 1 moments using the weights above. Assign final importance from {{critical, high, medium, low}}.
   You may DEMOTE a moment to a lower importance, but you may NOT invent new moments or timestamps.
   Keep the top 5-12 most valuable moments. Drop the rest.

2. Pick the best narrative_arc_recommendation from: {valid_arcs}
   Prefer Pass 1's detected_structure_hint unless the arc_patterns strongly suggest a different structure would perform better.

3. Decide the verdict — PRODUCE, HOLD, or SKIP. Use these calibrated thresholds:
   - PRODUCE: narrative_score >= {produce_score_thresh}, AND moment_density_score >= {produce_density_thresh}, AND
     at least one critical/high-importance moment of type contradiction/reveal/procedural_violation,
     AND artifact_completeness_score >= 50 (case has an artifact combo with some winner-pattern fit).
     If those are met, agree with PRODUCE — do NOT demote based on "feel". The math is calibrated.
   - SKIP: narrative_score < {skip_score_thresh}, OR zero critical/high moments,
     OR purely political/protest content without criminal arc,
     OR single-source admin/procedural material,
     OR (arc_similarity_score < 30 AND artifact_completeness_score < 70) — this catches cases
     that pump density on procedural/admin content but lack the structural fit and
     artifact diversity that real producible winners have.
   - HOLD: everything else — true uncertainty, NOT a default fallback.

4. Write content_pitch:
   - For PRODUCE: one paragraph (3-5 sentences) selling why this case works. Mention the hook, the key contradiction/reveal, and the emotional stakes.
   - For HOLD: one sentence explaining what's missing that could unlock a PRODUCE (e.g., "Strong interrogation but no incident footage — hold for portal scrape").
   - For SKIP: one sentence explaining the disqualification (e.g., "Political protest case, no criminal narrative").

5. Return ONLY this JSON:

{{
  "verdict": "PRODUCE|HOLD|SKIP",
  "confidence": 0.0-1.0,
  "narrative_arc_recommendation": "{valid_arcs_pipe}",
  "final_moments": [
    {{
      "moment_type": "one of the seven types",
      "source_idx": 0,
      "timestamp_sec": 123.4,
      "end_timestamp_sec": 145.0,
      "description": "what happens",
      "importance": "critical|high|medium|low",
      "transcript_excerpt": "verbatim quote"
    }}
  ],
  "content_pitch": "your pitch or explanation",
  "reasoning_summary": "2-3 sentences explaining the verdict"
}}

Return ONLY the JSON. No prose, no markdown, no thinking.
"""


def render_pass2(
    merged_transcript: dict,
    pass1_output: dict,
    weights: dict,
    scoring_breakdown: dict,
    combined_score: float,
    winner_count: int = 10,
) -> tuple:
    """Render Pass 2 system + user messages.
    Verdict thresholds in the prompt match scoring_math env-var settings so
    Pass 2 doesn't override deterministic verdicts using stale numbers.
    """
    import os
    produce_score_thresh = os.environ.get("P4_PRODUCE_SCORE_THRESH", "40")
    produce_density_thresh = os.environ.get("P4_PRODUCE_DENSITY_THRESH", "20")
    skip_score_thresh = os.environ.get("P4_SKIP_SCORE_THRESH", "15")

    user = PASS2_USER_TEMPLATE.format(
        case_id=merged_transcript["case_id"],
        total_sec=merged_transcript["total_duration_sec"],
        total_min=merged_transcript["total_duration_sec"] / 60,
        available_artifacts=json.dumps(merged_transcript.get("available_evidence_types", [])),
        winner_count=winner_count,
        moment_weights_json=json.dumps(weights.get("moment_weights", {}), indent=2),
        arc_patterns_json=json.dumps(weights.get("arc_patterns", []), indent=2),
        artifact_value_json=json.dumps(weights.get("artifact_value", {}), indent=2),
        pass1_json=json.dumps(pass1_output, indent=2)[:40000],  # hard cap
        md=scoring_breakdown.get("moment_density_score", 0),
        asim=scoring_breakdown.get("arc_similarity_score", 0),
        ac=scoring_breakdown.get("artifact_completeness_score", 0),
        un=scoring_breakdown.get("uniqueness_score", 0),
        combined=combined_score,
        valid_arcs=", ".join(VALID_ARC_TYPES),
        valid_arcs_pipe="|".join(VALID_ARC_TYPES),
        produce_score_thresh=produce_score_thresh,
        produce_density_thresh=produce_density_thresh,
        skip_score_thresh=skip_score_thresh,
    )
    return PASS2_SYSTEM, user


# ─────────────────────────────────────────────────────────────
# Stub Pass 1 result for dry-run
# ─────────────────────────────────────────────────────────────

DRY_RUN_PASS1_STUB = {
    "timeline": [
        {"source_idx": 0, "timestamp_sec": 15.0, "event": "EXAMPLE: officer arrives on scene",
         "speakers_involved": ["SPK0"], "emotional_intensity": 2}
    ],
    "moments": [
        {
            "source_idx": 0,
            "timestamp_sec": 180.5,
            "end_timestamp_sec": 205.0,
            "type": "contradiction",
            "description": "EXAMPLE: suspect denies being at the scene while surveillance video shows him there",
            "transcript_excerpt": "I was at home the whole time",
            "provisional_importance": "critical",
        },
        {
            "source_idx": 0,
            "timestamp_sec": 420.0,
            "end_timestamp_sec": 440.0,
            "type": "emotional_peak",
            "description": "EXAMPLE: victim's family member breaks down during testimony",
            "transcript_excerpt": "She was everything to us",
            "provisional_importance": "high",
        },
        {
            "source_idx": 0,
            "timestamp_sec": 600.0,
            "end_timestamp_sec": 615.0,
            "type": "reveal",
            "description": "EXAMPLE: forensics match the defendant's DNA to the weapon",
            "transcript_excerpt": "The lab report confirms...",
            "provisional_importance": "critical",
        },
    ],
    "contradictions": [
        {
            "statement_a": {"source_idx": 0, "timestamp_sec": 180.5, "speaker": "SPK1", "text": "I was at home"},
            "statement_b": {"source_idx": 1, "timestamp_sec": 45.0, "speaker": "SPK1", "text": "I only stopped by briefly"},
            "nature_of_contradiction": "EXAMPLE: subject changes story about whereabouts"
        }
    ],
    "speaker_dynamics": [
        {"speaker_pair": ["SPK0", "SPK1"], "interaction_type": "adversarial", "power_dynamic": "EXAMPLE: investigator pressing suspect"}
    ],
    "emotional_arc": [
        {"source_idx": 0, "segment_start_sec": 0, "segment_end_sec": 300, "avg_intensity": 2.0, "trend": "rising"},
        {"source_idx": 0, "segment_start_sec": 300, "segment_end_sec": 600, "avg_intensity": 3.5, "trend": "rising"},
    ],
    "factual_anchors": [
        {"type": "location", "value": "EXAMPLE: 1234 Main St", "source_idx": 0, "timestamp_sec": 25.0}
    ],
    "detected_structure_hint": "cold_open",
}
