"""
pipeline5_assemble.py — Pipeline 5: Content Assembly Brief
==========================================================

Merges outputs from all upstream pipelines into a single producer-ready
brief. No new LLM calls — this is a pure merge + formatter.

Inputs (any combination, at minimum P4 verdict):
  - P2 case research JSON        (discovered_cases/{case_id}.json)
  - P3 transcript JSON(s)        (pipeline3_audio/transcripts/{case_id}*_transcript.json)
  - P4 verdict JSON              (pipeline4_scoring/verdicts/{case_id}_verdict.json)  REQUIRED
  - P1 scoring_weights.json      (pipeline1_winners/scoring_weights.json) — optional

Output:
  - {case_id}_brief.json  — machine-readable merged dossier
  - {case_id}_brief.md    — human-readable production brief

CLI:
    # Single case — specify P4 verdict, auto-discover the rest by case_id
    python pipeline5_assemble.py --verdict ../pipeline4_scoring/verdicts/sfdpa_0409-18_verdict.json

    # Fully explicit
    python pipeline5_assemble.py \\
        --verdict ../pipeline4_scoring/verdicts/sfdpa_0409-18_verdict.json \\
        --case-research ../discovered_cases/sfdpa_0409-18.json \\
        --transcript-dir ../pipeline3_audio/transcripts \\
        --weights ../pipeline1_winners/scoring_weights.json \\
        --output briefs/

    # Batch all verdicts
    python pipeline5_assemble.py --verdict-dir ../pipeline4_scoring/verdicts --output briefs/

    # Dry run — print brief.json to stdout, no files written
    python pipeline5_assemble.py --verdict ../pipeline4_scoring/verdicts/sfdpa_0409-18_verdict.json --dry-run
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).parent.resolve()
REPO = HERE.parent

DEFAULT_OUTPUT = HERE / "briefs"
DEFAULT_VERDICT_DIR = REPO / "pipeline4_scoring" / "verdicts"
DEFAULT_TRANSCRIPT_DIR = REPO / "pipeline3_audio" / "transcripts"
DEFAULT_CASE_RESEARCH_DIR = REPO / "discovered_cases"
DEFAULT_WEIGHTS = REPO / "pipeline1_winners" / "scoring_weights.json"


# ─────────────────────────────────────────────────────────────
# I/O helpers
# ─────────────────────────────────────────────────────────────

def _load_json(path):
    if not path or not Path(path).exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"  [WARN] failed to read {path}: {e}", file=sys.stderr)
        return None


def _discover_transcripts(case_id, transcript_dir):
    """
    Find all P3 transcript files whose case_id matches. Supports multi-source
    cases where P3 wrote several files (e.g. sfdpa_0409-18_bwc_* + _interview_*).
    """
    td = Path(transcript_dir)
    if not td.exists():
        return []
    matches = []
    for path in sorted(td.glob("*_transcript.json")):
        d = _load_json(path)
        if d and d.get("case_id") == case_id:
            matches.append((path, d))
    # Fallback: prefix match on filename in case case_id field wasn't matched
    if not matches:
        prefix = f"{case_id}_"
        for path in sorted(td.glob(f"{prefix}*_transcript.json")):
            d = _load_json(path)
            if d:
                matches.append((path, d))
    return matches


def _discover_case_research(case_id, case_research_dir):
    """Look up P2 case JSON by case_id filename convention."""
    candidates = [
        Path(case_research_dir) / f"{case_id}.json",
    ]
    for c in candidates:
        d = _load_json(c)
        if d:
            return c, d
    return None, None


# ─────────────────────────────────────────────────────────────
# Brief construction
# ─────────────────────────────────────────────────────────────

def _fmt_timestamp(sec):
    """Human-friendly H:MM:SS from seconds."""
    if sec is None:
        return "?"
    try:
        sec = int(float(sec))
    except (TypeError, ValueError):
        return str(sec)
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


# Importance ordering used by the deterministic beat sheet selector.
_BEAT_IMPORTANCE_RANK = {"critical": 4, "high": 3, "medium": 2, "low": 1}

# Moment types that make good cold-open / hook beats.
_BEAT_HOOK_TYPES = {"reveal", "emotional_peak", "tension_shift"}

# Approximate runtime fractions per beat. Sum to 1.0. Tuned to mirror
# the typical winner-channel pacing called out in CLAUDE.md (cold open
# short, escalation longest, climax tight, aftermath light).
_BEAT_RUNTIME_FRACTIONS = [
    ("hook",        0.00, 0.15),
    ("setup",       0.15, 0.30),
    ("escalation",  0.30, 0.65),
    ("climax",      0.65, 0.85),
    ("aftermath",   0.85, 1.00),
]

# Generic descriptions used when no key_moment is available for a beat
# slot. Keeps the brief useful even when P4 returned few moments.
_BEAT_GENERIC_DESCRIPTION = {
    "hook":       "Open with case context or strongest available beat",
    "setup":      "Establish stakes, defendant, jurisdiction",
    "escalation": "Build to the central conflict / contradiction",
    "climax":     "Land the strongest narrative moment",
    "aftermath":  "Outcome / context wrap",
}


def _build_beat_sheet(narrative_arc, estimated_runtime_min, key_moments):
    """Pure helper: build a deterministic 5-beat sheet from
    P4 verdict signals.

    The beat sheet is an EDITORIAL SUGGESTION, not a scoring decision.
    It does not call an LLM, never mutates inputs, and falls back to
    generic beat rows when key_moments is empty or sparse.

    Selection order:
      1. hook   -- highest-importance reveal/emotional_peak/tension_shift,
                   else first chronological critical/high
      2. climax -- highest-importance critical/high not yet used by hook
      3. escalation -- next best high/medium not yet used
      4. setup  -- earliest chronological remaining moment
      5. aftermath -- latest chronological remaining moment

    Each beat is assigned an approximate minute range from
    estimated_runtime_min via _BEAT_RUNTIME_FRACTIONS. Defaults to a
    12-minute runtime if estimated_runtime_min is missing or invalid.
    """
    try:
        runtime = float(estimated_runtime_min) if estimated_runtime_min else 12.0
    except (TypeError, ValueError):
        runtime = 12.0
    if runtime <= 0:
        runtime = 12.0

    moments = list(key_moments or [])

    def _imp_rank(m):
        return _BEAT_IMPORTANCE_RANK.get(m.get("importance", ""), 0)

    def _chrono_key(m):
        return (m.get("source_idx", 0), m.get("timestamp_sec") or 0)

    used_ids = set()

    def _select_hook():
        eligible = [m for m in moments
                    if m.get("moment_type") in _BEAT_HOOK_TYPES]
        if eligible:
            return max(eligible, key=_imp_rank)
        eligible = [m for m in moments
                    if m.get("importance") in {"critical", "high"}]
        if eligible:
            return min(eligible, key=_chrono_key)
        return None

    def _select_climax():
        eligible = [m for m in moments
                    if m.get("importance") in {"critical", "high"}
                    and id(m) not in used_ids]
        if eligible:
            return max(eligible, key=_imp_rank)
        return None

    def _select_escalation():
        eligible = [m for m in moments
                    if m.get("importance") in {"high", "medium"}
                    and id(m) not in used_ids]
        if eligible:
            return max(eligible, key=_imp_rank)
        return None

    def _select_setup():
        eligible = [m for m in moments if id(m) not in used_ids]
        if eligible:
            return min(eligible, key=_chrono_key)
        return None

    def _select_aftermath():
        eligible = [m for m in moments if id(m) not in used_ids]
        if eligible:
            return max(eligible, key=_chrono_key)
        return None

    hook = _select_hook()
    if hook is not None:
        used_ids.add(id(hook))
    climax = _select_climax()
    if climax is not None:
        used_ids.add(id(climax))
    escalation = _select_escalation()
    if escalation is not None:
        used_ids.add(id(escalation))
    setup = _select_setup()
    if setup is not None:
        used_ids.add(id(setup))
    aftermath = _select_aftermath()
    if aftermath is not None:
        used_ids.add(id(aftermath))

    selections = {
        "hook": hook, "setup": setup, "escalation": escalation,
        "climax": climax, "aftermath": aftermath,
    }

    beats = []
    for name, start_pct, end_pct in _BEAT_RUNTIME_FRACTIONS:
        m = selections[name]
        beat = {
            "beat": name,
            "start_min": round(runtime * start_pct, 1),
            "end_min": round(runtime * end_pct, 1),
        }
        if m is not None:
            beat["moment_type"] = m.get("moment_type")
            beat["moment_importance"] = m.get("importance")
            beat["moment_description"] = m.get("description") or _BEAT_GENERIC_DESCRIPTION[name]
            beat["moment_timestamp_sec"] = m.get("timestamp_sec")
            beat["moment_source_idx"] = m.get("source_idx", 0)
        else:
            beat["moment_type"] = None
            beat["moment_importance"] = None
            beat["moment_description"] = _BEAT_GENERIC_DESCRIPTION[name]
            beat["moment_timestamp_sec"] = None
            beat["moment_source_idx"] = None
        beats.append(beat)

    return {
        "narrative_arc": narrative_arc,
        "estimated_runtime_min": runtime,
        "beats": beats,
    }


_VIDEO_SHARING_HOSTS = ("youtube.com", "youtu.be", "vimeo.com")


def _source_download_command(source):
    """Pure helper: derive a download/action instruction for one P2
    source dict. No network calls, no URL fetching, no classification
    beyond what the source dict itself carries.

    Returns the human-facing command string. Three categories:
      - YouTube / Vimeo / video-sharing hosts -> ``yt-dlp "URL"``
      - format in {video, audio, document}    -> ``Direct download: URL``
      - everything else (webpage, unknown)     -> ``Open/review manually: URL``

    The ``requires_download`` flag escalates a video/audio source to
    yt-dlp regardless of host (e.g., MuckRock CDN videos that need
    extraction).
    """
    if not source:
        return "Open/review manually: (no url)"
    url = (source.get("url") or "").strip()
    if not url:
        return "Open/review manually: (no url)"

    url_lower = url.lower()
    if any(h in url_lower for h in _VIDEO_SHARING_HOSTS):
        return f'yt-dlp "{url}"'

    fmt = (source.get("format") or "").lower()
    requires_dl = bool(source.get("requires_download", False))

    if requires_dl and fmt in {"video", "audio"}:
        return f'yt-dlp "{url}"'

    if fmt in {"video", "audio", "document"}:
        return f"Direct download: {url}"

    return f"Open/review manually: {url}"


def _with_download_command(source):
    """Return a copy of ``source`` with a ``download_command`` field
    added. Pure -- the input dict is never mutated, mirroring the
    ``_add_clip_boundaries`` pattern."""
    out = dict(source) if source else {}
    out["download_command"] = _source_download_command(source)
    return out


def _add_clip_boundaries(moment):
    """Return a copy of `moment` with `clip_start_sec` / `clip_end_sec`
    fields added for editor convenience.

    Defaults:
      clip_start_sec = max(0, timestamp_sec - 5)
      clip_end_sec   = (end_timestamp_sec + 3) if end_timestamp_sec
                       exists else (timestamp_sec + 3)

    Original `timestamp_sec` / `end_timestamp_sec` are preserved
    unchanged. Returns the moment dict unmodified (no clip_* fields)
    if `timestamp_sec` is missing or not numeric -- defensive against
    malformed key_moments entries from older P4 verdicts or edge cases.
    """
    out = dict(moment)
    ts = moment.get("timestamp_sec")
    if ts is None:
        return out
    try:
        ts_f = float(ts)
    except (TypeError, ValueError):
        return out
    out["clip_start_sec"] = max(0.0, ts_f - 5.0)
    end_ts = moment.get("end_timestamp_sec")
    if end_ts is not None:
        try:
            out["clip_end_sec"] = float(end_ts) + 3.0
        except (TypeError, ValueError):
            out["clip_end_sec"] = ts_f + 3.0
    else:
        out["clip_end_sec"] = ts_f + 3.0
    return out


def _assemble_production_caveats(verdict):
    """Read advisory caveats off the P4 verdict + metadata.

    Pure read-only — never modifies the verdict. All lookups defensive
    so verdict files predating Batch 2 (no resolution_status, no
    production_status_flag) still produce a valid (empty) caveats dict.

    Returns a dict with the full caveat surface plus a `has_any` flag
    that controls whether render_markdown emits the Production caveats
    section. `has_any` is True iff at least one human-relevant caveat
    fires: a non-null production_status_flag, a Pass-2-fallback
    `degraded` run, or a gate-applied verdict cap.
    """
    verdict = verdict or {}
    md = verdict.get("_pipeline4_metadata") or {}
    caveats = {
        "resolution_status": verdict.get("resolution_status"),
        "production_status_flag": verdict.get("production_status_flag"),
        "degraded": bool(md.get("degraded", False)),
        "resolution_gate_enabled": md.get("resolution_gate_enabled"),
        "resolution_gate_applied": md.get("resolution_gate_applied"),
        "pre_gate_verdict": md.get("pre_gate_verdict"),
        "resolution_source": md.get("resolution_source"),
    }
    caveats["has_any"] = bool(
        caveats["production_status_flag"]
        or caveats["degraded"]
        or caveats["resolution_gate_applied"]
    )
    return caveats


def build_brief(verdict, case_research, transcripts, weights):
    """
    Merge all upstream outputs into a unified brief dict.
    Returns a dict shaped for the {case_id}_brief.json artifact.
    """
    case_id = verdict["case_id"]

    # Sources from P2 (typed URLs), fallback empty
    p2_sources = []
    if case_research:
        p2_sources = case_research.get("sources", [])

    # Transcript manifest (path + evidence_type + duration)
    transcript_manifest = []
    for path, t in transcripts:
        transcript_manifest.append({
            "transcript_path": str(path.resolve()),
            "source_evidence_type": t.get("source_evidence_type"),
            "source_url": t.get("source_url"),
            "original_duration_sec": t.get("original_duration_sec"),
            "processed_duration_sec": t.get("processed_duration_sec"),
            "speaker_count": t.get("speaker_count"),
            "segment_count": len(t.get("transcript", [])),
        })

    # Suggested narrative arc: prefer P4 recommendation, fallback to P1's most-frequent winner arc
    narrative_arc = verdict.get("narrative_arc_recommendation") or "chronological"
    if weights:
        arc_patterns = sorted(
            weights.get("arc_patterns", []),
            key=lambda a: a.get("frequency", 0), reverse=True,
        )
        if arc_patterns and not verdict.get("narrative_arc_recommendation"):
            narrative_arc = arc_patterns[0]["structure_type"]

    # Key moments ordered by timestamp for editor-friendly timeline view.
    # Each moment is enriched with clip_start_sec / clip_end_sec for
    # editor convenience; original timestamp_sec / end_timestamp_sec
    # are preserved unchanged.
    key_moments = [
        _add_clip_boundaries(m)
        for m in sorted(
            verdict.get("key_moments", []) or [],
            key=lambda m: (m.get("source_idx", 0), m.get("timestamp_sec") or 0),
        )
    ]

    # Sources grouped by evidence type (makes markdown brief cleaner).
    # Each source is enriched with a download_command for editor
    # convenience; original case_research source dicts are not mutated.
    sources_by_type = {}
    for s in p2_sources:
        et = s.get("evidence_type", "other")
        sources_by_type.setdefault(et, []).append(_with_download_command(s))

    brief = {
        "case_id": case_id,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "verdict": verdict.get("verdict"),
        "narrative_score": verdict.get("narrative_score"),
        "confidence": verdict.get("confidence"),
        "content_pitch": verdict.get("content_pitch", ""),
        "narrative_arc_recommendation": narrative_arc,
        "estimated_runtime_min": verdict.get("estimated_runtime_min"),
        "case_summary": {
            "defendant": (case_research or {}).get("defendant"),
            "jurisdiction": (case_research or {}).get("jurisdiction"),
            "charges": (case_research or {}).get("charges", []),
            "incident_date": (case_research or {}).get("incident_date"),
            "summary_text": (case_research or {}).get("summary", ""),
            "confidence_tier": (case_research or {}).get("confidence_tier"),
            "research_score": (case_research or {}).get("research_score"),
        },
        "artifact_completeness": verdict.get("artifact_completeness", {}),
        "scoring_breakdown": verdict.get("scoring_breakdown", {}),
        "key_moments": key_moments,
        "beat_sheet": _build_beat_sheet(
            narrative_arc=narrative_arc,
            estimated_runtime_min=verdict.get("estimated_runtime_min"),
            key_moments=key_moments,
        ),
        "sources_by_type": sources_by_type,
        "transcripts": transcript_manifest,
        "production_caveats": _assemble_production_caveats(verdict),
        "_inputs": {
            "p2_case_research_present": case_research is not None,
            "p3_transcripts_count": len(transcript_manifest),
            "p4_verdict_present": True,
            "p1_weights_present": weights is not None,
        },
    }
    return brief


def render_markdown(brief):
    """Render the human-facing brief.md."""
    lines = []
    cid = brief["case_id"]
    cs = brief["case_summary"]

    lines.append(f"# Production Brief — {cid}")
    lines.append("")
    lines.append(f"**Verdict:** {brief['verdict']}  ")
    lines.append(f"**Narrative score:** {brief.get('narrative_score', 0):.1f} / 100  ")
    if brief.get("confidence") is not None:
        lines.append(f"**Confidence:** {brief['confidence']:.2f}  ")
    if brief.get("estimated_runtime_min"):
        lines.append(f"**Estimated runtime:** {brief['estimated_runtime_min']:.1f} min  ")
    lines.append(f"**Recommended arc:** {brief.get('narrative_arc_recommendation', 'chronological')}")
    lines.append("")

    # Case summary -- suppress entire section when no P2 research
    # data is available. Renders only when at least one case-summary
    # field has content.
    case_summary_has_content = any([
        cs.get("defendant"),
        cs.get("jurisdiction"),
        cs.get("charges"),
        cs.get("incident_date"),
        cs.get("confidence_tier"),
        cs.get("research_score") is not None,
        cs.get("summary_text"),
    ])
    if case_summary_has_content:
        lines.append("## Case summary")
        lines.append("")
        if cs.get("defendant"):
            lines.append(f"- **Defendant:** {cs['defendant']}")
        if cs.get("jurisdiction"):
            lines.append(f"- **Jurisdiction:** {cs['jurisdiction']}")
        if cs.get("charges"):
            lines.append(f"- **Charges:** {', '.join(cs['charges'])}")
        if cs.get("incident_date"):
            lines.append(f"- **Incident date:** {cs['incident_date']}")
        if cs.get("confidence_tier"):
            lines.append(f"- **P2 tier:** {cs['confidence_tier']}")
        if cs.get("research_score") is not None:
            lines.append(f"- **P2 research_score:** {cs['research_score']:.1f}")
        if cs.get("summary_text"):
            lines.append("")
            lines.append(cs["summary_text"])
        lines.append("")

    # Content pitch
    if brief.get("content_pitch"):
        lines.append("## Pitch")
        lines.append("")
        lines.append(brief["content_pitch"])
        lines.append("")

    # Production caveats (advisory -- never modifies the verdict).
    # Build per-bullet first; emit the section heading only if at
    # least one bullet survives. This handles the edge case where
    # `has_any` is True solely because of `resolution_gate_applied`
    # but the gate-cap bullet's defensive guard suppresses it (e.g.
    # synthetic / malformed input where pre_gate_verdict equals the
    # emitted verdict). Keeps the production_caveats JSON unchanged.
    caveats = brief.get("production_caveats") or {}
    if caveats.get("has_any"):
        caveat_bullets = []
        flag = caveats.get("production_status_flag")
        status = caveats.get("resolution_status")
        if flag:
            caveat_bullets.append(
                f"- **Production note: {flag}** "
                f"(`resolution_status` = `{status}`)"
            )
        if caveats.get("degraded"):
            caveat_bullets.append(
                "- **Pass 2 fallback** -- the LLM judgment step "
                "failed; verdict fell back to deterministic-only "
                "scoring. Treat as lower-confidence."
            )
        if caveats.get("resolution_gate_applied"):
            pre = caveats.get("pre_gate_verdict")
            emitted = brief.get("verdict")
            # Only render the cap line when the cap actually changed
            # the verdict. In real runs apply_resolution_gate sets
            # gate_applied=True only when pre != emitted, but be
            # defensive against synthetic / malformed inputs.
            if pre and emitted and pre != emitted:
                caveat_bullets.append(
                    f"- **Verdict capped by resolution gate** -- "
                    f"would have been `{pre}` without the gate; "
                    f"emitted as `{emitted}`."
                )
        if caveat_bullets:
            lines.append("## Production caveats")
            lines.append("")
            lines.append(
                "> Advisory notes for human review -- these do NOT "
                "modify or override the verdict above."
            )
            lines.append("")
            lines.extend(caveat_bullets)
            lines.append("")

    # Artifact completeness
    ac = brief.get("artifact_completeness") or {}
    if ac:
        lines.append("## Artifacts")
        lines.append("")
        if ac.get("available"):
            lines.append(f"- **Available:** {', '.join(ac['available'])}")
        if ac.get("missing_recommended"):
            lines.append(f"- **Missing (winners typically have):** {', '.join(ac['missing_recommended'])}")
        lines.append("")

    # Key moments
    if brief.get("key_moments"):
        lines.append("## Key moments (editor timeline)")
        lines.append("")
        lines.append("| src | timestamp | type | importance | description |")
        lines.append("| --- | --- | --- | --- | --- |")
        for m in brief["key_moments"]:
            lines.append(
                f"| {m.get('source_idx', 0)} "
                f"| {_fmt_timestamp(m.get('timestamp_sec'))} "
                f"| {m.get('moment_type', '?')} "
                f"| {m.get('importance', '?')} "
                f"| {(m.get('description') or '').replace('|', '\\|')[:120]} |"
            )
        lines.append("")

        # Moment details: clip-boundary suggestion per moment
        # (always rendered when boundaries exist), plus optional
        # transcript excerpt below the clip line. Replaces the prior
        # "Moment excerpts" block so every moment surfaces its
        # editor-friendly clip range, not just moments that happen
        # to carry an excerpt.
        lines.append("### Moment details")
        lines.append("")
        for m in brief["key_moments"]:
            t = _fmt_timestamp(m.get("timestamp_sec"))
            desc = (m.get("description") or "")
            lines.append(f"- **[{t}]** {desc}")
            clip_start_sec = m.get("clip_start_sec")
            clip_end_sec = m.get("clip_end_sec")
            if clip_start_sec is not None and clip_end_sec is not None:
                clip_start = _fmt_timestamp(clip_start_sec)
                clip_end = _fmt_timestamp(clip_end_sec)
                lines.append(f"  Clip suggestion: {clip_start} -> {clip_end}")
            if m.get("transcript_excerpt"):
                lines.append(f"  > {m['transcript_excerpt']}")
        lines.append("")

    # Narrative Arc + Suggested Beat Sheet (deterministic editorial
    # suggestion -- not a scoring decision; never modifies the verdict).
    bs = brief.get("beat_sheet") or {}
    beats = bs.get("beats") or []
    if beats:
        arc = bs.get("narrative_arc") or "chronological"
        lines.append(f"## Narrative Arc: {arc}")
        lines.append("")
        lines.append("### Suggested Beat Sheet")
        lines.append("")
        lines.append("| Beat | Timing (min) | Moment | Description |")
        lines.append("| --- | --- | --- | --- |")
        for beat in beats:
            name = beat.get("beat", "?")
            start_min = beat.get("start_min", 0)
            end_min = beat.get("end_min", 0)
            mtype = beat.get("moment_type")
            imp = beat.get("moment_importance")
            if mtype:
                moment_label = f"{mtype} / {imp}" if imp else mtype
            else:
                moment_label = "(generic)"
            desc = (beat.get("moment_description") or "").replace("|", r"\|")[:160]
            lines.append(
                f"| {name} | {start_min} - {end_min} | {moment_label} | {desc} |"
            )
        lines.append("")

    # Sources grouped by evidence_type
    sbt = brief.get("sources_by_type") or {}
    if sbt:
        lines.append("## Sources by evidence type")
        lines.append("")
        for et in sorted(sbt.keys()):
            items = sbt[et]
            lines.append(f"### {et} ({len(items)})")
            lines.append("")
            for s in items:
                domain = s.get("source_domain") or s.get("url", "")
                url = s.get("url", "")
                cmd = s.get("download_command") or ""
                note = f" -- {s['notes']}" if s.get("notes") else ""
                if cmd:
                    lines.append(f"- [{domain}]({url}) -- `{cmd}`{note}")
                else:
                    lines.append(f"- [{domain}]({url}){note}")
            lines.append("")

    # Transcripts manifest
    if brief.get("transcripts"):
        lines.append("## Transcripts used")
        lines.append("")
        for t in brief["transcripts"]:
            dur = t.get("original_duration_sec") or 0
            lines.append(
                f"- `{Path(t['transcript_path']).name}` — "
                f"{t.get('source_evidence_type', '?')} | "
                f"{dur:.0f}s ({dur/60:.1f}m) | "
                f"{t.get('segment_count', 0)} segments"
            )
        lines.append("")

    # Scoring breakdown
    sb = brief.get("scoring_breakdown") or {}
    if sb:
        lines.append("## Scoring breakdown")
        lines.append("")
        for k, v in sb.items():
            try:
                lines.append(f"- {k}: {float(v):.2f}")
            except (TypeError, ValueError):
                lines.append(f"- {k}: {v}")
        lines.append("")

    # Inputs audit
    inp = brief.get("_inputs") or {}
    lines.append("## Assembly provenance")
    lines.append("")
    lines.append(f"- Generated: {brief['generated_at']}")
    for k, v in inp.items():
        lines.append(f"- {k}: {v}")
    lines.append("")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────

VALID_VERDICTS = {"PRODUCE", "HOLD", "SKIP"}

def validate_brief(brief):
    """Minimal sanity checks. Returns (ok, errors)."""
    errors = []
    if not brief.get("case_id"):
        errors.append("missing case_id")
    if brief.get("verdict") not in VALID_VERDICTS:
        errors.append(f"verdict must be PRODUCE|HOLD|SKIP (got {brief.get('verdict')!r})")
    ns = brief.get("narrative_score")
    if ns is None or not (0 <= float(ns) <= 100):
        errors.append(f"narrative_score out of range: {ns!r}")
    return (len(errors) == 0, errors)


# ─────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────

def assemble_one(verdict_path, case_research_path, transcript_dir, weights_path, dry_run, output_dir):
    verdict = _load_json(verdict_path)
    if not verdict:
        print(f"[ERR] verdict not loadable: {verdict_path}")
        return None
    case_id = verdict.get("case_id")
    if not case_id:
        print(f"[ERR] verdict has no case_id: {verdict_path}")
        return None

    # Resolve the other inputs (explicit → auto-discover)
    case_research = _load_json(case_research_path) if case_research_path else None
    if case_research is None:
        _, case_research = _discover_case_research(case_id, DEFAULT_CASE_RESEARCH_DIR)

    transcripts = _discover_transcripts(case_id, transcript_dir or DEFAULT_TRANSCRIPT_DIR)
    weights = _load_json(weights_path or DEFAULT_WEIGHTS)

    brief = build_brief(verdict, case_research, transcripts, weights)
    ok, errors = validate_brief(brief)
    if not ok:
        print(f"  [VALIDATION FAIL] {errors}")
        return None

    md = render_markdown(brief)

    if dry_run:
        print(f"\n{'=' * 70}\n[DRY RUN] BRIEF — {case_id}\n{'=' * 70}")
        print(json.dumps(brief, indent=2, ensure_ascii=False, default=str))
        print(f"\n{'=' * 70}\n[DRY RUN] MARKDOWN PREVIEW\n{'=' * 70}")
        print(md)
        return brief

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"{case_id}_brief.json"
    md_path = out_dir / f"{case_id}_brief.md"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(brief, f, indent=2, ensure_ascii=False, default=str)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"  ✓ {json_path.name}  ({len(brief.get('key_moments', []))} moments, "
          f"{sum(len(v) for v in (brief.get('sources_by_type') or {}).values())} sources, "
          f"{len(brief.get('transcripts', []))} transcripts)")
    print(f"  ✓ {md_path.name}")
    return brief


# ─────────────────────────────────────────────────────────────
# Packet-mode adapter (post-PR #47)
# ─────────────────────────────────────────────────────────────
#
# Pipeline 1's stratified-random YouTube smoke produces packet stubs
# (see ``.tmp/packet_production_smoke/packets_master.jsonl``). Those
# stubs are P1-lane outputs and never flow through P2 → P3 → P4
# scoring; the existing ``--verdict`` mode therefore cannot consume
# them.
#
# This adapter adds an additive ``--packet`` mode that:
#   1. Reads a single packet stub JSON.
#   2. Loads cached YouTube caption ``.txt`` transcripts from a
#      directory (recursively).
#   3. Builds a deterministic brief dict mirroring the 11-section
#      shape of the hand-assembled top-5 briefs in ``.tmp/p5_briefs/``.
#   4. Renders Markdown with the same section layout.
#
# The packet-mode brief is intentionally a different output shape
# from the verdict-mode brief — packet stubs lack the P4 scoring
# signals (key_moments, narrative_score, beat-sheet importance
# rankings) that the verdict-mode brief leans on. We do not invent
# those signals; we work with what the packet carries.
#
# Existing ``--verdict`` mode is unchanged.


_PACKET_MAX_PARAGRAPHS_PER_TRANSCRIPT = 6
_PACKET_MIN_PARAGRAPH_CHARS = 40


def _load_packet_transcripts(transcript_dir):
    """Load cached caption ``.txt`` files from ``transcript_dir`` (recursive).

    Returns a list of ``{path, text, basename, parent_dirname}`` dicts in
    sorted-path order so output is deterministic. Empty / unreadable
    files are skipped.

    The caption text format is the output of
    ``tools/extract_youtube_transcripts.py`` — a flat plaintext rendering
    of the VTT captions. We do not parse speaker turns; the packet
    pipeline never produced them.
    """
    td = Path(transcript_dir)
    if not td.exists():
        return []
    out = []
    for path in sorted(td.rglob("*.txt")):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        text = text.strip()
        if not text:
            continue
        out.append({
            "path": str(path),
            "text": text,
            "basename": path.name,
            "parent_dirname": path.parent.name,
        })
    return out


def _chunk_packet_paragraphs(text, max_chunks=_PACKET_MAX_PARAGRAPHS_PER_TRANSCRIPT):
    """Split a caption text blob into deterministic paragraph chunks.

    yt-dlp caption text often comes in short single-line fragments,
    so we group every ~6 non-empty lines into a chunk and stop after
    ``max_chunks``. A chunk shorter than
    ``_PACKET_MIN_PARAGRAPH_CHARS`` is discarded as noise (header
    fragments like ``Kind: captions`` / ``Language: en`` filter out).
    """
    lines = [ln.strip() for ln in (text or "").splitlines()]
    chunks = []
    current = []
    GROUP = 6
    for ln in lines:
        if not ln:
            if current:
                joined = " ".join(current).strip()
                if len(joined) >= _PACKET_MIN_PARAGRAPH_CHARS:
                    chunks.append(joined)
                current = []
                if len(chunks) >= max_chunks:
                    break
            continue
        # Skip VTT/file-format headers
        if ln.lower().startswith(("kind:", "language:")):
            continue
        current.append(ln)
        if len(current) >= GROUP:
            joined = " ".join(current).strip()
            if len(joined) >= _PACKET_MIN_PARAGRAPH_CHARS:
                chunks.append(joined)
            current = []
            if len(chunks) >= max_chunks:
                break
    if current and len(chunks) < max_chunks:
        joined = " ".join(current).strip()
        if len(joined) >= _PACKET_MIN_PARAGRAPH_CHARS:
            chunks.append(joined)
    return chunks


def _packet_safe_id(packet):
    """Return a filesystem-safe id for a packet: prefer packet_id,
    fall back to candidate_id, then a hash-ish stub."""
    pid = packet.get("packet_id") or packet.get("candidate_id") or "packet"
    return str(pid).replace(":", "_").replace("/", "_")


_KEY_PEOPLE_GAP_NOTE = (
    "Packet schema does not carry involved_officers[] or "
    "family_decedent[]. Hand-assembled briefs extracted "
    "these from transcripts; adapter mode flags the gap "
    "for follow-up."
)

_GENERIC_PRODUCTION_ANGLE = {
    "rank": 1,
    "title": "(packet mode: angle not synthesized — hand-curate from case_summary + transcripts)",
    "recommended": True,
    "risks": [],
}


def _normalize_officer(o):
    """Coerce one involved_officers[] entry into a renderable dict.

    Accepts either a string (name only) or a dict carrying any subset of
    ``name / role / status / agency / badge``.
    """
    if isinstance(o, str):
        return {"name": o, "role": None, "status": None, "agency": None, "badge": None}
    if not isinstance(o, dict) or not o.get("name"):
        return None
    return {
        "name": o.get("name"),
        "role": o.get("role"),
        "status": o.get("status"),
        "agency": o.get("agency"),
        "badge": o.get("badge"),
    }


def _normalize_relation(r):
    """Coerce one family_decedent.primary_relations[] entry into a renderable dict."""
    if isinstance(r, str):
        return {"name": r, "relationship": None}
    if not isinstance(r, dict) or not r.get("name"):
        return None
    return {"name": r.get("name"), "relationship": r.get("relationship")}


def _normalize_timeline_event(ev):
    """Coerce one timeline_events[] entry into a renderable dict."""
    if not isinstance(ev, dict):
        return None
    if not (ev.get("date") or ev.get("event")):
        return None
    return {
        "date": ev.get("date"),
        "event": ev.get("event"),
        "source": ev.get("source"),
    }


def _normalize_production_angle(a):
    """Coerce one production_angles[] entry into a renderable dict."""
    if not isinstance(a, dict) or not a.get("title"):
        return None
    return {
        "rank": a.get("rank"),
        "title": a.get("title"),
        "recommended": bool(a.get("recommended", False)),
        "risks": list(a.get("risks") or []),
    }


def _normalize_case_outcome(co):
    """Coerce a case_outcome dict; return ``{}`` for missing/empty."""
    if not isinstance(co, dict):
        return {}
    fields = ("conviction_status", "sentence", "doj_url", "civil_suit_status", "court")
    out = {k: co.get(k) for k in fields if co.get(k)}
    return out


def build_packet_brief(packet, transcripts):
    """Build a brief dict from a packet stub + cached caption transcripts.

    Output shape mirrors the hand-assembled top-5 briefs in
    ``.tmp/p5_briefs/`` (11 sections). All values are derived
    deterministically from the packet + transcript text — no LLM,
    no fact synthesis. Missing packet fields fall back to ``None``.

    Optional structured fields (additive, all default-empty):
      - ``involved_officers[]`` → section 4 Key people (officers).
      - ``family_decedent``     → section 4 Key people (family + emotional anchors).
      - ``timeline_events[]``   → section 5 Timeline (replaces synthesized
                                  incident-only event when present).
      - ``narrative_spine``     → section 3 Narrative spine (any subset of
                                  setup/pursuit/death/investigation/outcome
                                  is rendered; packet may also use the legacy
                                  setup/incident/outcome shape).
      - ``production_angles[]`` → section 10 Production angle (replaces the
                                  generic hand-curation-gap placeholder).
      - ``case_outcome``        → sections 2 Case summary and 8 Why it matters
                                  (conviction_status / sentence / doj_url /
                                  civil_suit_status / court).
    """
    packet = packet or {}
    transcripts = transcripts or []

    # Per-transcript evidence: chunk into paragraphs, keep a path
    # provenance for every chunk.
    transcript_evidence = []
    for t in transcripts:
        chunks = _chunk_packet_paragraphs(t["text"])
        for i, chunk in enumerate(chunks):
            transcript_evidence.append({
                "source_path": t["path"],
                "source_basename": t["basename"],
                "source_video_dir": t["parent_dirname"],
                "chunk_index": i,
                "speaker": "unknown",
                "kind": "cached_caption_text",
                "excerpt": chunk,
            })

    # Source URLs — split into "official/court" vs "youtube" for the
    # rendered brief's evidence section.
    source_urls = list(packet.get("source_urls") or [])
    youtube_urls = [u for u in source_urls if "youtube.com" in u or "youtu.be" in u]
    other_urls = [u for u in source_urls if u not in youtube_urls]

    # Research gaps come from packet's missing_fields + next_search_tasks.
    gaps_from_missing = list(packet.get("missing_fields") or [])
    gaps_from_tasks = list(packet.get("next_search_tasks") or [])

    # Optional structured fields — additive, with fallbacks that
    # preserve the prior placeholder shape when absent.
    spine_in = packet.get("narrative_spine")
    if isinstance(spine_in, dict) and any(spine_in.values()):
        # Pass through any non-empty keys verbatim. Renderer iterates
        # in a stable preferred order.
        narrative_spine = {k: v for k, v in spine_in.items() if v}
    else:
        narrative_spine = {
            "setup": "(packet mode: setup not synthesized; fill from transcripts + research)",
            "incident": packet.get("incident_type"),
            "outcome": "(packet mode: see case_outcome / next_search_tasks)",
        }

    officers_named = [
        n for n in (_normalize_officer(o) for o in (packet.get("involved_officers") or []))
        if n
    ]
    family_in = packet.get("family_decedent") or {}
    family_named = [
        n for n in (_normalize_relation(r) for r in (family_in.get("primary_relations") or []))
        if n
    ]
    emotional_anchors = [a for a in (family_in.get("emotional_anchors") or []) if a]
    if officers_named or family_named or emotional_anchors:
        key_people = {
            "officers_named": officers_named,
            "family_named": family_named,
            "emotional_anchors": emotional_anchors,
            "structured_field_missing_note": None,
        }
    else:
        key_people = {
            "officers_named": [],
            "family_named": [],
            "emotional_anchors": [],
            "structured_field_missing_note": _KEY_PEOPLE_GAP_NOTE,
        }

    timeline_events_in = [
        ev for ev in (_normalize_timeline_event(e) for e in (packet.get("timeline_events") or []))
        if ev
    ]
    if timeline_events_in:
        timeline = timeline_events_in
    elif packet.get("incident_date"):
        timeline = [{"date": packet.get("incident_date"), "event": packet.get("incident_type") or "incident"}]
    else:
        timeline = []

    angles_in = [
        a for a in (_normalize_production_angle(a) for a in (packet.get("production_angles") or []))
        if a
    ]
    if angles_in:
        production_angles = sorted(
            angles_in, key=lambda a: (a.get("rank") if a.get("rank") is not None else 999)
        )
    else:
        production_angles = [dict(_GENERIC_PRODUCTION_ANGLE)]

    case_outcome = _normalize_case_outcome(packet.get("case_outcome"))

    brief = {
        "brief_kind": "packet_mode",
        "brief_id": f"{_packet_safe_id(packet)}_packet_brief",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "packet_id": packet.get("packet_id"),
        "candidate_id": packet.get("candidate_id"),
        "case_identity": {
            "subject_or_case": packet.get("subject_or_case"),
            "agency": packet.get("agency"),
            "jurisdiction": packet.get("jurisdiction"),
            "incident_type": packet.get("incident_type"),
            "incident_date": packet.get("incident_date"),
            "confidence_grade": packet.get("confidence_grade"),
            "source_lane": packet.get("source_lane"),
        },
        "case_summary_text": packet.get("confidence_reason") or "",
        "case_outcome": case_outcome,
        "narrative_spine": narrative_spine,
        "key_people": key_people,
        "timeline": timeline,
        "evidence_artifacts": {
            "youtube_urls": youtube_urls,
            "other_source_urls": other_urls,
            "transcript_paths": [t["path"] for t in transcripts],
            "artifact_indicators": packet.get("artifact_indicators", {}),
            "matched_terms": packet.get("matched_terms") or [],
        },
        "transcript_evidence": transcript_evidence,
        "why_it_matters": {
            "confidence_grade": packet.get("confidence_grade"),
            "confidence_reason": packet.get("confidence_reason") or "",
            "artifact_indicators": packet.get("artifact_indicators", {}),
            "case_outcome": case_outcome,
        },
        "research_gaps": {
            "missing_fields": gaps_from_missing,
            "next_search_tasks": gaps_from_tasks,
        },
        "production_angles": production_angles,
        "next_research_tasks": gaps_from_tasks,
        "_inputs": {
            "packet_path": packet.get("_master_source_file"),
            "transcripts_loaded": len(transcripts),
            "transcript_evidence_chunks": len(transcript_evidence),
            "youtube_urls_count": len(youtube_urls),
        },
    }
    return brief


def render_packet_markdown(brief):
    """Render the packet-mode brief.md mirroring the 11-section
    structure of the hand-assembled top-5 briefs."""
    lines = []
    ci = brief.get("case_identity", {})
    pid = brief.get("packet_id") or "(no packet_id)"

    # 1. Header + Case identity
    lines.append(f"# Production Brief (packet mode): {ci.get('subject_or_case') or pid}")
    lines.append("")
    lines.append(f"**Generated**: {brief.get('generated_at')}  ")
    lines.append(f"**Brief mode**: packet-mode adapter (auto-generated from packet stub + cached caption transcripts; **no hand curation**)")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 1. Case identity")
    lines.append("")
    lines.append(f"| Field | Value |")
    lines.append(f"|---|---|")
    lines.append(f"| packet_id | `{pid}` |")
    lines.append(f"| candidate_id | `{brief.get('candidate_id') or '(none)'}` |")
    lines.append(f"| Subject / case | {ci.get('subject_or_case') or '(missing)'} |")
    lines.append(f"| Agency | {ci.get('agency') or '(missing)'} |")
    lines.append(f"| Jurisdiction | {ci.get('jurisdiction') or '(missing)'} |")
    lines.append(f"| Incident type | {ci.get('incident_type') or '(missing)'} |")
    lines.append(f"| Incident date | {ci.get('incident_date') or '(missing)'} |")
    lines.append(f"| Confidence grade | {ci.get('confidence_grade') or '(missing)'} |")
    lines.append(f"| Source lane | {ci.get('source_lane') or '(missing)'} |")
    lines.append("")

    # 2. Case summary (from confidence_reason; enriched by case_outcome when present)
    lines.append("## 2. Case summary")
    lines.append("")
    summary_text = brief.get("case_summary_text") or "(packet has no confidence_reason — fill from research)"
    lines.append(summary_text)
    co = brief.get("case_outcome") or {}
    if co:
        lines.append("")
        lines.append("**Case outcome:**")
        if co.get("conviction_status"):
            lines.append(f"- Conviction status: {co['conviction_status']}")
        if co.get("sentence"):
            lines.append(f"- Sentence: {co['sentence']}")
        if co.get("civil_suit_status"):
            lines.append(f"- Civil suit: {co['civil_suit_status']}")
        if co.get("court"):
            lines.append(f"- Court: {co['court']}")
        if co.get("doj_url"):
            lines.append(f"- DOJ: {co['doj_url']}")
    lines.append("")

    # 3. Narrative spine
    ns = brief.get("narrative_spine", {}) or {}
    lines.append("## 3. Narrative spine")
    lines.append("")
    # Render keys in a stable preferred order. Supports both the legacy
    # placeholder shape (setup/incident/outcome) and the enriched shape
    # (setup/pursuit/death/investigation/outcome). Unknown extra keys
    # are appended in dict order so a future field addition still surfaces.
    _SPINE_ORDER = ("setup", "pursuit", "death", "incident", "investigation", "outcome")
    rendered_keys = []
    for k in _SPINE_ORDER:
        if k in ns:
            rendered_keys.append(k)
    for k in ns.keys():
        if k not in rendered_keys:
            rendered_keys.append(k)
    if rendered_keys:
        for k in rendered_keys:
            label = k.capitalize()
            value = ns.get(k) or "(not in packet)"
            lines.append(f"- **{label}**: {value}")
    else:
        lines.append("_(narrative_spine not provided; fill from transcripts + research)_")
    lines.append("")

    # 4. Key people (officers + family; falls back to a gap note when packet
    # lacks both involved_officers[] and family_decedent.primary_relations[])
    kp = brief.get("key_people", {}) or {}
    lines.append("## 4. Key people")
    lines.append("")
    officers = kp.get("officers_named") or []
    family = kp.get("family_named") or []
    anchors = kp.get("emotional_anchors") or []
    if officers or family or anchors:
        if officers:
            lines.append("### Involved officers")
            for o in officers:
                if isinstance(o, dict):
                    name = o.get("name") or "(unnamed)"
                    bits = []
                    if o.get("role"):
                        bits.append(o["role"])
                    if o.get("status"):
                        bits.append(o["status"])
                    if o.get("agency"):
                        bits.append(o["agency"])
                    if o.get("badge"):
                        bits.append(f"badge {o['badge']}")
                    suffix = f" — {', '.join(bits)}" if bits else ""
                    lines.append(f"- **{name}**{suffix}")
                else:
                    lines.append(f"- {o}")
            lines.append("")
        if family:
            lines.append("### Family / decedent")
            for f in family:
                if isinstance(f, dict):
                    name = f.get("name") or "(unnamed)"
                    rel = f.get("relationship")
                    suffix = f" — {rel}" if rel else ""
                    lines.append(f"- **{name}**{suffix}")
                else:
                    lines.append(f"- {f}")
            lines.append("")
        if anchors:
            lines.append("### Emotional anchors")
            for a in anchors:
                lines.append(f"- {a}")
            lines.append("")
    else:
        lines.append(f"_{kp.get('structured_field_missing_note') or 'Not available in packet schema.'}_")
        lines.append("")

    # 5. Timeline (renders timeline_events[] when present, else falls back
    # to the synthesized incident-only event)
    lines.append("## 5. Timeline")
    lines.append("")
    timeline = brief.get("timeline", []) or []
    if timeline:
        for t in timeline:
            date = t.get("date") or "(undated)"
            event = t.get("event") or ""
            source = t.get("source")
            suffix = f" _(source: {source})_" if source else ""
            lines.append(f"- **{date}**: {event}{suffix}")
    else:
        lines.append("_(packet does not carry timeline_events[]; expand from transcripts + research)_")
    lines.append("")

    # 6. Evidence and artifacts
    ev = brief.get("evidence_artifacts", {})
    lines.append("## 6. Evidence and artifacts")
    lines.append("")
    if ev.get("youtube_urls"):
        lines.append("### YouTube sources")
        for u in ev["youtube_urls"]:
            lines.append(f"- {u}")
        lines.append("")
    if ev.get("other_source_urls"):
        lines.append("### Other sources (court / news / DOJ)")
        for u in ev["other_source_urls"]:
            lines.append(f"- {u}")
        lines.append("")
    indicators = ev.get("artifact_indicators") or {}
    if indicators:
        lines.append("### Artifact indicators (from packet)")
        for k, v in indicators.items():
            lines.append(f"- {k}: {v}")
        lines.append("")
    if ev.get("transcript_paths"):
        lines.append("### Cached transcripts")
        for p in ev["transcript_paths"]:
            lines.append(f"- `{p}`")
        lines.append("")

    # 7. Transcript / source evidence
    lines.append("## 7. Transcript / source evidence")
    lines.append("")
    te = brief.get("transcript_evidence", [])
    if not te:
        lines.append("_(no captioned transcripts available for this packet)_")
    else:
        for chunk in te:
            lines.append(f"> {chunk['excerpt']}")
            lines.append(f">")
            lines.append(f"> — `{chunk['source_basename']}` (chunk #{chunk['chunk_index']})")
            lines.append("")

    # 8. Why this case matters (enriched by case_outcome when present)
    wm = brief.get("why_it_matters", {}) or {}
    lines.append("## 8. Why this case matters")
    lines.append("")
    grade = wm.get("confidence_grade") or "(unknown)"
    lines.append(f"**Confidence grade**: {grade}")
    lines.append("")
    if wm.get("confidence_reason"):
        lines.append(wm["confidence_reason"])
        lines.append("")
    co_wm = wm.get("case_outcome") or {}
    if co_wm:
        lines.append("**Case outcome:**")
        if co_wm.get("conviction_status"):
            lines.append(f"- Conviction status: {co_wm['conviction_status']}")
        if co_wm.get("sentence"):
            lines.append(f"- Sentence: {co_wm['sentence']}")
        if co_wm.get("civil_suit_status"):
            lines.append(f"- Civil suit: {co_wm['civil_suit_status']}")
        if co_wm.get("court"):
            lines.append(f"- Court: {co_wm['court']}")
        if co_wm.get("doj_url"):
            lines.append(f"- DOJ: {co_wm['doj_url']}")
        lines.append("")

    # 9. Missing fields / research gaps
    rg = brief.get("research_gaps", {})
    lines.append("## 9. Missing fields / research gaps")
    lines.append("")
    if rg.get("missing_fields"):
        lines.append("### Missing fields (from packet)")
        for m in rg["missing_fields"]:
            lines.append(f"- {m}")
        lines.append("")
    if rg.get("next_search_tasks"):
        lines.append("### Next search tasks (from packet)")
        for t in rg["next_search_tasks"]:
            lines.append(f"- {t}")
        lines.append("")
    if not rg.get("missing_fields") and not rg.get("next_search_tasks"):
        lines.append("_(packet did not carry missing_fields or next_search_tasks)_")
        lines.append("")

    # 10. Production angle (renders packet's production_angles[] when present;
    # else emits the generic hand-curation-gap placeholder)
    lines.append("## 10. Production angle")
    lines.append("")
    angles = brief.get("production_angles") or []
    for a in angles:
        rec = " (recommended)" if a.get("recommended") else ""
        rank = a.get("rank")
        rank_label = f"Rank {rank}" if rank is not None else "Angle"
        lines.append(f"- **{rank_label}**{rec}: {a.get('title')}")
        for risk in (a.get("risks") or []):
            lines.append(f"  - Risk: {risk}")
    lines.append("")

    # 11. Next research tasks
    lines.append("## 11. Next research tasks")
    lines.append("")
    nrt = brief.get("next_research_tasks") or []
    if nrt:
        for t in nrt:
            lines.append(f"- {t}")
    else:
        lines.append("_(none specified in packet)_")
    lines.append("")

    # Provenance footer
    inp = brief.get("_inputs") or {}
    lines.append("---")
    lines.append("")
    lines.append("## Brief metadata")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps({
        "brief_id": brief.get("brief_id"),
        "brief_kind": brief.get("brief_kind"),
        "packet_id": brief.get("packet_id"),
        "candidate_id": brief.get("candidate_id"),
        "generated_at": brief.get("generated_at"),
        "_inputs": inp,
    }, indent=2))
    lines.append("```")
    lines.append("")

    return "\n".join(lines)


def assemble_packet(packet_path, transcript_dir, dry_run, output_dir):
    """Top-level orchestrator for ``--packet`` mode.

    Returns the brief dict on success, ``None`` on missing-input
    failure. Mirrors the contract of ``assemble_one`` (verdict mode).
    """
    packet = _load_json(packet_path)
    if not packet:
        print(f"[ERR] packet not loadable: {packet_path}")
        return None
    if not isinstance(packet, dict):
        print(f"[ERR] packet must be a JSON object, got {type(packet).__name__}: {packet_path}")
        return None

    # Locate transcripts directory. If --transcript-dir is omitted we
    # warn (no transcripts loaded); the brief still renders with a
    # "no captioned transcripts available" note in section 7.
    transcripts = []
    if transcript_dir:
        transcripts = _load_packet_transcripts(transcript_dir)
        if not transcripts:
            print(f"  [WARN] no .txt transcripts found under: {transcript_dir}")

    brief = build_packet_brief(packet, transcripts)
    md = render_packet_markdown(brief)

    safe_id = _packet_safe_id(packet)

    if dry_run:
        print(f"\n{'=' * 70}\n[DRY RUN] PACKET BRIEF — {safe_id}\n{'=' * 70}")
        print(md)
        return brief

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / f"{safe_id}_packet_brief.md"
    json_path = out_dir / f"{safe_id}_packet_brief.json"
    md_path.write_text(md, encoding="utf-8")
    json_path.write_text(
        json.dumps(brief, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    # ASCII-only status markers so operator stdout works on Windows
    # cp1252; pre-existing --verdict mode uses non-ASCII '✓' but
    # that's out of scope for this PR.
    print(f"  [ok] {md_path.name}")
    print(f"  [ok] {json_path.name}")
    return brief


def main():
    parser = argparse.ArgumentParser(description="Pipeline 5: merge P2+P3+P4 into a production brief")
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--verdict", help="Path to a single P4 verdict JSON")
    src.add_argument("--verdict-dir", help="Directory containing *_verdict.json files")
    src.add_argument(
        "--packet",
        help=(
            "Path to a single packet stub JSON (P1 packet-production lane). "
            "Activates packet-mode brief generation, which reads cached "
            "YouTube caption .txt transcripts from --transcript-dir and "
            "emits a deterministic 11-section brief mirroring the hand-"
            "assembled top-5 briefs. Default behaviour (--verdict / "
            "--verdict-dir) is unchanged."
        ),
    )

    parser.add_argument("--case-research", help="P2 case research JSON (auto-discovered if omitted; verdict mode only)")
    parser.add_argument("--transcript-dir", default=None, help=f"P3 transcript directory (default: {DEFAULT_TRANSCRIPT_DIR}) — also used by --packet mode for cached caption .txt files")
    parser.add_argument("--weights", default=None, help=f"P1 scoring_weights.json (default: {DEFAULT_WEIGHTS}; verdict mode only)")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help=f"Output directory (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--dry-run", action="store_true", help="Print brief to stdout; don't write files")
    args = parser.parse_args()

    # Packet mode: short-circuit before the verdict-mode plumbing.
    if args.packet:
        print(f"Assembling 1 packet brief{' [DRY RUN]' if args.dry_run else ''}")
        if not args.dry_run:
            print(f"Output: {args.output}")
        try:
            b = assemble_packet(
                packet_path=Path(args.packet),
                transcript_dir=args.transcript_dir,
                dry_run=args.dry_run,
                output_dir=args.output,
            )
            built = 1 if b is not None else 0
            failed = 0 if b is not None else 1
        except Exception as e:
            print(f"  [ERR] {e}")
            built, failed = 0, 1
        print(f"\n{'=' * 60}")
        print(f"Built: {built} | Failed: {failed} | Total: 1")
        return

    verdict_paths = []
    if args.verdict:
        verdict_paths.append(Path(args.verdict))
    else:
        vdir = Path(args.verdict_dir)
        if not vdir.exists():
            print(f"[ERR] verdict dir does not exist: {vdir}")
            sys.exit(2)
        verdict_paths = sorted(vdir.glob("*_verdict.json"))
        if not verdict_paths:
            print(f"[ERR] no *_verdict.json files in {vdir}")
            sys.exit(2)

    print(f"Assembling {len(verdict_paths)} brief(s){' [DRY RUN]' if args.dry_run else ''}")
    if not args.dry_run:
        print(f"Output: {args.output}")

    built = 0
    failed = 0
    for vp in verdict_paths:
        print(f"\n-- {vp.name} --")
        try:
            b = assemble_one(
                verdict_path=vp,
                case_research_path=args.case_research,
                transcript_dir=args.transcript_dir,
                weights_path=args.weights,
                dry_run=args.dry_run,
                output_dir=args.output,
            )
            if b is not None:
                built += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  [ERR] {e}")
            failed += 1

    print(f"\n{'=' * 60}")
    print(f"Built: {built} | Failed: {failed} | Total: {len(verdict_paths)}")


if __name__ == "__main__":
    main()
