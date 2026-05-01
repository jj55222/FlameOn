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


def main():
    parser = argparse.ArgumentParser(description="Pipeline 5: merge P2+P3+P4 into a production brief")
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--verdict", help="Path to a single P4 verdict JSON")
    src.add_argument("--verdict-dir", help="Directory containing *_verdict.json files")

    parser.add_argument("--case-research", help="P2 case research JSON (auto-discovered if omitted)")
    parser.add_argument("--transcript-dir", default=None, help=f"P3 transcript directory (default: {DEFAULT_TRANSCRIPT_DIR})")
    parser.add_argument("--weights", default=None, help=f"P1 scoring_weights.json (default: {DEFAULT_WEIGHTS})")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help=f"Output directory (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--dry-run", action="store_true", help="Print brief to stdout; don't write files")
    args = parser.parse_args()

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
