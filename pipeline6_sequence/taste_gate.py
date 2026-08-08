"""Deterministic pre-render craft vetoes for P6.

Consumes the operator-authored TASTE_RULES.json plus a blueprint and realized
paper edit. It rejects/revises the CUT, never the case. Every result is an enum
with a concrete rule id and observed evidence; no uncalibrated numeric score.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

_SEVERITY = {"SHIP": 0, "REVISE": 1, "ABSTAIN": 2, "REJECT": 3}
_VISUAL_KINDS = {"bodycam", "dashcam", "surveillance", "video", "interview", "photo"}
_AUDIO_KINDS = {"911_audio", "radio", "audio"}


def _texts(obj: Any, path: str = "") -> Iterable[Tuple[str, str]]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _texts(v, f"{path}.{k}" if path else str(k))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _texts(v, f"{path}[{i}]")
    elif isinstance(obj, str):
        yield path, obj


def _duration(event: Dict) -> float:
    if event.get("kind") == "clip":
        return max(0.0, float(event.get("out_sec", 0)) - float(event.get("in_sec", 0)))
    return float(event.get("dur", 0) or 0)


def _route(bp: Dict) -> Optional[str]:
    treatment = bp.get("reference_treatment") or {}
    return treatment.get("route_id") or bp.get("route_id")


def _scope_applies(rule: Dict, fmt: str, platform: str) -> bool:
    scope = rule.get("scope") or {}
    formats = scope.get("formats") or []
    platforms = scope.get("platforms") or []
    return (not formats or fmt in formats) and (not platforms or platform in platforms)


def _finding(rule: Dict, status: str, observed: str, refs: Optional[List[str]] = None) -> Dict:
    return {"rule_id": rule.get("rule_id"), "status": status,
            "verdict": rule.get("on_fail") if status == "FAIL" else "SHIP",
            "reason": rule.get("reason"), "observed": observed,
            "source_refs": refs or rule.get("source_refs") or []}


def _check_forbidden_text(rule: Dict, bp: Dict, pe: Dict) -> Dict:
    params = (rule.get("check") or {}).get("parameters") or {}
    if params.get("assert_verbatim_against"):
        mismatches = []
        for i, ev in enumerate(pe.get("timeline") or []):
            quote = str(ev.get("transcript_excerpt") or "").strip()
            caps = " ".join(str(c.get("text") or "") for c in (ev.get("captions") or [])).strip()
            if quote and caps:
                nq = re.sub(r"\W+", " ", quote.lower()).strip()
                nc = re.sub(r"\W+", " ", caps.lower()).strip()
                if nq not in nc and nc not in nq:
                    mismatches.append(f"timeline[{i}]")
        return (_finding(rule, "FAIL", f"{len(mismatches)} quote/caption mismatch(es)", mismatches)
                if mismatches else _finding(rule, "PASS", "No independently testable mismatch found."))
    if params.get("category"):
        hits = [f"timeline[{i}]" for i, ev in enumerate(pe.get("timeline") or [])
                if params["category"] in (ev.get("content_categories") or [])]
        return (_finding(rule, "FAIL", f"Category present in {len(hits)} event(s).", hits)
                if hits else _finding(rule, "PASS", "Category not declared on the paper edit."))
    patterns = [str(p).lower() for p in params.get("patterns") or []]
    hits = []
    for path, text in _texts(pe):
        low = text.lower()
        for pat in patterns:
            if pat in low:
                hits.append(f"{path}:{pat}")
    return (_finding(rule, "FAIL", f"Found {len(hits)} internal/process phrase(s).", hits[:20])
            if hits else _finding(rule, "PASS", "No forbidden production language found."))


def _check_minimum_visual(rule: Dict, bp: Dict, pe: Dict) -> Dict:
    params = (rule.get("check") or {}).get("parameters") or {}
    funcs = set(params.get("accepted_functions") or [])
    visual = [ev for ev in pe.get("timeline") or [] if ev.get("kind") == "clip"
              and ev.get("asset_kind") in _VISUAL_KINDS
              and (not funcs or ev.get("function") in funcs)]
    need = int(params.get("minimum_count") or 1)
    return (_finding(rule, "PASS", f"{len(visual)} qualifying visual story beat(s).")
            if len(visual) >= need else
            _finding(rule, "FAIL", f"Only {len(visual)} qualifying visual story beat(s); need {need}."))


def _blur_segments(pe: Dict) -> List[Tuple[int, Dict]]:
    out = []
    for i, ev in enumerate(pe.get("timeline") or []):
        for seg in ev.get("blur_segments") or []:
            if isinstance(seg, dict):
                out.append((i, seg))
        if ev.get("full_frame_blur"):
            out.append((i, {"kind": "full_frame", "duration_sec": _duration(ev), "central_visual": True}))
    return out


def _check_max_duration(rule: Dict, bp: Dict, pe: Dict, platform: str) -> Dict:
    params = (rule.get("check") or {}).get("parameters") or {}
    if params.get("platform_scope") and platform not in params["platform_scope"]:
        return _finding(rule, "PASS", "Rule does not apply to this platform.")
    max_sec = float(params.get("max_consecutive_sec") or 0)
    hits = []
    for i, seg in _blur_segments(pe):
        if params.get("blur_kind") and seg.get("kind") != params["blur_kind"]:
            continue
        if params.get("applies_to") == "central_visual" and not seg.get("central_visual", True):
            continue
        dur = float(seg.get("duration_sec") or (float(seg.get("end", 0)) - float(seg.get("start", 0))))
        if dur > max_sec:
            hits.append(f"timeline[{i}] {dur:.1f}s")
    return (_finding(rule, "FAIL", f"{len(hits)} blur run(s) exceed {max_sec:g}s.", hits)
            if hits else _finding(rule, "PASS", f"No declared blur run exceeds {max_sec:g}s."))


def _check_promise_payoff_latency(rule: Dict, bp: Dict, pe: Dict) -> Dict:
    """After a setup that promises a specific payoff, the NEXT content beat must BE the payoff
    — not another exposition line. Operator (Zenobia, 2026-08-07): "once that setup has been made
    it should immediately be her asking the question, not another line of exposition."

    A beat is a "promise" when it carries promise_payoff: {id, expects} (the props adapter tags
    the setup/its payoff). Between promise and its matching payoff (payoff_for == id), no
    exposition-kind beat (narration/card whose function is 'exposition'/'context') may intervene
    beyond `max_intervening` (default 0).
    """
    params = (rule.get("check") or {}).get("parameters") or {}
    max_intervening = int(params.get("max_intervening") or 0)
    expo = set(params.get("exposition_functions")
               or ["exposition", "context", "establish", "backstory"])
    tl = pe.get("timeline") or []
    hits = []
    for i, ev in enumerate(tl):
        pp = ev.get("promise_payoff") or {}
        pid = pp.get("id")
        if not pid or pp.get("role") not in (None, "promise"):
            continue
        intervening = 0
        for j in range(i + 1, len(tl)):
            nxt = tl[j]
            if (nxt.get("promise_payoff") or {}).get("payoff_for") == pid:
                break                                   # payoff reached
            # "another line of exposition" = a narration/VO line, or an exposition-function clip,
            # sitting between the setup and its payoff. Structural cards (title/phase/outcome) are
            # NOT exposition and never count — only content that delays the payoff does.
            is_expo = (nxt.get("kind") == "narration"
                       or nxt.get("function") in expo
                       or (nxt.get("kind") == "card"
                           and nxt.get("card_kind") not in ("title", "phase", "outcome")))
            if is_expo:
                intervening += 1
                if intervening > max_intervening:
                    hits.append(f"timeline[{i}]→[{j}] promise '{pid}' delayed by exposition")
                    break
    return (_finding(rule, "FAIL", f"{len(hits)} promise(s) not paid off immediately.", hits)
            if hits else _finding(rule, "PASS", "Every setup pays off on the next beat."))


def _check_redundant_blur(rule: Dict, bp: Dict, pe: Dict) -> Dict:
    """Blur that persists after the sensitive subject is already redacted/removed is a defect.
    Operator (Zenobia): body redacted after ~40s, yet half-frame blur ran to 3:24 — "unnecessary".

    A blur segment is redundant when the same timeline event (or a declared redaction on it) marks
    subject_redacted / body_removed true, OR when a blur runs past `stale_after_sec` with no
    subject actually in the masked region (declared via seg.subject_present == false).
    """
    params = (rule.get("check") or {}).get("parameters") or {}
    stale_after = float(params.get("stale_after_sec") or 0)
    hits = []
    for i, seg in _blur_segments(pe):
        ev = (pe.get("timeline") or [])[i] if i < len(pe.get("timeline") or []) else {}
        dur = float(seg.get("duration_sec") or (float(seg.get("end", 0)) - float(seg.get("start", 0))))
        already_redacted = bool(ev.get("subject_redacted") or ev.get("body_removed")
                                or seg.get("subject_present") is False)
        if already_redacted:
            hits.append(f"timeline[{i}] blur over already-redacted subject ({dur:.1f}s)")
        elif stale_after and dur > stale_after and seg.get("subject_present") is None:
            hits.append(f"timeline[{i}] blur {dur:.1f}s > {stale_after:g}s with subject-presence unverified")
    return (_finding(rule, "FAIL", f"{len(hits)} redundant/stale blur run(s).", hits)
            if hits else _finding(rule, "PASS", "No blur persists past redaction."))


def _check_required_text(rule: Dict, bp: Dict, pe: Dict) -> Dict:
    """A required substring must appear (or a forbidden spelling must NOT) in the given fields.
    Used for brand rules: VO must say "Doctor" not the letters "D-R"/"D R"; a CTA slot must exist.
    parameters: require_any (list — at least one must be present) and/or forbid (list — none may
    appear), over `fields` (dot-paths into paper-edit text, e.g. narration.text, cta.slot).
    """
    params = (rule.get("check") or {}).get("parameters") or {}
    require_any = [str(x) for x in params.get("require_any") or []]
    forbid = [str(x).lower() for x in params.get("forbid") or []]
    blob = " ".join(text for _, text in _texts(pe)).strip()
    low = blob.lower()
    if require_any and not any(str(r).lower() in low for r in require_any):
        return _finding(rule, "FAIL", f"None of required {require_any} present.")
    bad = [f for f in forbid if re.search(r"(?<![a-z])" + re.escape(f) + r"(?![a-z])", low)]
    return (_finding(rule, "FAIL", f"Forbidden spelling(s) present: {bad}", bad)
            if bad else _finding(rule, "PASS", "Required text present, forbidden absent."))


def _check_disposition(rule: Dict, bp: Dict, pe: Dict) -> Dict:
    params = (rule.get("check") or {}).get("parameters") or {}
    route = _route(bp)
    if route not in set(params.get("trigger_routes") or []):
        return _finding(rule, "PASS", f"Route {route or 'none'} does not trigger this rail.")
    inc = bp.get("incident") or {}
    if inc.get("disposition_source"):
        return _finding(rule, "PASS", "Disposition has a source-of-record citation.",
                        [str(inc["disposition_source"])])
    return _finding(rule, "FAIL", "Accountability route has no incident.disposition_source.",
                    ["blueprint.incident.disposition_source"])


def _check_caption_authority(rule: Dict, bp: Dict, pe: Dict) -> Dict:
    params = (rule.get("check") or {}).get("parameters") or {}
    max_n = int(params.get("max_authorities_per_zone") or 1)
    zones = set(params.get("zones") or [])
    hits = []
    for i, ev in enumerate(pe.get("timeline") or []):
        counts: Dict[str, int] = {}
        for a in ev.get("caption_authorities") or []:
            zone = a.get("zone", "center") if isinstance(a, dict) else "center"
            if not zones or zone in zones:
                counts[zone] = counts.get(zone, 0) + 1
        hits += [f"timeline[{i}].{z}={n}" for z, n in counts.items() if n > max_n]
    return (_finding(rule, "FAIL", "Multiple caption authorities occupy the same zone.", hits)
            if hits else _finding(rule, "PASS", "At most one caption authority per checked zone."))


def _check_identity(rule: Dict, bp: Dict, pe: Dict) -> Dict:
    interviews = [(i, e) for i, e in enumerate(pe.get("timeline") or [])
                  if e.get("kind") == "clip" and e.get("asset_kind") == "interview"]
    hits = [f"timeline[{i}]" for i, ev in interviews if not ev.get("speaker_identified")]
    return (_finding(rule, "FAIL", "Interview dialogue appears before a usable identity.", hits)
            if hits else _finding(rule, "PASS", f"{len(interviews)} interview event(s) checked."))


def _check_temporal(rule: Dict, bp: Dict, pe: Dict) -> Dict:
    calls = [(i, e) for i, e in enumerate(pe.get("timeline") or [])
             if e.get("kind") == "clip" and e.get("asset_kind") == "911_audio"]
    hits = []
    temporal_words = re.compile(r"\b(before|after|earlier|later|minutes?|hours?|at \d|that (?:morning|night|day))\b", re.I)
    for i, ev in calls:
        text = " ".join(str(ev.get(k) or "") for k in ("temporal_relation", "narration_top", "description"))
        if not ev.get("temporal_relation") and not temporal_words.search(text):
            hits.append(f"timeline[{i}]")
    return (_finding(rule, "FAIL", "911 call lacks a temporal/factual anchor.", hits)
            if hits else _finding(rule, "PASS", f"{len(calls)} call event(s) checked."))


def _check_progression(rule: Dict, bp: Dict, pe: Dict) -> Dict:
    max_sec = float((((rule.get("check") or {}).get("parameters") or {}).get("max_static_sec") or 3))
    hits = []
    for i, ev in enumerate(pe.get("timeline") or []):
        if ev.get("kind") != "clip" or ev.get("asset_kind") not in _VISUAL_KINDS or _duration(ev) <= max_sec:
            continue
        has_progress = (ev.get("visual_progression") is True or bool(ev.get("narration_top"))
                        or bool(ev.get("captions")) or ev.get("consequential_atmosphere") is True)
        if not has_progress:
            hits.append(f"timeline[{i}] {_duration(ev):.1f}s")
    return (_finding(rule, "FAIL", "Silent/static footage exceeds the progression floor.", hits)
            if hits else _finding(rule, "PASS", "No long unmotivated static clip detected."))


def _check_required_slots(rule: Dict, bp: Dict, pe: Dict) -> Dict:
    missing = ((bp.get("treatment_validation") or {}).get("missing_required_slots") or [])
    refs = [f"{x.get('act_id')}:{x.get('slot_id')}" for x in missing]
    return (_finding(rule, "FAIL", f"{len(missing)} required evidence slot(s) are unbound.", refs)
            if missing else _finding(rule, "PASS", "All required treatment evidence slots are bound."))


def _short_structure_floor(fmt: str, pe: Dict) -> Optional[Dict]:
    if fmt != "shortform":
        return None
    funcs = [e.get("function") for e in pe.get("timeline") or [] if e.get("kind") == "clip"]
    groups = ({"hook", "emotional_peak", "reveal", "tension_shift"},
              {"context", "establish", "detail_noticed", "procedure"},
              {"payoff", "climax", "resolve", "callback", "reveal"})
    idx = []
    cursor = -1
    for accepted in groups:
        hit = next((i for i in range(cursor + 1, len(funcs)) if funcs[i] in accepted), None)
        if hit is None:
            return {"rule_id": "short_hook_context_payoff", "status": "FAIL", "verdict": "REJECT",
                    "reason": "A native short requires hook → context → payoff in that order.",
                    "observed": f"functions={funcs}", "source_refs": ["operator.short_craft_floor"]}
        idx.append(hit)
        cursor = hit
    return {"rule_id": "short_hook_context_payoff", "status": "PASS", "verdict": "SHIP",
            "reason": "Native short structure floor.", "observed": f"indices={idx}",
            "source_refs": ["operator.short_craft_floor"]}


# --- Kyle Gray v3 rebuild critique (operator-endorsed, 2026-08-08): BWC is the spine. ---

def _event_dur(ev: Dict) -> float:
    """Mirror render_blueprint.project_duration's timing model exactly:
    card=dur (default 5), clip=out-in (>=0.5), narration=3.0, gap=0."""
    k = ev.get("kind")
    if k == "card":
        return float(ev.get("dur", 5.0))
    if k == "clip":
        return max(0.5, float(ev.get("out_sec", 0)) - float(ev.get("in_sec", 0)))
    if k == "narration":
        return 3.0
    return 0.0


def _is_static(ev: Dict) -> bool:
    """Non-footage screen time: narration cards and non-structural cards. Structural
    title/phase/outcome cards are chrome, not evidence dwell (same exemption the
    promise-payoff scan uses)."""
    if ev.get("kind") == "narration":
        return True
    return ev.get("kind") == "card" and ev.get("card_kind") not in ("title", "phase", "outcome")


def _check_runtime_ratios(rule: Dict, bp: Dict, pe: Dict) -> Dict:
    """The film must live in event footage. Kyle Gray v3 targets: 70-85% of runtime in direct
    event footage, max 10-15% in documents/static evidence. Editorial targets, not quotas —
    pair with on_fail REVISE so drift is surfaced without stopping the encode. Narration that
    rides footage (narration_top on a clip) counts as footage: the screen stays in the event."""
    params = (rule.get("check") or {}).get("parameters") or {}
    event_min = float(params.get("event_min_ratio") or 0.70)
    static_max = float(params.get("static_max_ratio") or 0.15)
    tl = pe.get("timeline") or []
    total = sum(_event_dur(e) for e in tl)
    if total <= 0:
        return _finding(rule, "PASS", "Empty timeline — nothing to ratio.")
    clip = sum(_event_dur(e) for e in tl if e.get("kind") == "clip")
    static = sum(_event_dur(e) for e in tl if _is_static(e))
    obs = f"event footage {clip / total:.0%}, static/documents {static / total:.0%} of {total:.0f}s"
    hits = []
    if clip / total < event_min:
        hits.append(f"event footage {clip / total:.0%} < target {event_min:.0%}")
    if static / total > static_max:
        hits.append(f"static/documents {static / total:.0%} > target {static_max:.0%}")
    return (_finding(rule, "FAIL", f"{obs} — {'; '.join(hits)}", hits)
            if hits else _finding(rule, "PASS", obs))


def _check_max_static_dwell(rule: Dict, bp: Dict, pe: Dict) -> Dict:
    """No uninterrupted document/static run longer than ~max_consecutive_sec (Kyle Gray v3:
    8-12s): the viewer must stay inside the event. gap events render nothing and do not
    break a static run."""
    params = (rule.get("check") or {}).get("parameters") or {}
    max_sec = float(params.get("max_consecutive_sec") or 12)
    tl = pe.get("timeline") or []
    hits = []
    run, start_i = 0.0, None
    for i, ev in enumerate(tl):
        if _is_static(ev):
            if start_i is None:
                start_i = i
            run += _event_dur(ev)
        elif ev.get("kind") == "gap":
            continue
        else:
            if run > max_sec:
                hits.append(f"timeline[{start_i}..{i - 1}] {run:.1f}s static run")
            run, start_i = 0.0, None
    if run > max_sec:
        hits.append(f"timeline[{start_i}..] {run:.1f}s static run")
    return (_finding(rule, "FAIL", f"{len(hits)} static run(s) exceed {max_sec:g}s.", hits)
            if hits else _finding(rule, "PASS", f"No static run exceeds {max_sec:g}s."))


_CMD_RE = re.compile(
    r"shots? fired|drop the (gun|knife|weapon)|show me your hands|get on the ground|"
    r"put your hands|hands up|don't move|stop reaching|let me see your hands", re.I)


def _check_no_vo_over_commands(rule: Dict, bp: Dict, pe: Dict) -> Dict:
    """Protected no-VO zones (Kyle Gray v3 §7): the clearest commands, credited warnings,
    gunfire and immediate reactions play CLEAN — narration must not ride over them.
    Deterministic proxy: a clip whose timed captions / pull quote match the command lexicon
    while narration_top is non-empty."""
    tl = pe.get("timeline") or []
    hits = []
    for i, ev in enumerate(tl):
        if ev.get("kind") != "clip" or not (ev.get("narration_top") or "").strip():
            continue
        text = (" ".join(c.get("text", "") for c in ev.get("captions") or [])
                + " " + (ev.get("transcript_excerpt") or ""))
        m = _CMD_RE.search(text)
        if m:
            hits.append(f"timeline[{i}] narration over command audio ('{m.group(0)}')")
    return (_finding(rule, "FAIL", f"{len(hits)} narration line(s) ride command audio.", hits)
            if hits else _finding(rule, "PASS", "No narration rides command/warning audio."))


_CHECKS = {
    "forbidden_text": _check_forbidden_text,
    "minimum_visual_story_beats": _check_minimum_visual,
    "source_citation_required": _check_disposition,
    "maximum_caption_authorities": _check_caption_authority,
    "identity_context_required": _check_identity,
    "temporal_relation_required": _check_temporal,
    "progression_required": _check_progression,
    "required_evidence_slot": _check_required_slots,
    # Zenobia critique 2026-08-07:
    "promise_payoff_latency": _check_promise_payoff_latency,
    "redundant_blur": _check_redundant_blur,
    "required_text": _check_required_text,
    # Kyle Gray v3 rebuild critique 2026-08-08:
    "runtime_ratios": _check_runtime_ratios,
    "max_static_dwell": _check_max_static_dwell,
    "no_narration_over_commands": _check_no_vo_over_commands,
}


def evaluate_rules(ruleset: Dict, bp: Dict, pe: Dict, *, fmt: str, platform: str) -> Dict:
    findings: List[Dict] = []
    for rule in ruleset.get("rules") or []:
        if not rule.get("enabled", True) or not _scope_applies(rule, fmt, platform):
            continue
        typ = ((rule.get("check") or {}).get("type"))
        if typ == "maximum_consecutive_duration":
            findings.append(_check_max_duration(rule, bp, pe, platform))
        elif typ in _CHECKS:
            findings.append(_CHECKS[typ](rule, bp, pe))
        else:
            findings.append(_finding(rule, "FAIL", f"Unsupported deterministic check type: {typ}",
                                     [f"taste_rule:{rule.get('rule_id')}" ]))
            findings[-1]["verdict"] = "ABSTAIN"
    floor = _short_structure_floor(fmt, pe)
    if floor:
        findings.append(floor)
    failed = [f for f in findings if f["status"] == "FAIL"]
    verdict = max((f["verdict"] for f in failed), key=lambda v: _SEVERITY.get(v, 2), default="SHIP")
    return {"case_id": bp.get("case_id"), "verdict": verdict, "format": fmt,
            "platform": platform, "rule_set_id": ruleset.get("rule_set_id"),
            "findings": findings, "failed_rule_ids": [f["rule_id"] for f in failed]}


def render_markdown(report: Dict) -> str:
    lines = [f"# Paper Edit Validation — {report.get('case_id')}", "",
             f"**Verdict:** {report.get('verdict')}",
             f"**Format / platform:** {report.get('format')} / {report.get('platform')}", "",
             "## Rule results", ""]
    for f in report.get("findings") or []:
        mark = "PASS" if f["status"] == "PASS" else f"FAIL → {f['verdict']}"
        lines.append(f"- **{f['rule_id']}** — {mark}: {f['observed']}")
    return "\n".join(lines) + "\n"


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="P6 deterministic craft/taste veto")
    ap.add_argument("--blueprint", required=True, type=Path)
    ap.add_argument("--paper-edit", required=True, type=Path)
    ap.add_argument("--rules", required=True, type=Path)
    ap.add_argument("--format", dest="fmt", choices=["longform", "shortform"], default=None)
    ap.add_argument("--platform", choices=["youtube", "tiktok", "instagram"], default=None)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--gate", action="store_true")
    args = ap.parse_args(argv)
    bp = json.loads(args.blueprint.read_text(encoding="utf-8"))
    pe = json.loads(args.paper_edit.read_text(encoding="utf-8"))
    rules = json.loads(args.rules.read_text(encoding="utf-8"))
    fmt = args.fmt or pe.get("format") or "longform"
    platform = args.platform or pe.get("platform") or "youtube"
    report = evaluate_rules(rules, bp, pe, fmt=fmt, platform=platform)
    args.out.mkdir(parents=True, exist_ok=True)
    cid = bp.get("case_id", "case")
    jp = args.out / f"VALIDATION_{cid}.json"
    mp = args.out / f"VALIDATION_{cid}.md"
    jp.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    mp.write_text(render_markdown(report), encoding="utf-8")
    print(f"[taste-gate] {report['verdict']} -> {jp} + {mp}")
    # REVISE stays shippable-with-warning (the calibrated usable floor); only a
    # faithfulness rejection or true abstention stops the render.
    return 3 if args.gate and report["verdict"] in {"ABSTAIN", "REJECT"} else 0


if __name__ == "__main__":
    raise SystemExit(main())
