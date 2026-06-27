"""P6 / GOAL_D — deterministic production blueprint (the long-form rails).

Builds ``production_blueprint.json`` (schema: ``schemas/contracts.json`` →
``p6_blueprint``): the end-to-end resource guide for a 30+ minute cut, assembled
ENTIRELY from artifacts the engine already emits — no LLM, no paid calls, no
ffmpeg (durations come from ``artifacts.json``). It is the *rails*: an optional
LLM tier later shapes acts/beats WITHIN this skeleton, and a validator rejects
anything that references an asset or timecode not in ``asset_manifest``.

Inputs (all already produced upstream):
  --artifacts    timeline/artifacts.json   (D0 stamps: kind, duration, start_iso)
  --timeline     timeline/case_timeline.json (D1 phase buckets)
  --verdict      d2/verdicts/<id>_verdict.json (P4 key_moments)
  --doc-extract  docs/doc_extract.json [...]  (D5 disposition/charges/clip dirs)
  --media-dir    video/Video               (to resolve transcript media + stills)

    python pipeline6_sequence/blueprint.py \
        --artifacts .tmp/sac_poc/timeline/artifacts.json \
        --timeline  .tmp/sac_poc/timeline/case_timeline.json \
        --verdict   .tmp/sac_poc/d2/verdicts/vasquez_23117201_verdict.json \
        --doc-extract .tmp/sac_poc/docs_23117201/doc_extract.json \
        --media-dir .tmp/sac_poc/video/Video --agency "Sacramento County Sheriff" \
        --out .tmp/sac_poc/blueprint

Pure stdlib (+ reuse of render_rough_cut's source mapping). Global Python.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import render_rough_cut as rc  # noqa: E402  (only zero-ffmpeg helpers used)

DEFAULT_RUNTIME = 1800.0       # 30 min
COLD_OPEN_SEC = 45.0
PHASE_CARD_SEC = 4.0
BRIDGE_SEC = 6.0
HEAD_PAD, TAIL_PAD = 5.0, 3.0

# Canonical documentary phase order + how each maps to an act.
_PHASE_ACT = [
    ("pre_incident", "The Call", "establish"),
    ("incident", "The Incident", "escalate"),
    ("aftermath", "Aftermath", "aftermath"),
    ("transport", "Transport", "aftermath"),
    ("investigation", "The Record", "accountability"),
    ("outcome", "Outcome", "resolve"),
]
_PHASE_TITLE = {p: t for p, t, _ in _PHASE_ACT}
_PHASE_FUNC = {p: f for p, _, f in _PHASE_ACT}
_KIND_MAP = {"bodycam": "bodycam", "dashcam": "dashcam", "911": "911_audio",
             "radio": "radio"}
_VIDEO_KINDS = {"bodycam", "dashcam"}


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


# ---------------------------------------------------------------------------
# 1. Asset manifest — every asset, typed, in one place
# ---------------------------------------------------------------------------

def build_asset_manifest(artifacts: List[Dict], timeline_index: Dict[str, Dict],
                         transcribed_stems: set, doc_extracts: List[Dict],
                         doc_paths: List[Optional[str]], agency: str,
                         media_dir: Optional[Path]) -> Tuple[List[Dict], Dict[str, str]]:
    """Return (manifest, path_stem -> asset_id index)."""
    manifest: List[Dict] = []
    stem_to_id: Dict[str, str] = {}
    a911 = aradio = 0
    for a in artifacts:
        kind = _KIND_MAP.get(a.get("kind", ""), "other")
        pov = a.get("pov_label") or a.get("artifact_id") or ""
        if kind in _VIDEO_KINDS:
            asset_id = f"v_{_slug(pov)}"
        elif kind == "911_audio":
            a911 += 1
            asset_id = f"a_911_{a911}"
        elif kind == "radio":
            aradio += 1
            asset_id = f"a_radio_{aradio}"
        else:
            asset_id = f"x_{_slug(pov)}"
        path = a.get("path")
        stem = Path(path).stem if path else None
        ti = timeline_index.get(a.get("artifact_id"), {})
        manifest.append({
            "asset_id": asset_id, "kind": kind, "pov_label": pov, "path": path,
            "start_iso": a.get("start_iso"), "duration_sec": a.get("duration_sec"),
            "phase": a.get("phase") or ti.get("phase"),
            "has_transcript": bool(stem and stem in transcribed_stems),
            "custodian": agency, "timestamp_source": a.get("timestamp_source"),
            "doc_type": None,
        })
        if stem:
            stem_to_id[stem] = asset_id

    # Documents (from doc_extract; not in artifacts.json).
    for i, de in enumerate(doc_extracts):
        dt = de.get("doc_type") or "document"
        asset_id = f"doc_{_slug(dt)}" if dt != "document" else f"doc_{i + 1}"
        manifest.append({
            "asset_id": asset_id, "kind": "document", "pov_label": de.get("ia_case_number"),
            "path": doc_paths[i] if i < len(doc_paths) else None,
            "start_iso": None, "duration_sec": None, "phase": "investigation",
            "has_transcript": False, "custodian": agency,
            "timestamp_source": None, "doc_type": dt,
        })

    # Stills / photos (best-effort scan; documentary B-roll).
    if media_dir and Path(media_dir).exists():
        imgs = [p for p in sorted(Path(media_dir).rglob("*"))
                if p.suffix.lower() in {".jpg", ".jpeg", ".png"}]
        for i, p in enumerate(imgs, 1):
            manifest.append({
                "asset_id": f"still_{i}", "kind": "photo", "pov_label": p.stem,
                "path": str(p), "start_iso": None, "duration_sec": None,
                "phase": None, "has_transcript": False, "custodian": agency,
                "timestamp_source": None, "doc_type": None,
            })
    return manifest, stem_to_id


# ---------------------------------------------------------------------------
# 2. Incident facts (sourced) from doc_extract + timeline
# ---------------------------------------------------------------------------

def _narrative_text(de: Dict) -> Optional[str]:
    """doc_extract.narrative is sometimes a {text, page} object, sometimes a
    bare string. Return the prose either way — this is the authoritative,
    human-readable factual summary the LLM tier must ground its framing on."""
    n = de.get("narrative")
    if isinstance(n, dict):
        n = n.get("text")
    return n.strip() if isinstance(n, str) and n.strip() else None


def build_incident(doc_extracts: List[Dict], timeline: Dict) -> Dict:
    inc: Dict[str, Any] = {"date": None, "time": None, "location": None,
                           "subjects": [], "charges": [], "disposition": None,
                           "summary": None}
    for de in doc_extracts:
        inc["date"] = inc["date"] or de.get("doc_date")
        inc["time"] = inc["time"] or de.get("incident_time")
        inc["location"] = inc["location"] or de.get("location")
        # Subjects: prefer an explicit people[] list; fall back to the singular
        # ``subject`` field (our OCR extractor populates ``subject`` but leaves
        # ``people`` empty, which silently erased WHO the case is about).
        for p in de.get("people", []) or []:
            if p and p not in inc["subjects"]:
                inc["subjects"].append(p)
        subj = de.get("subject")
        if subj and subj not in inc["subjects"]:
            inc["subjects"].append(subj)
        # Charges: prefer charges[]; fall back to the SUSTAINED findings in the
        # disposition (same reason — charges[] is often empty while findings hold
        # the actual M.O.U. violations).
        for c in de.get("charges", []) or []:
            if c and c not in inc["charges"]:
                inc["charges"].append(c)
        if not inc["charges"]:
            for f in ((de.get("disposition") or {}).get("findings") or []):
                ch = _clean_charge(f.get("charge"))
                if ch and ch not in inc["charges"]:
                    inc["charges"].append(f"{f.get('finding', '')}: {ch}".strip(": "))
        inc["summary"] = inc["summary"] or _narrative_text(de)
        oc = de.get("outcome_card") or {}
        if not inc["disposition"] and (oc.get("subtitle") or (de.get("disposition") or {}).get("summary")):
            inc["disposition"] = oc.get("subtitle") or de["disposition"]["summary"]
    if not inc["date"] and timeline.get("anchor_iso"):
        inc["date"] = str(timeline["anchor_iso"])[:10]
    return inc


def _clean_charge(raw: Optional[str]) -> Optional[str]:
    """OCR'd finding lines carry leading cruft ('hat it causes discredit…',
    'ge 4 2. SCDSA M.O.U. 18.5(d) - Inexcusable Neglect of Duty -'). Trim to the
    readable violation: prefer the part after an 'M.O.U. <code> -' marker, else
    drop a short leading fragment. Best-effort — only used for display."""
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip().strip("-").strip()
    m = re.search(r"M\.O\.U\.\s*[\d.()a-z]+\s*-\s*(.+)", s)
    if m:
        return m.group(1).strip().strip("-").strip()
    return s or None


# ---------------------------------------------------------------------------
# 3. Beats — one per key moment, chronologically ordered, pinned to assets
# ---------------------------------------------------------------------------

def _asset_for_source(src, manifest_by_stem: Dict[str, str]) -> Optional[str]:
    if not src or not src.media_path:
        return None
    return manifest_by_stem.get(Path(str(src.media_path)).stem)


def _dur_of(asset_id: str, manifest: List[Dict]) -> float:
    for m in manifest:
        if m["asset_id"] == asset_id:
            return float(m.get("duration_sec") or 0.0)
    return 0.0


def build_beats(verdict: Dict, sources: List, manifest: List[Dict],
                manifest_by_stem: Dict[str, str], agency: str) -> List[Dict]:
    credit = f"Courtesy {agency}"
    raw = verdict.get("key_moments", []) or []

    def abs_t(m: Dict) -> Optional[float]:
        i = m.get("source_idx", 0)
        s = sources[i] if 0 <= i < len(sources) else None
        if s and s.start_epoch is not None:
            return s.start_epoch + float(m.get("timestamp_sec") or 0)
        return None

    timeline_mode = any(s.start_epoch is not None for s in sources)
    if timeline_mode:
        ordered = sorted(raw, key=lambda m: (abs_t(m) is None, abs_t(m) or 0.0))
    else:
        ordered = sorted(raw, key=lambda m: (m.get("source_idx", 0), m.get("timestamp_sec") or 0))

    beats: List[Dict] = []
    for n, m in enumerate(ordered):
        i = m.get("source_idx", 0)
        src = sources[i] if 0 <= i < len(sources) else None
        asset_id = _asset_for_source(src, manifest_by_stem)
        phase = (src.phase if src else None) or "incident"
        ts = float(m.get("timestamp_sec") or 0)
        end = float(m.get("end_timestamp_sec") or ts)
        primary = quote = None
        source_refs: List[str] = []
        if asset_id:
            dur = _dur_of(asset_id, manifest)
            in_sec = max(0.0, ts - HEAD_PAD)
            out_sec = min(dur, end + TAIL_PAD) if dur else end + TAIL_PAD
            primary = {"asset_id": asset_id, "in_sec": round(in_sec, 2), "out_sec": round(out_sec, 2)}
            source_refs.append(f"{asset_id}@{ts:g}")
        excerpt = (m.get("transcript_excerpt") or "").strip()
        if excerpt and asset_id:
            quote = {"text": excerpt, "source": asset_id, "timecode": ts}
        lt_text = (src.person.title() if src and src.person else
                   (src.label if src else "Source"))
        beats.append({
            "beat_id": f"b{n:02d}", "act_id": f"act_{phase}", "ordinal": n,
            "function": m.get("moment_type") or "beat",
            "target_duration_sec": round(min(20.0, max(6.0, (end - ts) + HEAD_PAD + TAIL_PAD)), 1),
            "primary_asset": primary,
            "quote": quote,
            "inserts": [],
            "lower_third": {"text": lt_text, "attribution_confidence": 0.5},
            "narration_bridge": None,   # filled by _add_bridges
            "credit": credit,
            "source_refs": source_refs,
            "_description": m.get("description", ""),
            "_importance": m.get("importance"),
            "_phase": phase,
            "_abs": abs_t(m),
        })
    return beats


def _add_bridges(beats: List[Dict], incident: Dict) -> None:
    """Flag a narration bridge where the act changes (and at the open). The
    LLM tier writes the line; the skeleton sets needed + a sourced brief."""
    prev_act = None
    fact_keys = [k for k in ("charges", "disposition", "location") if incident.get(k)]
    for b in beats:
        if b["act_id"] != prev_act:
            b["narration_bridge"] = {
                "needed": True,
                "brief": (f"Bridge into {b['act_id'].replace('act_', '').replace('_', ' ')}; "
                          f"establish: {b['_description'][:90]}"),
                "source_facts": [f"incident.{k}" for k in fact_keys] + b["source_refs"],
            }
            prev_act = b["act_id"]


# ---------------------------------------------------------------------------
# 4. Acts — narrative spine, runtime budgeted deterministically
# ---------------------------------------------------------------------------

def build_acts(timeline: Dict, beats: List[Dict], doc_extracts: List[Dict],
               target_runtime: float) -> List[Dict]:
    phases_present = list((timeline.get("phases") or {}).keys())
    # an accountability act exists if we have documents, even without media
    if doc_extracts and "investigation" not in phases_present:
        phases_present.append("investigation")
    ordered_phases = [p for p, _, _ in _PHASE_ACT if p in phases_present]

    beats_by_act: Dict[str, List[str]] = {}
    for b in beats:
        beats_by_act.setdefault(b["act_id"], []).append(b["beat_id"])

    acts: List[Dict] = [{
        "act_id": "act_cold_open", "title": "Cold Open", "phase": None,
        "function": "hook", "target_sec": COLD_OPEN_SEC, "thesis": None, "beat_ids": [],
    }]
    # Distribute the remaining budget across content acts ∝ beat count (acts with
    # no beats — e.g. an outcome carried only by the document — get a floor).
    rest = max(0.0, target_runtime - COLD_OPEN_SEC)
    nbeats_total = sum(len(beats_by_act.get(f"act_{p}", [])) for p in ordered_phases)
    FLOOR = 60.0
    for p in ordered_phases:
        bids = beats_by_act.get(f"act_{p}", [])
        share = (len(bids) / nbeats_total) if nbeats_total else 0.0
        target = round(rest * share, 1) if bids else FLOOR
        acts.append({
            "act_id": f"act_{p}", "title": _PHASE_TITLE.get(p, p.title()),
            "phase": p, "function": _PHASE_FUNC.get(p, "establish"),
            "target_sec": target, "thesis": None, "beat_ids": bids,
        })
    return acts


# ---------------------------------------------------------------------------
# 5. Integrity ledger + 6. gaps + 7. metadata
# ---------------------------------------------------------------------------

def build_integrity_ledger(beats: List[Dict], incident: Dict,
                           doc_extracts: List[Dict]) -> List[Dict]:
    ledger: List[Dict] = []
    # Document-derived on-screen facts.
    doc_src = doc_extracts[0].get("doc_type", "document") if doc_extracts else None
    if incident.get("charges"):
        ledger.append({"claim": f"charges: {', '.join(incident['charges'][:6])}",
                       "source": f"doc:{doc_src}", "ok": bool(doc_src), "beat_id": None})
    if incident.get("disposition"):
        ledger.append({"claim": f"disposition: {incident['disposition'][:80]}",
                       "source": f"doc:{doc_src}", "ok": bool(doc_src), "beat_id": None})
    # Per-beat footage + quote claims.
    for b in beats:
        ok = b["primary_asset"] is not None
        ledger.append({
            "claim": f"footage: {b['function']} ({b['lower_third']['text']})",
            "source": (b["source_refs"][0] if b["source_refs"] else None),
            "ok": ok, "beat_id": b["beat_id"],
        })
        if b["quote"]:
            ledger.append({
                "claim": f"quote: “{b['quote']['text'][:60]}”",
                "source": f"{b['quote']['source']}@{b['quote']['timecode']:g}",
                "ok": True, "beat_id": b["beat_id"],
            })
    return ledger


def build_gaps(manifest: List[Dict], timeline: Dict, beats: List[Dict],
               incident: Dict) -> List[Dict]:
    gaps: List[Dict] = []
    phases_present = set((timeline.get("phases") or {}).keys())
    kinds_present = {m["kind"] for m in manifest}
    beat_phases = {b["_phase"] for b in beats}

    # Phases with no narratable moment.
    for p in ("pre_incident", "incident", "aftermath"):
        if p in phases_present and p not in beat_phases:
            gaps.append({"phase": p, "kind": None,
                         "missing": f"no narratable key-moment in '{p}'",
                         "why": "assets exist for this phase but P4 surfaced no moment",
                         "acquire": "transcribe/score this phase's assets"})
    # Initial 911 call (the dispatch that sent units).
    if "911_audio" not in kinds_present:
        gaps.append({"phase": "pre_incident", "kind": "911_audio",
                     "missing": "initial 911 / dispatch audio",
                     "why": "no call audio to open on or establish the call",
                     "acquire": "FOIA agency CAD/911 for the event number"})
    # Stills / booking photo for cutaways.
    if "photo" not in kinds_present:
        gaps.append({"phase": None, "kind": "photo",
                     "missing": "scene stills / booking photo",
                     "why": "no still imagery for cutaways or the outcome card",
                     "acquire": "agency records / booking; scene photographs in discovery"})
    # Unsourced beats (hard error — a beat with no footage).
    for b in beats:
        if b["primary_asset"] is None:
            gaps.append({"phase": b["_phase"], "kind": None,
                         "missing": f"beat {b['beat_id']} ({b['function']}) has no resolvable footage",
                         "why": "key_moment source_idx did not map to a manifest asset",
                         "acquire": "verify transcript source_url / media presence"})
    return gaps


def build_metadata(manifest: List[Dict], acts: List[Dict], beats: List[Dict],
                   timeline: Dict, target_runtime: float) -> Dict:
    footage = sum(float(m.get("duration_sec") or 0) for m in manifest
                  if m["kind"] in (_VIDEO_KINDS | {"911_audio", "radio"}))
    planned = (COLD_OPEN_SEC + len(acts) * PHASE_CARD_SEC
               + sum(b["target_duration_sec"] for b in beats)
               + sum(BRIDGE_SEC for b in beats if b.get("narration_bridge")))
    cov: Dict[str, str] = {}
    phases = timeline.get("phases") or {}
    beats_by_phase: Dict[str, int] = {}
    for b in beats:
        beats_by_phase[b["_phase"]] = beats_by_phase.get(b["_phase"], 0) + 1
    for p, recs in phases.items():
        cov[p] = f"{len(recs)} asset(s), {beats_by_phase.get(p, 0)} beat(s)"
    canon = [p for p, _, _ in _PHASE_ACT]
    covered = sum(1 for p in canon if phases.get(p))
    return {
        "available_footage_sec": round(footage, 1),
        "planned_runtime_sec": round(planned, 1),
        "runtime_vs_target_sec": round(planned - target_runtime, 1),
        "coverage_by_phase": cov,
        "asset_completeness_pct": round(100 * covered / len(canon), 1),
        "unsourced_beats": sum(1 for b in beats if b["primary_asset"] is None),
        "built_by": "skeleton",
    }


# ---------------------------------------------------------------------------
# Vision beats — the silent visual moments a transcript can't see (vision_scan)
# ---------------------------------------------------------------------------

_FORCE_EVENTS = {"k9_deployment", "taser", "strike", "takedown", "weapon_drawn",
                 "firearm_pointed", "use_of_force_other"}
# Visual-action events keep their beat even next to dialogue: a spoken "dog on
# bite!" and the VISIBLE release are complementary angles, not a duplicate — the
# whole reason vision exists is to show what audio can't.
_VISUAL_KEEP = _FORCE_EVENTS | {"foot_pursuit", "handcuffing"}


def _humanize(event_type: str) -> str:
    return (event_type or "").replace("_", " ").title()


def build_vision_beats(vision_events: List[Dict], manifest: List[Dict],
                       timeline_index: Dict[str, Dict], agency: str,
                       existing_beats: List[Dict], pad: float = 4.0,
                       dedup_tol: float = 8.0, cross_pov_bucket: float = 5.0) -> List[Dict]:
    """Turn vision_scan events into sourced beats. Two key behaviours:

    * **POV-by-doer / cross-POV dedup** — the same force event seen on several
      cameras collapses to ONE beat, kept on the highest-confidence camera (the
      best view, ≈ the actor's angle).
    * **No double-count** — a vision event within ``dedup_tol`` (absolute time) of
      an existing transcript beat is dropped (the spoken moment already covers it).
    """
    if not vision_events:
        return []
    credit = f"Courtesy {agency}".strip()
    by_label = {m.get("pov_label"): m for m in manifest}
    by_id = {m["asset_id"]: m for m in manifest}

    def art_epoch(aid: str) -> Optional[float]:
        return (timeline_index.get(aid) or {}).get("start_epoch")

    def ev_abs(ev: Dict) -> Optional[float]:
        e = art_epoch(ev.get("artifact_id"))
        return (e + float(ev["timecode_sec"])) if e is not None else None

    # cross-POV dedup -> one event per (type, time-bucket), highest confidence wins
    groups: Dict[Any, Dict] = {}
    for i, ev in enumerate(vision_events):
        at = ev_abs(ev)
        key = (ev["event_type"], round(at / cross_pov_bucket) if at is not None else f"_{i}")
        if key not in groups or ev["confidence"] > groups[key]["confidence"]:
            groups[key] = ev

    # absolute times of the transcript beats, to skip vision duplicates of dialogue
    tb_abs: List[float] = []
    for b in existing_beats:
        pa = b.get("primary_asset") or {}
        m = by_id.get(pa.get("asset_id"))
        ep = art_epoch(m["pov_label"]) if m else None
        if ep is not None:
            tb_abs.append(ep + float(pa.get("in_sec", 0)) + HEAD_PAD)

    out: List[Dict] = []
    for ev in sorted(groups.values(), key=lambda e: (ev_abs(e) is None, ev_abs(e) or e["timecode_sec"])):
        asset = by_label.get(ev.get("artifact_id"))
        if not asset:
            continue
        at = ev_abs(ev)
        # generic vision events defer to dialogue; visual-action events never do.
        if (at is not None and ev["event_type"] not in _VISUAL_KEEP
                and any(abs(at - t) <= dedup_tol for t in tb_abs)):
            continue
        dur = float(asset.get("duration_sec") or 0)
        ts = float(ev["timecode_sec"])
        in_sec, out_sec = max(0.0, ts - pad), (min(dur, ts + pad) if dur else ts + pad)
        out.append({
            "act_id": f"act_{asset.get('phase') or 'incident'}",
            "function": ev["event_type"],
            "target_duration_sec": round(min(20.0, max(6.0, out_sec - in_sec)), 1),
            "primary_asset": {"asset_id": asset["asset_id"],
                              "in_sec": round(in_sec, 2), "out_sec": round(out_sec, 2)},
            "quote": None, "inserts": [],
            "lower_third": {"text": _humanize(ev["event_type"]),
                            "attribution_confidence": round(float(ev.get("confidence", 0.5)), 2)},
            "narration_bridge": None, "credit": credit,
            "source_refs": [f"{asset['asset_id']}@{ts:g}", "vision"],
            "source": "vision", "is_vision": True,
            "_description": ev.get("description", ""),
            "_importance": "high" if ev["event_type"] in _FORCE_EVENTS else "medium",
            "_phase": asset.get("phase") or "incident",
            "_abs": at,
        })
    return out


# ---------------------------------------------------------------------------
# Substance floor — accountability moments a "drama"-tuned scorer drops
# ---------------------------------------------------------------------------

# Procedural-violation / misconduct language: charge-stacking, record/report
# manipulation, coaching, cover-up. The accountability CORE — must never be
# crowded out of an 8-moment cap by a fifth emotional-peak.
_PROC_PAT = re.compile(
    r"\b(face your record|his record|your record|manipulat\w*|falsif\w*|fabricat\w*"
    r"|good for (?:a |a couple|several|multiple|some)?\s*\w*\s*felon\w*"
    r"|stack\w* charges?|add\w* (?:a )?charge|we'?ll (?:just )?say|make it look"
    r"|plant\w*|cover (?:it|this) up|off the record|coach\w*)\b", re.I)


def _sentences(text: str, n: int = 2) -> str:
    """First ``n`` sentences of a blob, trimmed — keeps a record card readable."""
    parts = re.split(r"(?<=[.!?])\s+", (text or "").strip())
    return " ".join(parts[:n]).strip()


def build_document_beats(doc_extracts: List[Dict], agency: str,
                         max_findings: int = 4) -> List[Dict]:
    """Turn the IA record into ACCOUNTABILITY beats — the act that footage can't
    carry. Each beat is a card-only beat (no primary_asset): real text from the
    disposition, narrative, and the IA's own clip directions, sourced to the
    document. These render as on-screen record cards in 'The Record' act, so the
    accountability core is shown, not just summarised by the closing outcome card.

    Faithful by construction: every line is verbatim/near-verbatim from
    ``doc_extract`` and attributed to it — nothing is invented, and no unrelated
    footage is played under a document claim.
    """
    credit = f"Courtesy {agency}".strip()
    beats: List[Dict] = []

    def _emit(text: str, label: str, refs: List[str], importance: str = "high",
              dur: float = 12.0) -> None:
        if not text or not text.strip():
            return
        n = len(beats)
        beats.append({
            "beat_id": f"doc{n:02d}", "act_id": "act_investigation", "ordinal": n,
            "function": "record", "target_duration_sec": round(dur, 1),
            "primary_asset": None, "quote": None, "inserts": [],
            "lower_third": {"text": label, "attribution_confidence": 1.0},
            "narration_bridge": {"needed": True, "text": text.strip(),
                                 "brief": None, "source_facts": refs},
            "credit": credit, "source_refs": refs,
            "source": "document", "is_document": True,
            "_description": text.strip()[:160], "_importance": importance,
            "_phase": "investigation", "_abs": None,
        })

    for de in doc_extracts:
        case_no = de.get("ia_case_number") or "the internal affairs case"
        dref = f"doc:{de.get('doc_type', 'document')}"
        narrative = _narrative_text(de) or ""
        # 1. What the investigation found (the spine of the act).
        intro = _sentences(narrative, 2)
        _emit(intro, f"IA {case_no}", [dref], "critical", 14.0)
        # 2. The pivot: he had seized the drugs himself earlier that shift.
        seize = next((s for s in re.split(r"(?<=[.!?])\s+", narrative)
                      if re.search(r"confiscat|seiz|tinfoil|pocket", s, re.I)), "")
        _emit(_sentences(seize, 2), "Internal Affairs Investigation", [dref], "critical", 12.0)
        # 3. The IA's own description of how he was found (its clip direction).
        for cd in (de.get("clip_directions") or []):
            ref = (cd.get("ref") or "").strip()
            if cd.get("kind") == "bwc_description" and len(ref) > 40:
                _emit(ref[:240], "From the Investigation Report",
                      [f"{dref}#p{cd.get('page')}"], "high", 12.0)
                break
        # 4. The sustained findings (the accountability verdict).
        findings = (de.get("disposition") or {}).get("findings") or []
        sustained = [f for f in findings if (f.get("finding") or "").upper() == "SUSTAINED"]
        for f in sustained[:max_findings]:
            ch = _clean_charge(f.get("charge"))
            if ch:
                _emit(f"Internal Affairs finding — SUSTAINED: {ch}.",
                      "Findings", [f"{dref}#p{f.get('page')}"], "high", 8.0)
    return beats


def substance_beats(verdict: Dict, sources: List, manifest: List[Dict],
                    manifest_by_stem: Dict[str, str], agency: str,
                    existing_beats: List[Dict], pad: float = 5.0) -> List[Dict]:
    """Scan the transcripts for procedural-violation language and emit must-keep
    beats for any the scorer missed. Sourced (the line is on tape), high
    importance, deduped against beats already present near the same spot."""
    credit = f"Courtesy {agency}".strip()
    have = set()
    for b in existing_beats:
        pa = b.get("primary_asset") or {}
        if pa.get("asset_id"):
            have.add((pa["asset_id"], round(float(pa.get("in_sec", 0)) / 12)))
    out: List[Dict] = []
    refs = verdict.get("transcript_refs") or []
    for idx, ref in enumerate(refs):
        try:
            tdata = json.loads(Path(str(ref)).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        src = sources[idx] if 0 <= idx < len(sources) else None
        asset_id = _asset_for_source(src, manifest_by_stem)
        if not asset_id:
            continue
        dur = _dur_of(asset_id, manifest)
        phase = (src.phase if src else None) or "incident"
        for seg in tdata.get("transcript", []):
            text = (seg.get("text") or "").strip()
            if not _PROC_PAT.search(text):
                continue
            ts = float(seg.get("start_sec", 0))
            key = (asset_id, round(ts / 12))
            if key in have:
                continue                       # scorer already has this moment
            have.add(key)
            in_sec = max(0.0, ts - pad)
            out_sec = min(dur, float(seg.get("end_sec", ts)) + pad) if dur else ts + pad
            out.append({
                "act_id": f"act_{phase}", "function": "procedural_violation",
                "target_duration_sec": round(min(20.0, max(6.0, out_sec - in_sec)), 1),
                "primary_asset": {"asset_id": asset_id, "in_sec": round(in_sec, 2), "out_sec": round(out_sec, 2)},
                "quote": {"text": text[:200], "source": asset_id, "timecode": ts},
                "inserts": [], "lower_third": {"text": "Procedural Concern", "attribution_confidence": 1.0},
                "narration_bridge": None, "credit": credit,
                "source_refs": [f"{asset_id}@{ts:g}", "substance_scan"],
                "source": "substance_scan", "is_substance": True,
                "_description": f"On-camera: “{text[:120]}”",
                "_importance": "critical", "_phase": phase,
                "_abs": (src.start_epoch + ts) if (src and src.start_epoch is not None) else None,
            })
    return out


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def resolve_same_asset_overlaps(beats: List[Dict], min_dur: float = 2.0) -> List[Dict]:
    """Stop the same footage replaying. When consecutive beats play the SAME
    asset with overlapping windows (the 6 K9 vision beats overlapped — 74-82
    played twice), split each overlap at its midpoint so the clips tile the
    footage contiguously and it plays through once, labels intact."""
    for i in range(1, len(beats)):
        ppa = (beats[i - 1] or {}).get("primary_asset")
        pa = (beats[i] or {}).get("primary_asset")
        if not (ppa and pa and ppa["asset_id"] == pa["asset_id"]):
            continue
        if pa["in_sec"] < ppa["out_sec"]:                      # overlap
            mid = round((pa["in_sec"] + ppa["out_sec"]) / 2.0, 2)
            ppa["out_sec"] = round(max(ppa["in_sec"] + min_dur, mid), 2)
            pa["in_sec"] = ppa["out_sec"]
            if pa["out_sec"] - pa["in_sec"] < min_dur:
                pa["out_sec"] = round(pa["in_sec"] + min_dur, 2)
        for b, q in ((beats[i - 1], ppa), (beats[i], pa)):
            b["target_duration_sec"] = round(q["out_sec"] - q["in_sec"], 1)
    return beats


def _strip_private(beats: List[Dict]) -> List[Dict]:
    return [{k: v for k, v in b.items() if not k.startswith("_")} for b in beats]


def build_blueprint(artifacts: List[Dict], timeline: Dict, verdict: Dict,
                    doc_extracts: List[Dict], doc_paths: List[Optional[str]],
                    sources: List, timeline_index: Dict[str, Dict], agency: str,
                    target_runtime: float = DEFAULT_RUNTIME,
                    media_dir: Optional[Path] = None,
                    vision_events: Optional[List[Dict]] = None) -> Dict:
    transcribed_stems = {Path(str(s.media_path)).stem for s in sources if s.media_path}
    manifest, stem_to_id = build_asset_manifest(
        artifacts, timeline_index, transcribed_stems, doc_extracts, doc_paths,
        agency, media_dir)
    incident = build_incident(doc_extracts, timeline)
    beats = build_beats(verdict, sources, manifest, stem_to_id, agency)
    _add_bridges(beats, incident)
    # Supplementary beat sources the transcript-scorer alone misses:
    #   vision  → silent visual moments (K9 release/takedown)
    #   substance → procedural-violation language an 8-moment cap crowds out
    extra: List[Dict] = []
    if vision_events:
        extra += build_vision_beats(vision_events, manifest, timeline_index, agency, beats)
    extra += substance_beats(verdict, sources, manifest, stem_to_id, agency, beats)
    # Accountability act: the IA record as on-screen beats (the document carries
    # the act that no footage can — sourced, not invented). Sorted last (no _abs).
    extra += build_document_beats(doc_extracts, agency)
    if extra:
        _PHASE_IDX = {p: i for i, (p, _, _) in enumerate(_PHASE_ACT)}
        beats = sorted(beats + extra,
                       key=lambda b: (_PHASE_IDX.get(b.get("_phase"), 99),
                                      b.get("_abs") is None, b.get("_abs") or 0.0))
        for n, b in enumerate(beats):
            b["beat_id"] = f"b{n:02d}"
            b["ordinal"] = n
    resolve_same_asset_overlaps(beats)   # no replaying the same footage
    acts = build_acts(timeline, beats, doc_extracts, target_runtime)
    ledger = build_integrity_ledger(beats, incident, doc_extracts)
    gaps = build_gaps(manifest, timeline, beats, incident)
    metadata = build_metadata(manifest, acts, beats, timeline, target_runtime)
    narration_points = [{"beat_id": b["beat_id"], **b["narration_bridge"]}
                        for b in beats if b.get("narration_bridge")]
    return {
        "case_id": verdict.get("case_id"),
        "logline": None,
        "target_runtime_sec": target_runtime,
        "agency": agency,
        "incident": incident,
        "asset_manifest": manifest,
        "acts": acts,
        "beats": _strip_private(beats),
        "narration_points": narration_points,
        "integrity_ledger": ledger,
        "gaps": gaps,
        "metadata": metadata,
    }


# ---------------------------------------------------------------------------
# Markdown render (the producer-facing doc)
# ---------------------------------------------------------------------------

def _mmss(sec: float) -> str:
    sec = int(round(sec))
    return f"{sec // 60}:{sec % 60:02d}"


def render_markdown(bp: Dict) -> str:
    inc = bp.get("incident", {})
    md: List[str] = []
    md.append(f"# Production Blueprint — {bp['case_id']}")
    sub = " · ".join(x for x in [inc.get("date"), inc.get("location"),
                                 f"target {_mmss(bp['target_runtime_sec'])}",
                                 bp.get("agency")] if x)
    md.append(f"_{sub}_\n")
    if inc.get("charges") or inc.get("disposition"):
        md.append(f"**Charges:** {', '.join(inc.get('charges', [])) or '—'}  ")
        md.append(f"**Disposition:** {inc.get('disposition') or '—'}\n")

    m = bp.get("metadata", {})
    md.append(f"> Planned **{_mmss(m.get('planned_runtime_sec', 0))}** vs target "
              f"{_mmss(bp.get('target_runtime_sec', 0))} "
              f"({m.get('runtime_vs_target_sec', 0):+.0f}s) · "
              f"{len(bp.get('beats', []))} beats · "
              f"{m.get('asset_completeness_pct', 0):.0f}% phase coverage · "
              f"{m.get('unsourced_beats', 0)} unsourced beat(s)\n")

    md.append("## Asset Manifest")
    md.append("| id | kind | label | phase | dur | transcript |")
    md.append("|---|---|---|---|---|---|")
    for a in bp["asset_manifest"]:
        d = _mmss(a["duration_sec"]) if a.get("duration_sec") else "—"
        md.append(f"| `{a['asset_id']}` | {a['kind']} | {a.get('pov_label') or '—'} "
                  f"| {a.get('phase') or '—'} | {d} | {'✓' if a['has_transcript'] else ''} |")
    md.append("")

    md.append("## Narrative Spine")
    for act in bp["acts"]:
        md.append(f"- **{act['title']}** ({act['function']}, ~{_mmss(act['target_sec'])}) "
                  f"— {len(act['beat_ids'])} beat(s)")
    md.append("")

    md.append("## Beat Sheet")
    beats_by_act: Dict[str, List[Dict]] = {}
    for b in bp["beats"]:
        beats_by_act.setdefault(b["act_id"], []).append(b)
    for act in bp["acts"]:
        bl = beats_by_act.get(act["act_id"], [])
        if not bl and act["act_id"] != "act_cold_open":
            continue
        md.append(f"### {act['title']}  ({_mmss(act['target_sec'])})")
        for b in bl:
            pa = b.get("primary_asset")
            loc = (f"`{pa['asset_id']}` {_mmss(pa['in_sec'])}–{_mmss(pa['out_sec'])}"
                   if pa else "⚠ NO FOOTAGE")
            q = f"  “{b['quote']['text']}”" if b.get("quote") else ""
            md.append(f"- **[{b['function']} · {_mmss(b['target_duration_sec'])}]** {loc}"
                      f" — {b['lower_third']['text']}{q}")
            if b.get("narration_bridge"):
                nb = b["narration_bridge"]
                md.append(f"    - _narration:_ {nb.get('text') or nb.get('brief') or ''}")
        md.append("")

    md.append("## Factual Integrity Ledger")
    for e in bp["integrity_ledger"]:
        mark = "✓" if e["ok"] else "⚠ UNSOURCED"
        md.append(f"- {mark} {e['claim']} — `{e.get('source') or 'NONE'}`")
    md.append("")

    md.append("## Gaps & Acquisition")
    if not bp["gaps"]:
        md.append("- none")
    for g in bp["gaps"]:
        where = g.get("phase") or g.get("kind") or "—"
        md.append(f"- **{g['missing']}** ({where}) — {g.get('why') or ''} → "
                  f"_{g.get('acquire') or ''}_")
    md.append("")
    return "\n".join(md)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="P6 — deterministic production blueprint (rails)")
    ap.add_argument("--artifacts", required=True, type=Path)
    ap.add_argument("--timeline", required=True, type=Path)
    ap.add_argument("--verdict", required=True, type=Path)
    ap.add_argument("--doc-extract", nargs="*", type=Path, default=[])
    ap.add_argument("--media-dir", type=Path, default=None)
    ap.add_argument("--agency", default="Releasing agency")
    ap.add_argument("--target-runtime", type=float, default=DEFAULT_RUNTIME)
    ap.add_argument("--vision", type=Path, default=None,
                    help="vision_scan events.json -> add silent visual moments as beats")
    ap.add_argument("--out", type=Path, default=Path(".tmp/blueprint"))
    args = ap.parse_args(argv)

    artifacts = json.loads(args.artifacts.read_text(encoding="utf-8"))
    timeline = json.loads(args.timeline.read_text(encoding="utf-8"))
    verdict = json.loads(args.verdict.read_text(encoding="utf-8"))
    doc_extracts, doc_paths = [], []
    for dp in args.doc_extract:
        doc_extracts.append(json.loads(dp.read_text(encoding="utf-8")))
        doc_paths.append(str(dp))
    vision_events = json.loads(args.vision.read_text(encoding="utf-8")) if args.vision else None

    timeline_index = rc._load_timeline_index(args.timeline)
    sources = rc.map_sources(verdict, args.media_dir, timeline_index=timeline_index)

    bp = build_blueprint(artifacts, timeline, verdict, doc_extracts, doc_paths,
                         sources, timeline_index, args.agency,
                         target_runtime=args.target_runtime, media_dir=args.media_dir,
                         vision_events=vision_events)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    cid = bp["case_id"]
    (out / f"{cid}_blueprint.json").write_text(json.dumps(bp, indent=2, ensure_ascii=False), encoding="utf-8")
    (out / f"{cid}_blueprint.md").write_text(render_markdown(bp), encoding="utf-8")
    m = bp["metadata"]
    print(f"[blueprint] {cid}: {len(bp['asset_manifest'])} assets, {len(bp['acts'])} acts, "
          f"{len(bp['beats'])} beats, {len(bp['gaps'])} gaps; planned {_mmss(m['planned_runtime_sec'])} "
          f"/ target {_mmss(bp['target_runtime_sec'])} ({m['unsourced_beats']} unsourced)")
    print(f"[blueprint] -> {out / (cid + '_blueprint.json')}  +  .md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
