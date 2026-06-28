"""P6 / GOAL_D — LLM shaping tier for the production blueprint.

The deterministic rails (``blueprint.py``) lay one sourced beat per key moment.
This tier lets an LLM do the EDITORIAL work the rails can't — select / order /
prune beats to fit the target runtime, write act theses + realized narration
from the briefs, and propose B-roll (from real-but-untranscribed assets) and
document/still inserts — and then a VALIDATOR rejects anything the model invented.

The integrity rail (generalized ±source guard): the LLM never returns footage,
quotes, or facts. It returns an *edit decision* that may only reference
``beat_id``s and ``asset_id``s that already exist in the blueprint, with
timecodes inside each asset's real duration. ``validate_edit`` drops every
reference that fails that test (recording each drop), and ``apply_edit`` rebuilds
the blueprint deterministically — primary footage and pinned quotes are carried
over untouched from the skeleton, never from the model. Runtime arithmetic stays
in code (the model picks; the code counts).

Backend-agnostic: ``shape_blueprint`` takes anything with a
``.complete(system, user, ...) -> str`` method — a real ``LLMBackend``
(pipeline4_scoring/llm_backends) or the ``MockBackend`` here (zero cost, for
tests and dry runs).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import blueprint as bpmod  # noqa: E402  (reuse _mmss + constants)

COLD_OPEN_SEC = bpmod.COLD_OPEN_SEC
PHASE_CARD_SEC = bpmod.PHASE_CARD_SEC
BRIDGE_SEC = bpmod.BRIDGE_SEC
MIN_BEAT_SEC, MAX_BEAT_SEC = 4.0, 90.0
MAX_BROLL_SEC = 25.0   # a B-roll establishing shot stays short


# ---------------------------------------------------------------------------
# The edit-decision contract (what the LLM returns) — see _build_prompt below.
#   { "logline": str,
#     "act_theses": {act_id: str},
#     "beat_order": [beat_id, ...],          # kept beats, in final order
#     "beat_durations": {beat_id: sec},      # optional, clamped to footage
#     "narration": {beat_id: str},           # realized text for needed bridges
#     "inserts": {beat_id: [{asset_id, kind, note}]},
#     "broll": [{after_beat, asset_id, in_sec, out_sec, role, note}] }
# ---------------------------------------------------------------------------


def _index(blueprint: Dict) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    beats_by_id = {b["beat_id"]: b for b in blueprint.get("beats", [])}
    assets_by_id = {a["asset_id"]: a for a in blueprint.get("asset_manifest", [])}
    return beats_by_id, assets_by_id


# LLMs are inconsistent about object-vs-array shapes; the guard tolerates both.
_ID_KEYS = ("act_id", "beat_id", "id", "key", "name")
_VAL_KEYS = ("thesis", "text", "value", "note", "duration", "sec", "seconds")


def _as_dict(v: Any) -> Dict[str, Any]:
    """Coerce a value to a dict. A list of ``{id-ish: ..., value-ish: ...}``
    objects (a common LLM rendering of a map) is folded into a dict."""
    if isinstance(v, dict):
        return v
    out: Dict[str, Any] = {}
    if isinstance(v, list):
        for it in v:
            if not isinstance(it, dict):
                continue
            kid = next((it[k] for k in _ID_KEYS if k in it), None)
            val = next((it[k] for k in _VAL_KEYS if k in it), None)
            if kid is not None and val is not None:
                out[str(kid)] = val
    return out


def _as_list(v: Any) -> List[Any]:
    if isinstance(v, list):
        return v
    return [] if v is None else [v]


def _as_id(v: Any) -> Optional[str]:
    """Coerce a beat-id-ish value to a hashable string id. Tolerates the model
    returning ``beat_order``/``cuts`` as objects (``{"beat_id": "b00"}``) instead of
    bare strings — without this, hashing a dict raises ``unhashable type: 'dict'``."""
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        for k in _ID_KEYS:
            if isinstance(v.get(k), (str, int)):
                return str(v[k])
        return None
    return str(v) if isinstance(v, (int,)) else None


def _inserts_map(v: Any) -> Dict[str, List[Dict]]:
    """Normalize inserts to ``{beat_id: [insert, ...]}`` whether the model sent a
    dict-of-lists or a flat list of ``{beat_id, asset_id, ...}`` objects."""
    if isinstance(v, dict):
        return {k: _as_list(items) for k, items in v.items()}
    out: Dict[str, List[Dict]] = {}
    for it in _as_list(v):
        if isinstance(it, dict) and it.get("beat_id"):
            out.setdefault(it["beat_id"], []).append(it)
    return out


def validate_edit(edit: Dict, blueprint: Dict) -> Tuple[Dict, List[Dict]]:
    """Strip every part of an LLM edit that isn't grounded in the blueprint.

    Returns ``(clean_edit, rejections)`` where ``rejections`` is a list of
    ``{field, value, reason}`` for transparency (surfaced in the edit report and
    the integrity story). Nothing here trusts the model's footage or facts.
    """
    beats_by_id, assets_by_id = _index(blueprint)
    rej: List[Dict] = []
    clean: Dict[str, Any] = {}

    clean["logline"] = (edit.get("logline") or None)

    # act theses: only for acts that exist
    act_ids = {a["act_id"] for a in blueprint.get("acts", [])}
    clean["act_theses"] = {}
    for aid, th in _as_dict(edit.get("act_theses")).items():
        if aid in act_ids and isinstance(th, str) and th.strip():
            clean["act_theses"][aid] = th.strip()
        elif aid not in act_ids:
            rej.append({"field": "act_theses", "value": aid, "reason": "unknown act_id"})

    # cuts: explicit, intentional prunes (omission alone never deletes a beat)
    clean["cuts"] = []
    for raw in _as_list(edit.get("cuts")):
        bid = _as_id(raw)
        if bid in beats_by_id:
            clean["cuts"].append(bid)
        else:
            rej.append({"field": "cuts", "value": bid, "reason": "unknown beat_id"})
    cut_set = set(clean["cuts"])

    # beat_order: must be a permutation/subset of real beat_ids; dups + cuts dropped
    seen: set = set()
    order: List[str] = []
    for raw in _as_list(edit.get("beat_order")):
        bid = _as_id(raw)
        if bid not in beats_by_id:
            rej.append({"field": "beat_order", "value": bid, "reason": "unknown beat_id"})
        elif bid in cut_set:
            continue   # explicitly cut elsewhere
        elif bid in seen:
            rej.append({"field": "beat_order", "value": bid, "reason": "duplicate"})
        else:
            seen.add(bid)
            order.append(bid)
    # beats the model forgot to mention are KEPT (omission never deletes); only an
    # explicit cut removes one. Re-append in original order — also covers the case
    # where the model gave no beat_order at all (then order == originals minus cuts).
    for b in blueprint.get("beats", []):
        if b["beat_id"] not in seen and b["beat_id"] not in cut_set:
            order.append(b["beat_id"])
    clean["beat_order"] = order

    # beat_durations: clamp to [MIN, footage length] of the beat's primary asset
    clean["beat_durations"] = {}
    for bid, sec in _as_dict(edit.get("beat_durations")).items():
        b = beats_by_id.get(bid)
        if not b:
            rej.append({"field": "beat_durations", "value": bid, "reason": "unknown beat_id"})
            continue
        try:
            sec = float(sec)
        except (TypeError, ValueError):
            rej.append({"field": "beat_durations", "value": bid, "reason": "non-numeric"})
            continue
        cap = MAX_BEAT_SEC
        pa = b.get("primary_asset")
        if pa:
            cap = min(cap, float(pa.get("out_sec", 0)) - float(pa.get("in_sec", 0)) + 1e-6)
        clean["beat_durations"][bid] = round(max(MIN_BEAT_SEC, min(sec, cap)), 1)

    # narration: realized text for real beats only
    clean["narration"] = {}
    for bid, text in _as_dict(edit.get("narration")).items():
        if bid in beats_by_id and isinstance(text, str) and text.strip():
            clean["narration"][bid] = text.strip()
        elif bid not in beats_by_id:
            rej.append({"field": "narration", "value": bid, "reason": "unknown beat_id"})

    # inserts: asset_id must exist in the manifest
    clean["inserts"] = {}
    for bid, items in _inserts_map(edit.get("inserts")).items():
        if bid not in beats_by_id:
            rej.append({"field": "inserts", "value": bid, "reason": "unknown beat_id"})
            continue
        kept = []
        for it in _as_list(items):
            if isinstance(it, str):          # a bare asset_id string
                it = {"asset_id": it}
            if not isinstance(it, dict):
                continue
            aid = it.get("asset_id")
            if aid in assets_by_id:
                kept.append({"asset_id": aid, "kind": it.get("kind") or assets_by_id[aid]["kind"],
                             "note": (it.get("note") or "")[:140]})
            else:
                rej.append({"field": "inserts", "value": aid, "reason": "unknown asset_id"})
        if kept:
            clean["inserts"][bid] = kept

    # broll: new sourced filler from a real asset; window inside its duration
    clean["broll"] = []
    for bz in _as_list(edit.get("broll")):
        if not isinstance(bz, dict):
            continue
        aid = bz.get("asset_id")
        asset = assets_by_id.get(aid)
        if not asset:
            rej.append({"field": "broll", "value": aid, "reason": "unknown asset_id"})
            continue
        dur = float(asset.get("duration_sec") or 0)
        try:
            in_sec = max(0.0, float(bz.get("in_sec", 0)))
            out_sec = float(bz.get("out_sec", in_sec + 10))
        except (TypeError, ValueError):
            rej.append({"field": "broll", "value": aid, "reason": "non-numeric window"})
            continue
        if dur:
            out_sec = min(out_sec, dur)
        out_sec = min(out_sec, in_sec + MAX_BROLL_SEC)
        if out_sec - in_sec < 1.0:
            rej.append({"field": "broll", "value": aid, "reason": "empty/out-of-range window"})
            continue
        after = bz.get("after_beat")
        if after is not None and after not in beats_by_id:
            rej.append({"field": "broll", "value": after, "reason": "unknown after_beat"})
            after = None
        # Cold open (front-anchored B-roll) must ESTABLISH, not spoil: only
        # dashcam/911/radio — never a bodycam, which shows the people/resolution
        # (a downed-suspect clip in the cold open gives away the ending).
        if after is None and asset.get("kind") not in ("dashcam", "911_audio", "radio"):
            rej.append({"field": "broll", "value": aid,
                        "reason": f"cold-open B-roll must establish (got {asset.get('kind')})"})
            continue
        clean["broll"].append({
            "after_beat": after, "asset_id": aid, "in_sec": round(in_sec, 1),
            "out_sec": round(out_sec, 1), "role": (bz.get("role") or "broll"),
            "note": (bz.get("note") or "")[:140],
        })
    return clean, rej


# ---------------------------------------------------------------------------
# Apply — rebuild the blueprint from the validated edit (deterministic)
# ---------------------------------------------------------------------------

def _broll_beat(bz: Dict, assets_by_id: Dict[str, Dict], credit: str, ordinal: int) -> Dict:
    aid = bz["asset_id"]
    asset = assets_by_id[aid]
    dur = round(bz["out_sec"] - bz["in_sec"], 1)
    return {
        "beat_id": f"br{ordinal:02d}", "act_id": None, "ordinal": ordinal,
        "function": bz.get("role") or "broll",
        "target_duration_sec": round(min(dur, MAX_BROLL_SEC), 1),
        "primary_asset": {"asset_id": aid, "in_sec": bz["in_sec"], "out_sec": bz["out_sec"]},
        "quote": None, "inserts": [],
        "lower_third": {"text": asset.get("pov_label") or asset["kind"], "attribution_confidence": 1.0},
        "narration_bridge": None, "credit": credit,
        "source_refs": [f"{aid}@{bz['in_sec']:g}"],
        "is_broll": True, "broll_note": bz.get("note", ""),
    }


def apply_edit(blueprint: Dict, clean: Dict, rejections: List[Dict]) -> Dict:
    """Produce the shaped blueprint. Footage + quotes are carried over from the
    skeleton untouched; the LLM only reorders, prunes, annotates, and adds
    sourced B-roll/inserts. Ledger + metadata are recomputed in code."""
    out = json.loads(json.dumps(blueprint))   # deep copy
    beats_by_id, assets_by_id = _index(out)
    credit = f"Courtesy {out.get('agency', '')}".strip()

    # 1. logline + act theses
    if clean.get("logline"):
        out["logline"] = clean["logline"]
    theses = clean.get("act_theses", {})
    for a in out.get("acts", []):
        if a["act_id"] in theses:
            a["thesis"] = theses[a["act_id"]]

    # 2. reorder + prune beats per beat_order (validator already re-appended any
    #    forgotten beats and removed cuts, so an empty list means "all cut").
    order = clean["beat_order"] if "beat_order" in clean else [b["beat_id"] for b in out["beats"]]
    new_beats = [beats_by_id[bid] for bid in order if bid in beats_by_id]

    # 3. per-beat: duration override, realized narration, inserts
    durs = clean.get("beat_durations", {})
    narr = clean.get("narration", {})
    inserts = clean.get("inserts", {})
    for b in new_beats:
        bid = b["beat_id"]
        if bid in durs:
            b["target_duration_sec"] = durs[bid]
        if bid in narr and b.get("narration_bridge"):
            b["narration_bridge"]["text"] = narr[bid]
        elif bid in narr:   # narration on a beat that had no bridge → create one, pinned
            b["narration_bridge"] = {"needed": True, "text": narr[bid],
                                     "brief": None, "source_facts": b.get("source_refs", [])}
        if bid in inserts:
            b["inserts"] = inserts[bid]

    # 4. splice in B-roll beats after their anchor (or at the front)
    broll = clean.get("broll", [])
    if broll:
        spliced: List[Dict] = []
        by_anchor: Dict[Optional[str], List[Dict]] = {}
        for i, bz in enumerate(broll):
            by_anchor.setdefault(bz.get("after_beat"), []).append(
                _broll_beat(bz, assets_by_id, credit, i))
        for bz in by_anchor.get(None, []):     # front-anchored (cold open) first
            spliced.append(bz)
        for b in new_beats:
            spliced.append(b)
            for bz in by_anchor.get(b["beat_id"], []):
                bz["act_id"] = b["act_id"]     # inherit the act it follows
                spliced.append(bz)
        new_beats = spliced

    # 5. renumber ordinals, write back
    for i, b in enumerate(new_beats):
        b["ordinal"] = i
    out["beats"] = new_beats

    # 6. recompute act beat_ids, integrity ledger, metadata (all deterministic)
    by_act: Dict[str, List[str]] = {}
    for b in new_beats:
        by_act.setdefault(b.get("act_id"), []).append(b["beat_id"])
    for a in out.get("acts", []):
        a["beat_ids"] = by_act.get(a["act_id"], [])

    out["integrity_ledger"] = _recompute_ledger(out, new_beats)
    out["narration_points"] = [{"beat_id": b["beat_id"], **b["narration_bridge"]}
                               for b in new_beats if b.get("narration_bridge")]
    out["metadata"] = _recompute_metadata(out, new_beats)
    out["metadata"]["built_by"] = "llm_shaped"
    out["edit_report"] = {
        "kept_beats": len([b for b in new_beats if not b.get("is_broll")]),
        "broll_added": len([b for b in new_beats if b.get("is_broll")]),
        "rejections": rejections,
    }
    return out


# ---------------------------------------------------------------------------
# Factual fact-check rail — grounding-as-precision extended from QUOTES to FRAMING
# ---------------------------------------------------------------------------
import re as _re

# Civilian-victim / custody framing. When the subject of the case is law
# enforcement (a deputy who overdosed on drugs he himself seized), narration that
# recasts the central figure as a handcuffed detainee in a patrol car is not a
# style choice — it inverts the case and defames the agency. These are the
# confabulation signatures observed in the wild (see HANDOFF cut critique).
_FALSE_CUSTODY_PAT = _re.compile(
    r"\b(detainee|arrestee|in handcuffs|handcuffed|hand-cuffed|prisoner|inmate"
    r"|patrol car|squad car|cruiser|the back of the (?:car|cruiser|vehicle)"
    r"|left (?:him|the \w+) alone)\b", _re.I)
_LE_PAT = _re.compile(r"\b(deputy|officer|sergeant|sgt|corporal|cpl|detective|trooper|the subject is an officer)\b", _re.I)


def _subject_is_le(facts: Dict) -> bool:
    blob = " ".join([facts.get("summary") or "", " ".join(facts.get("subjects") or []),
                     facts.get("disposition") or ""])
    return bool(_LE_PAT.search(blob))


# Analytical accusations — the strong claims an analysis voiceover might assert.
# Each is ALLOWED only if the case record (findings/facts) supports it; otherwise
# it is an ungrounded conclusion about a real person and is quarantined. This is
# the fact-check rail extended from FRAMING to ANALYTICAL CLAIMS (see _SYSTEM_ANALYSIS).
# (accusation pattern, support pattern). The support is intentionally NARROW and
# word-boundaried: a faithfulness rail should err toward FLAGGING (a false flag
# just degrades a line to its safe brief; a false PASS airs a defamatory claim).
# So generic words ('evidence', 'cover' inside 'discovered') must NOT satisfy support.
_ACCUSATIONS = [
    (_re.compile(r"\b(cover[- ]?up|covered (?:it|this|that) up)\b", _re.I),
     _re.compile(r"\b(dishonest\w*|conceal\w*|withh\w*|withheld|hid|hidden|untruth\w*|discredit\w*)\b", _re.I)),
    (_re.compile(r"\b(fabricat\w*|falsif\w*|forged?|doctored)\b", _re.I),
     _re.compile(r"\b(dishonest\w*|falsif\w*|fabricat\w*|forg\w*)\b", _re.I)),
    (_re.compile(r"\b(planted|plant(?:ing)? (?:the )?evidence|tamper\w*)\b", _re.I),
     _re.compile(r"\b(plant\w*|tamper\w*|fabricat\w*|forg\w*)\b", _re.I)),
    (_re.compile(r"\b(perjur\w*|lied|lying|dishonest\w*|untruthful)\b", _re.I),
     _re.compile(r"\b(dishonest\w*|untruth\w*|misle\w*|perjur\w*|\blie[ds]?\b|false statement)\b", _re.I)),
    (_re.compile(r"\b(murder\w*|homicide|execut(?:ed|ion)|unlawful killing)\b", _re.I),
     _re.compile(r"\b(murder\w*|homicide|unlawful killing|shot and killed)\b", _re.I)),
    (_re.compile(r"\b(excessive force|brutality|brutal beating)\b", _re.I),
     _re.compile(r"\b(excessive force|strike|struck|baton|taser|punch\w*|use of force|brutal\w*)\b", _re.I)),
    (_re.compile(r"\b(corrupt\w*|brib\w*|conspir\w*|racketeer\w*)\b", _re.I),
     _re.compile(r"\b(corrupt\w*|brib\w*|conspir\w*|racket\w*)\b", _re.I)),
]


def _unsupported_accusation(text: str, factblob: str) -> Optional[str]:
    """Return the first strong accusation in ``text`` that the record doesn't back."""
    for pat, support in _ACCUSATIONS:
        m = pat.search(text)
        if m and not support.search(factblob):
            return m.group(0)
    return None


def audit_narration(shaped: Dict, facts: Dict) -> List[Dict]:
    """Drop-on-fail faithfulness rail for narration, extended from QUOTES to FRAMING
    to ANALYTICAL CLAIMS. Two independent checks, both quarantine (blank) the
    offending logline / thesis / narration and log it:

      1. CONTRADICTED FRAMING — when the subject is law enforcement, narration that
         recasts them as a civilian detainee (a custody/patrol-car story the facts
         lack) inverts the case. Skipped if the record itself mentions custody.
      2. UNGROUNDED ACCUSATION — a strong analytical claim (cover-up, lie, crime,
         excessive force, corruption) the sustained findings / facts don't support.
         This is what lets the ANALYSIS tier make claims safely: the model may say
         what the record says, and no more.

    Returns ``{field, value, reason}`` flags (mutates ``shaped``)."""
    flags: List[Dict] = []
    factblob = " ".join([facts.get("summary") or "", facts.get("disposition") or "",
                         " ".join(facts.get("charges") or [])])
    # The custody check applies only when the subject is LE AND the record doesn't
    # itself establish a custody context; the accusation check always applies.
    custody_on = _subject_is_le(facts) and not _FALSE_CUSTODY_PAT.search(factblob)

    def _violation(text: Optional[str]):
        if not text:
            return None
        if custody_on:
            m = _FALSE_CUSTODY_PAT.search(text)
            if m:
                return (f"contradicts CASE_FACTS (subject is LE; '{m.group(0)}' "
                        f"implies a civilian detainee)")
        acc = _unsupported_accusation(text, factblob)
        if acc:
            return (f"ungrounded accusation '{acc}' — not in the sustained findings "
                    f"or established facts")
        return None

    reason = _violation(shaped.get("logline"))
    if reason:
        flags.append({"field": "logline", "value": (shaped["logline"] or "")[:120], "reason": reason})
        shaped["logline"] = None
    for a in shaped.get("acts", []):
        reason = _violation(a.get("thesis"))
        if reason:
            flags.append({"field": f"act_theses[{a.get('act_id')}]",
                          "value": (a.get("thesis") or "")[:120], "reason": reason})
            a["thesis"] = None
    for b in shaped.get("beats", []):
        nb = b.get("narration_bridge") or {}
        reason = _violation(nb.get("text"))
        if reason:
            flags.append({"field": f"narration[{b.get('beat_id')}]",
                          "value": (nb.get("text") or "")[:120], "reason": reason})
            nb["text"] = nb.get("brief")   # degrade to the sourced brief (or None)
    return flags


def _recompute_ledger(blueprint: Dict, beats: List[Dict]) -> List[Dict]:
    ledger: List[Dict] = []
    # keep the document-derived (non-beat) claims from the skeleton ledger
    for e in blueprint.get("integrity_ledger", []):
        if e.get("beat_id") is None:
            ledger.append(e)
    for b in beats:
        if b.get("is_document"):           # record beat — sourced to the IA report
            ledger.append({
                "claim": f"record: {((b.get('narration_bridge') or {}).get('text') or '')[:60]}",
                "source": (b["source_refs"][0] if b.get("source_refs") else "doc"),
                "ok": True, "beat_id": b["beat_id"],
            })
            continue
        ledger.append({
            "claim": f"footage: {b['function']} ({b['lower_third']['text']})",
            "source": (b["source_refs"][0] if b["source_refs"] else None),
            "ok": b.get("primary_asset") is not None, "beat_id": b["beat_id"],
        })
        if b.get("quote"):
            ledger.append({
                "claim": f"quote: “{b['quote']['text'][:60]}”",
                "source": f"{b['quote']['source']}@{b['quote']['timecode']:g}",
                "ok": True, "beat_id": b["beat_id"],
            })
    return ledger


def _recompute_metadata(blueprint: Dict, beats: List[Dict]) -> Dict:
    md = dict(blueprint.get("metadata", {}))
    planned = (COLD_OPEN_SEC + len(blueprint.get("acts", [])) * PHASE_CARD_SEC
               + sum(b["target_duration_sec"] for b in beats)
               + sum(BRIDGE_SEC for b in beats if b.get("narration_bridge")))
    md["planned_runtime_sec"] = round(planned, 1)
    md["runtime_vs_target_sec"] = round(planned - blueprint.get("target_runtime_sec", 0), 1)
    md["unsourced_beats"] = sum(1 for b in beats
                                if b.get("primary_asset") is None and not b.get("is_document"))
    return md


# ---------------------------------------------------------------------------
# Prompt + orchestration
# ---------------------------------------------------------------------------

_SYSTEM = (
    "You are a documentary editor assembling a long-form (target ~30 min) cut for a "
    "police-accountability film from a fixed pile of real evidence. You are given a "
    "production blueprint: a typed asset manifest, acts, and one sourced beat per "
    "key moment. Your job is purely EDITORIAL.\n\n"
    "HARD RULES (violations are discarded):\n"
    "- Never invent footage, quotes, or facts. Reference only beat_id and asset_id "
    "values that appear in the blueprint.\n"
    "- B-roll and inserts must cite an existing asset_id; B-roll windows must lie "
    "inside that asset's duration.\n"
    "- Narration is connective only; it may assert nothing beyond the beat's "
    "source_facts. Keep it spare.\n"
    "- FACTUAL GROUNDING (this is a real person and a real case — getting the story "
    "wrong is defamation): the logline, every act thesis, and all narration MUST be "
    "consistent with the CASE FACTS block. Use the named subject(s) and the events "
    "stated there. NEVER introduce a person, victim, detainee, arrest, or outcome "
    "that the CASE FACTS do not state. If the facts say the subject is an officer, do "
    "not reframe them as a civilian (or vice-versa). When unsure, stay closer to the "
    "literal facts and say less.\n"
    "Return ONLY a JSON object with keys: logline, act_theses, beat_order, cuts, "
    "beat_durations, narration, inserts, broll. (Omitting a beat keeps it; list it "
    "in 'cuts' to drop it.) No prose."
)

# Analysis tier — the EWU / Dr. Insanity voice: narration that explains and
# analyzes rather than merely connecting. Same integrity rails; the difference is
# the narration ROLE and a hard "ground your judgments in the record" rule, since
# analysis makes claims and unsupported claims about a real person are defamation.
_SYSTEM_ANALYSIS = (
    "You are writing the ANALYTICAL VOICEOVER for a long-form police-accountability "
    "documentary — the explain-and-analyze style of channels like Explore With Us and "
    "Dr. Insanity. The narration is the spine of the film: it doesn't just describe "
    "what's on screen, it ANALYZES — it explains procedure, points out what the viewer "
    "should notice, builds tension, and lands the accountability.\n\n"
    "HARD RULES (violations are discarded):\n"
    "- Never invent footage, quotes, or facts. Reference only beat_id and asset_id "
    "values that appear in the blueprint.\n"
    "- B-roll and inserts must cite an existing asset_id; windows inside its duration.\n"
    "- GROUNDED ANALYSIS (this is a real person; an unsupported claim is defamation): "
    "every analytical claim must rest on CASE_FACTS or ANALYSIS_BASIS. ATTRIBUTE "
    "judgments to the record — 'Internal Affairs would sustain a finding of...', 'the "
    "investigation found...', 'policy requires...' — never deliver a verdict the record "
    "doesn't support. Do NOT assert a cover-up, a lie, a crime, excessive force, or any "
    "wrongdoing unless it appears in the sustained findings or established facts. When "
    "the record is SILENT on a point, raise it as a QUESTION, do not assert the answer.\n"
    "- Name the real subject(s); introduce no person the facts don't name; never reframe "
    "an officer as a civilian or vice-versa.\n"
    "- Each beat's narration is 1-3 sentences — analytical, not rambling.\n"
    "Return ONLY a JSON object with keys: logline, act_theses, beat_order, cuts, "
    "beat_durations, narration, inserts, broll. No prose."
)


# Compact move definitions (decoupled copy of narration_grammar.MOVES) so a learned
# grammar profile can be turned into a prompt directive without a P4 import.
_MOVE_DEFS = {
    "hook": "open with a tease that promises a payoff",
    "scene_set": "describe what is happening on screen now",
    "foreshadow": "hint at what is coming; build dread",
    "procedure": "explain how the policing / policy / procedure normally works",
    "contradiction": "flag an inconsistency or what doesn't add up",
    "credibility": "assess the subject's truthfulness, motive, or character",
    "question": "pose a rhetorical question to the viewer",
    "stakes": "name the emotional weight or consequences",
    "context": "give background — who someone is, prior events",
    "legal_cover": "note it is based on official records / presumed innocent",
    "transition": "recap or move to the next phase",
    "verdict": "state the accountability outcome — findings, discipline",
}
_GRAMMAR_PHASE_ORDER = ["pre_incident", "incident", "aftermath", "transport", "investigation", "outcome"]


def _grammar_directive(profile: Dict) -> str:
    """Turn a learned narration-grammar profile (narration_grammar.py) into a prompt
    directive: per-phase analytical-move emphasis + the move meanings + cadence."""
    pxm = profile.get("phase_x_move", {}) or {}
    used: set = set()
    rows: List[str] = []
    for ph in _GRAMMAR_PHASE_ORDER:
        mv = pxm.get(ph)
        if mv:
            top = [m for m in list(mv.keys())[:4] if m != "scene_set"][:3] or list(mv.keys())[:3]
            used.update(top)
            rows.append(f"  {ph}: {', '.join(top)}")
    defs = "; ".join(f"{m} = {_MOVE_DEFS[m]}" for m in _MOVE_DEFS if m in used)
    cad = profile.get("cadence", {}) or {}
    vo = cad.get("vo_word_share") or 0
    run = cad.get("mean_footage_run_segments") or 0
    pace = ("intercut frequently — keep clips short and narrate often"
            if run and run < 11 else "let footage clips run longer; narrate in fuller passages")
    chan = profile.get("channel", "this channel")
    opens = ", ".join(dict.fromkeys(profile.get("open_moves", [])[:3])) or "hook, foreshadow"
    return (
        f"\nNARRATION GRAMMAR — write in the style of {chan} (learned from {profile.get('n_videos','its')} "
        f"of its videos). For each beat, make the analytical MOVE its phase calls for:\n"
        + "\n".join(rows) + "\n"
        f"MOVE MEANINGS: {defs}.\n"
        f"CADENCE: {chan} is voiceover ~{int(vo * 100)}% of the time — {pace}.\n"
        f"OPEN the cut with: {opens}. Build toward the VERDICT at the end.\n")


def _build_prompt(blueprint: Dict, style: str = "connective", grammar: Optional[Dict] = None) -> str:
    manifest = [{"asset_id": a["asset_id"], "kind": a["kind"], "label": a.get("pov_label"),
                 "phase": a.get("phase"), "duration_sec": a.get("duration_sec"),
                 "has_transcript": a.get("has_transcript")}
                for a in blueprint.get("asset_manifest", [])]
    acts = [{"act_id": a["act_id"], "title": a["title"], "function": a["function"],
             "target_sec": a["target_sec"]} for a in blueprint.get("acts", [])]
    beats = [{"beat_id": b["beat_id"], "act_id": b["act_id"], "function": b["function"],
              "kind": "record" if b.get("is_document") else "footage",
              "duration_sec": b["target_duration_sec"],
              "quote": (b["quote"] or {}).get("text"),
              "description": b.get("description") or b.get("_description"),
              "needs_narration": bool((b.get("narration_bridge") or {}).get("needed"))}
             for b in blueprint.get("beats", [])]
    inc = blueprint.get("incident") or {}
    case_facts = {
        "subjects": inc.get("subjects") or [],
        "date": inc.get("date"),
        "location": inc.get("location"),
        "summary": inc.get("summary"),          # authoritative prose from the IA record
        "charges": inc.get("charges") or [],
        "disposition": inc.get("disposition"),
    }
    payload = {
        "case_id": blueprint.get("case_id"),
        "target_runtime_sec": blueprint.get("target_runtime_sec"),
        "CASE_FACTS": case_facts,
        "incident": blueprint.get("incident"),
        "asset_manifest": manifest,
        "acts": acts,
        "beats": beats,
        "gaps": blueprint.get("gaps"),
        "current_planned_runtime_sec": blueprint.get("metadata", {}).get("planned_runtime_sec"),
    }
    if style == "analysis":
        # The sustained findings ARE the authoritative analysis — the only judgments
        # of wrongdoing the narration may invoke, and only attributed to the record.
        payload["ANALYSIS_BASIS"] = {
            "subject": case_facts["subjects"],
            "established_facts": case_facts["summary"],
            "sustained_findings": case_facts["charges"],
            "outcome": case_facts["disposition"],
            "how_to_use": ("These are the ONLY judgments of wrongdoing you may assert, and you must "
                           "attribute them to the record (e.g. 'Internal Affairs would sustain a "
                           "finding of inexcusable neglect'). For anything the findings do not cover, "
                           "pose it as an open question, never a conclusion."),
        }
        instruction = (
            "Write the ANALYTICAL VOICEOVER for this documentary in the explain-and-analyze style of "
            "long-form accountability channels (Explore With Us, Dr. Insanity).\n"
            "1. logline: one sharp analytical sentence (the case's meaning, not a plot summary).\n"
            "2. act_theses: one thesis per act — what this act PROVES.\n"
            "3. narration: write voiceover for the FOOTAGE beats (kind='footage') — ESPECIALLY the "
            "setup/seizure, the discovery, and any procedural beats. SAY WHAT THE VIEWER IS SEEING AND "
            "WHY IT MATTERS: on the setup, foreshadow ('this is the moment that will define everything "
            "after'); on the discovery, explain the significance; tie the arc to the sustained findings. "
            "The record beats (kind='record') ALREADY carry the findings text — do NOT just echo a "
            "beat's existing text; keep record beats terse and spend your words on the footage. Build "
            "ONE through-line across the acts.\n"
            "GROUND every analytical claim in CASE_FACTS / ANALYSIS_BASIS and ATTRIBUTE judgments to "
            "the record ('Internal Affairs would sustain...', 'policy requires...'); never assert "
            "wrongdoing the findings don't sustain — where the record is silent, raise the QUESTION, "
            "don't answer it. Each narration is 1-3 sentences. Then order beats chronologically within "
            "acts, prune weak/duplicate beats, preserve procedural_violation/record beats, and add "
            "sourced establishing B-roll (dashcam/911 only, never a bodycam) for the cold open.\n\n")
        if grammar:
            instruction += _grammar_directive(grammar) + "\n"
        return instruction + json.dumps(payload, ensure_ascii=False)
    return (
        "Ground EVERY word of the logline, act theses, and narration in CASE_FACTS "
        "below — it is the authoritative account from the official record. Use the "
        "named subject(s); do not invent any other person, victim, or outcome. Then "
        "shape this blueprint toward its target runtime. Order beats chronologically "
        "within acts; prune weak/duplicate beats; lengthen key beats and add sourced "
        "B-roll to build runtime HONESTLY from the manifest — do not pad with invented "
        "material. COLD-OPEN B-roll (broll with after_beat=null) must ESTABLISH only — "
        "dashcam or 911/radio audio, NEVER a bodycam (a bodycam shows the people and "
        "the resolution, spoiling the story). Preserve any procedural_violation or "
        "contradiction beat — those are the accountability core, never cut them. Write "
        "a tight logline, one thesis per act, and realized narration only where "
        "needs_narration is true.\n\n"
        + json.dumps(payload, ensure_ascii=False)
    )


class MockBackend:
    """Zero-cost backend for tests / dry runs. Returns a fixed edit-decision
    JSON (set at construction), so the whole shape→validate→apply path runs
    without a network call."""

    def __init__(self, edit: Optional[Dict] = None):
        self._edit = edit if edit is not None else {}
        self.last_prompt: Optional[str] = None

    def complete(self, system: str, user: str, **kw) -> str:  # noqa: D401
        self.last_prompt = user
        return json.dumps(self._edit, ensure_ascii=False)


def shape_blueprint(blueprint: Dict, backend, *, max_tokens: int = 4000,
                    temperature: float = 0.2, style: str = "connective",
                    grammar: Optional[Dict] = None) -> Tuple[Dict, Dict]:
    """Run one shaping pass. Returns ``(shaped_blueprint, report)``. ``report``
    carries the rejection list + raw edit, so a caller can see exactly what the
    model proposed and what the validator threw out. ``style='analysis'`` switches
    the narration from spare/connective to grounded analytical voiceover (the
    EWU / Dr. Insanity tier), behind the same fact-check rail. ``grammar`` (a learned
    narration_grammar profile) steers per-phase analytical moves + cadence."""
    system = _SYSTEM_ANALYSIS if style == "analysis" else _SYSTEM
    user = _build_prompt(blueprint, style=style, grammar=grammar)
    raw = backend.complete(system=system, user=user, max_tokens=max_tokens,
                           temperature=temperature)
    try:
        from llm_backends import clean_llm_output  # type: ignore
        raw = clean_llm_output(raw)
    except Exception:
        pass
    parse_error: Optional[str] = None
    try:
        edit = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except (json.JSONDecodeError, TypeError) as e:
        edit = {}
        nchars = len(raw) if isinstance(raw, str) else 0
        parse_error = (f"{type(e).__name__}: model output not valid JSON ({nchars} chars) — "
                       f"likely TRUNCATED at the token limit; raise --max-tokens")
    # The deterministic rails are guaranteed; the LLM tier is best-effort layered
    # on top. Any malformed edit degrades to the skeleton — it never crashes.
    try:
        clean, rejections = validate_edit(edit, blueprint)
        shaped = apply_edit(blueprint, clean, rejections)
    except Exception as e:  # noqa: BLE001
        shaped = json.loads(json.dumps(blueprint))   # untouched skeleton
        rejections = [{"field": "_shaping", "value": f"{type(e).__name__}: {str(e)[:120]}",
                       "reason": "shaping failed; kept deterministic skeleton"}]
        shaped.setdefault("edit_report", {})["error"] = rejections[0]["value"]
    # Fact-check rail: quarantine framing that contradicts the case facts. Run
    # after apply so it sees the final logline/theses/narration, and recompute the
    # narration_points that apply_edit derived (they may have been blanked).
    factual_flags = audit_narration(shaped, blueprint.get("incident") or {})
    if factual_flags:
        shaped["narration_points"] = [{"beat_id": b["beat_id"], **b["narration_bridge"]}
                                      for b in shaped.get("beats", []) if b.get("narration_bridge")]
        shaped.setdefault("edit_report", {})["factual_flags"] = factual_flags
        rejections = list(rejections) + factual_flags
    if parse_error:                       # surface truncation instead of silently degrading
        shaped.setdefault("edit_report", {})["parse_error"] = parse_error
        rejections = [{"field": "_parse", "value": parse_error,
                       "reason": "model output unparseable; kept skeleton"}] + list(rejections)
    report = {"rejections": rejections, "raw_edit": edit, "factual_flags": factual_flags,
              "parse_error": parse_error, "built_by": shaped["metadata"]["built_by"]}
    return shaped, report


def make_openrouter_backend(model: str = "google/gemini-3.1-flash-lite-preview"):
    """Build a live OpenRouter backend (reuses P4's llm_backends). Needs
    OPENROUTER_API_KEY (loaded from .env by the caller). Paid — call sparingly."""
    p4 = Path(__file__).resolve().parent.parent / "pipeline4_scoring"
    sys.path.insert(0, str(p4))
    from llm_backends import build_backend  # type: ignore
    return build_backend(model)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="P6 — shape a deterministic blueprint with the LLM tier (guarded)")
    ap.add_argument("--blueprint", required=True, type=Path, help="skeleton <id>_blueprint.json")
    ap.add_argument("--out", type=Path, default=None, help="output dir (default: alongside input)")
    ap.add_argument("--model", default=None, help="OpenRouter model (default: gemini flash-lite)")
    ap.add_argument("--mock", action="store_true",
                    help="no live call — run the guard/apply path with an empty edit (zero cost)")
    ap.add_argument("--analysis", action="store_true",
                    help="grounded ANALYTICAL voiceover (EWU / Dr. Insanity tier) instead of spare "
                         "connective narration — still behind the fact-check rail")
    ap.add_argument("--grammar", type=Path, default=None,
                    help="a learned narration_grammar profile json — steer per-phase analytical "
                         "moves + cadence in the style of that channel (use with --analysis)")
    ap.add_argument("--max-tokens", type=int, default=3500)
    args = ap.parse_args(argv)
    style = "analysis" if args.analysis else "connective"
    # Analytical voiceover is far more verbose than connective bridges — give it
    # room or the JSON truncates and silently degrades to the skeleton.
    if style == "analysis" and args.max_tokens < 12000:
        args.max_tokens = 12000

    blueprint = json.loads(args.blueprint.read_text(encoding="utf-8"))
    if args.mock:
        backend = MockBackend({})           # empty edit → skeleton passes through the guard
        print("[shape] MOCK backend (no network, no cost)")
    else:
        try:
            from dotenv import load_dotenv   # type: ignore
            load_dotenv()
        except Exception:
            pass
        backend = make_openrouter_backend(args.model) if args.model else make_openrouter_backend()
        gtag = f" grammar={args.grammar.stem}" if args.grammar else ""
        print(f"[shape] live: {backend.model}  (paid — one call)  style={style}{gtag}")

    grammar = json.loads(args.grammar.read_text(encoding="utf-8")) if args.grammar else None
    shaped, report = shape_blueprint(blueprint, backend, max_tokens=args.max_tokens,
                                     style=style, grammar=grammar)

    out_dir = Path(args.out) if args.out else args.blueprint.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    cid = shaped.get("case_id", "case")
    (out_dir / f"{cid}_blueprint_shaped.json").write_text(
        json.dumps(shaped, indent=2, ensure_ascii=False), encoding="utf-8")
    (out_dir / f"{cid}_blueprint_shaped.md").write_text(
        bpmod.render_markdown(shaped), encoding="utf-8")

    er = shaped.get("edit_report", {})
    m = shaped["metadata"]
    if er.get("parse_error"):
        print(f"[shape] ⚠ {er['parse_error']}")
    if er.get("error"):
        print(f"[shape] ⚠ shaping failed ({er['error']}) — kept deterministic skeleton")
    ff = report.get("factual_flags") or []
    if ff:
        print(f"[shape] ⚠ {len(ff)} FACTUAL flag(s) quarantined (framing contradicted CASE_FACTS):")
        for f in ff:
            print(f"        - {f['field']}: {f['reason']}")
    print(f"[shape] built_by={m['built_by']}  kept={er.get('kept_beats','?')} beats, "
          f"+{er.get('broll_added',0)} b-roll, {len(report['rejections'])} rejection(s); "
          f"planned {bpmod._mmss(m['planned_runtime_sec'])} / target {bpmod._mmss(shaped['target_runtime_sec'])}")
    print(f"[shape] -> {out_dir / (cid + '_blueprint_shaped.json')}  +  .md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
