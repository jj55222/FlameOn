"""GOAL_D — bundle evaluator (the judge).

Evaluates a produced EDIT BUNDLE (the blueprint / paper-edit that assembles the
mini-video) against the EVIDENCE it drew from — not the bundle in isolation. It
asks: given everything that was available, did this assembly make good choices?
It does NOT author anything (no narration, no fabrication) — it reads the plan
and reasons about the selection.

Two layers, mirroring the rest of the engine:

  1. DETERMINISTIC craft + coverage (objective, pure, zero-cost). Catches exactly
     the things a human reviewer catches by hand: footage that replays itself,
     unsourced beats, runtime vs target, whether the high-substance moments
     (procedural violations, contradictions, use-of-force) are actually present,
     and — given an evidence pool — what high-value material was left OUT.
  2. LLM editorial judgment (substance / salience / narrative), GROUNDED on the
     deterministic findings so it can't hand-wave past an objective defect.

Outputs a scored verdict + critique. ``compare_bundles`` does pairwise A-vs-B —
the preference signal that closes the autoresearch loop (the reward model).

Backend-agnostic: a live ``JudgeBackend`` (OpenRouter) or ``MockJudgeBackend``
(zero cost, tests/dry-runs). Pure helpers are import-light; the LLM is optional.
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Accountability-substance vs viewer-salience lenses (a moment can be both).
SUBSTANCE_TYPES = {
    "procedural_violation", "contradiction", "reveal", "detail_noticed",
    "k9_deployment", "taser", "strike", "takedown", "weapon_drawn",
    "firearm_pointed", "use_of_force_other",
}
SALIENCE_TYPES = {"emotional_peak", "reveal", "contradiction", "tension_shift",
                  "k9_deployment", "takedown", "foot_pursuit"}
# Beat FUNCTIONS that are themselves a use of force (the P4/evaluate moment-type path).
FORCE_FUNCTION_TYPES = {"k9_deployment", "taser", "strike", "takedown",
                        "weapon_drawn", "firearm_pointed", "use_of_force_other"}
_PHASE_ORDER = ["pre_incident", "incident", "aftermath", "transport",
                "investigation", "outcome"]

# asset_manifest kinds that are camera / moving-image footage (the A-roll). A
# flagship blueprint carries these in primary_asset and never sets beat["source"],
# so a 28-clip bodycam cut must be classified off the ASSET, not off b["source"].
VISION_KINDS = {"bodycam", "body_worn_camera", "bwc", "dashcam", "dash_cam", "dash",
                "video", "surveillance", "cctv", "helicopter", "aerial", "drone",
                "cellphone_video", "cell_video", "security_camera"}
# kinds that are still documents / images (record beats, not footage).
DOCUMENT_KINDS = {"document", "report", "pdf", "photo", "photos", "image", "images",
                  "exhibit", "map", "diagram"}
# Force is present even when no beat FUNCTION is a force label — a beat_miner-driven
# flagship tags a shooting as reveal/tension_shift, so the only signal is the words.
_FORCE_CUES = re.compile(
    r"\b(shots?\s+fired|gun\s*shots?|shot|shoot(?:s|ing)?|open(?:ed|ing)?\s+fire|"
    r"gunfire|discharg(?:e|ed|ing)|tas(?:er|ed|ing|e)|bean\s*bag|k-?9|canine|"
    r"takedown|baton|struck|punch(?:ed|es)?|pepper\s*spray|o\.?c\.?\s*spray)\b", re.I)


def _asset_kind_lookup(bundle: Dict) -> Dict[str, str]:
    """asset_id -> lowercased kind, from the bundle's asset_manifest (list or dict)."""
    am = bundle.get("asset_manifest") or []
    items = am.values() if isinstance(am, dict) else am
    out: Dict[str, str] = {}
    for a in items:
        if isinstance(a, dict) and a.get("asset_id"):
            out[a["asset_id"]] = str(a.get("kind") or "").lower()
    return out


def classify_beat_source(beat: Dict, kinds: Dict[str, str]) -> str:
    """Classify one beat into vision|broll|document|transcript|narration by the KIND
    of its primary asset (looked up in the asset_manifest), falling back to the
    asset-id naming convention (``v_`` = footage, ``x_doc_``/``doc_`` = document) and
    then the beat's own flags. This is the fix for the judge scoring flagship cuts
    ``vision:0``: the footage lives in primary_asset (ids ``v_*``, kind bodycam), the
    beat never sets ``source:"vision"``, and a quote is attached to every beat."""
    pa = beat.get("primary_asset") or {}
    aid = pa.get("asset_id") or ""
    kind = kinds.get(aid, "")
    # B-roll first: an explicit broll beat is broll even if its clip is footage.
    if beat.get("is_broll") or kind == "broll":
        return "broll"
    # Moving-image footage — the A-roll the "visual integration" critique is about.
    if kind in VISION_KINDS or aid.startswith("v_"):
        return "vision"
    # Documents / stills (record beats sourced to the IA report, photos, exhibits).
    if beat.get("is_document") or kind in DOCUMENT_KINDS or aid.startswith("x_doc") or aid.startswith("doc_"):
        return "document"
    # Legacy explicit tag wins if a producer set it.
    if beat.get("source") == "vision":
        return "vision"
    # An audio/other asset carrying a quote, or a quote-only beat ⇒ transcript.
    if beat.get("quote"):
        return "transcript"
    # No asset and no quote ⇒ authored narration bridge.
    if not aid:
        return "narration"
    return "transcript"


def _force_in_beats(beats: List[Dict]) -> bool:
    """True if any beat's words describe a use of force. Scans quote + narration +
    description so an OIS cut whose beats are labelled reveal/tension_shift still
    registers the shooting instead of a false ``has_use_of_force: false``."""
    for b in beats:
        parts = [(b.get("quote") or {}).get("text", ""),
                 (b.get("narration_bridge") or {}).get("text", ""),
                 str(b.get("description") or "")]
        if _FORCE_CUES.search(" ".join(p for p in parts if p)):
            return True
    return False


# ---------------------------------------------------------------------------
# Normalize either a shaped blueprint or a rendered paper_edit into clips/beats
# ---------------------------------------------------------------------------

def _clips(artifact: Dict) -> List[Dict]:
    """Ordered playable clips as ``{media, in_sec, out_sec}`` — works on a
    paper_edit (``timeline``) or a blueprint (``beats``)."""
    if "timeline" in artifact:
        return [{"media": e.get("media", ""), "in_sec": float(e.get("in_sec", 0)),
                 "out_sec": float(e.get("out_sec", 0))}
                for e in artifact["timeline"] if e.get("kind") == "clip"]
    out = []
    for b in artifact.get("beats", []):
        pa = b.get("primary_asset")
        if pa:
            out.append({"media": pa.get("asset_id", ""), "in_sec": float(pa.get("in_sec", 0)),
                        "out_sec": float(pa.get("out_sec", 0))})
    return out


# ---------------------------------------------------------------------------
# 1a. Deterministic CRAFT findings (objective)
# ---------------------------------------------------------------------------

def craft_findings(bundle: Dict, paper_edit: Optional[Dict] = None) -> Dict:
    """Objective assembly defects. ``paper_edit`` (the realized cut) gives the
    truest play order/windows; otherwise the blueprint beats are used."""
    clips = _clips(paper_edit or bundle)
    beats = bundle.get("beats", [])

    # replays: a clip starting before the previous clip on the SAME media ended
    replays: List[Dict] = []
    last_out: Dict[str, float] = {}
    for c in clips:
        m = c["media"]
        if m in last_out and c["in_sec"] < last_out[m] - 0.5:
            replays.append({"media": Path(m).name, "in": c["in_sec"], "prev_out": round(last_out[m], 1)})
        last_out[m] = max(last_out.get(m, 0.0), c["out_sec"])

    # A beat is unsourced only if it has neither footage NOR a document source —
    # record beats (is_document) are sourced to the IA report, not to a clip.
    unsourced = [b.get("beat_id") for b in beats
                 if not b.get("primary_asset") and not b.get("is_document")]
    md = bundle.get("metadata", {})
    planned, target = md.get("planned_runtime_sec", 0), bundle.get("target_runtime_sec", 0)
    ratio = round(planned / target, 2) if target else None
    return {
        "replays": replays,
        "replay_count": len(replays),
        "unsourced_beats": unsourced,
        "all_sourced": not unsourced,
        "planned_runtime_sec": planned,
        "target_runtime_sec": target,
        "runtime_ratio": ratio,
        "runtime_flag": ("under" if ratio is not None and ratio < 0.6
                         else "over" if ratio is not None and ratio > 1.2 else "ok"),
        "clip_count": len(clips),
    }


# ---------------------------------------------------------------------------
# 1b. Deterministic COVERAGE / omissions (vs the evidence pool)
# ---------------------------------------------------------------------------

def _beat_types(beats: List[Dict]) -> List[str]:
    return [b.get("function") or "" for b in beats]


def coverage_findings(bundle: Dict, pool: Optional[List[Dict]] = None) -> Dict:
    """What the bundle covers — substance vs salience — and, given a candidate
    ``pool`` (moments/events that were AVAILABLE), what high-value material it
    left out (an omission like the dropped procedural violation)."""
    beats = bundle.get("beats", [])
    types = _beat_types(beats)
    tset = set(types)
    phases = {(b.get("act_id") or "").replace("act_", "") for b in beats}

    substance_hit = sorted(tset & SUBSTANCE_TYPES)
    salience_hit = sorted(tset & SALIENCE_TYPES)

    # Classify every beat once by the KIND of its primary asset (not b["source"],
    # which flagship blueprints never set) so bodycam A-roll is counted as vision.
    kinds = _asset_kind_lookup(bundle)
    src_classes = [classify_beat_source(b, kinds) for b in beats]

    omissions: List[Dict] = []
    if pool:
        present = {(b.get("function"), round(float((b.get("primary_asset") or {}).get("in_sec", -1)) / 5))
                   for b in beats}
        for cand in pool:
            ct = cand.get("moment_type") or cand.get("function") or cand.get("event_type")
            ckey = (ct, round(float(cand.get("timestamp_sec", cand.get("timecode_sec", -1))) / 5))
            high_value = ct in SUBSTANCE_TYPES or ct in SALIENCE_TYPES
            if high_value and ckey not in present and not any(p[0] == ct for p in present):
                omissions.append({"type": ct, "desc": (cand.get("description") or "")[:100]})

    return {
        "beat_count": len(beats),
        "substance_types_present": substance_hit,
        "salience_types_present": salience_hit,
        "has_procedural": "procedural_violation" in tset,
        "has_use_of_force": bool(tset & FORCE_FUNCTION_TYPES) or _force_in_beats(beats),
        "phases_covered": sorted(p for p in phases if p),
        "source_mix": {
            "vision": src_classes.count("vision"),
            "broll": src_classes.count("broll"),
            "document": src_classes.count("document"),
            "transcript": src_classes.count("transcript"),
            "narration": src_classes.count("narration"),
        },
        "omissions": omissions,
    }


def deterministic_report(bundle: Dict, paper_edit: Optional[Dict] = None,
                         pool: Optional[List[Dict]] = None) -> Dict:
    return {"craft": craft_findings(bundle, paper_edit),
            "coverage": coverage_findings(bundle, pool)}


# ---------------------------------------------------------------------------
# 2. LLM judgment layer
# ---------------------------------------------------------------------------

_SYSTEM = (
    "You are a documentary supervising editor reviewing an automated rough-cut "
    "ASSEMBLY for a police-accountability film. You are given the edit bundle (an "
    "ordered beat list with sources) and a DETERMINISTIC report of objective "
    "defects already measured for you (replays, unsourced beats, runtime, which "
    "substance/salience moments are present, and material left out). Judge the "
    "SELECTION and ASSEMBLY against the evidence — do not rewrite it, do not invent "
    "moments. Weigh TWO axes separately: accountability-SUBSTANCE (does it carry "
    "the procedural violations, contradictions, uses of force that make it matter?) "
    "and viewer-SALIENCE (is it compelling, well-hooked, well-paced?). Trust the "
    "deterministic findings as ground truth. Return ONLY JSON: "
    '{"verdict":"SHIP|REVISE|REWORK","scores":{"substance":0-1,"salience":0-1,'
    '"craft":0-1,"overall":0-1},"strengths":["..."],"issues":[{"dimension":"..",'
    '"severity":"low|med|high","note":".."}],"omissions":["..."]}'
)

_ENUM_SYSTEM = (
    "You are the single editorial critic for a police-accountability documentary paper edit. "
    "Judge the assembly only: evidence use, thesis delivery, pacing, and visual storytelling. "
    "Never judge whether the underlying case is worth producing; never invent a replacement route. "
    "Trust the deterministic report as ground truth. Return ONLY JSON with no numeric scores: "
    '{"verdict":"SHIP|REVISE|ABSTAIN|REJECT","reasons":[{"code":"...","note":"...",'
    '"source_refs":["beat:b00","rule:no_trace_language"]}],"strengths":["..."],'
    '"omissions":["..."]}. Use ABSTAIN when the case/cut is outside the evidence you can judge.'
)


def _bundle_digest(bundle: Dict) -> Dict:
    kinds = _asset_kind_lookup(bundle)
    beats = []
    for b in bundle.get("beats", []):
        pa = b.get("primary_asset") or {}
        beats.append({
            "act": b.get("act_id"), "function": b.get("function"),
            "importance": b.get("importance") or b.get("_importance"),
            "source": classify_beat_source(b, kinds),
            "quote": (b.get("quote") or {}).get("text"),
            "desc": (b.get("description") or "")[:90],
            "asset": pa.get("asset_id"),
        })
    return {
        "case_id": bundle.get("case_id"), "logline": bundle.get("logline"),
        "incident": bundle.get("incident"),
        "acts": [{"title": a.get("title"), "thesis": a.get("thesis")} for a in bundle.get("acts", [])],
        "beats": beats,
    }


def build_prompt(bundle: Dict, report: Dict) -> str:
    return ("EDIT BUNDLE:\n" + json.dumps(_bundle_digest(bundle), ensure_ascii=False)
            + "\n\nDETERMINISTIC REPORT (ground truth):\n" + json.dumps(report, ensure_ascii=False)
            + "\n\nJudge the assembly. JSON only.")


def _strip_fences(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"^```(?:json)?\s*\n?", "", s)
    s = re.sub(r"\n?```\s*$", "", s).strip()
    a, b = s.find("{"), s.rfind("}")
    return s[a:b + 1] if (a != -1 and b > a) else s


def parse_verdict(raw: str) -> Dict:
    try:
        data = json.loads(_strip_fences(raw)) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


class MockJudgeBackend:
    """Returns a fixed verdict JSON (zero cost). Records the prompt it saw."""

    def __init__(self, verdict: Any = None):
        self._v = verdict if verdict is not None else {"verdict": "REVISE",
            "scores": {"substance": 0.7, "salience": 0.6, "craft": 0.8, "overall": 0.7},
            "strengths": [], "issues": [], "omissions": []}
        self.last_prompt: Optional[str] = None

    def complete(self, system: str, user: str, **kw) -> str:
        self.last_prompt = user
        return self._v if isinstance(self._v, str) else json.dumps(self._v)


class JudgeBackend:
    """Live OpenRouter judge (OpenAI-compatible). PAID. Needs OPENROUTER_API_KEY."""

    BASE_URL = "https://openrouter.ai/api/v1"

    def __init__(self, model: str = "google/gemini-3.1-flash-lite-preview",
                 api_key: Optional[str] = None, timeout: int = 240):
        self.model = model
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY", "")
        if not self.api_key:
            raise RuntimeError("OPENROUTER_API_KEY not set (load .env)")
        self.timeout = timeout
        self._client = None

    def complete(self, system: str, user: str, max_tokens: int = 1500,
                 temperature: float = 0.2) -> str:
        if self._client is None:
            from openai import OpenAI  # type: ignore
            self._client = OpenAI(api_key=self.api_key, base_url=self.BASE_URL)
        r = self._client.chat.completions.create(
            model=self.model, temperature=temperature, max_tokens=max_tokens, timeout=self.timeout,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            extra_headers={"HTTP-Referer": "https://github.com/jj55222/FlameOn", "X-Title": "FlameOn Judge"})
        return r.choices[0].message.content or ""


def _blend_scores(llm: Dict, report: Dict) -> Dict:
    """Ground the LLM scores with the objective findings — craft can't score high
    with replays/unsourced beats; substance can't if procedural moments are
    absent. The deterministic layer has veto power downward."""
    scores = dict((llm.get("scores") or {}))
    craft = report["craft"]
    cov = report["coverage"]
    if craft["replay_count"] or not craft["all_sourced"] or craft["runtime_flag"] != "ok":
        penalty = 0.15 * (bool(craft["replay_count"]) + (not craft["all_sourced"]) + (craft["runtime_flag"] != "ok"))
        scores["craft"] = round(max(0.0, min(scores.get("craft", 0.5), 1.0 - penalty)), 2)
    if not cov["substance_types_present"]:
        scores["substance"] = round(min(scores.get("substance", 0.5), 0.4), 2)
    s = [scores.get(k) for k in ("substance", "salience", "craft") if isinstance(scores.get(k), (int, float))]
    scores["overall"] = round(sum(s) / len(s), 2) if s else scores.get("overall", 0.0)
    return scores


def judge_bundle(bundle: Dict, backend, paper_edit: Optional[Dict] = None,
                 pool: Optional[List[Dict]] = None, max_tokens: int = 1500) -> Dict:
    report = deterministic_report(bundle, paper_edit, pool)
    raw = backend.complete(system=_SYSTEM, user=build_prompt(bundle, report), max_tokens=max_tokens)
    llm = parse_verdict(raw)
    scores = _blend_scores(llm, report)
    return {
        "case_id": bundle.get("case_id"),
        "verdict": llm.get("verdict", "REVISE"),
        "scores": scores,
        "strengths": llm.get("strengths", []),
        "issues": llm.get("issues", []),
        "omissions": (llm.get("omissions", []) or []) + [o["type"] for o in report["coverage"]["omissions"]],
        "deterministic": report,
    }


def deterministic_enum_judgment(bundle: Dict, paper_edit: Optional[Dict] = None,
                                pool: Optional[List[Dict]] = None,
                                taste: Optional[Dict] = None) -> Dict:
    """Zero-cost enum critic used by ``--mock --enum-only``."""
    report = deterministic_report(bundle, paper_edit, pool)
    craft = report["craft"]
    reasons: List[Dict] = []
    verdict = "SHIP"
    if not craft["all_sourced"]:
        verdict = "REJECT"
        reasons.append({"code": "unsourced_beats", "note": "One or more beats have no evidence source.",
                        "source_refs": [f"beat:{b}" for b in craft["unsourced_beats"]]})
    elif taste and taste.get("verdict") in {"ABSTAIN", "REJECT"}:
        verdict = taste["verdict"]
        reasons.append({"code": "deterministic_taste_veto", "note": "The paper edit failed a craft veto.",
                        "source_refs": [f"rule:{r}" for r in taste.get("failed_rule_ids", [])]})
    elif craft["replay_count"] or craft["runtime_flag"] != "ok" or (taste and taste.get("verdict") == "REVISE"):
        verdict = "REVISE"
        if craft["replay_count"]:
            reasons.append({"code": "replayed_footage", "note": "The paper edit reuses an overlapping source window.",
                            "source_refs": [f"media:{x['media']}" for x in craft["replays"]]})
        if craft["runtime_flag"] != "ok":
            reasons.append({"code": "runtime_drift", "note": f"Runtime is {craft['runtime_flag']} target.",
                            "source_refs": ["paper_edit.runtime"]})
        if taste and taste.get("verdict") == "REVISE":
            reasons.append({"code": "taste_revision", "note": "Operator-authored finishing rules need revision.",
                            "source_refs": [f"rule:{r}" for r in taste.get("failed_rule_ids", [])]})
    if not reasons:
        reasons.append({"code": "assembly_supported", "note": "No deterministic craft breach was found.",
                        "source_refs": ["paper_edit.timeline"]})
    return {"case_id": bundle.get("case_id"), "verdict": verdict, "reasons": reasons,
            "strengths": [], "omissions": [o["type"] for o in report["coverage"]["omissions"]],
            "deterministic": report, "taste": taste}


def judge_bundle_enum(bundle: Dict, backend, paper_edit: Optional[Dict] = None,
                      pool: Optional[List[Dict]] = None, taste: Optional[Dict] = None,
                      max_tokens: int = 1500) -> Dict:
    report = deterministic_report(bundle, paper_edit, pool)
    payload = {"bundle": _bundle_digest(bundle), "deterministic": report, "taste": taste}
    raw = backend.complete(system=_ENUM_SYSTEM, user=json.dumps(payload, ensure_ascii=False),
                           max_tokens=max_tokens, temperature=0.0)
    llm = parse_verdict(raw)
    verdict = llm.get("verdict") if llm.get("verdict") in {"SHIP", "REVISE", "ABSTAIN", "REJECT"} else "ABSTAIN"
    return {"case_id": bundle.get("case_id"), "verdict": verdict,
            "reasons": llm.get("reasons") or [{"code": "critic_unparseable",
                "note": "The critic did not return a valid enum verdict.", "source_refs": ["critic.output"]}],
            "strengths": llm.get("strengths") or [], "omissions": llm.get("omissions") or [],
            "deterministic": report, "taste": taste}


def compare_bundles(bundle_a: Dict, bundle_b: Dict, backend,
                    pe_a: Optional[Dict] = None, pe_b: Optional[Dict] = None,
                    pool: Optional[List[Dict]] = None) -> Dict:
    """Pairwise preference — the reward signal. Judges both, then asks which is
    better and why (LLMs rank more reliably than they score absolutely)."""
    ra = deterministic_report(bundle_a, pe_a, pool)
    rb = deterministic_report(bundle_b, pe_b, pool)
    sys_p = (_SYSTEM.split("Return ONLY JSON")[0]
             + 'Compare bundle A and B. Return ONLY JSON: '
               '{"winner":"A|B|tie","why":"..","margin":"slim|clear|decisive"}')
    user = ("BUNDLE A:\n" + json.dumps(_bundle_digest(bundle_a), ensure_ascii=False)
            + "\nA REPORT:\n" + json.dumps(ra, ensure_ascii=False)
            + "\n\nBUNDLE B:\n" + json.dumps(_bundle_digest(bundle_b), ensure_ascii=False)
            + "\nB REPORT:\n" + json.dumps(rb, ensure_ascii=False)
            + "\n\nWhich assembly is better? JSON only.")
    res = parse_verdict(backend.complete(system=sys_p, user=user))
    return {"winner": res.get("winner", "tie"), "why": res.get("why", ""),
            "margin": res.get("margin", "slim"),
            "report_a": ra, "report_b": rb}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def gate_decision(v: Dict) -> Tuple[str, int]:
    """Turn a judged verdict into a release gate: ``(label, exit_code)``.

    The faithfulness floor is DETERMINISTIC and non-negotiable — unsourced footage
    fails hard regardless of what the LLM thinks (exit 3). Above that floor the
    LLM verdict decides: REWORK fails, SHIP passes clean, anything else is REVISE
    (emit, but flag for a human). Soft craft issues (replays / runtime drift)
    downgrade a SHIP to REVISE but don't block. Exit 0 = emit, 3 = hold.
    """
    craft = (v.get("deterministic") or {}).get("craft") or {}
    if not craft.get("all_sourced", True):
        return "REWORK (unsourced footage — faithfulness breach)", 3
    if v.get("verdict") in {"REWORK", "REJECT", "ABSTAIN"}:
        return "REWORK", 3
    soft = bool(craft.get("replay_count")) or craft.get("runtime_flag", "ok") != "ok"
    if v.get("verdict") == "SHIP" and not soft:
        return "SHIP", 0
    return "REVISE (emit, flag for review)", 0


def main(argv: Optional[List[str]] = None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description="GOAL_D — judge an edit bundle against its evidence")
    ap.add_argument("--bundle", required=True, type=Path, help="shaped blueprint json")
    ap.add_argument("--paper-edit", type=Path, default=None, help="rendered paper_edit (truer craft)")
    ap.add_argument("--pool", type=Path, default=None, help="candidate moments json (for omissions)")
    ap.add_argument("--model", default=None)
    ap.add_argument("--mock", action="store_true", help="deterministic report only, no LLM (zero cost)")
    ap.add_argument("--enum-only", action="store_true",
                    help="critic returns SHIP|REVISE|ABSTAIN|REJECT with cited reasons and no scores")
    ap.add_argument("--taste-report", type=Path, default=None,
                    help="VALIDATION_<case>.json from taste_gate.py")
    ap.add_argument("--gate", action="store_true",
                    help="release gate: exit 3 if the cut should be held (REWORK / unsourced), else 0")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args(argv)

    bundle = json.loads(args.bundle.read_text(encoding="utf-8"))
    paper_edit = json.loads(args.paper_edit.read_text(encoding="utf-8")) if args.paper_edit else None
    pool = json.loads(args.pool.read_text(encoding="utf-8")) if args.pool else None
    taste = json.loads(args.taste_report.read_text(encoding="utf-8")) if args.taste_report else None

    if args.mock:
        backend = MockJudgeBackend()
        print("[judge] MOCK (deterministic only)")
    else:
        try:
            from dotenv import load_dotenv  # type: ignore
            load_dotenv(Path(__file__).resolve().parent.parent / ".env")
            load_dotenv()
        except Exception:
            pass
        backend = JudgeBackend(args.model) if args.model else JudgeBackend()
        print(f"[judge] live: {backend.model} (paid)")

    if args.enum_only and args.mock:
        v = deterministic_enum_judgment(bundle, paper_edit=paper_edit, pool=pool, taste=taste)
    elif args.enum_only:
        v = judge_bundle_enum(bundle, backend, paper_edit=paper_edit, pool=pool, taste=taste)
    else:
        v = judge_bundle(bundle, backend, paper_edit=paper_edit, pool=pool)
    if args.enum_only:
        print(f"\n  VERDICT: {v['verdict']}  (enum-only; no synthetic score)")
        for reason in v.get("reasons") or []:
            print(f"    - {reason.get('code')}: {reason.get('note')}")
    else:
        sc = v["scores"]
        print(f"\n  VERDICT: {v['verdict']}   substance={sc.get('substance')} "
              f"salience={sc.get('salience')} craft={sc.get('craft')} overall={sc.get('overall')}")
    cr = v["deterministic"]["craft"]
    print(f"  craft: {cr['replay_count']} replay(s), sourced={cr['all_sourced']}, "
          f"runtime {cr['runtime_flag']} ({cr['runtime_ratio']})")
    cv = v["deterministic"]["coverage"]
    sm = cv["source_mix"]
    print(f"  coverage: source_mix vision={sm['vision']} broll={sm['broll']} "
          f"document={sm.get('document', 0)} transcript={sm['transcript']} "
          f"narration={sm.get('narration', 0)} | use_of_force={cv['has_use_of_force']} "
          f"procedural={cv['has_procedural']}")
    if v["issues"]:
        print("  issues:")
        for i in v["issues"][:8]:
            print(f"    [{i.get('severity','?'):4s}] {i.get('dimension','?')}: {i.get('note','')[:90]}")
    if v["omissions"]:
        print("  omissions:", ", ".join(str(o) for o in v["omissions"][:6]))
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(v, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  -> {args.out}")
    if args.gate:
        label, code = gate_decision(v)
        print(f"\n  GATE: {'PASS' if code == 0 else 'HOLD'} — {label}")
        return code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
