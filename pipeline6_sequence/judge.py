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
_PHASE_ORDER = ["pre_incident", "incident", "aftermath", "transport",
                "investigation", "outcome"]


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

    unsourced = [b.get("beat_id") for b in beats if not b.get("primary_asset")]
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
        "has_use_of_force": bool(tset & {"k9_deployment", "taser", "strike", "takedown",
                                         "weapon_drawn", "firearm_pointed", "use_of_force_other"}),
        "phases_covered": sorted(p for p in phases if p),
        "source_mix": {
            "vision": sum(1 for b in beats if b.get("source") == "vision"),
            "broll": sum(1 for b in beats if b.get("is_broll")),
            "transcript": sum(1 for b in beats if b.get("quote")),
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


def _bundle_digest(bundle: Dict) -> Dict:
    beats = []
    for b in bundle.get("beats", []):
        pa = b.get("primary_asset") or {}
        beats.append({
            "act": b.get("act_id"), "function": b.get("function"),
            "importance": b.get("importance") or b.get("_importance"),
            "source": "vision" if b.get("source") == "vision" else ("broll" if b.get("is_broll") else "transcript"),
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
    if v.get("verdict") == "REWORK":
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
    ap.add_argument("--gate", action="store_true",
                    help="release gate: exit 3 if the cut should be held (REWORK / unsourced), else 0")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args(argv)

    bundle = json.loads(args.bundle.read_text(encoding="utf-8"))
    paper_edit = json.loads(args.paper_edit.read_text(encoding="utf-8")) if args.paper_edit else None
    pool = json.loads(args.pool.read_text(encoding="utf-8")) if args.pool else None

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

    v = judge_bundle(bundle, backend, paper_edit=paper_edit, pool=pool)
    sc = v["scores"]
    print(f"\n  VERDICT: {v['verdict']}   substance={sc.get('substance')} "
          f"salience={sc.get('salience')} craft={sc.get('craft')} overall={sc.get('overall')}")
    cr = v["deterministic"]["craft"]
    print(f"  craft: {cr['replay_count']} replay(s), sourced={cr['all_sourced']}, "
          f"runtime {cr['runtime_flag']} ({cr['runtime_ratio']})")
    if v["issues"]:
        print("  issues:")
        for i in v["issues"][:8]:
            print(f"    [{i.get('severity','?'):4s}] {i.get('dimension','?')}: {i.get('note','')[:90]}")
    if v["omissions"]:
        print("  omissions:", ", ".join(str(o) for o in v["omissions"][:6]))
    if args.out:
        args.out.write_text(json.dumps(v, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  -> {args.out}")
    if args.gate:
        label, code = gate_decision(v)
        print(f"\n  GATE: {'PASS' if code == 0 else 'HOLD'} — {label}")
        return code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
