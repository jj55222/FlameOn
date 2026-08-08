"""Pre-shaping thesis-survivability gate for P6.

The gate does not decide whether a case is worth producing. It answers a much
narrower craft question: can the upstream-selected thesis and treatment be cut
honestly from the evidence already bound into the deterministic blueprint?

Output is enum-only and cited. No synthetic quality score is emitted.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

VERDICTS = {"PASS", "REVISE_THESIS", "MISSING_EVIDENCE", "SUGGEST_UPSTREAM_REROUTE", "ABSTAIN"}


def _shape(contract: Dict) -> Dict:
    payload = contract.get("contract") if isinstance(contract.get("contract"), dict) else contract
    return payload.get("case_shape_card") if isinstance(payload.get("case_shape_card"), dict) else payload


def _route(contract: Dict, treatment: Dict) -> Optional[str]:
    s = _shape(contract)
    # New contracts carry chosen_route/route_id. Older bakeoff contracts only
    # have a broad production_route (e.g. bodycam_encounter), so the explicit
    # reference treatment is more specific and must win over that legacy label.
    return (s.get("chosen_route") or s.get("route_id") or treatment.get("route_id")
            or s.get("production_route"))


def _thesis(contract: Dict) -> Optional[str]:
    s = _shape(contract)
    return s.get("candidate_thesis") or s.get("chosen_thesis") or contract.get("chosen_thesis")


def _moments(contract: Dict, explicit: Optional[Any]) -> List[Dict]:
    if isinstance(explicit, dict):
        explicit = explicit.get("moments") or explicit.get("cards") or explicit.get("moment_slate")
    if isinstance(explicit, list):
        return [m for m in explicit if isinstance(m, dict)]
    payload = contract.get("contract") if isinstance(contract.get("contract"), dict) else contract
    vals = payload.get("moment_slate") or payload.get("moments") or []
    return [m for m in vals if isinstance(m, dict)]


def _reason(code: str, note: str, refs: List[str]) -> Dict:
    return {"code": code, "note": note, "source_refs": refs}


def _contradiction_strength(contract: Dict, moments: List[Dict]) -> Tuple[str, List[str]]:
    """strong|weak|unknown, with source refs.

    A boolean ``contradiction_present`` is not enough. Strong means the record
    carries two mutually exclusive, verbatim claims or an equivalent explicit
    machine check. This is intentionally strict so topical overlap cannot be
    promoted into a contradiction by persuasive narration.
    """
    payload = contract.get("contract") if isinstance(contract.get("contract"), dict) else contract
    shape = _shape(contract)
    explicit = (payload.get("contradiction_strength") or shape.get("contradiction_strength") or "").lower()
    refs: List[str] = []
    if explicit in {"strong", "weak"}:
        return explicit, ["contract.contradiction_strength"]
    candidates = [m for m in moments if str(m.get("moment_type") or "").lower() == "contradiction"]
    for m in candidates:
        mid = str(m.get("moment_id") or "moment")
        checks = m.get("machine_checks") or m.get("checks") or {}
        if isinstance(checks, list):
            checks = {str(x): True for x in checks}
        claims = m.get("claims") or m.get("verbatim_claims") or []
        strong = (checks.get("mutual_exclusivity_asserted_with_evidence") is True
                  and checks.get("verbatim_quotes_present", bool(claims)) is True)
        strong = strong or (m.get("mutually_exclusive") is True and len(claims) >= 2)
        if strong:
            return "strong", [f"moment:{mid}"]
    blob = " ".join(str(m.get(k) or "") for m in candidates for k in ("summary", "description", "notes")).lower()
    weak_cues = ("topical" in blob or "not mutually exclusive" in blob
                 or "same subject" in blob or "handgun-vs-handgun" in blob)
    if candidates or weak_cues or payload.get("contradiction_present") or shape.get("contradiction_present"):
        return "weak", [f"moment:{m.get('moment_id', 'unknown')}" for m in candidates] or ["contract.contradiction_present"]
    return "unknown", []


def evaluate_thesis(blueprint: Dict, contract: Dict, *, treatment: Optional[Dict] = None,
                    moments: Optional[Any] = None) -> Dict:
    treatment = treatment or blueprint.get("reference_treatment") or {}
    route = _route(contract, treatment)
    thesis = _thesis(contract)
    pool = _moments(contract, moments)
    reasons: List[Dict] = []
    warnings: List[Dict] = []
    verdict = "PASS"

    if not thesis:
        verdict = "ABSTAIN"
        reasons.append(_reason("thesis_missing", "The persisted contract has no selected thesis.",
                               ["contract.case_shape_card.chosen_thesis"]))
    if not treatment:
        verdict = "ABSTAIN"
        reasons.append(_reason("treatment_missing", "No reference treatment reached the blueprint.",
                               ["blueprint.reference_treatment"]))

    validation = blueprint.get("treatment_validation") or {}
    missing = validation.get("missing_required_slots") or []
    if missing:
        verdict = "MISSING_EVIDENCE"
        labels = ", ".join(f"{x.get('act_id')}:{x.get('slot_id')}" for x in missing[:8])
        reasons.append(_reason("required_slots_unbound",
                               f"Required treatment evidence is not bound: {labels}.",
                               [f"treatment_slot:{x.get('slot_id')}" for x in missing]))

    canonical_route = str(route or "").lower()
    if canonical_route in {"accountability_prosecution", "interrogation_contradiction"}:
        inc = blueprint.get("incident") or {}
        if inc.get("disposition") and not inc.get("disposition_source"):
            verdict = "MISSING_EVIDENCE"
            reasons.append(_reason(
                "disposition_uncited",
                "The direction relies on an accountability outcome, but its disposition has no source-of-record citation.",
                ["blueprint.incident.disposition_source"],
            ))

    strength, refs = _contradiction_strength(contract, pool)
    if canonical_route == "interrogation_contradiction":
        if strength == "weak":
            verdict = "REVISE_THESIS"
            reasons.append(_reason(
                "contradiction_not_mutually_exclusive",
                "The available statements overlap topically but are not proven mutually exclusive; the promised contradiction does not survive the evidence.",
                refs or ["moment_cards.contradiction"],
            ))
        elif strength == "unknown":
            verdict = "ABSTAIN"
            reasons.append(_reason(
                "contradiction_unverified",
                "The route promises a contradiction, but the contract does not carry two verbatim, mutually exclusive claims.",
                ["moment_cards.contradiction.machine_checks"],
            ))
    elif strength == "weak":
        warnings.append(_reason(
            "thin_secondary_contradiction",
            "A contradiction-like beat is weak and must not be presented as the story payoff.", refs))

    if verdict == "PASS" and not reasons:
        reasons.append(_reason("thesis_supported",
                               "The treatment's required evidence slots are bound and no direction-specific faithfulness rail failed.",
                               ["blueprint.treatment_validation"]))
    return {
        "case_id": blueprint.get("case_id"), "verdict": verdict,
        "route": route, "thesis": thesis,
        "treatment_id": treatment.get("treatment_id") or treatment.get("template_id"),
        "reasons": reasons, "warnings": warnings,
        "evidence": {"moment_count": len(pool), "required_slots_missing": len(missing)},
    }


_SYSTEM = (
    "You are a pre-edit thesis critic. Decide only whether the SELECTED direction can be cut "
    "honestly from the cited evidence; never judge whether the case is worth producing and never "
    "choose a new route. Return JSON only: {\"verdict\":\"PASS|REVISE_THESIS|MISSING_EVIDENCE|"
    "SUGGEST_UPSTREAM_REROUTE|ABSTAIN\",\"reasons\":[{\"code\":\"...\",\"note\":\"...\","
    "\"source_refs\":[\"...\"]}]}. A contradiction requires two mutually exclusive verbatim claims."
)


def _live_critic(report: Dict, blueprint: Dict, contract: Dict, model: str) -> Dict:
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass
    p4 = Path(__file__).resolve().parent.parent / "pipeline4_scoring"
    sys.path.insert(0, str(p4))
    from llm_backends import build_backend, clean_llm_output  # type: ignore
    backend = build_backend(model)
    payload = {"deterministic": report, "contract": _shape(contract),
               "acts": blueprint.get("acts"), "treatment_validation": blueprint.get("treatment_validation")}
    raw = backend.complete(system=_SYSTEM, user=json.dumps(payload, ensure_ascii=False),
                           max_tokens=1400, temperature=0.0)
    try:
        out = json.loads(clean_llm_output(raw))
    except Exception:
        return {"verdict": "ABSTAIN", "reasons": [_reason(
            "critic_unparseable", "The thesis critic did not return valid JSON.", ["critic.output"])]}
    if out.get("verdict") not in VERDICTS:
        out["verdict"] = "ABSTAIN"
    return out


def render_markdown(report: Dict) -> str:
    lines = [f"# Thesis Validation — {report.get('case_id')}", "",
             f"**Verdict:** {report.get('verdict')}",
             f"**Route:** {report.get('route') or '—'}",
             f"**Treatment:** {report.get('treatment_id') or '—'}", "", "## Reasons"]
    for r in report.get("reasons") or []:
        refs = ", ".join(r.get("source_refs") or [])
        lines.append(f"- **{r.get('code')}** — {r.get('note')}  `{refs}`")
    if report.get("warnings"):
        lines += ["", "## Warnings"]
        for r in report["warnings"]:
            lines.append(f"- **{r.get('code')}** — {r.get('note')}")
    return "\n".join(lines) + "\n"


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="P6 pre-shaping thesis-survivability gate")
    ap.add_argument("--blueprint", required=True, type=Path)
    ap.add_argument("--contract", required=True, type=Path)
    ap.add_argument("--reference-treatment", type=Path, default=None)
    ap.add_argument("--moments", type=Path, default=None)
    ap.add_argument("--model", default=None, help="optional one-call live critic; deterministic when omitted")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--gate", action="store_true")
    args = ap.parse_args(argv)

    bp = json.loads(args.blueprint.read_text(encoding="utf-8"))
    contract = json.loads(args.contract.read_text(encoding="utf-8"))
    treatment = (json.loads(args.reference_treatment.read_text(encoding="utf-8"))
                 if args.reference_treatment else bp.get("reference_treatment"))
    moments = json.loads(args.moments.read_text(encoding="utf-8")) if args.moments else None
    report = evaluate_thesis(bp, contract, treatment=treatment, moments=moments)
    if args.model and report["verdict"] == "PASS":
        live = _live_critic(report, bp, contract, args.model)
        report["critic"] = live
        if live.get("verdict") != "PASS":
            report["verdict"] = live.get("verdict", "ABSTAIN")
            report["reasons"] += live.get("reasons") or []

    args.out.mkdir(parents=True, exist_ok=True)
    cid = bp.get("case_id", "case")
    jp = args.out / f"{cid}_thesis_validation.json"
    mp = args.out / f"THESIS_VALIDATION_{cid}.md"
    jp.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    mp.write_text(render_markdown(report), encoding="utf-8")
    print(f"[thesis-gate] {report['verdict']} -> {jp} + {mp}")
    return 3 if args.gate and report["verdict"] != "PASS" else 0


if __name__ == "__main__":
    raise SystemExit(main())
