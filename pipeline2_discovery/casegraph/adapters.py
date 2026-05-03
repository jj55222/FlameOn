from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Iterable, List, Optional

from .models import LEGACY_EVIDENCE_TYPES, CasePacket
from .scoring import filter_stale_router_defaults


def _merge_unique(existing: List[str], additions: Optional[Iterable[str]]) -> List[str]:
    """Append items from ``additions`` to ``existing`` only when not
    already present, preserving the original order. Pure helper used
    by the handoff adapters to splice freshly computed advisory
    signals into the packet's stored arrays without disturbing the
    packet itself."""
    merged = list(existing)
    if not additions:
        return merged
    seen = set(merged)
    for item in additions:
        if item is None:
            continue
        if item not in seen:
            merged.append(item)
            seen.add(item)
    return merged


def _packet_dict(packet: CasePacket | Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(packet, dict):
        return packet
    if hasattr(packet, "to_dict"):
        return packet.to_dict()
    if is_dataclass(packet):
        return asdict(packet)
    raise TypeError(f"Unsupported packet type: {type(packet)!r}")


def _artifact_dicts(packet: CasePacket | Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(packet, dict):
        return list(packet.get("verified_artifacts", []))
    return [asdict(artifact) for artifact in packet.verified_artifacts]


def _case_title(packet_data: Dict[str, Any]) -> str:
    names = packet_data.get("case_identity", {}).get("defendant_names", []) or []
    return ", ".join(names) if names else packet_data.get("case_id", "")


def _p3_artifact_type(artifact_type: str) -> str:
    if artifact_type == "docket_docs":
        return "document"
    if artifact_type == "dash_cam":
        return "other_video"
    if artifact_type in {"bodycam", "interrogation", "court_video", "dispatch_911"}:
        return artifact_type
    return "document" if artifact_type == "document" else "other_video"


def _p3_authority(source_authority: str) -> str:
    if source_authority in {"official", "court", "foia", "news", "third_party"}:
        return source_authority
    return "third_party"


def export_p2_to_p3(packet: CasePacket | Dict[str, Any]) -> List[Dict[str, Any]]:
    packet_data = _packet_dict(packet)
    case_title = _case_title(packet_data)
    rows: List[Dict[str, Any]] = []
    for artifact in _artifact_dicts(packet):
        rows.append({
            "case_id": packet_data["case_id"],
            "case_title": case_title,
            "artifact_id": artifact["artifact_id"],
            "artifact_type": _p3_artifact_type(artifact["artifact_type"]),
            "source_url": artifact["artifact_url"],
            "source_authority": _p3_authority(artifact.get("source_authority", "unknown")),
            "downloadable": bool(artifact.get("downloadable")),
            "format": artifact.get("format", "unknown"),
            "duration_sec": artifact.get("duration_sec"),
            "requires_manual_download": bool(artifact.get("requires_manual_download", False)),
            "confidence": float(artifact.get("confidence", 0.0)),
            "matched_case_fields": list(artifact.get("matched_case_fields", [])),
        })
    return rows


def export_p2_to_p4(
    packet: CasePacket | Dict[str, Any],
    *,
    score_result: Optional[Any] = None,
) -> Dict[str, Any]:
    """Export a CasePacket as the P2->P4 context handoff.

    When ``score_result`` (an ``ActionabilityResult``) is supplied,
    advisory signals from the freshly computed
    ``score_result.risk_flags`` are merged into ``source_quality_notes``
    so downstream consumers see the same caution context the CLI's
    ``result`` section shows. Existing packet-level entries are kept
    first; fresh entries are appended only when not already present.

    When ``score_result`` is None, the export is byte-identical to
    pre-merge behavior — every existing direct call site continues to
    work unchanged.
    """
    packet_data = _packet_dict(packet)
    sources = packet_data.get("sources", [])
    summary_sources = [
        source["url"]
        for source in sources
        if set(source.get("source_roles", [])) & {"identity_source", "outcome_source"}
    ]
    identity = packet_data["case_identity"]
    source_quality_notes = list(packet_data.get("risk_flags", []))
    if score_result is not None:
        source_quality_notes = _merge_unique(
            source_quality_notes,
            getattr(score_result, "risk_flags", None),
        )
    source_quality_notes = filter_stale_router_defaults(
        source_quality_notes, packet_data
    )
    return {
        "case_id": packet_data["case_id"],
        "case_identity": identity,
        "outcome_status": identity.get("outcome_status", "unknown"),
        "charges": list(identity.get("charges", [])),
        "summary_sources": summary_sources,
        "artifact_refs": [artifact["artifact_id"] for artifact in packet_data.get("verified_artifacts", [])],
        "known_gaps": list(packet_data.get("input", {}).get("missing_fields", [])),
        "source_quality_notes": source_quality_notes,
    }


def export_p2_to_p5(
    packet: CasePacket | Dict[str, Any],
    *,
    score_result: Optional[Any] = None,
) -> Dict[str, Any]:
    """Export a CasePacket as the P2->P5 production-seed handoff.

    When ``score_result`` (an ``ActionabilityResult``) is supplied:
      - the export's ``verdict`` is sourced from
        ``score_result.verdict`` so the handoff reflects the fresh
        scorer output rather than the packet's stored router default
        (which stays "HOLD" for portal-replay packets even after the
        scorer reaches PRODUCE);
      - fresh advisory signals from ``score_result.risk_flags`` and
        ``score_result.next_actions`` are merged into the export's
        ``risk_flags`` and ``next_actions`` so downstream P5 consumers
        see the same caution context the CLI's ``result`` section
        shows.

    Existing packet-level risk_flags / next_actions entries are kept
    first; fresh entries are appended only when not already present.

    When ``score_result`` is None, the export is byte-identical to
    pre-PR behavior — every existing direct call site continues to
    work unchanged: ``verdict`` falls back to the packet's stored
    value, and ``risk_flags`` / ``next_actions`` mirror the packet.
    """
    packet_data = _packet_dict(packet)
    case_title = _case_title(packet_data)
    jurisdiction = packet_data["case_identity"].get("jurisdiction", {})
    location = ", ".join(
        part for part in [
            jurisdiction.get("city"),
            jurisdiction.get("county"),
            jurisdiction.get("state"),
        ]
        if part
    )
    case_summary = f"{case_title} is a CaseGraph packet"
    if location:
        case_summary += f" for {location}"
    case_summary += "."
    verdict = packet_data["verdict"]
    risk_flags = list(packet_data.get("risk_flags", []))
    next_actions = list(packet_data.get("next_actions", []))
    if score_result is not None:
        fresh_verdict = getattr(score_result, "verdict", None)
        if fresh_verdict:
            verdict = fresh_verdict
        risk_flags = _merge_unique(
            risk_flags,
            getattr(score_result, "risk_flags", None),
        )
        next_actions = _merge_unique(
            next_actions,
            getattr(score_result, "next_actions", None),
        )
    risk_flags = filter_stale_router_defaults(risk_flags, packet_data)
    return {
        "case_id": packet_data["case_id"],
        "verdict": verdict,
        "case_summary": case_summary,
        "artifact_table": list(packet_data.get("verified_artifacts", [])),
        "source_table": list(packet_data.get("sources", [])),
        "next_actions": next_actions,
        "risk_flags": risk_flags,
    }


def export_legacy_evaluate_result(packet: CasePacket | Dict[str, Any]) -> Dict[str, Any]:
    packet_data = _packet_dict(packet)
    evidence_found = {evidence_type: False for evidence_type in LEGACY_EVIDENCE_TYPES}
    for artifact in packet_data.get("verified_artifacts", []):
        artifact_type = artifact.get("artifact_type")
        if artifact_type in evidence_found:
            evidence_found[artifact_type] = True

    identity_confidence = packet_data.get("case_identity", {}).get("identity_confidence", "low")
    verified_artifacts = packet_data.get("verified_artifacts", [])
    downloadable_count = sum(1 for artifact in verified_artifacts if artifact.get("downloadable"))
    if identity_confidence == "high" and downloadable_count:
        confidence = "high"
    elif identity_confidence in {"medium", "high"} and verified_artifacts:
        confidence = "medium"
    else:
        confidence = "low"

    return {
        "evidence_found": evidence_found,
        "sources_found": list(packet_data.get("sources", [])),
        "confidence": confidence,
    }
