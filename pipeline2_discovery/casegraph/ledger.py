"""EVAL3 — Cost/runtime ledger for CaseGraph experiment runs.

Standardizes the JSONL row that future live experiments append to
`autoresearch/.runs/experiments.jsonl`. Each entry captures:

- experiment_id, timestamp, case_id
- API call counts per provider (courtlistener / muckrock /
  documentcloud / youtube / brave / firecrawl / llm)
- runtime in wall-clock seconds
- source-record yield, verified-artifact yield, media yield, document
  yield (read off the resulting CasePacket)
- cost estimate in USD, computed from api_calls + per-provider rates
- final verdict + scores (when a packet was scored)
- arbitrary notes

This module is deterministic and pure: building a ledger entry from a
CasePacket calls `score_case_packet` (read-only) and reads
`packet.verified_artifacts` / `packet.sources` — it never makes live
calls itself. It is required scaffolding before the LIVE1/LIVE2/LIVE3
experiments can run safely.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from .models import CasePacket, VerifiedArtifact
from .scoring import MEDIA_ARTIFACT_TYPES, MEDIA_FORMATS, score_case_packet


# Per-call cost rates in USD. Free providers stay at 0.0; callers can
# override any rate via `per_call_overrides` (e.g. to model an LLM cost
# per request). Keep this list aligned with the api_calls keys
# experiments emit.
COST_PER_CALL_USD: Dict[str, float] = {
    "brave": 0.005,
    "firecrawl": 0.001,
    "llm": 0.0,
    "courtlistener": 0.0,
    "muckrock": 0.0,
    "documentcloud": 0.0,
    "youtube": 0.0,
}

# Canonical zero-state api_calls dict. Callers should always start from
# this and increment, so the JSONL row keys are stable across runs.
DEFAULT_API_CALLS: Dict[str, int] = {provider: 0 for provider in COST_PER_CALL_USD}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _is_media_artifact(artifact: VerifiedArtifact) -> bool:
    return artifact.artifact_type in MEDIA_ARTIFACT_TYPES or artifact.format in MEDIA_FORMATS


@dataclass
class RunLedgerEntry:
    """One run = one JSONL row in autoresearch/.runs/experiments.jsonl.

    Fields are aligned with the existing experiment-row shape used by
    SYNC1 / DOC1 / W*-lite / H*-lite / F*-lite / EVAL* rows so future
    live experiments produce uniform output.
    """

    experiment_id: str
    timestamp: str = field(default_factory=_utc_now_iso)
    case_id: Optional[str] = None
    api_calls: Dict[str, int] = field(default_factory=lambda: dict(DEFAULT_API_CALLS))
    wallclock_seconds: float = 0.0
    source_record_count: int = 0
    verified_artifact_count: int = 0
    media_artifact_count: int = 0
    document_artifact_count: int = 0
    estimated_cost_usd: float = 0.0
    verdict: Optional[str] = None
    research_completeness_score: Optional[float] = None
    production_actionability_score: Optional[float] = None
    actionability_score: Optional[float] = None
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        # Preserve the canonical key order even when api_calls was
        # constructed from a sparse dict — guarantees stable JSON
        # output across runs.
        canonical_calls = {provider: 0 for provider in COST_PER_CALL_USD}
        for provider, count in (self.api_calls or {}).items():
            if provider not in canonical_calls:
                canonical_calls[provider] = count
            else:
                canonical_calls[provider] = count
        data["api_calls"] = canonical_calls
        return data

    def to_jsonl_row(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ": "), sort_keys=False)


def estimate_cost(
    api_calls: Dict[str, int],
    per_call_overrides: Optional[Dict[str, float]] = None,
) -> float:
    """Estimate USD cost from api_call counts using per-provider rates.

    `per_call_overrides` lets callers model variable LLM costs without
    mutating the module-level table.
    """
    rates = {**COST_PER_CALL_USD, **(per_call_overrides or {})}
    total = 0.0
    for provider, count in (api_calls or {}).items():
        rate = rates.get(provider, 0.0)
        total += count * rate
    return round(total, 4)


def normalize_api_calls(api_calls: Optional[Dict[str, int]]) -> Dict[str, int]:
    """Merge the supplied api_calls onto DEFAULT_API_CALLS so every
    canonical key is present. Unknown providers are preserved with
    their supplied count."""
    canonical = dict(DEFAULT_API_CALLS)
    for provider, count in (api_calls or {}).items():
        if provider in canonical:
            canonical[provider] = int(count)
        else:
            canonical[provider] = int(count)
    return canonical


def build_run_ledger_entry(
    *,
    experiment_id: str,
    packet: Optional[CasePacket] = None,
    case_id: Optional[str] = None,
    api_calls: Optional[Dict[str, int]] = None,
    wallclock_seconds: float = 0.0,
    estimated_cost_usd: Optional[float] = None,
    cost_overrides: Optional[Dict[str, float]] = None,
    notes: Optional[List[str]] = None,
    timestamp: Optional[str] = None,
) -> RunLedgerEntry:
    """Construct a `RunLedgerEntry` for a single experiment run.

    When `packet` is provided, source/artifact counts and scoring are
    derived from the packet (via the pure `score_case_packet`). When
    `packet` is None, the entry still carries api_calls / wallclock /
    cost so non-packet runs (e.g. preflight smokes) can be logged.

    `estimated_cost_usd` defaults to `estimate_cost(api_calls, cost_overrides)`
    when not provided, so callers don't have to compute it themselves.
    """

    normalized_calls = normalize_api_calls(api_calls)
    cost = (
        estimate_cost(normalized_calls, cost_overrides)
        if estimated_cost_usd is None
        else round(float(estimated_cost_usd), 4)
    )

    entry_kwargs: Dict[str, Any] = {
        "experiment_id": experiment_id,
        "case_id": case_id or (packet.case_id if packet is not None else None),
        "api_calls": normalized_calls,
        "wallclock_seconds": round(float(wallclock_seconds), 4),
        "estimated_cost_usd": cost,
        "notes": list(notes or []),
    }
    if timestamp is not None:
        entry_kwargs["timestamp"] = timestamp

    if packet is not None:
        media = [a for a in packet.verified_artifacts if _is_media_artifact(a)]
        documents = [a for a in packet.verified_artifacts if not _is_media_artifact(a)]
        entry_kwargs.update(
            {
                "source_record_count": len(packet.sources),
                "verified_artifact_count": len(packet.verified_artifacts),
                "media_artifact_count": len(media),
                "document_artifact_count": len(documents),
            }
        )
        result = score_case_packet(packet)
        entry_kwargs.update(
            {
                "verdict": result.verdict,
                "research_completeness_score": result.research_completeness_score,
                "production_actionability_score": result.production_actionability_score,
                "actionability_score": result.actionability_score,
            }
        )

    return RunLedgerEntry(**entry_kwargs)


def aggregate_ledger(entries: Iterable[RunLedgerEntry]) -> Dict[str, Any]:
    """Sum a sequence of ledger entries into a batch summary.

    Useful for end-of-batch reports and pre-flight budget checks before
    LIVE experiments fire.
    """

    entries_list = list(entries)
    if not entries_list:
        return {
            "run_count": 0,
            "api_calls_total": dict(DEFAULT_API_CALLS),
            "wallclock_seconds_total": 0.0,
            "estimated_cost_usd_total": 0.0,
            "source_record_total": 0,
            "verified_artifact_total": 0,
            "media_artifact_total": 0,
            "document_artifact_total": 0,
            "verdict_counts": {"PRODUCE": 0, "HOLD": 0, "SKIP": 0, "unknown": 0},
            "experiment_ids": [],
        }

    api_calls_total = dict(DEFAULT_API_CALLS)
    wallclock_total = 0.0
    cost_total = 0.0
    sources_total = 0
    verified_total = 0
    media_total = 0
    document_total = 0
    verdict_counts: Dict[str, int] = {"PRODUCE": 0, "HOLD": 0, "SKIP": 0, "unknown": 0}
    experiment_ids: List[str] = []

    for entry in entries_list:
        for provider, count in (entry.api_calls or {}).items():
            api_calls_total[provider] = api_calls_total.get(provider, 0) + int(count)
        wallclock_total += float(entry.wallclock_seconds or 0.0)
        cost_total += float(entry.estimated_cost_usd or 0.0)
        sources_total += int(entry.source_record_count or 0)
        verified_total += int(entry.verified_artifact_count or 0)
        media_total += int(entry.media_artifact_count or 0)
        document_total += int(entry.document_artifact_count or 0)
        verdict = entry.verdict or "unknown"
        verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1
        if entry.experiment_id and entry.experiment_id not in experiment_ids:
            experiment_ids.append(entry.experiment_id)

    return {
        "run_count": len(entries_list),
        "api_calls_total": api_calls_total,
        "wallclock_seconds_total": round(wallclock_total, 4),
        "estimated_cost_usd_total": round(cost_total, 4),
        "source_record_total": sources_total,
        "verified_artifact_total": verified_total,
        "media_artifact_total": media_total,
        "document_artifact_total": document_total,
        "verdict_counts": verdict_counts,
        "experiment_ids": experiment_ids,
    }


def append_ledger_entry(entry: RunLedgerEntry, path: str) -> None:
    """Append `entry.to_jsonl_row()` to a JSONL file. Creates the file
    if missing. Newline-terminated. Never overwrites existing lines."""
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(entry.to_jsonl_row())
        handle.write("\n")
