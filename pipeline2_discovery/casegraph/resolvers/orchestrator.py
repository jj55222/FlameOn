"""RESOLVE1 — Metadata-only resolver orchestrator.

Runs the registered metadata-only resolvers against a CasePacket (or
a sequence of SourceRecords) and aggregates the results. Each
underlying resolver (``resolve_muckrock_released_files``,
``resolve_documentcloud_files``, ``resolve_courtlistener_documents``)
already enforces:

- never download files
- never scrape pages
- never follow login / auth / token / private URLs
- never follow PACER URLs (CourtListener resolver)
- never follow non-public DocumentCloud access tiers
- claim text without a concrete URL never graduates into a
  VerifiedArtifact

The orchestrator adds:

- a single deterministic entrypoint (``run_metadata_only_resolvers``)
- a ``ResolverOrchestrationResult`` dataclass with per-resolver +
  aggregate counters
- a final cross-resolver URL dedupe pass (each resolver already
  dedupes against ``packet.verified_artifacts``, but if multiple
  resolvers see the same URL via different metadata shapes, we
  collapse to a single VerifiedArtifact)
- an ``allow_list`` argument so callers can scope the run (e.g. just
  MuckRock + DocumentCloud, skipping CourtListener)
- preservation of risk flags and next actions

The orchestrator never makes a network call. It is intended for use
after a connector smoke (LIVE1 / LIVE2 / LIVE3 / LIVE4 / LIVE5) when
the caller wants to know whether the returned SourceRecords carry
concrete public artifact URLs in their metadata.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence

from ..models import CasePacket, SourceRecord, VerifiedArtifact
from .courtlistener_documents import (
    CourtListenerDocumentResolution,
    resolve_courtlistener_documents,
)
from .documentcloud_files import (
    DocumentCloudFileResolution,
    resolve_documentcloud_files,
)
from .muckrock_files import MuckRockFileResolution, resolve_muckrock_released_files
from .youtube_media import YouTubeMediaResolution, resolve_youtube_media_sources


RESOLVER_NAMES = ("muckrock", "documentcloud", "courtlistener", "youtube")


@dataclass
class ResolverOrchestrationResult:
    """Aggregate of every resolver run by :func:`run_metadata_only_resolvers`.

    ``per_resolver`` carries the raw per-resolver result objects so
    callers can inspect resolver-specific risk flags / next_actions.
    The aggregate counters are pre-computed so callers don't have to
    iterate ``per_resolver`` to get totals.
    """

    verified_artifacts: List[VerifiedArtifact] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    next_actions: List[str] = field(default_factory=list)
    inspected_source_ids: List[str] = field(default_factory=list)
    resolvers_run: List[str] = field(default_factory=list)
    per_resolver: Dict[str, Any] = field(default_factory=dict)

    @property
    def verified_artifact_count(self) -> int:
        return len(self.verified_artifacts)

    @property
    def media_artifact_count(self) -> int:
        return sum(
            1
            for a in self.verified_artifacts
            if a.format in {"video", "audio"}
            or a.artifact_type
            in {
                "bodycam",
                "interrogation",
                "court_video",
                "dispatch_911",
                "surveillance_video",
                "dash_cam",
                "other_video",
                "audio",
            }
        )

    @property
    def document_artifact_count(self) -> int:
        return self.verified_artifact_count - self.media_artifact_count

    def to_diagnostics(self) -> Dict[str, Any]:
        """Stable JSON-serializable summary for the experiment ledger."""
        return {
            "resolvers_run": list(self.resolvers_run),
            "verified_artifact_count": self.verified_artifact_count,
            "media_artifact_count": self.media_artifact_count,
            "document_artifact_count": self.document_artifact_count,
            "verified_artifact_urls": [a.artifact_url for a in self.verified_artifacts],
            "risk_flags": list(self.risk_flags),
            "next_actions": list(self.next_actions),
            "inspected_source_ids": list(self.inspected_source_ids),
        }


def _append_unique(target: List[str], values: Iterable[str]) -> None:
    seen = set(target)
    for value in values:
        if value and value not in seen:
            target.append(value)
            seen.add(value)


def _validate_allow_list(allow_list: Optional[Sequence[str]]) -> List[str]:
    if allow_list is None:
        return list(RESOLVER_NAMES)
    cleaned: List[str] = []
    for name in allow_list:
        if name not in RESOLVER_NAMES:
            raise ValueError(
                f"unknown resolver {name!r}; allowed names: {sorted(RESOLVER_NAMES)}"
            )
        if name not in cleaned:
            cleaned.append(name)
    return cleaned


def run_metadata_only_resolvers(
    packet_or_sources,
    *,
    allow_list: Optional[Sequence[str]] = None,
) -> ResolverOrchestrationResult:
    """Run every enabled metadata-only resolver against a CasePacket
    (or a sequence of SourceRecords) and aggregate the result.

    Each resolver is called independently — the allow-list filters
    which ones run. When ``packet_or_sources`` is a CasePacket, the
    underlying resolvers append their VerifiedArtifacts directly onto
    the packet (and dedupe against existing entries by URL). When it
    is a bare sequence, the orchestrator collects the artifacts in its
    own result without mutating any packet.

    The orchestrator never makes a network call, never downloads, never
    scrapes — it is a coordination layer over deterministic,
    metadata-only resolvers.
    """

    enabled = _validate_allow_list(allow_list)
    is_packet = isinstance(packet_or_sources, CasePacket)
    sources = list(
        packet_or_sources.sources if is_packet else packet_or_sources
    )

    result = ResolverOrchestrationResult()

    # Track URLs across resolvers so a final cross-resolver dedupe pass
    # collapses any duplicates (each resolver already dedupes against
    # the packet, but a source visible to two resolvers via different
    # metadata fields could otherwise produce two artifacts).
    seen_urls = set()
    if is_packet:
        seen_urls.update(a.artifact_url for a in packet_or_sources.verified_artifacts)

    if "muckrock" in enabled:
        muckrock_result = resolve_muckrock_released_files(packet_or_sources if is_packet else sources)
        result.per_resolver["muckrock"] = muckrock_result
        result.resolvers_run.append("muckrock")
        _absorb(result, muckrock_result, seen_urls=seen_urls, packet=packet_or_sources if is_packet else None)

    if "documentcloud" in enabled:
        documentcloud_result = resolve_documentcloud_files(packet_or_sources if is_packet else sources)
        result.per_resolver["documentcloud"] = documentcloud_result
        result.resolvers_run.append("documentcloud")
        _absorb(result, documentcloud_result, seen_urls=seen_urls, packet=packet_or_sources if is_packet else None)

    if "courtlistener" in enabled:
        courtlistener_result = resolve_courtlistener_documents(packet_or_sources if is_packet else sources)
        result.per_resolver["courtlistener"] = courtlistener_result
        result.resolvers_run.append("courtlistener")
        _absorb(result, courtlistener_result, seen_urls=seen_urls, packet=packet_or_sources if is_packet else None)

    if "youtube" in enabled:
        youtube_result = resolve_youtube_media_sources(packet_or_sources if is_packet else sources)
        result.per_resolver["youtube"] = youtube_result
        result.resolvers_run.append("youtube")
        _absorb(result, youtube_result, seen_urls=seen_urls, packet=packet_or_sources if is_packet else None)

    inspected: List[str] = []
    for resolver_result in result.per_resolver.values():
        for sid in getattr(resolver_result, "inspected_source_ids", []):
            if sid and sid not in inspected:
                inspected.append(sid)
    result.inspected_source_ids = inspected

    return result


def _absorb(
    result: ResolverOrchestrationResult,
    resolver_result,
    *,
    seen_urls: set,
    packet: Optional[CasePacket],
) -> None:
    """Merge a per-resolver result into the orchestration aggregate.

    Skips any artifact whose URL we've already accepted (cross-resolver
    dedupe). When ``packet`` is provided and the artifact was newly
    appended by the resolver but is now considered a duplicate by the
    orchestrator, we pop the duplicate off the packet too — keeping
    ``packet.verified_artifacts`` aligned with the orchestrator's view.
    """
    new_artifacts: List[VerifiedArtifact] = []
    for artifact in getattr(resolver_result, "verified_artifacts", []):
        if artifact.artifact_url in seen_urls:
            if packet is not None:
                # Already-seen URL: ensure the packet doesn't carry a
                # duplicate that this specific resolver added.
                while packet.verified_artifacts and packet.verified_artifacts[-1] is artifact:
                    packet.verified_artifacts.pop()
            continue
        seen_urls.add(artifact.artifact_url)
        new_artifacts.append(artifact)
    result.verified_artifacts.extend(new_artifacts)
    _append_unique(result.risk_flags, getattr(resolver_result, "risk_flags", []))
    _append_unique(result.next_actions, getattr(resolver_result, "next_actions", []))


__all__ = [
    "RESOLVER_NAMES",
    "ResolverOrchestrationResult",
    "run_metadata_only_resolvers",
]
