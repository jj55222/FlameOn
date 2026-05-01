"""MEDIA2 — Pure no-live miner for media-positive cases in existing
project data.

Scans:
- ``autoresearch/calibration_data.json`` (immutable - read only)
- ``pipeline2_discovery/calibration_data.json`` (immutable - read only)
- ``tests/fixtures/casegraph_scenarios/`` (CasePacket fixtures)
- ``tests/fixtures/pilot_cases/`` (real-case + manifest)

For every case in calibration data, classifies known
``ground_truth`` flags (bodycam / dashcam / court_video /
interrogation / dispatch_911) plus the
``ground_truth.verified_sources`` URL list. URLs are run through the
existing :func:`classify_media_url` policy (MEDIA1) so the report
agrees with the central media classifier on what counts as media vs
document vs rejected.

Pure: never makes a network call, never downloads, never scrapes,
never invents media claims. Produces a deterministic ranked report.
The report is intended to seed media-yield pilot manifest entries
(MEDIA3) and inform LIVE8 pilot selection.

Usage::

    from pipeline2_discovery.casegraph.media_candidates import (
        find_media_candidates,
    )
    report = find_media_candidates()
    # report['candidates'] is sorted by media_confidence_score desc
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .media_policy import MediaClassification, classify_media_url


MEDIA_SIGNAL_TERMS = (
    "bodycam",
    "body cam",
    "body-worn camera",
    "body-worn",
    "bwc",
    "dashcam",
    "dash cam",
    "dash-cam",
    "surveillance",
    "cctv",
    "interrogation",
    "confession",
    "interview footage",
    "police interview",
    "911 call",
    "911 audio",
    "dispatch audio",
    "dispatch_911",
    "court video",
    "courtroom video",
    "sentencing video",
    "trial video",
    "youtube",
    "vimeo",
    "critical incident",
    "officer involved shooting",
    "officer-involved shooting",
    "ois",
    "video released",
    "footage released",
    "video evidence",
)

# Map calibration_data ground_truth flags (YES/NO) to canonical
# CaseGraph artifact_type values when the flag is "YES".
GROUND_TRUTH_MEDIA_FLAGS = (
    ("bodycam", "bodycam"),
    ("dashcam", "dashcam"),
    ("court_video", "court_video"),
    ("interrogation", "interrogation"),
    ("dispatch_911", "dispatch_911"),
    ("surveillance", "surveillance"),
    ("sentencing_video", "court_video"),
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _safe_load_json(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _signal_hits_in_text(text: str) -> List[str]:
    """Return media signal terms present in ``text`` (case-insensitive,
    deterministic order, no duplicates)."""
    if not text:
        return []
    lower = text.lower()
    hits: List[str] = []
    seen: set = set()
    for term in MEDIA_SIGNAL_TERMS:
        if term in lower and term not in seen:
            hits.append(term)
            seen.add(term)
    return hits


def _classify_urls(
    urls: Iterable[str],
) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Run each URL through the central media policy and split into
    (classified, media_count, document_count, rejected_count)."""
    classified: List[Dict[str, Any]] = []
    media_count = 0
    document_count = 0
    rejected_count = 0
    for url in urls:
        cls = classify_media_url(url)
        classified.append(cls.to_dict())
        if cls.is_media:
            media_count += 1
        elif cls.is_document:
            document_count += 1
        else:
            rejected_count += 1
    return classified, media_count, document_count, rejected_count


def _likely_connector_path(
    *,
    has_youtube_or_vimeo_url: bool,
    has_official_agency_url: bool,
    has_documentcloud_url: bool,
    has_court_video_signal: bool,
) -> List[str]:
    """Rank likely connector paths for surfacing media for this
    candidate. Order matters: the live runner should prefer the first
    path. YouTube wins when there's any known YouTube/Vimeo URL or any
    court_video signal (court records often appear on YouTube)."""
    path: List[str] = []
    if has_youtube_or_vimeo_url or has_court_video_signal:
        path.append("youtube")
    if has_official_agency_url:
        path.append("agency_ois")
    if has_documentcloud_url:
        path.append("documentcloud")
    # Document-only fallbacks are always last for media-yield pilots.
    if "documentcloud" not in path:
        path.append("documentcloud")
    path.append("courtlistener")
    path.append("muckrock")
    return path


def _candidate_queries(name: str, jurisdiction: str) -> List[str]:
    """Build candidate query strings for live YouTube/agency searches.
    Strips suffixes (Jr., Sr., II, III) so structured names don't
    poison the query."""
    if not name:
        return []
    primary = name.split(",")[0].strip()
    primary = re.sub(r"\s+(Jr\.?|Sr\.?|II|III|IV|V)$", "", primary, flags=re.IGNORECASE)
    juris_short = jurisdiction.split(",")[0].strip() if jurisdiction else ""

    queries: List[str] = []
    if primary:
        queries.append(f'"{primary}"')
    if primary and juris_short:
        queries.append(f'"{primary}" {juris_short}')
    if primary:
        queries.append(f'"{primary}" sentencing')
        queries.append(f'"{primary}" bodycam')
    return queries


def _candidate_from_calibration_entry(
    entry: Mapping[str, Any],
    *,
    source_path: str,
) -> Optional[Dict[str, Any]]:
    """Build a media candidate from a calibration_data entry."""
    case_id = entry.get("case_id")
    name = str(entry.get("defendant_names") or "")
    jurisdiction = str(entry.get("jurisdiction") or "")
    tier = str(entry.get("tier") or "")
    gt = entry.get("ground_truth") or {}
    if not isinstance(gt, Mapping):
        gt = {}

    # Map ground_truth YES flags to media signal terms + artifact types.
    media_signal_terms: List[str] = []
    desired_artifact_types: List[str] = []
    source_fields: List[str] = []
    for flag, art_type in GROUND_TRUTH_MEDIA_FLAGS:
        if str(gt.get(flag) or "").upper() == "YES":
            media_signal_terms.append(flag)
            if art_type not in desired_artifact_types:
                desired_artifact_types.append(art_type)
            source_fields.append(f"{source_path}.ground_truth.{flag}=YES")

    # Verified sources URL classification.
    raw_sources = gt.get("verified_sources") or []
    urls: List[str] = [str(u) for u in raw_sources if isinstance(u, str)]
    classified, media_count, document_count, rejected_count = _classify_urls(urls)
    youtube_urls = [
        c["url"] for c in classified
        if c["is_media"] and "youtube" in (c["url"] or "").lower() or "youtu.be" in (c["url"] or "").lower()
    ]
    vimeo_urls = [c["url"] for c in classified if c["is_media"] and "vimeo" in (c["url"] or "").lower()]

    if not (media_signal_terms or media_count or document_count):
        return None

    # Confidence score: ground_truth flags weigh heavily, public media
    # URLs more so, ENOUGH-tier cases get a small bump.
    score = (
        len(media_signal_terms) * 10
        + media_count * 12
        + document_count * 3
        + (5 if tier.upper() == "ENOUGH" else 0)
        + (2 if tier.upper() == "BORDERLINE" else 0)
    )

    risk_flags: List[str] = []
    if not media_signal_terms and not media_count:
        risk_flags.append("documents_only_candidate")
    for c in classified:
        if c.get("rejected") and c.get("rejection_reason") == "protected_or_nonpublic":
            if "protected_url_present" not in risk_flags:
                risk_flags.append("protected_url_present")

    has_official = any(
        ".gov" in (c["url"] or "").lower() or "police" in (c["url"] or "").lower()
        for c in classified
    )

    likely_path = _likely_connector_path(
        has_youtube_or_vimeo_url=bool(youtube_urls or vimeo_urls),
        has_official_agency_url=has_official,
        has_documentcloud_url=any("documentcloud" in (c["url"] or "").lower() for c in classified),
        has_court_video_signal="court_video" in media_signal_terms,
    )

    return {
        "case_id": case_id,
        "name": name,
        "jurisdiction": jurisdiction,
        "tier": tier,
        "source": source_path,
        "media_signal_terms": media_signal_terms,
        "desired_artifact_types": desired_artifact_types,
        "source_fields": source_fields,
        "known_urls": urls,
        "classified_urls": classified,
        "url_classification_summary": {
            "media_count": media_count,
            "document_count": document_count,
            "rejected_count": rejected_count,
        },
        "likely_connector_path": likely_path,
        "candidate_query_strings": _candidate_queries(name, jurisdiction),
        "media_confidence_score": score,
        "risk_flags": risk_flags,
    }


def _scan_fixture_dir(
    fixture_dir: Path,
) -> List[Dict[str, Any]]:
    """Scan a directory of CasePacket-shape fixtures for embedded
    media-signal hits in the title / snippet / raw_text / metadata
    text. Returns lightweight candidate entries (deduped on case_id).

    This catches references in scenario fixtures without inventing any
    URL: only existing fixture content is scanned.
    """
    out: List[Dict[str, Any]] = []
    if not fixture_dir.exists():
        return out
    for path in sorted(fixture_dir.glob("*.json")):
        data = _safe_load_json(path)
        if not isinstance(data, dict):
            continue
        if "case_id" not in data:
            continue
        text_blobs: List[str] = []
        for src in data.get("sources") or []:
            if isinstance(src, dict):
                for key in ("title", "snippet", "raw_text"):
                    val = src.get(key)
                    if isinstance(val, str) and val:
                        text_blobs.append(val)
        for art in data.get("verified_artifacts") or []:
            if isinstance(art, dict):
                url = art.get("artifact_url")
                if isinstance(url, str) and url:
                    text_blobs.append(url)
        all_text = " ".join(text_blobs)
        signals = _signal_hits_in_text(all_text)
        if not signals:
            continue
        out.append(
            {
                "case_id": data.get("case_id"),
                "fixture_path": str(path.relative_to(_repo_root())),
                "media_signal_terms": signals,
                "source": str(path.relative_to(_repo_root())),
            }
        )
    return out


def find_media_candidates(
    *,
    calibration_paths: Optional[Sequence[Path]] = None,
    fixture_dirs: Optional[Sequence[Path]] = None,
    top_n: Optional[int] = None,
) -> Dict[str, Any]:
    """Pure no-live scan for media-positive cases.

    Args:
        calibration_paths: list of calibration_data.json paths to
            scan. Defaults to the two project calibration files.
        fixture_dirs: list of fixture directories to scan for
            embedded media-signal hits. Defaults to scenario +
            pilot_cases dirs.
        top_n: when set, truncate the candidates list to the top N
            by media_confidence_score.

    Returns a JSON-serializable report.
    """
    repo = _repo_root()
    if calibration_paths is None:
        calibration_paths = [
            repo / "autoresearch" / "calibration_data.json",
            repo / "pipeline2_discovery" / "calibration_data.json",
        ]
    if fixture_dirs is None:
        fixture_dirs = [
            repo / "tests" / "fixtures" / "casegraph_scenarios",
            repo / "tests" / "fixtures" / "pilot_cases",
        ]

    scanned_sources: List[str] = []
    candidates: List[Dict[str, Any]] = []

    for path in calibration_paths:
        scanned_sources.append(str(path.relative_to(repo)) if str(path).startswith(str(repo)) else str(path))
        data = _safe_load_json(path)
        if not isinstance(data, list):
            continue
        for entry in data:
            if not isinstance(entry, Mapping):
                continue
            cand = _candidate_from_calibration_entry(
                entry, source_path=str(path.relative_to(repo))
            )
            if cand is not None:
                candidates.append(cand)

    # Dedupe by (case_id, source) - if two calibration files contain
    # the same case_id, keep the higher-scoring one.
    deduped: Dict[Tuple[Any, str], Dict[str, Any]] = {}
    for cand in candidates:
        key = (cand.get("case_id"), cand.get("name"))
        existing = deduped.get(key)
        if existing is None or cand["media_confidence_score"] > existing["media_confidence_score"]:
            deduped[key] = cand
    candidates = list(deduped.values())

    fixture_hits: List[Dict[str, Any]] = []
    for d in fixture_dirs:
        scanned_sources.append(str(d.relative_to(repo)) if str(d).startswith(str(repo)) else str(d))
        fixture_hits.extend(_scan_fixture_dir(d))

    candidates.sort(
        key=lambda c: (-c.get("media_confidence_score", 0), str(c.get("case_id") or "")),
    )
    if top_n is not None and top_n >= 0:
        candidates = candidates[:top_n]

    youtube_count = sum(
        1
        for c in candidates
        if any(
            "youtube" in (cls.get("url") or "").lower()
            for cls in c.get("classified_urls") or []
            if cls.get("is_media")
        )
    )
    media_url_total = sum(
        c.get("url_classification_summary", {}).get("media_count", 0) for c in candidates
    )

    return {
        "experiment_id": "MEDIA2",
        "scanned_sources": scanned_sources,
        "candidate_count": len(candidates),
        "fixture_signal_hits": fixture_hits,
        "summary": {
            "candidates_with_youtube_media": youtube_count,
            "total_known_media_urls": media_url_total,
        },
        "candidates": candidates,
    }
