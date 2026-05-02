"""PRIMARY1 - official-primary media candidate mining.

Read-only miner over existing project data. It looks for cases and
fixtures that already carry Tier A media signals (bodycam/BWC,
dashcam, interrogation, police interview, 911/dispatch audio,
surveillance, official critical-incident video, raw agency footage)
and emits ranked candidate leads for future controlled live pilots.

This module is deterministic and offline: no network, no scraping, no
downloads, no transcript fetching, and no LLMs.
"""
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import urlparse


TIER_A_TERMS: Sequence[tuple[str, str, str]] = (
    ("bodycam", "bodycam", "bodycam"),
    ("body cam", "bodycam", "bodycam"),
    ("body camera", "bodycam", "bodycam"),
    ("body-worn camera", "bodycam", "bodycam"),
    ("body worn camera", "bodycam", "bodycam"),
    ("bwc", "bodycam", "bodycam"),
    ("dashcam", "dashcam", "dashcam"),
    ("dash cam", "dashcam", "dashcam"),
    ("interrogation", "interrogation", "interrogation"),
    ("confession", "confession", "interrogation"),
    ("police interview", "police_interview", "interrogation"),
    ("911", "dispatch_911", "dispatch_911"),
    ("dispatch audio", "dispatch_911", "dispatch_911"),
    ("surveillance", "surveillance", "surveillance_video"),
    ("critical incident", "critical_incident", "bodycam"),
    ("officer involved shooting", "official_ois", "bodycam"),
    ("officer-involved shooting", "official_ois", "bodycam"),
    ("ois", "official_ois", "bodycam"),
    ("raw footage", "raw_footage", "bodycam"),
    ("released footage", "released_footage", "bodycam"),
    ("official video", "official_video", "bodycam"),
    ("agency video", "agency_video", "bodycam"),
    ("sheriff video", "sheriff_video", "bodycam"),
    ("police department video", "police_department_video", "bodycam"),
)

TIER_B_TERMS = {
    "court video",
    "court_video",
    "sentencing video",
    "trial video",
    "courtroom footage",
}

WEAK_HOSTS = {"youtube.com", "www.youtube.com", "youtu.be", "m.youtube.com"}
DOCUMENT_HOST_HINTS = {"courtlistener.com", "documentcloud.org", "justia.com"}


@dataclass
class PrimaryMediaCandidate:
    candidate_id: str
    case_name: str
    jurisdiction: Optional[str]
    agency: Optional[str]
    outcome: Optional[str]
    media_signal_terms: List[str] = field(default_factory=list)
    media_signal_source_fields: List[str] = field(default_factory=list)
    known_urls: List[str] = field(default_factory=list)
    likely_connector_path: List[str] = field(default_factory=list)
    suggested_queries: List[str] = field(default_factory=list)
    expected_artifact_type: str = "unknown"
    confidence_media_is_tier_a: float = 0.0
    risk_flags: List[str] = field(default_factory=list)
    source_path: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PrimaryMediaCandidateReport:
    total_candidates: int
    candidates: List[PrimaryMediaCandidate] = field(default_factory=list)
    source_paths: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_candidates": self.total_candidates,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "source_paths": list(self.source_paths),
            "warnings": list(self.warnings),
        }


def default_primary_media_source_paths(repo_root: Optional[Path] = None) -> List[Path]:
    root = Path(repo_root) if repo_root else Path(__file__).resolve().parents[2]
    return [
        root / "autoresearch" / "calibration_data.json",
        root / "pipeline2_discovery" / "calibration_data.json",
        root / "tests" / "fixtures" / "casegraph_scenarios",
        root / "tests" / "fixtures" / "pilot_cases",
        # Existing no-live agency OIS fixtures are local media-candidate
        # outputs, not live fetches. They are useful for official-primary
        # path ranking and remain read-only.
        root / "tests" / "fixtures" / "agency_ois",
    ]


def _append_unique(items: List[str], values: Iterable[str]) -> None:
    seen = set(items)
    for value in values:
        if value and value not in seen:
            items.append(value)
            seen.add(value)


def _json_files(paths: Iterable[Path]) -> List[Path]:
    files: List[Path] = []
    for path in paths:
        p = Path(path)
        if p.is_file() and p.suffix.lower() == ".json":
            files.append(p)
        elif p.is_dir():
            files.extend(sorted(child for child in p.rglob("*.json") if child.is_file()))
    return sorted(dict.fromkeys(files))


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _flatten(value: Any, prefix: str = "") -> List[tuple[str, str]]:
    rows: List[tuple[str, str]] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            field = f"{prefix}.{key}" if prefix else str(key)
            rows.extend(_flatten(item, field))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            rows.extend(_flatten(item, f"{prefix}[{idx}]"))
    elif value is not None:
        rows.append((prefix, str(value)))
    return rows


def _match_tier_a(rows: List[tuple[str, str]]) -> tuple[List[str], List[str], List[str]]:
    terms: List[str] = []
    fields: List[str] = []
    artifact_types: List[str] = []
    for field, value in rows:
        text = f"{field} {value}".lower()
        for needle, label, artifact_type in TIER_A_TERMS:
            if needle in text:
                _append_unique(terms, [label])
                _append_unique(fields, [field])
                _append_unique(artifact_types, [artifact_type])
    return terms, fields, artifact_types


def _match_tier_b(rows: List[tuple[str, str]]) -> bool:
    text = " ".join(f"{field} {value}" for field, value in rows).lower()
    return any(term in text for term in TIER_B_TERMS)


def _urls(rows: List[tuple[str, str]]) -> List[str]:
    urls: List[str] = []
    for _, value in rows:
        for hit in re.findall(r"https?://[^\s\"'<>),]+", value):
            _append_unique(urls, [hit.rstrip(".,;")])
    return urls


def _host(url: str) -> str:
    return urlparse(url).netloc.lower()


def _looks_official_url(url: str) -> bool:
    host = _host(url)
    return host.endswith(".gov") or "police" in host or "sheriff" in host


def _connector_path(urls: List[str], rows: List[tuple[str, str]], *, agency: Optional[str]) -> List[str]:
    text = " ".join(f"{field} {value}" for field, value in rows).lower()
    connectors: List[str] = []
    if "critical incident" in text or "ois" in text or any(_looks_official_url(url) for url in urls):
        connectors.append("agency_ois")
    if any(_host(url) in WEAK_HOSTS for url in urls) or "youtube" in text:
        connectors.append("youtube")
    if "muckrock" in text or "foia" in text or agency:
        connectors.append("muckrock")
    if "documentcloud" in text:
        connectors.append("documentcloud")
    if "courtlistener" in text or "court" in text:
        connectors.append("courtlistener")
    if not connectors:
        connectors.extend(["youtube", "muckrock"])
    return connectors


def _candidate_id(path: Path, item: Mapping[str, Any], index: int) -> str:
    raw = (
        item.get("case_id")
        or item.get("id")
        or item.get("candidate_id")
        or item.get("defendant_names")
        or item.get("subject_name")
        or path.stem
    )
    seed = f"{path.stem}-{index}-{raw}".lower()
    return re.sub(r"[^a-z0-9]+", "_", seed).strip("_")[:96]


def _case_name(item: Mapping[str, Any]) -> str:
    identity = item.get("case_identity") if isinstance(item.get("case_identity"), Mapping) else {}
    input_data = item.get("input") if isinstance(item.get("input"), Mapping) else {}
    known = input_data.get("known_fields") if isinstance(input_data.get("known_fields"), Mapping) else {}
    names = (
        identity.get("defendant_names")
        or known.get("defendant_names")
        or item.get("defendant_names")
        or item.get("subject_name")
        or item.get("subjects")
        or item.get("case_name")
    )
    if isinstance(names, list):
        return ", ".join(str(name) for name in names if name)
    return str(names or "unknown")


def _jurisdiction(item: Mapping[str, Any]) -> Optional[str]:
    identity = item.get("case_identity") if isinstance(item.get("case_identity"), Mapping) else {}
    input_data = item.get("input") if isinstance(item.get("input"), Mapping) else {}
    known = input_data.get("known_fields") if isinstance(input_data.get("known_fields"), Mapping) else {}
    juris = identity.get("jurisdiction") or known.get("jurisdiction") or item.get("jurisdiction")
    if isinstance(juris, Mapping):
        parts = [juris.get("city"), juris.get("county"), juris.get("state")]
        return ", ".join(str(part) for part in parts if part) or None
    return str(juris) if juris else None


def _agency(item: Mapping[str, Any]) -> Optional[str]:
    identity = item.get("case_identity") if isinstance(item.get("case_identity"), Mapping) else {}
    input_data = item.get("input") if isinstance(item.get("input"), Mapping) else {}
    known = input_data.get("known_fields") if isinstance(input_data.get("known_fields"), Mapping) else {}
    value = identity.get("agency") or known.get("agency") or item.get("agency")
    return str(value) if value else None


def _outcome(item: Mapping[str, Any]) -> Optional[str]:
    identity = item.get("case_identity") if isinstance(item.get("case_identity"), Mapping) else {}
    value = identity.get("outcome_status") or item.get("outcome_status") or item.get("outcome_text")
    return str(value) if value else None


def _suggested_queries(
    *,
    case_name: str,
    jurisdiction: Optional[str],
    agency: Optional[str],
    terms: List[str],
) -> List[str]:
    queries: List[str] = []
    primary = terms[0] if terms else "critical incident video"
    if agency and case_name != "unknown":
        queries.append(f'"{case_name}" "{agency}" {primary}')
    if agency:
        queries.append(f'"{agency}" critical incident video {case_name if case_name != "unknown" else ""}'.strip())
    if case_name != "unknown" and jurisdiction:
        queries.append(f'"{case_name}" "{jurisdiction}" {primary}')
    if case_name != "unknown":
        queries.append(f'"{case_name}" bodycam interrogation 911')
    return list(dict.fromkeys(q for q in queries if q))


def _score_candidate(
    *,
    terms: List[str],
    fields: List[str],
    urls: List[str],
    connectors: List[str],
    item: Mapping[str, Any],
    tier_b_only: bool,
) -> tuple[float, List[str]]:
    score = 0.0
    risks: List[str] = []
    has_verified_artifact_signal = any("verified_artifacts" in field for field in fields)
    has_claim_signal = any("claim" in field.lower() for field in fields)
    if terms:
        score += min(0.35, 0.08 * len(terms))
    if has_verified_artifact_signal:
        score += 0.35
    elif any(".media_links" in field or "media_links" in field or field.endswith(".url") for field in fields):
        score += 0.2
    if any(_looks_official_url(url) for url in urls) or "agency_ois" in connectors:
        score += 0.25
    if urls:
        score += 0.1
    if _outcome(item) in {"sentenced", "convicted", "closed"}:
        score += 0.05

    if tier_b_only and not terms:
        risks.append("tier_b_secondary_media_only")
    if urls and all(_host(url) in WEAK_HOSTS for url in urls):
        risks.append("generic_youtube_or_video_host_only")
        score -= 0.1
    if not urls:
        risks.append("media_signal_without_known_url")
    if has_claim_signal and not has_verified_artifact_signal:
        risks.append("claim_signal_not_verified_artifact")
        score = min(score - 0.15, 0.45)
    if case_name := _case_name(item):
        if "," in case_name:
            risks.append("multiple_names_need_disambiguation")
    return round(max(0.0, min(score, 1.0)), 2), risks


def _iter_candidate_items(data: Any) -> Iterable[tuple[int, Mapping[str, Any]]]:
    if isinstance(data, list):
        for index, item in enumerate(data):
            if isinstance(item, Mapping):
                yield index, item
    elif isinstance(data, Mapping) and isinstance(data.get("pilots"), list):
        for index, item in enumerate(data.get("pilots") or []):
            if isinstance(item, Mapping):
                yield index, item
    elif isinstance(data, Mapping):
        yield 0, data


def mine_primary_media_candidates(
    paths: Optional[Iterable[Path]] = None,
    *,
    repo_root: Optional[Path] = None,
    top_n: Optional[int] = None,
) -> PrimaryMediaCandidateReport:
    """Mine ranked Tier A candidate leads from existing local data."""

    source_paths = list(paths) if paths is not None else default_primary_media_source_paths(repo_root)
    files = _json_files(source_paths)
    candidates: List[PrimaryMediaCandidate] = []
    warnings: List[str] = []

    for path in files:
        try:
            data = _load_json(path)
        except (OSError, json.JSONDecodeError) as exc:
            warnings.append(f"could_not_read:{path}:{exc}")
            continue
        for index, item in _iter_candidate_items(data):
            rows = _flatten(item)
            terms, fields, artifact_types = _match_tier_a(rows)
            tier_b = _match_tier_b(rows)
            if not terms and not tier_b:
                continue
            urls = _urls(rows)
            name = _case_name(item)
            agency = _agency(item)
            jurisdiction = _jurisdiction(item)
            connectors = _connector_path(urls, rows, agency=agency)
            score, risks = _score_candidate(
                terms=terms,
                fields=fields,
                urls=urls,
                connectors=connectors,
                item=item,
                tier_b_only=tier_b and not terms,
            )
            if not terms and tier_b:
                expected_artifact_type = "court_video"
            else:
                expected_artifact_type = artifact_types[0] if artifact_types else "unknown"
            candidates.append(
                PrimaryMediaCandidate(
                    candidate_id=_candidate_id(path, item, index),
                    case_name=name,
                    jurisdiction=jurisdiction,
                    agency=agency,
                    outcome=_outcome(item),
                    media_signal_terms=terms or ["court_video"],
                    media_signal_source_fields=fields,
                    known_urls=urls,
                    likely_connector_path=connectors,
                    suggested_queries=_suggested_queries(
                        case_name=name,
                        jurisdiction=jurisdiction,
                        agency=agency,
                        terms=terms,
                    ),
                    expected_artifact_type=expected_artifact_type,
                    confidence_media_is_tier_a=score,
                    risk_flags=risks,
                    source_path=str(path),
                )
            )

    candidates.sort(
        key=lambda c: (
            -c.confidence_media_is_tier_a,
            "tier_b_secondary_media_only" in c.risk_flags,
            c.case_name,
            c.candidate_id,
        )
    )
    if top_n is not None:
        candidates = candidates[:top_n]

    return PrimaryMediaCandidateReport(
        total_candidates=len(candidates),
        candidates=candidates,
        source_paths=[str(path) for path in files],
        warnings=warnings,
    )
