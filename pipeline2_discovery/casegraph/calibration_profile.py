"""CAL1 - deterministic calibration-set profiler.

The legacy calibration JSON files are immutable benchmark inputs. This
module reads them, deduplicates mirrored rows, and emits a stable
CaseGraph-oriented profile for each case: known artifact signals,
source URL types, likely connector paths, portal-profile needs, and
support/blocker status against the current no-paid live connector set.

Pure/no-live: no network, scraping, downloads, transcript fetching, or
LLMs.
"""
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import unquote, urlparse

from .routers import parse_jurisdiction


YES_VALUES = {"YES", "MAYBE"}
MEDIA_SIGNAL_KEYS = ("bodycam", "interrogation", "court_video", "dispatch_911")
TIER_A_SIGNAL_KEYS = ("bodycam", "interrogation", "dispatch_911")
DOCUMENT_SIGNAL_KEYS = ("docket_docs",)

SUPPORTED_LIVE_CONNECTORS = {"youtube", "muckrock", "documentcloud", "courtlistener"}

ARTIFACT_LABELS = {
    "bodycam": "bodycam",
    "interrogation": "interrogation",
    "court_video": "court_video",
    "dispatch_911": "dispatch_911",
    "docket_docs": "docket_docs",
}

PORTAL_BY_CONNECTOR = {
    "agency_ois": "agency_ois_listing",
    "youtube": "youtube_agency_channel",
    "muckrock": "muckrock_request",
    "documentcloud": "documentcloud_search",
    "courtlistener": "courtlistener_search",
}

OUTCOME_STATUSES = ("sentenced", "convicted", "charged", "dismissed", "acquitted", "closed", "unknown")
OUTCOME_PATTERN_GROUPS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    (
        "sentenced",
        (
            "sentenced",
            "sentencing",
            "death-sentence",
            "judgment-as-to",
            "imprisonment",
            "execution-date",
            "order-setting-execution",
            "motion-set-execution-date",
        ),
    ),
    (
        "convicted",
        (
            "convicted",
            "found-guilty",
            "pleads-guilty",
            "pleaded-guilty",
            "plea-entered",
            "guilty-to",
            "guilty-of",
        ),
    ),
    ("acquitted", ("acquitted", "not-guilty")),
    ("dismissed", ("dismissed", "dismissal")),
    ("charged", ("charged", "criminal-complaint", "trial-set", "accused")),
    ("closed", ("case-closed", "closed-case")),
)


@dataclass
class CalibrationProfileRow:
    case_id: int
    title: str
    jurisdiction: str
    state: Optional[str]
    agency: Optional[str]
    tier: str
    outcome_status: str
    outcome_seed_status: str = "unknown"
    outcome_source_field: Optional[str] = None
    outcome_confidence: float = 0.0
    missing_outcome_reason: Optional[str] = "no_deterministic_outcome_seed"
    recommended_corrob_source: str = "manual seed needed"
    known_source_urls: List[str] = field(default_factory=list)
    known_media_artifact_signals: List[str] = field(default_factory=list)
    expected_artifact_types: List[str] = field(default_factory=list)
    expected_media_types: List[str] = field(default_factory=list)
    source_types_already_known: List[str] = field(default_factory=list)
    likely_connector_path: List[str] = field(default_factory=list)
    needed_portal_profile: str = "source_discovery_required"
    portal_profiles_needed: List[str] = field(default_factory=list)
    benchmark_role: str = "negative_or_no_artifact_case"
    benchmark_roles: List[str] = field(default_factory=list)
    supported_live_path_available: bool = False
    blocker_if_not_supported: Optional[str] = None
    risk_flags: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CalibrationProfileReport:
    total_cases: int
    profile_rows: List[CalibrationProfileRow] = field(default_factory=list)
    source_paths: List[str] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_cases": self.total_cases,
            "profile_rows": [row.to_dict() for row in self.profile_rows],
            "source_paths": list(self.source_paths),
            "summary": dict(self.summary),
            "warnings": list(self.warnings),
        }


def default_calibration_paths(repo_root: Optional[Path] = None) -> List[Path]:
    root = Path(repo_root) if repo_root else Path(__file__).resolve().parents[2]
    return [
        root / "autoresearch" / "calibration_data.json",
        root / "pipeline2_discovery" / "calibration_data.json",
    ]


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _truthy_signal(value: Any) -> bool:
    return str(value or "").strip().upper() in YES_VALUES


def _append_unique(items: List[str], values: Iterable[str]) -> None:
    seen = set(items)
    for value in values:
        if value and value not in seen:
            items.append(value)
            seen.add(value)


def _host(url: str) -> str:
    return urlparse(url or "").netloc.lower()


def _path(url: str) -> str:
    return urlparse(url or "").path.lower()


def _url_text(url: str) -> str:
    parsed = urlparse(url or "")
    text = " ".join(part for part in (parsed.netloc, parsed.path, parsed.query) if part)
    text = unquote(text).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


def _state_from_jurisdiction(jurisdiction: str) -> Optional[str]:
    parsed = parse_jurisdiction(jurisdiction or "")
    return parsed.state


def _source_type_for_url(url: str) -> List[str]:
    host = _host(url)
    path = _path(url)
    types: List[str] = []
    if not host:
        return ["unknown_url"]
    if host in {"youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be"}:
        types.append("youtube")
    if "muckrock.com" in host:
        types.append("muckrock")
    if "documentcloud.org" in host or "s3.documentcloud.org" in host:
        types.append("documentcloud")
    if "courtlistener.com" in host:
        types.append("courtlistener")
    if any(term in host for term in ("findlaw.com", "justia.com", "caselaw", "tncourts.gov", "azcourts.gov", "courts.gov")):
        types.append("court_public")
    if host.endswith(".gov") or ".gov" in host:
        types.append("official_gov")
        if any(term in host + path for term in ("police", "sheriff", "critical", "ois", "shooting", "transparency")):
            types.append("agency_ois")
    if any(term in host for term in ("abc", "nbc", "cbs", "fox", "news", "azcentral", "knoxnews", "courthousenews", "gazette", "firstcoastnews", "firstalert")):
        types.append("news")
    if any(term in host for term in ("reddit.com", "tiktok.com", "instagram.com")):
        types.append("social")
    if path.endswith(".pdf"):
        types.append("pdf_document")
    if path.endswith((".mp4", ".mov", ".webm", ".m3u8")):
        types.append("direct_video_url")
    if path.endswith((".mp3", ".wav", ".m4a")):
        types.append("direct_audio_url")
    return types or ["web"]


def _connectors_for_source_types(source_types: Sequence[str], expected: Sequence[str]) -> List[str]:
    connectors: List[str] = []
    if "agency_ois" in source_types or "direct_video_url" in source_types or "direct_audio_url" in source_types:
        connectors.append("agency_ois")
    if "youtube" in source_types:
        connectors.append("youtube")
    if "muckrock" in source_types:
        connectors.append("muckrock")
    if "documentcloud" in source_types:
        connectors.append("documentcloud")
    if "courtlistener" in source_types or "court_public" in source_types:
        connectors.append("courtlistener")
    if not connectors:
        if any(item in expected for item in MEDIA_SIGNAL_KEYS):
            connectors.extend(["youtube", "muckrock"])
        if "docket_docs" in expected:
            connectors.append("courtlistener")
    return list(dict.fromkeys(connectors))


def _portal_profiles(connectors: Sequence[str], source_types: Sequence[str], expected: Sequence[str]) -> List[str]:
    profiles: List[str] = []
    for connector in connectors:
        profile = PORTAL_BY_CONNECTOR.get(connector)
        if profile:
            profiles.append(profile)
    if "agency_ois" in source_types:
        profiles.insert(0, "agency_ois_detail")
    if "court_video" in expected and "courtlistener_search" not in profiles:
        profiles.append("court_case_detail")
    return list(dict.fromkeys(profiles)) or ["source_discovery_required"]


def _benchmark_roles(expected: Sequence[str], tier: str) -> List[str]:
    roles: List[str] = ["identity_case", "outcome_case"]
    if "docket_docs" in expected:
        roles.append("document_artifact_case")
    if any(item in expected for item in MEDIA_SIGNAL_KEYS):
        roles.append("media_artifact_case")
    if any(item in expected for item in TIER_A_SIGNAL_KEYS):
        roles.append("tier_a_primary_media_case")
    if not expected or tier == "INSUFFICIENT":
        roles.append("negative_or_no_artifact_case")
    return list(dict.fromkeys(roles))


def _primary_role(roles: Sequence[str]) -> str:
    for role in (
        "tier_a_primary_media_case",
        "media_artifact_case",
        "document_artifact_case",
        "negative_or_no_artifact_case",
        "outcome_case",
        "identity_case",
    ):
        if role in roles:
            return role
    return "identity_case"


def _risk_flags(
    *,
    expected: Sequence[str],
    source_types: Sequence[str],
    connectors: Sequence[str],
    tier: str,
    urls: Sequence[str],
) -> List[str]:
    risks: List[str] = []
    if tier == "INSUFFICIENT":
        risks.append("legacy_insufficient_case")
    if not expected:
        risks.append("no_known_artifact_signal")
    if any(item in expected for item in TIER_A_SIGNAL_KEYS):
        if "agency_ois" in connectors and not (set(connectors) & SUPPORTED_LIVE_CONNECTORS):
            risks.append("agency_ois_only")
        if source_types and set(source_types) <= {"youtube", "social"}:
            risks.append("generic_media_only")
        if "youtube" in source_types and "official_gov" not in source_types and "agency_ois" not in source_types:
            risks.append("tier_a_media_needs_primary_source_verification")
    if "docket_docs" in expected and not any(item in expected for item in MEDIA_SIGNAL_KEYS):
        risks.append("document_only_expected")
    if urls and not (set(connectors) & SUPPORTED_LIVE_CONNECTORS):
        risks.append("unsupported_portal_profile")
    if not urls and expected:
        risks.append("needs_seed_url_discovery")
    return list(dict.fromkeys(risks))


def _blocker(connectors: Sequence[str], risks: Sequence[str], expected: Sequence[str]) -> Optional[str]:
    if set(connectors) & SUPPORTED_LIVE_CONNECTORS:
        return None
    if "agency_ois_only" in risks:
        return "agency_ois_live_connector_missing"
    if "needs_seed_url_discovery" in risks:
        return "needs_seed_url_discovery"
    if expected:
        return "supported_connector_missing"
    return "no_known_artifact_signal"


def _normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", name.lower()).strip()


def _fixture_outcome_index(repo_root: Optional[Path]) -> Dict[str, Dict[str, Any]]:
    root = Path(repo_root) if repo_root else Path(__file__).resolve().parents[2]
    fixture_dir = root / "tests" / "fixtures" / "pilot_cases"
    index: Dict[str, Dict[str, Any]] = {}
    if not fixture_dir.exists():
        return index
    for path in sorted(fixture_dir.glob("*.json")):
        try:
            data = _load_json(path)
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(data, Mapping):
            continue
        identity = data.get("case_identity") if isinstance(data.get("case_identity"), Mapping) else {}
        status = str(identity.get("outcome_status") or "").strip().lower()
        if status not in OUTCOME_STATUSES or status == "unknown":
            continue
        names = identity.get("defendant_names") or []
        if isinstance(names, str):
            names = [names]
        for name in names:
            key = _normalize_name(str(name))
            if key:
                index[key] = {
                    "status": status,
                    "source_field": f"pilot_fixture:{path.relative_to(root).as_posix()}:case_identity.outcome_status",
                    "confidence": 0.95,
                }
    return index


def _outcome_from_fixture(row: Mapping[str, Any], fixture_index: Mapping[str, Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    title = str(row.get("defendant_names") or "")
    for name in [part.strip() for part in title.split(",") if part.strip()]:
        match = fixture_index.get(_normalize_name(name))
        if match:
            return dict(match)
    return None


def _outcome_from_urls(urls: Sequence[str]) -> Optional[Dict[str, Any]]:
    for index, url in enumerate(urls):
        text = _url_text(url)
        for status, patterns in OUTCOME_PATTERN_GROUPS:
            if any(pattern in text for pattern in patterns):
                return {
                    "status": status,
                    "source_field": f"ground_truth.verified_sources[{index}]",
                    "confidence": _confidence_for_url_status(status, url),
                }
    return None


def _confidence_for_url_status(status: str, url: str) -> float:
    source_types = set(_source_type_for_url(url))
    if status == "charged":
        return 0.62
    if source_types & {"courtlistener", "court_public", "official_gov", "pdf_document"}:
        return 0.84
    if "news" in source_types:
        return 0.78
    return 0.7


def _recommended_corrob_source(source_types: Sequence[str], connectors: Sequence[str]) -> str:
    if "courtlistener" in connectors or "courtlistener" in source_types:
        return "courtlistener"
    if "court_public" in source_types:
        return "court docket"
    if "documentcloud" in connectors or "documentcloud" in source_types:
        return "documentcloud"
    if "news" in source_types:
        return "news article"
    if "muckrock" in connectors:
        return "court docket"
    return "manual seed needed"


def _missing_outcome_reason(expected: Sequence[str], urls: Sequence[str]) -> str:
    if not urls:
        return "no_verified_source_url_text"
    if "docket_docs" in expected:
        return "document_signal_without_outcome_text"
    return "no_deterministic_outcome_terms"


def _profile_row(row: Mapping[str, Any], fixture_index: Optional[Mapping[str, Mapping[str, Any]]] = None) -> CalibrationProfileRow:
    gt = row.get("ground_truth") if isinstance(row.get("ground_truth"), Mapping) else {}
    urls = [str(url) for url in (gt.get("verified_sources") or []) if url]
    expected = [
        ARTIFACT_LABELS[key]
        for key in (*MEDIA_SIGNAL_KEYS, *DOCUMENT_SIGNAL_KEYS)
        if _truthy_signal(gt.get(key))
    ]
    media = [item for item in expected if item in MEDIA_SIGNAL_KEYS]
    source_types: List[str] = []
    for url in urls:
        _append_unique(source_types, _source_type_for_url(url))
    connectors = _connectors_for_source_types(source_types, expected)
    portals = _portal_profiles(connectors, source_types, expected)
    outcome_seed = _outcome_from_fixture(row, fixture_index or {}) or _outcome_from_urls(urls)
    outcome_status = str(outcome_seed["status"]) if outcome_seed else "unknown"
    tier = str(row.get("tier") or "UNKNOWN")
    roles = _benchmark_roles(expected, tier)
    risks = _risk_flags(
        expected=expected,
        source_types=source_types,
        connectors=connectors,
        tier=tier,
        urls=urls,
    )
    supported = bool(set(connectors) & SUPPORTED_LIVE_CONNECTORS)
    notes = [
        f"footage_assessment={gt.get('footage_assessment', 'unknown')}",
        f"primary_source_score={gt.get('primary_source_score', 'unknown')}",
        f"evidence_depth_score={gt.get('evidence_depth_score', 'unknown')}",
    ]
    if "agency_ois" in connectors:
        notes.append("agency/critical-incident portal likely needed for primary-source media")
    if supported and "generic_media_only" in risks:
        notes.append("supported path exists but media quality/relevance needs review")

    return CalibrationProfileRow(
        case_id=int(row.get("case_id")),
        title=str(row.get("defendant_names") or ""),
        jurisdiction=str(row.get("jurisdiction") or ""),
        state=_state_from_jurisdiction(str(row.get("jurisdiction") or "")),
        agency=None,
        tier=tier,
        outcome_status=outcome_status,
        outcome_seed_status=outcome_status,
        outcome_source_field=str(outcome_seed["source_field"]) if outcome_seed else None,
        outcome_confidence=float(outcome_seed["confidence"]) if outcome_seed else 0.0,
        missing_outcome_reason=None if outcome_seed else _missing_outcome_reason(expected, urls),
        recommended_corrob_source=_recommended_corrob_source(source_types, connectors),
        known_source_urls=urls,
        known_media_artifact_signals=media,
        expected_artifact_types=expected,
        expected_media_types=media,
        source_types_already_known=source_types,
        likely_connector_path=connectors,
        needed_portal_profile=portals[0],
        portal_profiles_needed=portals,
        benchmark_role=_primary_role(roles),
        benchmark_roles=roles,
        supported_live_path_available=supported,
        blocker_if_not_supported=_blocker(connectors, risks, expected),
        risk_flags=risks,
        notes=notes,
    )


def _dedupe_rows(rows: Iterable[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    deduped: Dict[int, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping) or "case_id" not in row:
            continue
        case_id = int(row["case_id"])
        deduped.setdefault(case_id, row)
    return [deduped[key] for key in sorted(deduped)]


def _summary(rows: Sequence[CalibrationProfileRow]) -> Dict[str, Any]:
    def count_if(predicate) -> int:
        return sum(1 for row in rows if predicate(row))

    connector_counts: Dict[str, int] = {}
    role_counts: Dict[str, int] = {}
    risk_counts: Dict[str, int] = {}
    outcome_counts: Dict[str, int] = {}
    for row in rows:
        for connector in row.likely_connector_path:
            connector_counts[connector] = connector_counts.get(connector, 0) + 1
        for role in row.benchmark_roles:
            role_counts[role] = role_counts.get(role, 0) + 1
        for risk in row.risk_flags:
            risk_counts[risk] = risk_counts.get(risk, 0) + 1
        outcome_counts[row.outcome_seed_status] = outcome_counts.get(row.outcome_seed_status, 0) + 1

    return {
        "total_cases": len(rows),
        "media_positive_cases": count_if(lambda row: bool(row.expected_media_types)),
        "tier_a_primary_media_cases": count_if(lambda row: "tier_a_primary_media_case" in row.benchmark_roles),
        "document_artifact_cases": count_if(lambda row: "document_artifact_case" in row.benchmark_roles),
        "negative_or_no_artifact_cases": count_if(lambda row: "negative_or_no_artifact_case" in row.benchmark_roles),
        "supported_live_path_cases": count_if(lambda row: row.supported_live_path_available),
        "outcome_seed_ready_cases": count_if(lambda row: row.outcome_seed_status != "unknown"),
        "connector_counts": dict(sorted(connector_counts.items())),
        "benchmark_role_counts": dict(sorted(role_counts.items())),
        "risk_flag_counts": dict(sorted(risk_counts.items())),
        "outcome_seed_status_counts": dict(sorted(outcome_counts.items())),
    }


def profile_calibration_set(
    paths: Optional[Iterable[Path]] = None,
    *,
    repo_root: Optional[Path] = None,
) -> CalibrationProfileReport:
    """Profile the legacy calibration set for CaseGraph replay work."""

    source_paths = list(paths) if paths is not None else default_calibration_paths(repo_root)
    loaded_rows: List[Mapping[str, Any]] = []
    warnings: List[str] = []
    existing_paths: List[str] = []
    for path in source_paths:
        p = Path(path)
        if not p.exists():
            warnings.append(f"missing_calibration_path:{p}")
            continue
        data = _load_json(p)
        if not isinstance(data, list):
            warnings.append(f"calibration_path_not_list:{p}")
            continue
        existing_paths.append(str(p))
        loaded_rows.extend(item for item in data if isinstance(item, Mapping))

    fixture_index = _fixture_outcome_index(repo_root)
    rows = [_profile_row(row, fixture_index) for row in _dedupe_rows(loaded_rows)]
    return CalibrationProfileReport(
        total_cases=len(rows),
        profile_rows=rows,
        source_paths=existing_paths,
        summary=_summary(rows),
        warnings=warnings,
    )
