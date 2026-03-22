"""Quality analysis for the Sunshine-Gated Closed-Case Pipeline.

Reads pipeline output (Google Sheets rows + links_inventory.json files) and
runs structured quality checks. Returns a JSON-serializable report with
pass/fail per check, counts, and specific case IDs for remediation.
"""

import json
import os
from datetime import datetime
from urllib.parse import urlparse

from .logger import get_logger
from .models import ValidationStatus

log = get_logger()

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

DEFAULT_THRESHOLDS = {
    "max_manual_review_rate": 0.15,
    "max_bad_names": 0,
    "max_duplicate_operations": 0,
    "max_fake_bwc_links": 0,
    "max_invalid_portal_urls": 0,
    "max_miscategorized_links": 0,
    "max_llm_resurrections": 0,
}

# ---------------------------------------------------------------------------
# Video-hosting domains (URLs here can legitimately be BWC footage)
# ---------------------------------------------------------------------------

VIDEO_HOSTING_DOMAINS = [
    "youtube.com", "youtu.be", "vimeo.com", "dailymotion.com",
    "rumble.com", "bitchute.com",
]

VIDEO_FILE_EXTENSIONS = [".mp4", ".m3u8", ".webm", ".avi", ".mov", ".mkv"]

# Patterns in URLs that indicate a video on a police/government site
VIDEO_URL_PATTERNS = ["/video/", "/media/", "/videos/", "/watch/", "/player/"]

# ---------------------------------------------------------------------------
# Bad-name patterns
# ---------------------------------------------------------------------------

SINGLE_WORD_EXCEPTIONS = {"unknown"}  # tolerated single-word names

OFFICER_TITLE_WORDS = [
    "officer", "deputy", "sergeant", "lieutenant", "captain", "chief",
    "detective", "corporal", "trooper", "commander", "inspector", "marshal",
    "sheriff", "agent", "director",
]

GENERIC_BAD_NAMES = [
    "unknown", "suspect", "n/a", "none", "tbd", "john doe", "jane doe",
    "unidentified", "not identified", "pending",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_video_hosting_url(url: str) -> bool:
    """Check if URL is on a known video-hosting domain or has video file extension."""
    url_lower = url.lower()
    parsed = urlparse(url_lower)
    domain = parsed.netloc

    # Check video hosting domains
    for vd in VIDEO_HOSTING_DOMAINS:
        if vd in domain:
            return True

    # Check video file extensions
    for ext in VIDEO_FILE_EXTENSIONS:
        if url_lower.endswith(ext):
            return True

    # Check police/gov sites with video paths
    if any(d in domain for d in [".gov", "sheriff", "police", "pd.org"]):
        if any(p in url_lower for p in VIDEO_URL_PATTERNS):
            return True

    return False


def _load_all_inventories(storage) -> dict[str, dict]:
    """Load all links_inventory.json files. Returns {folder_name: inventory_dict}."""
    inventories = {}

    # Check 02_Validated-Closed subfolders
    validated_dir = storage.validated_closed
    if not os.path.isdir(validated_dir):
        return inventories

    for folder_name in os.listdir(validated_dir):
        inv_path = os.path.join(validated_dir, folder_name, "links_inventory.json")
        if os.path.isfile(inv_path):
            try:
                with open(inv_path, "r", encoding="utf-8") as f:
                    inventories[folder_name] = json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                log.warning("Failed to read inventory %s: %s", inv_path, e)

    return inventories


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_manual_review_rate(rows: list[dict], threshold: float) -> dict:
    """Check what fraction of cases ended up in manual_review."""
    total = len(rows)
    if total == 0:
        return {"passed": True, "value": 0.0, "threshold": threshold, "cases": []}

    mr_cases = [r for r in rows if r.get("validation_status") == ValidationStatus.MANUAL_REVIEW.value]
    rate = len(mr_cases) / total

    return {
        "passed": rate <= threshold,
        "value": round(rate, 4),
        "threshold": threshold,
        "count": len(mr_cases),
        "cases": [
            {"case_id": r.get("case_id", ""), "reason": r.get("manual_review_reason", "")}
            for r in mr_cases
        ],
    }


def _check_bad_names(rows: list[dict]) -> dict:
    """Check for officer names, single-word names, or generic names in validated cases."""
    validated = [r for r in rows if r.get("validation_status") in (
        ValidationStatus.VALIDATED_CLOSED.value,
        ValidationStatus.LINKS_DISCOVERED.value,
        ValidationStatus.DOWNLOADS_COMPLETED.value,
    )]

    flagged = []
    for row in validated:
        name = row.get("suspect_name", "").strip()
        if not name:
            continue

        name_lower = name.lower()
        agency = row.get("agency_name", "").lower()
        reason = None

        # Single-word name (no space)
        if " " not in name and name_lower not in SINGLE_WORD_EXCEPTIONS:
            reason = "single_word_name"

        # Generic/placeholder name
        elif name_lower in GENERIC_BAD_NAMES:
            reason = "generic_name"

        # Name starts with officer title
        elif any(name_lower.startswith(t) for t in OFFICER_TITLE_WORDS):
            reason = "officer_title_prefix"

        # Name looks like operation name
        elif name_lower.startswith("operation "):
            reason = "operation_name"

        # Name is substring of agency name (e.g. "Jacksonville" from "Jacksonville Sheriff's Office")
        elif len(name) > 3 and name_lower in agency:
            reason = "agency_name_substring"

        if reason:
            flagged.append({
                "case_id": row.get("case_id", ""),
                "suspect_name": name,
                "reason": reason,
            })

    return {
        "passed": len(flagged) == 0,
        "count": len(flagged),
        "cases": flagged,
    }


def _check_duplicate_operations(rows: list[dict]) -> dict:
    """Check for the same operation queried multiple times with different queries."""
    # Group by normalized operation name
    ops: dict[str, list[dict]] = {}
    for row in rows:
        name = row.get("suspect_name", "").strip()
        if not name.lower().startswith("operation "):
            continue
        key = name.lower().strip()
        ops.setdefault(key, []).append(row)

    flagged = []
    for op_name, op_rows in ops.items():
        if len(op_rows) <= 1:
            continue
        # Check if they had different validation queries (indicating redundant work)
        queries = set(r.get("validation_query", "") for r in op_rows)
        if len(queries) > 1:
            flagged.append({
                "operation_name": op_name,
                "count": len(op_rows),
                "case_ids": [r.get("case_id", "") for r in op_rows],
                "queries": list(queries),
            })

    return {
        "passed": len(flagged) == 0,
        "count": len(flagged),
        "groups": flagged,
    }


def _check_fake_bwc_links(inventories: dict[str, dict]) -> dict:
    """Check for links tagged bwc_video that are actually news articles."""
    flagged = []

    for folder_name, inv in inventories.items():
        case_id = inv.get("case_id", folder_name)
        for link in inv.get("links", []):
            if link.get("link_type") != "bwc_video":
                continue
            url = link.get("url", "")
            if not _is_video_hosting_url(url):
                flagged.append({
                    "case_id": case_id,
                    "url": url,
                    "actual_domain": urlparse(url).netloc,
                })

    return {
        "passed": len(flagged) == 0,
        "count": len(flagged),
        "cases": flagged,
    }


def _check_invalid_portal_urls(inventories: dict[str, dict]) -> dict:
    """Check for [direct-url] links that were never HTTP-validated."""
    flagged = []

    for folder_name, inv in inventories.items():
        case_id = inv.get("case_id", folder_name)
        for link in inv.get("links", []):
            notes = link.get("notes", "")
            if "[direct-url]" in notes:
                flagged.append({
                    "case_id": case_id,
                    "url": link.get("url", ""),
                    "notes": notes,
                })

    return {
        "passed": len(flagged) == 0,
        "count": len(flagged),
        "cases": flagged,
    }


def _check_miscategorized_links(inventories: dict[str, dict]) -> dict:
    """Check for links where link_type is inconsistent with URL domain."""
    # Court/legal link types that should only appear on .gov / clerk / court domains
    LEGAL_TYPES = {"court_docket", "sentencing_order", "judgment", "indictment", "minute_entry"}
    LEGAL_DOMAIN_MARKERS = [".gov", "clerk.", "court", "judiciary.", "docket", "caseinfo"]

    flagged = []

    for folder_name, inv in inventories.items():
        case_id = inv.get("case_id", folder_name)
        for link in inv.get("links", []):
            link_type = link.get("link_type", "")
            url = link.get("url", "").lower()

            if link_type in LEGAL_TYPES:
                is_legal_domain = any(m in url for m in LEGAL_DOMAIN_MARKERS)
                if not is_legal_domain:
                    flagged.append({
                        "case_id": case_id,
                        "url": link.get("url", ""),
                        "link_type": link_type,
                        "reason": f"{link_type} on non-legal domain",
                    })

    return {
        "passed": len(flagged) == 0,
        "count": len(flagged),
        "cases": flagged,
    }


def _check_llm_resurrection(rows: list[dict]) -> dict:
    """Heuristic check for names that were rejected then reappeared downstream.

    Looks for validated cases whose suspect_name appears in a rejected case's
    manual_review_reason or validation_note, suggesting the name was filtered
    once then accepted elsewhere.
    """
    # Build set of rejected names
    rejected_names = set()
    for row in rows:
        status = row.get("validation_status", "")
        if status == ValidationStatus.REJECTED_OPEN_OR_UNCONFIRMED.value:
            name = row.get("suspect_name", "").strip().lower()
            if name:
                rejected_names.add(name)

    # Check validated cases against rejected names
    flagged = []
    validated = [r for r in rows if r.get("validation_status") in (
        ValidationStatus.VALIDATED_CLOSED.value,
        ValidationStatus.LINKS_DISCOVERED.value,
        ValidationStatus.DOWNLOADS_COMPLETED.value,
    )]

    for row in validated:
        name = row.get("suspect_name", "").strip().lower()
        if name in rejected_names:
            flagged.append({
                "case_id": row.get("case_id", ""),
                "suspect_name": row.get("suspect_name", ""),
                "reason": "name_also_rejected_in_another_case",
            })

    return {
        "passed": len(flagged) == 0,
        "count": len(flagged),
        "cases": flagged,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def analyze_pipeline_quality(sheet, storage, thresholds: dict = None) -> dict:
    """Run all quality checks and return a structured report.

    Args:
        sheet: SheetRegistry instance (or None to skip sheet-based checks).
        storage: PipelineStorage instance.
        thresholds: Override DEFAULT_THRESHOLDS values.

    Returns:
        JSON-serializable dict with pass/fail per check and actionable findings.
    """
    t = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    log.info("Running pipeline quality analysis...")

    # Load data once
    rows = sheet.get_all_rows() if sheet else []
    inventories = _load_all_inventories(storage)

    # Run checks
    checks = {
        "manual_review_rate": _check_manual_review_rate(rows, t["max_manual_review_rate"]),
        "bad_names": _check_bad_names(rows),
        "duplicate_operations": _check_duplicate_operations(rows),
        "fake_bwc_links": _check_fake_bwc_links(inventories),
        "invalid_portal_urls": _check_invalid_portal_urls(inventories),
        "miscategorized_links": _check_miscategorized_links(inventories),
        "llm_resurrection": _check_llm_resurrection(rows),
    }

    # Summary stats
    total = len(rows)
    by_status = {}
    for row in rows:
        s = row.get("validation_status", "unknown")
        by_status[s] = by_status.get(s, 0) + 1

    # Build actionable findings
    findings = []
    for name, result in checks.items():
        if not result["passed"]:
            count = result.get("count", 0)
            findings.append(f"{name}: {count} issue(s) found")

    passed = all(c["passed"] for c in checks.values())

    report = {
        "passed": passed,
        "timestamp": datetime.utcnow().isoformat(),
        "summary": {
            "total_cases": total,
            "by_status": by_status,
            "inventories_loaded": len(inventories),
        },
        "checks": checks,
        "actionable_findings": findings,
    }

    log.info("Quality analysis complete: %s (%d findings)",
             "PASSED" if passed else "FAILED", len(findings))

    return report


def save_quality_report(report: dict, storage, filename: str = "quality_report.json") -> str:
    """Write quality report as JSON to the pipeline root."""
    path = os.path.join(storage.root, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    # Also write human-readable summary
    txt_path = path.replace(".json", ".txt")
    lines = [
        f"Pipeline Quality Report — {report['timestamp']}",
        f"Overall: {'PASSED' if report['passed'] else 'FAILED'}",
        "",
        "Summary:",
        f"  Total cases: {report['summary']['total_cases']}",
        f"  Inventories loaded: {report['summary']['inventories_loaded']}",
    ]
    for status, count in report["summary"].get("by_status", {}).items():
        lines.append(f"  {status}: {count}")

    lines.append("")
    lines.append("Checks:")
    for name, result in report["checks"].items():
        status = "PASS" if result["passed"] else "FAIL"
        count = result.get("count", result.get("value", ""))
        lines.append(f"  {name}: {status} ({count})")

    if report["actionable_findings"]:
        lines.append("")
        lines.append("Actionable Findings:")
        for f in report["actionable_findings"]:
            lines.append(f"  - {f}")

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    log.info("Quality report saved to %s", path)
    return path
