"""Remediation functions for the Sunshine-Gated Closed-Case Pipeline.

Each function takes a quality report, fixes the identified issues in the
sheet/inventory data, and returns which pipeline stages need to be re-run.
"""

import json
import os
import time
from urllib.parse import urlparse

import requests

from .analyze import VIDEO_HOSTING_DOMAINS, VIDEO_FILE_EXTENSIONS, VIDEO_URL_PATTERNS
from .logger import get_logger
from .models import SourceRank, ValidationStatus

log = get_logger()


# ---------------------------------------------------------------------------
# Individual remediations
# ---------------------------------------------------------------------------

def remediate_bad_names(report: dict, sheet, storage, config: dict) -> list[str]:
    """Reset cases with bad names back to new_candidate for re-validation."""
    check = report["checks"].get("bad_names", {})
    if check.get("passed", True):
        return []

    cases = check.get("cases", [])
    if not cases:
        return []

    log.info("Remediating %d bad-name cases", len(cases))

    for item in cases:
        case_id = item["case_id"]
        try:
            sheet.update_case(case_id, {
                "validation_status": ValidationStatus.NEW_CANDIDATE.value,
                "suspect_name": "",
                "validation_note": f"[auto-remediated] bad name removed: {item['suspect_name']} ({item['reason']})",
            })
            log.info("Reset case %s (bad name: %s)", case_id, item["suspect_name"])
        except Exception as e:
            log.error("Failed to remediate bad name for %s: %s", case_id, e)

    return ["validate"]


def remediate_duplicate_operations(report: dict, sheet, storage, config: dict) -> list[str]:
    """Mark duplicate operation entries with dedup notes."""
    check = report["checks"].get("duplicate_operations", {})
    if check.get("passed", True):
        return []

    groups = check.get("groups", [])
    if not groups:
        return []

    log.info("Remediating %d duplicate operation groups", len(groups))

    for group in groups:
        case_ids = group["case_ids"]
        primary = case_ids[0]
        for dup_id in case_ids[1:]:
            try:
                sheet.update_case(dup_id, {
                    "validation_note": f"[auto-remediated] dedup: shares results with {primary}",
                })
                log.info("Marked %s as duplicate of %s", dup_id, primary)
            except Exception as e:
                log.error("Failed to dedup %s: %s", dup_id, e)

    return []


def remediate_fake_bwc_links(report: dict, sheet, storage, config: dict) -> list[str]:
    """Reclassify fake BWC links as news_article in inventory files."""
    check = report["checks"].get("fake_bwc_links", {})
    if check.get("passed", True):
        return []

    cases = check.get("cases", [])
    if not cases:
        return []

    log.info("Remediating %d fake BWC links", len(cases))

    # Group flagged URLs by case for batch inventory rewrites
    flagged_by_case: dict[str, set[str]] = {}
    for item in cases:
        flagged_by_case.setdefault(item["case_id"], set()).add(item["url"])

    _rewrite_inventories(storage, flagged_by_case, _fix_fake_bwc)
    return []


def _fix_fake_bwc(link: dict, flagged_urls: set[str]) -> bool:
    """Fix a single fake BWC link. Returns True if modified."""
    if link.get("url") in flagged_urls and link.get("link_type") == "bwc_video":
        link["link_type"] = "news_article"
        link["download_recommended"] = False
        link["notes"] = link.get("notes", "") + " [auto-reclassified: bwc_video→news_article]"
        return True
    return False


def remediate_invalid_portal_urls(report: dict, sheet, storage, config: dict) -> list[str]:
    """HTTP HEAD each [direct-url] link; remove dead ones, validate live ones."""
    check = report["checks"].get("invalid_portal_urls", {})
    if check.get("passed", True):
        return []

    cases = check.get("cases", [])
    if not cases:
        return []

    log.info("Validating %d portal URLs via HTTP HEAD", len(cases))

    # Test each URL and categorize
    live_urls = set()
    dead_urls = set()

    for item in cases:
        url = item["url"]
        try:
            resp = requests.head(url, timeout=5, allow_redirects=True,
                                 headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code < 400:
                live_urls.add(url)
                log.info("Portal URL alive (%d): %s", resp.status_code, url)
            else:
                dead_urls.add(url)
                log.info("Portal URL dead (%d): %s", resp.status_code, url)
        except (requests.RequestException, OSError) as e:
            dead_urls.add(url)
            log.info("Portal URL unreachable: %s (%s)", url, e)
        time.sleep(0.5)  # be polite

    # Group by case for inventory rewrites
    all_urls_by_case: dict[str, dict[str, str]] = {}  # case_id -> {url: "live"|"dead"}
    for item in cases:
        case_id = item["case_id"]
        url = item["url"]
        status = "live" if url in live_urls else "dead"
        all_urls_by_case.setdefault(case_id, {})[url] = status

    # Rewrite inventories
    validated_dir = storage.validated_closed
    if not os.path.isdir(validated_dir):
        return []

    for folder_name in os.listdir(validated_dir):
        inv_path = os.path.join(validated_dir, folder_name, "links_inventory.json")
        if not os.path.isfile(inv_path):
            continue

        try:
            with open(inv_path, "r", encoding="utf-8") as f:
                inv = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        case_id = inv.get("case_id", folder_name)
        url_statuses = all_urls_by_case.get(case_id)
        if not url_statuses:
            continue

        modified = False
        new_links = []
        for link in inv.get("links", []):
            url = link.get("url", "")
            notes = link.get("notes", "")

            if "[direct-url]" not in notes or url not in url_statuses:
                new_links.append(link)
                continue

            if url_statuses[url] == "live":
                link["notes"] = notes.replace("[direct-url]", "[validated-url]")
                new_links.append(link)
                modified = True
            else:
                # Remove dead links
                modified = True
                log.info("Removed dead portal URL from %s: %s", case_id, url)

        if modified:
            inv["links"] = new_links
            with open(inv_path, "w", encoding="utf-8") as f:
                json.dump(inv, f, indent=2, ensure_ascii=False)

    return []


def remediate_miscategorized_links(report: dict, sheet, storage, config: dict) -> list[str]:
    """Re-classify links where link_type is inconsistent with URL domain."""
    check = report["checks"].get("miscategorized_links", {})
    if check.get("passed", True):
        return []

    cases = check.get("cases", [])
    if not cases:
        return []

    log.info("Remediating %d miscategorized links", len(cases))

    flagged_by_case: dict[str, set[str]] = {}
    for item in cases:
        flagged_by_case.setdefault(item["case_id"], set()).add(item["url"])

    _rewrite_inventories(storage, flagged_by_case, _fix_miscategorized)
    return []


def _fix_miscategorized(link: dict, flagged_urls: set[str]) -> bool:
    """Reclassify a miscategorized link to news_article. Returns True if modified."""
    if link.get("url") in flagged_urls:
        old_type = link.get("link_type", "")
        link["link_type"] = "news_article"
        link["notes"] = link.get("notes", "") + f" [auto-reclassified: {old_type}→news_article]"
        return True
    return False


# ---------------------------------------------------------------------------
# Shared inventory rewrite helper
# ---------------------------------------------------------------------------

def _rewrite_inventories(storage, flagged_by_case: dict[str, set[str]], fix_fn) -> None:
    """Iterate inventory files and apply fix_fn to flagged links."""
    validated_dir = storage.validated_closed
    if not os.path.isdir(validated_dir):
        return

    for folder_name in os.listdir(validated_dir):
        inv_path = os.path.join(validated_dir, folder_name, "links_inventory.json")
        if not os.path.isfile(inv_path):
            continue

        try:
            with open(inv_path, "r", encoding="utf-8") as f:
                inv = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        case_id = inv.get("case_id", folder_name)
        flagged_urls = flagged_by_case.get(case_id)
        if not flagged_urls:
            continue

        modified = False
        for link in inv.get("links", []):
            if fix_fn(link, flagged_urls):
                modified = True

        if modified:
            with open(inv_path, "w", encoding="utf-8") as f:
                json.dump(inv, f, indent=2, ensure_ascii=False)
            log.info("Rewrote inventory for %s", case_id)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

REMEDIATIONS = {
    "bad_names": remediate_bad_names,
    "duplicate_operations": remediate_duplicate_operations,
    "fake_bwc_links": remediate_fake_bwc_links,
    "invalid_portal_urls": remediate_invalid_portal_urls,
    "miscategorized_links": remediate_miscategorized_links,
    # manual_review_rate: intentionally NOT auto-remediated
    # llm_resurrection: informational only, needs code-level fix
}


def run_remediations(report: dict, sheet, storage, config: dict) -> list[str]:
    """Apply all applicable remediations. Returns list of stages to re-run.

    Only remediates checks that failed. Skips manual_review_rate and
    llm_resurrection (these are informational and require human/code-level fixes).
    """
    stages_to_rerun = set()

    for check_name, fn in REMEDIATIONS.items():
        check = report["checks"].get(check_name, {})
        if check.get("passed", True):
            continue

        log.info("Running remediation: %s", check_name)
        try:
            stages = fn(report, sheet, storage, config)
            stages_to_rerun.update(stages)
        except Exception as e:
            log.error("Remediation %s failed: %s", check_name, e)

    return sorted(stages_to_rerun)
