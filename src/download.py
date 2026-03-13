"""Stage 3B — Asset Download.

Downloads selected assets from the approved discovered-link inventory.
Only runs after link discovery is complete.
Does NOT combine discovery and download.
"""

import os
import re
import time
from urllib.parse import urlparse

import requests

from .logger import get_logger

log = get_logger()


def _safe_filename(url: str, max_length: int = 100) -> str:
    """Generate a safe filename from a URL."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    name = os.path.basename(path) if path else "download"

    # Clean up
    name = re.sub(r"[^\w.\-]", "_", name)
    if len(name) > max_length:
        name = name[:max_length]

    # Ensure it has an extension
    if "." not in name:
        name += ".html"

    return name


def download_asset(
    url: str,
    download_dir: str,
    max_size_mb: int = 100,
    timeout: int = 60,
) -> dict:
    """Download a single asset.

    Returns a dict with: success, local_path, error, size_bytes
    """
    result = {
        "url": url,
        "success": False,
        "local_path": "",
        "error": "",
        "size_bytes": 0,
    }

    try:
        # Stream the download to check size before committing
        with requests.get(url, stream=True, timeout=timeout, allow_redirects=True) as resp:
            resp.raise_for_status()

            # Check content-length if available
            content_length = resp.headers.get("Content-Length")
            if content_length and int(content_length) > max_size_mb * 1024 * 1024:
                result["error"] = f"File too large: {int(content_length)} bytes (max {max_size_mb}MB)"
                log.warning("Skipping large file %s: %s", url, result["error"])
                return result

            filename = _safe_filename(url)

            # Check for content-disposition header for a better filename
            cd = resp.headers.get("Content-Disposition", "")
            if "filename=" in cd:
                fn_match = re.search(r'filename="?([^";\n]+)"?', cd)
                if fn_match:
                    filename = re.sub(r"[^\w.\-]", "_", fn_match.group(1))

            filepath = os.path.join(download_dir, filename)

            # Avoid overwriting
            if os.path.exists(filepath):
                base, ext = os.path.splitext(filename)
                counter = 1
                while os.path.exists(filepath):
                    filepath = os.path.join(download_dir, f"{base}_{counter}{ext}")
                    counter += 1

            total = 0
            max_bytes = max_size_mb * 1024 * 1024

            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    total += len(chunk)
                    if total > max_bytes:
                        result["error"] = f"Download exceeded max size ({max_size_mb}MB)"
                        log.warning("Download aborted for %s: exceeded size limit", url)
                        f.close()
                        os.remove(filepath)
                        return result
                    f.write(chunk)

            result["success"] = True
            result["local_path"] = filepath
            result["size_bytes"] = total
            log.info("Downloaded: %s → %s (%d bytes)", url, filepath, total)

    except requests.exceptions.Timeout:
        result["error"] = f"Timeout after {timeout}s"
        log.warning("Download timeout for %s", url)
    except requests.exceptions.HTTPError as e:
        result["error"] = f"HTTP error: {e.response.status_code}"
        log.warning("Download HTTP error for %s: %s", url, e)
    except Exception as e:
        result["error"] = str(e)
        log.error("Download failed for %s: %s", url, e)

    return result


def download_from_inventory(
    inventory: dict,
    download_dir: str,
    max_size_mb: int = 100,
    timeout: int = 60,
    rate_limit: float = 2.0,
) -> tuple[int, int, list[dict]]:
    """Download all recommended assets from a link inventory.

    Returns (success_count, failure_count, download_results).
    """
    links = inventory.get("links", [])
    recommended = [link for link in links if link.get("download_recommended", False)]

    if not recommended:
        log.info("No links marked for download in inventory for %s", inventory.get("case_id", ""))
        return 0, 0, []

    log.info(
        "Downloading %d of %d links for %s",
        len(recommended),
        len(links),
        inventory.get("case_id", ""),
    )

    os.makedirs(download_dir, exist_ok=True)

    successes = 0
    failures = 0
    results = []

    for link in recommended:
        url = link.get("url", "")
        if not url:
            continue

        result = download_asset(url, download_dir, max_size_mb, timeout)
        results.append(result)

        if result["success"]:
            successes += 1
            # Update the link entry with download info
            link["download_attempted"] = True
            link["download_success"] = True
            link["local_path"] = result["local_path"]
        else:
            failures += 1
            link["download_attempted"] = True
            link["download_success"] = False

        time.sleep(rate_limit)

    log.info(
        "Downloads complete for %s: %d success, %d failed",
        inventory.get("case_id", ""),
        successes,
        failures,
    )

    return successes, failures, results
