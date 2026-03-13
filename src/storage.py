"""Local filesystem storage for the Sunshine-Gated Closed-Case Pipeline."""

import json
import os
import re
from typing import Optional

from .logger import get_logger


def _safe_folder_name(value: str) -> str:
    """Sanitize a string for use as a folder name."""
    value = value.strip().replace(" ", "-")
    value = re.sub(r"[^\w\-]", "", value)
    return value or "Unknown"


def make_case_folder_name(
    state: str,
    suspect_name: str = "",
    disposition: str = "",
    year: str = "",
    video_id: str = "",
) -> str:
    """Build a deterministic folder name: {State}_{LastName}_{Disposition}_{Year}.

    Falls back to video_id if suspect_name is missing.
    """
    state_part = _safe_folder_name(state.upper()) if state else "XX"

    if suspect_name:
        parts = suspect_name.strip().split()
        last_name = _safe_folder_name(parts[-1]) if parts else "Unknown"
    elif video_id:
        last_name = f"vid-{_safe_folder_name(video_id)}"
    else:
        last_name = "Unknown"

    disp_part = _safe_folder_name(disposition) if disposition else "Sentenced"
    year_part = _safe_folder_name(year) if year else "NoYear"

    return f"{state_part}_{last_name}_{disp_part}_{year_part}"


class PipelineStorage:
    """Manages local folder structure for the pipeline."""

    def __init__(self, pipeline_root: str = "./CrimeDoc-Pipeline"):
        self.root = os.path.abspath(pipeline_root)
        self.raw_candidates = os.path.join(self.root, "01_Raw-Candidates")
        self.validated_closed = os.path.join(self.root, "02_Validated-Closed")
        self.link_inventories = os.path.join(self.root, "03_Link-Inventories")
        self.downloaded_assets = os.path.join(self.root, "04_Downloaded-Assets")
        self.manual_review = os.path.join(self.root, "05_Manual-Review")
        self.log = get_logger()

    def init_dirs(self):
        """Create top-level pipeline directories."""
        for d in [
            self.raw_candidates,
            self.validated_closed,
            self.link_inventories,
            self.downloaded_assets,
            self.manual_review,
        ]:
            os.makedirs(d, exist_ok=True)
        self.log.info("Pipeline directories initialized at %s", self.root)

    # --- Raw Candidate ---

    def save_raw_candidate(self, case_id: str, data: dict) -> str:
        """Save raw candidate data as JSON. Returns the folder path."""
        folder = os.path.join(self.raw_candidates, _safe_folder_name(case_id))
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, "candidate_data.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        self.log.debug("Saved raw candidate: %s", path)
        return folder

    # --- Validated Case ---

    def create_validated_folder(self, folder_name: str) -> str:
        """Create a folder under 02_Validated-Closed/. Returns the folder path."""
        folder = os.path.join(self.validated_closed, folder_name)
        os.makedirs(folder, exist_ok=True)
        os.makedirs(os.path.join(folder, "downloads"), exist_ok=True)
        self.log.info("Created validated case folder: %s", folder)
        return folder

    def write_validation_log(
        self,
        folder_path: str,
        case_id: str,
        video_url: str,
        validation_query: str,
        validation_source_url: str,
        explanation: str,
        confirmation_type: str = "local_news_only",
    ) -> str:
        """Write validation_log.txt for a validated case."""
        log_path = os.path.join(folder_path, "validation_log.txt")
        content = (
            f"case_id: {case_id}\n"
            f"source_video_url: {video_url}\n"
            f"validation_date: {__import__('datetime').datetime.utcnow().isoformat()}\n"
            f"validation_query: {validation_query}\n"
            f"validation_source_url: {validation_source_url}\n"
            f"explanation: {explanation}\n"
            f"confirmation_type: {confirmation_type}\n"
        )
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(content)
        self.log.debug("Wrote validation log: %s", log_path)
        return log_path

    def write_links_inventory(self, folder_path: str, inventory: dict) -> str:
        """Write links_inventory.json for a validated case."""
        path = os.path.join(folder_path, "links_inventory.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(inventory, f, indent=2, ensure_ascii=False)
        self.log.debug("Wrote links inventory: %s", path)
        return path

    def copy_links_inventory(self, case_id: str, inventory: dict) -> str:
        """Write a copy of links inventory to 03_Link-Inventories/ for quick browsing."""
        path = os.path.join(self.link_inventories, f"{_safe_folder_name(case_id)}_links.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(inventory, f, indent=2, ensure_ascii=False)
        return path

    def get_downloads_dir(self, folder_path: str) -> str:
        """Return the downloads/ subdirectory for a validated case."""
        d = os.path.join(folder_path, "downloads")
        os.makedirs(d, exist_ok=True)
        return d

    # --- Manual Review ---

    def save_manual_review(
        self,
        case_id: str,
        promising: str,
        ambiguous: str,
        failed_queries: str,
        improvement_notes: str,
    ) -> str:
        """Write manual_review_note.txt. Returns the folder path."""
        folder = os.path.join(self.manual_review, _safe_folder_name(case_id))
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, "manual_review_note.txt")
        content = (
            f"case_id: {case_id}\n"
            f"date: {__import__('datetime').datetime.utcnow().isoformat()}\n\n"
            f"WHAT LOOKED PROMISING:\n{promising}\n\n"
            f"WHAT WAS AMBIGUOUS:\n{ambiguous}\n\n"
            f"QUERY PATTERNS THAT FAILED:\n{failed_queries}\n\n"
            f"NOTES FOR FUTURE PROMPT IMPROVEMENT:\n{improvement_notes}\n"
        )
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        self.log.info("Saved manual review note: %s", path)
        return folder

    def relative_path(self, absolute_path: str) -> str:
        """Convert an absolute path to a path relative to pipeline root."""
        try:
            return os.path.relpath(absolute_path, self.root)
        except ValueError:
            return absolute_path
