"""CLI orchestrator for the Sunshine-Gated Closed-Case Pipeline.

Usage:
    python -m src.main --full             # Run entire pipeline
    python -m src.main --intake-only      # Only ingest new YouTube uploads
    python -m src.main --validate-only    # Only validate existing new_candidates
    python -m src.main --discover-only    # Only run link discovery on validated cases
    python -m src.main --download-only    # Only download from existing link inventories
"""

import argparse
import json
import os
import sys
from dataclasses import asdict

import yaml

from .dedup import deduplicate_candidates
from .discovery import run_discovery
from .download import download_from_inventory
from .intake import (
    run_intake, _extract_name_regex, _extract_name_llm, _extract_date_regex,
    _generate_case_id, classify_video_content,
)
from .logger import get_logger, setup_logger
from .models import (
    CaseCandidate,
    ChannelConfig,
    CorroborationStatus,
    ValidationStatus,
)
from .sheet import SheetRegistry
from .storage import PipelineStorage, make_case_folder_name
from .validation import validate_case


def load_config(config_path: str = None) -> dict:
    """Load settings from YAML config file."""
    if config_path and os.path.exists(config_path):
        path = config_path
    elif os.path.exists("config/settings.local.yaml"):
        path = "config/settings.local.yaml"
    elif os.path.exists("config/settings.yaml"):
        path = "config/settings.yaml"
    else:
        print("ERROR: No config file found. Copy config/settings.yaml to config/settings.local.yaml and fill in API keys.")
        sys.exit(1)

    with open(path, "r") as f:
        config = yaml.safe_load(f)

    return config


def load_channels(channels_path: str = "config/channels.yaml") -> list[ChannelConfig]:
    """Load channel directory from YAML."""
    with open(channels_path, "r") as f:
        data = yaml.safe_load(f)

    channels = []
    for ch in data.get("channels", []):
        channels.append(ChannelConfig(
            handle=ch["handle"],
            agency_name=ch["agency_name"],
            state=ch["state"],
            city=ch.get("city", ""),
            channel_id=ch.get("channel_id"),
            uploads_playlist_id=ch.get("uploads_playlist_id"),
        ))
    return channels


def stage_intake(config: dict, channels: list[ChannelConfig], sheet: SheetRegistry, storage: PipelineStorage) -> list[CaseCandidate]:
    """Stage 1: YouTube intake."""
    log = get_logger()
    log.info("=" * 60)
    log.info("STAGE 1 — YOUTUBE INTAKE")
    log.info("=" * 60)

    # Get existing video IDs for dedup
    existing_video_ids = sheet.get_existing_video_ids()
    existing_cases = sheet.get_existing_cases()
    log.info("Existing records: %d video IDs, %d cases", len(existing_video_ids), len(existing_cases))

    # Incremental write callback — dedup + save + write per channel
    all_new_candidates = []

    def _on_channel_complete(channel_candidates):
        new, dupes = deduplicate_candidates(channel_candidates, existing_video_ids, existing_cases)
        if dupes:
            log.info("Dedup: %d new, %d duplicates skipped", len(new), len(dupes))
        for c in new:
            storage.save_raw_candidate(c.case_id, asdict(c))
            # Track video IDs so later channels dedup against earlier ones
            existing_video_ids.add(c.video_id)
        if new:
            sheet.append_cases_batch(new)
            log.info("Wrote %d candidates to sheet", len(new))
        all_new_candidates.extend(new)

    # Run intake with incremental writes
    run_intake(
        youtube_api_key=config["youtube_api_key"],
        channels=channels,
        max_videos_per_channel=config.get("max_videos_per_channel", 50),
        rate_limit=1.0 / config.get("youtube_requests_per_second", 1),
        openrouter_api_key=config.get("openrouter_api_key", ""),
        openrouter_model=config.get("openrouter_model_extraction", "google/gemini-flash-1.5"),
        openrouter_base_url=config.get("openrouter_base_url", "https://openrouter.ai/api/v1"),
        max_total_videos=config.get("max_total_videos", 100),
        video_published_before=config.get("video_published_before", ""),
        video_published_after=config.get("video_published_after", ""),
        on_channel_complete=_on_channel_complete,
    )

    log.info("Stage 1 complete: %d new candidates ingested", len(all_new_candidates))
    return all_new_candidates


def stage_validate(config: dict, sheet: SheetRegistry, storage: PipelineStorage, candidates: list[CaseCandidate] = None):
    """Stage 2: Cheap validation gate."""
    log = get_logger()
    log.info("=" * 60)
    log.info("STAGE 2 — CHEAP VALIDATION GATE")
    log.info("=" * 60)

    # If no candidates passed in, load new_candidate rows from sheet
    if candidates is None:
        rows = sheet.find_cases_by_status(ValidationStatus.NEW_CANDIDATE.value)
        candidates = []
        for r in rows:
            candidates.append(CaseCandidate(**{
                k: r.get(k, "") for k in CaseCandidate.__dataclass_fields__
            }))
        log.info("Loaded %d new_candidate cases from sheet", len(candidates))

    validated = []
    rejected = 0
    manual = 0

    for c in candidates:
        log.info("Validating: %s (name=%s)", c.case_id, c.suspect_name or "(none)")

        result = validate_case(
            candidate=c,
            brave_api_key=config["brave_api_key"],
            openrouter_api_key=config.get("openrouter_api_key", ""),
            openrouter_model=config.get("openrouter_model_validation", "google/gemini-flash-1.5"),
            openrouter_base_url=config.get("openrouter_base_url", "https://openrouter.ai/api/v1"),
            max_queries=config.get("max_brave_queries_per_case", 3),
        )

        # Update candidate fields
        c.validation_status = result.status
        c.validation_query = result.query_used
        c.validation_note = result.note
        c.validation_source_url = result.source_url
        c.source_rank_used = result.source_rank

        if result.status == ValidationStatus.VALIDATED_CLOSED.value:
            validated.append(c)

            # Determine confirmation type
            if result.source_rank in ["court_gov", "county_clerk"]:
                confirmation_type = "official_record"
            else:
                confirmation_type = "local_news_only"

            # Create validated case folder
            year = c.published_at[:4] if c.published_at else ""
            folder_name = make_case_folder_name(c.state, c.suspect_name, "Sentenced", year, c.video_id)
            folder_path = storage.create_validated_folder(folder_name)
            c.local_case_folder = storage.relative_path(folder_path)

            # Write validation log
            storage.write_validation_log(
                folder_path=folder_path,
                case_id=c.case_id,
                video_url=c.video_url,
                validation_query=result.query_used,
                validation_source_url=result.source_url,
                explanation=result.note,
                confirmation_type=confirmation_type,
            )

        elif result.status == ValidationStatus.MANUAL_REVIEW.value:
            manual += 1
            c.manual_review_reason = result.manual_review_reason

            # Save manual review note
            storage.save_manual_review(
                case_id=c.case_id,
                promising=f"Suspect: {c.suspect_name}, Keywords: {c.case_keywords}",
                ambiguous=result.note,
                failed_queries=result.query_used,
                improvement_notes=result.manual_review_reason,
            )

        else:
            rejected += 1

        # Update sheet
        sheet.update_case(c.case_id, {
            "validation_status": c.validation_status,
            "validation_query": c.validation_query,
            "validation_note": c.validation_note[:500],
            "validation_source_url": c.validation_source_url,
            "source_rank_used": c.source_rank_used,
            "manual_review_reason": c.manual_review_reason,
            "local_case_folder": c.local_case_folder,
        })

    log.info(
        "Stage 2 complete: %d validated, %d rejected, %d manual review",
        len(validated), rejected, manual,
    )
    return validated


def stage_discover(config: dict, sheet: SheetRegistry, storage: PipelineStorage, validated: list[CaseCandidate] = None):
    """Stage 3A: Link discovery."""
    log = get_logger()
    log.info("=" * 60)
    log.info("STAGE 3A — LINK DISCOVERY")
    log.info("=" * 60)

    # If no validated cases passed in, load from sheet
    if validated is None:
        rows = sheet.find_cases_by_status(ValidationStatus.VALIDATED_CLOSED.value)
        validated = []
        for r in rows:
            validated.append(CaseCandidate(**{
                k: r.get(k, "") for k in CaseCandidate.__dataclass_fields__
            }))
        log.info("Loaded %d validated_closed cases from sheet", len(validated))

    for c in validated:
        log.info("Discovering links for: %s", c.case_id)

        inventory = run_discovery(
            candidate=c,
            brave_api_key=config["brave_api_key"],
            rate_limit=1.0 / config.get("brave_requests_per_second", 1),
            courtlistener_api_key=config.get("courtlistener_api_key", ""),
            caselaw_api_key=config.get("caselaw_api_key", ""),
        )

        # Determine official corroboration
        has_official = any(
            link.get("official_corroboration", False)
            for link in inventory.links
        )
        corroboration = (
            CorroborationStatus.CONFIRMED.value if has_official
            else CorroborationStatus.NOT_FOUND.value
        )

        # Save inventory locally
        if c.local_case_folder:
            folder_path = os.path.join(storage.root, c.local_case_folder)
        else:
            year = c.published_at[:4] if c.published_at else ""
            folder_name = make_case_folder_name(c.state, c.suspect_name, "Sentenced", year, c.video_id)
            folder_path = storage.create_validated_folder(folder_name)
            c.local_case_folder = storage.relative_path(folder_path)

        inv_dict = asdict(inventory)
        # Persist enrichment metadata (case numbers, citations) if present
        if hasattr(inventory, "enrichment"):
            inv_dict["enrichment"] = inventory.enrichment
        storage.write_links_inventory(folder_path, inv_dict)
        storage.copy_links_inventory(c.case_id, inv_dict)

        # Update sheet
        c.validation_status = ValidationStatus.LINKS_DISCOVERED.value
        c.links_discovered = len(inventory.links)
        c.official_corroboration_status = corroboration

        sheet.update_case(c.case_id, {
            "validation_status": c.validation_status,
            "links_discovered": c.links_discovered,
            "official_corroboration_status": c.official_corroboration_status,
            "local_case_folder": c.local_case_folder,
        })

        if corroboration == CorroborationStatus.NOT_FOUND.value:
            log.warning(
                "No official corroboration found for %s — logged as edge case", c.case_id
            )

    log.info("Stage 3A complete: processed %d cases", len(validated))
    return validated


def stage_download(config: dict, sheet: SheetRegistry, storage: PipelineStorage, cases: list[CaseCandidate] = None):
    """Stage 3B: Asset download."""
    log = get_logger()
    log.info("=" * 60)
    log.info("STAGE 3B — ASSET DOWNLOAD")
    log.info("=" * 60)

    # If no cases passed in, load links_discovered from sheet
    if cases is None:
        rows = sheet.find_cases_by_status(ValidationStatus.LINKS_DISCOVERED.value)
        cases = []
        for r in rows:
            cases.append(CaseCandidate(**{
                k: r.get(k, "") for k in CaseCandidate.__dataclass_fields__
            }))
        log.info("Loaded %d links_discovered cases from sheet", len(cases))

    for c in cases:
        if not c.local_case_folder:
            log.warning("No local folder for %s, skipping download", c.case_id)
            continue

        folder_path = os.path.join(storage.root, c.local_case_folder)
        inv_path = os.path.join(folder_path, "links_inventory.json")

        if not os.path.exists(inv_path):
            log.warning("No links_inventory.json for %s, skipping download", c.case_id)
            continue

        with open(inv_path, "r") as f:
            inventory = json.load(f)

        download_dir = storage.get_downloads_dir(folder_path)

        successes, failures, results = download_from_inventory(
            inventory=inventory,
            download_dir=download_dir,
            max_size_mb=config.get("max_download_size_mb", 100),
            timeout=config.get("download_timeout_seconds", 60),
            rate_limit=1.0 / config.get("download_requests_per_second", 0.5),
        )

        # Update inventory with download results
        storage.write_links_inventory(folder_path, inventory)

        # Update sheet
        c.validation_status = ValidationStatus.DOWNLOADS_COMPLETED.value
        c.downloads_completed = successes

        sheet.update_case(c.case_id, {
            "validation_status": c.validation_status,
            "downloads_completed": c.downloads_completed,
        })

        log.info(
            "Downloads for %s: %d success, %d failed",
            c.case_id, successes, failures,
        )

    log.info("Stage 3B complete: processed %d cases", len(cases))


def stage_reextract_names(config: dict, sheet: SheetRegistry, storage: PipelineStorage):
    """Re-run name extraction on all existing rows using improved regex + LLM.

    Uses video_title and video_description already stored in the sheet —
    zero YouTube API calls. Updates suspect_name and case_id in-place.
    """
    log = get_logger()
    log.info("=" * 60)
    log.info("RE-EXTRACT NAMES — reprocessing existing rows")
    log.info("=" * 60)

    rows = sheet.get_all_rows()
    log.info("Loaded %d rows from sheet", len(rows))

    updated = 0
    skipped_non_crime = 0
    officer_rejected = 0

    for row in rows:
        old_name = row.get("suspect_name", "")
        title = row.get("video_title", "")
        description = row.get("video_description", "")
        case_id = row.get("case_id", "")

        if not title and not description:
            continue

        # Classify video content first
        classification = classify_video_content(title, description)

        if classification["skip"]:
            if old_name:
                log.info(
                    "Flagging non-crime video: '%s' (had name '%s') — %s",
                    title[:60], old_name, classification["skip_reason"],
                )
                sheet.update_case(case_id, {
                    "suspect_name": "",
                    "validation_status": "rejected_open_or_unconfirmed",
                    "validation_note": f"Auto-rejected: {classification['skip_reason']}",
                })
                skipped_non_crime += 1
            continue

        text = f"{title}\n{description}"

        # Try improved regex first
        new_name = _extract_name_regex(text)

        # Check if extracted name is actually an officer
        officer_names_lower = [n.lower() for n in classification["officer_names"]]
        if new_name and new_name.lower() in officer_names_lower:
            log.info("Rejected officer name '%s' for %s", new_name, case_id)
            new_name = ""
            officer_rejected += 1

        # For OIS, check name context
        if classification["is_ois"] and new_name:
            from .intake import OFFICER_ROLE_PHRASES
            name_lower = new_name.lower()
            desc_lower = description.lower()
            name_pos = desc_lower.find(name_lower)
            if name_pos >= 0:
                ctx_start = max(0, name_pos - 100)
                ctx_end = min(len(desc_lower), name_pos + len(name_lower) + 100)
                context = desc_lower[ctx_start:ctx_end]
                for phrase in OFFICER_ROLE_PHRASES:
                    if phrase in context:
                        log.info("Rejected OIS officer name '%s' for %s", new_name, case_id)
                        new_name = ""
                        officer_rejected += 1
                        break

        # Fall back to LLM if regex still empty
        if not new_name and config.get("openrouter_api_key"):
            new_name = _extract_name_llm(
                title, description,
                config["openrouter_api_key"],
                config.get("openrouter_model_extraction", "google/gemini-flash-1.5"),
                config.get("openrouter_base_url", "https://openrouter.ai/api/v1"),
            )
            # Double-check LLM result against officer names
            if new_name and new_name.lower() in officer_names_lower:
                log.info("Rejected LLM officer name '%s' for %s", new_name, case_id)
                new_name = ""

        # Cold cases: the extracted name is the VICTIM, not a suspect
        if classification["is_cold_case"] and new_name and not new_name.startswith("VICTIM:"):
            log.info("Cold case detected for %s — '%s' is the victim", case_id, new_name)
            new_name = f"VICTIM: {new_name}"

        # Re-extract date if missing
        old_date = row.get("incident_date", "")
        date_update = {}
        if not old_date:
            text = f"{title}\n{description}"
            new_date = _extract_date_regex(text, row.get("published_at", ""))
            if new_date:
                date_update["incident_date"] = new_date
                log.info("Extracted date for %s: %s", case_id, new_date)

        # Tag operations in keywords AND use operation name as suspect_name
        old_keywords = row.get("case_keywords", "")
        keyword_update = {}
        if classification["is_operation"]:
            op_tag = classification["operation_name"] or "sting_operation"
            if classification["operation_arrest_count"]:
                op_tag = f"{op_tag} ({classification['operation_arrest_count']} arrests)"
            if op_tag.lower() not in old_keywords.lower():
                new_keywords = f"{old_keywords}, {op_tag}" if old_keywords else op_tag
                keyword_update["case_keywords"] = new_keywords
            # Use operation name as the case identifier when no suspect name
            if not new_name and classification["operation_name"]:
                new_name = classification["operation_name"]

        if classification["is_cold_case"] and "cold_case" not in old_keywords.lower():
            ck = keyword_update.get("case_keywords", old_keywords)
            keyword_update["case_keywords"] = f"{ck}, cold_case" if ck else "cold_case"

        if new_name != old_name or keyword_update or date_update:
            updates = {**keyword_update, **date_update}

            if new_name != old_name:
                new_case_id = _generate_case_id(
                    row.get("state", ""),
                    new_name,
                    row.get("video_id", ""),
                    row.get("published_at", ""),
                )
                updates["suspect_name"] = new_name
                updates["case_id"] = new_case_id

                log.info(
                    "Updated: '%s' -> '%s' (case_id: %s -> %s)",
                    old_name, new_name, case_id, new_case_id if new_name != old_name else case_id,
                )

            sheet.update_case(case_id, updates)
            updated += 1

    log.info(
        "Re-extraction complete: %d updated, %d non-crime skipped, %d officer names rejected (of %d rows)",
        updated, skipped_non_crime, officer_rejected, len(rows),
    )
    return updated


def run_pipeline(
    config: dict,
    stages: list[str] = None,
    sheet: SheetRegistry = None,
    channels: list[ChannelConfig] = None,
    channels_path: str = "config/channels.yaml",
    pipeline_root: str = None,
    wipe_sheet: bool = False,
):
    """Programmatic entry point for running the pipeline (e.g. from Colab).

    Args:
        config: Settings dict (API keys, tuning knobs).
        stages: List of stages to run. Options: "intake", "validate", "discover",
                "download". If None, runs all stages.
        sheet: Pre-built SheetRegistry (e.g. from Colab auth). If None, builds
               one from config credentials.
        channels: Pre-built list of ChannelConfig. If None, loads from channels_path.
        channels_path: Path to channels.yaml (used only if channels is None).
        pipeline_root: Override for pipeline storage root directory.
        wipe_sheet: If True, clear all data rows before running.

    Returns:
        dict with keys: candidates, validated, discovered (lists of CaseCandidate)
    """
    if stages is None:
        stages = ["intake", "validate", "discover", "download"]

    root = pipeline_root or config.get("pipeline_root", "./CrimeDoc-Pipeline")
    setup_logger(root)
    log = get_logger()

    log.info("Sunshine-Gated Closed-Case Pipeline starting (programmatic)")
    log.info("Pipeline root: %s", os.path.abspath(root))

    storage = PipelineStorage(root)
    storage.init_dirs()

    if sheet is None:
        sheet = SheetRegistry(
            credentials_file=config["google_sheets_credentials_file"],
            spreadsheet_id=config["google_sheets_spreadsheet_id"],
            tab_name=config.get("google_sheets_tab_name", "CaseRegistry"),
        )
    sheet.ensure_headers()

    if wipe_sheet:
        log.info("Wiping all data from sheet before running")
        sheet.clear_all_data()

    if channels is None and "intake" in stages:
        channels = load_channels(channels_path)

    results = {"candidates": None, "validated": None, "discovered": None}

    if "reextract_names" in stages:
        stage_reextract_names(config, sheet, storage)

    if "intake" in stages:
        results["candidates"] = stage_intake(config, channels, sheet, storage)

    if "validate" in stages:
        results["validated"] = stage_validate(
            config, sheet, storage,
            results["candidates"] if "intake" in stages else None,
        )

    if "discover" in stages:
        results["discovered"] = stage_discover(
            config, sheet, storage,
            results["validated"] if "validate" in stages else None,
        )

    if "download" in stages:
        stage_download(
            config, sheet, storage,
            results["discovered"] if "discover" in stages else None,
        )

    log.info("Pipeline complete")
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Sunshine-Gated Closed-Case Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", help="Path to settings YAML file")
    parser.add_argument("--full", action="store_true", help="Run entire pipeline")
    parser.add_argument("--intake-only", action="store_true", help="Only run YouTube intake")
    parser.add_argument("--validate-only", action="store_true", help="Only run validation on new_candidates")
    parser.add_argument("--discover-only", action="store_true", help="Only run link discovery on validated cases")
    parser.add_argument("--download-only", action="store_true", help="Only download from link inventories")
    parser.add_argument("--reextract-names", action="store_true", help="Re-run name extraction on existing data (no API calls)")
    parser.add_argument("--wipe", action="store_true", help="Wipe all data rows from the sheet before running")
    args = parser.parse_args()

    # Default to --full if no stage specified
    if not any([args.full, args.intake_only, args.validate_only, args.discover_only, args.download_only, args.reextract_names]):
        args.full = True

    # Load config
    config = load_config(args.config)
    channels = load_channels()

    # Setup
    pipeline_root = config.get("pipeline_root", "./CrimeDoc-Pipeline")
    logger = setup_logger(pipeline_root)
    log = get_logger()

    log.info("Sunshine-Gated Closed-Case Pipeline starting")
    log.info("Pipeline root: %s", os.path.abspath(pipeline_root))

    storage = PipelineStorage(pipeline_root)
    storage.init_dirs()

    sheet = SheetRegistry(
        credentials_file=config["google_sheets_credentials_file"],
        spreadsheet_id=config["google_sheets_spreadsheet_id"],
        tab_name=config.get("google_sheets_tab_name", "CaseRegistry"),
    )
    sheet.ensure_headers()

    if args.wipe:
        log.info("Wiping all data from sheet before running")
        sheet.clear_all_data()

    # Execute stages
    candidates = None
    validated = None
    discovered = None

    if args.reextract_names:
        stage_reextract_names(config, sheet, storage)
        log.info("Re-extract names mode — done")
        return

    if args.full or args.intake_only:
        candidates = stage_intake(config, channels, sheet, storage)
        if args.intake_only:
            log.info("Intake-only mode — stopping after Stage 1")
            return

    if args.full or args.validate_only:
        validated = stage_validate(config, sheet, storage, candidates if args.full else None)
        if args.validate_only:
            log.info("Validate-only mode — stopping after Stage 2")
            return

    if args.full or args.discover_only:
        discovered = stage_discover(config, sheet, storage, validated if args.full else None)
        if args.discover_only:
            log.info("Discover-only mode — stopping after Stage 3A")
            return

    if args.full or args.download_only:
        stage_download(config, sheet, storage, discovered if args.full else None)

    log.info("Pipeline complete")


if __name__ == "__main__":
    main()
