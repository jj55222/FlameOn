"""
sync_to_gdrive.py — Mirrors the FlameOn repo to a Google Drive folder.

Fires on EVERY Write/Edit via PostToolUse hook. On each call it:
  1. Reads stdin to find which file was just changed (or syncs everything if run manually)
  2. Ensures the "FlameOn" Drive folder + sub-folder tree exists
  3. Uploads or updates only the changed file(s)

Also maintains the legacy CLAUDE.md → Google Doc sync.

State is stored in gdrive_state.json:
  {
    "root_folder_id": "...",
    "gdoc_id": "...",            # CLAUDE.md Google Doc (text-formatted)
    "folders": { "rel/path": "driveId", ... },
    "files":   { "rel/path": { "drive_id": "...", "mtime": 1234.5 }, ... }
  }

CLI:
    python sync_to_gdrive.py              # hook mode (reads stdin for changed file)
    python sync_to_gdrive.py --full       # full repo sync
    python sync_to_gdrive.py --setup      # first-time OAuth
    python sync_to_gdrive.py --file path  # sync one specific file
"""

import json
import os
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent  # FlameOn-main
REPO_ROOT = PROJECT_ROOT          # what we mirror

STATE_FILE = SCRIPT_DIR / "gdrive_state.json"
TOKEN_FILE = SCRIPT_DIR / "gdoc_token.json"
CREDS_FILE = SCRIPT_DIR / "gdoc_credentials.json"
DRIVE_FOLDER_NAME = "FlameOn"

SCOPES = [
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive.file",
]

# ── Skip patterns ──────────────────────────────────────────────────────────
SKIP_DIRS = {
    ".git", ".claude", "__pycache__", "node_modules", ".venv", "venv",
    "env", ".mypy_cache", ".pytest_cache",
}

SKIP_FILES = {
    ".env", "gdoc_credentials.json", "gdoc_token.json",
    "gdrive_state.json", "gdoc_state.json",
}

SKIP_EXTENSIONS = {
    ".pyc", ".pyo", ".so", ".dll", ".exe", ".bin",
}

# Max file size to upload (50 MB) — skip large audio/video cached files
MAX_FILE_SIZE = 50 * 1024 * 1024

MIME_MAP = {
    ".py": "text/x-python",
    ".md": "text/markdown",
    ".json": "application/json",
    ".txt": "text/plain",
    ".tsv": "text/tab-separated-values",
    ".csv": "text/csv",
    ".sh": "text/x-shellscript",
    ".yaml": "text/yaml",
    ".yml": "text/yaml",
    ".toml": "text/plain",
    ".cfg": "text/plain",
    ".ini": "text/plain",
    ".mp3": "audio/mpeg",
    ".m4a": "audio/mp4",
    ".webm": "audio/webm",
    ".wav": "audio/wav",
    ".pdf": "application/pdf",
    ".ipynb": "application/json",
}


def should_skip(rel_path: Path) -> bool:
    """Return True if this path should NOT be synced."""
    parts = rel_path.parts
    # Skip hidden dirs and known junk
    for part in parts:
        if part in SKIP_DIRS or part.startswith("."):
            return True
    if rel_path.name in SKIP_FILES:
        return True
    if rel_path.suffix in SKIP_EXTENSIONS:
        return True
    return False


# ── stdin hook filter ──────────────────────────────────────────────────────

def get_changed_file_from_stdin() -> str:
    """Read the PostToolUse JSON from stdin, return the file_path or ''."""
    if sys.stdin.isatty():
        return ""
    try:
        data = json.load(sys.stdin)
        tool_input = data.get("tool_input", {})
        return tool_input.get("file_path", "") or ""
    except Exception:
        return ""


# ── Google API helpers ─────────────────────────────────────────────────────

def get_credentials():
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow

    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CREDS_FILE.exists():
                print(f"[sync] ERROR: {CREDS_FILE} not found. Run --setup first.")
                sys.exit(1)
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDS_FILE), SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_FILE.write_text(creds.to_json())

    return creds


def build_services(creds):
    from googleapiclient.discovery import build
    docs = build("docs", "v1", credentials=creds)
    drive = build("drive", "v3", credentials=creds)
    return docs, drive


# ── State management ───────────────────────────────────────────────────────

def load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ── Drive folder operations ───────────────────────────────────────────────

def find_or_create_folder(drive, name: str, parent_id: str = None) -> str:
    """Find a folder by name under parent, or create it. Returns folder ID."""
    q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        q += f" and '{parent_id}' in parents"

    results = drive.files().list(q=q, fields="files(id,name)", pageSize=5).execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]

    # Create
    meta = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    if parent_id:
        meta["parents"] = [parent_id]
    folder = drive.files().create(body=meta, fields="id").execute()
    print(f"[sync] Created folder: {name}")
    return folder["id"]


def ensure_folder_path(drive, rel_dir: str, state: dict) -> str:
    """Ensure the full folder path exists in Drive, return the leaf folder ID."""
    if not rel_dir or rel_dir == ".":
        return state["root_folder_id"]

    folders = state.setdefault("folders", {})
    if rel_dir in folders:
        return folders[rel_dir]

    # Walk the path, creating as needed
    parts = Path(rel_dir).parts
    parent_id = state["root_folder_id"]
    built = ""
    for part in parts:
        built = str(Path(built) / part) if built else part
        if built in folders:
            parent_id = folders[built]
        else:
            parent_id = find_or_create_folder(drive, part, parent_id)
            folders[built] = parent_id

    return parent_id


# ── File upload / update ──────────────────────────────────────────────────

def upload_file(drive, local_path: Path, rel_path: str, parent_folder_id: str, state: dict) -> bool:
    """Upload a new file or update existing. Returns True if work was done."""
    from googleapiclient.http import MediaFileUpload

    files_state = state.setdefault("files", {})
    mtime = local_path.stat().st_mtime
    size = local_path.stat().st_size

    # Skip if unchanged
    existing = files_state.get(rel_path)
    if existing and abs(existing.get("mtime", 0) - mtime) < 0.5:
        return False

    # Skip oversized
    if size > MAX_FILE_SIZE:
        return False

    mime = MIME_MAP.get(local_path.suffix.lower(), "application/octet-stream")
    media = MediaFileUpload(str(local_path), mimetype=mime, resumable=size > 5 * 1024 * 1024)

    if existing and existing.get("drive_id"):
        # Update
        try:
            drive.files().update(
                fileId=existing["drive_id"],
                media_body=media,
            ).execute()
            files_state[rel_path] = {"drive_id": existing["drive_id"], "mtime": mtime, "size": size}
            return True
        except Exception as e:
            if "404" in str(e):
                pass  # File deleted on Drive, re-create below
            else:
                print(f"[sync] WARN update failed {rel_path}: {e}")
                return False

    # Create new
    meta = {
        "name": local_path.name,
        "parents": [parent_folder_id],
    }
    try:
        f = drive.files().create(body=meta, media_body=media, fields="id").execute()
        files_state[rel_path] = {"drive_id": f["id"], "mtime": mtime, "size": size}
        return True
    except Exception as e:
        print(f"[sync] WARN create failed {rel_path}: {e}")
        return False


# ── CLAUDE.md → Google Doc (legacy) ───────────────────────────────────────

def sync_claude_md_to_gdoc(docs, drive, state: dict):
    """Keep the human-readable Google Doc version of CLAUDE.md in sync."""
    claude_md = REPO_ROOT / "autoresearch" / "CLAUDE.md"
    if not claude_md.exists():
        return

    content = claude_md.read_text(encoding="utf-8")
    gdoc_id = state.get("gdoc_id")

    if gdoc_id:
        try:
            doc = docs.documents().get(documentId=gdoc_id).execute()
            body_content = doc.get("body", {}).get("content", [])
            end_index = body_content[-1].get("endIndex", 1) if body_content else 1
            reqs = []
            if end_index > 2:
                reqs.append({"deleteContentRange": {"range": {"startIndex": 1, "endIndex": end_index - 1}}})
            reqs.append({"insertText": {"location": {"index": 1}, "text": content}})
            docs.documents().batchUpdate(documentId=gdoc_id, body={"requests": reqs}).execute()
            return
        except Exception as e:
            if "404" not in str(e):
                print(f"[sync] WARN gdoc update: {e}")
                return

    # Create new
    doc = docs.documents().create(body={"title": "FlameOn - Project Context"}).execute()
    gdoc_id = doc["documentId"]
    docs.documents().batchUpdate(
        documentId=gdoc_id,
        body={"requests": [{"insertText": {"location": {"index": 1}, "text": content}}]},
    ).execute()
    state["gdoc_id"] = gdoc_id
    print(f"[sync] CLAUDE.md Google Doc: https://docs.google.com/document/d/{gdoc_id}/edit")


# ── Sync orchestration ────────────────────────────────────────────────────

def sync_single_file(drive, docs, abs_path: str, state: dict) -> int:
    """Sync one file. Returns count of files synced."""
    local = Path(abs_path)
    if not local.exists() or not local.is_file():
        return 0

    try:
        rel = local.relative_to(REPO_ROOT)
    except ValueError:
        return 0

    if should_skip(rel):
        return 0

    rel_str = str(rel).replace("\\", "/")
    rel_dir = str(rel.parent).replace("\\", "/")

    parent_id = ensure_folder_path(drive, rel_dir, state)
    did_work = upload_file(drive, local, rel_str, parent_id, state)

    # Also update CLAUDE.md Google Doc if that's what changed
    if "CLAUDE.md" in rel_str:
        sync_claude_md_to_gdoc(docs, drive, state)

    return 1 if did_work else 0


def sync_full_repo(drive, docs, state: dict) -> int:
    """Walk the repo and sync everything. Returns count of files synced."""
    count = 0
    for root, dirs, files in os.walk(REPO_ROOT):
        # Prune skip dirs in-place
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS and not d.startswith(".")]

        for fname in files:
            local = Path(root) / fname
            try:
                rel = local.relative_to(REPO_ROOT)
            except ValueError:
                continue
            if should_skip(rel):
                continue
            if local.stat().st_size > MAX_FILE_SIZE:
                continue

            rel_str = str(rel).replace("\\", "/")
            rel_dir = str(rel.parent).replace("\\", "/")

            parent_id = ensure_folder_path(drive, rel_dir, state)
            if upload_file(drive, local, rel_str, parent_id, state):
                count += 1

    # Always sync CLAUDE.md Google Doc on full
    sync_claude_md_to_gdoc(docs, drive, state)
    return count


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Sync FlameOn repo to Google Drive")
    parser.add_argument("--full", action="store_true", help="Full repo sync")
    parser.add_argument("--file", type=str, help="Sync one specific file")
    parser.add_argument("--setup", action="store_true", help="Run OAuth setup")
    args = parser.parse_args()

    try:
        creds = get_credentials()
    except SystemExit:
        return
    except Exception as e:
        print(f"[sync] Auth error: {e}")
        return

    docs, drive = build_services(creds)
    state = load_state()

    # Ensure root folder
    if not state.get("root_folder_id"):
        state["root_folder_id"] = find_or_create_folder(drive, DRIVE_FOLDER_NAME)
        save_state(state)
        print(f"[sync] Drive root folder: {DRIVE_FOLDER_NAME} ({state['root_folder_id']})")

    # Migrate legacy gdoc_id from old gdoc_state.json
    if not state.get("gdoc_id"):
        old_state_file = SCRIPT_DIR / "gdoc_state.json"
        if old_state_file.exists():
            try:
                old = json.loads(old_state_file.read_text())
                if old.get("doc_id"):
                    state["gdoc_id"] = old["doc_id"]
            except Exception:
                pass

    if args.setup:
        print("[sync] OAuth setup complete. Credentials saved.")
        save_state(state)
        return

    if args.full:
        t0 = time.time()
        n = sync_full_repo(drive, docs, state)
        save_state(state)
        print(f"[sync] Full sync done: {n} files uploaded/updated in {time.time()-t0:.1f}s")
        return

    if args.file:
        n = sync_single_file(drive, docs, args.file, state)
        save_state(state)
        if n:
            print(f"[sync] Synced: {args.file}")
        return

    # Hook mode: read stdin for changed file
    changed = get_changed_file_from_stdin()
    if changed:
        n = sync_single_file(drive, docs, changed, state)
        save_state(state)
    else:
        # No specific file — do a full sync (manual invocation)
        t0 = time.time()
        n = sync_full_repo(drive, docs, state)
        save_state(state)
        print(f"[sync] Full sync: {n} files in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
