"""
sync_to_gdoc.py — Syncs CLAUDE.md to a Google Doc titled 'FlameOn - Project Context'.

Called automatically by Claude Code's PostToolUse hook after any Write/Edit to CLAUDE.md.
Also safe to run manually at any time.

First-run setup (one time only):
    pip install google-api-python-client google-auth-oauthlib google-auth-httplib2
    python sync_to_gdoc.py --setup

    This opens a browser OAuth flow and saves credentials to gdoc_credentials.json.
    After that, the hook keeps everything in sync automatically.

State is stored in gdoc_state.json (same directory):
    { "doc_id": "1ABC...", "doc_url": "https://docs.google.com/..." }
"""

import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CLAUDE_MD   = os.path.join(SCRIPT_DIR, "CLAUDE.md")
STATE_FILE  = os.path.join(SCRIPT_DIR, "gdoc_state.json")
TOKEN_FILE  = os.path.join(SCRIPT_DIR, "gdoc_token.json")
CREDS_FILE  = os.path.join(SCRIPT_DIR, "gdoc_credentials.json")
DOC_TITLE   = "FlameOn - Project Context"

SCOPES = [
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive.file",
]


# ── Hook filter ─────────────────────────────────────────────────────────────
# When called by the PostToolUse hook, Claude Code pipes the tool-use JSON to
# stdin. We check whether CLAUDE.md was the file being modified. If not, exit
# silently so we don't slow down every single file edit.

def was_claude_md_modified():
    """Return True if stdin indicates CLAUDE.md was the file just touched."""
    if sys.stdin.isatty():
        return True          # called directly (e.g. python sync_to_gdoc.py) — always run
    try:
        data = json.load(sys.stdin)
        tool_input = data.get("tool_input", {})
        file_path = tool_input.get("file_path", "") or ""
        return "CLAUDE.md" in file_path
    except Exception:
        return True          # if we can't parse, run anyway to be safe


# ── Google API helpers ───────────────────────────────────────────────────────

def get_credentials():
    """Load saved OAuth token or run the browser flow to get one."""
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print("[sync_to_gdoc] ERROR: Missing dependencies.")
        print("  Run: pip install google-api-python-client google-auth-oauthlib google-auth-httplib2")
        sys.exit(1)

    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDS_FILE):
                print(f"[sync_to_gdoc] ERROR: {CREDS_FILE} not found.")
                print("  1. Go to https://console.cloud.google.com/")
                print("  2. Create a project → Enable 'Google Docs API' + 'Google Drive API'")
                print("  3. Create OAuth 2.0 credentials (Desktop app)")
                print(f"  4. Download and save as: {CREDS_FILE}")
                print("  5. Re-run: python sync_to_gdoc.py")
                sys.exit(1)
            flow = InstalledAppFlow.from_client_secrets_file(CREDS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    return creds


def build_services(creds):
    from googleapiclient.discovery import build
    docs_service  = build("docs",  "v1", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)
    return docs_service, drive_service


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ── Doc create / update ─────────────────────────────────────────────────────

def create_doc(docs_service, drive_service, content):
    """Create a new Google Doc and return its ID and URL."""
    body = {"title": DOC_TITLE}
    doc = docs_service.documents().create(body=body).execute()
    doc_id = doc["documentId"]

    # Insert content
    requests_body = [{"insertText": {"location": {"index": 1}, "text": content}}]
    docs_service.documents().batchUpdate(
        documentId=doc_id,
        body={"requests": requests_body},
    ).execute()

    doc_url = f"https://docs.google.com/document/d/{doc_id}/edit"
    print(f"[sync_to_gdoc] Created Google Doc: {doc_url}")
    return doc_id, doc_url


def update_doc(docs_service, doc_id, content):
    """Replace all content in the Google Doc with new content."""
    # Get current doc to find end index
    doc = docs_service.documents().get(documentId=doc_id).execute()
    body_content = doc.get("body", {}).get("content", [])
    # End index of the last structural element
    end_index = body_content[-1].get("endIndex", 1) if body_content else 1

    requests_body = []
    # Delete all existing content (leave index 1 to avoid "can't delete last newline" error)
    if end_index > 2:
        requests_body.append({
            "deleteContentRange": {
                "range": {"startIndex": 1, "endIndex": end_index - 1}
            }
        })
    # Insert fresh content
    requests_body.append({
        "insertText": {"location": {"index": 1}, "text": content}
    })

    docs_service.documents().batchUpdate(
        documentId=doc_id,
        body={"requests": requests_body},
    ).execute()

    doc_url = f"https://docs.google.com/document/d/{doc_id}/edit"
    print(f"[sync_to_gdoc] Updated Google Doc: {doc_url}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    setup_mode = "--setup" in sys.argv

    if not setup_mode and not was_claude_md_modified():
        # Hook fired for a different file — nothing to do
        sys.exit(0)

    if not os.path.exists(CLAUDE_MD):
        print(f"[sync_to_gdoc] CLAUDE.md not found at {CLAUDE_MD}")
        sys.exit(1)

    with open(CLAUDE_MD, "r", encoding="utf-8") as f:
        content = f.read()

    creds = get_credentials()
    docs_service, drive_service = build_services(creds)
    state = load_state()

    if state.get("doc_id"):
        # Doc already exists — update it
        try:
            update_doc(docs_service, state["doc_id"], content)
        except Exception as e:
            if "404" in str(e):
                # Doc was deleted — recreate it
                print("[sync_to_gdoc] Doc not found (deleted?), recreating...")
                doc_id, doc_url = create_doc(docs_service, drive_service, content)
                save_state({"doc_id": doc_id, "doc_url": doc_url})
            else:
                raise
    else:
        # First run — create the doc
        doc_id, doc_url = create_doc(docs_service, drive_service, content)
        save_state({"doc_id": doc_id, "doc_url": doc_url})


if __name__ == "__main__":
    main()
