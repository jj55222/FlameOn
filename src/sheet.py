"""Google Sheet registry for the Sunshine-Gated Closed-Case Pipeline.

One tab only. Reads and writes case data to/from the sheet.
"""

import time
from dataclasses import asdict
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from .logger import get_logger
from .models import SHEET_COLUMNS, CaseCandidate

log = get_logger()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class SheetRegistry:
    """Manages the one-tab Google Sheet case registry."""

    def __init__(
        self,
        credentials_file: str,
        spreadsheet_id: str,
        tab_name: str = "CaseRegistry",
    ):
        self.spreadsheet_id = spreadsheet_id
        self.tab_name = tab_name
        self._client = None
        self._sheet = None
        self._credentials_file = credentials_file

    @classmethod
    def from_credentials(cls, credentials, spreadsheet_id: str, tab_name: str = "CaseRegistry"):
        """Create a SheetRegistry from pre-built credentials (e.g. Colab auth).

        Usage in Colab:
            from google.colab import auth
            auth.authenticate_user()
            import google.auth
            creds, _ = google.auth.default(scopes=SCOPES)
            sheet = SheetRegistry.from_credentials(creds, SPREADSHEET_ID)
        """
        instance = cls.__new__(cls)
        instance.spreadsheet_id = spreadsheet_id
        instance.tab_name = tab_name
        instance._credentials_file = None
        instance._client = gspread.authorize(credentials)
        instance._sheet = None
        return instance

    def _connect(self):
        """Authenticate and open the sheet."""
        if self._sheet is not None:
            return

        if self._client is None:
            creds = Credentials.from_service_account_file(self._credentials_file, scopes=SCOPES)
            self._client = gspread.authorize(creds)

        try:
            spreadsheet = self._client.open_by_key(self.spreadsheet_id)
            self._sheet = spreadsheet.worksheet(self.tab_name)
            log.info("Connected to Google Sheet: %s / %s", self.spreadsheet_id, self.tab_name)
        except gspread.WorksheetNotFound:
            spreadsheet = self._client.open_by_key(self.spreadsheet_id)
            self._sheet = spreadsheet.add_worksheet(title=self.tab_name, rows=1000, cols=len(SHEET_COLUMNS))
            # Write header row
            self._sheet.update("A1", [SHEET_COLUMNS])
            log.info("Created new worksheet: %s", self.tab_name)

    def ensure_headers(self):
        """Ensure the header row exists and matches SHEET_COLUMNS."""
        self._connect()
        header = self._sheet.row_values(1)
        if header != SHEET_COLUMNS:
            self._sheet.update("A1", [SHEET_COLUMNS])
            log.info("Updated sheet headers")

    def get_all_rows(self) -> list[dict]:
        """Read all data rows as dicts."""
        self._connect()
        records = self._sheet.get_all_records()
        return records

    def get_existing_video_ids(self) -> set[str]:
        """Get set of all video_ids currently in the sheet."""
        self._connect()
        try:
            col_idx = SHEET_COLUMNS.index("video_id") + 1
            values = self._sheet.col_values(col_idx)
            # Skip header
            return set(values[1:]) if len(values) > 1 else set()
        except Exception as e:
            log.error("Failed to get existing video IDs: %s", e)
            return set()

    def get_existing_cases(self) -> list[dict]:
        """Get simplified case data for deduplication checks."""
        rows = self.get_all_rows()
        return [
            {
                "case_id": r.get("case_id", ""),
                "suspect_name": r.get("suspect_name", ""),
                "state": r.get("state", ""),
                "city": r.get("city", ""),
                "incident_date": r.get("incident_date", ""),
            }
            for r in rows
        ]

    def _candidate_to_row(self, candidate: CaseCandidate) -> list[str]:
        """Convert a CaseCandidate to a row list matching SHEET_COLUMNS order."""
        data = asdict(candidate)
        row = []
        for col in SHEET_COLUMNS:
            val = data.get(col, "")
            row.append(str(val) if val is not None else "")
        return row

    def append_case(self, candidate: CaseCandidate):
        """Append a new case row to the sheet."""
        self._connect()
        candidate.touch()
        row = self._candidate_to_row(candidate)
        self._retry_on_quota(self._sheet.append_row, row, value_input_option="USER_ENTERED")
        log.info("Appended case to sheet: %s", candidate.case_id)

    def append_cases_batch(self, candidates: list[CaseCandidate]):
        """Append multiple case rows in a single API call to avoid rate limits."""
        if not candidates:
            return
        self._connect()
        rows = []
        for c in candidates:
            c.touch()
            rows.append(self._candidate_to_row(c))
        self._retry_on_quota(self._sheet.append_rows, rows, value_input_option="USER_ENTERED")
        log.info("Batch appended %d cases to sheet", len(rows))

    def _retry_on_quota(self, fn, *args, max_retries=4, **kwargs):
        """Retry a Sheets API call with exponential backoff on 429 errors."""
        for attempt in range(max_retries + 1):
            try:
                return fn(*args, **kwargs)
            except gspread.exceptions.APIError as e:
                if e.response.status_code == 429 and attempt < max_retries:
                    wait = 2 ** (attempt + 1)  # 2, 4, 8, 16 seconds
                    log.warning("Sheets quota exceeded, retrying in %ds (attempt %d/%d)",
                                wait, attempt + 1, max_retries)
                    time.sleep(wait)
                else:
                    raise

    def update_case(self, case_id: str, updates: dict):
        """Update specific fields for an existing case row.

        Uses a single batch API call instead of per-cell updates to avoid
        hitting Sheets write quota limits.

        updates: dict of {column_name: new_value}
        """
        self._connect()

        # Find the row with this case_id
        try:
            cell = self._sheet.find(case_id, in_column=1)
        except gspread.CellNotFound:
            log.warning("Case %s not found in sheet for update", case_id)
            return

        if not cell:
            log.warning("Case %s not found in sheet for update", case_id)
            return

        row_num = cell.row

        # Always update updated_at
        updates["updated_at"] = datetime.utcnow().isoformat()

        # Read current row, apply updates, write back as single API call
        current_row = self._sheet.row_values(row_num)
        # Pad row to full width if needed
        while len(current_row) < len(SHEET_COLUMNS):
            current_row.append("")

        for col_name, value in updates.items():
            if col_name in SHEET_COLUMNS:
                col_idx = SHEET_COLUMNS.index(col_name)
                current_row[col_idx] = str(value) if value is not None else ""

        # Single API call to update the entire row
        col_letter = gspread.utils.rowcol_to_a1(1, len(SHEET_COLUMNS)).rstrip("1")
        cell_range = f"A{row_num}:{col_letter}{row_num}"

        self._retry_on_quota(
            self._sheet.update,
            cell_range,
            [current_row],
            value_input_option="USER_ENTERED",
        )

        log.debug("Updated case %s in sheet (batch): %s", case_id, list(updates.keys()))

    def find_cases_by_status(self, status: str) -> list[dict]:
        """Find all cases with a given validation_status."""
        rows = self.get_all_rows()
        return [r for r in rows if r.get("validation_status") == status]
