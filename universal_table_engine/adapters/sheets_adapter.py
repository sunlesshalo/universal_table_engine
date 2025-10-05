from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:  # pragma: no cover - optional dependency
    gspread = None
    Credentials = None

from ..settings import AppSettings

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]


def export_to_sheets(
    df: pd.DataFrame,
    *,
    settings: AppSettings,
    worksheet_name: Optional[str],
    client_id: Optional[str],
    primary_key: Optional[str] = None,
    mode: Optional[str] = None,
) -> Dict[str, Any]:
    if not settings.enable_sheets_adapter:
        return {"adapter": "sheets", "status": "skipped", "reason": "disabled"}
    if gspread is None or Credentials is None:
        return {"adapter": "sheets", "status": "skipped", "reason": "dependencies_missing"}
    if not settings.sheets_spreadsheet_id or not settings.sheets_service_account_file:
        return {"adapter": "sheets", "status": "skipped", "reason": "missing_credentials"}

    credentials = Credentials.from_service_account_file(str(settings.sheets_service_account_file), scopes=SCOPES)
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(settings.sheets_spreadsheet_id)

    name = worksheet_name or client_id or "default"
    try:
        worksheet = spreadsheet.worksheet(name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=name, rows=str(len(df) + 10), cols=str(len(df.columns) + 10))

    write_mode = (mode or settings.sheets_mode).lower()
    if write_mode == "replace":
        worksheet.clear()
        worksheet.update([df.columns.tolist()] + df.fillna("").values.tolist())
    else:  # append
        existing_records: list[dict[str, Any]] = []
        if primary_key:
            existing_records = worksheet.get_all_records()
            existing_keys = {str(item.get(primary_key)) for item in existing_records if item.get(primary_key) is not None}
            rows_to_append = []
            for record in df.fillna("").to_dict(orient="records"):
                key = record.get(primary_key)
                if key is None or str(key) not in existing_keys:
                    rows_to_append.append(record)
            payload = rows_to_append
        else:
            payload = df.fillna("").to_dict(orient="records")
        if payload:
            columns = df.columns.tolist()
            rows = [[record.get(column, "") for column in columns] for record in payload]
            worksheet.append_rows(rows, value_input_option="USER_ENTERED")
    return {"adapter": "sheets", "status": "ok", "worksheet": name}


__all__ = ["export_to_sheets"]
