from __future__ import annotations

from pathlib import Path

import pandas as pd

from universal_table_engine.adapters import bigquery_adapter, sheets_adapter
from universal_table_engine.settings import get_settings


def test_json_adapter_writes_file(client, data_dir, tmp_path):
    settings = get_settings()
    settings.output_dir = tmp_path
    path = data_dir / "sample_smartbill.csv"
    with path.open("rb") as handle:
        response = client.post(
            "/parse",
            files={"file": (path.name, handle, "text/csv")},
            params={"client_id": "petchef", "adapter": "json", "enable_llm": "false"},
        )
    assert response.status_code == 200
    out_dir = tmp_path / "petchef"
    files = list(out_dir.glob("*.json"))
    assert files, "JSON export not created"


def test_sheets_adapter_skipped_when_disabled():
    settings = get_settings()
    df = pd.DataFrame({"a": [1]})
    result = sheets_adapter.export_to_sheets(
        df,
        settings=settings,
        worksheet_name="Test",
        client_id="demo",
    )
    assert result["status"] == "skipped"


def test_bigquery_adapter_skipped_without_config():
    settings = get_settings()
    df = pd.DataFrame({"a": [1]})
    result = bigquery_adapter.export_to_bigquery(df, settings=settings)
    assert result["status"] in {"skipped", "error"}
