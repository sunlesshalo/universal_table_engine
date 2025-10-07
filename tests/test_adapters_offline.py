from __future__ import annotations

import json
from pathlib import Path
import gzip

import pandas as pd
import pytest

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
    envelope_files = list(out_dir.glob("*.json"))
    assert envelope_files, "JSON envelope not created"

    response_payload = response.json()
    adapter_results = response_payload["adapter_results"] or []
    assert any(result["adapter"] == "json" for result in adapter_results)

    ndjson_entry = next((result for result in adapter_results if result["adapter"] == "ndjson"), None)
    assert ndjson_entry is not None, "NDJSON adapter result missing"
    assert ndjson_entry["status"] == "ok"
    ndjson_artifact = ndjson_entry["artifacts"][0]
    ndjson_path = Path(ndjson_artifact["path"])
    assert ndjson_path.exists(), "NDJSON file not written"

    lines = ndjson_path.read_text("utf-8").splitlines()
    assert lines, "NDJSON file empty"
    meta = json.loads(lines[0])
    assert meta["type"] == "meta"
    assert meta["ndjson_version"] == "1"
    assert meta["rows"] == len(lines) - 1
    assert meta["content_hash"]
    sanitized_map = meta["sanitized_columns"]
    assert isinstance(sanitized_map, dict)
    assert all(" " not in value for value in sanitized_map.values())
    assert response_payload["notes"] and {"ndjson_written", "ndjson_sanitized"}.issubset(set(response_payload["notes"]))

    parsed_rows = [json.loads(line) for line in lines[1:]]
    assert parsed_rows, "Row payload missing"
    assert all("row" not in row for row in parsed_rows)
    assert all(" " not in key for row in parsed_rows for key in row)
    assert len(parsed_rows) == meta["rows"]


def test_sheets_adapter_skipped_when_disabled():
    settings = get_settings()
    df = pd.DataFrame({"a": [1]})
    result = sheets_adapter.export_to_sheets(
        df,
        settings=settings,
        worksheet_name="Test",
        client_id="demo",
    )
    assert result.status == "skipped"


def test_bigquery_adapter_skipped_without_config():
    settings = get_settings()
    df = pd.DataFrame({"a": [1]})
    result = bigquery_adapter.export_to_bigquery(df, settings=settings)
    assert result.status in {"skipped", "error"}


def test_bigquery_load_from_file(monkeypatch, tmp_path):
    settings = get_settings()
    monkeypatch.setattr(settings, "enable_bigquery_adapter", True)
    monkeypatch.setattr(settings, "bigquery_project", "demo")
    monkeypatch.setattr(settings, "bigquery_dataset", "dataset")
    monkeypatch.setattr(settings, "bigquery_table", "table")
    monkeypatch.setattr(settings, "bigquery_location", "EU")
    monkeypatch.setattr(settings, "bigquery_time_partition_field", None)
    monkeypatch.setattr(settings, "bigquery_cluster_fields", ["client_id"])
    monkeypatch.setattr(settings, "bigquery_string_fields", ["invoice_id"])
    monkeypatch.setattr(settings, "bigquery_dedup_key", "invoice_id")

    class FakeJob:
        job_id = "fake-job"

        def __init__(self, config):
            self.config = config

        def result(self):
            return self

    class FakeBigQuery:
        _last_client = None

        class LoadJobConfig:
            def __init__(self, **kwargs):
                self.source_format = kwargs.get("source_format")
                self.write_disposition = kwargs.get("write_disposition")
                self.create_disposition = kwargs.get("create_disposition")
                self.autodetect = kwargs.get("autodetect", False)
                self.schema = None
                self.time_partitioning = None
                self.clustering_fields = None
                self.compression = kwargs.get("compression")

        class SourceFormat:
            NEWLINE_DELIMITED_JSON = "NEWLINE_DELIMITED_JSON"

        class WriteDisposition:
            WRITE_APPEND = "WRITE_APPEND"

        class CreateDisposition:
            CREATE_IF_NEEDED = "CREATE_IF_NEEDED"

        class Compression:
            GZIP = "GZIP"

        class TimePartitioning:
            def __init__(self, *, field: str):
                self.field = field

        class SchemaField:
            def __init__(self, name: str, field_type: str):
                self.name = name
                self.field_type = field_type

        class Client:
            def __init__(self, project: str):
                self.project = project
                self.last_file_args = None
                self.executed_queries = []
                FakeBigQuery._last_client = self

            def load_table_from_file(self, source, table_ref, job_config, location=None):
                self.last_file_args = {
                    "table_ref": table_ref,
                    "job_config": job_config,
                    "location": location,
                }
                return FakeJob(job_config)

            def load_table_from_dataframe(self, *args, **kwargs):  # pragma: no cover - not exercised here
                job_config = kwargs.get("job_config")
                self.last_df_args = {"job_config": job_config}
                return FakeJob(job_config)

            def query(self, query, location=None):
                self.executed_queries.append({"query": query, "location": location})
                return FakeJob(None)

    monkeypatch.setattr(bigquery_adapter, "bigquery", FakeBigQuery)

    ndjson_path = tmp_path / "upload.ndjson.gz"
    with gzip.open(ndjson_path, "wt", encoding="utf-8") as handle:
        handle.write("{\"created_at\": \"2024-01-01T00:00:00Z\"}\n")

    df = pd.DataFrame(
        {
            "created_at": ["2024-01-01T00:00:00Z"],
            "invoice_id": ["INV-1"],
            "client_id": ["demo"],
        }
    )

    result = bigquery_adapter.export_to_bigquery(
        df,
        settings=settings,
        mode="file",
        file_path=ndjson_path,
        schema_columns=["created_at", "invoice_id", "client_id"],
    )

    assert result.status == "ok"
    assert result.mode == "file"
    assert result.table == "demo.dataset.table"
    assert result.job_id == "fake-job"
    assert "bq_loaded_via=file" in result.notes
    assert "bq_dedup_applied" in result.notes

    client = FakeBigQuery._last_client
    assert client is not None
    job_config = client.last_file_args["job_config"]
    assert job_config.source_format == FakeBigQuery.SourceFormat.NEWLINE_DELIMITED_JSON
    assert job_config.autodetect is True
    assert job_config.time_partitioning.field == "created_at"
    assert job_config.clustering_fields == ["client_id"]
    assert job_config.schema
    assert [field.field_type for field in job_config.schema] == ["STRING"]
    assert job_config.compression == FakeBigQuery.Compression.GZIP

    assert client.executed_queries, "Dedup query not executed"
    assert "PARTITION BY `invoice_id`" in client.executed_queries[0]["query"]
