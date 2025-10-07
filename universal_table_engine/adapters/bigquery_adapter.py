from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd

try:
    from google.cloud import bigquery
except ImportError:  # pragma: no cover - optional dependency
    bigquery = None

from ..models import AdapterResult
from ..settings import AppSettings
from ..utils.bigquery import sanitize_field_name


def export_to_bigquery(
    df: pd.DataFrame,
    *,
    settings: AppSettings,
    dataset: Optional[str] = None,
    table: Optional[str] = None,
    partition_field: Optional[str] = None,
    mode: str = "stream",
    file_path: Optional[Path] = None,
    schema_columns: Optional[Sequence[str]] = None,
    field_map: Optional[Dict[str, str]] = None,
) -> AdapterResult:
    if not settings.enable_bigquery_adapter:
        return AdapterResult(adapter="bigquery", status="skipped", reason="disabled")
    if bigquery is None:
        return AdapterResult(adapter="bigquery", status="skipped", reason="dependencies_missing")

    project = settings.bigquery_project
    dataset_id = dataset or settings.bigquery_dataset
    table_id = table or settings.bigquery_table
    if not project or not dataset_id or not table_id:
        return AdapterResult(adapter="bigquery", status="skipped", reason="missing_configuration")

    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset_id}.{table_id}"
    requested_mode = (mode or "stream").lower()
    effective_mode = "file" if requested_mode == "file" else "stream"
    notes = [f"bq_loaded_via={effective_mode}"]

    rename_map = {
        column: field_map.get(column, sanitize_field_name(column)) if field_map else sanitize_field_name(column)
        for column in df.columns
    }
    sanitized_df = df.rename(columns=rename_map)
    sanitized_columns = list(sanitized_df.columns)
    sanitized_schema_columns: Optional[List[str]] = (
        [
            field_map.get(column, sanitize_field_name(column)) if field_map else sanitize_field_name(column)
            for column in schema_columns
        ]
        if schema_columns
        else None
    )
    sanitized_partition_field = (
        field_map.get(partition_field, sanitize_field_name(partition_field))
        if partition_field
        else None
    )

    try:
        if effective_mode == "file":
            if not file_path or not file_path.exists():
                notes.append("ndjson_missing")
                job = _load_dataframe(
                    client,
                    sanitized_df,
                    table_ref,
                    settings=settings,
                    partition_field=sanitized_partition_field,
                    field_map=field_map,
                )
                effective_mode = "stream"
                notes[0] = "bq_loaded_via=stream"
            else:
                job = load_from_file(
                    client,
                    file_path=file_path,
                    table_ref=table_ref,
                    settings=settings,
                    partition_field=sanitized_partition_field,
                    schema_columns=sanitized_schema_columns,
                    field_map=field_map,
                )
        else:
            job = _load_dataframe(
                client,
                sanitized_df,
                table_ref,
                settings=settings,
                partition_field=sanitized_partition_field,
                field_map=field_map,
            )
    except Exception as exc:  # pragma: no cover - network failure
        return AdapterResult(
            adapter="bigquery",
            status="error",
            reason=str(exc),
            notes=notes,
        )

    notes.extend(_maybe_run_dedup(client, table_ref, settings, sanitized_columns, field_map))

    return AdapterResult(
        adapter="bigquery",
        status="ok",
        mode=effective_mode,
        table=table_ref,
        job_id=getattr(job, "job_id", None),
        notes=notes,
        details={
            "location": settings.bigquery_location,
        },
    )


def load_from_file(
    client: "bigquery.Client",
    *,
    file_path: Path,
    table_ref: str,
    settings: AppSettings,
    partition_field: Optional[str],
    schema_columns: Optional[Sequence[str]] = None,
    field_map: Optional[Dict[str, str]] = None,
):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        autodetect=True,
    )

    partition = _resolve_partition_field(partition_field, schema_columns, settings)
    if partition:
        job_config.time_partitioning = bigquery.TimePartitioning(field=partition)

    cluster_fields = [field for field in settings.bigquery_cluster_fields if field]
    if cluster_fields:
        job_config.clustering_fields = [
            field_map.get(field, sanitize_field_name(field)) if field_map else sanitize_field_name(field)
            for field in cluster_fields
        ]

    string_fields = [field for field in settings.bigquery_string_fields if field]
    if string_fields:
        job_config.schema = [
            bigquery.SchemaField(
                field_map.get(name, sanitize_field_name(name)) if field_map else sanitize_field_name(name),
                "STRING",
            )
            for name in string_fields
        ]

    if file_path.suffix == ".gz":
        job_config.compression = bigquery.Compression.GZIP

    with file_path.open("rb") as source:
        load_job = client.load_table_from_file(
            source,
            table_ref,
            job_config=job_config,
            location=settings.bigquery_location,
        )
    load_job.result()
    return load_job


def _load_dataframe(
    client: "bigquery.Client",
    df: pd.DataFrame,
    table_ref: str,
    *,
    settings: AppSettings,
    partition_field: Optional[str],
    field_map: Optional[Dict[str, str]] = None,
):
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    if partition_field and partition_field in df.columns:
        job_config.time_partitioning = bigquery.TimePartitioning(field=partition_field)

    cluster_fields = [field for field in settings.bigquery_cluster_fields if field]
    if cluster_fields:
        job_config.clustering_fields = [
            field_map.get(field, sanitize_field_name(field)) if field_map else sanitize_field_name(field)
            for field in cluster_fields
        ]

    load_job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config,
        location=settings.bigquery_location,
    )
    load_job.result()
    return load_job


def _resolve_partition_field(
    partition_field: Optional[str],
    schema_columns: Optional[Sequence[str]],
    settings: AppSettings,
) -> Optional[str]:
    if partition_field:
        return partition_field
    candidate = settings.bigquery_time_partition_field or "created_at"
    sanitized_candidate = sanitize_field_name(candidate)
    if schema_columns and sanitized_candidate not in schema_columns:
        return None
    return sanitized_candidate


def _maybe_run_dedup(
    client: "bigquery.Client",
    table_ref: str,
    settings: AppSettings,
    sanitized_columns: Sequence[str],
    field_map: Optional[Dict[str, str]],
) -> List[str]:
    if not settings.bigquery_dedup_key:
        return []

    dedup_key_original = settings.bigquery_dedup_key
    dedup_key = field_map.get(dedup_key_original, sanitize_field_name(dedup_key_original)) if field_map else sanitize_field_name(dedup_key_original)
    if dedup_key not in sanitized_columns:
        return [f"bq_dedup_skipped_missing_key={dedup_key}"]

    ordering_candidates = [
        field_map.get(column, sanitize_field_name(column)) if field_map else sanitize_field_name(column)
        for column in ("created_at", "updated_at", "timestamp", "date")
    ]
    order_column = next((candidate for candidate in ordering_candidates if candidate in sanitized_columns), dedup_key)

    query = f"""
        CREATE OR REPLACE TABLE `{table_ref}` AS
        SELECT * EXCEPT(__row_number)
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY `{dedup_key}`
                       ORDER BY `{order_column}` DESC
                   ) AS __row_number
            FROM `{table_ref}`
        )
        WHERE __row_number = 1
    """

    try:
        job = client.query(query, location=settings.bigquery_location)
        job.result()
        return ["bq_dedup_applied"]
    except Exception as exc:  # pragma: no cover - network failure
        return [f"bq_dedup_failed={exc}"]


__all__ = ["export_to_bigquery", "load_from_file"]
