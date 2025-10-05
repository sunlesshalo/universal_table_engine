from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

try:
    from google.cloud import bigquery
except ImportError:  # pragma: no cover - optional dependency
    bigquery = None

from ..settings import AppSettings


def export_to_bigquery(
    df: pd.DataFrame,
    *,
    settings: AppSettings,
    dataset: Optional[str] = None,
    table: Optional[str] = None,
    partition_field: Optional[str] = None,
) -> Dict[str, Any]:
    if not settings.enable_bigquery_adapter:
        return {"adapter": "bigquery", "status": "skipped", "reason": "disabled"}
    if bigquery is None:
        return {"adapter": "bigquery", "status": "skipped", "reason": "dependencies_missing"}
    project = settings.bigquery_project
    dataset_id = dataset or settings.bigquery_dataset
    table_id = table or settings.bigquery_table
    if not project or not dataset_id or not table_id:
        return {"adapter": "bigquery", "status": "skipped", "reason": "missing_configuration"}

    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    if partition_field and partition_field in df.columns:
        job_config.time_partitioning = bigquery.TimePartitioning(field=partition_field)

    try:
        load_job = client.load_table_from_dataframe(
            df,
            table_ref,
            job_config=job_config,
            location=settings.bigquery_location,
        )
        load_job.result()
    except Exception as exc:  # pragma: no cover - network failure
        return {"adapter": "bigquery", "status": "error", "reason": str(exc)}
    return {"adapter": "bigquery", "status": "ok", "table": table_ref}


__all__ = ["export_to_bigquery"]
