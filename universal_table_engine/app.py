from __future__ import annotations

import math
import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import structlog
from fastapi import Depends, FastAPI, File, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from .adapters import bigquery_adapter, json_adapter, sheets_adapter
from .ingest import file_reader, header_detect, normalize, rules_loader
from .ingest.llm_helper import build_alias_client, build_header_client
from .logging_conf import configure_logging
from .models import HealthResponse, ParseResponse, PIIMetadata, RulesResponse, SchemaMetadata, SourceMetadata
from .settings import AppSettings, get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()
configure_logging(settings.log_level)

app = FastAPI(title="Universal Table Engine", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_start_time = time.monotonic()


def get_app_settings() -> AppSettings:
    return settings


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    uptime = time.monotonic() - _start_time
    return HealthResponse(status="ok", uptime_seconds=uptime, timestamp=datetime.utcnow())


@app.get("/rules", response_model=RulesResponse)
def list_rules(config: AppSettings = Depends(get_app_settings)) -> RulesResponse:
    if not config.rules_dir.exists():
        return RulesResponse(rules=[])
    rules = sorted(path.stem for path in config.rules_dir.glob("*.json"))
    return RulesResponse(rules=rules)


@app.post("/parse", response_model=ParseResponse)
async def parse_file(
    file: UploadFile = File(...),
    client_id: Optional[str] = Query(default=None),
    source_hint: Optional[str] = Query(default=None),
    adapter: Optional[str] = Query(default=None, pattern="^(json|sheets|bigquery|none)$"),
    sheet_name: Optional[str] = Query(default=None),
    enable_llm: Optional[bool] = Query(default=None),
    config: AppSettings = Depends(get_app_settings),
) -> ParseResponse:
    request_notes: List[str] = []
    started = time.perf_counter()
    try:
        raw_bytes = await file.read()
        size_bytes = len(raw_bytes)
        if size_bytes == 0:
            raise ValueError("empty file uploaded")
        if size_bytes > config.max_upload_size_mb * 1024 * 1024:
            raise ValueError("file exceeds maximum size")

        sample = file_reader.load_file(
            raw_bytes,
            file.filename,
            sheet_name=sheet_name,
            sample_limit=config.csv_sample_rows,
            max_size_bytes=config.max_upload_size_mb * 1024 * 1024,
        )
        request_notes.append(f"detected_format={sample.detected_format}")
        if sample.sheet_choice:
            request_notes.append(f"sheet_selected={sample.sheet_choice.name}")

        header_client = build_header_client(config, enable_llm)
        header_result = header_detect.detect_header(
            sample.sample_rows,
            llm_client=header_client,
            max_rows=config.header_search_rows,
        )

        rules, rule_notes = rules_loader.load_matching_rule(
            filename=file.filename,
            columns=header_result.columns,
            settings=config,
            source_hint=source_hint,
        )

        if rule_notes:
            request_notes.extend(rule_notes)

        alias_client = build_alias_client(config, enable_llm)
        alias_mapping = None
        if alias_client and header_result.columns:
            alias_samples = _build_alias_samples(header_result.header_row, header_result.columns, sample.sample_rows)
            if alias_samples:
                alias_mapping = alias_client(header_result.columns, alias_samples)

        normalization = normalize.normalize_table(
            sample,
            header_result.header_row,
            header_result.columns,
            settings=config,
            rules=rules,
            llm_aliases=alias_mapping,
        )

        status = normalization.status_hint
        overall_confidence = float(
            max(0.0, min(1.0, (header_result.confidence + normalization.confidence) / 2))
        )
        if status == "ok" and overall_confidence < 0.6:
            status = "parsed_with_low_confidence"
        if rules is None and status != "ok":
            status = "needs_rulefile"

        notes = request_notes + header_result.notes + normalization.notes
        notes = list(dict.fromkeys(notes))

        records = _serialize_records(normalization.dataframe)

        source = SourceMetadata(
            filename=file.filename,
            client_id=client_id,
            detected_format=sample.detected_format,
            sheet=sample.sheet_choice.name if sample.sheet_choice else None,
        )

        table_schema = SchemaMetadata(**normalization.schema)  # type: ignore[arg-type]
        pii_meta = PIIMetadata(**normalization.pii_flags)

        response_payload: Dict[str, object] = {
            "status": status,
            "confidence": round(overall_confidence, 3),
            "source": source,
            "table_schema": table_schema,
            "data": records,
            "notes": notes,
            "pii_detected": pii_meta,
        }

        adapter_results = []
        adapter_choice = (adapter or config.default_adapter).lower()
        if adapter_choice == "json" and config.enable_json_adapter:
            adapter_results.append(
                json_adapter.export_json(
                    _payload_to_dict(response_payload), settings=config, client_id=client_id, filename=file.filename
                )
            )
        elif adapter_choice == "sheets":
            adapter_results.append(
                sheets_adapter.export_to_sheets(
                    normalization.dataframe,
                    settings=config,
                    worksheet_name=sheet_name,
                    client_id=client_id,
                    primary_key=(rules or {}).get("primary_key") if rules else None,
                    mode=(rules or {}).get("sheets_mode") if rules else None,
                )
            )
        elif adapter_choice == "bigquery":
            adapter_results.append(
                bigquery_adapter.export_to_bigquery(
                    normalization.dataframe,
                    settings=config,
                    dataset=(rules or {}).get("bigquery_dataset") if rules else None,
                    table=(rules or {}).get("bigquery_table") if rules else None,
                    partition_field=_find_partition_field(normalization.schema),
                )
            )
        elif adapter_choice == "none":
            pass

        response_payload["adapter_results"] = adapter_results or None

        duration = time.perf_counter() - started
        row_count = int(normalization.dataframe.shape[0])
        column_count = int(normalization.dataframe.shape[1])

        logger.info(
            "parse_success",
            filename=file.filename,
            client_id=client_id,
            status=status,
            confidence=overall_confidence,
            adapters=[result.get("adapter") for result in adapter_results],
            rows=row_count,
            cols=column_count,
            duration_ms=round(duration * 1000, 2),
        )

        return ParseResponse(**response_payload)
    except Exception as exc:
        logger.warning("parse_fallback", error=str(exc), filename=getattr(file, "filename", "unknown"))
        fallback_notes = request_notes + [f"error:{exc}"]
        source = SourceMetadata(
            filename=getattr(file, "filename", "unknown"),
            client_id=client_id,
            detected_format="csv",
            sheet=None,
        )
        table_schema = SchemaMetadata(columns=[], types={}, aliases={}, dataset_type="unknown")
        pii_meta = PIIMetadata(email=False, phone=False)
        return ParseResponse(
            status="parsed_with_low_confidence",
            confidence=0.2,
            source=source,
            table_schema=table_schema,
            data=[],
            notes=fallback_notes,
            pii_detected=pii_meta,
            adapter_results=None,
        )


@app.post("/parse/batch")
def parse_batch() -> Dict[str, str]:
    return {"status": "not_implemented"}


def _serialize_records(df: pd.DataFrame) -> List[Dict[str, object]]:
    records = []
    for record in df.to_dict(orient="records"):
        clean: Dict[str, object] = {}
        for key, value in record.items():
            if value is None:
                clean[key] = None
            elif isinstance(value, float) and math.isnan(value):
                clean[key] = None
            elif pd.isna(value):  # type: ignore[arg-type]
                clean[key] = None
            elif isinstance(value, pd.Timestamp):
                clean[key] = value.to_pydatetime().replace(microsecond=0).isoformat()
            elif isinstance(value, datetime):
                clean[key] = value.replace(microsecond=0).isoformat()
            else:
                clean[key] = value
        records.append(clean)
    return records


def _payload_to_dict(payload: Dict[str, object]) -> Dict[str, object]:
    result: Dict[str, object] = {}
    for key, value in payload.items():
        target_key = "schema" if key == "table_schema" else key
        if hasattr(value, "model_dump"):
            result[target_key] = value.model_dump(by_alias=True)
        elif isinstance(value, list):
            result[target_key] = [
                item.model_dump(by_alias=True) if hasattr(item, "model_dump") else item for item in value
            ]
        else:
            result[target_key] = value
    return result


def _build_alias_samples(
    header_row: int,
    columns: List[str],
    sample_rows: List[List[str]],
    sample_size: int = 5,
) -> List[Dict[str, str]]:
    samples: List[Dict[str, str]] = []
    for row in sample_rows[header_row + 1 : header_row + 1 + sample_size]:
        record: Dict[str, str] = {}
        for index, column in enumerate(columns):
            record[column] = str(row[index]).strip() if index < len(row) else ""
        samples.append(record)
    return samples


def _find_partition_field(schema: Dict[str, object]) -> Optional[str]:
    aliases = schema.get("aliases") if isinstance(schema, dict) else None
    if isinstance(aliases, dict):
        for column, alias in aliases.items():
            if alias == "date":
                return column
    return None


__all__ = ["app"]
