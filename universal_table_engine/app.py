from __future__ import annotations

import base64
import hashlib
import hmac
import io
import json
import math
import time
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import structlog
from fastapi import BackgroundTasks, Depends, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette import status

import httpx

from .adapters import bigquery_adapter, json_adapter, sheets_adapter
from .http_errors import bad_request, forbidden, not_found, unauthorized
from .ingest import file_reader, header_detect, normalize, rules_loader
from .ingest.llm_helper import build_alias_client, build_header_client
from .logging_conf import configure_logging
from .models import (
    DeliverySummary,
    ErrorResponse,
    HealthResponse,
    ParseResponse,
    PIIMetadata,
    PresetPayload,
    RulesResponse,
    SchemaMetadata,
    SourceMetadata,
    WebhookReceipt,
)
from .presets import Preset, list_presets, load_preset, merge_with_preset, preset_path
from .settings import AppSettings, get_settings
from .webhook_store import WebhookStore

logger = structlog.get_logger(__name__)
settings = get_settings()
configure_logging(settings.log_level)
webhook_store = WebhookStore(settings)
UI_DIST_PATH = (Path(__file__).resolve().parent.parent / "ui" / "dist").resolve()

app = FastAPI(title="Universal Table Engine", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


_start_time = time.monotonic()


@dataclass(slots=True)
class ParseExecutionResult:
    response: ParseResponse
    adapter_results: List[Dict[str, Any]]
    notes: List[str]
    rule_applied: Optional[str]
    detected_format: str
    duration_ms: float
    rows: int
    cols: int


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
    header_row: Optional[int] = Query(default=None, ge=0),
    preset_id: Optional[str] = Query(default=None),
    dayfirst: Optional[bool] = Query(default=None),
    decimal_style: Optional[str] = Query(default=None, pattern="^(auto|comma|dot)$"),
    dry_run: Optional[bool] = Query(default=None),
    config: AppSettings = Depends(get_app_settings),
) -> ParseResponse:
    try:
        raw_bytes = await file.read()
        size_bytes = len(raw_bytes)
        if size_bytes == 0:
            raise ValueError("empty file uploaded")
        if size_bytes > config.max_upload_size_mb * 1024 * 1024:
            raise ValueError("file exceeds maximum size")
        preset = None
        if preset_id and client_id:
            preset = load_preset(client_id, preset_id, config)
        preset_defaults = preset.defaults if preset else {}
        options = merge_with_preset(
            preset_defaults,
            {
                "source_hint": source_hint,
                "sheet_name": sheet_name,
                "enable_llm": enable_llm,
                "adapter": adapter,
                "header_row": header_row,
                "dayfirst": dayfirst,
                "decimal_style": decimal_style,
                "dry_run": dry_run,
            },
        )

        result = await _run_parse_from_bytes(
            raw_bytes,
            filename=file.filename,
            client_id=client_id,
            adapter=options.get("adapter"),
            source_hint=options.get("source_hint"),
            sheet_name=options.get("sheet_name"),
            enable_llm=options.get("enable_llm"),
            config=config,
            options=options,
        )

        logger.info(
            "parse_success",
            filename=file.filename,
            client_id=client_id,
            status=result.response.status,
            confidence=result.response.confidence,
            adapters=[item.get("adapter") for item in result.adapter_results],
            rows=result.rows,
            cols=result.cols,
            duration_ms=result.duration_ms,
        )

        return result.response
    except Exception as exc:
        logger.warning("parse_fallback", error=str(exc), filename=getattr(file, "filename", "unknown"))
        fallback_notes = [f"error:{exc}"]
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


@app.post("/webhook/v1/intake", response_model=WebhookReceipt)
@app.post("/webhook/v1/intake/{client_id}", response_model=WebhookReceipt)
@app.post("/webhook/v1/intake/{client_id}/{preset_id}", response_model=WebhookReceipt)
async def webhook_intake(
    request: Request,
    background_tasks: BackgroundTasks,
    client_id: Optional[str] = None,
    preset_id: Optional[str] = None,
    sync: Optional[str] = Query(default=None),
    config: AppSettings = Depends(get_app_settings),
) -> WebhookReceipt:
    if not config.webhook_enable:
        raise not_found("webhook_disabled", "webhook intake is disabled")

    raw_body = await request.body()
    request._body = raw_body  # allow downstream form/json parsing

    _verify_ip_allowlist(request, config)
    _authorize_webhook(request, raw_body, client_id, config)

    preset: Optional[Preset] = None
    if preset_id:
        preset = load_preset(client_id or "default", preset_id, config)
        if preset is None:
            raise not_found("preset_not_found", "preset not found for client", hint=preset_id)

    content_type = request.headers.get("content-type", "")
    options: Dict[str, Any] = {}
    metadata: Dict[str, Any] | None = None

    if "multipart/form-data" in content_type:
        form = await request.form()
        upload = form.get("file")
        if not isinstance(upload, UploadFile):
            raise bad_request("missing_file", "multipart payload requires file field")
        file_bytes = await upload.read()
        _enforce_size(len(file_bytes), config.webhook_max_upload_size_mb)
        filename = upload.filename or "upload.bin"
        options = _extract_intake_options_from_mapping({key: value for key, value in form.multi_items() if key != "file"})
        metadata = None
        header_idempotency = request.headers.get("X-UTE-Idempotency-Key")
        idempotency_key = header_idempotency or _generate_idempotency_key(client_id, file_bytes)
    else:
        if not raw_body:
            raise bad_request("empty_body", "request body required")
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise bad_request("invalid_json", "body must be valid JSON") from exc
        if not isinstance(payload, dict):
            raise bad_request("invalid_json", "body must be a JSON object")
        metadata = dict(payload)
        if "file_b64" in metadata:
            metadata["file_b64"] = "__omitted__"
        options = _extract_intake_options_from_mapping(payload)
        file_url = payload.get("file_url")
        file_b64 = payload.get("file_b64")
        if file_url and file_b64:
            raise bad_request("conflicting_payload", "provide either file_url or file_b64")
        if not file_url and not file_b64:
            raise bad_request("missing_file_reference", "file_url or file_b64 is required")
        limit_bytes = config.webhook_max_upload_size_mb * 1024 * 1024
        if file_url:
            file_bytes, filename = await _download_file_from_url(file_url, max_bytes=limit_bytes)
        else:
            file_bytes = _decode_base64(str(file_b64))
            _enforce_size(len(file_bytes), config.webhook_max_upload_size_mb)
            filename = payload.get("filename", "upload.bin")
        idempotency_key = request.headers.get("X-UTE-Idempotency-Key")
        if not idempotency_key:
            raise bad_request("missing_idempotency_key", "X-UTE-Idempotency-Key header required for JSON intake")

    effective_client_id = options.get("client_id") or client_id
    if not effective_client_id:
        effective_client_id = "default"
    if "client_id" in options:
        options.pop("client_id")

    if "preset_id" in options and not preset_id:
        preset_id = options.pop("preset_id")

    preset_defaults = preset.defaults if preset else {}
    merged_options = merge_with_preset(preset_defaults, options)

    sync_flag = _resolve_sync_flag(sync, merged_options, config)

    duplicate = webhook_store.find_by_idempotency(effective_client_id, idempotency_key)
    if duplicate:
        return duplicate.model_copy(update={"duplicate": True})

    intake_id = uuid.uuid4().hex
    artifacts = _store_source_files(
        config,
        client_id=effective_client_id,
        intake_id=intake_id,
        filename=filename,
        file_bytes=file_bytes,
        metadata=metadata,
    )
    intake_dir = _intake_directory(config, effective_client_id, intake_id)
    receipt_path = intake_dir / "receipt.json"
    artifacts["receipt"] = str(receipt_path)
    results_url = f"/admin/deliveries/{intake_id}"
    artifacts.setdefault("results_url", results_url)

    parse_options = {key: value for key, value in merged_options.items() if key not in {"sync"}}

    received_at = datetime.utcnow()

    if sync_flag:
        result = await _run_parse_from_bytes(
            file_bytes,
            filename=filename,
            client_id=effective_client_id,
            adapter=parse_options.get("adapter"),
            source_hint=parse_options.get("source_hint"),
            sheet_name=parse_options.get("sheet_name"),
            enable_llm=parse_options.get("enable_llm"),
            config=config,
            options=parse_options,
        )

        artifacts.update({
            "adapter_results": json.dumps(result.adapter_results, ensure_ascii=False),
        })

        receipt = WebhookReceipt(
            intake_id=intake_id,
            client_id=effective_client_id,
            preset_id=preset_id,
            idempotency_key=idempotency_key,
            status=result.response.status,
            processing=False,
            duplicate=False,
            sync=True,
            received_at=received_at,
            filename=filename,
            notes=result.notes,
            parse=result.response,
            artifacts=artifacts,
            results_url=results_url,
        )

        webhook_store.save_receipt(receipt, client_id=effective_client_id, idempotency_key=idempotency_key)
        logger.info(
            "webhook_processed",
            intake_id=intake_id,
            client_id=effective_client_id,
            status=receipt.status,
            sync=True,
            filename=filename,
            confidence=receipt.parse.confidence if receipt.parse else None,
        )
        return receipt

    # async processing
    initial_receipt = WebhookReceipt(
        intake_id=intake_id,
        client_id=effective_client_id,
        preset_id=preset_id,
        idempotency_key=idempotency_key,
        status="queued",
        processing=True,
        duplicate=False,
        sync=False,
        received_at=received_at,
        filename=filename,
        notes=["queued"],
        parse=None,
        artifacts=artifacts,
        results_url=results_url,
    )

    webhook_store.save_receipt(initial_receipt, client_id=effective_client_id, idempotency_key=idempotency_key)

    background_tasks.add_task(
        _process_async_intake,
        file_bytes,
        filename,
        effective_client_id,
        preset_id,
        idempotency_key,
        intake_id,
        parse_options,
        config,
        artifacts,
    )

    logger.info(
        "webhook_enqueued",
        intake_id=intake_id,
        client_id=effective_client_id,
        filename=filename,
        sync=False,
    )

    return initial_receipt


@app.get("/admin/deliveries", response_model=List[DeliverySummary])
def list_deliveries_admin(
    client_id: Optional[str] = Query(default=None),
    status_filter: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    config: AppSettings = Depends(get_app_settings),
) -> List[DeliverySummary]:
    return webhook_store.list_deliveries(
        client_id=client_id,
        status_filter=status_filter,
        search=search,
        limit=limit,
    )


@app.get("/admin/deliveries/{intake_id}", response_model=WebhookReceipt)
def delivery_detail(
    intake_id: str,
    client_id: Optional[str] = Query(default=None),
    config: AppSettings = Depends(get_app_settings),
) -> WebhookReceipt:
    receipt = webhook_store.get_receipt(intake_id, client_id)
    if receipt is None:
        raise not_found("intake_not_found", "intake not found")
    return receipt


@app.get("/admin/deliveries/{intake_id}/artifacts.zip")
def download_artifacts(
    intake_id: str,
    client_id: Optional[str] = Query(default=None),
    config: AppSettings = Depends(get_app_settings),
):
    receipt = webhook_store.get_receipt(intake_id, client_id)
    if receipt is None:
        raise not_found("intake_not_found", "intake not found")
    client_folder = config.output_dir / ((receipt.client_id) or "default")
    intake_dir = client_folder / "intakes" / intake_id
    if not intake_dir.exists():
        raise not_found("artifacts_missing", "no artifacts available for this intake")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zipper:
        for file_path in intake_dir.iterdir():
            if file_path.is_file():
                zipper.write(file_path, arcname=file_path.name)
    buffer.seek(0)
    headers = {"Content-Disposition": f"attachment; filename={intake_id}-artifacts.zip"}
    return StreamingResponse(buffer, media_type="application/zip", headers=headers)


@app.post("/admin/deliveries/{intake_id}/replay", response_model=WebhookReceipt)
async def replay_delivery(
    intake_id: str,
    request: Request,
    client_id: Optional[str] = Query(default=None),
    config: AppSettings = Depends(get_app_settings),
) -> WebhookReceipt:
    original = webhook_store.get_receipt(intake_id, client_id)
    if original is None:
        raise not_found("intake_not_found", "intake not found")
    source_path = original.artifacts.get("source")
    if not source_path or not Path(source_path).exists():
        raise not_found("source_missing", "source artifact missing; cannot replay")
    file_bytes = Path(source_path).read_bytes()
    overrides_body: Dict[str, Any] = {}
    if request.headers.get("content-length") not in (None, "0"):
        data = await request.json()
        if isinstance(data, dict):
            overrides_body = data
    options = _extract_intake_options_from_mapping(overrides_body)

    new_intake_id = uuid.uuid4().hex
    client_for_replay = original.client_id
    filename = original.filename or Path(source_path).name

    artifacts = _store_source_files(
        config,
        client_id=client_for_replay,
        intake_id=new_intake_id,
        filename=filename,
        file_bytes=file_bytes,
        metadata={"replay_of": intake_id, "overrides": overrides_body},
    )
    intake_dir = config.output_dir / ((client_for_replay) or "default") / "intakes" / new_intake_id
    receipt_path = intake_dir / "receipt.json"
    artifacts["receipt"] = str(receipt_path)
    results_url = f"/admin/deliveries/{new_intake_id}"
    artifacts["results_url"] = results_url

    idempotency_key = _generate_idempotency_key(client_for_replay, file_bytes) + ":replay"
    received_at = datetime.utcnow()

    result = await _run_parse_from_bytes(
        file_bytes,
        filename=filename,
        client_id=client_for_replay,
        adapter=options.get("adapter"),
        source_hint=options.get("source_hint"),
        sheet_name=options.get("sheet_name"),
        enable_llm=options.get("enable_llm"),
        config=config,
        options=options,
    )

    replay_notes = [f"replay_of={intake_id}"] + result.notes
    artifacts["adapter_results"] = json.dumps(result.adapter_results, ensure_ascii=False)

    receipt = WebhookReceipt(
        intake_id=new_intake_id,
        client_id=client_for_replay,
        preset_id=original.preset_id,
        idempotency_key=idempotency_key,
        status=result.response.status,
        processing=False,
        duplicate=False,
        sync=True,
        received_at=received_at,
        filename=filename,
        notes=replay_notes,
        parse=result.response,
        artifacts=artifacts,
        results_url=results_url,
    )

    webhook_store.save_receipt(receipt, client_id=client_for_replay, idempotency_key=idempotency_key)
    logger.info(
        "delivery_replayed",
        replay_of=intake_id,
        new_intake_id=new_intake_id,
        client_id=client_for_replay,
        status=receipt.status,
    )
    return receipt




@app.get("/admin/presets")
def list_presets_endpoint(
    client_id: Optional[str] = Query(default=None),
    config: AppSettings = Depends(get_app_settings),
):
    presets = list_presets(config, client_id)
    return [
        {
            "client_id": preset.client_id,
            "preset_id": preset.preset_id,
            "defaults": preset.defaults,
        }
        for preset in presets
    ]


@app.post("/admin/presets", response_model=PresetPayload)
def save_preset(payload: PresetPayload, config: AppSettings = Depends(get_app_settings)) -> PresetPayload:
    if not payload.client_id or not payload.preset_id:
        raise bad_request("invalid_preset", "client_id and preset_id are required")
    path = preset_path(payload.client_id, payload.preset_id, config)
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(
        {
            "client_id": payload.client_id,
            "preset_id": payload.preset_id,
            "defaults": payload.defaults,
        },
        ensure_ascii=False,
        indent=2,
    )
    path.write_text(serialized, encoding="utf-8")
    return payload


@app.delete("/admin/presets/{client_id}/{preset_id}")
def delete_preset(client_id: str, preset_id: str, config: AppSettings = Depends(get_app_settings)):
    path = preset_path(client_id, preset_id, config)
    if not path.exists():
        raise not_found("preset_not_found", "preset does not exist")
    path.unlink()
    return {"status": "deleted"}


@app.get("/admin/settings")
def admin_settings(config: AppSettings = Depends(get_app_settings)):
    return {
        "environment": config.environment,
        "api_base_url": f"http://{config.host}:{config.port}",
        "webhook": {
            "enable": config.webhook_enable,
            "require_auth": config.webhook_require_auth,
            "async_default": config.webhook_async_default,
            "clock_skew_seconds": config.webhook_clock_skew_seconds,
        },
        "adapters": {
            "json": config.enable_json_adapter,
            "sheets": config.enable_sheets_adapter,
            "bigquery": config.enable_bigquery_adapter,
        },
        "limits": {
            "parse_max_mb": config.max_upload_size_mb,
            "webhook_max_mb": config.webhook_max_upload_size_mb,
        },
    }
@app.post("/parse/batch")
def parse_batch() -> Dict[str, str]:
    return {"status": "not_implemented"}


async def _run_parse_from_bytes(
    file_bytes: bytes,
    *,
    filename: str,
    client_id: Optional[str],
    adapter: Optional[str],
    source_hint: Optional[str],
    sheet_name: Optional[str],
    enable_llm: Optional[bool],
    config: AppSettings,
    options: Dict[str, Any],
) -> ParseExecutionResult:
    request_notes: List[str] = []
    started = time.perf_counter()

    effective_sheet_name = options.get("sheet_name") or sheet_name
    max_size = config.max_upload_size_mb * 1024 * 1024

    sample = file_reader.load_file(
        file_bytes,
        filename,
        sheet_name=effective_sheet_name,
        sample_limit=config.csv_sample_rows,
        max_size_bytes=max_size,
    )
    request_notes.append(f"detected_format={sample.detected_format}")
    if sample.sheet_choice:
        request_notes.append(f"sheet_selected={sample.sheet_choice.name}")

    effective_enable_llm = options.get("enable_llm", enable_llm)
    header_row_option = options.get("header_row")
    header_result: header_detect.HeaderDetectionResult
    if header_row_option not in (None, ""):
        try:
            header_override = int(header_row_option)
        except (TypeError, ValueError) as exc:
            raise bad_request("invalid_header_row", "header_row must be an integer >= 0") from exc
        if header_override < 0:
            raise bad_request("invalid_header_row", "header_row must be an integer >= 0")
        if header_override >= len(sample.sample_rows):
            raise bad_request(
                "header_row_out_of_range",
                "header_row outside sampled range; try uploading without override",
            )
        override_columns = [str(value).strip() for value in sample.sample_rows[header_override]]
        header_result = header_detect.HeaderDetectionResult(
            header_row=header_override,
            columns=override_columns,
            confidence=1.0,
            notes=[f"manual_header_row={header_override}"],
            used_llm=False,
        )
    else:
        header_client = build_header_client(config, effective_enable_llm)
        header_result = header_detect.detect_header(
            sample.sample_rows,
            llm_client=header_client,
            max_rows=config.header_search_rows,
        )

    effective_source_hint = options.get("source_hint") or source_hint
    rules, rule_notes = rules_loader.load_matching_rule(
        filename=filename,
        columns=header_result.columns,
        settings=config,
        source_hint=effective_source_hint,
    )
    if rule_notes:
        request_notes.extend(rule_notes)

    alias_client = build_alias_client(config, effective_enable_llm)
    alias_mapping = None
    if alias_client and header_result.columns:
        alias_samples = _build_alias_samples(header_result.header_row, header_result.columns, sample.sample_rows)
        if alias_samples:
            alias_mapping = alias_client(header_result.columns, alias_samples)

    dayfirst_option = options.get("dayfirst")
    if isinstance(dayfirst_option, str):
        parsed_dayfirst = _parse_bool_param(dayfirst_option)
        dayfirst_option = parsed_dayfirst if parsed_dayfirst is not None else None
    decimal_style_option = options.get("decimal_style")
    if isinstance(decimal_style_option, str):
        decimal_style_option = decimal_style_option.lower()
        if decimal_style_option not in {"auto", "comma", "dot"}:
            decimal_style_option = None

    normalization = normalize.normalize_table(
        sample,
        header_result.header_row,
        header_result.columns,
        settings=config,
        rules=rules,
        llm_aliases=alias_mapping,
        dayfirst=dayfirst_option if isinstance(dayfirst_option, bool) else None,
        decimal_style=decimal_style_option if isinstance(decimal_style_option, str) else None,
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
        filename=filename,
        client_id=client_id,
        detected_format=sample.detected_format,
        sheet=sample.sheet_choice.name if sample.sheet_choice else None,
    )

    table_schema = SchemaMetadata(**normalization.schema)  # type: ignore[arg-type]
    pii_meta = PIIMetadata(**normalization.pii_flags)

    response_payload: Dict[str, Any] = {
        "status": status,
        "confidence": round(overall_confidence, 3),
        "source": source,
        "table_schema": table_schema,
        "data": records,
        "notes": notes,
        "pii_detected": pii_meta,
    }

    adapter_results: List[Dict[str, Any]] = []
    effective_adapter = options.get("adapter") or adapter or config.default_adapter
    effective_adapter = effective_adapter.lower()
    dry_run = bool(options.get("dry_run"))

    if not dry_run:
        if effective_adapter == "json" and config.enable_json_adapter:
            adapter_results.append(
                json_adapter.export_json(
                    _payload_to_dict(response_payload),
                    settings=config,
                    client_id=client_id,
                    filename=filename,
                )
            )
        elif effective_adapter == "sheets":
            adapter_results.append(
                sheets_adapter.export_to_sheets(
                    normalization.dataframe,
                    settings=config,
                    worksheet_name=effective_sheet_name,
                    client_id=client_id,
                    primary_key=(rules or {}).get("primary_key") if rules else None,
                    mode=(rules or {}).get("sheets_mode") if rules else None,
                )
            )
        elif effective_adapter == "bigquery":
            adapter_results.append(
                bigquery_adapter.export_to_bigquery(
                    normalization.dataframe,
                    settings=config,
                    dataset=(rules or {}).get("bigquery_dataset") if rules else None,
                    table=(rules or {}).get("bigquery_table") if rules else None,
                    partition_field=_find_partition_field(normalization.schema),
                )
            )

    response_payload["adapter_results"] = adapter_results or None

    duration_ms = round((time.perf_counter() - started) * 1000, 2)
    row_count = int(normalization.dataframe.shape[0])
    column_count = int(normalization.dataframe.shape[1])

    rule_applied = None
    for note in notes:
        if note.startswith("rule_applied="):
            rule_applied = note.split("=", 1)[1]
            break

    return ParseExecutionResult(
        response=ParseResponse(**response_payload),
        adapter_results=adapter_results,
        notes=notes,
        rule_applied=rule_applied,
        detected_format=sample.detected_format,
        duration_ms=duration_ms,
        rows=row_count,
        cols=column_count,
    )
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


def _parse_bool_param(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    lowered = value.strip().lower()
    if lowered in {"true", "1", "yes", "y"}:
        return True
    if lowered in {"false", "0", "no", "n"}:
        return False
    return None


def _enforce_size(byte_length: int, limit_mb: int, *, error_code: str = "payload_too_large") -> None:
    if byte_length > limit_mb * 1024 * 1024:
        raise bad_request(error_code, f"payload exceeds {limit_mb}MB limit")


def _generate_idempotency_key(client_id: Optional[str], file_bytes: bytes) -> str:
    digest = hashlib.md5(file_bytes).hexdigest()
    prefix = client_id or "default"
    return f"{prefix}:{digest}"


async def _download_file_from_url(url: str, *, max_bytes: int) -> Tuple[bytes, str]:
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        response = await client.get(url, follow_redirects=True)
        if response.status_code >= 400:
            raise bad_request("download_failed", f"failed to fetch file from url ({response.status_code})")
        content = await response.aread()
    if len(content) == 0:
        raise bad_request("empty_download", "downloaded file is empty")
    if len(content) > max_bytes:
        raise bad_request("download_too_large", "download exceeds configured limit")
    filename = _filename_from_url(url)
    return content, filename


def _filename_from_url(url: str) -> str:
    name = Path(url.split("?")[0]).name
    return name or "remote.bin"


def _decode_base64(data: str) -> bytes:
    try:
        return base64.b64decode(data, validate=True)
    except Exception as exc:  # pragma: no cover - invalid payload
        raise bad_request("invalid_base64", "file_b64 must be valid base64") from exc


async def _process_async_intake(
    file_bytes: bytes,
    filename: str,
    client_id: Optional[str],
    preset_id: Optional[str],
    idempotency_key: str,
    intake_id: str,
    options: Dict[str, Any],
    config: AppSettings,
    artifacts: Dict[str, str],
) -> None:
    received_at = datetime.utcnow()
    try:
        result = await _run_parse_from_bytes(
            file_bytes,
            filename=filename,
            client_id=client_id,
            adapter=options.get("adapter"),
            source_hint=options.get("source_hint"),
            sheet_name=options.get("sheet_name"),
            enable_llm=options.get("enable_llm"),
            config=config,
            options=options,
        )
        artifacts.update({
            "adapter_results": json.dumps(result.adapter_results, ensure_ascii=False),
        })
        receipt = WebhookReceipt(
            intake_id=intake_id,
            client_id=client_id,
            preset_id=preset_id,
            idempotency_key=idempotency_key,
            status=result.response.status,
            processing=False,
            duplicate=False,
            sync=False,
            received_at=received_at,
            filename=filename,
            notes=result.notes,
            parse=result.response,
            artifacts=artifacts,
            results_url=f"/admin/deliveries/{intake_id}",
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("webhook_async_failure", intake_id=intake_id, error=str(exc))
        receipt = WebhookReceipt(
            intake_id=intake_id,
            client_id=client_id,
            preset_id=preset_id,
            idempotency_key=idempotency_key,
            status="failed",
            processing=False,
            duplicate=False,
            sync=False,
            received_at=received_at,
            filename=filename,
            notes=[f"error:{exc}"],
            parse=None,
            artifacts=artifacts,
            results_url=f"/admin/deliveries/{intake_id}",
        )

    webhook_store.save_receipt(receipt, client_id=client_id, idempotency_key=idempotency_key)


def _intake_directory(config: AppSettings, client_id: Optional[str], intake_id: str) -> Path:
    client = client_id or "default"
    intake_dir = config.output_dir / client / "intakes" / intake_id
    intake_dir.mkdir(parents=True, exist_ok=True)
    return intake_dir


def _store_source_files(
    config: AppSettings,
    *,
    client_id: Optional[str],
    intake_id: str,
    filename: str,
    file_bytes: bytes,
    metadata: Dict[str, Any] | None = None,
) -> Dict[str, str]:
    intake_dir = _intake_directory(config, client_id, intake_id)
    safe_name = Path(filename).name or "source.bin"
    source_path = intake_dir / safe_name
    source_path.write_bytes(file_bytes)
    artifacts = {"source": str(source_path)}
    if metadata is not None:
        request_path = intake_dir / "request.json"
        request_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        artifacts["request"] = str(request_path)
    return artifacts


def _verify_ip_allowlist(request: Request, config: AppSettings) -> None:
    if not config.webhook_allowed_ips:
        return
    client_host = request.client.host if request.client else None
    if client_host not in config.webhook_allowed_ips:
        raise forbidden("ip_not_allowed", "request origin not allowed", hint="configure webhook_allowed_ips")


def _extract_hmac_secret(client_id: Optional[str], config: AppSettings) -> Optional[str]:
    if client_id and client_id in config.webhook_hmac_secrets:
        return config.webhook_hmac_secrets[client_id]
    return config.webhook_hmac_secrets.get("default")


def _check_hmac_signature(
    request: Request,
    raw_body: bytes,
    client_id: Optional[str],
    config: AppSettings,
) -> bool:
    signature_header = request.headers.get("X-UTE-Signature")
    timestamp_header = request.headers.get("X-UTE-Timestamp")
    if not signature_header or not timestamp_header:
        return False
    if not signature_header.startswith("sha256="):
        raise unauthorized("invalid_signature_format", "signature must start with sha256=")

    try:
        timestamp = int(timestamp_header)
    except ValueError as exc:
        raise unauthorized("invalid_timestamp", "X-UTE-Timestamp must be epoch seconds") from exc

    now = int(time.time())
    if abs(now - timestamp) > config.webhook_clock_skew_seconds:
        raise unauthorized("timestamp_out_of_range", "timestamp outside allowed skew window")

    secret = _extract_hmac_secret(client_id, config)
    if not secret:
        raise unauthorized("missing_hmac_secret", "no HMAC secret configured for client")

    expected = hmac.new(secret.encode("utf-8"), msg=raw_body, digestmod=hashlib.sha256).hexdigest()
    provided = signature_header.split("=", 1)[1]
    if not hmac.compare_digest(expected, provided):
        raise unauthorized("signature_mismatch", "signature verification failed")
    return True


def _check_api_key(request: Request, config: AppSettings) -> bool:
    auth_header = request.headers.get("Authorization") or ""
    if not auth_header.lower().startswith("bearer "):
        return False
    token = auth_header.split(" ", 1)[1]
    if not token:
        return False
    return token in config.webhook_api_keys.values()


def _authorize_webhook(
    request: Request,
    raw_body: bytes,
    client_id: Optional[str],
    config: AppSettings,
) -> None:
    if not config.webhook_require_auth:
        return
    api = _check_api_key(request, config)
    try:
        hmac_valid = _check_hmac_signature(request, raw_body, client_id, config)
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - unexpected
        logger.warning("webhook_hmac_error", error=str(exc))
        hmac_valid = False
    if not (api or hmac_valid):
        raise unauthorized("authentication_required", "provide valid API key or HMAC signature")


def _extract_intake_options_from_mapping(payload: Dict[str, Any]) -> Dict[str, Any]:
    recognised_keys = {
        "adapter",
        "source_hint",
        "sheet_name",
        "enable_llm",
        "dry_run",
        "sync",
        "client_id",
        "preset_id",
        "dayfirst",
        "decimal_style",
        "header_row",
    }
    options: Dict[str, Any] = {}
    for key in recognised_keys:
        if key in payload:
            options[key] = payload[key]
    return options


def _resolve_sync_flag(
    sync_query: Optional[str],
    options: Dict[str, Any],
    config: AppSettings,
) -> bool:
    if sync_query is not None:
        parsed = _parse_bool_param(sync_query)
        if parsed is None:
            raise bad_request("invalid_sync_param", "sync must be true or false")
        return parsed
    if "sync" in options:
        value = options.get("sync")
        if isinstance(value, bool):
            return value
        parsed = _parse_bool_param(str(value))
        if parsed is None:
            raise bad_request("invalid_sync_param", "sync must be true or false")
        return parsed
    return not config.webhook_async_default


if UI_DIST_PATH.exists():
    assets_dir = UI_DIST_PATH / "assets"
    if assets_dir.exists():
        app.mount("/admin/assets", StaticFiles(directory=assets_dir), name="admin-assets")
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")
    index_file = UI_DIST_PATH / "index.html"

    @app.get("/admin", include_in_schema=False)
    async def serve_admin_index():
        if not index_file.exists():
            raise not_found("admin_ui_missing", "admin UI build not found")
        return FileResponse(index_file)

    @app.get("/admin/{spa_path:path}", include_in_schema=False)
    async def serve_admin_spa(spa_path: str):
        if not index_file.exists():
            raise not_found("admin_ui_missing", "admin UI build not found")
        return FileResponse(index_file)


__all__ = ["app"]
