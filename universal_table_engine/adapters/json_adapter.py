from __future__ import annotations

import gzip
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from ..models import AdapterResult, ArtifactDescriptor
from ..settings import AppSettings
from ..utils.bigquery import sanitize_field_name

NDJSON_VERSION = "1"


def export_json(
    payload: Dict[str, Any],
    *,
    settings: AppSettings,
    client_id: Optional[str],
    filename: str,
    field_map: Optional[Dict[str, str]] = None,
) -> List[AdapterResult]:
    """Persist the envelope and NDJSON sidecar according to configuration."""

    exports = {name.strip().lower() for name in settings.json_exports or []}
    if not exports:
        return []

    target_dir = settings.output_dir / (client_id or "default")
    target_dir.mkdir(parents=True, exist_ok=True)
    base_name = Path(filename or "data").stem or "data"

    rows = _coerce_rows(payload.get("data"))
    row_count = len(rows)
    schema = payload.get("schema")
    notes = payload.get("notes") or []
    source = payload.get("source")

    results: List[AdapterResult] = []

    if "envelope" in exports:
        envelope_path = target_dir / f"{base_name}.json"
        raw_envelope = json.dumps(payload, ensure_ascii=True, indent=2)
        _atomic_write_bytes(envelope_path, raw_envelope.encode("utf-8"))
        results.append(
            AdapterResult(
                adapter="json",
                status="ok",
                artifacts=[
                    ArtifactDescriptor(
                        name="envelope",
                        path=str(envelope_path),
                        size_bytes=envelope_path.stat().st_size,
                        content_type="application/json",
                        meta={"rows": row_count},
                    )
                ],
            )
        )

    if "ndjson" in exports:
        ndjson_filename = f"{base_name}.ndjson"
        content_type = "application/x-ndjson"
        if settings.json_ndjson_gzip:
            ndjson_filename += ".gz"
            content_type = "application/gzip"
        ndjson_path = target_dir / ndjson_filename

        sanitized_rows = _apply_field_map(rows, field_map)
        prepared_rows, content_hash = _encode_rows(
            sanitized_rows,
            drop_nulls=settings.json_ndjson_drop_nulls,
        )
        created_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        meta_line = json.dumps(
            {
                "type": "meta",
                "client_id": client_id,
                "source": source,
                "filename": filename,
                "rows": row_count,
                "schema": schema,
                "notes": notes,
                "created_at": created_at,
                "ndjson_version": NDJSON_VERSION,
                "content_hash": content_hash,
                "sanitized_columns": field_map or {},
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )

        lines = [meta_line] + prepared_rows
        payload_bytes = ("\n".join(lines) + "\n").encode("utf-8")
        if settings.json_ndjson_gzip:
            payload_bytes = gzip.compress(payload_bytes)
        _atomic_write_bytes(ndjson_path, payload_bytes)

        results.append(
            AdapterResult(
                adapter="ndjson",
                status="ok",
                notes=[
                    "ndjson_written",
                    f"ndjson_gzip={'true' if settings.json_ndjson_gzip else 'false'}",
                    "ndjson_sanitized",
                ],
                artifacts=[
                    ArtifactDescriptor(
                        name="ndjson",
                        path=str(ndjson_path),
                        size_bytes=ndjson_path.stat().st_size,
                        content_type=content_type,
                        meta={
                            "rows": row_count,
                            "gzip": settings.json_ndjson_gzip,
                            "content_hash": content_hash,
                            "created_at": created_at,
                            "sanitized_columns": field_map or {},
                        },
                    )
                ],
            )
        )

    return results


def _coerce_rows(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, Sequence):
        result: List[Dict[str, Any]] = []
        for item in data:
            if isinstance(item, dict):
                result.append(dict(item))
        return result
    return []


def _apply_field_map(rows: Iterable[Dict[str, Any]], field_map: Optional[Dict[str, str]]) -> List[Dict[str, Any]]:
    if not field_map:
        return [
            {sanitize_field_name(key): value for key, value in row.items()}
            for row in rows
        ]
    sanitized: List[Dict[str, Any]] = []
    for row in rows:
        mapped: Dict[str, Any] = {}
        for key, value in row.items():
            target = field_map.get(key, sanitize_field_name(key))
            mapped[target] = value
        sanitized.append(mapped)
    return sanitized


def _encode_rows(rows: Iterable[Dict[str, Any]], *, drop_nulls: bool) -> (List[str], str):
    prepared: List[str] = []
    digest = hashlib.sha256()
    for row in rows:
        candidate = row if not drop_nulls else {k: v for k, v in row.items() if v is not None}
        line = json.dumps(candidate, ensure_ascii=False, separators=(",", ":"))
        prepared.append(line)
        digest.update(line.encode("utf-8"))
        digest.update(b"\n")
    return prepared, digest.hexdigest()


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    tmp_path = path.with_name(path.name + ".tmp")
    with open(tmp_path, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


__all__ = ["export_json", "NDJSON_VERSION"]
