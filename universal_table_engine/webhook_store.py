from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from .models import DeliverySummary, WebhookReceipt
from .settings import AppSettings


def _rule_from_notes(notes: List[str]) -> Optional[str]:
    for note in notes:
        if note.startswith("rule_applied="):
            return note.split("=", 1)[1]
    return None


class WebhookStore:
    def __init__(self, settings: AppSettings):
        self.settings = settings
        self._lock = threading.RLock()
        self._idempotency_cache: Dict[Tuple[str, str], Path] = {}

    def _client_root(self, client_id: Optional[str]) -> Path:
        client = client_id or "default"
        base = self.settings.output_dir / client
        (base / "intakes").mkdir(parents=True, exist_ok=True)
        (base / "receipts").mkdir(parents=True, exist_ok=True)
        return base

    def _index_path(self, client_id: Optional[str]) -> Path:
        return self._client_root(client_id) / "receipts" / "index.ndjson"

    def _receipt_path(self, client_id: Optional[str], intake_id: str) -> Path:
        return self._client_root(client_id) / "intakes" / intake_id / "receipt.json"

    def _load_index(self, client_id: Optional[str]) -> List[Dict[str, object]]:
        index_path = self._index_path(client_id)
        if not index_path.exists():
            return []
        entries: List[Dict[str, object]] = []
        with index_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    entries.append(payload)
        return entries

    def _write_index(self, client_id: Optional[str], entries: Iterable[Dict[str, object]]) -> None:
        index_path = self._index_path(client_id)
        index_path.parent.mkdir(parents=True, exist_ok=True)
        with index_path.open("w", encoding="utf-8") as handle:
            for entry in entries:
                handle.write(json.dumps(entry, ensure_ascii=False))
                handle.write("\n")

    def _cache_key(self, client_id: Optional[str], idempotency_key: str) -> Tuple[str, str]:
        return (client_id or "default", idempotency_key)

    def load_receipt(self, client_id: Optional[str], intake_id: str) -> Optional[WebhookReceipt]:
        path = self._receipt_path(client_id, intake_id)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError:
            return None
        return WebhookReceipt.model_validate(data)

    def get_receipt(self, intake_id: str, client_id: Optional[str] = None) -> Optional[WebhookReceipt]:
        if client_id is not None:
            return self.load_receipt(client_id, intake_id)
        for directory in self.settings.output_dir.iterdir():
            if not directory.is_dir():
                continue
            receipt = self.load_receipt(directory.name, intake_id)
            if receipt is not None:
                return receipt
        return None

    def find_by_idempotency(
        self, client_id: Optional[str], idempotency_key: str
    ) -> Optional[WebhookReceipt]:
        cache_key = self._cache_key(client_id, idempotency_key)
        with self._lock:
            cached_path = self._idempotency_cache.get(cache_key)
            if cached_path and cached_path.exists():
                try:
                    data = json.loads(cached_path.read_text())
                    return WebhookReceipt.model_validate(data)
                except json.JSONDecodeError:
                    pass

            entries = self._load_index(client_id)
            for entry in entries:
                if entry.get("idempotency_key") == idempotency_key:
                    receipt_path = Path(entry.get("receipt_path", ""))
                    if receipt_path.exists():
                        self._idempotency_cache[cache_key] = receipt_path
                        try:
                            data = json.loads(receipt_path.read_text())
                            return WebhookReceipt.model_validate(data)
                        except json.JSONDecodeError:
                            return None
            return None

    def save_receipt(
        self,
        receipt: WebhookReceipt,
        *,
        client_id: Optional[str],
        idempotency_key: str,
    ) -> None:
        intake_dir = self._client_root(client_id) / "intakes" / receipt.intake_id
        intake_dir.mkdir(parents=True, exist_ok=True)
        receipt_path = intake_dir / "receipt.json"
        receipt_path.write_text(
            json.dumps(receipt.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        cache_key = self._cache_key(client_id, idempotency_key)
        with self._lock:
            self._idempotency_cache[cache_key] = receipt_path
            entries = self._load_index(client_id)
            updated = False
            rule_applied = _rule_from_notes(receipt.notes)
            for entry in entries:
                if entry.get("intake_id") == receipt.intake_id:
                    entry.update(
                        {
                            "client_id": receipt.client_id,
                            "preset_id": receipt.preset_id,
                            "status": receipt.status,
                            "confidence": receipt.parse.confidence if receipt.parse else None,
                            "received_at": receipt.received_at.isoformat(),
                            "filename": receipt.filename,
                            "idempotency_key": idempotency_key,
                            "receipt_path": str(receipt_path),
                            "rule_applied": rule_applied,
                            "notes": receipt.notes[:10],
                        }
                    )
                    updated = True
                    break
            if not updated:
                entries.append(
                    {
                        "intake_id": receipt.intake_id,
                        "client_id": receipt.client_id,
                        "preset_id": receipt.preset_id,
                        "status": receipt.status,
                        "confidence": receipt.parse.confidence if receipt.parse else None,
                        "received_at": receipt.received_at.isoformat(),
                        "filename": receipt.filename,
                        "idempotency_key": idempotency_key,
                        "receipt_path": str(receipt_path),
                        "rule_applied": rule_applied,
                        "notes": receipt.notes[:10],
                    }
                )
            self._write_index(client_id, entries)

    def list_deliveries(
        self,
        client_id: Optional[str] = None,
        status_filter: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 50,
    ) -> List[DeliverySummary]:
        entries = self._load_index(client_id)
        entries.sort(key=lambda entry: entry.get("received_at", ""), reverse=True)
        summaries: List[DeliverySummary] = []
        for entry in entries:
            if status_filter and entry.get("status") != status_filter:
                continue
            if search:
                term = search.lower()
                values = [
                    str(entry.get("intake_id", "")),
                    str(entry.get("filename", "")),
                    str(entry.get("idempotency_key", "")),
                ]
                if not any(term in value.lower() for value in values):
                    continue
            try:
                received_at = datetime.fromisoformat(entry.get("received_at"))
            except (TypeError, ValueError):
                received_at = datetime.utcnow()
            summaries.append(
                DeliverySummary(
                    intake_id=entry.get("intake_id"),
                    client_id=entry.get("client_id"),
                    preset_id=entry.get("preset_id"),
                    status=entry.get("status", "unknown"),
                    confidence=entry.get("confidence"),
                    received_at=received_at,
                    filename=entry.get("filename"),
                    rule_applied=entry.get("rule_applied"),
                    notes=entry.get("notes", []),
                )
            )
            if len(summaries) >= limit:
                break
        return summaries


__all__ = ["WebhookStore"]
