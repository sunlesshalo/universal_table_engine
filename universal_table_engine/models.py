from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field
from pydantic.config import ConfigDict


class SourceMetadata(BaseModel):
    filename: str
    client_id: Optional[str] = None
    detected_format: Literal["csv", "xls", "xlsx"]
    sheet: Optional[str] = None


class SchemaMetadata(BaseModel):
    columns: List[str]
    types: Dict[str, str]
    aliases: Dict[str, str]
    dataset_type: Literal["financial", "orders", "marketing", "unknown"]


class PIIMetadata(BaseModel):
    email: bool
    phone: bool


class ParseResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    status: Literal["ok", "parsed_with_low_confidence", "needs_rulefile"]
    confidence: float = Field(ge=0.0, le=1.0)
    source: SourceMetadata
    table_schema: SchemaMetadata = Field(alias="schema")
    data: List[Dict[str, object]]
    notes: List[str]
    pii_detected: PIIMetadata
    adapter_results: Optional[List[Dict[str, object]]] = None


class HealthResponse(BaseModel):
    status: Literal["ok"]
    uptime_seconds: float
    timestamp: datetime


class RulesResponse(BaseModel):
    rules: List[str]


class ErrorResponse(BaseModel):
    error_code: str
    message: str
    hint: Optional[str] = None


class WebhookReceipt(BaseModel):
    intake_id: str
    client_id: Optional[str]
    preset_id: Optional[str] = None
    idempotency_key: str
    status: Literal["ok", "parsed_with_low_confidence", "queued", "failed", "needs_rulefile"]
    processing: bool
    duplicate: bool = False
    sync: bool = True
    received_at: datetime
    filename: Optional[str] = None
    notes: List[str] = []
    parse: Optional[ParseResponse] = None
    artifacts: Dict[str, str] = Field(default_factory=dict)
    results_url: Optional[str] = None


class DeliverySummary(BaseModel):
    intake_id: str
    client_id: Optional[str]
    preset_id: Optional[str] = None
    status: str
    confidence: Optional[float] = None
    received_at: datetime
    filename: Optional[str] = None
    rule_applied: Optional[str] = None
    notes: List[str] = []


class PresetPayload(BaseModel):
    client_id: str
    preset_id: str
    defaults: Dict[str, object]


__all__ = [
    "ParseResponse",
    "SourceMetadata",
    "SchemaMetadata",
    "PIIMetadata",
    "HealthResponse",
    "RulesResponse",
    "ErrorResponse",
    "WebhookReceipt",
    "DeliverySummary",
    "PresetPayload",
]
