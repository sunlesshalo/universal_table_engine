from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field


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
    status: Literal["ok", "parsed_with_low_confidence", "needs_rulefile"]
    confidence: float = Field(ge=0.0, le=1.0)
    source: SourceMetadata
    schema: SchemaMetadata
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


__all__ = [
    "ParseResponse",
    "SourceMetadata",
    "SchemaMetadata",
    "PIIMetadata",
    "HealthResponse",
    "RulesResponse",
]
