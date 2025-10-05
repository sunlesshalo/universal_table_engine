from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import httpx

from ..settings import AppSettings

HeaderLLMClient = Callable[[List[List[str]]], Optional["HeaderPrediction"]]
AliasLLMClient = Callable[[List[str], List[Dict[str, str]]], Optional[Dict[str, str]]]


@dataclass(slots=True)
class HeaderPrediction:
    header_row: int
    columns: List[str]
    confidence: float


def build_header_client(settings: AppSettings, enable_override: Optional[bool]) -> Optional[HeaderLLMClient]:
    enabled = settings.enable_llm if enable_override is None else enable_override
    if not enabled or not settings.llm_api_key:
        return None
    provider = settings.llm_provider or "openai"
    if provider != "openai":
        return None

    def _client(rows: List[List[str]]) -> Optional[HeaderPrediction]:
        return _request_header_prediction(rows, settings)

    return _client


def build_alias_client(settings: AppSettings, enable_override: Optional[bool]) -> Optional[AliasLLMClient]:
    enabled = settings.enable_llm if enable_override is None else enable_override
    if not enabled or not settings.llm_api_key:
        return None
    provider = settings.llm_provider or "openai"
    if provider != "openai":
        return None

    def _client(columns: List[str], sample_rows: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
        return _request_alias_prediction(columns, sample_rows, settings)

    return _client


def _request_header_prediction(rows: List[List[str]], settings: AppSettings) -> Optional[HeaderPrediction]:
    prompt = _format_rows_for_prompt(rows)
    messages = [
        {
            "role": "system",
            "content": "You are a CSV structure recognizer. Reply with JSON only, schema: {header_row:int, columns:list, confidence:number}.",
        },
        {
            "role": "user",
            "content": (
                "Identify the header row index (0-based) and the cleaned column names for the table in the text below."
                " Respond with JSON only.\n" + prompt
            ),
        },
    ]
    try:
        response = _call_openai_chat(messages, settings)
    except Exception:
        return None
    if not response:
        return None
    try:
        payload = json.loads(response)
    except json.JSONDecodeError:
        cleaned = _extract_json(response)
        if cleaned is None:
            return None
        payload = cleaned
    header_row = int(payload.get("header_row", 0))
    columns = payload.get("columns") or []
    if not isinstance(columns, list):
        columns = []
    columns = [str(item).strip() for item in columns if str(item).strip()]
    confidence = float(payload.get("confidence", 0.5))
    confidence = max(0.0, min(confidence, 1.0))
    return HeaderPrediction(header_row=header_row, columns=columns, confidence=confidence)


def _request_alias_prediction(
    columns: List[str], sample_rows: List[Dict[str, str]], settings: AppSettings
) -> Optional[Dict[str, str]]:
    preview = json.dumps({"columns": columns, "samples": sample_rows[:5]}, ensure_ascii=False)
    messages = [
        {
            "role": "system",
            "content": (
                "Map given columns + sample rows to canonical aliases (amount, date, invoice_number, order_id, "
                "customer_email, customer_name, vat, quantity, region, payment_method, status). JSON only."
            ),
        },
        {
            "role": "user",
            "content": (
                "Return {\"aliases\": {src: alias}, \"confidence\": number between 0 and 1}. Exclude columns"
                " you are unsure about.\n" + preview
            ),
        },
    ]
    try:
        response = _call_openai_chat(messages, settings)
    except Exception:
        return None
    if not response:
        return None
    try:
        payload = json.loads(response)
    except json.JSONDecodeError:
        cleaned = _extract_json(response)
        if cleaned is None:
            return None
        payload = cleaned
    aliases = payload.get("aliases", {})
    if not isinstance(aliases, dict):
        return None
    result = {str(key).strip(): str(value).strip() for key, value in aliases.items() if str(value).strip()}
    return result or None


def _call_openai_chat(messages: List[Dict[str, str]], settings: AppSettings) -> Optional[str]:
    headers = {
        "Authorization": f"Bearer {settings.llm_api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": settings.llm_model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": messages,
    }
    timeout = settings.llm_timeout_seconds
    with httpx.Client(timeout=timeout) as client:
        response = client.post("https://api.openai.com/v1/chat/completions", headers=headers, json=body)
        if response.status_code >= 400:
            return None
        data = response.json()
    choices = data.get("choices") or []
    if not choices:
        return None
    message = choices[0].get("message", {})
    return message.get("content")


def _format_rows_for_prompt(rows: List[List[str]], limit: int = 25) -> str:
    compiled: List[str] = []
    for idx, row in enumerate(rows[:limit]):
        cells = [str(cell).replace("\n", " ").strip() for cell in row]
        compiled.append(f"Row {idx}: {', '.join(cells)}")
    return "\n".join(compiled)


def _extract_json(raw: str) -> Optional[dict]:
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        return json.loads(raw[start : end + 1])
    except json.JSONDecodeError:
        return None


__all__ = [
    "HeaderPrediction",
    "HeaderLLMClient",
    "AliasLLMClient",
    "build_header_client",
    "build_alias_client",
]
