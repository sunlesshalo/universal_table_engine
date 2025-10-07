from __future__ import annotations

import re
from typing import Dict, Sequence


_SANITIZE_PATTERN = re.compile(r"[^A-Za-z0-9_]")


def sanitize_field_name(name: str) -> str:
    sanitized = _SANITIZE_PATTERN.sub("_", name or "")
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    if not sanitized:
        sanitized = "field"
    if sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized.lower()


def build_bigquery_field_map(columns: Sequence[str]) -> Dict[str, str]:
    used: Dict[str, int] = {}
    mapping: Dict[str, str] = {}
    for column in columns:
        base = sanitize_field_name(column)
        counter = used.get(base, 0)
        candidate = base if counter == 0 else f"{base}_{counter}"
        while candidate in used:
            counter += 1
            candidate = f"{base}_{counter}"
        used[base] = counter + 1
        used[candidate] = 1
        mapping[column] = candidate
    return mapping


__all__ = ["sanitize_field_name", "build_bigquery_field_map"]
