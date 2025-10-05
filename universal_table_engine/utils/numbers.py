from __future__ import annotations

import re
from typing import Optional

import pandas as pd

# Currency symbols and common codes; case-insensitive
_CURRENCY_RE = re.compile(r"(?:[€$£¥₽₴₺₦]|\b(?:lei|ron|usd|eur|gbp)\b)", re.IGNORECASE)
# Thousands separators allowed between digits
_THOUSANDS_RE = re.compile(r"(?<=\d)[\s'`·_](?=\d)")
# Characters to strip (everything except digits, signs, comma, dot)
_NON_NUMERIC_RE = re.compile(r"[^0-9.,+-]")


def normalize_numeric_string(value: str) -> str:
    cleaned = value.strip()
    cleaned = cleaned.replace("\u00A0", " ")  # normalize non-breaking space
    cleaned = _CURRENCY_RE.sub("", cleaned)
    cleaned = cleaned.replace("%", "")
    cleaned = _THOUSANDS_RE.sub("", cleaned)
    cleaned = cleaned.replace(" ", "")
    return cleaned


def parse_number(value: str) -> Optional[float]:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None

    pct = text.endswith("%")

    normalized = normalize_numeric_string(text)
    if not normalized:
        return None

    if normalized.count(",") and normalized.count("."):
        decimal_char = "," if normalized.rfind(",") > normalized.rfind(".") else "."
    elif normalized.count(","):
        decimal_char = ","
    else:
        decimal_char = "."

    if decimal_char == ",":
        normalized = normalized.replace(".", "")
        normalized = normalized.replace(",", ".")
    else:
        normalized = normalized.replace(",", "")

    normalized = _NON_NUMERIC_RE.sub("", normalized)
    if normalized in {"", ".", "+", "-", "-."}:
        return None

    try:
        number = float(normalized)
    except ValueError:
        return None

    return number / 100 if pct else number


def is_numeric_series(values: list[str], success_threshold: float = 0.6) -> bool:
    if not values:
        return False
    success = sum(1 for value in values if parse_number(value) is not None)
    return success / len(values) >= success_threshold


def coerce_numeric_series(series: pd.Series) -> pd.Series:
    """Convert text-like numeric series to floats with NaN for invalid entries."""
    parsed = series.astype(str).map(parse_number)
    return pd.to_numeric(parsed, errors="coerce")


__all__ = [
    "normalize_numeric_string",
    "parse_number",
    "is_numeric_series",
    "coerce_numeric_series",
]
