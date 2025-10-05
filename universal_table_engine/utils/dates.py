from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd
from dateutil import parser

_DATE_KEYWORDS = {
    "date",
    "data",
    "created",
    "updated",
    "invoice",
    "order",
    "issued",
}


def parse_date(value: str, dayfirst: bool = True) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    normalized_digits = _digits_only_to_iso(text, dayfirst=dayfirst)
    if normalized_digits is not None:
        return datetime.fromisoformat(normalized_digits)

    try:
        return parser.parse(text, dayfirst=dayfirst)
    except (ValueError, OverflowError, TypeError):
        return None


def normalize_date(value: str, dayfirst: bool = True) -> Optional[str]:
    parsed = parse_date(value, dayfirst=dayfirst)
    if parsed is None:
        return None
    return parsed.replace(microsecond=0).isoformat()


def coerce_date_series(series: pd.Series, dayfirst: bool = True) -> pd.Series:
    text_series = series.astype(str).str.strip()
    parsed = pd.to_datetime(text_series, errors="coerce", dayfirst=dayfirst, utc=False)

    mask = parsed.isna() & text_series.ne("")
    if mask.any():
        supplemental = text_series[mask].apply(lambda value: _digits_only_to_iso(value, dayfirst=dayfirst))
        parsed.loc[mask] = pd.to_datetime(supplemental, errors="coerce", utc=False)

    return parsed


def is_date_series(values: list[str], success_threshold: float = 0.6) -> bool:
    if not values:
        return False
    success = sum(1 for value in values if parse_date(value) is not None)
    return success / len(values) >= success_threshold


def keyword_is_date(column_name: str) -> bool:
    lowered = column_name.lower()
    return any(keyword in lowered for keyword in _DATE_KEYWORDS)


def _digits_only_to_iso(value: str, dayfirst: bool = True) -> Optional[str]:
    digits = "".join(ch for ch in value if ch.isdigit())
    if not digits:
        return None
    if len(digits) == 7:
        digits = digits.zfill(8)
    if len(digits) != 8:
        return None
    if dayfirst:
        day, month, year = digits[0:2], digits[2:4], digits[4:8]
    else:
        month, day, year = digits[0:2], digits[2:4], digits[4:8]
    try:
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}T00:00:00"
    except ValueError:
        return None


__all__ = [
    "parse_date",
    "normalize_date",
    "coerce_date_series",
    "is_date_series",
    "keyword_is_date",
]
