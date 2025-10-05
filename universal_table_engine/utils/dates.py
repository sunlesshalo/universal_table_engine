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
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=dayfirst)
    need_extra = parsed.isna() & series.notna()
    if need_extra.any():
        normalized = series[need_extra].apply(lambda value: normalize_date(value, dayfirst=dayfirst))
        parsed.loc[need_extra] = pd.to_datetime(normalized, errors="coerce")
    return parsed


def is_date_series(values: list[str], success_threshold: float = 0.6) -> bool:
    if not values:
        return False
    success = sum(1 for value in values if parse_date(value) is not None)
    return success / len(values) >= success_threshold


def keyword_is_date(column_name: str) -> bool:
    lowered = column_name.lower()
    return any(keyword in lowered for keyword in _DATE_KEYWORDS)


__all__ = [
    "parse_date",
    "normalize_date",
    "coerce_date_series",
    "is_date_series",
    "keyword_is_date",
]
