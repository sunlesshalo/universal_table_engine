from __future__ import annotations

import re
from typing import Iterable

from unidecode import unidecode


_WORD_BREAK_RE = re.compile(r"[^0-9a-zA-Z]+")
_SNAKE_RE = re.compile(r"__+")


def strip_diacritics(value: str) -> str:
    return unidecode(value)


def to_snake_case(value: str) -> str:
    lowered = strip_diacritics(value).lower()
    parts = [segment for segment in _WORD_BREAK_RE.split(lowered) if segment]
    if not parts:
        return ""
    snake = "_".join(parts)
    snake = _SNAKE_RE.sub("_", snake)
    return snake.strip("_")


def normalize_column_name(name: str) -> str:
    snake = to_snake_case(name)
    return snake or "column"


def dedupe_names(names: Iterable[str]) -> list[str]:
    seen: dict[str, int] = {}
    result: list[str] = []
    for name in names:
        base = name or "column"
        count = seen.get(base, 0)
        if count == 0:
            result.append(base)
        else:
            result.append(f"{base}_{count + 1}")
        seen[base] = count + 1
    return result


__all__ = [
    "strip_diacritics",
    "to_snake_case",
    "normalize_column_name",
    "dedupe_names",
]
