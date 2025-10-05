from __future__ import annotations

import re
from typing import Optional

import pandas as pd  # <-- szükséges a Series-es konverterhez

# Devizajelek és gyakori 3–4 betűs kódok; kis/nagybetű független
_CURRENCY_RE = re.compile(r"(?:[€$£¥₽₴₺₦]|\b(?:lei|ron|usd|eur|gbp)\b)", re.IGNORECASE)
# Ezres elválasztók: szóköz, aposztróf, backtick, középpont, aláhúzás számok között
_THOUSANDS_RE = re.compile(r"(?<=\d)[\s'`·_](?=\d)")
# Minden, ami nem szám, pont, vessző, előjel
_NON_NUMERIC_RE = re.compile(r"[^0-9.,+-]")


def normalize_numeric_string(value: str) -> str:
    cleaned = value.strip()
    cleaned = cleaned.replace("\u00A0", " ")  # nem törhető szóköz → sima szóköz
    cleaned = _CURRENCY_RE.sub("", cleaned)   # pénznem jel/kód ki
    cleaned = cleaned.replace("%", "")        # % jel ki (percentet később kezeljük)
    cleaned = _THOUSANDS_RE.sub("", cleaned)  # ezres elválasztók ki
    cleaned = cleaned.replace(" ", "")        # maradék szóközök ki
    return cleaned


def parse_number(value: str) -> Optional[float]:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None

    # százalék megjegyzés — a végén osztjuk 100-zal
    pct = text.endswith("%")

    normalized = normalize_numeric_string(text)
    if not normalized:
        return None

    # tizedesjel meghatározása
    if normalized.count(",") and normalized.count("."):
        # ahol később fordul elő a kettő közül, az lesz a tizedes
        decimal_char = "," if normalized.rfind(",") > normalized.rfind(".") else "."
    elif normalized.count(","):
        decimal_char = ","
    else:
        decimal_char = "."

    # egységesítés pont tizedesre
    if decimal_char == ",":
        normalized = normalized.replace(".", "")   # pont: ezres
        normalized = normalized.replace(",", ".")  # vessző: tizedes
    else:
        normalized = normalized.replace(",", "")   # vessző: ezres

    # nem numerikus karakterek dobása
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
    """
    Pandas Series → float (NaN, ha nem értelmezhető).
    Kezeli: pénznem jelek/kódok, ezres elválasztók, tizedes vessző/pont, százalék.
    """
    # mindent str-re alakítunk és a parse_number-rel értelmezzük
    parsed = series.astype(str).map(parse_number)
    # to_numeric konzisztens NaN kezeléshez
    return pd.to_numeric(parsed, errors="coerce")


__all__ = [
    "normalize_numeric_string",
    "parse_number",
    "is_numeric_series",
    "coerce_numeric_series",   # <-- exportáljuk is
]

