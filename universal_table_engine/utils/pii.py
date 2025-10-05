from __future__ import annotations

import re
from typing import Iterable, Tuple

import pandas as pd
import pandas.api.types as ptypes

# --- Email minta ---
_EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)

# --- Telefon minta (törzs) ---
# Legalább 10 számjegy összesen; megengedett +, szóköz, -, (), .
_PHONE_BODY_RE = re.compile(r"(?:\+?\d[\d\-\s().]{8,}\d)")

# ISO dátumok, amelyeket ki akarunk zárni a telefon-detektálásból
_ISO_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_iso_date_like(text: str) -> bool:
    return bool(_ISO_DATETIME_RE.match(text) or _ISO_DATE_RE.match(text))


# ----------------------
# Maszkoló függvények
# ----------------------
def mask_email(value: str) -> str:
    local, _, domain = value.partition("@")
    if not local or not domain:
        return value
    if len(local) <= 2:
        masked_local = "*" * len(local)
    else:
        masked_local = local[0] + "*" * (len(local) - 2) + local[-1]
    return f"{masked_local}@{domain}"


def mask_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) <= 4:
        return "*" * len(digits)
    return "*" * (len(digits) - 2) + digits[-2:]


# ----------------------
# Detektálók (egyetlen értékre)
# ----------------------
def contains_email(value: str) -> bool:
    return bool(_EMAIL_RE.search(value or ""))


def contains_phone(value: str) -> bool:
    if not value:
        return False
    text = str(value).strip()

    # ISO dátumok kizárása (különben téves pozitív lehet)
    if _is_iso_date_like(text):
        return False

    # Telefonnak csak reális hosszú számsorok számítsanak
    digits = re.sub(r"\D", "", text)
    if not (10 <= len(digits) <= 15):
        return False

    return bool(_PHONE_BODY_RE.search(text))


# ----------------------
# Sorozat / DataFrame szintű PII scan
# ----------------------
def scan_series(series: Iterable[str]) -> Tuple[bool, bool]:
    any_email = False
    any_phone = False
    for value in series:
        if value is None:
            continue
        text = str(value)
        if not any_email and contains_email(text):
            any_email = True
        if not any_phone and contains_phone(text):
            any_phone = True
        if any_email and any_phone:
            break
    return any_email, any_phone


def detect_pii_frame(df: pd.DataFrame) -> dict[str, bool]:
    """
    Csak szöveg-oszlopokat vizsgáljunk PII-re; numerikus és dátum oszlopokat hagyjuk ki.
    """
    email = False
    phone = False
    for column in df.columns:
        col = df[column]
        # Skip: datetime és numerikus oszlopok
        if ptypes.is_datetime64_any_dtype(col) or ptypes.is_numeric_dtype(col):
            continue
        # A többieket szövegként vizsgáljuk
        col_email, col_phone = scan_series(col.astype(str, errors="ignore"))
        email = email or col_email
        phone = phone or col_phone
        if email and phone:
            break
    return {"email": email, "phone": phone}


def maybe_mask_value(value: str, mask_email_flag: bool, mask_phone_flag: bool) -> str:
    if value is None:
        return value
    text = str(value)
    if mask_email_flag and contains_email(text):
        match = _EMAIL_RE.search(text)
        if match:
            email = match.group(0)
            text = text.replace(email, mask_email(email))
    if mask_phone_flag and contains_phone(text):
        match = _PHONE_BODY_RE.search(text)
        if match:
            phone = match.group(0)
            text = text.replace(phone, mask_phone(phone))
    return text


__all__ = [
    "mask_email",
    "mask_phone",
    "contains_email",
    "contains_phone",
    "scan_series",
    "detect_pii_frame",
    "maybe_mask_value",
]

