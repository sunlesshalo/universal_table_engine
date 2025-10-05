from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import pandas as pd

from ..settings import AppSettings
from ..utils import dates, numbers, pii, text
from .file_reader import FileSample
from .validators import drop_empty_columns, ensure_minimum_rows, sanitize_dataframe

BOOLEAN_TRUE = {"true", "yes", "y", "1", "da", "ok", "igen"}
BOOLEAN_FALSE = {"false", "no", "n", "0", "nu", "nem"}
DATE_HINTS = ("date", "data", "issued", "invoice_date", "created", "created_at")
NUMBER_HINTS = ("amount", "total", "valoare", "price", "pret", "vat", "tva", "qty", "quantity")


@dataclass(slots=True)
class NormalizationResult:
    dataframe: pd.DataFrame
    schema: Dict[str, object]
    notes: List[str]
    confidence: float
    pii_flags: Dict[str, bool]
    status_hint: str


def normalize_table(
    sample: FileSample,
    header_row: int,
    raw_columns: Iterable[str],
    *,
    settings: AppSettings,
    rules: Optional[dict] = None,
    llm_aliases: Optional[Dict[str, str]] = None,
) -> NormalizationResult:
    original_columns = list(raw_columns)
    df = _read_dataframe(sample, header_row)
    df = drop_empty_columns(df)
    df = sanitize_dataframe(df)

    cleaned_columns, column_notes = _clean_columns(list(df.columns))
    df.columns = cleaned_columns

    conversions, type_labels, type_confidences, type_notes = _convert_columns(df, cleaned_columns)
    df = conversions

    pii_flags = pii.detect_pii_frame(df)
    if settings.mask_pii:
        df = _mask_pii(df)

    aliases, alias_notes = _build_aliases(cleaned_columns, rules, llm_aliases)
    dataset_type = _infer_dataset_type(aliases, rules)

    notes = column_notes + type_notes + alias_notes
    if original_columns and len(original_columns) != len(cleaned_columns):
        notes.append("columns_count_adjusted")
    notes.append(f"header_assumed_row={header_row}")

    ensure_minimum_rows(df)
    avg_confidence = float(sum(type_confidences.values()) / max(len(type_confidences), 1))

    schema = {
        "columns": cleaned_columns,
        "types": type_labels,
        "aliases": aliases,
        "dataset_type": dataset_type,
    }

    status_hint = "ok" if avg_confidence >= 0.65 else "parsed_with_low_confidence"

    return NormalizationResult(
        dataframe=df,
        schema=schema,
        notes=list(dict.fromkeys(notes)),
        confidence=avg_confidence,
        pii_flags=pii_flags,
        status_hint=status_hint,
    )


def _read_dataframe(sample: FileSample, header_row: int) -> pd.DataFrame:
    if sample.detected_format == "csv":
        buffer = sample.open_text()
        buffer.seek(0)
        df = pd.read_csv(
            buffer,
            sep=sample.delimiter or None,
            header=header_row,
            dtype=str,
            keep_default_na=False,
            na_values=[""],
            engine="python",
        )
    else:
        sheet = sample.sheet_choice.name if sample.sheet_choice else 0
        df = pd.read_excel(
            sample.open_bytes(),
            sheet_name=sheet,
            header=header_row,
            dtype=str,
        )
    df = df.fillna("")
    return df


def _clean_columns(columns: List[str]) -> tuple[List[str], List[str]]:
    normalized = [text.normalize_column_name(name) for name in columns]
    deduped = text.dedupe_names(normalized)
    notes: List[str] = []
    if deduped != normalized:
        notes.append("columns_deduped")
    if any(name != original for name, original in zip(deduped, columns)):
        notes.append("columns_normalized")
    return deduped, notes


def _convert_columns(
    df: pd.DataFrame, columns: List[str]
) -> tuple[pd.DataFrame, Dict[str, str], Dict[str, float], List[str]]:
    output = pd.DataFrame(index=df.index)
    type_labels: Dict[str, str] = {}
    confidences: Dict[str, float] = {}
    notes: List[str] = []

    for column in columns:
        series = df[column]
        converted, type_label, confidence, column_notes = _convert_series(series, column)
        output[column] = converted
        type_labels[column] = type_label
        confidences[column] = confidence
        notes.extend(column_notes)

    return output, type_labels, confidences, list(dict.fromkeys(notes))


def _convert_series(series: pd.Series, column_name: str) -> tuple[pd.Series, str, float, List[str]]:
    text_series = series.astype(str)
    stripped = text_series.str.strip()
    non_empty_mask = stripped.ne("")
    non_empty_count = int(non_empty_mask.sum())
    notes: List[str] = []

    if non_empty_count == 0:
        return text_series.mask(non_empty_mask, None), "string", 0.3, notes

    bool_series, bool_conf, bool_note = _attempt_bool(stripped)
    if bool_series is not None and bool_conf >= 0.7:
        if bool_note:
            notes.append(bool_note)
        return bool_series, "boolean", bool_conf, notes

    lower_name = column_name.lower()
    is_date_hint = any(token in lower_name for token in DATE_HINTS)
    is_number_hint = any(token in lower_name for token in NUMBER_HINTS)

    if is_date_hint:
        coerced_dates = dates.coerce_date_series(text_series, dayfirst=True)
        success = int(coerced_dates.notna().sum())
        if success:
            confidence = success / non_empty_count if non_empty_count else 0.0
            notes.append("dates_normalized")
            return coerced_dates, "date", max(confidence, 0.7), notes

    if is_number_hint:
        coerced_numbers = numbers.coerce_numeric_series(text_series)
        success = int(coerced_numbers.notna().sum())
        if success:
            confidence = success / non_empty_count if non_empty_count else 0.0
            if any(("," in value and "." not in value) for value in stripped[non_empty_mask]):
                notes.append("decimal_comma_normalized")
            else:
                notes.append("numbers_normalized")
            return coerced_numbers, "number", max(confidence, 0.7), notes

    numeric_candidate = numbers.coerce_numeric_series(text_series)
    numeric_success = int(numeric_candidate.notna().sum())
    numeric_conf = numeric_success / non_empty_count if non_empty_count else 0.0
    if numeric_conf >= 0.6:
        if any(("," in value and "." not in value) for value in stripped[non_empty_mask]):
            notes.append("decimal_comma_normalized")
        else:
            notes.append("numbers_normalized")
        return numeric_candidate, "number", numeric_conf, notes

    date_candidate = dates.coerce_date_series(text_series, dayfirst=True)
    date_success = int(date_candidate.notna().sum())
    date_conf = date_success / non_empty_count if non_empty_count else 0.0
    if date_conf >= 0.5 or (is_date_hint and date_success):
        notes.append("dates_normalized")
        return date_candidate, "date", max(date_conf, 0.5), notes

    return text_series.mask(stripped.eq(""), None), "string", 0.5, notes


def _attempt_bool(stripped: pd.Series) -> tuple[Optional[pd.Series], float, Optional[str]]:
    mapped: List[Optional[bool]] = []
    filtered: List[str] = []
    success = 0

    for value in stripped.tolist():
        key = value.lower()
        if key:
            filtered.append(key)
        if key in BOOLEAN_TRUE:
            mapped.append(True)
            success += 1
        elif key in BOOLEAN_FALSE:
            mapped.append(False)
            success += 1
        elif key == "":
            mapped.append(None)
        else:
            mapped.append(None)

    if not filtered:
        return None, 0.0, None

    confidence = success / len(filtered)
    if confidence >= 0.7:
        return pd.Series(mapped, dtype="boolean"), confidence, "boolean_normalized"
    return None, 0.0, None


def _build_aliases(
    columns: List[str],
    rules: Optional[dict],
    llm_aliases: Optional[Dict[str, str]],
) -> tuple[Dict[str, str], List[str]]:
    notes: List[str] = []
    mapping: Dict[str, str] = {}
    heuristic = _heuristic_aliases(columns)
    if heuristic:
        notes.append("aliases_from_heuristic")
    mapping.update(heuristic)

    if llm_aliases:
        mapping.update(llm_aliases)
        notes.append("aliases_from_llm")

    if rules and rules.get("column_aliases"):
        mapping.update({text.normalize_column_name(k): v for k, v in rules["column_aliases"].items()})
        notes.append("aliases_from_rules")

    cleaned = {column: mapping.get(column, column) for column in columns}
    return cleaned, notes


def _heuristic_aliases(columns: Iterable[str]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for column in columns:
        lowered = column.lower()
        if any(token in lowered for token in {"amount", "total", "value", "sum"}):
            result[column] = "amount"
        elif any(token in lowered for token in {"date", "data"}):
            result[column] = "date"
        elif "invoice" in lowered:
            result[column] = "invoice_number"
        elif "order" in lowered:
            result[column] = "order_id"
        elif "email" in lowered:
            result[column] = "customer_email"
        elif any(token in lowered for token in {"client", "customer"}):
            result[column] = "customer_name"
        elif "vat" in lowered:
            result[column] = "vat"
        elif any(token in lowered for token in {"qty", "quantity", "cantitate"}):
            result[column] = "quantity"
        elif "region" in lowered:
            result[column] = "region"
        elif "payment" in lowered:
            result[column] = "payment_method"
        elif "status" in lowered:
            result[column] = "status"
    return result


def _infer_dataset_type(aliases: Dict[str, str], rules: Optional[dict]) -> str:
    if rules and rules.get("dataset_type"):
        return str(rules["dataset_type"])
    alias_values = set(aliases.values())
    if {"amount", "vat", "invoice_number"} & alias_values:
        return "financial"
    if {"order_id", "quantity"} & alias_values:
        return "orders"
    if {"customer_email", "customer_name"} <= alias_values:
        return "marketing"
    return "unknown"


def _mask_pii(df: pd.DataFrame) -> pd.DataFrame:
    masked = df.copy()
    for column in masked.columns:
        masked[column] = masked[column].apply(
            lambda value: pii.maybe_mask_value(value, True, True) if isinstance(value, str) else value
        )
    return masked


__all__ = ["NormalizationResult", "normalize_table"]
