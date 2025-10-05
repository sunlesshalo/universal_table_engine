from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from ..settings import AppSettings
from ..utils import dates, numbers, pii, text
from .file_reader import FileSample
from .validators import drop_empty_columns, ensure_minimum_rows, sanitize_dataframe

BOOLEAN_TRUE = {"true", "yes", "y", "1", "da", "ok"}
BOOLEAN_FALSE = {"false", "no", "n", "0", "nu"}


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

    if raw_columns and len(list(raw_columns)) != len(df.columns):
        pass

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
        notes=notes,
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
    notes = []
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
        converted, type_label, confidence, note = _convert_series(series, column)
        output[column] = converted
        type_labels[column] = type_label
        confidences[column] = confidence
        if note:
            notes.append(note)
    return output, type_labels, confidences, notes


def _convert_series(series: pd.Series, column_name: str) -> tuple[pd.Series, str, float, Optional[str]]:
    values = series.astype(str).tolist()
    stripped = [value.strip() for value in values]
    filtered = [value for value in stripped if value]

    if not filtered:
        return series.where(series != "", None), "string", 0.3, None

    bool_series, bool_conf, bool_note = _attempt_bool(series, filtered)
    if bool_series is not None:
        return bool_series, "boolean", bool_conf, bool_note

    prioritized_date = dates.keyword_is_date(column_name)

    if prioritized_date:
        date_series, date_conf, date_note = _attempt_date(series, filtered, column_name)
        if date_series is not None:
            return date_series, "date", date_conf, date_note

    number_series, number_conf, number_note = _attempt_number(series, filtered)
    if number_series is not None:
        return number_series, "number", number_conf, number_note

    if not prioritized_date:
        date_series, date_conf, date_note = _attempt_date(series, filtered, column_name)
        if date_series is not None:
            return date_series, "date", date_conf, date_note

    return series.where(series != "", None), "string", 0.5, None


def _attempt_bool(series: pd.Series, filtered: List[str]) -> tuple[Optional[pd.Series], float, Optional[str]]:
    mapped: List[Optional[bool]] = []
    success = 0
    for value in series.astype(str).tolist():
        key = value.strip().lower()
        if key in BOOLEAN_TRUE:
            mapped.append(True)
            success += 1
        elif key in BOOLEAN_FALSE:
            mapped.append(False)
            success += 1
        elif not key:
            mapped.append(None)
        else:
            mapped.append(None)
    if not filtered:
        return pd.Series(mapped), 0.5, None
    confidence = success / max(len(filtered), 1)
    if confidence >= 0.7:
        return pd.Series(mapped), confidence, "boolean_normalized"
    return None, 0.0, None


def _attempt_number(series: pd.Series, filtered: List[str]) -> tuple[Optional[pd.Series], float, Optional[str]]:
    converted: List[Optional[float]] = []
    success = 0
    decimal_note = False
    for value in series.astype(str).tolist():
        parsed = numbers.parse_number(value)
        if parsed is not None:
            converted.append(parsed)
            success += 1
            if "," in value and "." not in value:
                decimal_note = True
        else:
            converted.append(None)
    if not filtered:
        return None, 0.0, None
    confidence = success / max(len(filtered), 1)
    if confidence >= 0.6:
        note = "decimal_comma_normalized" if decimal_note else None
        return pd.Series(converted), confidence, note
    return None, 0.0, None


def _attempt_date(series: pd.Series, filtered: List[str], column_name: str) -> tuple[Optional[pd.Series], float, Optional[str]]:
    converted: List[Optional[str]] = []
    success = 0
    for value in series.astype(str).tolist():
        normalized = dates.normalize_date(value, dayfirst=True)
        if normalized is not None:
            converted.append(normalized)
            success += 1
        else:
            converted.append(None)
    if not filtered:
        return None, 0.0, None
    confidence = success / max(len(filtered), 1)
    if confidence >= 0.5 or dates.keyword_is_date(column_name):
        return pd.Series(converted), max(confidence, 0.5), "dates_normalized"
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
