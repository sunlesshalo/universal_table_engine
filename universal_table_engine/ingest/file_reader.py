from __future__ import annotations

import csv
from dataclasses import dataclass
from io import BytesIO, StringIO
from typing import Iterable, Literal, Optional

import chardet
import pandas as pd

try:
    import magic  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    magic = None

from .sheet_picker import SheetChoice, pick_sheet


DetectedFormat = Literal["csv", "xls", "xlsx"]


@dataclass(slots=True)
class FileSample:
    filename: str
    raw_bytes: bytes
    detected_format: DetectedFormat
    encoding: Optional[str]
    delimiter: Optional[str]
    sample_rows: list[list[str]]
    sheet_choice: Optional[SheetChoice]
    size_bytes: int

    def open_text(self) -> StringIO:
        if self.detected_format != "csv":
            raise ValueError("text stream only available for CSV files")
        encoding = self.encoding or "utf-8"
        text = self.raw_bytes.decode(encoding, errors="ignore")
        return StringIO(text)

    def open_bytes(self) -> BytesIO:
        return BytesIO(self.raw_bytes)


def detect_format(filename: str, file_bytes: bytes) -> DetectedFormat:
    lowered = filename.lower()
    if lowered.endswith(".csv"):
        return "csv"
    if lowered.endswith(".xls"):
        return "xls"
    if lowered.endswith(".xlsx"):
        return "xlsx"
    if magic is not None:
        mime = magic.from_buffer(file_bytes, mime=True)
        if mime in {"text/csv", "text/plain"}:
            return "csv"
        if mime in {"application/vnd.ms-excel"}:
            return "xls"
        if mime in {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}:
            return "xlsx"
    raise ValueError("Unsupported file type. Only CSV, XLS, XLSX are accepted.")


def detect_encoding(file_bytes: bytes) -> Optional[str]:
    detection = chardet.detect(file_bytes[:100_000])
    encoding = detection.get("encoding")
    if encoding:
        return encoding
    return None


def sniff_delimiter(text: str) -> Optional[str]:
    try:
        dialect = csv.Sniffer().sniff(text, delimiters=",;\t|:")
        return dialect.delimiter
    except (csv.Error, TypeError):
        return None


def sample_csv_rows(text: str, limit: int) -> list[list[str]]:
    reader = csv.reader(StringIO(text))
    rows: list[list[str]] = []
    for index, row in enumerate(reader):
        if index >= limit:
            break
        rows.append(row)
    return rows


def load_file(
    file_bytes: bytes,
    filename: str,
    *,
    sheet_name: Optional[str] = None,
    sample_limit: int = 50,
    max_size_bytes: Optional[int] = None,
) -> FileSample:
    size = len(file_bytes)
    if max_size_bytes is not None and size > max_size_bytes:
        raise ValueError("Uploaded file exceeds size limit")

    detected_format = detect_format(filename, file_bytes)
    encoding: Optional[str] = None
    delimiter: Optional[str] = None
    sample_rows: list[list[str]] = []
    sheet_choice: Optional[SheetChoice] = None

    if detected_format == "csv":
        encoding = detect_encoding(file_bytes)
        text = file_bytes.decode(encoding or "utf-8", errors="ignore")
        delimiter = sniff_delimiter(text[:5_000])
        sample_rows = sample_csv_rows(text, limit=sample_limit)
    else:
        sheet_choice = pick_sheet(file_bytes, sheet_name)
        buffer = BytesIO(file_bytes)
        df = pd.read_excel(buffer, sheet_name=sheet_choice.name, nrows=sample_limit, header=None, dtype=str)
        sample_rows = df.fillna("").values.tolist()

    return FileSample(
        filename=filename,
        raw_bytes=file_bytes,
        detected_format=detected_format,
        encoding=encoding,
        delimiter=delimiter,
        sample_rows=sample_rows,
        sheet_choice=sheet_choice,
        size_bytes=size,
    )


def iter_rows(sample: FileSample, header_row: int) -> Iterable[list[str]]:
    if sample.detected_format == "csv":
        text_stream = sample.open_text()
        if header_row > 0:
            for _ in range(header_row):
                text_stream.readline()
        reader = csv.reader(text_stream, delimiter=sample.delimiter or ",")
        yield from reader
    else:
        sheet = sample.sheet_choice.name if sample.sheet_choice else 0
        df = pd.read_excel(sample.open_bytes(), sheet_name=sheet, header=None, dtype=str)
        for _, row in df.iloc[header_row + 1 :].iterrows():
            yield [value if value is not None else "" for value in row.tolist()]


__all__ = ["FileSample", "load_file", "iter_rows", "DetectedFormat"]
