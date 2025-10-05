from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Optional

import pandas as pd


@dataclass(slots=True)
class SheetChoice:
    name: str
    non_empty_cells: int


def pick_sheet(excel_bytes: bytes, sheet_name: Optional[str] = None) -> SheetChoice:
    buffer = BytesIO(excel_bytes)
    with pd.ExcelFile(buffer) as workbook:
        sheets = workbook.sheet_names
        if not sheets:
            raise ValueError("Excel file contains no sheets")
        if sheet_name and sheet_name in sheets:
            sample = workbook.parse(sheet_name, nrows=200)
            score = int(sample.count().sum())
            return SheetChoice(name=sheet_name, non_empty_cells=score)
        best_choice: SheetChoice | None = None
        for name in sheets:
            sample = workbook.parse(name, nrows=200)
            score = int(sample.count().sum())
            if best_choice is None or score > best_choice.non_empty_cells:
                best_choice = SheetChoice(name=name, non_empty_cells=score)
        assert best_choice is not None
        return best_choice


__all__ = ["SheetChoice", "pick_sheet"]
