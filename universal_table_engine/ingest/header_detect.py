from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

from .llm_helper import HeaderLLMClient, HeaderPrediction

_KEYWORDS = {
    "date",
    "data",
    "order",
    "invoice",
    "numar",
    "valoare",
    "tva",
    "client",
    "email",
    "total",
    "amount",
    "qty",
    "quantity",
    "method",
    "status",
}


@dataclass(slots=True)
class HeuristicResult:
    header_row: int
    columns: List[str]
    score: float
    notes: List[str]


@dataclass(slots=True)
class HeaderDetectionResult:
    header_row: int
    columns: List[str]
    confidence: float
    notes: List[str]
    used_llm: bool


def detect_header(
    sample_rows: Iterable[Iterable[str]],
    *,
    llm_client: Optional[HeaderLLMClient] = None,
    max_rows: int = 50,
    llm_threshold: float = 0.7,
) -> HeaderDetectionResult:
    rows = [list(row) for index, row in zip(range(max_rows), sample_rows)]
    heuristic = _heuristic_detect(rows)
    notes = list(heuristic.notes)
    used_llm = False
    confidence = min(max(heuristic.score, 0.2), 0.95)

    if llm_client is not None:
        llm_prediction = llm_client(rows)
        if llm_prediction is not None:
            notes.append("llm_header_prediction_available")
            if llm_prediction.confidence >= llm_threshold:
                used_llm = True
                confidence = float(llm_prediction.confidence)
                notes.append("llm_header_selected")
                return HeaderDetectionResult(
                    header_row=llm_prediction.header_row,
                    columns=llm_prediction.columns,
                    confidence=confidence,
                    notes=notes,
                    used_llm=used_llm,
                )
            else:
                confidence = max(confidence, float(llm_prediction.confidence) * 0.8)
                notes.append("llm_confidence_low_using_heuristic")
    return HeaderDetectionResult(
        header_row=heuristic.header_row,
        columns=heuristic.columns,
        confidence=confidence,
        notes=notes,
        used_llm=used_llm,
    )


def _heuristic_detect(rows: List[List[str]]) -> HeuristicResult:
    best_score = -1.0
    best_row = 0
    best_columns: List[str] = []
    notes: List[str] = []

    for idx, row in enumerate(rows):
        non_empty = sum(1 for cell in row if str(cell).strip())
        alpha_cells = sum(1 for cell in row if any(ch.isalpha() for ch in str(cell)))
        keyword_hits = sum(1 for cell in row if _contains_keyword(str(cell)))

        if len(row) == 0:
            continue
        density = non_empty / max(len(row), 1)
        alpha_ratio = alpha_cells / max(len(row), 1)

        score = density * 0.45 + alpha_ratio * 0.35 + keyword_hits * 0.05 + non_empty * 0.02
        if non_empty == 0:
            score = 0.0
        if score > best_score:
            best_score = score
            best_row = idx
            best_columns = [str(cell).strip() for cell in row]

    if best_score < 0.3:
        notes.append("low_heuristic_confidence_header")
    notes.append(f"heuristic_header_row={best_row}")

    return HeuristicResult(header_row=best_row, columns=best_columns, score=best_score, notes=notes)


def _contains_keyword(value: str) -> bool:
    lowered = value.lower()
    return any(keyword in lowered for keyword in _KEYWORDS)


__all__ = ["HeaderDetectionResult", "detect_header"]
