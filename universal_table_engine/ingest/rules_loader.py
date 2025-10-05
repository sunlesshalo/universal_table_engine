from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

from ..settings import AppSettings


@dataclass(slots=True)
class LoadedRule:
    name: str
    payload: Dict[str, object]
    score: float


def load_matching_rule(
    filename: str,
    columns: Iterable[str],
    *,
    settings: AppSettings,
    source_hint: Optional[str] = None,
) -> Tuple[Optional[dict], list[str]]:
    notes: list[str] = []
    rules_dir = settings.rules_dir
    if not rules_dir.exists():
        notes.append("rules_directory_missing")
        return None, notes

    candidates: list[LoadedRule] = []
    for path in rules_dir.glob("*.json"):
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            notes.append(f"rule_invalid_json:{path.name}")
            continue
        score = _score_rule(payload, filename, columns, source_hint)
        if score > 0:
            candidates.append(LoadedRule(name=path.stem, payload=payload, score=score))
        elif path.stem == "default":
            candidates.append(LoadedRule(name=path.stem, payload=payload, score=0.1))

    if not candidates:
        return None, notes

    selected = max(candidates, key=lambda rule: rule.score)
    if selected.name == "default" and selected.score < 0.5:
        notes.append("default_rule_applied")
    else:
        notes.append(f"rule_selected:{selected.name}")
    return selected.payload, notes


def _score_rule(payload: dict, filename: str, columns: Iterable[str], source_hint: Optional[str]) -> float:
    match = payload.get("match", {}) if isinstance(payload, dict) else {}
    score = 0.0

    filenames = [value.lower() for value in match.get("filenames", [])]
    lowered_filename = filename.lower()
    for token in filenames:
        if token and token in lowered_filename:
            score += 0.6

    if source_hint:
        lowered_hint = source_hint.lower()
        hints = [value.lower() for value in match.get("hints", [])]
        for token in hints:
            if token and token in lowered_hint:
                score += 0.6

    column_keywords = [value.lower() for value in match.get("columns", [])]
    lowered_columns = [value.lower() for value in columns]
    overlap = len(set(lowered_columns) & set(column_keywords))
    if overlap:
        score += min(0.4, overlap * 0.1)

    return score


__all__ = ["load_matching_rule"]
