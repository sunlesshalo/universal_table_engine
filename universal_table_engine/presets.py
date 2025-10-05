from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from .settings import AppSettings


@dataclass(slots=True)
class Preset:
    client_id: str
    preset_id: str
    defaults: Dict[str, Any]
    path: Path


def _preset_filename(client_id: str, preset_id: str) -> str:
    return f"{client_id}__{preset_id}.json"


def preset_path(client_id: str, preset_id: str, settings: AppSettings) -> Path:
    return settings.presets_dir / _preset_filename(client_id, preset_id)


def load_preset(client_id: str, preset_id: str, settings: AppSettings) -> Optional[Preset]:
    path = preset_path(client_id, preset_id, settings)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        defaults = data.get("defaults") if isinstance(data, dict) else None
        if defaults is None:
            defaults = data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return None
    return Preset(client_id=client_id, preset_id=preset_id, defaults=defaults, path=path)


def list_presets(settings: AppSettings, client_id: Optional[str] = None) -> Iterable[Preset]:
    directory = settings.presets_dir
    if not directory.exists():
        return []
    results: list[Preset] = []
    for entry in sorted(directory.glob("*.json")):
        name = entry.stem
        if "__" not in name:
            continue
        prefix, preset_id = name.split("__", 1)
        if client_id and prefix != client_id:
            continue
        preset = load_preset(prefix, preset_id, settings)
        if preset:
            results.append(preset)
    return results


def merge_with_preset(defaults: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    merged.update(defaults or {})
    merged.update({key: value for key, value in overrides.items() if value is not None})
    return merged


__all__ = ["Preset", "load_preset", "preset_path", "list_presets", "merge_with_preset"]
