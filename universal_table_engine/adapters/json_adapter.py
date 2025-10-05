from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from ..settings import AppSettings


def export_json(
    payload: Dict[str, Any],
    *,
    settings: AppSettings,
    client_id: Optional[str],
    filename: str,
) -> Dict[str, Any]:
    target_dir = settings.output_dir / (client_id or "default")
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{Path(filename).stem}.json"
    target_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2))
    return {
        "adapter": "json",
        "status": "ok",
        "path": str(target_path),
    }


__all__ = ["export_json"]
