from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_json(path: Path, value: dict[str, Any]) -> None:
    """Write one deterministic fixture record for product-owned tests."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
