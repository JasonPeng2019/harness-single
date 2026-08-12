from __future__ import annotations

"""Read-only access to the release asset manifest shipped with the package."""

import json
from importlib import resources
from typing import Any


def release_manifest() -> dict[str, Any]:
    resource = resources.files("orchestrator_harness").joinpath(
        "assets", "release", "manifest.json"
    )
    try:
        value = json.loads(resource.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"release asset manifest is unavailable: {exc}") from exc
    if (
        not isinstance(value, dict)
        or value.get("schema") != "orchestrator-release-assets/v1"
    ):
        raise RuntimeError("release asset manifest has an invalid schema")
    return value


def read_package_asset(relative_path: str) -> str:
    """Read one declared package asset without reaching outside the package."""

    parts = tuple(
        part
        for part in relative_path.replace("\\", "/").split("/")
        if part not in ("", ".")
    )
    if not parts or ".." in parts:
        raise ValueError("package asset path must be relative")
    resource = resources.files("orchestrator_harness")
    for part in parts:
        resource = resource.joinpath(part)
    if not resource.is_file():
        raise FileNotFoundError(relative_path)
    return resource.read_text(encoding="utf-8")


__all__ = ["read_package_asset", "release_manifest"]
