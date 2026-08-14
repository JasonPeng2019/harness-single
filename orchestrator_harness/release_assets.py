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


def manifest_asset_paths() -> tuple[str, ...]:
    """Return and validate every canonical asset path declared by the manifest."""

    manifest = release_manifest()
    paths: list[str] = []
    for field in ("examples", "release_evidence_templates"):
        values = manifest.get(field)
        if not isinstance(values, list) or any(
            not isinstance(item, str) for item in values
        ):
            raise RuntimeError(f"release asset manifest field {field} is invalid")
        paths.extend(values)
    if len(set(paths)) != len(paths):
        raise RuntimeError("release asset manifest contains duplicate canonical paths")
    for path in paths:
        read_package_asset(path)
    return tuple(paths)


__all__ = ["manifest_asset_paths", "read_package_asset", "release_manifest"]
