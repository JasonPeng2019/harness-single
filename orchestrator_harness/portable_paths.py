"""One spelling for product identifiers and portable repository entries."""
from __future__ import annotations

from pathlib import PureWindowsPath


def component(value: object) -> str:
    """Reject components that escape, alias or select Windows devices/streams."""
    if (not isinstance(value, str) or not value or value in {".", ".."}
            or value[-1] in {".", " "}
            or any(ord(char) < 32 or char in '<>:"/\\|?*' for char in value)
            or PureWindowsPath(value).is_reserved()):
        raise ValueError(f"invalid portable path component: {value!r}")
    return value


def repository_parts(value: object) -> tuple[str, ...]:
    """Validate literal, slash-separated repository paths without normalizing."""
    if not isinstance(value, str) or not value:
        raise ValueError("repository path must be a nonempty relative string")
    return tuple(component(part) for part in value.split("/"))


def admit_entry(nodes: dict[str, tuple[str, str]], parts: tuple[str, ...]) -> None:
    """Admit a file only if every directory and file keeps its exact spelling."""
    for index in range(1, len(parts) + 1):
        spelling = "/".join(parts[:index])
        kind = "file" if index == len(parts) else "directory"
        key = spelling.casefold()
        previous = nodes.get(key)
        if previous is not None and (previous != (spelling, kind) or kind == "file"):
            raise ValueError(f"public input paths alias or conflict: {spelling}")
        nodes[key] = spelling, kind
