"""Watcher-owned output through the harness's prepared file boundary."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from orchestrator_harness.stable_io import PreparedOutput, PathKeyedAppendLock


def validate_path(root: Path, target: Path) -> Path:
    return PreparedOutput(Path(root).absolute()).validate_path(Path(target).absolute())


def output(root: Path, target: Path) -> PreparedOutput:
    transaction = PreparedOutput(Path(root).absolute())
    transaction.prepare()
    transaction.admit(Path(target).absolute())
    transaction.ensure_parent(Path(target).absolute())
    return transaction


def read_json(root: Path, path: Path) -> dict[str, Any] | None:
    transaction = PreparedOutput(Path(root).absolute())
    transaction.validate_path(Path(path).absolute())
    if not transaction.root.exists():
        return None
    transaction.prepare()
    transaction.admit(Path(path).absolute())
    try:
        raw = transaction.read_bytes(path)
    except FileNotFoundError:
        return None
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f"watcher record is not an object: {path}")
    return value


def atomic_json(root: Path, path: Path, value: Any) -> None:
    transaction = output(root, path)
    transaction.publish_json(path, value)


@contextmanager
def lock(root: Path, path: Path) -> Iterator[None]:
    transaction = output(root, path)
    with PathKeyedAppendLock(path, lock_root=path.parent):
        transaction.revalidate(path)
        yield
        transaction.revalidate(path)


def append_records(root: Path, path: Path, values: list[dict[str, Any]]) -> None:
    transaction = output(root, path)
    transaction.append_jsonl(path, values)
