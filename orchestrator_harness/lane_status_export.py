"""Export a coding lane's controller summary outside its experiment tree."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence


EXPORT_SCHEMA = "orchestrator-lane-status-export/v1"


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _read_object(path: Path, description: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {description}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{description} must be a JSON object")
    return value


def _read_optional_object(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return _read_object(path, path.name)


def _read_optional_text(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"cannot read {path.name}: {exc}") from exc


def _read_optional_tail(path: Path, maximum_bytes: int = 8192) -> str | None:
    if not path.exists():
        return None
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            handle.seek(max(0, handle.tell() - maximum_bytes))
            return handle.read().decode("utf-8", errors="replace")
    except OSError as exc:
        raise ValueError(f"cannot read {path.name}: {exc}") from exc


def _string(value: Mapping[str, Any], field: str) -> str:
    item = value.get(field)
    if not isinstance(item, str) or not item.strip():
        raise ValueError(f"invocation {field} must be a non-empty string")
    return item.strip()


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def export_lane_status(
    invocation_path: str | Path, result_path: str | Path
) -> dict[str, Any]:
    """Copy controller facts, worker result, and final message outside the lane."""

    invocation_file = Path(invocation_path).resolve(strict=True)
    invocation = _read_object(invocation_file, "coding invocation")
    if invocation.get("schema") != "orchestrator-coding-invocation/v1":
        raise ValueError("invocation must use orchestrator-coding-invocation/v1")
    lane_id = _string(invocation, "lane_id")
    worker_invocation_id = _string(invocation, "worker_invocation_id")
    run_root = Path(_string(invocation, "run_root")).resolve(strict=True)
    workspace = (run_root / ".agent-workspace").resolve(strict=True)
    if invocation_file.parent != workspace:
        raise ValueError("invocation must be directly under its lane workspace")
    worktrees_root = run_root.parent
    if worktrees_root.name != "worktrees":
        raise ValueError("lane run_root must be directly below an experiment worktrees directory")
    experiment_root = worktrees_root.parent
    destination = Path(result_path).resolve(strict=False)
    if _inside(destination, experiment_root):
        raise ValueError("export destination must be outside the lane workspace and experiment tree")

    exported = {
        "schema": EXPORT_SCHEMA,
        "lane_id": lane_id,
        "worker_invocation_id": worker_invocation_id,
        "controller_status": _read_optional_object(workspace / "controller.status.json"),
        "worker_result": _read_optional_object(workspace / "RESULT.json"),
        "worker_last_message": _read_optional_text(workspace / "last-message.txt"),
        "worker_stderr_tail": _read_optional_tail(workspace / "codex.stderr.log"),
    }
    _atomic_json(destination, exported)
    return exported


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export a coding lane status/result outside the experiment tree"
    )
    parser.add_argument("invocation", type=Path)
    parser.add_argument("--result", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        exported = export_lane_status(args.invocation, args.result)
    except ValueError as exc:
        print(json.dumps({"schema": EXPORT_SCHEMA, "status": "failed", "error": str(exc)}))
        return 1
    print(json.dumps(exported, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
